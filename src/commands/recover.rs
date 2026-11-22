//! `recover`: copy values that the storage files hold but no query returns.
//!
//! InfluxDB finds the values of a series through the shard's field index. If a
//! field is missing there (seen in real buckets after deletes and type-conflict
//! clean-ups), Flux and InfluxQL skip its values without an error, and so does
//! a migration through the query API. `influxd inspect export-lp` reads the TSM
//! and WAL files directly and still returns them.
//!
//! "Visible" is defined by the query API itself: per window, the
//! (measurement, field) pairs that a query returns. export-lp lines of pairs
//! that are not visible in their window are converted with the migration rules
//! and written; visible pairs are never written, so nothing the migration
//! already copied is touched. Visibility includes the value type: a value
//! whose stored type differs from the type the query returns for that pair
//! (left behind by type conflicts; queries skip it) counts as hidden even
//! when the pair itself is visible.

use super::migrate::parse_scope;
use super::{Resources, connect, connect_target, date, human_duration, sh};
use crate::cli::{Global, OnReject, RecoverArgs};
use crate::convert::Rules;
use crate::error::{Failure, Kind, fail};
use crate::export::{Export, timestamp};
use crate::fast::Counters;
use crate::fastlp::{FieldSpan, LineError, LpConverter, MAX_LINE_BYTES, split_fields, trim_end};
use crate::flux::{self, for_each_row};
use crate::guard::{InfluxGate, MemWatch, RateLimit, Shared, aborted_error};
use crate::influx::{Bucket, Client};
use crate::output::{Outcome, n};
use crate::pipeline::{Config, Event, LineSink, Pipeline};
use crate::state::{self, Header, WindowRecord};
use crate::window::{NS, Window, chunk_group, grid_secs, now_ns, parse_time, windows};
use anyhow::Context as _;
use crossbeam_channel::{bounded, select, unbounded};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;
use std::io::BufRead;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tracing::{info, warn};

const PROGRESS_EVERY: Duration = Duration::from_secs(30);

/// (measurement, field) as bytes: unescaped measurement, 0, unescaped field,
/// decoded like the query API decodes them (invalid UTF-8 replaced). The
/// visible sets append 0 and a value type (see [`value_type`]).
type PairKey = Vec<u8>;

#[derive(Serialize)]
struct PairOut {
    measurement: String,
    field: String,
    values: u64,
}

#[derive(Serialize)]
struct WindowOut {
    start: String,
    hidden_pairs: usize,
    hidden_values: u64,
    values_written: u64,
}

#[derive(Serialize)]
struct Report {
    source: String,
    target: String,
    dry_run: bool,
    state_file: String,
    window_secs: i64,
    export_chunk_days: i64,
    windows_total: usize,
    windows_skipped_already_done: usize,
    windows_checked_now: usize,
    windows_with_hidden_values: usize,
    visible_query_fallbacks: usize,
    lines_read: u64,
    hidden_pairs: usize,
    hidden_values: u64,
    values_written: u64,
    values_dropped: u64,
    write_requests: u64,
    rejected_batches: u64,
    elapsed_secs: u64,
    lines_per_sec: u64,
    /// Every hidden (measurement, field) pair, most values first.
    pairs: Vec<PairOut>,
    /// Windows that had hidden values.
    windows: Vec<WindowOut>,
    input_types: BTreeMap<String, u64>,
    actions: BTreeMap<String, u64>,
    rules: Rules,
    convert_where_tag: Vec<String>,
    resources: Resources,
}

#[derive(Default, Clone)]
struct WinStat {
    hidden_values: u64,
    pairs: HashMap<PairKey, u64>,
}

/// What a reader hands back when a chunk is fully converted.
struct ReadDone {
    counters: Counters,
    stats: Vec<WinStat>,
    /// window start ns -> measurement -> written values
    written: HashMap<i64, BTreeMap<String, u64>>,
    batches: u64,
    fallbacks: usize,
}

#[derive(Default)]
struct JobProgress {
    read: Option<ReadDone>,
    acked: u64,
    rejected: u64,
}

fn slug(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Unescape a line protocol key (`\,` `\=` `\ ` `\\`) into `out`.
fn unescape_into(s: &[u8], out: &mut Vec<u8>) {
    let mut i = 0;
    while i < s.len() {
        if s[i] == b'\\' && i + 1 < s.len() && matches!(s[i + 1], b',' | b'=' | b' ' | b'\\') {
            i += 1;
        }
        out.push(s[i]);
        i += 1;
    }
}

/// The pair key of a measurement and field key as written in line protocol.
fn pair_key(measurement: &[u8], field: &[u8], out: &mut PairKey) {
    out.clear();
    unescape_into(measurement, out);
    out.push(0);
    unescape_into(field, out);
    if std::str::from_utf8(out).is_err() {
        *out = String::from_utf8_lossy(out).into_owned().into_bytes();
    }
}

fn pair_key_str(measurement: &str, field: &str) -> PairKey {
    let mut k = Vec::with_capacity(measurement.len() + field.len() + 1);
    k.extend_from_slice(measurement.as_bytes());
    k.push(0);
    k.extend_from_slice(field.as_bytes());
    k
}

/// One byte per value type: f(loat), i(nt), u(int), s(tring), b(ool).
fn value_type(v: &[u8]) -> u8 {
    match v {
        [b'"', ..] => b's',
        b"t" | b"T" | b"true" | b"True" | b"TRUE" | b"f" | b"F" | b"false" | b"False"
        | b"FALSE" => b'b',
        [.., b'i'] => b'i',
        [.., b'u'] => b'u',
        _ => b'f',
    }
}

/// The value type of a Flux `#datatype` annotation; `None` for anything else.
fn flux_type(datatype: &str) -> Option<u8> {
    Some(match datatype {
        "double" => b'f',
        "long" => b'i',
        "unsignedLong" => b'u',
        "string" => b's',
        "boolean" => b'b',
        _ => return None,
    })
}

fn typed_key(key: &[u8], ty: u8, out: &mut PairKey) {
    out.clear();
    out.extend_from_slice(key);
    out.push(0);
    out.push(ty);
}

fn split_key(k: &[u8]) -> (String, String) {
    let (m, f) = match k.iter().position(|b| *b == 0) {
        Some(p) => (&k[..p], &k[p + 1..]),
        None => (k, &b""[..]),
    };
    (
        String::from_utf8_lossy(m).into_owned(),
        String::from_utf8_lossy(f).into_owned(),
    )
}

/// The (measurement, field, value type) triples the query API returns for one
/// window.
///
/// `first()` is pushed down into the storage engine and returns one row per
/// series and field, which is cheap. InfluxDB 2.x can panic in that path
/// ("unreachable: <nil>"); such a window is scanned row by row instead.
/// Both go through the same field index, so they see the same pairs.
fn visible_pairs(
    client: &Client,
    bucket: &str,
    w: Window,
    measurements: &[String],
) -> anyhow::Result<(HashSet<PairKey>, bool)> {
    let src = flux::source(bucket, w, measurements);
    let collect = |q: &str| -> anyhow::Result<HashSet<PairKey>> {
        let mut set = HashSet::new();
        for_each_row(client.query(q)?, |h, r| {
            let get = |c: &str| h.index(c).and_then(|i| r.get(i)).unwrap_or("");
            let datatype = h
                .index("_value")
                .and_then(|i| h.types.get(i))
                .map_or("", String::as_str);
            let Some(ty) = flux_type(datatype) else {
                return Err(fail(
                    Kind::Internal,
                    format!("unexpected _value type `{datatype}` in query result"),
                )
                .into());
            };
            let mut k = Vec::new();
            typed_key(
                &pair_key_str(get("_measurement"), get("_field")),
                ty,
                &mut k,
            );
            set.insert(k);
            Ok(())
        })?;
        Ok(set)
    };
    let fast = format!(
        "{src} |> first() |> group(columns: [\"_measurement\", \"_field\"]) |> limit(n: 1) |> keep(columns: [\"_measurement\", \"_field\", \"_value\"])"
    );
    match collect(&fast) {
        Ok(set) => Ok((set, false)),
        Err(e) if format!("{e:#}").contains("panic") => {
            warn!(window = %date(w.start), error = %format!("{e:#}"), "InfluxDB failed on the pushed-down query, scanning the window's rows instead");
            let scan =
                format!("{src} |> keep(columns: [\"_measurement\", \"_field\", \"_value\"])");
            Ok((collect(&scan)?, true))
        }
        Err(e) => Err(e),
    }
}

/// Decides per value whether it is hidden; remembers the last field and type
/// so the many consecutive lines of one series and field (export-lp is sorted
/// by series key) cost one comparison each.
struct Classifier {
    raw: Vec<u8>,
    win: usize,
    key: PairKey,
    typed: PairKey,
    ty: u8,
    hidden: bool,
}

impl Classifier {
    fn new() -> Self {
        Self {
            raw: Vec::new(),
            win: usize::MAX,
            key: Vec::new(),
            typed: Vec::new(),
            ty: 0,
            hidden: false,
        }
    }

    /// Returns whether a value of type `ty` is hidden in window `win`.
    fn classify(
        &mut self,
        measurement: &[u8],
        field: &[u8],
        ty: u8,
        win: usize,
        visible: &HashSet<PairKey>,
    ) -> bool {
        let same = win == self.win
            && self.raw.len() == measurement.len() + 1 + field.len()
            && self.raw.starts_with(measurement)
            && self.raw[measurement.len()] == 0
            && &self.raw[measurement.len() + 1..] == field;
        if !same {
            self.raw.clear();
            self.raw.extend_from_slice(measurement);
            self.raw.push(0);
            self.raw.extend_from_slice(field);
            self.win = win;
            pair_key(measurement, field, &mut self.key);
            self.ty = 0;
        }
        if ty != self.ty {
            typed_key(&self.key, ty, &mut self.typed);
            self.hidden = !visible.contains(self.typed.as_slice());
            self.ty = ty;
        }
        self.hidden
    }
}

/// Filters export-lp lines down to hidden fields; returns the line to write
/// (the original or one rebuilt with only the hidden fields) or `None`.
struct Filter<'a> {
    windows: &'a [Window],
    visible: &'a [HashSet<PairKey>],
    measurements: HashSet<Vec<u8>>,
    fields: Vec<FieldSpan>,
    hidden: Vec<usize>,
    rebuilt: Vec<u8>,
    meas_buf: Vec<u8>,
    cls: Classifier,
    stats: Vec<WinStat>,
}

impl<'a> Filter<'a> {
    fn new(
        windows: &'a [Window],
        visible: &'a [HashSet<PairKey>],
        measurements: &[String],
    ) -> Self {
        Self {
            windows,
            visible,
            measurements: measurements.iter().map(|m| m.as_bytes().to_vec()).collect(),
            fields: Vec::with_capacity(8),
            hidden: Vec::with_capacity(8),
            rebuilt: Vec::new(),
            meas_buf: Vec::new(),
            cls: Classifier::new(),
            stats: vec![WinStat::default(); windows.len()],
        }
    }

    /// `Ok(None)`: nothing hidden (or outside the windows); `Ok(Some(..))`:
    /// the line protocol to convert and write.
    fn apply<'l>(&'l mut self, line: &'l [u8]) -> Result<Option<&'l [u8]>, LineError> {
        let line = trim_end(line);
        if line.is_empty() || line[0] == b'#' {
            return Ok(None);
        }
        let (meas_end, series_end, fields_end) = split_fields(line, &mut self.fields)?;
        let Some(ts) = timestamp(line) else {
            return Err(LineError::Invalid("missing or invalid timestamp"));
        };
        let i = self.windows.partition_point(|w| w.start <= ts);
        if i == 0 || ts >= self.windows[i - 1].stop {
            return Ok(None);
        }
        let win = i - 1;
        let measurement = &line[..meas_end];
        if !self.measurements.is_empty() {
            self.meas_buf.clear();
            unescape_into(measurement, &mut self.meas_buf);
            if !self.measurements.contains(&self.meas_buf) {
                return Ok(None);
            }
        }
        self.hidden.clear();
        for (k, ((ks, ke), (vs, ve))) in self.fields.iter().enumerate() {
            let ty = value_type(&line[*vs..*ve]);
            if self
                .cls
                .classify(measurement, &line[*ks..*ke], ty, win, &self.visible[win])
            {
                self.hidden.push(k);
                let st = &mut self.stats[win];
                st.hidden_values += 1;
                match st.pairs.get_mut(self.cls.key.as_slice()) {
                    Some(c) => *c += 1,
                    None => {
                        st.pairs.insert(self.cls.key.clone(), 1);
                    }
                }
            }
        }
        if self.hidden.is_empty() {
            return Ok(None);
        }
        if self.hidden.len() == self.fields.len() {
            return Ok(Some(line));
        }
        // Mixed line (export-lp writes one field per line, other sources may not).
        self.rebuilt.clear();
        self.rebuilt.extend_from_slice(&line[..series_end]);
        self.rebuilt.push(b' ');
        for (n, &k) in self.hidden.iter().enumerate() {
            if n > 0 {
                self.rebuilt.push(b',');
            }
            let ((ks, _), (_, ve)) = self.fields[k];
            self.rebuilt.extend_from_slice(&line[ks..ve]);
        }
        self.rebuilt.extend_from_slice(&line[fields_end..]);
        Ok(Some(&self.rebuilt))
    }
}

pub fn run(g: &Global, a: &RecoverArgs) -> anyhow::Result<Outcome> {
    if a.batch_size == 0 || a.batch_mb == 0 || a.writers == 0 || g.parallel == 0 {
        return Err(fail(
            Kind::Usage,
            "--batch-size, --batch-mb, --writers and --parallel must be at least 1",
        )
        .into());
    }
    if a.source_bucket == a.target_bucket && g.target_url.is_none() {
        return Err(fail(Kind::Refused, "source and target bucket are the same")
            .hint("recover into the migrated bucket, e.g. --source-bucket <old> --target-bucket <migrated>")
            .into());
    }
    let shared = Shared::new_registered();
    let mem = MemWatch::start(g.memory_limit_mb, shared.clone());
    let r = run_inner(g, a, &shared, &mem);
    mem.stop();
    r
}

fn run_inner(
    g: &Global,
    a: &RecoverArgs,
    shared: &Arc<Shared>,
    mem: &MemWatch,
) -> anyhow::Result<Outcome> {
    let rules = a.rules.rules();
    let scope = parse_scope(&a.convert_where_tag)?;
    let export = Export::new(&a.influxd, &a.engine_path)?;
    let src = Arc::new(connect(g)?);
    let tgt = Arc::new(connect_target(g)?);
    let separate_target = g.target_url.is_some() || g.target_org.is_some();
    let source: Bucket = src.bucket(&a.source_bucket)?;
    let target = tgt.find_bucket(&a.target_bucket)?;
    match &target {
        Some(t) if !separate_target && t.id == source.id => {
            return Err(fail(Kind::Refused, "source and target are the same bucket").into());
        }
        None if !a.dry_run => {
            return Err(fail(
                Kind::NotFound,
                format!("target bucket `{}` does not exist", a.target_bucket),
            )
            .hint("recover writes into an existing bucket, usually the one `migrate` created")
            .into());
        }
        _ => {}
    }

    let grid = grid_secs(source.shard_group_secs, None)?;
    let chunk_secs = if a.export_chunk_days > 0 {
        i64::from(a.export_chunk_days) * 86_400
    } else {
        source.shard_group_secs.max(86_400)
    };
    let state_path = a.state.clone().unwrap_or_else(|| {
        PathBuf::from(format!(
            "lmh-recover-{}-to-{}{}.jsonl",
            slug(&a.source_bucket),
            slug(&a.target_bucket),
            if a.dry_run { "-dry-run" } else { "" }
        ))
    });
    let header = Header {
        kind: "header".into(),
        version: state::VERSION,
        source: format!("recover:{}", a.source_bucket),
        target: a.target_bucket.clone(),
        dry_run: a.dry_run,
        window_secs: grid,
        rules: Some(rules.clone()),
        convert_where_tag: a.convert_where_tag.clone(),
    };
    let mut state_writer = state::Writer::open(&state_path, &header)?;
    let finished = if a.redo {
        Default::default()
    } else {
        state::finished_windows(&state_path)?
    };

    let start = parse_time(&a.start)?;
    let stop = parse_time(&a.stop)?;
    let all = windows(start, stop, grid);
    let todo: Vec<Window> = all
        .iter()
        .copied()
        .filter(|w| !finished.contains(&w.start))
        .collect();
    let skipped = all.len() - todo.len();
    // One job per shard-group-aligned chunk: one export call decodes it once.
    let mut jobs: Vec<Vec<Window>> = Vec::new();
    for w in todo {
        match jobs.last_mut() {
            Some(c) if chunk_group(c[0].start, chunk_secs) == chunk_group(w.start, chunk_secs) => {
                c.push(w)
            }
            _ => jobs.push(vec![w]),
        }
    }
    info!(
        source = %source.name, source_id = %source.id, target = %a.target_bucket, dry_run = a.dry_run,
        windows = all.len(), already_done = skipped, chunks = jobs.len(),
        window = %human_duration(grid), chunk = %human_duration(chunk_secs),
        start = %date(start), stop = %date(stop), engine_path = %a.engine_path, influxd = %a.influxd,
        state_file = %state_path.display(), "recover started"
    );

    let gate = InfluxGate::start(
        tgt.clone(),
        if a.dry_run {
            0
        } else {
            a.influx_heap_headroom_mb
        },
        Duration::from_secs(a.max_pause_secs),
    );
    let rate = Arc::new(RateLimit::new(a.max_rate));
    let readers = g.parallel.min(jobs.len()).max(1);
    let pipeline = Pipeline::start(
        tgt.clone(),
        Config {
            bucket: a.target_bucket.clone(),
            dry_run: a.dry_run,
            gzip: !a.no_gzip,
            skip_rejected: a.on_reject == OnReject::Skip,
            batch_values: a.batch_size,
            batch_bytes: a.batch_mb << 20,
        },
        a.writers,
        readers,
        shared.clone(),
        gate.clone(),
        rate,
    );

    let began = Instant::now();
    let lines_seen = AtomicU64::new(0);
    let mut totals = Counters::default();
    let mut write_requests = 0u64;
    let mut rejected_batches = 0u64;
    let mut first_rejection: Option<String> = None;
    let mut chunks_done = 0usize;
    let mut windows_done = 0usize;
    let mut acked_values = 0u64;
    let mut fallbacks = 0usize;
    let mut pair_totals: HashMap<PairKey, u64> = HashMap::new();
    let mut window_out: Vec<WindowOut> = Vec::new();

    let (job_tx, job_rx) = bounded::<usize>(jobs.len().max(1));
    for i in 0..jobs.len() {
        let _ = job_tx.send(i);
    }
    drop(job_tx);
    let (res_tx, res_rx) = unbounded::<(usize, ReadDone)>();

    std::thread::scope(|ts| -> anyhow::Result<()> {
        for r in 0..readers.min(jobs.len()) {
            let (
                job_rx,
                res_tx,
                pipeline,
                src,
                shared,
                rules,
                scope,
                lines_seen,
                export,
                jobs,
                source,
            ) = (
                job_rx.clone(),
                res_tx.clone(),
                &pipeline,
                src.clone(),
                shared.clone(),
                &rules,
                &scope,
                &lines_seen,
                &export,
                &jobs,
                &source,
            );
            std::thread::Builder::new()
                .name(format!("reader-{r}"))
                .spawn_scoped(ts, move || {
                    for job in job_rx.iter() {
                        if shared.aborted() {
                            break;
                        }
                        let wins = &jobs[job];
                        let res = read_chunk(
                            &src, export, source, wins, a, g.parallel, rules, scope, grid,
                            pipeline, job, &shared, lines_seen,
                        )
                        .with_context(|| {
                            format!(
                                "chunk {} .. {}",
                                date(wins[0].start),
                                date(wins[wins.len() - 1].stop)
                            )
                        });
                        match res {
                            Ok(done) => {
                                let _ = res_tx.send((job, done));
                            }
                            Err(e) => {
                                shared.fail(e);
                                break;
                            }
                        }
                    }
                })
                .expect("spawning reader");
        }
        drop(res_tx);

        let mut progress: HashMap<usize, JobProgress> = HashMap::new();
        let mut last_log = Instant::now();
        while chunks_done < jobs.len() && !shared.aborted() {
            let mut touched = None;
            select! {
                recv(pipeline.events) -> ev => match ev {
                    Ok(Event::Acked { job, values }) => {
                        write_requests += 1;
                        acked_values += values;
                        progress.entry(job).or_default().acked += 1;
                        touched = Some(job);
                    }
                    Ok(Event::Rejected { job, message }) => {
                        rejected_batches += 1;
                        first_rejection.get_or_insert(message);
                        progress.entry(job).or_default().rejected += 1;
                        touched = Some(job);
                    }
                    Err(_) => break,
                },
                recv(res_rx) -> r => if let Ok((job, done)) = r {
                    progress.entry(job).or_default().read = Some(done);
                    touched = Some(job);
                },
                default(Duration::from_millis(500)) => {}
            }
            if let Some(job) = touched {
                let complete = progress
                    .get(&job)
                    .and_then(|p| p.read.as_ref().map(|r| p.acked + p.rejected == r.batches))
                    .unwrap_or(false);
                if complete {
                    let p = progress.remove(&job).expect("present");
                    let done = p.read.expect("checked");
                    totals.merge(&done.counters);
                    fallbacks += done.fallbacks;
                    let finished_at = date(now_ns());
                    for (w, st) in jobs[job].iter().zip(done.stats) {
                        let by_m = done.written.get(&w.start).cloned().unwrap_or_default();
                        let written: u64 = by_m.values().sum();
                        if st.hidden_values > 0 {
                            info!(start = %date(w.start), hidden_pairs = st.pairs.len(), hidden_values = st.hidden_values, values_written = written, "window recovered");
                            window_out.push(WindowOut {
                                start: date(w.start),
                                hidden_pairs: st.pairs.len(),
                                hidden_values: st.hidden_values,
                                values_written: written,
                            });
                        }
                        for (k, c) in st.pairs {
                            *pair_totals.entry(k).or_default() += c;
                        }
                        state_writer.append(&WindowRecord {
                            kind: "window".into(),
                            start_ns: w.start,
                            stop_ns: w.stop,
                            values_read: st.hidden_values,
                            values_written: written,
                            values_dropped: st.hidden_values.saturating_sub(written),
                            rejected_batches: p.rejected,
                            written_by_measurement: by_m,
                            finished_at: finished_at.clone(),
                        })?;
                        windows_done += 1;
                    }
                    chunks_done += 1;
                }
            }
            if last_log.elapsed() >= PROGRESS_EVERY {
                info!(
                    chunks_done = format!("{chunks_done}/{}", jobs.len()),
                    windows_done,
                    lines_read = lines_seen.load(Ordering::Relaxed),
                    values_acked = acked_values,
                    rss_mib = crate::guard::rss_bytes().map(|b| b >> 20),
                    "progress"
                );
                last_log = Instant::now();
            }
        }
        Ok(())
    })?;
    pipeline.close();
    gate.stop();

    if shared.aborted() {
        let e = shared.take_reason().unwrap_or_else(aborted_error);
        info!(windows_finished = windows_done, state_file = %state_path.display(), "recover stopped; finished chunks are recorded, rerun to resume");
        return Err(e);
    }

    let elapsed = began.elapsed();
    let lines_read = lines_seen.load(Ordering::Relaxed);
    let mut pairs: Vec<PairOut> = pair_totals
        .into_iter()
        .map(|(k, values)| {
            let (measurement, field) = split_key(&k);
            PairOut {
                measurement,
                field,
                values,
            }
        })
        .collect();
    pairs.sort_by(|x, y| {
        y.values
            .cmp(&x.values)
            .then_with(|| x.measurement.cmp(&y.measurement))
            .then_with(|| x.field.cmp(&y.field))
    });
    window_out.sort_by(|x, y| x.start.cmp(&y.start));
    let hidden_values: u64 = pairs.iter().map(|p| p.values).sum();
    let report = Report {
        source: a.source_bucket.clone(),
        target: a.target_bucket.clone(),
        dry_run: a.dry_run,
        state_file: state_path.display().to_string(),
        window_secs: grid,
        export_chunk_days: chunk_secs / 86_400,
        windows_total: all.len(),
        windows_skipped_already_done: skipped,
        windows_checked_now: windows_done,
        windows_with_hidden_values: window_out.len(),
        visible_query_fallbacks: fallbacks,
        lines_read,
        hidden_pairs: pairs.len(),
        hidden_values,
        values_written: totals.values_written,
        values_dropped: totals.values_dropped,
        write_requests,
        rejected_batches,
        elapsed_secs: elapsed.as_secs(),
        lines_per_sec: (lines_read as f64 / elapsed.as_secs_f64().max(0.001)) as u64,
        pairs,
        windows: window_out,
        input_types: totals.types_map(),
        actions: totals.actions_map(),
        rules: rules.clone(),
        convert_where_tag: a.convert_where_tag.clone(),
        resources: Resources {
            peak_rss_mib: mem.peak_mib(),
            memory_limit_mib: mem.limit_bytes >> 20,
            influx_heap_start_mib: gate
                .enabled
                .load(Ordering::Relaxed)
                .then(|| gate.baseline.load(Ordering::Relaxed) >> 20),
            influx_heap_peak_mib: gate
                .enabled
                .load(Ordering::Relaxed)
                .then(|| gate.peak.load(Ordering::Relaxed) >> 20),
            influx_paused_secs: gate
                .enabled
                .load(Ordering::Relaxed)
                .then(|| gate.paused_total_ms.load(Ordering::Relaxed) / 1000),
        },
    };
    info!(
        hidden_pairs = report.hidden_pairs,
        hidden_values = report.hidden_values,
        values_written = report.values_written,
        lines_read,
        lines_per_sec = report.lines_per_sec,
        rejected_batches,
        elapsed_secs = report.elapsed_secs,
        peak_rss_mib = report.resources.peak_rss_mib,
        "recover finished"
    );

    let mut human = String::new();
    let _ = writeln!(
        human,
        "{} {} -> {}: {} windows of {} checked ({} already done), {} with values only on disk",
        if a.dry_run { "DRY RUN" } else { "Recovered" },
        report.source,
        report.target,
        report.windows_checked_now,
        human_duration(grid),
        report.windows_skipped_already_done,
        report.windows_with_hidden_values
    );
    let _ = writeln!(
        human,
        "Hidden: {} values in {} measurement/field pairs; written {}, dropped by the rules {} ({} lines read in {} s)",
        n(report.hidden_values),
        report.hidden_pairs,
        n(report.values_written),
        n(report.values_dropped),
        n(report.lines_read),
        report.elapsed_secs
    );
    for p in report.pairs.iter().take(a.top) {
        let _ = writeln!(human, "  {} {}: {}", p.measurement, p.field, n(p.values));
    }
    if report.pairs.len() > a.top {
        let _ = writeln!(
            human,
            "  ... {} more (--output json lists all)",
            report.pairs.len() - a.top
        );
    }
    let _ = writeln!(human, "State: {}", report.state_file);

    let mut o = Outcome::ok(&report, human);
    if fallbacks > 0 {
        o.warnings.push(format!("{fallbacks} windows were scanned row by row because InfluxDB failed on the pushed-down query"));
    }
    if skipped > 0 && report.windows_checked_now == 0 {
        o.warnings.push("all windows were already checked according to the state file; use --redo to check them again".into());
    } else if report.hidden_values == 0 {
        o.warnings.push(
            "no values were found that only the storage files hold; nothing to recover".into(),
        );
    }
    if a.dry_run {
        if report.hidden_values > 0 {
            o.next_steps
                .push("rerun the same command without --dry-run to write the values".into());
        }
    } else if report.values_written > 0 {
        o.next_steps.push(format!(
            "loxinflux-migration-helper verify --state <migration state file> --deep --read-via export-lp   # source and target now hold the same values ({} -> {})",
            sh(&a.source_bucket),
            sh(&a.target_bucket)
        ));
    }
    if rejected_batches > 0 {
        o.failure = Some(
            Failure::new(
                Kind::WriteRejected,
                format!(
                    "{rejected_batches} batches were rejected; first: {}",
                    first_rejection.as_deref().unwrap_or("?")
                ),
            )
            .hint("the target may hold these fields with another type; check the first rejection"),
        );
    }
    Ok(o)
}

#[allow(clippy::too_many_arguments)]
fn read_chunk(
    src: &Client,
    export: &Export,
    source: &Bucket,
    wins: &[Window],
    a: &RecoverArgs,
    parallel: usize,
    rules: &Rules,
    scope: &[(String, String)],
    grid: i64,
    pipeline: &Pipeline,
    job: usize,
    shared: &Shared,
    lines_seen: &AtomicU64,
) -> anyhow::Result<ReadDone> {
    // 1. Visible pairs per window, several queries at once.
    let next = AtomicUsize::new(0);
    let mut visible: Vec<HashSet<PairKey>> = vec![HashSet::new(); wins.len()];
    let fallbacks = AtomicUsize::new(0);
    std::thread::scope(|ts| -> anyhow::Result<()> {
        let handles: Vec<_> = (0..parallel.min(wins.len()).max(1))
            .map(|_| {
                ts.spawn(|| -> anyhow::Result<Vec<(usize, HashSet<PairKey>)>> {
                    let mut out = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= wins.len() || shared.aborted() {
                            return Ok(out);
                        }
                        let (set, fb) = visible_pairs(src, &source.name, wins[i], &a.measurements)
                            .with_context(|| format!("window {}", date(wins[i].start)))?;
                        if fb {
                            fallbacks.fetch_add(1, Ordering::Relaxed);
                        }
                        out.push((i, set));
                    }
                })
            })
            .collect();
        for h in handles {
            for (i, set) in h.join().expect("visible-pair thread panicked")? {
                visible[i] = set;
            }
        }
        Ok(())
    })?;
    if shared.aborted() {
        return Err(aborted_error());
    }

    // 2. One export call for the chunk; keep only hidden fields.
    let mut stream = export.open(&source.id, wins[0].start, wins[wins.len() - 1].stop)?;
    let mut sink = pipeline.sink(job)?;
    let mut conv = LpConverter::new(rules, scope, grid);
    let mut filter = Filter::new(wins, &visible, &a.measurements);
    let mut line = Vec::with_capacity(4096);
    let mut lines: u64 = 0;
    loop {
        line.clear();
        if stream.input.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        lines += 1;
        if lines & 0xfff == 0 {
            lines_seen.fetch_add(0x1000, Ordering::Relaxed);
            if shared.aborted() {
                return Err(aborted_error());
            }
        }
        loop {
            match filter.apply(&line) {
                Ok(None) => break,
                Ok(Some(lp)) => match conv.process(lp, sink.buf()) {
                    Ok(n) => {
                        if n > 0 {
                            sink.commit(n)?;
                        }
                        break;
                    }
                    Err(e) => {
                        return Err(
                            fail(Kind::Internal, format!("export-lp line {lines}: {e:?}")).into(),
                        );
                    }
                },
                Err(LineError::Unterminated) => {
                    if line.len() > MAX_LINE_BYTES
                        || stream.input.read_until(b'\n', &mut line)? == 0
                    {
                        return Err(fail(
                            Kind::Internal,
                            format!("export-lp line {lines}: unterminated string"),
                        )
                        .into());
                    }
                    lines += 1;
                }
                Err(LineError::Invalid(msg)) => {
                    return Err(
                        fail(Kind::Internal, format!("export-lp line {lines}: {msg}")).into(),
                    );
                }
            }
        }
    }
    lines_seen.fetch_add(lines & 0xfff, Ordering::Relaxed);
    stream.finish()?;
    let batches = sink.finish()?;
    conv.windows.flush();
    let names = std::mem::take(&mut conv.windows.names);
    let mut written: HashMap<i64, BTreeMap<String, u64>> = HashMap::new();
    for ((win, id), c) in conv.windows.counts.drain() {
        *written
            .entry(win * grid * NS)
            .or_default()
            .entry(names[id as usize].clone())
            .or_default() += c;
    }
    Ok(ReadDone {
        counters: conv.counters,
        stats: std::mem::take(&mut filter.stats),
        written,
        batches,
        fallbacks: fallbacks.load(Ordering::Relaxed),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pairs visible with every value type.
    fn set(pairs: &[(&str, &str)]) -> HashSet<PairKey> {
        let mut out = HashSet::new();
        for (m, f) in pairs {
            for ty in *b"fiusb" {
                let mut k = Vec::new();
                typed_key(&pair_key_str(m, f), ty, &mut k);
                out.insert(k);
            }
        }
        out
    }

    fn typed(m: &str, f: &str, ty: u8) -> PairKey {
        let mut k = Vec::new();
        typed_key(&pair_key_str(m, f), ty, &mut k);
        k
    }

    #[test]
    fn value_types() {
        assert_eq!(value_type(br#""19.9\xc2\xb0""#), b's');
        assert_eq!(value_type(b"16i"), b'i');
        assert_eq!(value_type(b"3u"), b'u');
        assert_eq!(value_type(b"true"), b'b');
        assert_eq!(value_type(b"F"), b'b');
        assert_eq!(value_type(b"-1.5e3"), b'f');
        assert_eq!(flux_type("double"), Some(b'f'));
        assert_eq!(flux_type("dateTime:RFC3339"), None);
    }

    #[test]
    fn values_of_another_type_are_hidden() {
        // `a` is visible as float only: its strings and ints are hidden.
        let vis = [
            [typed("m", "a", b'f')].into_iter().collect(),
            HashSet::new(),
        ];
        let mut f = Filter::new(&W, &vis, &[]);
        assert_eq!(f.apply(b"m,t=1 a=1.5 10\n").unwrap(), None);
        assert!(f.apply(br#"m,t=1 a="19.9" 11"#).unwrap().is_some());
        assert!(f.apply(b"m,t=1 a=16i 12").unwrap().is_some());
        assert_eq!(f.apply(b"m,t=1 a=2 13").unwrap(), None);
        assert_eq!(f.stats[0].hidden_values, 2);
        assert_eq!(f.stats[0].pairs[&pair_key_str("m", "a")], 2);
    }

    const W: [Window; 2] = [
        Window {
            start: 0,
            stop: 100,
        },
        Window {
            start: 100,
            stop: 200,
        },
    ];

    #[test]
    fn keys_are_unescaped_like_the_query_api_returns_them() {
        let mut k = Vec::new();
        pair_key(br"Heat\ pump\,x", br"a\=b\ c", &mut k);
        assert_eq!(k, pair_key_str("Heat pump,x", "a=b c"));
        assert_eq!(split_key(&k), ("Heat pump,x".into(), "a=b c".into()));
        // Invalid UTF-8 is replaced, as the CSV reader does.
        pair_key(b"caf\xe9", b"v", &mut k);
        assert_eq!(k, pair_key_str("caf\u{fffd}", "v"));
    }

    #[test]
    fn only_hidden_pairs_pass() {
        let vis = [set(&[("m", "a")]), set(&[("m", "a"), ("m", "b")])];
        let mut f = Filter::new(&W, &vis, &[]);
        // a visible -> skip; b hidden in window 0, visible in window 1.
        assert_eq!(f.apply(b"m,t=1 a=1i 10\n").unwrap(), None);
        assert_eq!(
            f.apply(b"m,t=1 b=2i 10\n").unwrap(),
            Some(&b"m,t=1 b=2i 10"[..])
        );
        assert_eq!(f.apply(b"m,t=1 b=2i 110\n").unwrap(), None);
        // Outside every window.
        assert_eq!(f.apply(b"m,t=1 b=2i 500\n").unwrap(), None);
        assert_eq!(f.stats[0].hidden_values, 1);
        assert_eq!(f.stats[1].hidden_values, 0);
    }

    #[test]
    fn escapes_and_quoted_spaces() {
        let vis = [set(&[("Heat pump,x", "visible field")]), HashSet::new()];
        let mut f = Filter::new(&W, &vis, &[]);
        let l: &[u8] = br#"Heat\ pump\,x,room=Bath\ Up visible\ field="a b, c=d" 20"#;
        assert_eq!(f.apply(l).unwrap(), None);
        let l: &[u8] = br#"Heat\ pump\,x,room=Bath\ Up hidden="a b \"q\" c" 21"#;
        assert_eq!(f.apply(l).unwrap(), Some(l));
        let (m, fld) = split_key(f.stats[0].pairs.keys().next().unwrap());
        assert_eq!((m.as_str(), fld.as_str()), ("Heat pump,x", "hidden"));
    }

    #[test]
    fn mixed_lines_keep_only_hidden_fields() {
        let vis = [set(&[("m", "a")]), HashSet::new()];
        let mut f = Filter::new(&W, &vis, &[]);
        assert_eq!(
            f.apply(br#"m a=1i,b="x y",c=2 30"#).unwrap(),
            Some(&br#"m b="x y",c=2 30"#[..])
        );
        assert_eq!(f.stats[0].hidden_values, 2);
    }

    #[test]
    fn measurement_filter_and_counting() {
        let vis = [HashSet::new(), HashSet::new()];
        let mut f = Filter::new(&W, &vis, &["keep me".to_owned()]);
        assert_eq!(f.apply(b"other v=1 1").unwrap(), None);
        for ts in [1, 2, 3] {
            let l = format!("keep\\ me v=1 {ts}");
            assert!(f.apply(l.as_bytes()).unwrap().is_some());
        }
        assert_eq!(f.stats[0].pairs[&pair_key_str("keep me", "v")], 3);
    }

    #[test]
    fn unterminated_strings_are_reported() {
        let vis = [HashSet::new(), HashSet::new()];
        let mut f = Filter::new(&W, &vis, &[]);
        assert_eq!(
            f.apply(b"m s=\"line one\n").unwrap_err(),
            LineError::Unterminated
        );
        assert_eq!(
            f.apply(b"m s=\"line one\nline two\" 5\n").unwrap(),
            Some(&b"m s=\"line one\nline two\" 5"[..])
        );
    }
}
