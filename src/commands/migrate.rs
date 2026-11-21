//! `migrate`: copy into the target bucket, converting values on the way.
//!
//! Readers (one per concurrent window, or one for a line protocol stream)
//! convert into pooled buffers; writer threads send them. A window is
//! recorded in the state file only after every one of its batches was
//! acknowledged by InfluxDB, so an interrupted run resumes exactly.

use super::{Resources, connect, connect_target, date, human_duration, sh};
use crate::cli::{Global, MigrateArgs, OnReject};
use crate::convert::Rules;
use crate::error::{Failure, Kind, fail};
use crate::fast::Counters;
use crate::fastcsv::CsvConverter;
use crate::fastlp::{LineError, LpConverter, MAX_LINE_BYTES};
use crate::flux;
use crate::guard::{InfluxGate, MemWatch, RateLimit, Shared, aborted_error};
use crate::influx::{Bucket, Client};
use crate::output::{Outcome, n};
use crate::pipeline::{Config, Event, LineSink, Pipeline};
use crate::state::{self, Header, WindowRecord};
use crate::window::{NS, Window, grid_secs, now_ns, parse_duration_secs, parse_time, windows};
use crossbeam_channel::{bounded, select, unbounded};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::io::BufRead;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::info;

const PROGRESS_EVERY: Duration = Duration::from_secs(30);
const DEFAULT_SHARD_SECS: i64 = 7 * 86_400;

#[derive(Serialize)]
struct Report {
    source: String,
    target: String,
    dry_run: bool,
    state_file: String,
    target_created: bool,
    window_secs: i64,
    windows_total: usize,
    windows_skipped_already_done: usize,
    windows_migrated_now: usize,
    write_requests: u64,
    rejected_batches: u64,
    elapsed_secs: u64,
    values_per_sec: u64,
    values_read: u64,
    values_written: u64,
    values_dropped: u64,
    input_types: BTreeMap<String, u64>,
    actions: BTreeMap<String, u64>,
    rules: Rules,
    convert_where_tag: Vec<String>,
    resources: Resources,
}

pub(crate) fn parse_scope(raw: &[String]) -> anyhow::Result<Vec<(String, String)>> {
    raw.iter()
        .map(|s| {
            s.split_once('=')
                .filter(|(k, v)| !k.is_empty() && !v.is_empty())
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .ok_or_else(|| {
                    fail(
                        Kind::Usage,
                        format!("--convert-where-tag expects KEY=VALUE, got `{s}`"),
                    )
                    .into()
                })
        })
        .collect()
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

/// What a reader hands back when its job is fully converted.
struct ReadDone {
    counters: Counters,
    /// (window start ns, measurement) -> written values
    written: Vec<(i64, String, u64)>,
    batches: u64,
}

#[derive(Default)]
struct JobProgress {
    read: Option<ReadDone>,
    acked: u64,
    rejected: u64,
}

pub fn run(g: &Global, a: &MigrateArgs) -> anyhow::Result<Outcome> {
    if a.batch_size == 0 || a.batch_mb == 0 || a.writers == 0 || g.parallel == 0 {
        return Err(fail(
            Kind::Usage,
            "--batch-size, --batch-mb, --writers and --parallel must be at least 1",
        )
        .into());
    }
    if a.source_bucket.as_deref() == Some(a.target_bucket.as_str()) && g.target_url.is_none() {
        return Err(fail(Kind::Refused, "source and target bucket are the same")
            .hint("migrate into a new bucket; InfluxDB cannot change a field's type in place")
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
    a: &MigrateArgs,
    shared: &Arc<Shared>,
    mem: &MemWatch,
) -> anyhow::Result<Outcome> {
    let rules = a.rules.rules();
    let scope = parse_scope(&a.convert_where_tag)?;
    let requested_window = a.window.as_deref().map(parse_duration_secs).transpose()?;
    // A dry run over a line protocol file needs no server at all.
    let offline = a.dry_run && a.from_lp.is_some();
    let (src, tgt) = if offline {
        info!("offline dry run: no InfluxDB connection");
        let c = Arc::new(Client::new(&g.url, "offline", "offline", g.timeout_secs)?);
        (c.clone(), c)
    } else {
        (Arc::new(connect(g)?), Arc::new(connect_target(g)?))
    };
    let separate_target = g.target_url.is_some() || g.target_org.is_some();

    let source_label = match (&a.source_bucket, &a.from_lp) {
        (Some(b), _) => b.clone(),
        (None, Some(p)) => format!("file:{}", p.display()),
        (None, None) => unreachable!("clap requires one of them"),
    };
    let source: Option<Bucket> = a
        .source_bucket
        .as_deref()
        .map(|b| src.bucket(b))
        .transpose()?;

    let mut target_created = false;
    let found = if offline {
        None
    } else {
        tgt.find_bucket(&a.target_bucket)?
    };
    let target: Option<Bucket> = match found {
        Some(t) => {
            if !separate_target && source.as_ref().is_some_and(|s| s.id == t.id) {
                return Err(fail(Kind::Refused, "source and target are the same bucket").into());
            }
            Some(t)
        }
        None if a.dry_run => None,
        None if a.create_target => {
            let (retention, shard, origin) = match &source {
                Some(s) => (s.retention_secs, s.shard_group_secs, s.name.clone()),
                None => (0, DEFAULT_SHARD_SECS, source_label.clone()),
            };
            let org_id = match (&source, separate_target) {
                (Some(s), false) => s.org_id.clone(),
                _ => tgt.org_id()?,
            };
            let t = tgt.create_bucket(&a.target_bucket, &org_id, retention, shard, &origin)?;
            info!(bucket = %t.name, id = %t.id, retention = %human_duration(t.retention_secs), shard_group = %human_duration(t.shard_group_secs), "target bucket created");
            target_created = true;
            Some(t)
        }
        None => {
            return Err(fail(Kind::NotFound, format!("target bucket `{}` does not exist", a.target_bucket))
                .hint("add --create-target to create it with the source bucket's retention and shard duration")
                .into());
        }
    };

    let shard_for_grid = source
        .as_ref()
        .or(target.as_ref())
        .map_or(DEFAULT_SHARD_SECS, |b| b.shard_group_secs);
    let grid = grid_secs(shard_for_grid, requested_window)?;
    let state_path = a.state.clone().unwrap_or_else(|| {
        PathBuf::from(format!(
            "lmh-state-{}-to-{}{}.jsonl",
            slug(&source_label),
            slug(&a.target_bucket),
            if a.dry_run { "-dry-run" } else { "" }
        ))
    });
    let header = Header {
        kind: "header".into(),
        version: state::VERSION,
        source: source_label.clone(),
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
    let readers = if source.is_some() { g.parallel } else { 1 };
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

    // Jobs: windows (API) or the single stream (file).
    let jobs: Vec<Window> = match &source {
        Some(s) => {
            let start = parse_time(a.start.as_deref().expect("clap requires --start"))?;
            let stop = parse_time(&a.stop)?;
            let all = windows(start, stop, grid);
            info!(
                source = %s.name, target = %a.target_bucket, dry_run = a.dry_run,
                windows = all.len(), already_done = all.iter().filter(|w| finished.contains(&w.start)).count(),
                window = %human_duration(grid), parallel = readers, writers = a.writers,
                start = %date(start), stop = %date(stop), state_file = %state_path.display(),
                "migrate started"
            );
            all
        }
        None => {
            info!(source = %source_label, target = %a.target_bucket, dry_run = a.dry_run, writers = a.writers, state_file = %state_path.display(), "migrate started");
            vec![Window { start: 0, stop: 0 }]
        }
    };
    let todo: Vec<usize> = (0..jobs.len())
        .filter(|&i| source.is_none() || !finished.contains(&jobs[i].start))
        .collect();
    let skipped = jobs.len() - todo.len();

    let began = Instant::now();
    let lines_seen = AtomicU64::new(0);
    let mut totals = Counters::default();
    let mut write_requests = 0u64;
    let mut rejected_batches = 0u64;
    let mut first_rejection: Option<String> = None;
    let mut done_now = 0usize;
    let mut acked_values = 0u64;

    let (job_tx, job_rx) = bounded::<usize>(todo.len().max(1));
    for &i in &todo {
        let _ = job_tx.send(i);
    }
    drop(job_tx);
    let (res_tx, res_rx) = unbounded::<(usize, ReadDone)>();

    std::thread::scope(|ts| -> anyhow::Result<()> {
        for r in 0..readers.min(todo.len()) {
            let (job_rx, res_tx, pipeline, src, shared, rules, scope_tags, lines_seen) = (
                job_rx.clone(),
                res_tx.clone(),
                &pipeline,
                src.clone(),
                shared.clone(),
                &rules,
                &scope,
                &lines_seen,
            );
            let (source, measurements, from_lp, jobs) =
                (&source, &a.measurements, &a.from_lp, &jobs);
            std::thread::Builder::new()
                .name(format!("reader-{r}"))
                .spawn_scoped(ts, move || {
                    for job in job_rx.iter() {
                        if shared.aborted() {
                            break;
                        }
                        let res = match source {
                            Some(b) => read_window(
                                &src,
                                &b.name,
                                jobs[job],
                                measurements,
                                rules,
                                scope_tags,
                                pipeline,
                                job,
                                &shared,
                            ),
                            None => read_lp(
                                from_lp.as_ref().expect("file mode"),
                                rules,
                                scope_tags,
                                grid,
                                pipeline,
                                &shared,
                                lines_seen,
                            ),
                        };
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
        let mut last_values = 0u64;
        while done_now < todo.len() && !shared.aborted() {
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
                    finish_job(
                        &mut state_writer,
                        &source,
                        jobs[job],
                        grid,
                        done,
                        p.rejected,
                    )?;
                    done_now += 1;
                }
            }
            if last_log.elapsed() >= PROGRESS_EVERY {
                let secs = last_log.elapsed().as_secs_f64();
                let rate = ((acked_values - last_values) as f64 / secs) as u64;
                let remaining = todo.len() - done_now;
                let eta = if done_now > 0 && source.is_some() {
                    format!(
                        "{}s",
                        (began.elapsed().as_secs_f64() / done_now as f64 * remaining as f64) as u64
                    )
                } else {
                    "?".into()
                };
                info!(
                    windows_done = format!("{done_now}/{}", todo.len()),
                    values_acked = acked_values,
                    lines_read = lines_seen.load(Ordering::Relaxed),
                    values_per_sec = rate,
                    eta = %eta,
                    rss_mib = crate::guard::rss_bytes().map(|b| b >> 20),
                    influx_heap_peak_mib = gate.enabled.load(Ordering::Relaxed).then(|| gate.peak.load(Ordering::Relaxed) >> 20),
                    "progress"
                );
                last_log = Instant::now();
                last_values = acked_values;
            }
        }
        Ok(())
    })?;
    pipeline.close();
    gate.stop();

    if shared.aborted() {
        let e = shared.take_reason().unwrap_or_else(aborted_error);
        info!(windows_finished = done_now, state_file = %state_path.display(), "migrate stopped; finished windows are recorded, rerun to resume");
        return Err(e);
    }

    let elapsed = began.elapsed();
    let resources = Resources {
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
    };
    let report = Report {
        source: source_label,
        target: a.target_bucket.clone(),
        dry_run: a.dry_run,
        state_file: state_path.display().to_string(),
        target_created,
        window_secs: grid,
        windows_total: if source.is_some() { jobs.len() } else { 1 },
        windows_skipped_already_done: if source.is_some() { skipped } else { 0 },
        windows_migrated_now: done_now,
        write_requests,
        rejected_batches,
        elapsed_secs: elapsed.as_secs(),
        values_per_sec: (totals.values_read as f64 / elapsed.as_secs_f64().max(0.001)) as u64,
        values_read: totals.values_read,
        values_written: totals.values_written,
        values_dropped: totals.values_dropped,
        input_types: totals.types_map(),
        actions: totals.actions_map(),
        rules: rules.clone(),
        convert_where_tag: a.convert_where_tag.clone(),
        resources,
    };
    info!(
        values_read = report.values_read,
        values_written = report.values_written,
        values_dropped = report.values_dropped,
        values_per_sec = report.values_per_sec,
        rejected_batches,
        elapsed_secs = report.elapsed_secs,
        peak_rss_mib = report.resources.peak_rss_mib,
        "migrate finished"
    );

    let mut human = String::new();
    let _ = writeln!(
        human,
        "{} {} -> {}: {} windows of {} ({} migrated now, {} already done)",
        if a.dry_run { "DRY RUN" } else { "Migrated" },
        report.source,
        report.target,
        report.windows_total,
        human_duration(grid),
        report.windows_migrated_now,
        report.windows_skipped_already_done
    );
    let _ = writeln!(
        human,
        "Values: read {}, written {}, dropped {} in {} s ({} values/s); {} write requests, {} rejected",
        n(report.values_read),
        n(report.values_written),
        n(report.values_dropped),
        report.elapsed_secs,
        n(report.values_per_sec),
        report.write_requests,
        report.rejected_batches
    );
    let acts: Vec<String> = report
        .actions
        .iter()
        .map(|(k, v)| format!("{k} {}", n(*v)))
        .collect();
    let _ = writeln!(human, "Actions: {}", acts.join(", "));
    if let Some(p) = report.resources.peak_rss_mib {
        let _ = writeln!(
            human,
            "Peak memory: {p} MiB (limit {} MiB)",
            report.resources.memory_limit_mib
        );
    }
    let _ = writeln!(human, "State: {}", report.state_file);

    let mut o = Outcome::ok(&report, human);
    if skipped > 0 && report.values_read == 0 {
        o.warnings.push("all windows were already migrated according to the state file; use --redo to migrate them again".into());
    }
    if a.dry_run {
        o.next_steps.push("rerun the same command without --dry-run (add --create-target if the target does not exist)".into());
    } else {
        o.next_steps.push(format!(
            "loxinflux-migrationshelper verify --state {}",
            sh(&report.state_file)
        ));
        if let Some(src) = &a.source_bucket {
            o.next_steps.push(format!(
                "loxinflux-migrationshelper cutover --source-bucket {} --target-bucket {}",
                sh(src),
                sh(&a.target_bucket)
            ));
        }
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
            .hint("run verify to see which measurements are incomplete"),
        );
    }
    Ok(o)
}

#[allow(clippy::too_many_arguments)]
fn read_window(
    src: &Client,
    bucket: &str,
    w: Window,
    measurements: &[String],
    rules: &Rules,
    scope: &[(String, String)],
    pipeline: &Pipeline,
    job: usize,
    shared: &Shared,
) -> anyhow::Result<ReadDone> {
    let q = format!(
        "{} |> drop(columns: [\"_start\", \"_stop\"])",
        flux::source(bucket, w, measurements)
    );
    let resp = src.query(&q)?;
    let mut sink = pipeline.sink(job)?;
    let mut conv = CsvConverter::new(rules, scope);
    conv.run(resp, &mut sink, shared.flag())?;
    if shared.aborted() {
        return Err(aborted_error());
    }
    let batches = sink.finish()?;
    let written = conv
        .runs
        .take()
        .into_iter()
        .map(|(m, c)| (w.start, m, c))
        .collect();
    Ok(ReadDone {
        counters: conv.counters,
        written,
        batches,
    })
}

fn read_lp(
    path: &PathBuf,
    rules: &Rules,
    scope: &[(String, String)],
    grid: i64,
    pipeline: &Pipeline,
    shared: &Shared,
    lines_seen: &AtomicU64,
) -> anyhow::Result<ReadDone> {
    let input: Box<dyn BufRead> = if path.as_os_str() == "-" {
        Box::new(std::io::BufReader::with_capacity(1 << 20, std::io::stdin()))
    } else {
        Box::new(std::io::BufReader::with_capacity(
            1 << 20,
            std::fs::File::open(path).map_err(|e| {
                fail(
                    Kind::NotFound,
                    format!("cannot open {}: {e}", path.display()),
                )
            })?,
        ))
    };
    let mut input = input;
    let mut sink = pipeline.sink(0)?;
    let mut conv = LpConverter::new(rules, scope, grid);
    let mut line = Vec::with_capacity(4096);
    let mut line_no: u64 = 0;
    loop {
        line.clear();
        let start_no = line_no + 1;
        if input.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        line_no += 1;
        loop {
            match conv.process(&line, sink.buf()) {
                Ok(n) => {
                    if n > 0 {
                        sink.commit(n)?;
                    }
                    break;
                }
                Err(LineError::Unterminated) => {
                    if line.len() > MAX_LINE_BYTES || input.read_until(b'\n', &mut line)? == 0 {
                        return Err(fail(
                            Kind::Usage,
                            format!(
                                "line {start_no}: unterminated string (or a line over {} MiB)",
                                MAX_LINE_BYTES >> 20
                            ),
                        )
                        .into());
                    }
                    line_no += 1;
                }
                Err(LineError::Invalid(msg)) => {
                    return Err(Failure::new(Kind::Usage, format!("{} line {start_no}: {msg}", path.display()))
                        .hint("the input must be line protocol with timestamps, e.g. from `influxd inspect export-lp`")
                        .into());
                }
            }
        }
        if line_no & 0xfff == 0 {
            lines_seen.store(line_no, Ordering::Relaxed);
            if shared.aborted() {
                return Err(aborted_error());
            }
        }
    }
    lines_seen.store(line_no, Ordering::Relaxed);
    let batches = sink.finish()?;
    conv.windows.flush();
    let names = std::mem::take(&mut conv.windows.names);
    let written = conv
        .windows
        .counts
        .drain()
        .map(|((win, id), c)| (win * grid * NS, names[id as usize].clone(), c))
        .collect();
    Ok(ReadDone {
        counters: conv.counters,
        written,
        batches,
    })
}

/// Record a completed job in the state file.
fn finish_job(
    w: &mut state::Writer,
    source: &Option<Bucket>,
    window: Window,
    grid: i64,
    done: ReadDone,
    rejected: u64,
) -> anyhow::Result<()> {
    let finished_at = date(now_ns());
    if source.is_some() {
        let rec = WindowRecord {
            kind: "window".into(),
            start_ns: window.start,
            stop_ns: window.stop,
            values_read: done.counters.values_read,
            values_written: done.counters.values_written,
            values_dropped: done.counters.values_dropped,
            rejected_batches: rejected,
            written_by_measurement: done.written.into_iter().map(|(_, m, c)| (m, c)).collect(),
            finished_at,
        };
        if rec.values_read > 0 {
            info!(
                start = %date(rec.start_ns), values_read = rec.values_read, values_written = rec.values_written,
                values_dropped = rec.values_dropped, "window migrated"
            );
        }
        return w.append(&rec);
    }
    // File mode: one record per grid window that received data.
    let mut by_window: BTreeMap<i64, BTreeMap<String, u64>> = BTreeMap::new();
    for (start, m, c) in done.written {
        *by_window.entry(start).or_default().entry(m).or_default() += c;
    }
    for (start, by_m) in by_window {
        let written: u64 = by_m.values().sum();
        w.append(&WindowRecord {
            kind: "window".into(),
            start_ns: start,
            stop_ns: start + grid * NS,
            values_read: 0,
            values_written: written,
            values_dropped: 0,
            rejected_batches: rejected,
            written_by_measurement: by_m,
            finished_at: finished_at.clone(),
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_syntax_is_checked() {
        assert!(parse_scope(&["a".into()]).is_err());
        assert!(parse_scope(&["=b".into()]).is_err());
        assert_eq!(
            parse_scope(&["a=b=c".into()]).unwrap(),
            vec![("a".into(), "b=c".into())]
        );
    }

    #[test]
    fn slugs_are_file_safe() {
        assert_eq!(slug("file:/work/src.lp"), "file__work_src_lp");
    }
}
