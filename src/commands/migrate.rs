//! `migrate`: copy into the target bucket, converting values on the way.

use super::{connect, date, human_duration, sh};
use crate::cli::{Global, MigrateArgs, OnReject};
use crate::convert::{Action, Rules, convert};
use crate::error::{Failure, Kind, classify, fail};
use crate::flux::{self, for_each_row, row_to_point};
use crate::influx::{Bucket, Client};
use crate::lp::{self, Point, write_point};
use crate::output::{Outcome, n};
use crate::state::{State, WindowRecord};
use crate::window::{NS, now_ns, parse_time, windows};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::io::BufRead;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tracing::{debug, error, info};

const PROGRESS_EVERY: Duration = Duration::from_secs(30);
const DEFAULT_SHARD_SECS: i64 = 7 * 86_400;

/// Buffers line protocol and writes it in batches.
struct Sink<'a> {
    client: &'a Client,
    bucket: String,
    dry_run: bool,
    batch_size: usize,
    pause: Duration,
    on_reject: OnReject,
    buf: String,
    buf_values: usize,
    requests: u64,
    rejected_batches: u64,
    first_rejection: Option<String>,
}

impl Sink<'_> {
    /// Returns true when the buffer was flushed.
    fn push(&mut self, p: &Point) -> anyhow::Result<bool> {
        write_point(&mut self.buf, p);
        self.buf_values += p.fields.len();
        if self.buf_values >= self.batch_size {
            self.flush()?;
            return Ok(true);
        }
        Ok(false)
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        if !self.dry_run {
            match self.client.write(&self.bucket, &self.buf) {
                Ok(()) => self.requests += 1,
                Err(e)
                    if classify(&e).0 == Kind::WriteRejected
                        && self.on_reject == OnReject::Skip =>
                {
                    self.rejected_batches += 1;
                    error!(error = %e, values = self.buf_values, "batch rejected, continuing (--on-reject skip)");
                    self.first_rejection.get_or_insert_with(|| e.to_string());
                }
                Err(e) => return Err(e),
            }
            if !self.pause.is_zero() {
                std::thread::sleep(self.pause);
            }
        }
        self.buf.clear();
        self.buf_values = 0;
        Ok(())
    }
}

#[derive(Default, Serialize)]
struct Totals {
    values_read: u64,
    values_written: u64,
    values_dropped: u64,
    input_types: BTreeMap<String, u64>,
    actions: BTreeMap<String, u64>,
}

struct Converter<'a> {
    rules: &'a Rules,
    scope: &'a [(String, String)],
}

impl Converter<'_> {
    /// Convert a point, update the counters, return what is to be written.
    fn apply(&self, p: Point, rec: &mut WindowRecord, t: &mut Totals) -> Option<Point> {
        let in_scope =
            self.scope.is_empty() || self.scope.iter().any(|(k, v)| p.tag(k) == Some(v.as_str()));
        let mut out = Vec::with_capacity(p.fields.len());
        for (field, value) in p.fields {
            rec.values_read += 1;
            t.values_read += 1;
            *t.input_types.entry(value.type_name().into()).or_default() += 1;
            let (action, output) = if in_scope {
                let c = convert(&field, value, self.rules);
                (c.action, c.output)
            } else {
                (Action::CopiedUnchanged, Some((field, value)))
            };
            *t.actions.entry(action.as_str().into()).or_default() += 1;
            match output {
                Some(o) => out.push(o),
                None => {
                    rec.values_dropped += 1;
                    t.values_dropped += 1;
                }
            }
        }
        if out.is_empty() {
            return None;
        }
        let written = out.len() as u64;
        rec.values_written += written;
        t.values_written += written;
        *rec.written_by_measurement
            .entry(p.measurement.clone())
            .or_default() += written;
        Some(Point { fields: out, ..p })
    }
}

#[derive(Serialize)]
struct Report {
    source: String,
    target: String,
    dry_run: bool,
    state_file: String,
    target_created: bool,
    windows_total: usize,
    windows_skipped_already_done: usize,
    windows_migrated_now: usize,
    write_requests: u64,
    rejected_batches: u64,
    elapsed_secs: u64,
    rules: Rules,
    convert_where_tag: Vec<String>,
    #[serde(flatten)]
    totals: Totals,
}

fn parse_scope(raw: &[String]) -> anyhow::Result<Vec<(String, String)>> {
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

pub fn run(g: &Global, a: &MigrateArgs) -> anyhow::Result<Outcome> {
    let rules = a.rules.rules();
    let scope = parse_scope(&a.convert_where_tag)?;
    if a.batch_size == 0 {
        return Err(fail(Kind::Usage, "--batch-size must be at least 1").into());
    }
    if a.source_bucket.as_deref() == Some(a.target_bucket.as_str()) {
        return Err(fail(Kind::Refused, "source and target bucket are the same")
            .hint("migrate into a new bucket; InfluxDB cannot change a field's type in place")
            .into());
    }
    let client = connect(g)?;
    let source_label = match (&a.source_bucket, &a.from_lp) {
        (Some(b), _) => b.clone(),
        (None, Some(p)) => format!("file:{}", p.display()),
        (None, None) => unreachable!("clap requires one of them"),
    };
    let source = match &a.source_bucket {
        Some(b) => Some(client.bucket(b)?),
        None => None,
    };

    let mut target_created = false;
    let target: Option<Bucket> = match client.find_bucket(&a.target_bucket)? {
        Some(t) => {
            if source.as_ref().is_some_and(|s| s.id == t.id) {
                return Err(fail(Kind::Refused, "source and target are the same bucket").into());
            }
            Some(t)
        }
        None if a.dry_run => None,
        None if a.create_target => {
            let (org_id, retention, shard, origin) = match &source {
                Some(s) => (
                    s.org_id.clone(),
                    s.retention_secs,
                    s.shard_group_secs,
                    s.name.clone(),
                ),
                None => (
                    client.org_id()?,
                    0,
                    DEFAULT_SHARD_SECS,
                    source_label.clone(),
                ),
            };
            let t = client.create_bucket(&a.target_bucket, &org_id, retention, shard, &origin)?;
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

    let state_path = a.state.clone().unwrap_or_else(|| {
        PathBuf::from(format!(
            "lmh-state-{}-to-{}{}.json",
            slug(&source_label),
            slug(&a.target_bucket),
            if a.dry_run { "-dry-run" } else { "" }
        ))
    });
    let mut state = State::resume(&state_path, &source_label, &a.target_bucket, a.dry_run)?;

    let mut sink = Sink {
        client: &client,
        bucket: a.target_bucket.clone(),
        dry_run: a.dry_run,
        batch_size: a.batch_size,
        pause: Duration::from_millis(a.pause_ms),
        on_reject: a.on_reject,
        buf: String::with_capacity(a.batch_size * 200),
        buf_values: 0,
        requests: 0,
        rejected_batches: 0,
        first_rejection: None,
    };
    let conv = Converter {
        rules: &rules,
        scope: &scope,
    };
    let mut totals = Totals::default();
    let began = Instant::now();
    let (windows_total, skipped, migrated);

    if let Some(src) = &source {
        let start = parse_time(a.start.as_deref().expect("clap requires --start"))?;
        let stop = parse_time(&a.stop)?;
        let ws = windows(start, stop, src.shard_group_secs);
        windows_total = ws.len();
        let already = ws
            .iter()
            .filter(|w| state.windows.contains_key(&w.start))
            .count();
        info!(
            source = %src.name, target = %a.target_bucket, dry_run = a.dry_run,
            windows = ws.len(), already_done = if a.redo { 0 } else { already },
            shard_group = %human_duration(src.shard_group_secs),
            start = %date(start), stop = %date(stop),
            state_file = %state_path.display(), "migrate started"
        );
        let mut done_now = 0;
        let mut skipped_now = 0;
        for (i, w) in ws.iter().enumerate() {
            let label = format!("{}/{}", i + 1, ws.len());
            if !a.redo && state.windows.contains_key(&w.start) {
                debug!(window = %label, start = %date(w.start), "already migrated, skipping");
                skipped_now += 1;
                continue;
            }
            let mut rec = WindowRecord {
                stop_ns: w.stop,
                ..Default::default()
            };
            let rejected_before = sink.rejected_batches;
            let t0 = Instant::now();
            let mut last_log = Instant::now();
            let resp = client.query(&flux::source(&src.name, *w, &a.measurements))?;
            for_each_row(resp, |h, r| {
                if let Some(p) = row_to_point(h, r)?
                    && let Some(out) = conv.apply(p, &mut rec, &mut totals)
                {
                    sink.push(&out)?;
                }
                if last_log.elapsed() >= PROGRESS_EVERY {
                    info!(window = %label, values_read = rec.values_read, values_written = rec.values_written, "window in progress");
                    last_log = Instant::now();
                }
                Ok(())
            })?;
            sink.flush()?;
            rec.rejected_batches = sink.rejected_batches - rejected_before;
            rec.finished_at = date(now_ns());
            let secs = t0.elapsed().as_secs_f64();
            if rec.values_read == 0 {
                debug!(window = %label, start = %date(w.start), "window empty");
            } else {
                info!(
                    window = %label, start = %date(w.start), stop = %date(w.stop),
                    values_read = rec.values_read, values_written = rec.values_written,
                    values_dropped = rec.values_dropped, rejected_batches = rec.rejected_batches,
                    secs = format!("{secs:.1}"),
                    values_per_sec = (rec.values_read as f64 / secs.max(0.001)) as u64,
                    "window migrated"
                );
            }
            state.windows.insert(w.start, rec);
            state.save(&state_path)?;
            done_now += 1;
        }
        skipped = skipped_now;
        migrated = done_now;
    } else {
        let path = a.from_lp.as_ref().expect("clap requires --from-lp");
        let shard = target
            .as_ref()
            .map_or(DEFAULT_SHARD_SECS, |t| t.shard_group_secs);
        let step = shard * NS;
        let reader: Box<dyn BufRead> = if path.as_os_str() == "-" {
            Box::new(std::io::stdin().lock())
        } else {
            Box::new(std::io::BufReader::new(std::fs::File::open(path).map_err(
                |e| {
                    fail(
                        Kind::NotFound,
                        format!("cannot open {}: {e}", path.display()),
                    )
                },
            )?))
        };
        let skip = if a.redo { 0 } else { state.file_points_done };
        if a.redo {
            state.windows.clear();
        }
        info!(source = %source_label, target = %a.target_bucket, dry_run = a.dry_run, resume_after_points = skip, state_file = %state_path.display(), "migrate started");
        let mut processed: u64 = 0;
        let mut last_log = Instant::now();
        for item in lp::Reader::new(reader) {
            let p = item.map_err(|e| {
                Failure::new(
                    Kind::Usage,
                    format!("{} line {}: {}", path.display(), e.line_no, e.message),
                )
                .hint("the file must be line protocol, e.g. from `influxd inspect export-lp`")
            })?;
            processed += 1;
            if processed <= skip {
                continue;
            }
            let key = p.time.div_euclid(step) * step;
            let rec = state.windows.entry(key).or_insert_with(|| WindowRecord {
                stop_ns: key + step,
                ..Default::default()
            });
            let out = conv.apply(p, rec, &mut totals);
            if let Some(out) = out
                && sink.push(&out)?
            {
                state.file_points_done = processed;
                state.save(&state_path)?;
            }
            if last_log.elapsed() >= PROGRESS_EVERY {
                info!(
                    points = processed,
                    values_written = totals.values_written,
                    "file in progress"
                );
                last_log = Instant::now();
            }
        }
        sink.flush()?;
        let finished = date(now_ns());
        for rec in state.windows.values_mut() {
            rec.finished_at.clone_from(&finished);
        }
        state.file_points_done = processed;
        state.save(&state_path)?;
        windows_total = state.windows.len();
        skipped = 0;
        migrated = state.windows.len();
    }

    let report = Report {
        source: source_label.clone(),
        target: a.target_bucket.clone(),
        dry_run: a.dry_run,
        state_file: state_path.display().to_string(),
        target_created,
        windows_total,
        windows_skipped_already_done: skipped,
        windows_migrated_now: migrated,
        write_requests: sink.requests,
        rejected_batches: sink.rejected_batches,
        elapsed_secs: began.elapsed().as_secs(),
        rules: rules.clone(),
        convert_where_tag: a.convert_where_tag.clone(),
        totals,
    };
    info!(
        values_read = report.totals.values_read,
        values_written = report.totals.values_written,
        values_dropped = report.totals.values_dropped,
        rejected_batches = report.rejected_batches,
        elapsed_secs = report.elapsed_secs,
        "migrate finished"
    );

    let mut human = String::new();
    let _ = writeln!(
        human,
        "{} {} -> {}: {} windows ({} migrated now, {} already done)",
        if a.dry_run { "DRY RUN" } else { "Migrated" },
        report.source,
        report.target,
        report.windows_total,
        report.windows_migrated_now,
        report.windows_skipped_already_done
    );
    let _ = writeln!(
        human,
        "Values: read {}, written {}, dropped {}; {} write requests, {} rejected",
        n(report.totals.values_read),
        n(report.totals.values_written),
        n(report.totals.values_dropped),
        report.write_requests,
        report.rejected_batches
    );
    let acts: Vec<String> = report
        .totals
        .actions
        .iter()
        .map(|(k, v)| format!("{k} {}", n(*v)))
        .collect();
    let _ = writeln!(human, "Actions: {}", acts.join(", "));
    let _ = writeln!(human, "State: {}", report.state_file);

    let mut o = Outcome::ok(&report, human);
    if skipped > 0 && report.totals.values_read == 0 {
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
    if report.rejected_batches > 0 {
        o.failure = Some(
            Failure::new(
                Kind::WriteRejected,
                format!(
                    "{} batches were rejected; first: {}",
                    report.rejected_batches,
                    sink.first_rejection.as_deref().unwrap_or("?")
                ),
            )
            .hint("run verify to see which measurements are incomplete"),
        );
    }
    Ok(o)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::convert::FieldValue;

    fn point(fields: Vec<(&str, FieldValue)>, tags: Vec<(&str, &str)>) -> Point {
        Point {
            measurement: "Temperatur".into(),
            tags: tags
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            fields: fields.into_iter().map(|(k, v)| (k.into(), v)).collect(),
            time: 1,
        }
    }

    #[test]
    fn converter_counts_and_scopes() {
        let rules = Rules::default();
        let scope = parse_scope(&["application=loxInFlux".into()]).unwrap();
        let conv = Converter {
            rules: &rules,
            scope: &scope,
        };
        let mut rec = WindowRecord::default();
        let mut t = Totals::default();
        let out = conv
            .apply(
                point(
                    vec![
                        ("AQ", FieldValue::Int(7)),
                        ("Default", FieldValue::Str("19.6°".into())),
                        ("API", FieldValue::Str("".into())),
                    ],
                    vec![("application", "loxInFlux")],
                ),
                &mut rec,
                &mut t,
            )
            .unwrap();
        assert_eq!(
            out.fields,
            vec![
                ("AQ".into(), FieldValue::Float(7.0)),
                ("Default".into(), FieldValue::Float(19.6))
            ]
        );
        // Out of scope: copied with its original type.
        let out = conv
            .apply(
                point(
                    vec![("state", FieldValue::Int(1))],
                    vec![("application", "other")],
                ),
                &mut rec,
                &mut t,
            )
            .unwrap();
        assert_eq!(out.fields, vec![("state".into(), FieldValue::Int(1))]);
        assert_eq!(
            (rec.values_read, rec.values_written, rec.values_dropped),
            (4, 3, 1)
        );
        assert_eq!(rec.written_by_measurement["Temperatur"], 3);
        assert_eq!(t.actions["copied_unchanged"], 1);
        assert_eq!(t.actions["dropped_empty"], 1);
        // Only dropped values: nothing to write.
        assert!(
            conv.apply(
                point(
                    vec![("x", FieldValue::Str("<v.1>".into()))],
                    vec![("application", "loxInFlux")]
                ),
                &mut rec,
                &mut t
            )
            .is_none()
        );
    }

    #[test]
    fn scope_syntax_is_checked() {
        assert!(parse_scope(&["a".into()]).is_err());
        assert!(parse_scope(&["=b".into()]).is_err());
        assert_eq!(
            parse_scope(&["a=b=c".into()]).unwrap(),
            vec![("a".into(), "b=c".into())]
        );
    }
}
