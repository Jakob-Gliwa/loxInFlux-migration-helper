//! `verify`: compare the counts recorded by `migrate` with the target bucket.
//!
//! The state file is streamed; windows are counted in parallel (`--parallel`)
//! with one `count()` query each, which InfluxDB answers from its index.

use super::{Resources, connect, connect_target, date, sh};
use crate::cli::{Global, VerifyArgs};
use crate::convert::Rules;
use crate::deep::{DeepResult, deep_window};
use crate::error::{Failure, Kind, fail};
use crate::flux::{for_each_row, lit};
use crate::guard::{MemWatch, Shared};
use crate::influx::Client;
use crate::output::{Outcome, n};
use crate::state::{self, WindowRecord};
use crossbeam_channel::{bounded, unbounded};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;
use tracing::{debug, info, warn};

#[derive(Serialize)]
struct WindowResult {
    start: String,
    stop: String,
    expected: u64,
    actual: u64,
    missing: u64,
    extra: u64,
    missing_by_measurement: BTreeMap<String, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deep: Option<DeepResult>,
}

#[derive(Serialize, Default)]
struct DeepTotals {
    windows_compared: usize,
    windows_with_differences: usize,
    series_compared: usize,
    series_missing: usize,
    series_different: usize,
    series_extra: usize,
    source_values: u64,
    target_values: u64,
    examples: Vec<String>,
}

#[derive(Serialize)]
struct Report {
    target: String,
    source: String,
    state_file: String,
    windows_checked: usize,
    windows_ok: usize,
    windows_with_missing: usize,
    windows_with_extra: usize,
    expected_total: u64,
    actual_total: u64,
    missing_total: u64,
    extra_total: u64,
    missing_by_measurement: BTreeMap<String, u64>,
    /// Only windows that are not exactly as expected (sorted by start).
    windows: Vec<WindowResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deep: Option<DeepTotals>,
    resources: Resources,
}

struct DeepCtx {
    src: Client,
    src_bucket: String,
    rules: Rules,
    scope: Vec<(String, String)>,
    every: usize,
}

fn check(client: &Client, bucket: &str, rec: &WindowRecord) -> anyhow::Result<WindowResult> {
    let q = format!(
        "from(bucket: {}) |> range(start: {}, stop: {}) |> count() |> group(columns: [\"_measurement\"]) |> sum()",
        lit(bucket),
        date(rec.start_ns),
        date(rec.stop_ns)
    );
    let mut actual: BTreeMap<String, u64> = BTreeMap::new();
    for_each_row(client.query(&q)?, |h, r| {
        let get = |c: &str| h.index(c).and_then(|i| r.get(i)).unwrap_or("");
        *actual.entry(get("_measurement").to_owned()).or_default() +=
            get("_value").parse::<u64>().unwrap_or(0);
        Ok(())
    })?;
    let mut w = WindowResult {
        start: date(rec.start_ns),
        stop: date(rec.stop_ns),
        expected: rec.values_written,
        actual: actual.values().sum(),
        missing: 0,
        extra: 0,
        missing_by_measurement: BTreeMap::new(),
        deep: None,
    };
    for (m, exp) in &rec.written_by_measurement {
        let got = actual.get(m).copied().unwrap_or(0);
        if got < *exp {
            w.missing += exp - got;
            w.missing_by_measurement.insert(m.clone(), exp - got);
        }
    }
    for (m, got) in &actual {
        let exp = rec.written_by_measurement.get(m).copied().unwrap_or(0);
        if *got > exp {
            w.extra += got - exp;
        }
    }
    Ok(w)
}

pub fn run(g: &Global, a: &VerifyArgs) -> anyhow::Result<Outcome> {
    let header = state::read_header(&a.state)?.ok_or_else(|| {
        fail(
            Kind::NotFound,
            format!("state file {} not found", a.state.display()),
        )
        .hint("pass the --state file that migrate printed")
    })?;
    if header.dry_run {
        return Err(fail(
            Kind::Refused,
            "this state file is from a dry run; nothing was written",
        )
        .hint("run migrate without --dry-run first")
        .into());
    }
    let shared = Shared::new_registered();
    let mem = MemWatch::start(g.memory_limit_mb, shared.clone());
    let client = Arc::new(connect_target(g)?);
    let target = client.bucket(&header.target)?;
    let deep = if a.deep {
        if header.source.starts_with("file:") {
            return Err(fail(
                Kind::Usage,
                "--deep needs the source bucket; this migration came from a line protocol file",
            )
            .into());
        }
        let rules = header.rules.clone().unwrap_or_default();
        let scope = crate::commands::migrate::parse_scope(&header.convert_where_tag)?;
        Some(DeepCtx {
            src: connect(g)?,
            src_bucket: header.source.clone(),
            rules,
            scope,
            every: a.sample_every.max(1),
        })
    } else {
        None
    };
    info!(target = %target.name, state_file = %a.state.display(), parallel = g.parallel, deep = a.deep, "verify started");

    let (job_tx, job_rx) = bounded::<(usize, WindowRecord)>(g.parallel.max(1) * 2);
    let (res_tx, res_rx) = unbounded::<anyhow::Result<(WindowRecord, WindowResult)>>();
    let mut out = Report {
        target: target.name.clone(),
        source: header.source.clone(),
        state_file: a.state.display().to_string(),
        windows_checked: 0,
        windows_ok: 0,
        windows_with_missing: 0,
        windows_with_extra: 0,
        expected_total: 0,
        actual_total: 0,
        missing_total: 0,
        extra_total: 0,
        missing_by_measurement: BTreeMap::new(),
        windows: Vec::new(),
        deep: a.deep.then(DeepTotals::default),
        resources: Resources::default(),
    };

    let scoped = std::thread::scope(|s| -> anyhow::Result<()> {
        for _ in 0..g.parallel.max(1) {
            let (job_rx, res_tx, client, bucket, shared, deep) = (
                job_rx.clone(),
                res_tx.clone(),
                client.clone(),
                &target.name,
                &shared,
                &deep,
            );
            s.spawn(move || {
                for (idx, rec) in job_rx.iter() {
                    if shared.aborted() {
                        break;
                    }
                    let r = check(&client, bucket, &rec).and_then(|mut w| {
                        if let Some(d) = deep
                            && idx % d.every == 0
                        {
                            let win = crate::window::Window {
                                start: rec.start_ns,
                                stop: rec.stop_ns,
                            };
                            w.deep = Some(deep_window(
                                &d.src,
                                &d.src_bucket,
                                &client,
                                bucket,
                                win,
                                &d.rules,
                                &d.scope,
                                shared.flag(),
                            )?);
                        }
                        Ok(w)
                    });
                    let r = r.map(|w| (rec, w));
                    let failed = r.is_err();
                    let _ = res_tx.send(r);
                    if failed {
                        break;
                    }
                }
            });
        }
        drop(res_tx);
        drop(job_rx);
        let state_path = a.state.clone();
        let feeder = s.spawn(move || {
            let mut idx = 0usize;
            let r = state::for_each_window(&state_path, |rec| {
                idx += 1;
                job_tx
                    .send((idx - 1, rec))
                    .map_err(|_| anyhow::anyhow!("verify workers stopped"))
            });
            drop(job_tx);
            r
        });
        for r in res_rx.iter() {
            let (_rec, w) = match r {
                Ok(x) => x,
                Err(e) => {
                    shared.fail(e);
                    break;
                }
            };
            out.windows_checked += 1;
            out.expected_total += w.expected;
            out.actual_total += w.actual;
            out.missing_total += w.missing;
            out.extra_total += w.extra;
            for (m, c) in &w.missing_by_measurement {
                *out.missing_by_measurement.entry(m.clone()).or_default() += c;
            }
            if w.missing > 0 {
                out.windows_with_missing += 1;
                warn!(start = %w.start, expected = w.expected, actual = w.actual, missing = w.missing, "window incomplete");
            } else {
                debug!(start = %w.start, expected = w.expected, actual = w.actual, extra = w.extra, "window ok");
            }
            if w.extra > 0 {
                out.windows_with_extra += 1;
            }
            let mut deep_bad = false;
            if let (Some(d), Some(t)) = (&w.deep, out.deep.as_mut()) {
                t.windows_compared += 1;
                t.series_compared += d.series_compared;
                t.series_missing += d.series_missing;
                t.series_different += d.series_different;
                t.series_extra += d.series_extra;
                t.source_values += d.source_values;
                t.target_values += d.target_values;
                deep_bad = d.series_missing + d.series_different > 0;
                if deep_bad {
                    t.windows_with_differences += 1;
                    warn!(start = %w.start, missing = d.series_missing, different = d.series_different, "deep compare: values differ");
                    for e in &d.examples {
                        if t.examples.len() < 20 {
                            t.examples.push(format!("{}: {e}", w.start));
                        }
                    }
                }
            }
            if w.missing == 0 && w.extra == 0 && !deep_bad {
                out.windows_ok += 1;
            } else {
                out.windows.push(w);
            }
            if out.windows_checked.is_multiple_of(50) {
                info!(
                    windows_checked = out.windows_checked,
                    missing = out.missing_total,
                    "verify progress"
                );
            }
        }
        match feeder.join() {
            Ok(r) => r.map(|_| ()),
            Err(_) => Err(anyhow::anyhow!("state reader panicked")),
        }
    });
    mem.stop();
    if shared.aborted() {
        return Err(shared
            .take_reason()
            .unwrap_or_else(crate::guard::aborted_error));
    }
    scoped?;
    out.windows.sort_by(|a, b| a.start.cmp(&b.start));
    out.resources = Resources {
        peak_rss_mib: mem.peak_mib(),
        memory_limit_mib: mem.limit_bytes >> 20,
        ..Default::default()
    };
    info!(
        windows = out.windows_checked,
        missing = out.missing_total,
        extra = out.extra_total,
        "verify finished"
    );

    let mut human = String::new();
    let _ = writeln!(
        human,
        "Verified {} windows of {} (from {}): {} ok, {} incomplete, {} with extra values",
        out.windows_checked,
        out.target,
        out.source,
        out.windows_ok,
        out.windows_with_missing,
        out.windows_with_extra
    );
    let _ = writeln!(
        human,
        "Values: expected {}, found {}, missing {}, extra {}",
        n(out.expected_total),
        n(out.actual_total),
        n(out.missing_total),
        n(out.extra_total)
    );
    let mut worst: Vec<(&String, &u64)> = out.missing_by_measurement.iter().collect();
    worst.sort_by(|a, b| b.1.cmp(a.1));
    for (m, c) in worst.iter().take(a.top) {
        let _ = writeln!(human, "  missing in {m}: {}", n(**c));
    }

    if let Some(d) = &out.deep {
        let _ = writeln!(
            human,
            "Deep: {} windows, {} series compared ({} source values, {} target values): {} missing, {} different, {} extra",
            d.windows_compared,
            n(d.series_compared as u64),
            n(d.source_values),
            n(d.target_values),
            d.series_missing,
            d.series_different,
            d.series_extra
        );
        for e in d.examples.iter().take(a.top) {
            let _ = writeln!(human, "  {e}");
        }
    }
    let deep_bad = out
        .deep
        .as_ref()
        .is_some_and(|d| d.series_missing + d.series_different > 0);
    let mut o = Outcome::ok(&out, human);
    if out.extra_total > 0 {
        o.warnings.push("the target holds more values than were migrated; expected if live data is also written to it (dual write), otherwise check for other writers".into());
    }
    if out.missing_total > 0 || deep_bad {
        let first = out
            .windows
            .iter()
            .find(|w| {
                w.missing > 0
                    || w.deep
                        .as_ref()
                        .is_some_and(|d| d.series_missing + d.series_different > 0)
            })
            .expect("a window differs");
        if !header.source.starts_with("file:") {
            o.next_steps.push(format!(
                "loxinflux-migrationshelper migrate --source-bucket {} --target-bucket {} --start {} --stop {} --redo --state {}",
                sh(&header.source),
                sh(&header.target),
                first.start,
                first.stop,
                sh(&out.state_file)
            ));
        }
        o.failure = Some(
            Failure::new(
                Kind::VerifyMismatch,
                if out.missing_total > 0 {
                    format!("{} values are missing in {} windows", out.missing_total, out.windows_with_missing)
                } else {
                    "deep comparison found series whose values differ between source and target".to_owned()
                },
            )
            .hint("re-migrate the affected windows with --redo and a --start/--stop covering them, then verify again"),
        );
    } else {
        o.next_steps.push(format!(
            "loxinflux-migrationshelper cutover --source-bucket {} --target-bucket {}",
            sh(header.source.trim_start_matches("file:")),
            sh(&header.target)
        ));
    }
    Ok(o)
}
