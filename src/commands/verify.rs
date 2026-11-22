//! `verify`: compare the counts recorded by `migrate` with the target bucket.
//!
//! The state file is streamed; windows are counted in parallel (`--parallel`)
//! with one `count()` query each, which InfluxDB answers from its index.
//! With `--deep` the value comparison also yields the counts, so deep windows
//! skip the count query. `--read-via export-lp` reads both buckets from the
//! storage files in chunks of consecutive windows (see `export.rs`).

use super::{Resources, connect, connect_target, date, sh};
use crate::cli::{Global, ReadVia, VerifyArgs};
use crate::convert::Rules;
use crate::deep::{DeepResult, HashSink, deep_window, finish, scan};
use crate::error::{Failure, Kind, fail};
use crate::export::Export;
use crate::flux::{for_each_row, lit};
use crate::guard::{MemWatch, Shared};
use crate::influx::Client;
use crate::output::{Outcome, n};
use crate::state::{self, WindowRecord};
use anyhow::Context as _;
use crossbeam_channel::{bounded, unbounded};
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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
    /// export-lp saw a difference; this result comes from a Flux re-check.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    rechecked_via_flux: bool,
    /// Source values export-lp found on disk that Flux does not return.
    #[serde(skip_serializing_if = "is_zero")]
    source_values_only_on_disk: u64,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

#[derive(Serialize, Default)]
struct DeepTotals {
    read_via: &'static str,
    windows_compared: usize,
    /// export-lp only: windows that differed there and were re-checked through Flux.
    windows_rechecked_via_flux: usize,
    /// export-lp only: source values stored on disk that no query returns
    /// (so they were not migrated either). Counted in re-checked windows.
    source_values_only_on_disk: u64,
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
    export: Option<ExportCtx>,
}

struct ExportCtx {
    export: Export,
    src_id: String,
    tgt_id: String,
    /// Chunk length; chunks are aligned like shard groups (multiples of
    /// the length since 0001-01-01), so one chunk decodes one shard group.
    chunk_secs: i64,
    /// The warning about values only on disk was logged.
    warned_hidden: AtomicBool,
}

enum Job {
    /// Count check, or a deep comparison through Flux.
    One(usize, WindowRecord),
    /// Consecutive windows compared with one export-lp call per bucket.
    Chunk(Vec<WindowRecord>),
}

/// Seconds from 0001-01-01 (InfluxDB's shard group origin) to 1970-01-01.
const GO_ZERO_SECS: i64 = 62_135_596_800;

fn chunk_group(start_ns: i64, chunk_secs: i64) -> i64 {
    (start_ns.div_euclid(1_000_000_000) + GO_ZERO_SECS).div_euclid(chunk_secs)
}

fn window_of(rec: &WindowRecord) -> crate::window::Window {
    crate::window::Window {
        start: rec.start_ns,
        stop: rec.stop_ns,
    }
}

/// Deep comparison of one window through Flux; also yields the counts.
fn flux_deep(
    d: &DeepCtx,
    client: &Client,
    bucket: &str,
    rec: &WindowRecord,
    abort: &AtomicBool,
) -> anyhow::Result<WindowResult> {
    let dw = deep_window(
        &d.src,
        &d.src_bucket,
        client,
        bucket,
        window_of(rec),
        &d.rules,
        &d.scope,
        abort,
    )?;
    let mut w = count_result(rec, &dw.target_by_measurement);
    w.deep = Some(dw.result);
    Ok(w)
}

fn differs(w: &WindowResult) -> bool {
    w.missing > 0
        || w.deep
            .as_ref()
            .is_some_and(|d| d.series_missing + d.series_different > 0)
}

/// Deep comparison of consecutive windows via export-lp: one export call per
/// bucket for the whole chunk, then the windows are compared in parallel.
///
/// A window that differs is re-checked through Flux: first only the source
/// (export-lp can show values no query returns, e.g. a field missing from the
/// shard's field index), then, if it still differs, both sides. Only a Flux
/// result is ever reported as a mismatch.
fn export_chunk(
    d: &DeepCtx,
    x: &ExportCtx,
    client: &Client,
    bucket: &str,
    recs: &[WindowRecord],
    threads: usize,
    abort: &AtomicBool,
) -> anyhow::Result<Vec<WindowResult>> {
    let wins: Vec<_> = recs.iter().map(window_of).collect();
    info!(
        start = %date(recs[0].start_ns),
        stop = %date(recs[recs.len() - 1].stop_ns),
        windows = recs.len(),
        "export-lp chunk"
    );
    let (src, tgt) = std::thread::scope(|s| {
        let src = s.spawn(|| x.export.scan(&x.src_id, &wins, &d.rules, &d.scope, abort));
        let tgt = x.export.scan(&x.tgt_id, &wins, &d.rules, &d.scope, abort);
        let src = src
            .join()
            .unwrap_or_else(|_| Err(anyhow::anyhow!("export reader panicked")));
        (src, tgt)
    });
    let src = Mutex::new(src?);
    let tgt = Mutex::new(tgt?);
    let next = AtomicUsize::new(0);
    let results: Mutex<Vec<Option<WindowResult>>> =
        Mutex::new((0..recs.len()).map(|_| None).collect());
    std::thread::scope(|s| -> anyhow::Result<()> {
        let workers: Vec<_> = (0..threads.clamp(1, recs.len()))
            .map(|_| {
                s.spawn(|| -> anyhow::Result<()> {
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= recs.len() || abort.load(Ordering::Relaxed) {
                            return Ok(());
                        }
                        let t = tgt.lock().expect("lock").window(i);
                        let s = src.lock().expect("lock").window(i);
                        let w = export_window(d, x, client, bucket, &recs[i], s, t, abort)?;
                        results.lock().expect("lock")[i] = Some(w);
                    }
                })
            })
            .collect();
        for w in workers {
            w.join()
                .unwrap_or_else(|_| Err(anyhow::anyhow!("compare worker panicked")))?;
        }
        Ok(())
    })?;
    if abort.load(Ordering::Relaxed) {
        return Err(crate::guard::aborted_error());
    }
    Ok(results
        .into_inner()
        .expect("lock")
        .into_iter()
        .map(|w| w.expect("every window compared"))
        .collect())
}

#[allow(clippy::too_many_arguments)]
fn export_window(
    d: &DeepCtx,
    x: &ExportCtx,
    client: &Client,
    bucket: &str,
    rec: &WindowRecord,
    src: HashSink,
    tgt: HashSink,
    abort: &AtomicBool,
) -> anyhow::Result<WindowResult> {
    let dw = finish(&src, &tgt);
    let mut w = count_result(rec, &dw.target_by_measurement);
    w.deep = Some(dw.result);
    if !differs(&w) {
        return Ok(w);
    }
    let r = w.deep.as_ref().expect("deep result");
    debug!(
        window = %w.start,
        source_values = r.source_values,
        target_values = r.target_values,
        series_missing = r.series_missing,
        series_different = r.series_different,
        example = r.examples.first().map_or("", String::as_str),
        "export-lp shows a difference, re-reading the source through Flux"
    );
    let export_src = src.values;
    drop(src);
    let fsrc = scan(
        &d.src,
        &d.src_bucket,
        window_of(rec),
        &d.rules,
        &d.scope,
        abort,
    )?;
    let dw = finish(&fsrc, &tgt);
    let mut w = count_result(rec, &dw.target_by_measurement);
    w.deep = Some(dw.result);
    w.rechecked_via_flux = true;
    if !differs(&w) {
        w.source_values_only_on_disk = export_src.saturating_sub(fsrc.values);
        if w.source_values_only_on_disk > 0 && !x.warned_hidden.swap(true, Ordering::Relaxed) {
            warn!(
                window = %w.start,
                "the source bucket holds values on disk that Flux does not return (they could not be migrated); such windows are compared with the source read through Flux"
            );
        }
        return Ok(w);
    }
    info!(window = %w.start, "difference persists, re-checking the target through Flux too");
    let mut w = flux_deep(d, client, bucket, rec, abort)?;
    w.rechecked_via_flux = true;
    Ok(w)
}

fn check(client: &Client, bucket: &str, rec: &WindowRecord) -> anyhow::Result<WindowResult> {
    let q = format!(
        "from(bucket: {}) |> range(start: {}, stop: {}) |> count() |> group(columns: [\"_measurement\"]) |> sum()",
        lit(bucket),
        date(rec.start_ns),
        date(rec.stop_ns)
    );
    let mut actual: BTreeMap<String, u64> = BTreeMap::new();
    let pushed_down = for_each_row(client.query(&q)?, |h, r| {
        let get = |c: &str| h.index(c).and_then(|i| r.get(i)).unwrap_or("");
        *actual.entry(get("_measurement").to_owned()).or_default() +=
            get("_value").parse::<u64>().unwrap_or(0);
        Ok(())
    });
    if let Err(e) = pushed_down {
        // InfluxDB 2.x can panic in the pushed-down count() ("unreachable: <nil>");
        // count the rows instead.
        let msg = format!("{e:#}");
        if !msg.contains("panic") {
            return Err(e);
        }
        warn!(window = %date(rec.start_ns), error = %msg, "InfluxDB failed on count(), counting rows instead");
        actual.clear();
        let q = format!(
            "from(bucket: {}) |> range(start: {}, stop: {}) |> keep(columns: [\"_measurement\"])",
            lit(bucket),
            date(rec.start_ns),
            date(rec.stop_ns)
        );
        for_each_row(client.query(&q)?, |h, r| {
            let m = h.index("_measurement").and_then(|i| r.get(i)).unwrap_or("");
            match actual.get_mut(m) {
                Some(c) => *c += 1,
                None => {
                    actual.insert(m.to_owned(), 1);
                }
            }
            Ok(())
        })?;
    }
    Ok(count_result(rec, &actual))
}

/// Compare the per-measurement counts found in the target with the record.
fn count_result(rec: &WindowRecord, actual: &BTreeMap<String, u64>) -> WindowResult {
    let mut w = WindowResult {
        start: date(rec.start_ns),
        stop: date(rec.stop_ns),
        expected: rec.values_written,
        actual: actual.values().sum(),
        missing: 0,
        extra: 0,
        missing_by_measurement: BTreeMap::new(),
        deep: None,
        rechecked_via_flux: false,
        source_values_only_on_disk: 0,
    };
    for (m, exp) in &rec.written_by_measurement {
        let got = actual.get(m).copied().unwrap_or(0);
        if got < *exp {
            w.missing += exp - got;
            w.missing_by_measurement.insert(m.clone(), exp - got);
        }
    }
    for (m, got) in actual {
        let exp = rec.written_by_measurement.get(m).copied().unwrap_or(0);
        if *got > exp {
            w.extra += got - exp;
        }
    }
    w
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
    if a.read_via == ReadVia::ExportLp {
        if !a.deep {
            return Err(
                fail(Kind::Usage, "--read-via export-lp only applies to --deep")
                    .hint("add --deep, or drop --read-via (the count check always uses Flux)")
                    .into(),
            );
        }
        if g.target_url.as_deref().is_some_and(|u| u != g.url) {
            return Err(fail(
                Kind::Usage,
                "--read-via export-lp needs source and target on the same InfluxDB",
            )
            .hint("use --read-via flux for a target on another server")
            .into());
        }
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
        let src = connect(g)?;
        let export = if a.read_via == ReadVia::ExportLp {
            let src_bucket = src.bucket(&header.source)?;
            // export-lp decodes whole TSM blocks, which span a shard group:
            // reading one shard group per call avoids decoding it repeatedly.
            let chunk_secs = if a.export_chunk_days > 0 {
                i64::from(a.export_chunk_days) * 86_400
            } else {
                src_bucket
                    .shard_group_secs
                    .max(target.shard_group_secs)
                    .max(86_400)
            };
            info!(engine_path = %a.engine_path, influxd = %a.influxd, source_id = %src_bucket.id, target_id = %target.id, chunk_days = chunk_secs / 86_400, "deep compare reads the storage files via export-lp");
            Some(ExportCtx {
                export: Export::new(&a.influxd, &a.engine_path)?,
                src_id: src_bucket.id,
                tgt_id: target.id.clone(),
                chunk_secs,
                warned_hidden: AtomicBool::new(false),
            })
        } else {
            None
        };
        Some(DeepCtx {
            src,
            src_bucket: header.source.clone(),
            rules,
            scope,
            every: a.sample_every.max(1),
            export,
        })
    } else {
        None
    };
    info!(target = %target.name, state_file = %a.state.display(), parallel = g.parallel, deep = a.deep, "verify started");

    let (job_tx, job_rx) = bounded::<Job>(g.parallel.max(1) * 2);
    let (res_tx, res_rx) = unbounded::<anyhow::Result<WindowResult>>();
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
        deep: a.deep.then(|| DeepTotals {
            read_via: match a.read_via {
                ReadVia::Flux => "flux",
                ReadVia::ExportLp => "export-lp",
            },
            ..Default::default()
        }),
        resources: Resources::default(),
    };

    let scoped = std::thread::scope(|s| -> anyhow::Result<()> {
        for _ in 0..g.parallel.max(1) {
            let threads = g.parallel.max(1);
            let (job_rx, res_tx, client, bucket, shared, deep) = (
                job_rx.clone(),
                res_tx.clone(),
                client.clone(),
                &target.name,
                &shared,
                &deep,
            );
            s.spawn(move || {
                for job in job_rx.iter() {
                    if shared.aborted() {
                        break;
                    }
                    let results: Vec<anyhow::Result<WindowResult>> = match job {
                        Job::One(idx, rec) => {
                            let r = match deep {
                                Some(d) if idx % d.every == 0 => {
                                    flux_deep(d, &client, bucket, &rec, shared.flag())
                                }
                                _ => check(&client, bucket, &rec),
                            };
                            vec![r.with_context(|| format!("window {}", date(rec.start_ns)))]
                        }
                        Job::Chunk(recs) => {
                            let d = deep.as_ref().expect("chunks only in deep mode");
                            let x = d.export.as_ref().expect("chunks only with export-lp");
                            match export_chunk(d, x, &client, bucket, &recs, threads, shared.flag())
                            {
                                Ok(ws) => ws.into_iter().map(Ok).collect(),
                                Err(e) => vec![Err(e.context(format!(
                                    "windows {} to {}",
                                    date(recs[0].start_ns),
                                    date(recs[recs.len() - 1].stop_ns)
                                )))],
                            }
                        }
                    };
                    let mut failed = false;
                    for r in results {
                        failed |= r.is_err();
                        let _ = res_tx.send(r);
                    }
                    if failed {
                        break;
                    }
                }
            });
        }
        drop(res_tx);
        drop(job_rx);
        let state_path = a.state.clone();
        let (every, chunk_secs) = match &deep {
            Some(d) => (d.every, d.export.as_ref().map(|x| x.chunk_secs)),
            None => (1, None),
        };
        let feeder = s.spawn(move || {
            let send = |j: Job| {
                job_tx
                    .send(j)
                    .map_err(|_| anyhow::anyhow!("verify workers stopped"))
            };
            let mut idx = 0usize;
            // The state file lists windows in completion order (parallel
            // migration), so export chunks are collected per shard group
            // and sent once the file is read.
            let mut groups: BTreeMap<i64, Vec<WindowRecord>> = BTreeMap::new();
            let r = state::for_each_window(&state_path, |rec| {
                idx += 1;
                let Some(span) = chunk_secs.filter(|_| (idx - 1).is_multiple_of(every)) else {
                    return send(Job::One(idx - 1, rec));
                };
                groups
                    .entry(chunk_group(rec.start_ns, span))
                    .or_default()
                    .push(rec);
                Ok(())
            })
            .and_then(|_| {
                for (_, mut chunk) in groups {
                    chunk.sort_by_key(|r| r.start_ns);
                    send(Job::Chunk(chunk))?;
                }
                Ok(())
            });
            drop(job_tx);
            r
        });
        for r in res_rx.iter() {
            let w = match r {
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
                t.windows_rechecked_via_flux += usize::from(w.rechecked_via_flux);
                t.source_values_only_on_disk += w.source_values_only_on_disk;
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
            "Deep ({}): {} windows, {} series compared ({} source values, {} target values): {} missing, {} different, {} extra",
            d.read_via,
            d.windows_compared,
            n(d.series_compared as u64),
            n(d.source_values),
            n(d.target_values),
            d.series_missing,
            d.series_different,
            d.series_extra
        );
        if d.windows_rechecked_via_flux > 0 {
            let _ = writeln!(
                human,
                "  {} windows differed in export-lp and were re-checked through Flux",
                d.windows_rechecked_via_flux
            );
        }
        if d.source_values_only_on_disk > 0 {
            let _ = writeln!(
                human,
                "  {} source values are stored on disk but no query returns them",
                n(d.source_values_only_on_disk)
            );
        }
        for e in d.examples.iter().take(a.top) {
            let _ = writeln!(human, "  {e}");
        }
    }
    let deep_bad = out
        .deep
        .as_ref()
        .is_some_and(|d| d.series_missing + d.series_different > 0);
    let mut o = Outcome::ok(&out, human);
    if out
        .deep
        .as_ref()
        .is_some_and(|d| d.source_values_only_on_disk > 0)
    {
        o.warnings.push("the source bucket stores values that Flux and InfluxQL do not return (e.g. a field missing from the shard's field index); they were invisible before the migration and are not in the target".into());
    }
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
                "loxinflux-migration-helper migrate --source-bucket {} --target-bucket {} --start {} --stop {} --redo --state {}",
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
            "loxinflux-migration-helper cutover --source-bucket {} --target-bucket {}",
            sh(header.source.trim_start_matches("file:")),
            sh(&header.target)
        ));
    }
    Ok(o)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunks_align_with_shard_groups() {
        let day = 86_400;
        let ns = |s: &str| crate::window::parse_time(s).unwrap();
        // InfluxDB placed a 364-day shard group at 2024-02-05 and 7-day ones on Mondays.
        let g = |t: &str, d: i64| chunk_group(ns(t), d * day);
        assert_eq!(g("2024-02-04", 364) + 1, g("2024-02-05", 364));
        assert_eq!(g("2024-02-05", 364), g("2025-02-02", 364));
        assert_eq!(g("2025-06-08", 7) + 1, g("2025-06-09", 7));
        assert_eq!(g("2025-06-09", 7), g("2025-06-15", 7));
    }
}
