//! `analyze`: read-only inventory of a bucket, one shard window at a time.

use super::{Resources, connect, date, human_duration, sh};
use crate::cli::{AnalyzeArgs, Global};
use crate::convert::{Action, Rules, convert};
use crate::flux::{self, for_each_row, lit};
use crate::guard::{MemWatch, Shared};
use crate::influx::Client;
use crate::output::{Outcome, n};
use crate::window::{Window, grid_secs, parse_duration_secs, parse_time, windows};
use anyhow::Context as _;
use crossbeam_channel::{bounded, unbounded};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

#[derive(Default, Serialize)]
struct TypeStat {
    values: u64,
    windows: u32,
    first_window: String,
    last_window: String,
}

#[derive(Default, Serialize)]
struct FieldReport {
    measurement: String,
    field: String,
    values: u64,
    types: BTreeMap<String, TypeStat>,
    /// Only with --classify-strings: what `migrate` would do with the string values.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    string_actions: BTreeMap<String, u64>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    string_examples: BTreeMap<String, Vec<String>>,
}

#[derive(Serialize)]
struct Report {
    bucket: String,
    shard_group_secs: i64,
    retention_secs: i64,
    window_secs: i64,
    start: String,
    stop: String,
    windows: usize,
    windows_with_data: usize,
    values_total: u64,
    values_by_type: BTreeMap<String, u64>,
    fields_total: usize,
    fields_with_type_conflicts: usize,
    /// What `migrate` with default rules would do, by action (strings only if classified).
    migrate_estimate: BTreeMap<String, u64>,
    strings_classified: bool,
    /// Windows where InfluxDB failed on the pushed-down queries (rows were scanned instead).
    windows_with_influx_query_bug: Vec<String>,
    fields: Vec<FieldReport>,
    resources: Resources,
}

const EXAMPLES_PER_ACTION: usize = 3;
const PAIRS_PER_QUERY: usize = 40;

/// Per action: how many values and up to three examples.
type ActionStats = BTreeMap<&'static str, (u64, Vec<String>)>;

/// What one window contributed.
#[derive(Default)]
struct WindowScan {
    /// (measurement, field) -> (flux type, values)
    fields: HashMap<(String, String), (String, u64)>,
    /// (measurement, field) -> action -> (count, examples)
    strings: HashMap<(String, String), ActionStats>,
    string_values: u64,
    /// The pushed-down queries failed in InfluxDB; rows were scanned instead.
    fallback_scan: bool,
}

fn scan_window(
    client: &Client,
    bucket: &str,
    w: Window,
    measurements: &[String],
    classify: bool,
    rules: &Rules,
) -> anyhow::Result<WindowScan> {
    let src = flux::source(bucket, w, measurements);
    let mut scan = WindowScan::default();
    // Within one shard a field has exactly one type, so grouping is safe.
    // first()/count() are pushed down into the storage engine, which is fast;
    // InfluxDB 2.x can panic there on some series ("unreachable: <nil>"), so a
    // failing window is retried with a plain scan of its rows.
    let fast = (|| -> anyhow::Result<()> {
        let q = format!(
            "{src} |> first() |> group(columns: [\"_measurement\", \"_field\"]) |> limit(n: 1)"
        );
        let mut types: HashMap<(String, String), String> = HashMap::new();
        for_each_row(client.query(&q)?, |h, r| {
            let get = |c: &str| h.index(c).and_then(|i| r.get(i)).unwrap_or("").to_owned();
            let t = h
                .index("_value")
                .and_then(|i| h.types.get(i))
                .cloned()
                .unwrap_or_default();
            types.insert((get("_measurement"), get("_field")), t);
            Ok(())
        })?;
        if types.is_empty() {
            return Ok(());
        }
        let q =
            format!("{src} |> count() |> group(columns: [\"_measurement\", \"_field\"]) |> sum()");
        for_each_row(client.query(&q)?, |h, r| {
            let get = |c: &str| h.index(c).and_then(|i| r.get(i)).unwrap_or("").to_owned();
            let key = (get("_measurement"), get("_field"));
            let count: u64 = get("_value").parse().unwrap_or(0);
            let t = types.get(&key).cloned().unwrap_or_else(|| "unknown".into());
            let e = scan.fields.entry(key).or_insert((t, 0));
            e.1 += count;
            Ok(())
        })?;
        Ok(())
    })();
    if let Err(e) = fast {
        let msg = format!("{e:#}");
        if !msg.contains("panic") {
            return Err(e);
        }
        warn!(window = %date(w.start), error = %msg, "InfluxDB failed on the pushed-down query, scanning the window's rows instead");
        scan.fields.clear();
        scan.fallback_scan = true;
        for_each_row(client.query(&src)?, |h, r| {
            let get = |c: &str| h.index(c).and_then(|i| r.get(i)).unwrap_or("");
            let t = h
                .index("_value")
                .and_then(|i| h.types.get(i))
                .cloned()
                .unwrap_or_default();
            let key = (get("_measurement").to_owned(), get("_field").to_owned());
            match scan.fields.get_mut(&key) {
                Some(e) => e.1 += 1,
                None => {
                    scan.fields.insert(key, (t, 1));
                }
            }
            Ok(())
        })?;
    }
    if scan.fields.is_empty() {
        return Ok(scan);
    }
    let types: HashMap<(String, String), String> = scan
        .fields
        .iter()
        .map(|(k, (t, _))| (k.clone(), t.clone()))
        .collect();
    if classify {
        let pairs: Vec<&(String, String)> = types
            .iter()
            .filter(|(_, t)| t.as_str() == "string")
            .map(|(k, _)| k)
            .collect();
        for chunk in pairs.chunks(PAIRS_PER_QUERY) {
            let cond: Vec<String> = chunk
                .iter()
                .map(|(m, f)| format!("(r._measurement == {} and r._field == {})", lit(m), lit(f)))
                .collect();
            let q = format!(
                "{} |> filter(fn: (r) => {})",
                flux::source(bucket, w, &[]),
                cond.join(" or ")
            );
            for_each_row(client.query(&q)?, |h, r| {
                let Some(p) = flux::row_to_point(h, r)? else {
                    return Ok(());
                };
                let (field, value) = p.fields.into_iter().next().expect("one field per row");
                let raw = match &value {
                    crate::convert::FieldValue::Str(s) => s.clone(),
                    _ => return Ok(()),
                };
                scan.string_values += 1;
                let action = convert(&field, value, rules).action;
                let e = scan
                    .strings
                    .entry((p.measurement, field))
                    .or_default()
                    .entry(action.as_str())
                    .or_default();
                e.0 += 1;
                if e.1.len() < EXAMPLES_PER_ACTION && !e.1.contains(&raw) {
                    e.1.push(raw);
                }
                Ok(())
            })?;
        }
    }
    Ok(scan)
}

pub fn run(g: &Global, a: &AnalyzeArgs) -> anyhow::Result<Outcome> {
    let shared = Shared::new_registered();
    let mem = MemWatch::start(g.memory_limit_mb, shared.clone());
    let client = Arc::new(connect(g)?);
    let bucket = client.bucket(&a.bucket)?;
    let start = parse_time(&a.range.start)?;
    let stop = parse_time(&a.range.stop)?;
    let requested = a
        .range
        .window
        .as_deref()
        .map(parse_duration_secs)
        .transpose()?;
    let grid = grid_secs(bucket.shard_group_secs, requested)?;
    let ws = windows(start, stop, grid);
    info!(
        bucket = %bucket.name,
        windows = ws.len(),
        window = %human_duration(grid),
        shard_group = %human_duration(bucket.shard_group_secs),
        start = %date(start),
        stop = %date(stop),
        classify_strings = a.classify_strings,
        parallel = g.parallel,
        "analyze started"
    );
    let rules = Rules::default();
    let mut fields: BTreeMap<(String, String), FieldReport> = BTreeMap::new();
    let mut windows_with_data = 0;
    let mut bug_windows: Vec<String> = Vec::new();
    let began = Instant::now();

    let (job_tx, job_rx) = bounded::<usize>(ws.len().max(1));
    for i in 0..ws.len() {
        let _ = job_tx.send(i);
    }
    drop(job_tx);
    let (res_tx, res_rx) = unbounded::<(usize, anyhow::Result<WindowScan>)>();
    let mut done = 0usize;
    let mut last_log = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..g.parallel.max(1).min(ws.len()) {
            let (job_rx, res_tx, client, shared, rules, ws, bucket) = (
                job_rx.clone(),
                res_tx.clone(),
                &client,
                &shared,
                &rules,
                &ws,
                &bucket.name,
            );
            let (measurements, classify) = (&a.range.measurements, a.classify_strings);
            s.spawn(move || {
                for i in job_rx.iter() {
                    if shared.aborted() {
                        break;
                    }
                    let r = scan_window(client, bucket, ws[i], measurements, classify, rules)
                        .with_context(|| format!("window {}", date(ws[i].start)));
                    let failed = r.is_err();
                    let _ = res_tx.send((i, r));
                    if failed {
                        break;
                    }
                }
            });
        }
        drop(res_tx);
        for (i, r) in res_rx.iter() {
            let scan = match r {
                Ok(x) => x,
                Err(e) => {
                    shared.fail(e);
                    break;
                }
            };
            done += 1;
            let w = ws[i];
            if scan.fallback_scan {
                bug_windows.push(date(w.start));
            }
            if scan.fields.is_empty() {
                debug!(start = %date(w.start), "window empty");
            } else {
                windows_with_data += 1;
                let wdate = date(w.start);
                for ((m, f), (t, count)) in scan.fields {
                    let entry =
                        fields
                            .entry((m.clone(), f.clone()))
                            .or_insert_with(|| FieldReport {
                                measurement: m,
                                field: f,
                                ..Default::default()
                            });
                    entry.values += count;
                    let ts = entry.types.entry(type_name(&t)).or_default();
                    ts.values += count;
                    ts.windows += 1;
                    if ts.first_window.is_empty() || wdate < ts.first_window {
                        ts.first_window.clone_from(&wdate);
                    }
                    if wdate > ts.last_window {
                        ts.last_window.clone_from(&wdate);
                    }
                }
                for (key, actions) in scan.strings {
                    let entry = fields.entry(key).or_default();
                    for (action, (c, ex)) in actions {
                        *entry.string_actions.entry(action.into()).or_default() += c;
                        let e = entry.string_examples.entry(action.into()).or_default();
                        for x in ex {
                            if e.len() < EXAMPLES_PER_ACTION && !e.contains(&x) {
                                e.push(x);
                            }
                        }
                    }
                }
            }
            if last_log.elapsed().as_secs() >= 30 {
                info!(
                    windows_done = format!("{done}/{}", ws.len()),
                    fields = fields.len(),
                    rss_mib = crate::guard::rss_bytes().map(|b| b >> 20),
                    "analyze progress"
                );
                last_log = Instant::now();
            }
        }
    });
    mem.stop();
    if shared.aborted() {
        return Err(shared
            .take_reason()
            .unwrap_or_else(crate::guard::aborted_error));
    }

    let mut values_by_type: BTreeMap<String, u64> = BTreeMap::new();
    let mut estimate: BTreeMap<String, u64> = BTreeMap::new();
    for f in fields.values() {
        for (t, s) in &f.types {
            *values_by_type.entry(t.clone()).or_default() += s.values;
            let action = match t.as_str() {
                "float" => Some(Action::FloatKept),
                "integer" => Some(Action::IntToFloat),
                "unsigned" => Some(Action::UnsignedToFloat),
                "boolean" => Some(Action::BoolToFloat),
                _ => None,
            };
            if let Some(action) = action {
                *estimate.entry(action.as_str().into()).or_default() += s.values;
            } else if t == "string" && !a.classify_strings {
                *estimate.entry("string_unclassified".into()).or_default() += s.values;
            }
        }
        for (action, c) in &f.string_actions {
            *estimate.entry(action.clone()).or_default() += c;
        }
    }
    let mut list: Vec<FieldReport> = fields.into_values().collect();
    list.sort_by(|a, b| {
        (b.types.len(), b.values)
            .cmp(&(a.types.len(), a.values))
            .then_with(|| (&a.measurement, &a.field).cmp(&(&b.measurement, &b.field)))
    });
    let conflicts = list.iter().filter(|f| f.types.len() > 1).count();
    let report = Report {
        bucket: bucket.name.clone(),
        shard_group_secs: bucket.shard_group_secs,
        retention_secs: bucket.retention_secs,
        window_secs: grid,
        start: date(start),
        stop: date(stop),
        windows: ws.len(),
        windows_with_data,
        values_total: values_by_type.values().sum(),
        values_by_type,
        fields_total: list.len(),
        fields_with_type_conflicts: conflicts,
        migrate_estimate: estimate,
        strings_classified: a.classify_strings,
        windows_with_influx_query_bug: {
            bug_windows.sort();
            bug_windows
        },
        fields: list,
        resources: Resources {
            peak_rss_mib: mem.peak_mib(),
            memory_limit_mib: mem.limit_bytes >> 20,
            ..Default::default()
        },
    };
    info!(
        values = report.values_total,
        fields = report.fields_total,
        conflicts,
        elapsed_secs = began.elapsed().as_secs(),
        "analyze finished"
    );

    let human = render(&report, a.top);
    let mut o = Outcome::ok(&report, human);
    if report.values_total == 0 {
        o.warnings
            .push("no data in the given range; check --start/--stop and --measurement".into());
    }
    if conflicts > 0 {
        o.warnings.push(format!(
            "{conflicts} fields have different types in different shards; Flux queries across those shards silently return only one type"
        ));
    }
    if !a.classify_strings && report.values_by_type.contains_key("string") {
        o.next_steps.push(
            "rerun with --classify-strings to see how string values would be converted".to_string(),
        );
    }
    o.next_steps.push(format!(
        "loxinflux-migration-helper migrate --source-bucket {} --target-bucket {} --start {} --dry-run",
        sh(&bucket.name),
        sh(&format!("{}_v2", bucket.name)),
        sh(&a.range.start)
    ));
    Ok(o)
}

fn type_name(flux_type: &str) -> String {
    match flux_type {
        "double" => "float",
        "long" => "integer",
        "unsignedLong" => "unsigned",
        "boolean" => "boolean",
        "string" => "string",
        other => other,
    }
    .to_owned()
}

fn render(r: &Report, top: usize) -> String {
    let mut s = String::new();
    let _ = writeln!(
        s,
        "Bucket {} (shard group {}, retention {}), {} .. {}: {} windows, {} with data",
        r.bucket,
        human_duration(r.shard_group_secs),
        human_duration(r.retention_secs),
        r.start,
        r.stop,
        r.windows,
        r.windows_with_data
    );
    let types: Vec<String> = r
        .values_by_type
        .iter()
        .map(|(t, v)| format!("{t} {}", n(*v)))
        .collect();
    let _ = writeln!(s, "Values: {} ({})", n(r.values_total), types.join(", "));
    let _ = writeln!(
        s,
        "Fields: {} (measurement + field), {} with more than one type",
        n(r.fields_total as u64),
        r.fields_with_type_conflicts
    );
    if r.fields_with_type_conflicts > 0 {
        let _ = writeln!(s, "\nType conflicts across shards (top {top}):");
        for f in r.fields.iter().filter(|f| f.types.len() > 1).take(top) {
            let parts: Vec<String> = f
                .types
                .iter()
                .map(|(t, st)| {
                    format!(
                        "{t} {} values in {} windows ({}..{})",
                        n(st.values),
                        st.windows,
                        &st.first_window[..10],
                        &st.last_window[..10]
                    )
                })
                .collect();
            let _ = writeln!(s, "  {}.{}: {}", f.measurement, f.field, parts.join("; "));
        }
    }
    let mut strings: Vec<&FieldReport> = r
        .fields
        .iter()
        .filter(|f| f.types.contains_key("string"))
        .collect();
    strings.sort_by(|a, b| b.types["string"].values.cmp(&a.types["string"].values));
    if !strings.is_empty() {
        let _ = writeln!(s, "\nString fields (top {top} of {}):", strings.len());
        for f in strings.iter().take(top) {
            let mut line = format!(
                "  {}.{}: {} values",
                f.measurement,
                f.field,
                n(f.types["string"].values)
            );
            if !f.string_actions.is_empty() {
                let acts: Vec<String> = f
                    .string_actions
                    .iter()
                    .map(|(a, c)| {
                        let ex = f
                            .string_examples
                            .get(a)
                            .map(|e| e.join(" | "))
                            .unwrap_or_default();
                        format!("{a} {} [{ex}]", n(*c))
                    })
                    .collect();
                let _ = write!(line, " -> {}", acts.join("; "));
            }
            let _ = writeln!(s, "{line}");
        }
    }
    let est: Vec<String> = r
        .migrate_estimate
        .iter()
        .map(|(a, c)| format!("{a} {}", n(*c)))
        .collect();
    let _ = writeln!(s, "\nmigrate would do: {}", est.join(", "));
    s
}
