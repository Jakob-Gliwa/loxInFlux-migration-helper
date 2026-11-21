//! `analyze`: read-only inventory of a bucket, one shard window at a time.

use super::{connect, date, human_duration, sh};
use crate::cli::{AnalyzeArgs, Global};
use crate::convert::{Action, Rules, convert};
use crate::flux::{self, for_each_row, lit};
use crate::output::{Outcome, n};
use crate::window::{parse_time, windows};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::time::Instant;
use tracing::{debug, info};

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
    fields: Vec<FieldReport>,
}

const EXAMPLES_PER_ACTION: usize = 3;
const PAIRS_PER_QUERY: usize = 40;

pub fn run(g: &Global, a: &AnalyzeArgs) -> anyhow::Result<Outcome> {
    let client = connect(g)?;
    let bucket = client.bucket(&a.bucket)?;
    let start = parse_time(&a.range.start)?;
    let stop = parse_time(&a.range.stop)?;
    let ws = windows(start, stop, bucket.shard_group_secs);
    info!(
        bucket = %bucket.name,
        windows = ws.len(),
        shard_group = %human_duration(bucket.shard_group_secs),
        start = %date(start),
        stop = %date(stop),
        classify_strings = a.classify_strings,
        "analyze started"
    );
    let rules = Rules::default();
    let mut fields: BTreeMap<(String, String), FieldReport> = BTreeMap::new();
    let mut windows_with_data = 0;
    let began = Instant::now();

    for (i, w) in ws.iter().enumerate() {
        let src = flux::source(&bucket.name, *w, &a.range.measurements);
        // Within one shard a field has exactly one type, so grouping is safe.
        let mut types: HashMap<(String, String), String> = HashMap::new();
        let q = format!(
            "{src} |> first() |> group(columns: [\"_measurement\", \"_field\"]) |> limit(n: 1)"
        );
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
            debug!(window = format!("{}/{}", i + 1, ws.len()), start = %date(w.start), "window empty");
            continue;
        }
        windows_with_data += 1;
        let q =
            format!("{src} |> count() |> group(columns: [\"_measurement\", \"_field\"]) |> sum()");
        let mut window_values = 0u64;
        for_each_row(client.query(&q)?, |h, r| {
            let get = |c: &str| h.index(c).and_then(|i| r.get(i)).unwrap_or("").to_owned();
            let key = (get("_measurement"), get("_field"));
            let count: u64 = get("_value").parse().unwrap_or(0);
            window_values += count;
            let t = types.get(&key).cloned().unwrap_or_else(|| "unknown".into());
            let f = fields.entry(key.clone()).or_insert_with(|| FieldReport {
                measurement: key.0.clone(),
                field: key.1.clone(),
                ..Default::default()
            });
            f.values += count;
            let ts = f.types.entry(type_name(&t)).or_default();
            ts.values += count;
            ts.windows += 1;
            if ts.first_window.is_empty() {
                ts.first_window = date(w.start);
            }
            ts.last_window = date(w.start);
            Ok(())
        })?;

        let mut window_strings = 0u64;
        if a.classify_strings {
            let pairs: Vec<&(String, String)> = types
                .iter()
                .filter(|(_, t)| t.as_str() == "string")
                .map(|(k, _)| k)
                .collect();
            for chunk in pairs.chunks(PAIRS_PER_QUERY) {
                let cond: Vec<String> = chunk
                    .iter()
                    .map(|(m, f)| {
                        format!("(r._measurement == {} and r._field == {})", lit(m), lit(f))
                    })
                    .collect();
                let q = format!(
                    "{} |> filter(fn: (r) => {})",
                    flux::source(&bucket.name, *w, &[]),
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
                    window_strings += 1;
                    let action = convert(&field, value, &rules).action;
                    let f = fields
                        .entry((p.measurement.clone(), field.clone()))
                        .or_default();
                    *f.string_actions.entry(action.as_str().into()).or_default() += 1;
                    let ex = f.string_examples.entry(action.as_str().into()).or_default();
                    if ex.len() < EXAMPLES_PER_ACTION && !ex.contains(&raw) {
                        ex.push(raw);
                    }
                    Ok(())
                })?;
            }
        }
        info!(
            window = format!("{}/{}", i + 1, ws.len()),
            start = %date(w.start),
            fields = types.len(),
            values = window_values,
            string_values_classified = window_strings,
            elapsed_secs = began.elapsed().as_secs(),
            "window analyzed"
        );
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
        fields: list,
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
        "loxinflux-migrationshelper migrate --source-bucket {} --target-bucket {} --start {} --dry-run",
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
