//! `verify`: compare the counts recorded by `migrate` with the target bucket.

use super::{connect, date, sh};
use crate::cli::{Global, VerifyArgs};
use crate::error::{Failure, Kind, fail};
use crate::flux::{for_each_row, lit};
use crate::output::{Outcome, n};
use crate::state::State;
use serde::Serialize;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use tracing::{info, warn};

#[derive(Serialize)]
struct WindowResult {
    start: String,
    stop: String,
    expected: u64,
    actual: u64,
    missing: u64,
    extra: u64,
    missing_by_measurement: BTreeMap<String, u64>,
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
    /// Only windows that are not exactly as expected.
    windows: Vec<WindowResult>,
}

pub fn run(g: &Global, a: &VerifyArgs) -> anyhow::Result<Outcome> {
    let state = State::load(&a.state)?.ok_or_else(|| {
        fail(
            Kind::NotFound,
            format!("state file {} not found", a.state.display()),
        )
        .hint("pass the --state file that migrate printed")
    })?;
    if state.dry_run {
        return Err(fail(
            Kind::Refused,
            "this state file is from a dry run; nothing was written",
        )
        .hint("run migrate without --dry-run first")
        .into());
    }
    let client = connect(g)?;
    let target = client.bucket(&state.target)?;
    info!(target = %target.name, windows = state.windows.len(), state_file = %a.state.display(), "verify started");

    let mut out = Report {
        target: target.name.clone(),
        source: state.source.clone(),
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
    };
    let total = state.windows.len();
    for (i, (start, rec)) in state.windows.iter().enumerate() {
        let q = format!(
            "from(bucket: {}) |> range(start: {}, stop: {}) |> count() |> group(columns: [\"_measurement\"]) |> sum()",
            lit(&target.name),
            date(*start),
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
            start: date(*start),
            stop: date(rec.stop_ns),
            expected: rec.values_written,
            actual: actual.values().sum(),
            missing: 0,
            extra: 0,
            missing_by_measurement: BTreeMap::new(),
        };
        for (m, exp) in &rec.written_by_measurement {
            let got = actual.get(m).copied().unwrap_or(0);
            if got < *exp {
                w.missing += exp - got;
                w.missing_by_measurement.insert(m.clone(), exp - got);
                *out.missing_by_measurement.entry(m.clone()).or_default() += exp - got;
            }
        }
        for (m, got) in &actual {
            let exp = rec.written_by_measurement.get(m).copied().unwrap_or(0);
            if *got > exp {
                w.extra += got - exp;
            }
        }
        out.windows_checked += 1;
        out.expected_total += w.expected;
        out.actual_total += w.actual;
        out.missing_total += w.missing;
        out.extra_total += w.extra;
        let label = format!("{}/{}", i + 1, total);
        if w.missing > 0 {
            out.windows_with_missing += 1;
            warn!(window = %label, start = %w.start, expected = w.expected, actual = w.actual, missing = w.missing, "window incomplete");
        } else {
            info!(window = %label, start = %w.start, expected = w.expected, actual = w.actual, extra = w.extra, "window ok");
        }
        if w.extra > 0 {
            out.windows_with_extra += 1;
        }
        if w.missing == 0 && w.extra == 0 {
            out.windows_ok += 1;
        } else {
            out.windows.push(w);
        }
    }
    info!(
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

    let mut o = Outcome::ok(&out, human);
    if out.extra_total > 0 {
        o.warnings.push("the target holds more values than were migrated; expected if live data is also written to it (dual write), otherwise check for other writers".into());
    }
    if out.missing_total > 0 {
        let first = out
            .windows
            .iter()
            .find(|w| w.missing > 0)
            .expect("missing > 0");
        if !state.source.starts_with("file:") {
            o.next_steps.push(format!(
                "loxinflux-migrationshelper migrate --source-bucket {} --target-bucket {} --start {} --stop {} --redo --state {}",
                sh(&state.source),
                sh(&state.target),
                first.start,
                first.stop,
                sh(&out.state_file)
            ));
        }
        o.failure = Some(
            Failure::new(
                Kind::VerifyMismatch,
                format!("{} values are missing in {} windows", out.missing_total, out.windows_with_missing),
            )
            .hint("re-migrate the affected windows with --redo and a --start/--stop covering them, then verify again"),
        );
    } else {
        o.next_steps.push(format!(
            "loxinflux-migrationshelper cutover --source-bucket {} --target-bucket {}",
            sh(state.source.trim_start_matches("file:")),
            sh(&state.target)
        ));
    }
    Ok(o)
}
