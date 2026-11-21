//! Time handling and shard-aligned windows.
//!
//! InfluxDB fixes a field's type per shard, and a Flux query spanning shards
//! with different types for one field silently returns only one of them. The
//! tool therefore reads exactly one shard group at a time: windows are aligned
//! to the bucket's shard group duration, counted from the Unix epoch, which is
//! how InfluxDB places shard groups.

use crate::error::{Kind, fail};
use chrono::{DateTime, NaiveDate, Utc};

pub const NS: i64 = 1_000_000_000;

/// Parse `2024-01-31`, `2024-01-31T12:00:00Z` or `now`.
pub fn parse_time(s: &str) -> anyhow::Result<i64> {
    if s == "now" {
        return Ok(now_ns());
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return dt.timestamp_nanos_opt().ok_or_else(|| out_of_range(s));
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return d
            .and_hms_opt(0, 0, 0)
            .and_then(|dt| dt.and_utc().timestamp_nanos_opt())
            .ok_or_else(|| out_of_range(s));
    }
    Err(fail(Kind::Usage, format!("cannot parse time `{s}`"))
        .hint("use YYYY-MM-DD, RFC 3339 (2024-01-31T12:00:00Z) or `now`")
        .into())
}

fn out_of_range(s: &str) -> anyhow::Error {
    fail(Kind::Usage, format!("time `{s}` is out of range")).into()
}

pub fn now_ns() -> i64 {
    Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX)
}

/// RFC 3339 with nanoseconds, UTC — the form Flux `range()` accepts.
pub fn fmt_time(ns: i64) -> String {
    DateTime::<Utc>::from_timestamp_nanos(ns).to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
}

/// InfluxDB's default shard group duration for a retention period
/// (0 = infinite), used when the bucket does not state one.
pub fn default_shard_duration_secs(retention_secs: i64) -> i64 {
    const HOUR: i64 = 3600;
    const DAY: i64 = 24 * HOUR;
    match retention_secs {
        0 => 7 * DAY,
        r if r < 2 * DAY => HOUR,
        r if r <= 180 * DAY => DAY,
        _ => 7 * DAY,
    }
}

fn gcd(a: i64, b: i64) -> i64 {
    if b == 0 { a } else { gcd(b, a % b) }
}

/// Query window length. It must never span two shard groups, including shard
/// groups created under an earlier, different shard duration that the bucket
/// no longer reports. Shard groups start at multiples of their duration
/// counted from 0001-01-01 (Go's zero time), which is a whole number of days
/// before the Unix epoch; so a grid that divides one day lines up with every
/// shard duration that is a multiple of it. Default: the largest such grid
/// that also divides the current shard duration (1 day for 1d/7d/364d shards).
pub fn grid_secs(shard_secs: i64, requested: Option<i64>) -> anyhow::Result<i64> {
    let safe = gcd(shard_secs.max(1), 86_400);
    match requested {
        None => Ok(safe),
        Some(r) if r > 0 && 86_400 % r == 0 && shard_secs % r == 0 => Ok(r),
        Some(r) => Err(fail(
            Kind::Usage,
            format!("--window {r}s must divide both one day and the shard group duration ({shard_secs}s)"),
        )
        .hint(format!("use {safe}s or a divisor of it, e.g. 6h or 1h"))
        .into()),
    }
}

/// Parse `90s`, `15m`, `6h`, `1d`.
pub fn parse_duration_secs(s: &str) -> anyhow::Result<i64> {
    let (num, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let n: i64 = num
        .parse()
        .map_err(|_| fail(Kind::Usage, format!("invalid duration `{s}`")))?;
    let mult = match unit {
        "s" | "" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86_400,
        _ => {
            return Err(fail(Kind::Usage, format!("invalid duration unit in `{s}`"))
                .hint("use s, m, h or d")
                .into());
        }
    };
    Ok(n * mult)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Window {
    pub start: i64,
    pub stop: i64,
}

/// Half-open windows `[start, stop)` covering `[start, stop)`, cut at shard
/// group boundaries.
pub fn windows(start: i64, stop: i64, shard_secs: i64) -> Vec<Window> {
    let step = shard_secs.max(1) * NS;
    let mut out = Vec::new();
    if start >= stop {
        return out;
    }
    let mut cur = start;
    while cur < stop {
        let boundary = cur.div_euclid(step) * step + step;
        let next = boundary.min(stop);
        out.push(Window {
            start: cur,
            stop: next,
        });
        cur = next;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_dates_and_rfc3339() {
        assert_eq!(parse_time("1970-01-02").unwrap(), 86_400 * NS);
        assert_eq!(parse_time("1970-01-01T00:00:01.5Z").unwrap(), NS + NS / 2);
        assert!(parse_time("gestern").is_err());
    }

    #[test]
    fn windows_follow_shard_boundaries() {
        let week = 7 * 86_400;
        // Epoch-aligned weeks start on Thursdays, like InfluxDB shard groups.
        let start = parse_time("2023-11-14").unwrap();
        let stop = parse_time("2023-11-30").unwrap();
        let w = windows(start, stop, week);
        assert_eq!(w.first().unwrap().start, start);
        assert_eq!(w.last().unwrap().stop, stop);
        assert_eq!(fmt_time(w[0].stop), "2023-11-16T00:00:00Z");
        assert_eq!(fmt_time(w[1].stop), "2023-11-23T00:00:00Z");
        assert_eq!(w.len(), 3);
        for pair in w.windows(2) {
            assert_eq!(pair[0].stop, pair[1].start);
        }
        assert!(windows(stop, start, week).is_empty());
    }

    #[test]
    fn grid_is_shard_safe() {
        assert_eq!(grid_secs(604_800, None).unwrap(), 86_400);
        assert_eq!(grid_secs(8736 * 3600, None).unwrap(), 86_400);
        assert_eq!(grid_secs(3600, None).unwrap(), 3600);
        assert_eq!(grid_secs(604_800, Some(21_600)).unwrap(), 21_600);
        assert!(grid_secs(604_800, Some(172_800)).is_err());
        assert!(grid_secs(3600, Some(86_400)).is_err());
        assert_eq!(parse_duration_secs("6h").unwrap(), 21_600);
        assert_eq!(parse_duration_secs("1d").unwrap(), 86_400);
        assert!(parse_duration_secs("1w").is_err());
    }

    #[test]
    fn default_shard_durations_match_influxdb() {
        assert_eq!(default_shard_duration_secs(0), 604_800);
        assert_eq!(default_shard_duration_secs(3600), 3600);
        assert_eq!(default_shard_duration_secs(30 * 86_400), 86_400);
        assert_eq!(default_shard_duration_secs(365 * 86_400), 604_800);
    }
}
