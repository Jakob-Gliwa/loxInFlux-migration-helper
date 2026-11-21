//! Hot-path helpers shared by the CSV and line protocol converters: byte-level
//! escaping, timestamp parsing and allocation-free counters.

use crate::convert::Action;
use std::collections::HashMap;

/// Append `s`, backslash-escaping every byte in `specials`.
#[inline]
pub fn escape_into(out: &mut Vec<u8>, s: &[u8], specials: &[u8]) {
    if !s.iter().any(|b| specials.contains(b)) {
        out.extend_from_slice(s);
        return;
    }
    for &b in s {
        if specials.contains(&b) {
            out.push(b'\\');
        }
        out.push(b);
    }
}

pub const MEASUREMENT_SPECIALS: &[u8] = b", ";
pub const KEY_SPECIALS: &[u8] = b",= ";
pub const STRING_SPECIALS: &[u8] = b"\"\\";

#[inline]
fn digits(s: &[u8]) -> Option<i64> {
    let mut v: i64 = 0;
    for &b in s {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (b - b'0') as i64;
    }
    Some(v)
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant).
#[inline]
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Parse the UTC form Flux emits (`2026-11-19T00:00:03.123456789Z`, fraction
/// optional) to Unix nanoseconds. Falls back to a full RFC 3339 parser for
/// anything else (offsets, lowercase).
pub fn parse_time_ns(s: &[u8]) -> Option<i64> {
    let fast = (|| {
        if s.len() < 20
            || s[4] != b'-'
            || s[7] != b'-'
            || s[10] != b'T'
            || s[13] != b':'
            || s[16] != b':'
        {
            return None;
        }
        let (y, mo, d) = (digits(&s[0..4])?, digits(&s[5..7])?, digits(&s[8..10])?);
        let (h, mi, se) = (
            digits(&s[11..13])?,
            digits(&s[14..16])?,
            digits(&s[17..19])?,
        );
        let mut i = 19;
        let mut frac: i64 = 0;
        if s[i] == b'.' {
            i += 1;
            let start = i;
            while i < s.len() && s[i].is_ascii_digit() {
                i += 1;
            }
            let n = i - start;
            if n == 0 || n > 9 {
                return None;
            }
            frac = digits(&s[start..i])? * 10i64.pow((9 - n) as u32);
        }
        if i + 1 != s.len() || s[i] != b'Z' {
            return None;
        }
        let secs = days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + se;
        secs.checked_mul(1_000_000_000)?.checked_add(frac)
    })();
    fast.or_else(|| {
        chrono::DateTime::parse_from_rfc3339(std::str::from_utf8(s).ok()?)
            .ok()?
            .timestamp_nanos_opt()
    })
}

/// Counters of one unit of work (a window or a file), mergeable.
#[derive(Clone, Debug, Default)]
pub struct Counters {
    pub actions: [u64; Action::COUNT],
    pub input_types: [u64; 5],
    pub values_read: u64,
    pub values_written: u64,
    pub values_dropped: u64,
}

pub const TYPE_NAMES: [&str; 5] = ["float", "integer", "unsigned", "boolean", "string"];
pub const T_FLOAT: usize = 0;
pub const T_INT: usize = 1;
pub const T_UINT: usize = 2;
pub const T_BOOL: usize = 3;
pub const T_STRING: usize = 4;

impl Counters {
    #[inline]
    pub fn record(&mut self, input_type: usize, action: Action) {
        self.values_read += 1;
        self.input_types[input_type] += 1;
        self.actions[action.index()] += 1;
        if action.is_drop() {
            self.values_dropped += 1;
        } else {
            self.values_written += 1;
        }
    }

    pub fn merge(&mut self, o: &Counters) {
        for (a, b) in self.actions.iter_mut().zip(o.actions) {
            *a += b;
        }
        for (a, b) in self.input_types.iter_mut().zip(o.input_types) {
            *a += b;
        }
        self.values_read += o.values_read;
        self.values_written += o.values_written;
        self.values_dropped += o.values_dropped;
    }

    pub fn actions_map(&self) -> std::collections::BTreeMap<String, u64> {
        Action::ALL
            .iter()
            .zip(self.actions)
            .filter(|(_, n)| *n > 0)
            .map(|(a, n)| (a.as_str().to_owned(), n))
            .collect()
    }

    pub fn types_map(&self) -> std::collections::BTreeMap<String, u64> {
        TYPE_NAMES
            .iter()
            .zip(self.input_types)
            .filter(|(_, n)| *n > 0)
            .map(|(t, n)| ((*t).to_owned(), n))
            .collect()
    }
}

/// Written values per measurement, counted in runs: consecutive values of the
/// same measurement only touch the map once.
#[derive(Default)]
pub struct MeasurementRuns {
    pub map: HashMap<String, u64>,
    current: Vec<u8>,
    run: u64,
}

impl MeasurementRuns {
    #[inline]
    pub fn add(&mut self, measurement: &[u8], n: u64) {
        if n == 0 {
            return;
        }
        if measurement != self.current.as_slice() {
            self.flush();
            self.current.clear();
            self.current.extend_from_slice(measurement);
        }
        self.run += n;
    }

    pub fn flush(&mut self) {
        if self.run > 0 {
            let key = String::from_utf8_lossy(&self.current).into_owned();
            *self.map.entry(key).or_default() += self.run;
            self.run = 0;
        }
    }

    pub fn take(&mut self) -> HashMap<String, u64> {
        self.flush();
        std::mem::take(&mut self.map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fast_time_matches_chrono() {
        for s in [
            "2026-11-19T00:00:03Z",
            "2026-11-19T00:00:03.5Z",
            "2023-11-14T22:13:20.123456789Z",
            "1970-01-01T00:00:00Z",
            "2000-02-29T23:59:59.000000001Z",
            "1969-12-31T23:59:59Z",
        ] {
            let want = chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap();
            assert_eq!(parse_time_ns(s.as_bytes()), Some(want), "{s}");
        }
        assert_eq!(
            parse_time_ns(b"2026-11-19T01:00:00+01:00"),
            Some(
                chrono::DateTime::parse_from_rfc3339("2026-11-19T00:00:00Z")
                    .unwrap()
                    .timestamp_nanos_opt()
                    .unwrap()
            )
        );
        assert_eq!(parse_time_ns(b"garbage"), None);
    }

    #[test]
    fn escaping_is_skipped_when_not_needed() {
        let mut v = Vec::new();
        escape_into(&mut v, b"Energy Heating", MEASUREMENT_SPECIALS);
        escape_into(&mut v, b"|plain", KEY_SPECIALS);
        assert_eq!(v, b"Energy\\ Heating|plain");
    }

    #[test]
    fn runs_count_per_measurement() {
        let mut r = MeasurementRuns::default();
        r.add(b"a", 2);
        r.add(b"a", 3);
        r.add(b"b", 1);
        r.add(b"a", 1);
        let m = r.take();
        assert_eq!(m["a"], 6);
        assert_eq!(m["b"], 1);
    }
}
