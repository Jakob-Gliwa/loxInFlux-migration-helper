//! `verify --deep`: value-level comparison of source and target.
//!
//! Both sides of a window are streamed through the same converter (same rules
//! and scope as the migration; conversion is idempotent, so already migrated
//! target data comes out unchanged). Per series and field we keep only a count
//! and an order-independent checksum over (timestamp, value), so memory stays
//! proportional to the number of series, not the number of values.

use crate::convert::Rules;
use crate::fastcsv::CsvConverter;
use crate::flux;
use crate::influx::Client;
use crate::pipeline::LineSink;
use crate::window::Window;
use serde::Serialize;
use std::collections::HashMap;
use std::hash::{BuildHasher, BuildHasherDefault, DefaultHasher, Hasher};
use std::sync::atomic::AtomicBool;

/// Accumulates `series,tags field` -> (count, wrapping sum of value hashes).
#[derive(Default)]
pub struct HashSink {
    buf: Vec<u8>,
    pub series: HashMap<Vec<u8>, (u64, u64)>,
    pub values: u64,
}

fn value_hash(time: &[u8], value: &[u8]) -> u64 {
    let mut h = BuildHasherDefault::<DefaultHasher>::default().build_hasher();
    h.write(time);
    h.write_u8(0);
    // Numbers are compared by value, not by spelling (`1e+06` == `1000000.0`).
    if value.first() != Some(&b'"')
        && !matches!(value.last(), Some(b'i' | b'u'))
        && let Some(f) = std::str::from_utf8(value)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
    {
        h.write_u8(b'f');
        h.write_u64(f.to_bits());
    } else {
        h.write(value);
    }
    h.finish()
}

impl HashSink {
    fn absorb(&mut self) {
        for line in self.buf.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
            // One field per line: <series> <field>=<value> <time>
            let mut i = 0;
            while i < line.len() && line[i] != b' ' {
                i += if line[i] == b'\\' { 2 } else { 1 };
            }
            let mut eq = i + 1;
            while eq < line.len() && line[eq] != b'=' {
                eq += if line[eq] == b'\\' { 2 } else { 1 };
            }
            let Some(sp) = line.iter().rposition(|b| *b == b' ') else {
                continue;
            };
            if eq >= sp {
                continue;
            }
            let key = &line[..eq];
            let h = value_hash(&line[sp + 1..], &line[eq + 1..sp]);
            match self.series.get_mut(key) {
                Some(e) => {
                    e.0 += 1;
                    e.1 = e.1.wrapping_add(h);
                }
                None => {
                    self.series.insert(key.to_vec(), (1, h));
                }
            }
        }
        self.buf.clear();
    }
}

impl LineSink for HashSink {
    fn buf(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }
    fn commit(&mut self, values: u64) -> anyhow::Result<()> {
        self.values += values;
        self.absorb();
        Ok(())
    }
}

#[derive(Debug, Default, Serialize)]
pub struct DeepResult {
    pub source_values: u64,
    pub target_values: u64,
    pub series_compared: usize,
    pub series_missing: usize,
    pub series_different: usize,
    pub series_extra: usize,
    /// Up to 5 affected `measurement,tags field` keys.
    pub examples: Vec<String>,
}

pub fn compare(source: &HashSink, target: &HashSink) -> DeepResult {
    let mut r = DeepResult {
        source_values: source.values,
        target_values: target.values,
        series_compared: source.series.len(),
        ..Default::default()
    };
    for (k, s) in &source.series {
        match target.series.get(k) {
            None => {
                r.series_missing += 1;
                push_example(&mut r, k, "missing");
            }
            Some(t) if t != s => {
                r.series_different += 1;
                push_example(&mut r, k, &format!("source {} values, target {}", s.0, t.0));
            }
            Some(_) => {}
        }
    }
    r.series_extra = target
        .series
        .keys()
        .filter(|k| !source.series.contains_key(*k))
        .count();
    r
}

fn push_example(r: &mut DeepResult, key: &[u8], what: &str) {
    if r.examples.len() < 5 {
        r.examples
            .push(format!("{} ({what})", String::from_utf8_lossy(key)));
    }
}

fn scan(
    client: &Client,
    bucket: &str,
    w: Window,
    rules: &Rules,
    scope: &[(String, String)],
    abort: &AtomicBool,
) -> anyhow::Result<HashSink> {
    let q = format!(
        "{} |> drop(columns: [\"_start\", \"_stop\"])",
        flux::source(bucket, w, &[])
    );
    let mut sink = HashSink::default();
    CsvConverter::new(rules, scope).run(client.query(&q)?, &mut sink, abort)?;
    Ok(sink)
}

#[allow(clippy::too_many_arguments)]
pub fn deep_window(
    src: &Client,
    src_bucket: &str,
    tgt: &Client,
    tgt_bucket: &str,
    w: Window,
    rules: &Rules,
    scope: &[(String, String)],
    abort: &AtomicBool,
) -> anyhow::Result<DeepResult> {
    let s = scan(src, src_bucket, w, rules, scope, abort)?;
    let t = scan(tgt, tgt_bucket, w, rules, scope, abort)?;
    Ok(compare(&s, &t))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sink(lines: &str) -> HashSink {
        let mut s = HashSink::default();
        for l in lines.lines() {
            s.buf().extend_from_slice(l.as_bytes());
            s.buf().push(b'\n');
            s.commit(1).unwrap();
        }
        s
    }

    #[test]
    fn equal_data_in_any_order_and_spelling_matches() {
        let a = sink("m,t=a v=1000000.0 1\nm,t=a v=2 2\nm,t\\ x=b s=\"x y\" 3\n");
        let b = sink("m,t\\ x=b s=\"x y\" 3\nm,t=a v=2.0 2\nm,t=a v=1e+06 1\n");
        let r = compare(&a, &b);
        assert_eq!(
            (r.series_missing, r.series_different, r.series_extra),
            (0, 0, 0)
        );
        assert_eq!(r.series_compared, 2);
    }

    #[test]
    fn detects_missing_changed_and_extra() {
        let a = sink("m,t=a v=1 1\nm,t=a v=2 2\nm,t=b v=1 1\nm,t=c v=1 1\n");
        let b = sink("m,t=a v=1 1\nm,t=a v=3 2\nm,t=c v=1 1\nm,t=d v=1 1\n");
        let r = compare(&a, &b);
        assert_eq!(
            (r.series_missing, r.series_different, r.series_extra),
            (1, 1, 1)
        );
        // Same count, different timestamp: still detected.
        let r = compare(&sink("m v=1 1\n"), &sink("m v=1 2\n"));
        assert_eq!(r.series_different, 1);
        // Integer vs float of the same number are different types.
        let r = compare(&sink("m v=1i 1\n"), &sink("m v=1 1\n"));
        assert_eq!(r.series_different, 1);
    }
}
