//! `verify --deep`: value-level comparison of source and target.
//!
//! Both sides of a window are streamed through the same converter (same rules
//! and scope as the migration; conversion is idempotent, so already migrated
//! target data comes out unchanged). Per series and field we keep only a count
//! and an order-independent checksum over (timestamp, value), so memory stays
//! proportional to the number of series, not the number of values.

use crate::convert::Rules;
use crate::fastcsv::CsvConverter;
use crate::fastlp::unescape_key;
use crate::flux;
use crate::influx::Client;
use crate::pipeline::LineSink;
use crate::window::Window;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::AtomicBool;

/// Accumulates `series,tags field` -> (count, wrapping sum of value hashes).
///
/// Both readers deliver one series at a time (a Flux table, a TSM key), so
/// the running series is kept aside and only touches the map when the key
/// changes. Call [`HashSink::flush`] before reading `series`.
#[derive(Default)]
pub struct HashSink {
    buf: Vec<u8>,
    key: Vec<u8>,
    last: Vec<u8>,
    pending: (u64, u64),
    pub series: HashMap<Vec<u8>, (u64, u64)>,
    pub values: u64,
}

/// splitmix64 finalizer.
fn mix(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn bytes_hash(b: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325 ^ b.len() as u64;
    for chunk in b.chunks(8) {
        let mut w = [0u8; 8];
        w[..chunk.len()].copy_from_slice(chunk);
        h = mix(h ^ u64::from_le_bytes(w));
    }
    h
}

fn value_hash(time: &[u8], value: &[u8]) -> u64 {
    let t = std::str::from_utf8(time)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .map_or_else(|| bytes_hash(time), |t| t as u64);
    // Numbers are compared by value, not by spelling (`1e+06` == `1000000.0`).
    let v = if value.first() != Some(&b'"')
        && !matches!(value.last(), Some(b'i' | b'u'))
        && let Some(f) = std::str::from_utf8(value)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
    {
        mix(f.to_bits() ^ 0x5bd1_e995_0000_0001)
    } else {
        bytes_hash(value)
    };
    mix(mix(t) ^ v.rotate_left(17))
}

/// Calls `f(series_field_key, value_hash)` for every field of a line
/// `<series> <field>=<value>[,<field>=<value>...] <time>`.
fn for_each_field(line: &[u8], key: &mut Vec<u8>, mut f: impl FnMut(&[u8], u64)) {
    let mut i = 0;
    while i < line.len() && line[i] != b' ' {
        i += if line[i] == b'\\' { 2 } else { 1 };
    }
    let Some(sp) = line.iter().rposition(|b| *b == b' ') else {
        return;
    };
    if sp <= i {
        return;
    }
    let time = &line[sp + 1..];
    let fields = &line[i + 1..sp];
    let (mut j, mut start, mut eq, mut quoted) = (0, 0, None, false);
    while j <= fields.len() {
        let b = fields.get(j).copied().unwrap_or(b',');
        if j < fields.len() && b == b'\\' {
            j += 2;
            continue;
        }
        if quoted {
            quoted = b != b'"';
        } else if b == b'"' {
            quoted = true;
        } else if b == b'=' && eq.is_none() {
            eq = Some(j);
        } else if b == b',' {
            if let Some(e) = eq {
                key.clear();
                key.extend_from_slice(&line[..=i]);
                key.extend_from_slice(&fields[start..e]);
                f(key, value_hash(time, &fields[e + 1..j.min(fields.len())]));
            }
            start = j + 1;
            eq = None;
        }
        j += 1;
    }
}

fn for_each_line(buf: &[u8], key: &mut Vec<u8>, mut f: impl FnMut(&[u8], u64)) {
    for line in buf.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        for_each_field(line, key, &mut f);
    }
}

impl HashSink {
    fn absorb(&mut self) {
        let (buf, mut key) = (std::mem::take(&mut self.buf), std::mem::take(&mut self.key));
        for_each_line(&buf, &mut key, |k, h| self.add(k, h));
        (self.buf, self.key) = (buf, key);
        self.buf.clear();
    }

    fn add(&mut self, key: &[u8], h: u64) {
        if self.last != key {
            self.flush();
            self.last.extend_from_slice(key);
        }
        self.pending.0 += 1;
        self.pending.1 = self.pending.1.wrapping_add(h);
    }

    /// Move the running series into the map.
    pub fn flush(&mut self) {
        if self.pending.0 > 0 {
            let (c, h) = std::mem::take(&mut self.pending);
            match self.series.get_mut(self.last.as_slice()) {
                Some(e) => {
                    e.0 += c;
                    e.1 = e.1.wrapping_add(h);
                }
                None => {
                    self.series.insert(self.last.clone(), (c, h));
                }
            }
        }
        self.last.clear();
    }

    /// Values per (unescaped) measurement name.
    pub fn by_measurement(&self) -> BTreeMap<String, u64> {
        debug_assert!(self.pending.0 == 0, "flush() first");
        let mut out: BTreeMap<String, u64> = BTreeMap::new();
        for (k, (c, _)) in &self.series {
            let mut i = 0;
            while i < k.len() && k[i] != b',' && k[i] != b' ' {
                i += if k[i] == b'\\' { 2 } else { 1 };
            }
            *out.entry(unescape_key(&k[..i.min(k.len())])).or_default() += c;
        }
        out
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

/// Checksums of many consecutive windows from one pass over a bucket
/// (export-lp prints a bucket key by key, each key's values in time order).
/// Keys are stored once per chunk; each window keeps only key ids, so a
/// year of day windows costs a few tens of MB.
#[derive(Default)]
pub struct ChunkSink {
    /// Window that `commit` assigns the buffered lines to.
    pub current: usize,
    buf: Vec<u8>,
    key: Vec<u8>,
    ids: HashMap<Vec<u8>, u32>,
    keys: Vec<Vec<u8>>,
    per_window: Vec<HashMap<u32, (u64, u64)>>,
    values: Vec<u64>,
    last_key: Vec<u8>,
    last_id: u32,
    run: Option<(usize, u32)>,
    pending: (u64, u64),
}

impl ChunkSink {
    pub fn new(windows: usize) -> Self {
        Self {
            per_window: (0..windows).map(|_| HashMap::new()).collect(),
            values: vec![0; windows],
            ..Default::default()
        }
    }

    fn add(&mut self, key: &[u8], h: u64) {
        if self.last_key != key || self.keys.is_empty() {
            self.last_id = match self.ids.get(key) {
                Some(id) => *id,
                None => {
                    let id = self.keys.len() as u32;
                    self.keys.push(key.to_vec());
                    self.ids.insert(key.to_vec(), id);
                    id
                }
            };
            self.last_key.clear();
            self.last_key.extend_from_slice(key);
        }
        let at = (self.current, self.last_id);
        if self.run != Some(at) {
            self.flush();
            self.run = Some(at);
        }
        self.pending.0 += 1;
        self.pending.1 = self.pending.1.wrapping_add(h);
    }

    pub fn flush(&mut self) {
        if let Some((w, id)) = self.run.take() {
            let (c, h) = std::mem::take(&mut self.pending);
            let e = self.per_window[w].entry(id).or_default();
            e.0 += c;
            e.1 = e.1.wrapping_add(h);
        }
    }

    /// One window as a plain sink (call `flush` first).
    pub fn window(&mut self, i: usize) -> HashSink {
        debug_assert!(self.run.is_none(), "flush() first");
        let series = std::mem::take(&mut self.per_window[i])
            .into_iter()
            .map(|(id, v)| (self.keys[id as usize].clone(), v))
            .collect();
        HashSink {
            series,
            values: self.values[i],
            ..Default::default()
        }
    }
}

impl LineSink for ChunkSink {
    fn buf(&mut self) -> &mut Vec<u8> {
        &mut self.buf
    }
    fn commit(&mut self, values: u64) -> anyhow::Result<()> {
        self.values[self.current] += values;
        let (buf, mut key) = (std::mem::take(&mut self.buf), std::mem::take(&mut self.key));
        for_each_line(&buf, &mut key, |k, h| self.add(k, h));
        (self.buf, self.key) = (buf, key);
        self.buf.clear();
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
    debug_assert!(
        source.pending.0 == 0 && target.pending.0 == 0,
        "flush() first"
    );
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

/// Read one window of a bucket through Flux into a checksum sink.
pub fn scan(
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
    sink.flush();
    Ok(sink)
}

/// Result of one deep window: the comparison plus the target's values per
/// measurement (replaces the separate count query).
pub struct DeepWindow {
    pub result: DeepResult,
    pub target_by_measurement: BTreeMap<String, u64>,
}

pub fn finish(source: &HashSink, target: &HashSink) -> DeepWindow {
    DeepWindow {
        result: compare(source, target),
        target_by_measurement: target.by_measurement(),
    }
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
) -> anyhow::Result<DeepWindow> {
    let s = scan(src, src_bucket, w, rules, scope, abort)?;
    let t = scan(tgt, tgt_bucket, w, rules, scope, abort)?;
    Ok(finish(&s, &t))
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
        s.flush();
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

    #[test]
    fn multi_field_lines_equal_single_field_lines() {
        let a = sink("m,t=a v=1,s=\"x, y=z\",w=2i 5\n");
        let b = sink("m,t=a w=2i 5\nm,t=a s=\"x, y=z\" 5\nm,t=a v=1.0 5\n");
        let r = compare(&a, &b);
        assert_eq!(
            (
                r.series_compared,
                r.series_missing,
                r.series_different,
                r.series_extra
            ),
            (3, 0, 0, 0)
        );
    }

    #[test]
    fn chunk_sink_matches_per_window_sinks() {
        let lines = [
            (0, "m,t=a v=1 1"),
            (0, "m,t=a v=2 2"),
            (1, "m,t=a v=3 30"),
            (1, "m,t=b v=1,w=\"x\" 31"),
            (0, "m,t=b v=5 3"),
            (1, "m,t=a v=4 32"),
        ];
        let mut c = ChunkSink::new(2);
        let mut per = [HashSink::default(), HashSink::default()];
        for (w, l) in lines {
            c.current = w;
            c.buf().extend_from_slice(l.as_bytes());
            c.buf().push(b'\n');
            c.commit(1).unwrap();
            per[w].buf().extend_from_slice(l.as_bytes());
            per[w].buf().push(b'\n');
            per[w].commit(1).unwrap();
        }
        c.flush();
        for (w, p) in per.iter_mut().enumerate() {
            p.flush();
            let got = c.window(w);
            assert_eq!(got.series, p.series);
            assert_eq!(got.values, p.values);
        }
    }

    #[test]
    fn counts_per_measurement_unescaped() {
        let s = sink("m\\ x,t=a v=1 1\nm\\ x v=2,w=3 2\nn v=1 1\n");
        let m = s.by_measurement();
        assert_eq!(m.get("m x"), Some(&3));
        assert_eq!(m.get("n"), Some(&1));
    }
}
