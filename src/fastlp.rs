//! Line protocol (e.g. `influxd inspect export-lp`) -> converted line protocol.
//!
//! Zero-copy fast path: a line whose fields need no change (floats, existing
//! `__text__` fields, or points outside the conversion scope) is copied
//! verbatim. Only lines with integers, booleans or strings are rewritten, and
//! even then the series key and timestamp are copied as bytes.

use crate::convert::{Action, Rules, TEXT_PREFIX, TextClass, classify_text};
use crate::fast::{Counters, KEY_SPECIALS, T_BOOL, T_FLOAT, T_INT, T_STRING, T_UINT, escape_into};
use std::collections::HashMap;

/// Longest accepted logical line (strings may span physical lines).
pub const MAX_LINE_BYTES: usize = 4 << 20;

#[derive(Debug, PartialEq)]
pub enum LineError {
    /// Ends inside a quoted string: append the next physical line and retry.
    Unterminated,
    Invalid(&'static str),
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
    Float,
    Int,
    UInt,
    Bool(bool),
    Str,
}

#[derive(Clone, Copy)]
struct Field {
    key: (usize, usize),
    val: (usize, usize),
    kind: Kind,
}

/// Written values per (window, measurement), counted in runs.
#[derive(Default)]
pub struct WindowCounts {
    pub ids: HashMap<Vec<u8>, u32>,
    pub names: Vec<String>,
    pub counts: HashMap<(i64, u32), u64>,
    cur_meas: Vec<u8>,
    cur_id: u32,
    cur_win: i64,
    run: u64,
}

impl WindowCounts {
    #[inline]
    fn add(&mut self, measurement: &[u8], win: i64, n: u64) {
        if measurement != self.cur_meas.as_slice() {
            self.flush();
            self.cur_meas.clear();
            self.cur_meas.extend_from_slice(measurement);
            let next = self.names.len() as u32;
            self.cur_id = *self.ids.entry(measurement.to_vec()).or_insert_with(|| {
                self.names.push(unescape_key(measurement));
                next
            });
        } else if win != self.cur_win {
            self.flush();
        }
        self.cur_win = win;
        self.run += n;
    }

    pub fn flush(&mut self) {
        if self.run > 0 {
            *self.counts.entry((self.cur_win, self.cur_id)).or_default() += self.run;
            self.run = 0;
        }
    }
}

fn unescape_key(s: &[u8]) -> String {
    let mut out = Vec::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        if s[i] == b'\\' && i + 1 < s.len() && matches!(s[i + 1], b',' | b'=' | b' ' | b'\\') {
            i += 1;
        }
        out.push(s[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub struct LpConverter<'a> {
    rules: &'a Rules,
    /// Escaped `,key=value` needles for --convert-where-tag (SIMD substring search).
    needles: Vec<memchr::memmem::Finder<'static>>,
    window_ns: i64,
    pub counters: Counters,
    pub windows: WindowCounts,
    fields: Vec<Field>,
    text: String,
}

impl<'a> LpConverter<'a> {
    pub fn new(rules: &'a Rules, scope: &[(String, String)], window_secs: i64) -> Self {
        let needles = scope
            .iter()
            .map(|(k, v)| {
                let mut n = vec![b','];
                escape_into(&mut n, k.as_bytes(), KEY_SPECIALS);
                n.push(b'=');
                escape_into(&mut n, v.as_bytes(), KEY_SPECIALS);
                memchr::memmem::Finder::new(&n).into_owned()
            })
            .collect();
        Self {
            rules,
            needles,
            window_ns: window_secs * 1_000_000_000,
            counters: Counters::default(),
            windows: WindowCounts::default(),
            fields: Vec::with_capacity(8),
            text: String::new(),
        }
    }

    fn in_scope(&self, series: &[u8]) -> bool {
        if self.needles.is_empty() {
            return true;
        }
        self.needles.iter().any(|n| {
            let mut from = 0;
            while let Some(pos) = n.find(&series[from..]) {
                let end = from + pos + n.needle().len();
                // Match a whole tag: next byte must end the tag value.
                if end == series.len() || series[end] == b',' {
                    // ... and the needle's comma must not be escaped.
                    let start = from + pos;
                    if start == 0 || series[start - 1] != b'\\' {
                        return true;
                    }
                }
                from += pos + 1;
            }
            false
        })
    }

    /// Convert one logical line (without trailing newline). Returns the number
    /// of values written to `out` (0 if all were dropped or the line was blank).
    pub fn process(&mut self, line: &[u8], out: &mut Vec<u8>) -> Result<u64, LineError> {
        let line = trim_end(line);
        if line.is_empty() || line[0] == b'#' {
            return Ok(0);
        }
        // Series key: up to the first unescaped space. memchr jumps (SIMD)
        // from one candidate byte to the next instead of looking at each.
        let (meas_end, i) = scan_series(line).ok_or(LineError::Invalid("missing fields"))?;
        let series = &line[..i];
        let measurement = &series[..meas_end];
        let fstart = i + 1;

        // Fields: split on unescaped commas outside quotes, end at the first
        // unescaped space outside quotes.
        self.fields.clear();
        let mut j = fstart;
        let mut key_start = fstart;
        let mut eq = None;
        let mut quoted = false;
        loop {
            let at_end = j >= line.len();
            let b = if at_end { b' ' } else { line[j] };
            if !at_end && b == b'\\' {
                j += 2;
                continue;
            }
            if quoted {
                if at_end {
                    return Err(LineError::Unterminated);
                }
                if b == b'"' {
                    quoted = false;
                }
                j += 1;
                continue;
            }
            match b {
                b'"' => quoted = true,
                b'=' if eq.is_none() => eq = Some(j),
                b',' | b' ' => {
                    let e = eq.ok_or(LineError::Invalid("field without value"))?;
                    let val = (e + 1, j.min(line.len()));
                    let kind = kind_of(&line[val.0..val.1])?;
                    self.fields.push(Field {
                        key: (key_start, e),
                        val,
                        kind,
                    });
                    eq = None;
                    if b == b' ' {
                        break;
                    }
                    key_start = j + 1;
                }
                _ => {}
            }
            j += 1;
        }
        let fields_end = j.min(line.len());
        let ts = if fields_end < line.len() {
            trim(&line[fields_end + 1..])
        } else {
            b"" as &[u8]
        };
        let time = parse_i64(ts).ok_or(LineError::Invalid("missing or invalid timestamp"))?;
        let win = time.div_euclid(self.window_ns);
        let in_scope = self.in_scope(series);

        let needs_rewrite = in_scope
            && self.fields.iter().any(|f| match f.kind {
                Kind::Float => false,
                Kind::Str => !line[f.key.0..f.key.1].starts_with(TEXT_PREFIX.as_bytes()),
                _ => true,
            });
        if !needs_rewrite {
            let n = self.fields.len() as u64;
            for f in &self.fields {
                let (ty, action) = match (in_scope, f.kind) {
                    (false, k) => (type_index(k), Action::CopiedUnchanged),
                    (true, Kind::Float) => (T_FLOAT, Action::FloatKept),
                    (true, k) => (type_index(k), Action::TextFieldKept),
                };
                self.counters.record(ty, action);
            }
            out.extend_from_slice(line);
            out.push(b'\n');
            self.windows.add(measurement, win, n);
            return Ok(n);
        }

        let mark = out.len();
        out.extend_from_slice(series);
        out.push(b' ');
        let mut written = 0u64;
        for k in 0..self.fields.len() {
            let f = self.fields[k];
            let key = &line[f.key.0..f.key.1];
            let val = &line[f.val.0..f.val.1];
            let sep = |out: &mut Vec<u8>, written: u64| {
                if written > 0 {
                    out.push(b',');
                }
            };
            let (ty, action) = match f.kind {
                Kind::Float => {
                    sep(out, written);
                    push_kv(out, key, val);
                    (T_FLOAT, Action::FloatKept)
                }
                Kind::Int | Kind::UInt => {
                    sep(out, written);
                    push_kv(out, key, &val[..val.len() - 1]);
                    let a = if f.kind == Kind::Int {
                        Action::IntToFloat
                    } else {
                        Action::UnsignedToFloat
                    };
                    (type_index(f.kind), a)
                }
                Kind::Bool(b) => {
                    sep(out, written);
                    if self.rules.bool_to_float {
                        push_kv(out, key, if b { b"1" } else { b"0" });
                        (T_BOOL, Action::BoolToFloat)
                    } else {
                        push_kv(out, key, val);
                        (T_BOOL, Action::BoolKept)
                    }
                }
                Kind::Str if key.starts_with(TEXT_PREFIX.as_bytes()) => {
                    sep(out, written);
                    push_kv(out, key, val);
                    (T_STRING, Action::TextFieldKept)
                }
                Kind::Str => {
                    unescape_string(&val[1..val.len() - 1], &mut self.text);
                    match classify_text(&self.text, self.rules) {
                        TextClass::Drop(a) => {
                            self.counters.record(T_STRING, a);
                            continue;
                        }
                        TextClass::Number(n, a) => {
                            sep(out, written);
                            push_kv(out, key, ryu::Buffer::new().format_finite(n).as_bytes());
                            (T_STRING, a)
                        }
                        TextClass::Literal if self.rules.separate_text_fields => {
                            sep(out, written);
                            out.extend_from_slice(TEXT_PREFIX.as_bytes());
                            push_kv(out, key, val);
                            (T_STRING, Action::StringToTextField)
                        }
                        TextClass::Literal => {
                            sep(out, written);
                            push_kv(out, key, val);
                            (T_STRING, Action::StringKept)
                        }
                    }
                }
            };
            self.counters.record(ty, action);
            written += 1;
        }
        if written == 0 {
            out.truncate(mark);
            return Ok(0);
        }
        out.push(b' ');
        out.extend_from_slice(ts);
        out.push(b'\n');
        self.windows.add(measurement, win, written);
        Ok(written)
    }
}

fn type_index(k: Kind) -> usize {
    match k {
        Kind::Float => T_FLOAT,
        Kind::Int => T_INT,
        Kind::UInt => T_UINT,
        Kind::Bool(_) => T_BOOL,
        Kind::Str => T_STRING,
    }
}

#[inline]
fn push_kv(out: &mut Vec<u8>, key: &[u8], val: &[u8]) {
    out.extend_from_slice(key);
    out.push(b'=');
    out.extend_from_slice(val);
}

fn kind_of(v: &[u8]) -> Result<Kind, LineError> {
    let bad = LineError::Invalid("invalid field value");
    match v {
        [] => Err(bad),
        [b'"', .., b'"'] if v.len() >= 2 => Ok(Kind::Str),
        b"t" | b"T" | b"true" | b"True" | b"TRUE" => Ok(Kind::Bool(true)),
        b"f" | b"F" | b"false" | b"False" | b"FALSE" => Ok(Kind::Bool(false)),
        [.., b'i'] if is_int(&v[..v.len() - 1]) => Ok(Kind::Int),
        [.., b'u'] if is_int(&v[..v.len() - 1]) => Ok(Kind::UInt),
        _ if v
            .iter()
            .all(|b| b.is_ascii_digit() || matches!(b, b'.' | b'-' | b'+' | b'e' | b'E')) =>
        {
            Ok(Kind::Float)
        }
        _ => Err(bad),
    }
}

fn is_int(s: &[u8]) -> bool {
    let s = s.strip_prefix(b"-").unwrap_or(s);
    !s.is_empty() && s.iter().all(u8::is_ascii_digit)
}

fn parse_i64(s: &[u8]) -> Option<i64> {
    std::str::from_utf8(s).ok()?.parse().ok()
}

fn unescape_string(s: &[u8], out: &mut String) {
    out.clear();
    let mut buf = Vec::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        if s[i] == b'\\' && i + 1 < s.len() && matches!(s[i + 1], b'"' | b'\\') {
            i += 1;
        }
        buf.push(s[i]);
        i += 1;
    }
    out.push_str(&String::from_utf8_lossy(&buf));
}

fn trim_end(s: &[u8]) -> &[u8] {
    let mut e = s.len();
    while e > 0 && matches!(s[e - 1], b'\n' | b'\r') {
        e -= 1;
    }
    &s[..e]
}

fn trim(s: &[u8]) -> &[u8] {
    let s = trim_end(s);
    let mut a = 0;
    let mut e = s.len();
    while a < e && s[a] == b' ' {
        a += 1;
    }
    while e > a && s[e - 1] == b' ' {
        e -= 1;
    }
    &s[a..e]
}

/// (end of measurement, end of series key): the first unescaped `,` and the
/// first unescaped space. `None` if there is no space (no fields).
#[inline]
fn scan_series(line: &[u8]) -> Option<(usize, usize)> {
    let mut meas_end = None;
    let mut i = 0;
    while i < line.len() {
        let hit = if meas_end.is_none() {
            memchr::memchr3(b'\\', b',', b' ', &line[i..])
        } else {
            memchr::memchr2(b'\\', b' ', &line[i..])
        }?;
        let p = i + hit;
        match line[p] {
            b'\\' => i = p + 2,
            b',' => {
                meas_end = Some(p);
                i = p + 1;
            }
            _ => return Some((meas_end.unwrap_or(p), p)),
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::convert::{FieldValue, convert};
    use crate::lp::{parse_line, write_point};

    const INPUT: &[&str] = &[
        r#"Vent_Alarm,application=loxInFlux,category=Not\ assigned,room=Hall AQ=0i 1700000000123456789"#,
        r#"Temperatur,application=loxInFlux,room=Bad Default="19.6°" 1700000000000000000"#,
        r#"Temperatur,application=loxInFlux,room=Bad Default="Ein, \"x\" y=1" 1700000000000000001"#,
        r#"Temperatur,application=loxInFlux,room=Bad Default="" 1700000000000000002"#,
        r#"Temperatur,application=loxInFlux,room=Bad Default="<v.1> °C" 1700000000000000003"#,
        r#"Temperatur,application=loxInFlux,room=Bad AQ=21.25 1700000000000000004"#,
        r#"Temperatur,application=loxInFlux,room=Bad __text__Default="Aus" 1700000000000000005"#,
        r#"Alarm,application=loxInFlux on=true,n=5u,AQ=1.5,s="12:30" 1700000000000000006"#,
        r#"Energy\ Heating\,x,application=loxInFlux ORd=0.25 1700000000987654321"#,
        r#"zigbee,application=z2m state=1i,txt="x" 1700000000000000007"#,
        r#"zigbee,application=z2m\,application=loxInFlux state=2i 1700000000000000008"#,
    ];

    /// Differential test against parse_line -> convert -> write_point.
    #[test]
    fn fast_path_equals_reference_path() {
        let rules = Rules::default();
        let scope = vec![("application".to_owned(), "loxInFlux".to_owned())];
        let mut c = LpConverter::new(&rules, &scope, 86_400);
        let mut out = Vec::new();
        let mut written = 0;
        for l in INPUT {
            written += c.process(l.as_bytes(), &mut out).unwrap();
        }
        let fast: Vec<_> = String::from_utf8(out)
            .unwrap()
            .lines()
            .map(|l| parse_line(l, 0).unwrap())
            .collect();

        let mut reference = Vec::new();
        for l in INPUT {
            let mut p = parse_line(l, 0).unwrap();
            let in_scope = p.tag("application") == Some("loxInFlux");
            let fields = std::mem::take(&mut p.fields);
            for (f, v) in fields {
                let o = if in_scope {
                    convert(&f, v, &rules).output
                } else {
                    Some((f, v))
                };
                p.fields.extend(o);
            }
            if !p.fields.is_empty() {
                let mut s = String::new();
                write_point(&mut s, &p);
                reference.push(parse_line(s.trim_end(), 0).unwrap());
            }
        }
        assert_eq!(fast, reference);
        assert_eq!(written, c.counters.values_written);
        assert_eq!(c.counters.values_read, 15);
        assert_eq!(c.counters.values_dropped, 2);
        // The escaped comma in the tag value must not count as a scope match.
        assert!(
            fast.iter()
                .any(|p| p.fields[0] == ("state".into(), FieldValue::Int(2)))
        );
    }

    #[test]
    fn untouched_lines_are_copied_verbatim() {
        let rules = Rules::default();
        let mut c = LpConverter::new(&rules, &[], 86_400);
        let mut out = Vec::new();
        let l = r#"Energy\ Heating,room=Bath\ Up ORd=0.25,x=1e+06 1700000000987654321"#;
        c.process(l.as_bytes(), &mut out).unwrap();
        assert_eq!(out, format!("{l}\n").into_bytes());
    }

    #[test]
    fn counts_per_window_and_measurement() {
        let rules = Rules::default();
        let mut c = LpConverter::new(&rules, &[], 86_400);
        let mut out = Vec::new();
        for l in [
            "a v=1 0",
            "a v=1 1",
            "b v=1 2",
            "a v=1 86400000000000",
            "a\\ b v=1i,w=2i 3",
        ] {
            c.process(l.as_bytes(), &mut out).unwrap();
        }
        c.windows.flush();
        let id = |n: &str| c.windows.names.iter().position(|x| x == n).unwrap() as u32;
        assert_eq!(c.windows.counts[&(0, id("a"))], 2);
        assert_eq!(c.windows.counts[&(1, id("a"))], 1);
        assert_eq!(c.windows.counts[&(0, id("b"))], 1);
        assert_eq!(c.windows.counts[&(0, id("a b"))], 2);
    }

    #[test]
    fn errors() {
        let rules = Rules::default();
        let mut c = LpConverter::new(&rules, &[], 86_400);
        let mut out = Vec::new();
        assert_eq!(
            c.process(b"m s=\"open 1", &mut out),
            Err(LineError::Unterminated)
        );
        assert!(matches!(
            c.process(b"m", &mut out),
            Err(LineError::Invalid(_))
        ));
        assert!(matches!(
            c.process(b"m v=1", &mut out),
            Err(LineError::Invalid(_))
        ));
        assert!(matches!(
            c.process(b"m v=abc 1", &mut out),
            Err(LineError::Invalid(_))
        ));
        assert_eq!(c.process(b"# comment", &mut out), Ok(0));
        assert!(out.is_empty());
    }
}
