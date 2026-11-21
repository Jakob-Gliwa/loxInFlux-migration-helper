//! InfluxDB line protocol: serialising points and parsing `export-lp` files.

use crate::convert::FieldValue;
use std::fmt::Write as _;

#[derive(Clone, Debug, PartialEq)]
pub struct Point {
    pub measurement: String,
    /// Sorted by key (InfluxDB sorts them anyway; sorted input is cheaper).
    pub tags: Vec<(String, String)>,
    pub fields: Vec<(String, FieldValue)>,
    /// Nanoseconds since the Unix epoch.
    pub time: i64,
}

impl Point {
    pub fn tag(&self, key: &str) -> Option<&str> {
        self.tags
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
}

fn escape_into(out: &mut String, s: &str, specials: &[char]) {
    for c in s.chars() {
        if specials.contains(&c) {
            out.push('\\');
        }
        out.push(c);
    }
}

/// Append one point as a line (with trailing newline). Points without fields
/// produce nothing.
pub fn write_point(out: &mut String, p: &Point) {
    if p.fields.is_empty() {
        return;
    }
    escape_into(out, &p.measurement, &[',', ' ']);
    for (k, v) in &p.tags {
        if v.is_empty() {
            continue;
        }
        out.push(',');
        escape_into(out, k, &[',', '=', ' ']);
        out.push('=');
        escape_into(out, v, &[',', '=', ' ']);
    }
    out.push(' ');
    for (i, (k, v)) in p.fields.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        escape_into(out, k, &[',', '=', ' ']);
        out.push('=');
        match v {
            FieldValue::Float(f) => out.push_str(ryu::Buffer::new().format_finite(*f)),
            FieldValue::Int(i) => {
                let _ = write!(out, "{i}i");
            }
            FieldValue::UInt(u) => {
                let _ = write!(out, "{u}u");
            }
            FieldValue::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            FieldValue::Str(s) => {
                out.push('"');
                escape_into(out, s, &['"', '\\']);
                out.push('"');
            }
        }
    }
    let _ = writeln!(out, " {}", p.time);
}

#[derive(Debug, PartialEq)]
pub enum ParseError {
    /// The line ends inside a quoted string; the caller should append the
    /// next physical line (string fields may contain raw newlines).
    Unterminated,
    Invalid(String),
}

/// Split at the first unescaped occurrence of `stop` (outside quotes when
/// `quotes` is true). Returns (head, rest-after-stop).
fn split_unescaped<'a>(
    s: &'a str,
    stop: &[char],
    quotes: bool,
) -> Result<(&'a str, Option<&'a str>), ParseError> {
    let mut escaped = false;
    let mut quoted = false;
    for (i, c) in s.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if c == '\\' {
            escaped = true;
            continue;
        }
        if quotes && c == '"' {
            quoted = !quoted;
            continue;
        }
        if !quoted && stop.contains(&c) {
            return Ok((&s[..i], Some(&s[i + c.len_utf8()..])));
        }
    }
    if quoted {
        return Err(ParseError::Unterminated);
    }
    Ok((s, None))
}

fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\'
            && let Some(&n) = chars.peek()
            && matches!(n, ',' | '=' | ' ' | '"' | '\\')
        {
            out.push(n);
            chars.next();
            continue;
        }
        out.push(c);
    }
    out
}

fn parse_value(raw: &str) -> Result<FieldValue, ParseError> {
    let bad = || ParseError::Invalid(format!("invalid field value `{raw}`"));
    if let Some(inner) = raw.strip_prefix('"') {
        let inner = inner.strip_suffix('"').ok_or_else(bad)?;
        return Ok(FieldValue::Str(unescape(inner)));
    }
    match raw {
        "t" | "T" | "true" | "True" | "TRUE" => return Ok(FieldValue::Bool(true)),
        "f" | "F" | "false" | "False" | "FALSE" => return Ok(FieldValue::Bool(false)),
        _ => {}
    }
    if let Some(n) = raw.strip_suffix('i') {
        return n.parse().map(FieldValue::Int).map_err(|_| bad());
    }
    if let Some(n) = raw.strip_suffix('u') {
        return n.parse().map(FieldValue::UInt).map_err(|_| bad());
    }
    raw.parse().map(FieldValue::Float).map_err(|_| bad())
}

/// Parse one logical line. Lines without a timestamp get `default_time`.
pub fn parse_line(line: &str, default_time: i64) -> Result<Point, ParseError> {
    let line = line.trim_end_matches(['\n', '\r']);
    let (series, rest) = split_unescaped(line, &[' '], false)?;
    let rest = rest.ok_or_else(|| ParseError::Invalid("missing fields".into()))?;
    // Fields end at the first unescaped space outside quotes.
    let (fields_raw, time_raw) = split_unescaped(rest, &[' '], true)?;

    let mut parts = Vec::new();
    let mut s = series;
    loop {
        let (head, tail) = split_unescaped(s, &[','], false)?;
        parts.push(head);
        match tail {
            Some(t) => s = t,
            None => break,
        }
    }
    let measurement = unescape(parts[0]);
    if measurement.is_empty() {
        return Err(ParseError::Invalid("empty measurement".into()));
    }
    let mut tags = Vec::with_capacity(parts.len() - 1);
    for kv in &parts[1..] {
        let (k, v) = split_unescaped(kv, &['='], false)?;
        let v = v.ok_or_else(|| ParseError::Invalid(format!("tag without value `{kv}`")))?;
        tags.push((unescape(k), unescape(v)));
    }
    tags.sort();

    let mut fields = Vec::new();
    let mut s = fields_raw;
    loop {
        let (kv, tail) = split_unescaped(s, &[','], true)?;
        let (k, v) = split_unescaped(kv, &['='], false)?;
        let v = v.ok_or_else(|| ParseError::Invalid(format!("field without value `{kv}`")))?;
        fields.push((unescape(k), parse_value(v)?));
        match tail {
            Some(t) => s = t,
            None => break,
        }
    }

    let time = match time_raw.map(str::trim) {
        None | Some("") => default_time,
        Some(t) => t
            .parse()
            .map_err(|_| ParseError::Invalid(format!("invalid timestamp `{t}`")))?,
    };
    Ok(Point {
        measurement,
        tags,
        fields,
        time,
    })
}

/// Iterates over the points of a line protocol stream. Comment lines (`#`)
/// and blank lines are skipped; strings with embedded newlines are joined.
pub struct Reader<R> {
    inner: R,
    line_no: u64,
    buf: String,
}

#[derive(Debug)]
pub struct LineError {
    pub line_no: u64,
    pub message: String,
}

impl<R: std::io::BufRead> Reader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            line_no: 0,
            buf: String::new(),
        }
    }
}

impl<R: std::io::BufRead> Iterator for Reader<R> {
    type Item = Result<Point, LineError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.buf.clear();
            let start = self.line_no + 1;
            match self.inner.read_line(&mut self.buf) {
                Ok(0) => return None,
                Ok(_) => self.line_no += 1,
                Err(e) => {
                    return Some(Err(LineError {
                        line_no: start,
                        message: e.to_string(),
                    }));
                }
            }
            let trimmed = self.buf.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            loop {
                match parse_line(&self.buf, 0) {
                    Ok(p) => return Some(Ok(p)),
                    Err(ParseError::Unterminated) => match self.inner.read_line(&mut self.buf) {
                        Ok(0) => {
                            return Some(Err(LineError {
                                line_no: start,
                                message: "unterminated string at end of input".into(),
                            }));
                        }
                        Ok(_) => self.line_no += 1,
                        Err(e) => {
                            return Some(Err(LineError {
                                line_no: start,
                                message: e.to_string(),
                            }));
                        }
                    },
                    Err(ParseError::Invalid(message)) => {
                        return Some(Err(LineError {
                            line_no: start,
                            message,
                        }));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(p: &Point) -> String {
        let mut s = String::new();
        write_point(&mut s, p);
        s
    }

    #[test]
    fn roundtrip_with_escaping_and_all_types() {
        let p = Point {
            measurement: "Energy Heating,x".into(),
            tags: vec![
                ("room".into(), "Bath, Up=1".into()),
                ("uuid".into(), "1a".into()),
            ],
            fields: vec![
                ("AQ".into(), FieldValue::Float(21.2)),
                ("n".into(), FieldValue::Int(-3)),
                ("u".into(), FieldValue::UInt(4)),
                ("b".into(), FieldValue::Bool(true)),
                (
                    "__text__Default".into(),
                    FieldValue::Str("a \"b\" \\ c,d=e".into()),
                ),
            ],
            time: 1700000000555555555,
        };
        let l = line(&p);
        assert_eq!(
            l,
            "Energy\\ Heating\\,x,room=Bath\\,\\ Up\\=1,uuid=1a AQ=21.2,n=-3i,u=4u,b=true,__text__Default=\"a \\\"b\\\" \\\\ c,d=e\" 1700000000555555555\n"
        );
        assert_eq!(parse_line(&l, 0).unwrap(), p);
    }

    #[test]
    fn whole_floats_keep_a_decimal_point() {
        let p = Point {
            measurement: "m".into(),
            tags: vec![],
            fields: vec![("v".into(), FieldValue::Float(42.0))],
            time: 1,
        };
        assert_eq!(line(&p), "m v=42.0 1\n");
    }

    #[test]
    fn parses_export_lp_lines() {
        let p = parse_line(
            r#"Vent_Alarm,analog=1,category=Not\ assigned,room=Hall AQ=0i 1700000000123456789"#,
            0,
        )
        .unwrap();
        assert_eq!(p.measurement, "Vent_Alarm");
        assert_eq!(p.tag("category"), Some("Not assigned"));
        assert_eq!(p.fields, vec![("AQ".into(), FieldValue::Int(0))]);
        let p = parse_line(r#"T Default="19.6°" 5"#, 0).unwrap();
        assert_eq!(
            p.fields,
            vec![("Default".into(), FieldValue::Str("19.6°".into()))]
        );
        let p = parse_line("m a=1,b=t,c=F", 9).unwrap();
        assert_eq!(p.time, 9);
        assert_eq!(p.fields[1].1, FieldValue::Bool(true));
    }

    #[test]
    fn reader_skips_comments_and_joins_multiline_strings() {
        let input = "# DDL\n\nm s=\"a\nb\" 1\nm v=2 2\nbroken\n";
        let mut r = Reader::new(std::io::Cursor::new(input));
        let p = r.next().unwrap().unwrap();
        assert_eq!(p.fields[0].1, FieldValue::Str("a\nb".into()));
        assert_eq!(r.next().unwrap().unwrap().time, 2);
        let e = r.next().unwrap().unwrap_err();
        assert_eq!(e.line_no, 6);
        assert!(r.next().is_none());
    }

    #[test]
    fn rejects_garbage() {
        assert!(matches!(
            parse_line("m v=abc 1", 0),
            Err(ParseError::Invalid(_))
        ));
        assert!(matches!(
            parse_line("m,t v=1 1", 0),
            Err(ParseError::Invalid(_))
        ));
        assert_eq!(parse_line("m v=\"open 1", 0), Err(ParseError::Unterminated));
    }
}
