//! Flux annotated CSV -> converted line protocol, without per-row allocation.
//!
//! A Flux table is one series and one field, so everything except the value
//! and the timestamp is constant within a table. The line prefix
//! (`measurement,tags field=`) is escaped once per table; each row then only
//! appends the value and the timestamp. Numeric values are copied as bytes:
//! Flux prints doubles and integers in a form that line protocol parses as a
//! float (`7`, `21.5`, `1e+06`), which is exactly the integer -> float
//! conversion we want.

use crate::convert::{Action, Rules, TEXT_PREFIX, TextClass, classify_text};
use crate::error::{Kind, fail};
use crate::fast::{
    Counters, KEY_SPECIALS, MEASUREMENT_SPECIALS, MeasurementRuns, STRING_SPECIALS, T_BOOL,
    T_FLOAT, T_INT, T_STRING, T_UINT, escape_into, parse_time_ns,
};
use crate::pipeline::LineSink;
use csv::ByteRecord;
use std::io::Read;
use std::sync::atomic::{AtomicBool, Ordering};

/// Rows larger than this are rejected instead of buffered.
pub const MAX_RECORD_BYTES: usize = 4 << 20;

#[derive(Clone, Copy, PartialEq)]
enum VType {
    Double,
    Long,
    ULong,
    Bool,
    Str,
}

struct Layout {
    time: usize,
    value: usize,
    field: usize,
    measurement: usize,
    table: usize,
    /// Tag columns: index, escaped key, raw key.
    tags: Vec<(usize, Vec<u8>, String)>,
    vtype: VType,
}

#[derive(Default)]
struct Table {
    id: Vec<u8>,
    measurement: Vec<u8>,
    /// `measurement,tags field=`
    prefix: Vec<u8>,
    /// `measurement,tags __text__field=`
    text_prefix: Vec<u8>,
    field_is_text: bool,
    in_scope: bool,
}

pub struct CsvConverter<'a> {
    pub rules: &'a Rules,
    pub scope: &'a [(String, String)],
    pub counters: Counters,
    pub runs: MeasurementRuns,
    text: String,
}

const RESERVED: [&[u8]; 9] = [
    b"",
    b"result",
    b"table",
    b"_start",
    b"_stop",
    b"_time",
    b"_value",
    b"_field",
    b"_measurement",
];

impl<'a> CsvConverter<'a> {
    pub fn new(rules: &'a Rules, scope: &'a [(String, String)]) -> Self {
        Self {
            rules,
            scope,
            counters: Counters::default(),
            runs: MeasurementRuns::default(),
            text: String::new(),
        }
    }

    pub fn run<R: Read>(
        &mut self,
        input: R,
        sink: &mut impl LineSink,
        abort: &AtomicBool,
    ) -> anyhow::Result<()> {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .buffer_capacity(256 << 10)
            .from_reader(input);
        let mut rec = ByteRecord::new();
        let mut types: Vec<Vec<u8>> = Vec::new();
        let mut layout: Option<Layout> = None;
        let mut expect_header = false;
        let mut table = Table::default();
        let mut rows: u64 = 0;
        while rdr
            .read_byte_record(&mut rec)
            .map_err(crate::flux::read_error)?
        {
            let first = rec.get(0).unwrap_or(b"");
            if first.first() == Some(&b'#') {
                if first == b"#datatype" {
                    types = rec.iter().map(<[u8]>::to_vec).collect();
                }
                expect_header = true;
                continue;
            }
            if expect_header || layout.is_none() {
                layout = Some(self.layout(&rec, &types)?);
                table.id.clear();
                table.id.push(0xff); // never equal to a real table id
                expect_header = false;
                continue;
            }
            let l = layout.as_ref().expect("set above");
            if rec.as_slice().len() > MAX_RECORD_BYTES {
                return Err(fail(
                    Kind::Internal,
                    format!("a result row is larger than {} MiB", MAX_RECORD_BYTES >> 20),
                )
                .into());
            }
            rows += 1;
            if rows & 0xfff == 0 && abort.load(Ordering::Relaxed) {
                return Ok(());
            }
            let tid = rec.get(l.table).unwrap_or(b"");
            if tid != table.id.as_slice() {
                self.new_table(&mut table, l, &rec);
            }
            let raw = rec.get(l.value).unwrap_or(b"");
            if raw.is_empty() && l.vtype != VType::Str {
                continue; // null
            }
            let t = rec.get(l.time).unwrap_or(b"");
            let time = parse_time_ns(t).ok_or_else(|| {
                fail(
                    Kind::Internal,
                    format!("unexpected _time `{}`", String::from_utf8_lossy(t)),
                )
            })?;
            let written = self.emit(&table, l.vtype, raw, time, sink.buf());
            if written {
                self.runs.add(&table.measurement, 1);
                sink.commit(1)?;
            }
        }
        self.runs.flush();
        Ok(())
    }

    fn layout(&self, names: &ByteRecord, types: &[Vec<u8>]) -> anyhow::Result<Layout> {
        let idx = |n: &[u8]| names.iter().position(|c| c == n);
        if idx(b"error").is_some() && idx(b"reference").is_some() {
            return Err(fail(Kind::Internal, "Flux returned an error table").into());
        }
        let (Some(time), Some(value), Some(field), Some(measurement), Some(table)) = (
            idx(b"_time"),
            idx(b"_value"),
            idx(b"_field"),
            idx(b"_measurement"),
            idx(b"table"),
        ) else {
            return Err(fail(
                Kind::Internal,
                "query result lacks _time/_value/_field/_measurement/table",
            )
            .into());
        };
        let vtype = match types.get(value).map(Vec::as_slice) {
            Some(b"double") => VType::Double,
            Some(b"long") => VType::Long,
            Some(b"unsignedLong") => VType::ULong,
            Some(b"boolean") => VType::Bool,
            Some(b"string") | None => VType::Str,
            Some(other) => {
                return Err(fail(
                    Kind::Internal,
                    format!(
                        "unsupported _value type `{}`",
                        String::from_utf8_lossy(other)
                    ),
                )
                .into());
            }
        };
        let mut tags = Vec::new();
        for (i, name) in names.iter().enumerate() {
            if RESERVED.contains(&name) {
                continue;
            }
            let mut esc = Vec::new();
            escape_into(&mut esc, name, KEY_SPECIALS);
            tags.push((i, esc, String::from_utf8_lossy(name).into_owned()));
        }
        Ok(Layout {
            time,
            value,
            field,
            measurement,
            table,
            tags,
            vtype,
        })
    }

    fn new_table(&self, t: &mut Table, l: &Layout, rec: &ByteRecord) {
        t.id.clear();
        t.id.extend_from_slice(rec.get(l.table).unwrap_or(b""));
        t.measurement.clear();
        t.measurement
            .extend_from_slice(rec.get(l.measurement).unwrap_or(b""));
        let field = rec.get(l.field).unwrap_or(b"");
        t.prefix.clear();
        escape_into(&mut t.prefix, &t.measurement, MEASUREMENT_SPECIALS);
        t.in_scope = self.scope.is_empty();
        for (i, key, raw_key) in &l.tags {
            let v = rec.get(*i).unwrap_or(b"");
            if v.is_empty() {
                continue;
            }
            t.prefix.push(b',');
            t.prefix.extend_from_slice(key);
            t.prefix.push(b'=');
            escape_into(&mut t.prefix, v, KEY_SPECIALS);
            if !t.in_scope
                && self
                    .scope
                    .iter()
                    .any(|(k, sv)| k == raw_key && sv.as_bytes() == v)
            {
                t.in_scope = true;
            }
        }
        t.prefix.push(b' ');
        t.text_prefix.clear();
        t.text_prefix.extend_from_slice(&t.prefix);
        t.text_prefix.extend_from_slice(TEXT_PREFIX.as_bytes());
        escape_into(&mut t.prefix, field, KEY_SPECIALS);
        escape_into(&mut t.text_prefix, field, KEY_SPECIALS);
        t.prefix.push(b'=');
        t.text_prefix.push(b'=');
        t.field_is_text = field.starts_with(TEXT_PREFIX.as_bytes());
    }

    /// Append one converted line; returns false when the value is dropped.
    #[inline]
    fn emit(&mut self, t: &Table, vt: VType, raw: &[u8], time: i64, out: &mut Vec<u8>) -> bool {
        let c = &mut self.counters;
        let ty = match vt {
            VType::Double => T_FLOAT,
            VType::Long => T_INT,
            VType::ULong => T_UINT,
            VType::Bool => T_BOOL,
            VType::Str => T_STRING,
        };
        let nonfinite = vt == VType::Double && matches!(raw, b"NaN" | b"+Inf" | b"-Inf");
        if nonfinite {
            c.record(ty, Action::DroppedNonFinite);
            return false;
        }
        if !t.in_scope {
            out.extend_from_slice(&t.prefix);
            match vt {
                VType::Double | VType::Bool => out.extend_from_slice(raw),
                VType::Long => {
                    out.extend_from_slice(raw);
                    out.push(b'i');
                }
                VType::ULong => {
                    out.extend_from_slice(raw);
                    out.push(b'u');
                }
                VType::Str => quote(out, raw),
            }
            c.record(ty, Action::CopiedUnchanged);
            finish(out, time);
            return true;
        }
        let action = match vt {
            VType::Double => {
                out.extend_from_slice(&t.prefix);
                out.extend_from_slice(raw);
                Action::FloatKept
            }
            VType::Long => {
                out.extend_from_slice(&t.prefix);
                out.extend_from_slice(raw);
                Action::IntToFloat
            }
            VType::ULong => {
                out.extend_from_slice(&t.prefix);
                out.extend_from_slice(raw);
                Action::UnsignedToFloat
            }
            VType::Bool => {
                out.extend_from_slice(&t.prefix);
                let truthy = raw == b"true";
                if self.rules.bool_to_float {
                    out.push(if truthy { b'1' } else { b'0' });
                    Action::BoolToFloat
                } else {
                    out.extend_from_slice(if truthy { b"true" } else { b"false" });
                    Action::BoolKept
                }
            }
            VType::Str => {
                if t.field_is_text {
                    out.extend_from_slice(&t.prefix);
                    quote(out, raw);
                    Action::TextFieldKept
                } else {
                    self.text.clear();
                    self.text.push_str(&String::from_utf8_lossy(raw));
                    match classify_text(&self.text, self.rules) {
                        TextClass::Drop(a) => {
                            c.record(ty, a);
                            return false;
                        }
                        TextClass::Number(n, a) => {
                            out.extend_from_slice(&t.prefix);
                            out.extend_from_slice(ryu::Buffer::new().format_finite(n).as_bytes());
                            a
                        }
                        TextClass::Literal if self.rules.separate_text_fields => {
                            out.extend_from_slice(&t.text_prefix);
                            quote(out, raw);
                            Action::StringToTextField
                        }
                        TextClass::Literal => {
                            out.extend_from_slice(&t.prefix);
                            quote(out, raw);
                            Action::StringKept
                        }
                    }
                }
            }
        };
        c.record(ty, action);
        finish(out, time);
        true
    }
}

#[inline]
fn quote(out: &mut Vec<u8>, raw: &[u8]) {
    out.push(b'"');
    escape_into(out, raw, STRING_SPECIALS);
    out.push(b'"');
}

#[inline]
fn finish(out: &mut Vec<u8>, time: i64) {
    out.push(b' ');
    out.extend_from_slice(itoa::Buffer::new().format(time).as_bytes());
    out.push(b'\n');
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::convert::{FieldValue, convert};
    use crate::flux::{for_each_row, row_to_point};
    use crate::lp::{parse_line, write_point};
    use crate::pipeline::VecSink;

    fn csv_table(types: &str, rows: &[&str]) -> String {
        let mut s = format!(
            "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339Nano,{types},string,string,string,string\n#group,false,false,true,true,false,false,true,true,true,true\n#default,_result,,,,,,,,,\n,result,table,_start,_stop,_time,_value,_field,_measurement,application,room\n"
        );
        for r in rows {
            s.push_str(r);
            s.push('\n');
        }
        s.push('\n');
        s
    }

    fn sample() -> String {
        let mut s = String::new();
        s += &csv_table(
            "long",
            &[
                ",,0,2023-11-14T00:00:00Z,2023-11-15T00:00:00Z,2023-11-14T22:13:20Z,7,AQ,Temperatur,loxInFlux,\"Bath, Up\"",
                ",,0,2023-11-14T00:00:00Z,2023-11-15T00:00:00Z,2023-11-14T22:14:20.5Z,-3,AQ,Temperatur,loxInFlux,\"Bath, Up\"",
            ],
        );
        s += &csv_table(
            "string",
            &[
                ",,1,2023-11-14T00:00:00Z,2023-11-15T00:00:00Z,2023-11-14T22:13:20Z,19.6°,Default,Temperatur,loxInFlux,",
                ",,1,2023-11-14T00:00:00Z,2023-11-15T00:00:00Z,2023-11-14T22:13:21Z,\"Ein \"\"x\"\" \\\\ y\",Default,Temperatur,loxInFlux,",
                ",,1,2023-11-14T00:00:00Z,2023-11-15T00:00:00Z,2023-11-14T22:13:22Z,,Default,Temperatur,loxInFlux,",
                ",,1,2023-11-14T00:00:00Z,2023-11-15T00:00:00Z,2023-11-14T22:13:23Z,<v.1>,Default,Temperatur,loxInFlux,",
                ",,2,2023-11-14T00:00:00Z,2023-11-15T00:00:00Z,2023-11-14T22:13:23Z,Aus,__text__Default,Temperatur,loxInFlux,",
            ],
        );
        s += &csv_table(
            "double",
            &[
                ",,3,2023-11-14T00:00:00Z,2023-11-15T00:00:00Z,2023-11-14T22:13:20Z,21.25,AQ,Energy Heating,loxInFlux,",
                ",,3,2023-11-14T00:00:00Z,2023-11-15T00:00:00Z,2023-11-14T22:13:21Z,NaN,AQ,Energy Heating,loxInFlux,",
                ",,3,2023-11-14T00:00:00Z,2023-11-15T00:00:00Z,2023-11-14T22:13:22Z,1e+06,AQ,Energy Heating,loxInFlux,",
            ],
        );
        s += &csv_table(
            "boolean",
            &[
                ",,4,2023-11-14T00:00:00Z,2023-11-15T00:00:00Z,2023-11-14T22:13:20Z,true,on,Alarm,loxInFlux,",
            ],
        );
        s += &csv_table(
            "long",
            &[
                ",,5,2023-11-14T00:00:00Z,2023-11-15T00:00:00Z,2023-11-14T22:13:20Z,1,state,zigbee,z2m,",
            ],
        );
        s += &csv_table(
            "unsignedLong",
            &[",,6,2023-11-14T00:00:00Z,2023-11-15T00:00:00Z,2023-11-14T22:13:20Z,5,n,zigbee,z2m,"],
        );
        s
    }

    /// Differential test: the fast path must produce the same points as the
    /// simple, obviously-correct path (row_to_point -> convert -> write_point).
    #[test]
    fn fast_path_equals_reference_path() {
        let rules = Rules::default();
        let scope = vec![("application".to_owned(), "loxInFlux".to_owned())];
        let csv = sample();

        let mut fast = CsvConverter::new(&rules, &scope);
        let mut sink = VecSink::default();
        fast.run(csv.as_bytes(), &mut sink, &AtomicBool::new(false))
            .unwrap();
        let fast_points: Vec<_> = String::from_utf8(sink.data.clone())
            .unwrap()
            .lines()
            .map(|l| parse_line(l, 0).unwrap())
            .collect();

        let mut reference = Vec::new();
        for_each_row(csv.as_bytes(), |h, r| {
            let Some(mut p) = row_to_point(h, r)? else {
                return Ok(());
            };
            let in_scope = p.tag("application") == Some("loxInFlux");
            let (f, v) = p.fields.pop().unwrap();
            let out = if in_scope {
                convert(&f, v, &rules).output
            } else {
                Some((f, v))
            };
            if let Some(o) = out {
                p.fields.push(o);
                let mut s = String::new();
                write_point(&mut s, &p);
                reference.push(parse_line(s.trim_end(), 0).unwrap());
            }
            Ok(())
        })
        .unwrap();

        assert_eq!(fast_points, reference);
        assert_eq!(fast.counters.values_read, 13);
        assert_eq!(fast.counters.values_dropped, 3);
        assert_eq!(fast.counters.values_written, sink.values);
        let m = fast.runs.take();
        assert_eq!(m["Temperatur"], 5);
        assert_eq!(m["zigbee"], 2);
        // Integers became floats, the out-of-scope ones kept their type.
        assert_eq!(fast_points[0].fields[0].1, FieldValue::Float(7.0));
        assert!(
            fast_points
                .iter()
                .any(|p| p.fields[0] == ("state".into(), FieldValue::Int(1)))
        );
        assert!(
            fast_points
                .iter()
                .any(|p| p.fields[0] == ("n".into(), FieldValue::UInt(5)))
        );
    }

    #[test]
    fn invalid_utf8_text_is_copied_byte_exact() {
        let mut csv = csv_table("string", &[]).into_bytes();
        csv.truncate(csv.len() - 1); // drop the table's trailing blank line
        csv.extend_from_slice(
            b",,0,2023-11-14T00:00:00Z,2023-11-15T00:00:00Z,2023-11-14T22:13:20Z,B",
        );
        csv.push(0xfc);
        csv.extend_from_slice(b"ro,Default,Raum,loxInFlux,\n");
        let rules = Rules::default();
        let mut c = CsvConverter::new(&rules, &[]);
        let mut sink = VecSink::default();
        c.run(csv.as_slice(), &mut sink, &AtomicBool::new(false))
            .unwrap();
        let mut want = b"Raum,application=loxInFlux __text__Default=\"B".to_vec();
        want.push(0xfc);
        want.extend_from_slice(b"ro\" 1700000000000000000\n");
        assert_eq!(sink.data, want);
    }

    #[test]
    fn error_table_is_reported() {
        let csv =
            "#datatype,string,string\n#group,true,true\n#default,,\n,error,reference\n,boom,\n";
        let rules = Rules::default();
        let mut c = CsvConverter::new(&rules, &[]);
        assert!(
            c.run(
                csv.as_bytes(),
                &mut VecSink::default(),
                &AtomicBool::new(false)
            )
            .is_err()
        );
    }
}
