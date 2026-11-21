//! Flux query text and a streaming reader for annotated CSV results.

use crate::convert::FieldValue;
use crate::error::{Kind, fail};
use crate::lp::Point;
use crate::window::{Window, fmt_time};
use anyhow::Context as _;
use chrono::DateTime;
use std::io::Read;

/// Quote a string literal for Flux.
pub fn lit(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '$' => out.push_str("\\$"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}

/// `from |> range` plus an optional measurement filter.
pub fn source(bucket: &str, w: Window, measurements: &[String]) -> String {
    let mut q = format!(
        "from(bucket: {}) |> range(start: {}, stop: {})",
        lit(bucket),
        fmt_time(w.start),
        fmt_time(w.stop)
    );
    if !measurements.is_empty() {
        let cond: Vec<String> = measurements
            .iter()
            .map(|m| format!("r._measurement == {}", lit(m)))
            .collect();
        q.push_str(&format!(" |> filter(fn: (r) => {})", cond.join(" or ")));
    }
    q
}

pub struct Header {
    pub names: Vec<String>,
    pub types: Vec<String>,
}

impl Header {
    pub fn index(&self, name: &str) -> Option<usize> {
        self.names.iter().position(|n| n == name)
    }
}

/// Calls `f` for every data row of an annotated CSV stream. A Flux error
/// table is turned into an error.
pub fn for_each_row<R: Read>(
    input: R,
    mut f: impl FnMut(&Header, &csv::StringRecord) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(input);
    let mut header: Option<Header> = None;
    let mut types: Vec<String> = Vec::new();
    let mut expect_header = false;
    let mut error_table = false;
    let mut rec = csv::StringRecord::new();
    while rdr
        .read_record(&mut rec)
        .context("reading the query result")?
    {
        let first = rec.get(0).unwrap_or("");
        if first.starts_with('#') {
            if first == "#datatype" {
                types = rec.iter().map(str::to_owned).collect();
            }
            expect_header = true;
            continue;
        }
        if expect_header || header.is_none() {
            let names: Vec<String> = rec.iter().map(str::to_owned).collect();
            error_table =
                names.iter().any(|n| n == "error") && names.iter().any(|n| n == "reference");
            header = Some(Header {
                names,
                types: std::mem::take(&mut types),
            });
            expect_header = false;
            continue;
        }
        let h = header.as_ref().expect("header set above");
        if error_table {
            let msg = h
                .index("error")
                .and_then(|i| rec.get(i))
                .unwrap_or("unknown error");
            return Err(fail(Kind::Internal, format!("Flux query failed: {msg}")).into());
        }
        f(h, &rec)?;
    }
    Ok(())
}

const RESERVED: [&str; 9] = [
    "",
    "result",
    "table",
    "_start",
    "_stop",
    "_time",
    "_value",
    "_field",
    "_measurement",
];

pub fn parse_value(datatype: &str, raw: &str) -> anyhow::Result<FieldValue> {
    let bad = || {
        fail(
            Kind::Internal,
            format!("unexpected {datatype} value `{raw}` in query result"),
        )
    };
    Ok(match datatype {
        "double" => FieldValue::Float(match raw {
            "+Inf" => f64::INFINITY,
            "-Inf" => f64::NEG_INFINITY,
            "NaN" => f64::NAN,
            _ => raw.parse().map_err(|_| bad())?,
        }),
        "long" => FieldValue::Int(raw.parse().map_err(|_| bad())?),
        "unsignedLong" => FieldValue::UInt(raw.parse().map_err(|_| bad())?),
        "boolean" => FieldValue::Bool(match raw {
            "true" => true,
            "false" => false,
            _ => return Err(bad().into()),
        }),
        "string" | "" => FieldValue::Str(raw.to_owned()),
        other => {
            return Err(fail(Kind::Internal, format!("unsupported column type `{other}`")).into());
        }
    })
}

/// Turn a raw row (`from |> range` output) into a single-field point.
/// Rows with a null value yield `None`.
pub fn row_to_point(h: &Header, rec: &csv::StringRecord) -> anyhow::Result<Option<Point>> {
    let get = |name: &str| h.index(name).and_then(|i| rec.get(i));
    let (Some(m), Some(field), Some(time), Some(vi)) = (
        get("_measurement"),
        get("_field"),
        get("_time"),
        h.index("_value"),
    ) else {
        return Err(fail(
            Kind::Internal,
            "query result lacks _measurement/_field/_time/_value columns",
        )
        .into());
    };
    let raw = rec.get(vi).unwrap_or("");
    let datatype = h.types.get(vi).map(String::as_str).unwrap_or("string");
    if raw.is_empty() && datatype != "string" {
        return Ok(None);
    }
    let value = parse_value(datatype, raw)?;
    let time = DateTime::parse_from_rfc3339(time)
        .ok()
        .and_then(|t| t.timestamp_nanos_opt())
        .ok_or_else(|| fail(Kind::Internal, format!("unexpected _time `{time}`")))?;
    let mut tags: Vec<(String, String)> = h
        .names
        .iter()
        .zip(rec.iter())
        .filter(|(n, v)| !v.is_empty() && !RESERVED.contains(&n.as_str()))
        .map(|(n, v)| (n.clone(), v.to_owned()))
        .collect();
    tags.sort();
    Ok(Some(Point {
        measurement: m.to_owned(),
        tags,
        fields: vec![(field.to_owned(), value)],
        time,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    const CSV: &str = "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339Nano,long,string,string,string\n\
#group,false,false,true,true,false,false,true,true,true\n\
#default,_result,,,,,,,,\n\
,result,table,_start,_stop,_time,_value,_field,_measurement,room\n\
,,0,2023-11-14T00:00:00Z,2023-11-15T00:00:00Z,2023-11-14T22:13:20.5Z,7,AQ,Temperatur,\"Bath, Up\"\n\
\n\
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339Nano,string,string,string,string\n\
#group,false,false,true,true,false,false,true,true,true\n\
#default,_result,,,,,,,,\n\
,result,table,_start,_stop,_time,_value,_field,_measurement,room\n\
,,1,2023-11-14T00:00:00Z,2023-11-15T00:00:00Z,2023-11-14T22:13:20Z,19.6°,Default,Temperatur,\n";

    #[test]
    fn reads_tables_with_types_and_tags() {
        let mut points = Vec::new();
        for_each_row(CSV.as_bytes(), |h, r| {
            points.extend(row_to_point(h, r)?);
            Ok(())
        })
        .unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].fields[0], ("AQ".into(), FieldValue::Int(7)));
        assert_eq!(points[0].tags, vec![("room".into(), "Bath, Up".into())]);
        assert_eq!(points[0].time, 1_700_000_000_500_000_000);
        assert_eq!(
            points[1].fields[0],
            ("Default".into(), FieldValue::Str("19.6°".into()))
        );
        assert!(points[1].tags.is_empty());
    }

    #[test]
    fn flux_error_tables_become_errors() {
        let csv = "#datatype,string,string\n#group,true,true\n#default,,\n,error,reference\n,\"type conflict\",\n";
        let err = for_each_row(csv.as_bytes(), |_, _| Ok(())).unwrap_err();
        assert!(err.to_string().contains("type conflict"));
    }

    #[test]
    fn query_text_is_quoted() {
        let w = Window {
            start: 0,
            stop: 1_000_000_000,
        };
        assert_eq!(
            source("lox\"in", w, &["a b".into(), "c".into()]),
            "from(bucket: \"lox\\\"in\") |> range(start: 1970-01-01T00:00:00Z, stop: 1970-01-01T00:00:01Z) |> filter(fn: (r) => r._measurement == \"a b\" or r._measurement == \"c\")"
        );
    }
}
