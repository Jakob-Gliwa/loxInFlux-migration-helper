//! `convert-value`: show how one stored value would be migrated (offline).

use crate::cli::{ConvertValueArgs, ValueType};
use crate::convert::{FieldValue, convert};
use crate::error::{Kind, fail};
use crate::lp::{Point, write_point};
use crate::output::Outcome;
use serde::Serialize;

#[derive(Serialize)]
struct Report {
    field: String,
    input_type: &'static str,
    input: String,
    action: &'static str,
    output_field: Option<String>,
    output_type: Option<&'static str>,
    /// The field as it would appear in line protocol.
    output_line_protocol: Option<String>,
}

pub fn run(a: &ConvertValueArgs) -> anyhow::Result<Outcome> {
    let bad = |t: &str| fail(Kind::Usage, format!("`{}` is not a valid {t}", a.value));
    let value = match a.value_type {
        ValueType::String => FieldValue::Str(a.value.clone()),
        ValueType::Float => FieldValue::Float(a.value.parse().map_err(|_| bad("float"))?),
        ValueType::Integer => FieldValue::Int(a.value.parse().map_err(|_| bad("integer"))?),
        ValueType::Unsigned => {
            FieldValue::UInt(a.value.parse().map_err(|_| bad("unsigned integer"))?)
        }
        ValueType::Boolean => FieldValue::Bool(match a.value.as_str() {
            "true" | "t" | "1" => true,
            "false" | "f" | "0" => false,
            _ => return Err(bad("boolean").into()),
        }),
    };
    let input_type = value.type_name();
    let c = convert(&a.field, value, &a.rules.rules());
    let (output_field, output_type, lp) = match &c.output {
        Some((f, v)) => {
            let mut s = String::new();
            write_point(
                &mut s,
                &Point {
                    measurement: "m".into(),
                    tags: vec![],
                    fields: vec![(f.clone(), v.clone())],
                    time: 0,
                },
            );
            let field_part = s
                .trim_end()
                .strip_prefix("m ")
                .and_then(|x| x.strip_suffix(" 0"))
                .unwrap_or(&s)
                .to_owned();
            (Some(f.clone()), Some(v.type_name()), Some(field_part))
        }
        None => (None, None, None),
    };
    let report = Report {
        field: a.field.clone(),
        input_type,
        input: a.value.clone(),
        action: c.action.as_str(),
        output_field,
        output_type,
        output_line_protocol: lp.clone(),
    };
    let human = match &lp {
        Some(l) => format!(
            "{} {}={:?} -> {} ({})\n",
            input_type, a.field, a.value, l, report.action
        ),
        None => format!(
            "{} {}={:?} -> dropped ({})\n",
            input_type, a.field, a.value, report.action
        ),
    };
    Ok(Outcome::ok(&report, human))
}
