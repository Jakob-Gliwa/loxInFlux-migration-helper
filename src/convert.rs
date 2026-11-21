//! Value conversion to the loxInFlux 0.5 schema.
//!
//! The rules mirror `loxinflux/src/value.rs` and `protocol.rs` of loxInFlux
//! 0.5 so that migrated history and newly written data share one schema:
//!
//! * integers, unsigned integers and booleans become floats (same field name);
//! * floats are copied unchanged;
//! * strings are trimmed and then
//!   - dropped when empty or a Loxone placeholder (`<v.1>`),
//!   - written as float when they are a plain number (`"21.5"`) or a number
//!     with a unit (`"19.8°"`, `"523ppm"`, `"-331 W"`),
//!   - otherwise moved to the text field `__text__<field>`.
//!
//! Comma decimals (`"1,5 kW"`), dates and sentences that start with a number
//! stay text, exactly as in loxInFlux.
//!
//! The conversion is idempotent: a string already stored in a `__text__*`
//! field is copied unchanged, and floats are never renamed, so running the
//! tool over its own output changes nothing.

use regex::Regex;
use serde::Serialize;
use std::sync::LazyLock;

#[derive(Clone, Debug, PartialEq)]
pub enum FieldValue {
    Float(f64),
    Int(i64),
    UInt(u64),
    Bool(bool),
    Str(String),
}

impl FieldValue {
    pub fn type_name(&self) -> &'static str {
        match self {
            FieldValue::Float(_) => "float",
            FieldValue::Int(_) => "integer",
            FieldValue::UInt(_) => "unsigned",
            FieldValue::Bool(_) => "boolean",
            FieldValue::Str(_) => "string",
        }
    }
}

pub const TEXT_PREFIX: &str = "__text__";

/// Switches that correspond to the loxInFlux `[processing]` options.
#[derive(Clone, Debug, PartialEq, Serialize, serde::Deserialize)]
pub struct Rules {
    pub bool_to_float: bool,
    pub parse_numeric_units: bool,
    pub drop_empty: bool,
    pub drop_placeholders: bool,
    pub separate_text_fields: bool,
}

impl Default for Rules {
    fn default() -> Self {
        Self {
            bool_to_float: true,
            parse_numeric_units: true,
            drop_empty: true,
            drop_placeholders: true,
            separate_text_fields: true,
        }
    }
}

/// What happened to one field value. Used for statistics, so the names are
/// part of the JSON output.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    FloatKept,
    IntToFloat,
    UnsignedToFloat,
    BoolToFloat,
    BoolKept,
    StringToFloat,
    StringWithUnitToFloat,
    StringToTextField,
    TextFieldKept,
    StringKept,
    /// Copied unchanged because the point is outside the conversion scope.
    CopiedUnchanged,
    DroppedEmpty,
    DroppedPlaceholder,
    DroppedNonFinite,
}

impl Action {
    pub const COUNT: usize = 14;
    pub const ALL: [Action; Action::COUNT] = [
        Action::FloatKept,
        Action::IntToFloat,
        Action::UnsignedToFloat,
        Action::BoolToFloat,
        Action::BoolKept,
        Action::StringToFloat,
        Action::StringWithUnitToFloat,
        Action::StringToTextField,
        Action::TextFieldKept,
        Action::StringKept,
        Action::CopiedUnchanged,
        Action::DroppedEmpty,
        Action::DroppedPlaceholder,
        Action::DroppedNonFinite,
    ];
    pub fn index(self) -> usize {
        self as usize
    }
    pub fn is_drop(self) -> bool {
        matches!(
            self,
            Action::DroppedEmpty | Action::DroppedPlaceholder | Action::DroppedNonFinite
        )
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Action::FloatKept => "float_kept",
            Action::IntToFloat => "int_to_float",
            Action::UnsignedToFloat => "unsigned_to_float",
            Action::BoolToFloat => "bool_to_float",
            Action::BoolKept => "bool_kept",
            Action::StringToFloat => "string_to_float",
            Action::StringWithUnitToFloat => "string_with_unit_to_float",
            Action::StringToTextField => "string_to_text_field",
            Action::TextFieldKept => "text_field_kept",
            Action::StringKept => "string_kept",
            Action::CopiedUnchanged => "copied_unchanged",
            Action::DroppedEmpty => "dropped_empty",
            Action::DroppedPlaceholder => "dropped_placeholder",
            Action::DroppedNonFinite => "dropped_non_finite",
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Converted {
    pub action: Action,
    /// `None` when the value is dropped.
    pub output: Option<(String, FieldValue)>,
}

// Copied from loxInFlux 0.5 (value.rs): full match, not merely a numeric
// prefix. Units may contain /, °, % and superscripts, but not a second ASCII
// number or whitespace inside the unit.
static WITH_UNIT: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"^([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?)\s*([\p{L}°%µμΩ‰/²³][\p{L}°%µμΩ‰/²³]*)$",
    )
    .unwrap()
});

fn unit_number(text: &str) -> Option<f64> {
    let caps = WITH_UNIT.captures(text)?;
    if matches!(caps.get(2)?.as_str(), "e" | "E") {
        return None;
    }
    caps.get(1)?.as_str().parse().ok()
}

pub enum TextClass {
    Number(f64, Action),
    Literal,
    Drop(Action),
}

/// Classify a string value (trimmed) without allocating.
pub fn classify_text(value: &str, rules: &Rules) -> TextClass {
    let trimmed = value.trim();
    if rules.drop_empty && trimmed.is_empty() {
        return TextClass::Drop(Action::DroppedEmpty);
    }
    if rules.drop_placeholders && trimmed.contains("<v.") {
        return TextClass::Drop(Action::DroppedPlaceholder);
    }
    if let Ok(n) = trimmed.parse::<f64>() {
        return number(n, Action::StringToFloat);
    }
    if rules.parse_numeric_units
        && trimmed
            .as_bytes()
            .first()
            .is_some_and(|b| b.is_ascii_digit() || matches!(b, b'+' | b'-' | b'.'))
        && let Some(n) = unit_number(trimmed)
    {
        return number(n, Action::StringWithUnitToFloat);
    }
    TextClass::Literal
}

fn number(n: f64, action: Action) -> TextClass {
    if n.is_finite() {
        TextClass::Number(if n == 0.0 { 0.0 } else { n }, action)
    } else {
        TextClass::Drop(Action::DroppedNonFinite)
    }
}

fn keep(field: &str, value: FieldValue, action: Action) -> Converted {
    Converted {
        action,
        output: Some((field.to_owned(), value)),
    }
}

fn drop(action: Action) -> Converted {
    Converted {
        action,
        output: None,
    }
}

/// Convert one field value.
pub fn convert(field: &str, value: FieldValue, rules: &Rules) -> Converted {
    match value {
        FieldValue::Float(f) if f.is_finite() => {
            keep(field, FieldValue::Float(f), Action::FloatKept)
        }
        FieldValue::Float(_) => drop(Action::DroppedNonFinite),
        FieldValue::Int(i) => keep(field, FieldValue::Float(i as f64), Action::IntToFloat),
        FieldValue::UInt(u) => keep(field, FieldValue::Float(u as f64), Action::UnsignedToFloat),
        FieldValue::Bool(b) if rules.bool_to_float => keep(
            field,
            FieldValue::Float(if b { 1.0 } else { 0.0 }),
            Action::BoolToFloat,
        ),
        FieldValue::Bool(b) => keep(field, FieldValue::Bool(b), Action::BoolKept),
        FieldValue::Str(s) if field.starts_with(TEXT_PREFIX) => {
            keep(field, FieldValue::Str(s), Action::TextFieldKept)
        }
        FieldValue::Str(s) => match classify_text(&s, rules) {
            TextClass::Drop(action) => drop(action),
            TextClass::Number(n, action) => keep(field, FieldValue::Float(n), action),
            TextClass::Literal if rules.separate_text_fields => Converted {
                action: Action::StringToTextField,
                output: Some((format!("{TEXT_PREFIX}{field}"), FieldValue::Str(s))),
            },
            TextClass::Literal => keep(field, FieldValue::Str(s), Action::StringKept),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conv(field: &str, v: FieldValue) -> Converted {
        convert(field, v, &Rules::default())
    }
    fn out(field: &str, v: FieldValue) -> (Action, Option<(String, FieldValue)>) {
        let c = conv(field, v);
        (c.action, c.output)
    }
    fn s(v: &str) -> FieldValue {
        FieldValue::Str(v.into())
    }

    #[test]
    fn numbers_become_floats_under_the_same_name() {
        assert_eq!(
            out("AQ", FieldValue::Int(7)),
            (
                Action::IntToFloat,
                Some(("AQ".into(), FieldValue::Float(7.0)))
            )
        );
        assert_eq!(
            out("AQ", FieldValue::UInt(3)),
            (
                Action::UnsignedToFloat,
                Some(("AQ".into(), FieldValue::Float(3.0)))
            )
        );
        assert_eq!(
            out("AQ", FieldValue::Bool(true)),
            (
                Action::BoolToFloat,
                Some(("AQ".into(), FieldValue::Float(1.0)))
            )
        );
        assert_eq!(
            out("AQ", FieldValue::Float(21.25)),
            (
                Action::FloatKept,
                Some(("AQ".into(), FieldValue::Float(21.25)))
            )
        );
    }

    #[test]
    fn units_are_parsed_like_loxinflux() {
        for (raw, n) in [
            ("19.8°", 19.8),
            ("523ppm", 523.0),
            ("0.5A", 0.5),
            ("-331 W", -331.0),
            ("70Lx", 70.0),
            ("22%", 22.0),
            ("3.2km/h", 3.2),
            ("2460.0seconds", 2460.0),
        ] {
            assert_eq!(
                out("Default", s(raw)),
                (
                    Action::StringWithUnitToFloat,
                    Some(("Default".into(), FieldValue::Float(n)))
                ),
                "{raw}"
            );
        }
        assert_eq!(out("Default", s(" 21.5 ")).0, Action::StringToFloat);
    }

    #[test]
    fn text_moves_to_its_own_field() {
        for raw in [
            "1,5 kW",
            "Ein",
            "2026-11-05",
            "12:30",
            "3 Fenster offen",
            "1e",
        ] {
            assert_eq!(
                out("Default", s(raw)),
                (
                    Action::StringToTextField,
                    Some(("__text__Default".into(), s(raw)))
                ),
                "{raw}"
            );
        }
    }

    #[test]
    fn empty_placeholders_and_non_finite_are_dropped() {
        assert_eq!(out("API", s("")), (Action::DroppedEmpty, None));
        assert_eq!(out("API", s("   ")), (Action::DroppedEmpty, None));
        assert_eq!(out("x", s("<v.1> °C")), (Action::DroppedPlaceholder, None));
        for raw in ["NaN", "inf", "1e999"] {
            assert_eq!(out("x", s(raw)), (Action::DroppedNonFinite, None), "{raw}");
        }
        assert_eq!(
            out("x", FieldValue::Float(f64::NAN)).0,
            Action::DroppedNonFinite
        );
    }

    #[test]
    fn conversion_is_idempotent() {
        let first = conv("Default", s("Ein")).output.unwrap();
        let second = conv(&first.0, first.1.clone()).output.unwrap();
        assert_eq!(first, second);
        let first = conv("Default", s("19.6°")).output.unwrap();
        assert_eq!(conv(&first.0, first.1.clone()).output.unwrap(), first);
    }

    #[test]
    fn rules_can_be_switched_off() {
        let rules = Rules {
            bool_to_float: false,
            parse_numeric_units: false,
            drop_empty: false,
            drop_placeholders: false,
            separate_text_fields: false,
        };
        let c = |f: &str, v| convert(f, v, &rules);
        assert_eq!(c("a", FieldValue::Bool(false)).action, Action::BoolKept);
        assert_eq!(c("a", s("19.8°")).output, Some(("a".into(), s("19.8°"))));
        assert_eq!(c("a", s("")).action, Action::StringKept);
        assert_eq!(c("a", s("<v.1>")).action, Action::StringKept);
    }

    #[test]
    fn action_index_matches_all() {
        for (i, a) in Action::ALL.iter().enumerate() {
            assert_eq!(a.index(), i);
        }
    }

    #[test]
    fn negative_zero_is_normalised() {
        let c = conv("a", s("-0"));
        assert_eq!(c.output, Some(("a".into(), FieldValue::Float(0.0))));
        if let Some((_, FieldValue::Float(f))) = c.output {
            assert!(f.is_sign_positive());
        }
    }
}
