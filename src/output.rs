//! Result rendering.
//!
//! stdout carries exactly one result: a short human summary, or with
//! `--output json` a single JSON envelope (schema in docs/output.md). Logs and
//! progress go to stderr, so stdout stays machine-readable.

use crate::error::{Failure, Kind};
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Format {
    Human,
    Json,
}

/// What a command produced. A command may finish with a result *and* a
/// failure kind (e.g. `verify` found gaps), which sets the exit code.
pub struct Outcome {
    pub result: serde_json::Value,
    pub human: String,
    pub warnings: Vec<String>,
    pub next_steps: Vec<String>,
    pub failure: Option<Failure>,
}

impl Outcome {
    pub fn ok(result: impl Serialize, human: String) -> Self {
        Self {
            result: serde_json::to_value(result).unwrap_or(serde_json::Value::Null),
            human,
            warnings: Vec::new(),
            next_steps: Vec::new(),
            failure: None,
        }
    }
}

#[derive(Serialize)]
struct ErrorOut<'a> {
    kind: Kind,
    message: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<&'a str>,
}

#[derive(Serialize)]
struct Envelope<'a> {
    tool: &'static str,
    version: &'static str,
    command: &'a str,
    status: &'static str,
    exit_code: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<&'a serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<ErrorOut<'a>>,
    warnings: &'a [String],
    next_steps: &'a [String],
}

/// Print the result and return the exit code.
pub fn emit(
    format: Format,
    command: &str,
    outcome: Result<Outcome, (Kind, String, Option<String>)>,
) -> i32 {
    let empty: Vec<String> = Vec::new();
    match outcome {
        Ok(o) => {
            let (status, code, error) = match &o.failure {
                None => ("ok", 0, None),
                Some(f) => (
                    "error",
                    f.kind.exit_code(),
                    Some(ErrorOut {
                        kind: f.kind,
                        message: &f.message,
                        hint: f.hint.as_deref(),
                    }),
                ),
            };
            match format {
                Format::Json => print_json(&Envelope {
                    tool: env!("CARGO_PKG_NAME"),
                    version: env!("CARGO_PKG_VERSION"),
                    command,
                    status,
                    exit_code: code,
                    result: Some(&o.result),
                    error,
                    warnings: &o.warnings,
                    next_steps: &o.next_steps,
                }),
                Format::Human => {
                    print!("{}", o.human);
                    if !o.human.ends_with('\n') {
                        println!();
                    }
                    for w in &o.warnings {
                        println!("WARNING: {w}");
                    }
                    if let Some(f) = &o.failure {
                        println!("ERROR ({}): {}", kind_name(f.kind), f.message);
                        if let Some(h) = &f.hint {
                            println!("HINT: {h}");
                        }
                    }
                    for s in &o.next_steps {
                        println!("NEXT: {s}");
                    }
                }
            }
            code
        }
        Err((kind, message, hint)) => {
            let code = kind.exit_code();
            match format {
                Format::Json => print_json(&Envelope {
                    tool: env!("CARGO_PKG_NAME"),
                    version: env!("CARGO_PKG_VERSION"),
                    command,
                    status: "error",
                    exit_code: code,
                    result: None,
                    error: Some(ErrorOut {
                        kind,
                        message: &message,
                        hint: hint.as_deref(),
                    }),
                    warnings: &empty,
                    next_steps: &empty,
                }),
                Format::Human => {
                    println!("ERROR ({}): {message}", kind_name(kind));
                    if let Some(h) = hint {
                        println!("HINT: {h}");
                    }
                }
            }
            code
        }
    }
}

pub fn kind_name(kind: Kind) -> String {
    serde_json::to_value(kind)
        .ok()
        .and_then(|v| v.as_str().map(str::to_owned))
        .unwrap_or_else(|| "internal".into())
}

fn print_json(v: &impl Serialize) {
    match serde_json::to_string_pretty(v) {
        Ok(s) => println!("{s}"),
        Err(e) => println!(
            "{{\"status\":\"error\",\"exit_code\":1,\"error\":{{\"kind\":\"internal\",\"message\":\"{e}\"}}}}"
        ),
    }
}

/// Thousands separators for human output.
pub fn n(v: u64) -> String {
    let s = v.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(' ');
        }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    #[test]
    fn groups_thousands() {
        assert_eq!(super::n(0), "0");
        assert_eq!(super::n(1234567), "1 234 567");
        assert_eq!(super::n(999), "999");
    }
}
