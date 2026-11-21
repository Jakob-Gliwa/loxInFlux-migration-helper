//! Error classes with stable exit codes.
//!
//! Every failure that a caller (human or agent) can act on is raised as a
//! [`Failure`] with a [`Kind`]. The kind decides the process exit code and the
//! `error.kind` string in JSON output, so both are part of the public contract
//! and must not change between minor versions.

use serde::Serialize;
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// Bad command line or option combination.
    Usage,
    /// InfluxDB is unreachable, or the URL is not an InfluxDB v2 API.
    Connection,
    /// The token was rejected or lacks a permission.
    Auth,
    /// A bucket, organisation or state file does not exist.
    NotFound,
    /// InfluxDB rejected points (typically `field type conflict`).
    WriteRejected,
    /// Verification found fewer points in the target than were migrated.
    VerifyMismatch,
    /// The tool refused to do something unsafe (e.g. source == target).
    Refused,
    /// A resource guard stopped the run (own memory limit, InfluxDB heap).
    ResourceLimit,
    /// SIGINT/SIGTERM: stopped cleanly, finished work is recorded.
    Interrupted,
    /// Anything else, usually a bug or an unexpected server answer.
    Internal,
}

impl Kind {
    pub fn exit_code(self) -> i32 {
        match self {
            Kind::Internal => 1,
            Kind::Usage => 2,
            Kind::Connection => 3,
            Kind::Auth => 4,
            Kind::NotFound => 5,
            Kind::WriteRejected => 6,
            Kind::VerifyMismatch => 7,
            Kind::Refused => 8,
            Kind::ResourceLimit => 9,
            Kind::Interrupted => 10,
        }
    }
}

#[derive(Debug)]
pub struct Failure {
    pub kind: Kind,
    pub message: String,
    /// One sentence telling the caller what to do next.
    pub hint: Option<String>,
}

impl Failure {
    pub fn new(kind: Kind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            hint: None,
        }
    }
    pub fn hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Failure {}

/// Shorthand: `return Err(fail(Kind::Usage, "...").into())`.
pub fn fail(kind: Kind, message: impl Into<String>) -> Failure {
    Failure::new(kind, message)
}

/// Classify any error chain: the first [`Failure`] in it wins, otherwise the
/// error is internal.
pub fn classify(err: &anyhow::Error) -> (Kind, Option<String>) {
    for cause in err.chain() {
        if let Some(f) = cause.downcast_ref::<Failure>() {
            return (f.kind, f.hint.clone());
        }
    }
    (Kind::Internal, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_codes_are_distinct_and_stable() {
        let kinds = [
            Kind::Internal,
            Kind::Usage,
            Kind::Connection,
            Kind::Auth,
            Kind::NotFound,
            Kind::WriteRejected,
            Kind::VerifyMismatch,
            Kind::Refused,
            Kind::ResourceLimit,
            Kind::Interrupted,
        ];
        let codes: Vec<i32> = kinds.iter().map(|k| k.exit_code()).collect();
        assert_eq!(codes, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn classify_finds_failure_behind_context() {
        let err = anyhow::Error::new(fail(Kind::Auth, "no").hint("check token"))
            .context("while listing buckets");
        assert_eq!(classify(&err), (Kind::Auth, Some("check token".into())));
        assert_eq!(classify(&anyhow::anyhow!("boom")).0, Kind::Internal);
    }
}
