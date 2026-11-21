//! Checkpoint file: which windows are migrated and what was written.
//!
//! The file makes `migrate` resumable (finished windows are skipped) and gives
//! `verify` the expected point counts. It is plain JSON so that humans and
//! agents can inspect it; it is replaced atomically after every window.

use crate::error::{Kind, fail};
use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub const VERSION: u32 = 1;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct WindowRecord {
    pub stop_ns: i64,
    pub values_read: u64,
    pub values_written: u64,
    pub values_dropped: u64,
    pub rejected_batches: u64,
    /// Field values written per measurement (for `verify`).
    pub written_by_measurement: BTreeMap<String, u64>,
    pub finished_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct State {
    pub version: u32,
    pub source: String,
    pub target: String,
    pub dry_run: bool,
    /// Window start (ns) -> result, only for finished windows.
    pub windows: BTreeMap<i64, WindowRecord>,
    /// Line-protocol input: number of points already processed.
    #[serde(default)]
    pub file_points_done: u64,
}

impl State {
    pub fn new(source: &str, target: &str, dry_run: bool) -> Self {
        Self {
            version: VERSION,
            source: source.to_owned(),
            target: target.to_owned(),
            dry_run,
            windows: BTreeMap::new(),
            file_points_done: 0,
        }
    }

    pub fn load(path: &Path) -> anyhow::Result<Option<Self>> {
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(e).with_context(|| format!("reading state file {}", path.display()));
            }
        };
        let s: State = serde_json::from_str(&text).map_err(|e| {
            fail(
                Kind::Usage,
                format!("state file {} is not valid: {e}", path.display()),
            )
            .hint("delete it to start over, or pass another --state path")
        })?;
        if s.version != VERSION {
            return Err(fail(
                Kind::Usage,
                format!(
                    "state file {} has version {}, expected {VERSION}",
                    path.display(),
                    s.version
                ),
            )
            .into());
        }
        Ok(Some(s))
    }

    /// Load and check that the file belongs to this source/target pair.
    pub fn resume(path: &Path, source: &str, target: &str, dry_run: bool) -> anyhow::Result<Self> {
        match Self::load(path)? {
            None => Ok(Self::new(source, target, dry_run)),
            Some(s) if s.source == source && s.target == target && s.dry_run == dry_run => Ok(s),
            Some(s) => Err(fail(
                Kind::Refused,
                format!(
                    "state file {} belongs to {} -> {} (dry_run={}), not {source} -> {target} (dry_run={dry_run})",
                    path.display(), s.source, s.target, s.dry_run
                ),
            )
            .hint("pass a different --state path for this migration")
            .into()),
        }
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        let tmp: PathBuf = path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(self)?)
            .with_context(|| format!("writing {}", tmp.display()))?;
        std::fs::rename(&tmp, path).with_context(|| format!("replacing {}", path.display()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn save_load_and_pair_check() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("state.json");
        let mut s = State::new("old", "new", false);
        s.windows.insert(
            5,
            WindowRecord {
                stop_ns: 9,
                values_written: 3,
                ..Default::default()
            },
        );
        s.save(&p).unwrap();
        assert_eq!(State::resume(&p, "old", "new", false).unwrap(), s);
        let err = State::resume(&p, "old", "other", false).unwrap_err();
        assert_eq!(crate::error::classify(&err).0, Kind::Refused);
        assert!(
            State::resume(&dir.path().join("none.json"), "a", "b", true)
                .unwrap()
                .windows
                .is_empty()
        );
    }
}
