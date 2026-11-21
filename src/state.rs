//! Checkpoint file (JSON lines): one header, then one line per finished window.
//!
//! Appending a line per window keeps the cost of a checkpoint constant, no
//! matter how many windows a migration has, and lets `verify` stream the file
//! instead of loading it. Humans and agents can inspect it with `jq`.
//!
//! ```text
//! {"kind":"header","version":2,"source":"loxinflux","target":"loxinflux_v2","dry_run":false,"window_secs":86400}
//! {"kind":"window","start_ns":...,"stop_ns":...,"values_read":...,"values_written":...,"written_by_measurement":{...},...}
//! ```
//! A window can appear more than once (after `--redo`); the last line wins.

use crate::error::{Kind, fail};
use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

pub const VERSION: u32 = 2;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Header {
    pub kind: String,
    pub version: u32,
    pub source: String,
    pub target: String,
    pub dry_run: bool,
    pub window_secs: i64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct WindowRecord {
    pub kind: String,
    pub start_ns: i64,
    pub stop_ns: i64,
    pub values_read: u64,
    pub values_written: u64,
    pub values_dropped: u64,
    pub rejected_batches: u64,
    pub written_by_measurement: BTreeMap<String, u64>,
    pub finished_at: String,
}

#[derive(Deserialize)]
struct KindOnly {
    kind: String,
    #[serde(default)]
    start_ns: i64,
}

fn invalid(path: &Path, line: usize, e: impl std::fmt::Display) -> anyhow::Error {
    fail(
        Kind::Usage,
        format!(
            "state file {} line {line} is not valid: {e}",
            path.display()
        ),
    )
    .hint("delete it to start over, or pass another --state path")
    .into()
}

pub fn read_header(path: &Path) -> anyhow::Result<Option<Header>> {
    let f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let mut first = String::new();
    BufReader::new(f).read_line(&mut first)?;
    let h: Header = serde_json::from_str(&first).map_err(|e| invalid(path, 1, e))?;
    if h.kind != "header" || h.version != VERSION {
        return Err(invalid(
            path,
            1,
            format!("expected a version {VERSION} header"),
        ));
    }
    Ok(Some(h))
}

/// Start times of finished windows (small: no per-measurement data is kept).
pub fn finished_windows(path: &Path) -> anyhow::Result<HashSet<i64>> {
    let mut out = HashSet::new();
    let f = BufReader::new(std::fs::File::open(path)?);
    for (i, line) in f.lines().enumerate().skip(1) {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let k: KindOnly = serde_json::from_str(&line).map_err(|e| invalid(path, i + 1, e))?;
        if k.kind == "window" {
            out.insert(k.start_ns);
        }
    }
    Ok(out)
}

/// Stream the window records, keeping only the last record per window and
/// never holding more than one record's measurement map at a time.
pub fn for_each_window(
    path: &Path,
    mut f: impl FnMut(WindowRecord) -> anyhow::Result<()>,
) -> anyhow::Result<usize> {
    // Pass 1: line number of the last record per window.
    let mut last: HashMap<i64, usize> = HashMap::new();
    for (i, line) in BufReader::new(std::fs::File::open(path)?)
        .lines()
        .enumerate()
        .skip(1)
    {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let k: KindOnly = serde_json::from_str(&line).map_err(|e| invalid(path, i + 1, e))?;
        if k.kind == "window" {
            last.insert(k.start_ns, i);
        }
    }
    let wanted: HashSet<usize> = last.into_values().collect();
    let mut n = 0;
    // Pass 2: parse only the wanted lines.
    for (i, line) in BufReader::new(std::fs::File::open(path)?)
        .lines()
        .enumerate()
        .skip(1)
    {
        let line = line?;
        if !wanted.contains(&i) {
            continue;
        }
        let rec: WindowRecord = serde_json::from_str(&line).map_err(|e| invalid(path, i + 1, e))?;
        f(rec)?;
        n += 1;
    }
    Ok(n)
}

pub struct Writer {
    file: std::fs::File,
}

impl Writer {
    /// Open for appending; writes the header when the file is new. Refuses a
    /// file that belongs to another migration.
    pub fn open(path: &Path, header: &Header) -> anyhow::Result<Self> {
        if let Some(h) = read_header(path)? {
            if h != *header {
                return Err(fail(
                    Kind::Refused,
                    format!(
                        "state file {} belongs to {} -> {} (dry_run={}, window {}s), not {} -> {} (dry_run={}, window {}s)",
                        path.display(),
                        h.source,
                        h.target,
                        h.dry_run,
                        h.window_secs,
                        header.source,
                        header.target,
                        header.dry_run,
                        header.window_secs
                    ),
                )
                .hint("pass a different --state path for this migration")
                .into());
            }
            let file = std::fs::OpenOptions::new().append(true).open(path)?;
            return Ok(Self { file });
        }
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .with_context(|| format!("creating state file {}", path.display()))?;
        let mut line = serde_json::to_vec(header)?;
        line.push(b'\n');
        file.write_all(&line)?;
        file.sync_data()?;
        Ok(Self { file })
    }

    pub fn append(&mut self, rec: &WindowRecord) -> anyhow::Result<()> {
        let mut line = serde_json::to_vec(rec)?;
        line.push(b'\n');
        self.file
            .write_all(&line)
            .context("appending to the state file")?;
        self.file.sync_data()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(target: &str) -> Header {
        Header {
            kind: "header".into(),
            version: VERSION,
            source: "old".into(),
            target: target.into(),
            dry_run: false,
            window_secs: 86_400,
        }
    }

    #[test]
    fn append_resume_and_last_record_wins() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("s.jsonl");
        let mut w = Writer::open(&p, &header("new")).unwrap();
        let rec = |start, written| WindowRecord {
            kind: "window".into(),
            start_ns: start,
            stop_ns: start + 1,
            values_written: written,
            ..Default::default()
        };
        w.append(&rec(1, 5)).unwrap();
        w.append(&rec(2, 7)).unwrap();
        w.append(&rec(1, 9)).unwrap();
        drop(w);
        assert_eq!(finished_windows(&p).unwrap(), HashSet::from([1, 2]));
        let mut seen = Vec::new();
        let n = for_each_window(&p, |r| {
            seen.push((r.start_ns, r.values_written));
            Ok(())
        })
        .unwrap();
        seen.sort();
        assert_eq!((n, seen), (2, vec![(1, 9), (2, 7)]));
        // Reopen with the same header appends; another target is refused.
        Writer::open(&p, &header("new")).unwrap();
        let err = Writer::open(&p, &header("other")).err().unwrap();
        assert_eq!(crate::error::classify(&err).0, Kind::Refused);
    }
}
