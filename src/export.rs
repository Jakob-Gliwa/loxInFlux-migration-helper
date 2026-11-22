//! `verify --deep --read-via export-lp`: read a bucket straight from the
//! storage engine files with `influxd inspect export-lp` instead of the Flux
//! query engine.
//!
//! InfluxDB spends most of a Flux read on planning, cursors and CSV encoding;
//! export-lp decodes the TSM blocks and prints line protocol. One export call
//! covers several consecutive windows; each line is converted with the same
//! rules as the migration and routed by its timestamp into the window's
//! checksum sink.
//!
//! export-lp reads TSM and WAL files as they are. It can return points that
//! no query returns: overwritten or deleted points not yet compacted, and
//! values of a field that is missing from the shard's field index (seen in a
//! real bucket after type-conflict clean-ups). Such a window looks different
//! here, so the caller re-checks every window that differs through Flux
//! before it reports a mismatch.

use crate::convert::Rules;
use crate::deep::ChunkSink;
use crate::error::{Kind, fail};
use crate::fastlp::{LineError, LpConverter, MAX_LINE_BYTES};
use crate::guard::aborted_error;
use crate::pipeline::LineSink;
use crate::window::{Window, fmt_time};
use std::io::{BufRead, BufReader, Read};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::debug;

pub struct Export {
    program: String,
    prefix: Vec<String>,
    engine_path: String,
}

/// Kills the export process if reading stops early (error, abort).
struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Export {
    /// `influxd` is the command that runs influxd, split on spaces.
    pub fn new(influxd: &str, engine_path: &str) -> anyhow::Result<Self> {
        let mut parts = influxd.split_whitespace().map(str::to_owned);
        let program = parts.next().ok_or_else(|| {
            fail(Kind::Usage, "--influxd is empty")
                .hint("pass the influxd command, e.g. --influxd influxd or --influxd 'docker exec influxdb influxd'")
        })?;
        Ok(Self {
            program,
            prefix: parts.collect(),
            engine_path: engine_path.to_owned(),
        })
    }

    fn command(&self, bucket_id: &str, start: i64, stop: i64) -> Command {
        let mut c = Command::new(&self.program);
        c.args(&self.prefix)
            .args(["inspect", "export-lp", "--bucket-id", bucket_id])
            .args(["--engine-path", &self.engine_path])
            .args(["--start", &fmt_time(start), "--end", &fmt_time(stop)])
            .args(["--output-path", "-", "--log-level", "error"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        c
    }

    /// Read `windows` (sorted, non-overlapping) of one bucket in a single
    /// export call. Returns the checksums of every window.
    pub fn scan(
        &self,
        bucket_id: &str,
        windows: &[Window],
        rules: &Rules,
        scope: &[(String, String)],
        abort: &AtomicBool,
    ) -> anyhow::Result<ChunkSink> {
        let mut sink = ChunkSink::new(windows.len());
        let (Some(first), Some(last)) = (windows.first(), windows.last()) else {
            return Ok(sink);
        };
        let mut cmd = self.command(bucket_id, first.start, last.stop);
        debug!(command = ?cmd, "export-lp");
        let child = cmd.spawn().map_err(|e| {
            fail(
                Kind::Usage,
                format!("cannot run `{} inspect export-lp`: {e}", self.program),
            )
            .hint("export-lp needs the influxd binary and the engine directory: run the tool where both are, or pass --influxd 'docker exec <container> influxd'")
        })?;
        let mut run = Running(child);
        let stdout = run.0.stdout.take().expect("piped stdout");
        let mut stderr = run.0.stderr.take().expect("piped stderr");
        let err_reader = std::thread::spawn(move || {
            let mut s = String::new();
            let _ = stderr.read_to_string(&mut s);
            s
        });

        let mut input = BufReader::with_capacity(1 << 20, stdout);
        let mut conv = LpConverter::new(rules, scope, 86_400);
        let mut line = Vec::with_capacity(4096);
        let mut skipped = Vec::new();
        let mut lines: u64 = 0;
        loop {
            line.clear();
            if input.read_until(b'\n', &mut line)? == 0 {
                break;
            }
            lines += 1;
            if lines & 0xfff == 0 && abort.load(Ordering::Relaxed) {
                return Err(aborted_error());
            }
            loop {
                // Pick the window from the timestamp, convert straight into its sink.
                let win = timestamp(&line).and_then(|ts| {
                    let i = windows.partition_point(|w| w.start <= ts);
                    (i > 0 && ts < windows[i - 1].stop).then(|| i - 1)
                });
                let out = match win {
                    Some(i) => {
                        sink.current = i;
                        sink.buf()
                    }
                    None => {
                        skipped.clear();
                        &mut skipped
                    }
                };
                match conv.process(&line, out) {
                    Ok(n) => {
                        if win.is_some() && n > 0 {
                            sink.commit(n)?;
                        }
                        break;
                    }
                    Err(LineError::Unterminated) => {
                        if line.len() > MAX_LINE_BYTES || input.read_until(b'\n', &mut line)? == 0 {
                            return Err(fail(
                                Kind::Internal,
                                format!("export-lp line {lines}: unterminated string"),
                            )
                            .into());
                        }
                    }
                    Err(LineError::Invalid(msg)) => {
                        return Err(
                            fail(Kind::Internal, format!("export-lp line {lines}: {msg}")).into(),
                        );
                    }
                }
            }
        }
        sink.flush();
        drop(input);
        let status = run.0.wait()?;
        let err = err_reader.join().unwrap_or_default();
        if !status.success() {
            let msg = err.trim();
            let kind = if msg.contains("not found") || msg.contains("no such file") {
                Kind::NotFound
            } else {
                Kind::Internal
            };
            return Err(fail(
                kind,
                format!(
                    "influxd inspect export-lp failed ({status}): {}",
                    msg.lines().last().unwrap_or("no message")
                ),
            )
            .hint("check --engine-path (the directory that contains data/ and wal/) and that the bucket id exists there")
            .into());
        }
        debug!(bucket_id, lines, "export-lp done");
        Ok(sink)
    }
}

/// The timestamp of a complete line protocol line (its last token).
fn timestamp(line: &[u8]) -> Option<i64> {
    let mut end = line.len();
    while end > 0 && line[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    let start = line[..end].iter().rposition(|b| *b == b' ')? + 1;
    std::str::from_utf8(&line[start..end]).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamp_is_last_token() {
        assert_eq!(
            timestamp(b"m,t=a v=1 1700000000000000000\n"),
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(timestamp(b"m s=\"a b\" 5\r\n"), Some(5));
        assert_eq!(timestamp(b"garbage"), None);
    }

    /// A fake influxd: prints line protocol to stdout and checks the arguments.
    #[cfg(unix)]
    #[test]
    fn routes_lines_into_windows_and_converts() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let script = dir.path().join("influxd");
        std::fs::write(
            &script,
            "#!/bin/sh\n[ \"$1 $2\" = \"inspect export-lp\" ] || exit 3\n\
             printf 'm,application=x v=1i 10\\nm,application=x s=\"5 kWh\" 20\\nm,application=x s=\"\" 21\\nm,application=y v=2i 25\\nm v=9 99\\n'\n",
        )
        .unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        let e = Export::new(script.to_str().unwrap(), "/engine").unwrap();
        let rules = Rules::default();
        let scope = vec![("application".to_owned(), "x".to_owned())];
        let w = [
            Window { start: 0, stop: 20 },
            Window {
                start: 20,
                stop: 30,
            },
        ];
        let mut s = e
            .scan("b", &w, &rules, &scope, &AtomicBool::new(false))
            .unwrap();
        assert_eq!(s.window(0).values, 1);
        // "5 kWh" -> 5, "" dropped, y out of scope kept, 99 outside all windows.
        let w1 = s.window(1);
        assert_eq!(w1.values, 2);
        assert_eq!(w1.series.len(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn failing_export_is_reported() {
        let e = Export::new("false", "/engine").unwrap();
        let w = [Window { start: 0, stop: 10 }];
        let err = e
            .scan("b", &w, &Rules::default(), &[], &AtomicBool::new(false))
            .err()
            .expect("export must fail");
        assert!(format!("{err:#}").contains("export-lp failed"));
        let e = Export::new("/nonexistent/influxd", "/engine").unwrap();
        let err = e
            .scan("b", &w, &Rules::default(), &[], &AtomicBool::new(false))
            .err()
            .expect("export must fail");
        assert_eq!(crate::error::classify(&err).0, Kind::Usage);
    }
}
