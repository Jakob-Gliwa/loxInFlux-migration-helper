# Changelog

All notable changes are listed here. The JSON output, exit codes, action names
and the state file format are a public contract (see AGENTS.md).

## 0.1.0 - 2026-11-22

First release.

- `analyze`: field types per shard window, type conflicts across shards,
  optional classification of every string value.
- `migrate`: shard-window copy into a new bucket with the loxInFlux 0.5
  conversion rules; resumable checkpoint file, dry run, `--convert-where-tag`
  scope, gzip batches with retries, export-lp file input.
- `verify`: per-window and per-measurement comparison with the target.
- `cutover`: bucket rename with checks for ID-bound tokens and DBRP mappings.
- `convert-value`: offline preview of one conversion.
- `--output json` envelope, `--log-format json`, stable exit codes.
- Day-aligned windows, safe for shard durations the bucket used in the past.
- Parallel readers (`--parallel`) and writers (`--writers`) with a bounded
  buffer pool; allocation-free CSV and line protocol conversion.
- Memory guards: `--memory-limit-mb` for the tool, `--influx-heap-headroom-mb`
  pauses writes while InfluxDB's heap is high; exit code 9.
- `--target-url/--target-token/--target-org`, `--max-rate`, `--no-gzip`,
  `--from-lp -` (stdin), JSON-lines state file.
- `verify --deep`: per-series count and checksum of every value, source vs
  target (`--sample-every`). Deep windows take their counts from the
  comparison (no separate count query).
- `verify --deep --read-via export-lp`: reads both buckets with
  `influxd inspect export-lp` (`--influxd`, `--engine-path`,
  `--export-chunk-days`, one shard group per call by default); windows that
  differ are re-checked through Flux; reports source values stored on disk
  that no query returns.
- Clean stop on SIGINT/SIGTERM (exit 10), state file lock, repair of a
  partial last line, lost connections classified as exit 3.
- `analyze` works around an InfluxDB 2.x panic in pushed-down `first()`/`count()`
  ("unreachable: <nil>") by scanning the affected window; errors name the window.
- Command line errors are JSON with `--output json`; timeouts get a resume hint.
- SIMD (memchr) scanning in the line protocol path; offline dry run for files.
- Step-by-step switchover guide (`docs/switchover.md`) and a README written
  for people doing the migration; recovery of values InfluxDB no longer
  returns via `export-lp` and `--from-lp`.
- Name `loxinflux-migration-helper` (binary, image, JSON `tool` field).
- reqwest 0.13 (rustls, system certificate store); container images carry a
  CA bundle for https.
