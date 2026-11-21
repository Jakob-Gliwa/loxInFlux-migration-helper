# Changelog

All notable changes are listed here. The JSON output, exit codes, action names
and the state file format are a public contract (see AGENTS.md).

## 0.1.0 - unreleased

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
