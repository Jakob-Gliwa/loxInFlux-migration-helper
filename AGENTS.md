# AGENTS.md

Guidance for LLM agents that **run** this tool on someone's InfluxDB, and for
agents that **change** its code.

## Running the tool

### Contract

* Always pass `--output json`. stdout then holds exactly one JSON object;
  everything else (logs, progress) is on stderr. Schema: [docs/output.md](docs/output.md).
* Decide by `exit_code` / `error.kind`, not by parsing text:
  `0` ok · `2 usage` · `3 connection` · `4 auth` · `5 not_found` ·
  `6 write_rejected` · `7 verify_mismatch` · `8 refused` · `9 resource_limit` · `10 interrupted` · `1 internal`.
* `error.hint` says what to change; `next_steps` holds copy-ready follow-up
  commands. They are suggestions — check them against the user's goal.
* Configure via environment, never interactively: `INFLUX_URL`, `INFLUX_ORG`,
  `INFLUX_TOKEN` (also `LMH_OUTPUT=json`, `LMH_LOG_FORMAT=json`, `LMH_LOG_LEVEL`).
  The tool never prompts. Do not echo the token into logs or chat.
* For machine-readable progress add `--log-format json`; each stderr line is a
  JSON object with `level`, `message` and fields such as `window` (`"12/150"`),
  `values_read`, `values_written`.

### Safe and unsafe commands

| Command                                   | Effect                                    | Needs human approval |
|-------------------------------------------|-------------------------------------------|----------------------|
| `convert-value`                           | offline                                   | no                   |
| `analyze`                                 | read-only                                 | no                   |
| `migrate --dry-run`                       | read-only, writes a local state file      | no                   |
| `migrate`                                 | writes into the **target** bucket only    | yes (creates load, may create a bucket) |
| `verify`                                  | read-only                                 | no                   |
| `recover --dry-run`                       | read-only (export-lp), local state file   | no                   |
| `recover`                                 | writes hidden values into the **target**  | yes                  |
| `cutover` (without `--execute`)           | read-only plan                            | no                   |
| `cutover --execute --yes`                 | renames production buckets                | **yes, always**      |

Never add `--yes` on your own. Never point `migrate` at a target that other
systems already write to unless the user said so (dual-write is a deliberate
choice; `verify` will then report extra values as a warning).

### Recommended loop

1. `analyze --bucket B --start <date> --classify-strings --output json`
   → report `fields_with_type_conflicts`, `values_by_type`, `migrate_estimate`
   to the user.
2. `migrate ... --dry-run --output json` → check `values_written`,
   `values_dropped`, `actions`. Explain drops (empty strings, placeholders).
3. With approval: `migrate ... --create-target --output json`.
   * Exit 3, 10 or a timeout: rerun the identical command; finished windows are
     skipped (state file). Do not delete the state file.
   * Exit 8 "in use": another run holds the state file; do not start a second one.
   * Exit 9: a memory guard stopped the run. Rerun with lower `--parallel`,
     `--writers` or `--batch-mb` (own memory) or wait and/or lower `--writers`
     (InfluxDB heap). Finished windows are kept.
   * Exit 6: the target already has conflicting types — usually the wrong
     target bucket. Stop and ask.
4. `verify --state <file> --deep --output json` (`--deep` reads both buckets
   fully; for a quick check drop it or use `--sample-every 7`). If the tool runs
   where `influxd` and the engine directory are available, add
   `--read-via export-lp` (faster; see docs/verify-export.md). A warning about
   `source_values_only_on_disk` means the source has values no query returns:
   report it to the user; it is not a migration error. With approval, copy them
   with `recover` (step 4b).
   * Exit 7: rerun `migrate` with `--redo --start <window start> --stop <window stop>`
     for the windows listed in `result.windows` (or use `next_steps[0]`), then verify again.
4b. Optional, needs `influxd` and the engine directory like export-lp:
   `recover --source-bucket <old> --target-bucket <migrated> --start .. --stop ..
   <same rule flags and --convert-where-tag as migrate> --dry-run --output json`
   → report `hidden_values`, `hidden_pairs` and the top of `pairs` (and
   `visible_query_fallbacks` if > 0) → with approval the same command without
   `--dry-run`. It writes only (measurement, field) pairs that the query API does
   not return in a window, so values the migration copied are never touched.
   Rerun after exit 3/9/10 resumes per export chunk (`--export-chunk-days`).
   Then `verify --deep --read-via export-lp` with the migration's state file.
5. `cutover ... --output json` (plan) → show `warnings` (tokens bound to the old
   bucket ID, InfluxQL mappings) to the user → only with approval
   `cutover ... --execute --yes`.

### Gotchas

* `--start` must be before the oldest data, otherwise older data is silently
  out of scope. When unsure, use an early date; empty windows are cheap.
* `--measurement` limits what is copied at all; `--convert-where-tag` limits
  what is converted (the rest is copied unchanged). For a complete migration
  use `--convert-where-tag`, not `--measurement`.
* A dry run uses its own state file (`…-dry-run.jsonl`); it cannot be verified.
* `recover` keeps its own state file (`lmh-recover-…jsonl`); do not pass it to
  `verify`, which expects the migration's state file.
* On a production server, keep it gentle: `--writers 2` and/or `--max-rate`.
  The run is CPU-bound on the InfluxDB side.

## Changing the code

* Rust 2024, MSRV 1.99. Before committing:
  `cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test`.
* End-to-end: `tests/e2e.sh <binary>` against a throwaway InfluxDB v2 (see the
  script header). CI runs it against `influxdb:2`.
* Public contract — keep stable within 0.x minors, document changes in
  CHANGELOG.md: exit codes and `error.kind` names (`src/error.rs`), JSON field
  names (`docs/output.md`), action names (`src/convert.rs`), state file format
  (`src/state.rs`, bump `VERSION` on incompatible changes).
* Conversion rules must stay identical to loxInFlux 0.5
  (`loxinflux/src/value.rs`). If loxInFlux changes them, change both and add a
  test case to `src/convert.rs`.
* Logs: `tracing` with structured fields (`info!(window = %label, values_read = n, "window migrated")`).
  Message = what happened, fields = numbers. Never log the token.
* Every user-facing failure is a `Failure` with a `Kind` and, where possible, a
  `hint` that tells the reader what to do next.
