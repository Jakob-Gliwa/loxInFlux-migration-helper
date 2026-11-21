# JSON output (`--output json`)

Every command prints exactly one object to stdout:

```json
{
  "tool": "loxinflux-migrationshelper",
  "version": "0.1.0",
  "command": "migrate",
  "status": "ok",
  "exit_code": 0,
  "result": { "...": "command specific, see below" },
  "error": { "kind": "verify_mismatch", "message": "...", "hint": "..." },
  "warnings": ["..."],
  "next_steps": ["loxinflux-migrationshelper verify --state ..."]
}
```

* `status` is `ok` or `error`; `exit_code` equals the process exit code.
* `error` is present only when `status` is `error`. `kind` is one of
  `usage`, `connection`, `auth`, `not_found`, `write_rejected`,
  `verify_mismatch`, `refused`, `internal`.
* `result` can be present **together with** `error` — e.g. `verify` reports the
  gaps it found and exits with 7. If the command failed before producing a
  result, `result` is absent.
* Times are RFC 3339 UTC strings. Counts are numbers of field values (one line
  protocol line can carry several fields).

## `analyze`

| Field | Meaning |
|---|---|
| `bucket`, `shard_group_secs`, `retention_secs` (0 = infinite) | the inspected bucket |
| `start`, `stop`, `windows`, `windows_with_data` | analysed range |
| `values_total`, `values_by_type` | `float`, `integer`, `unsigned`, `boolean`, `string` |
| `fields_total`, `fields_with_type_conflicts` | measurement+field pairs; pairs with more than one type across windows |
| `migrate_estimate` | action → count (see actions below); `string_unclassified` without `--classify-strings` |
| `strings_classified` | whether `--classify-strings` was used |
| `fields[]` | per pair: `measurement`, `field`, `values`, `types{type: {values, windows, first_window, last_window}}`, with classification also `string_actions{action: n}` and up to 3 `string_examples` per action. Sorted: conflicts first, then by values |

## `migrate`

| Field | Meaning |
|---|---|
| `source` | bucket name, or `file:<path>` for `--from-lp` |
| `target`, `dry_run`, `state_file`, `target_created` | |
| `windows_total`, `windows_skipped_already_done`, `windows_migrated_now` | |
| `values_read`, `values_written`, `values_dropped` | totals of this run |
| `input_types` | type → values read |
| `actions` | action → values |
| `write_requests`, `rejected_batches` | HTTP writes; rejected batches (only with `--on-reject skip`) |
| `elapsed_secs`, `rules`, `convert_where_tag` | |

Actions: `float_kept`, `int_to_float`, `unsigned_to_float`, `bool_to_float`,
`bool_kept`, `string_to_float`, `string_with_unit_to_float`,
`string_to_text_field`, `text_field_kept`, `string_kept`, `copied_unchanged`,
`dropped_empty`, `dropped_placeholder`, `dropped_non_finite`.

## `verify`

| Field | Meaning |
|---|---|
| `source`, `target`, `state_file` | |
| `windows_checked`, `windows_ok`, `windows_with_missing`, `windows_with_extra` | |
| `expected_total`, `actual_total`, `missing_total`, `extra_total` | values |
| `missing_by_measurement` | measurement → missing values (all windows) |
| `windows[]` | only windows that differ: `start`, `stop`, `expected`, `actual`, `missing`, `extra`, `missing_by_measurement` |

## `cutover`

| Field | Meaning |
|---|---|
| `executed` | false for the plan, true after renaming |
| `source`, `source_id`, `target`, `target_id`, `archive_name` | |
| `steps[]` | the plan in order |
| `tokens_bound_to_source[]`, `tokens_bound_to_target[]` | `id`, `description`, `actions` — tokens that address a bucket by ID |
| `dbrp_mappings_of_source[]` | `id`, `database`, `retention_policy` — InfluxQL mappings by ID |

## `convert-value`

`field`, `input_type`, `input`, `action`, `output_field`, `output_type`,
`output_line_protocol` (the `field=value` part, `null` when dropped).

## State file

`migrate` writes `lmh-state-<source>-to-<target>[-dry-run].json`:
`version` (1), `source`, `target`, `dry_run`, `file_points_done` (export-file
mode), and `windows`: window start in ns (as string key) →
`{stop_ns, values_read, values_written, values_dropped, rejected_batches,
written_by_measurement, finished_at}`.
