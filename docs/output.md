# JSON output (`--output json`)

Every command prints exactly one object to stdout:

```json
{
  "tool": "loxinflux-migration-helper",
  "version": "0.1.0",
  "command": "migrate",
  "status": "ok",
  "exit_code": 0,
  "result": { "...": "command specific, see below" },
  "error": { "kind": "verify_mismatch", "message": "...", "hint": "..." },
  "warnings": ["..."],
  "next_steps": ["loxinflux-migration-helper verify --state ..."]
}
```

* `status` is `ok` or `error`; `exit_code` equals the process exit code.
* `error` is present only when `status` is `error`. `kind` is one of
  `usage`, `connection`, `auth`, `not_found`, `write_rejected`,
  `verify_mismatch`, `refused`, `resource_limit`, `interrupted`, `internal`.
* `analyze`, `migrate` and `verify` results contain `resources`:
  `peak_rss_mib`, `memory_limit_mib`, and for migrate `influx_heap_start_mib`,
  `influx_heap_peak_mib`, `influx_paused_secs` (when the InfluxDB guard is active).
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
| `windows_with_influx_query_bug` | windows where InfluxDB panicked on the pushed-down queries and rows were scanned instead |
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
| `elapsed_secs`, `values_per_sec`, `window_secs`, `rules`, `convert_where_tag` | |

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
| `windows[]` | only windows that differ: `start`, `stop`, `expected`, `actual`, `missing`, `extra`, `missing_by_measurement`, with `--deep` also `deep` (see below) |
| `deep` | with `--deep`: `read_via` (`flux` or `export-lp`), `windows_compared`, `windows_with_differences`, `series_compared`, `series_missing`, `series_different`, `series_extra`, `source_values`, `target_values`, `examples[]` (up to 20 `window: series field (what)`); export-lp only: `windows_rechecked_via_flux`, `source_values_only_on_disk` (see [verify-export.md](verify-export.md)) |
| `windows[].rechecked_via_flux`, `windows[].source_values_only_on_disk` | export-lp only: the window differed there and its result comes from Flux; source values on disk that no query returns |

With `--deep`, the counts (`actual`, `missing`, `extra`) of compared windows
come from the deep comparison; only windows skipped by `--sample-every` run a
separate count query.

## `recover`

| Field | Meaning |
|---|---|
| `source`, `target`, `dry_run`, `state_file`, `window_secs`, `export_chunk_days` | `export_chunk_days`: days per export-lp call (one shard group by default) |
| `windows_total`, `windows_skipped_already_done`, `windows_checked_now`, `windows_with_hidden_values` | day windows in `--start`/`--stop` |
| `visible_query_fallbacks` | windows whose visible pairs had to be read row by row (InfluxDB panicked on the pushed-down query) |
| `lines_read`, `lines_per_sec` | export-lp lines read |
| `hidden_pairs`, `hidden_values` | (measurement, field) pairs and values on disk that no query returned in their window |
| `values_written`, `values_dropped` | after the conversion rules (dropped: empty strings, placeholders, ...) |
| `pairs[]` | every hidden pair, most values first: `measurement`, `field`, `values` |
| `windows[]` | windows with hidden values: `start`, `hidden_pairs`, `hidden_values`, `values_written` |
| `write_requests`, `rejected_batches`, `elapsed_secs`, `input_types`, `actions`, `rules`, `convert_where_tag`, `resources` | as for `migrate` |

A value counts as visible in a window (one day) when the query API returns
values of the same measurement, field and type there (`first()` per series and
field, or a row scan when InfluxDB fails on that). Visible values are never
written. Hidden are fields missing from a shard's field index and values whose
stored type differs from the type the query returns for that field (left
behind by type conflicts), e.g. `"19.9°"` strings in a float field.

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

`migrate` writes `lmh-state-<source>-to-<target>[-dry-run].jsonl` (JSON lines):
a header `{"kind":"header","version":2,"source","target","dry_run","window_secs","rules","convert_where_tag"}`,
then one line per finished window `{"kind":"window","start_ns","stop_ns",
"values_read","values_written","values_dropped","rejected_batches",
"written_by_measurement","finished_at"}`. After `--redo` a window can appear
again; the last line wins. In line protocol mode `values_read` and
`values_dropped` are 0 per window (totals are in the migrate result).

`recover` writes `lmh-recover-<source>-to-<target>[-dry-run].jsonl` in the same
format, with `"source":"recover:<bucket>"`; per window `values_read` is the
number of hidden values. Windows are recorded per export chunk, after all its
batches were acknowledged.
