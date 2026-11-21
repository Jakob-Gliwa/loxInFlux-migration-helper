# loxInFlux-migrationshelper

Move the history that [loxInFlux](https://github.com/Jakob-Gliwa/loxInFlux) 0.3
wrote into InfluxDB v2 to the schema of loxInFlux 0.5 — every number a float,
text in its own `__text__<field>` — so old and new data can be queried together.

```
analyze  ->  migrate --dry-run  ->  migrate  ->  verify  ->  cutover
(read-only)                         (new bucket)              (rename buckets)
```

The source bucket is never modified. Everything is resumable, every result is
available as JSON, and every failure has a stable exit code, so the tool can be
driven by scripts and LLM agents as well as by hand (see [AGENTS.md](AGENTS.md)).

## Why this is needed

loxInFlux 0.5 writes `42.0` where 0.3 wrote `42i`, `19.8` where 0.3 wrote the
string `"19.8°"`, and moves real text to `__text__Default`
([breaking changes](https://github.com/Jakob-Gliwa/loxInFlux#breaking-changes-in-050)).
InfluxDB fixes a field's type **per shard** when the field is first written:

* Writing a float into a field the current shard holds as integer or string
  fails with `422 field type conflict` and the points are dropped. (loxInFlux
  0.3 hits this too: it decides int vs float per value, so `"19"` and `"19.6"`
  of the same sensor already fight over the field type.)
* In the *next* shard the new type is accepted — but a Flux query that spans
  shards with different types for one field **silently returns only one of
  them**. Your history seems to have gaps although the data is there.

Field types cannot be changed in place. This tool therefore copies the bucket
shard by shard into a new bucket, converting each value with exactly the rules
loxInFlux 0.5 applies to new data, and then swaps the bucket names.

## Install

Download a binary from the [releases](https://github.com/Jakob-Gliwa/loxInFlux-migrationshelper/releases)
(Linux x86_64/arm64 static, macOS, Windows), or use the container image:

```sh
docker run --rm --network host -v "$PWD:/work" -w /work \
  -e INFLUX_URL=http://localhost:8086 -e INFLUX_ORG=home -e INFLUX_TOKEN=... \
  ghcr.io/jakob-gliwa/loxinflux-migrationshelper analyze --bucket loxinflux --start 2023-01-01
```

or build it: `cargo install --git https://github.com/Jakob-Gliwa/loxInFlux-migrationshelper`.

Connection settings come from options or environment variables:
`--url`/`INFLUX_URL` (default `http://localhost:8086`), `--org`/`INFLUX_ORG`,
`--token`/`INFLUX_TOKEN`. Use an all-access or operator token; `migrate` needs
read on the source and write on the target, `cutover` needs bucket write.

## Walkthrough

```sh
export INFLUX_URL=http://influx:8086 INFLUX_ORG=home INFLUX_TOKEN=...

# 1. What is stored? Types per shard window, conflicts, how strings would convert.
loxinflux-migrationshelper analyze --bucket loxinflux --start 2023-01-01 --classify-strings

# 2. Rehearse: reads and converts everything, writes nothing.
loxinflux-migrationshelper migrate --source-bucket loxinflux --target-bucket loxinflux_v2 \
  --start 2023-01-01 --convert-where-tag application=loxInFlux --dry-run

# 3. Point loxInFlux 0.5 (via Telegraf) at loxinflux_v2 now, so new data lands there,
#    then copy the history. Interrupt any time; rerun the same command to resume.
loxinflux-migrationshelper migrate --source-bucket loxinflux --target-bucket loxinflux_v2 \
  --start 2023-01-01 --convert-where-tag application=loxInFlux --create-target

# 4. Compare what was written with what the target holds, window by window.
loxinflux-migrationshelper verify --state lmh-state-loxinflux-to-loxinflux_v2.json

# 5. Swap names so dashboards that use the bucket name see the migrated data.
loxinflux-migrationshelper cutover --source-bucket loxinflux --target-bucket loxinflux_v2      # plan only
loxinflux-migrationshelper cutover --source-bucket loxinflux --target-bucket loxinflux_v2 --execute --yes
```

`--start` is required for API reads: pick a date before your oldest data.
`--stop` defaults to now.

## Conversion rules

Identical to loxInFlux 0.5 (`value.rs`), so migrated and new data match.

| Stored value (0.3)                | Written (0.5 schema)            | Action                      |
|-----------------------------------|---------------------------------|-----------------------------|
| `AQ=7i`                           | `AQ=7.0`                        | `int_to_float`              |
| `AQ=7u`                           | `AQ=7.0`                        | `unsigned_to_float`         |
| `Default=true`                    | `Default=1.0`                   | `bool_to_float`             |
| `AQ=21.25`                        | unchanged                       | `float_kept`                |
| `Default="21.5"`                  | `Default=21.5`                  | `string_to_float`           |
| `Default="19.8°"`, `"523ppm"`, `"-331 W"` | `Default=19.8` …        | `string_with_unit_to_float` |
| `Default="Ein"`, `"1,5 kW"`, `"12:30"` | `__text__Default="Ein"` …  | `string_to_text_field`      |
| `__text__Default="Ein"`           | unchanged                       | `text_field_kept`           |
| `API=""`                          | dropped                         | `dropped_empty`             |
| `Default="<v.1> °C"`              | dropped                         | `dropped_placeholder`       |
| `NaN`, `inf`                      | dropped                         | `dropped_non_finite`        |

Comma decimals, dates, times and sentences that merely start with a number stay
text. Tags and timestamps are copied unchanged. Running the tool over its own
output changes nothing.

Try a single value offline: `loxinflux-migrationshelper convert-value --value '19.8°'`.

The switches mirror loxInFlux's `[processing]` options: `--keep-bools`,
`--no-parse-units`, `--keep-empty`, `--keep-placeholders`, `--no-separate-text`.

### Buckets with data from other sources

Telegraf often writes several sources into one bucket. With
`--convert-where-tag application=loxInFlux` (repeatable, OR) only matching points
are converted; everything else is **copied unchanged**, original types included.
`--measurement NAME` (repeatable) instead restricts what is copied at all —
omit it for a complete move.

## How it works and what it guarantees

* **One shard group per query.** Windows are aligned to the source bucket's shard
  group duration, so every query sees exactly one type per field.
* **The source is read-only.** Source and target must differ (exit code 8 otherwise).
* **Resumable and idempotent.** After each window the checkpoint file
  (`lmh-state-<source>-to-<target>.json`) is replaced atomically. A rerun skips
  finished windows; rewriting a window overwrites identical points (same series,
  timestamp and field), so nothing is duplicated. `--redo` with a
  `--start/--stop` range re-migrates selected windows.
* **Verifiable.** `migrate` records how many values it wrote per window and
  measurement; `verify` counts what the target holds and reports gaps. Extra
  values (e.g. live data already arriving in the target) are reported as a
  warning, not an error.
* **Gentle on the server.** Writes are gzip-compressed batches (`--batch-size`,
  default 5000 values) with retries and backoff on 429/5xx; `--pause-ms` adds
  a pause between requests.

### From an export file instead of the API

If you have shell access to the InfluxDB host, `influxd inspect export-lp`
reads the storage files directly, which is faster for large buckets:

```sh
influxd inspect export-lp --bucket-id <id> --engine-path /var/lib/influxdb2/engine --output-path bucket.lp
loxinflux-migrationshelper migrate --from-lp bucket.lp --target-bucket loxinflux_v2 --create-target
```

Counts are recorded per target shard window, so `verify` works the same way.

### Cutover and what still points at the old bucket

Renaming keeps bucket IDs. Clients that use the **name** (Telegraf `bucket = …`,
Flux `from(bucket: …)` in Grafana) follow the rename. Things bound to the
**ID** do not, and `cutover` lists them before you execute:

* tokens whose permissions name the old bucket — create new ones for the
  migrated bucket;
* InfluxQL DBRP mappings (Grafana InfluxQL data sources) — remap them.

Stop the writers (e.g. Telegraf) for the few seconds between the two renames.
The old bucket stays as `<source>_pre_migration` until you delete it.

## Output, logs, exit codes

* **stdout:** one result. Human summary by default; `--output json` prints a
  single JSON envelope (`status`, `exit_code`, `result`, `error`, `warnings`,
  `next_steps`) — see [docs/output.md](docs/output.md).
* **stderr:** logs. Plain text by default (no colour unless a terminal),
  `--log-format json` for one JSON object per line. `--log-level debug` adds the
  Flux queries.

| Code | Kind             | Meaning                                                    |
|------|------------------|------------------------------------------------------------|
| 0    | –                | success                                                    |
| 1    | `internal`       | unexpected error (please report)                           |
| 2    | `usage`          | invalid option, time or input file                         |
| 3    | `connection`     | InfluxDB unreachable or not a v2 API                       |
| 4    | `auth`           | token rejected or missing a permission                     |
| 5    | `not_found`      | bucket, organisation or state file missing                 |
| 6    | `write_rejected` | InfluxDB rejected points (e.g. `field type conflict`)      |
| 7    | `verify_mismatch`| the target holds fewer values than were migrated           |
| 8    | `refused`        | unsafe request (same bucket, `--execute` without `--yes`)  |

## Limits

* InfluxDB **v2** (OSS 2.x; tested with 2.9). Not for 1.x or 3.x.
* API reads stream each shard window as CSV; one window must finish within
  `--timeout-secs` (default 900). Raise it for very dense buckets, or use the
  export-file mode.
* Points that InfluxDB already rejected in the past (type conflicts under 0.3)
  are not in the source and cannot be recovered.

## Development

```sh
cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test
# end-to-end against a throwaway InfluxDB (see the header of the script)
tests/e2e.sh target/release/loxinflux-migrationshelper
```

## License

MIT, see [LICENSE](LICENSE).
