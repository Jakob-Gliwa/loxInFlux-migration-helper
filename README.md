# loxInFlux-migration-helper

Upgrading [loxInFlux](https://github.com/Jakob-Gliwa/loxInFlux) from 0.3 to 0.5
changes how values are stored in InfluxDB: every number becomes a float, and
text moves to its own field. This tool converts your existing history the same
way, so old and new data fit together and your graphs keep working.

**→ Step-by-step guide: [Switching from loxInFlux 0.3 to 0.5](docs/switchover.md)**

## Do I need it?

Yes, if loxInFlux 0.3 has been writing into an InfluxDB v2 bucket and you want
to keep that history in the same place after upgrading to 0.5.

Why you cannot simply point 0.5 at the old bucket: InfluxDB decides the type of
a field (integer, float or text) when it is first written, and keeps it. 0.3
stored many numbers as integers (`42i`) or as text (`"19.8°"`). When 0.5 sends
`42.0` or `19.8` into such a field, InfluxDB rejects the points with
`field type conflict`, and that data is lost. Where the types do get mixed
across time ranges, queries silently show only one of them, and your history
looks like it has gaps.

The types of stored data cannot be changed in place. So this tool copies the
bucket into a new one, converting every value exactly like loxInFlux 0.5 does,
and then swaps the bucket names.

## How it works

```
analyze      →   migrate        →   verify          →   cutover
look at the      copy into a        compare every       swap the bucket
data             new bucket         value               names
(read-only)      (old one stays)    (read-only)
```

* Your original bucket is **never changed**. After the switch it is kept as
  `<name>_pre_migration`.
* You can **stop and resume** at any time. Progress is saved per day in a small
  state file; running the same command again continues where it stopped.
* `verify --deep` **proves** the result: every value with its time stamp is
  compared with the original.
* It stays **light on your server**: the tool needs well under 100 MB of memory
  and slows down by itself if InfluxDB gets short of memory.

## Install

Pick one:

* **Docker** (no installation):

  ```sh
  docker run --rm -v "$PWD:/work" -w /work \
    -e INFLUX_URL=http://<influx-host>:8086 -e INFLUX_ORG=<org> -e INFLUX_TOKEN=<token> \
    ghcr.io/jakob-gliwa/loxinflux-migration-helper analyze --bucket loxinflux --start 2020-01-01
  ```

* **Binary** for Linux, macOS or Windows from the
  [releases page](https://github.com/Jakob-Gliwa/loxInFlux-migration-helper/releases).
* **From source**:
  `cargo install --git https://github.com/Jakob-Gliwa/loxInFlux-migration-helper`

Use an InfluxDB token with full access to the org (an operator or all-access
token). The connection is set with `INFLUX_URL`, `INFLUX_ORG` and
`INFLUX_TOKEN`, or with `--url`, `--org` and `--token`.

## In short

The [guide](docs/switchover.md) explains each step, what to check and how to go
back. The core commands:

```sh
# 1. Look at the data (read-only)
loxinflux-migration-helper analyze --bucket loxinflux --start 2020-01-01 --classify-strings

# 2. Copy the history into a new bucket, up to today 00:00 UTC (resumable)
loxinflux-migration-helper migrate --source-bucket loxinflux --target-bucket loxinflux_v2 \
  --create-target --start 2020-01-01 --stop 2026-11-21 \
  --convert-where-tag application=loxInFlux --state state.jsonl

# 3. Check every value
loxinflux-migration-helper verify --state state.jsonl --deep

# 4. During the switch: stop 0.3, copy today, swap the names, start 0.5
loxinflux-migration-helper migrate ... --start 2026-11-21 --stop 2026-11-22 --state state.jsonl
loxinflux-migration-helper cutover --source-bucket loxinflux --target-bucket loxinflux_v2 --execute --yes
```

`--start` must be before your oldest data. Add `--dry-run` to `migrate` for a
rehearsal that reads and converts everything but writes nothing.

## What gets converted

The same rules as loxInFlux 0.5:

| Stored by 0.3                             | After the migration           |
| ----------------------------------------- | ----------------------------- |
| `AQ=7i` (integer)                         | `AQ=7.0`                      |
| `Default=true`                            | `Default=1.0`                 |
| `Default="21.5"`                          | `Default=21.5`                |
| `Default="19.8°"`, `"523ppm"`, `"-331 W"` | `Default=19.8`, `523.0`, …    |
| `Default="On"`, `"1,5 kW"`, `"12:30"`     | `__text__Default="On"`, …     |
| `API=""` (empty)                          | dropped                       |
| `Default="<v.1> °C"` (placeholder)        | dropped                       |

Floats stay as they are. Tags and time stamps are never changed. Comma decimals,
dates, times and sentences that only start with a number stay text.

Each rule can be switched off, matching the `[processing]` options of
loxInFlux 0.5: `--keep-bools`, `--no-parse-units`, `--keep-empty`,
`--keep-placeholders`, `--no-separate-text`. Use the same choice in both places.
To test a single value: `loxinflux-migration-helper convert-value --value '19.8°'`.

**Other data in the same bucket?** If Telegraf also writes other sources into
this bucket, add `--convert-where-tag application=loxInFlux`. Only loxInFlux data
is converted then; everything else is copied unchanged.

## Useful options

| Option                        | What it does                                                        |
| ----------------------------- | ------------------------------------------------------------------- |
| `--dry-run`                   | read and convert, write nothing                                     |
| `--state FILE`                | where progress is saved (needed for resume, `verify` and catch-up)  |
| `--redo`                      | copy the days in `--start`/`--stop` again                           |
| `--measurement NAME`          | copy only this measurement (repeatable)                             |
| `--target-url/-token/-org`    | write into a different InfluxDB, e.g. a test instance               |
| `--max-rate N`                | at most N values per second, to keep the server responsive          |
| `--output json`               | machine-readable result, see [docs/output.md](docs/output.md)       |
| `verify --read-via export-lp` | read the storage files directly, several times faster (experimental, [details](docs/verify-export.md)) |
| `recover`                     | copy values that only the storage files still hold (see below)      |

All options: `loxinflux-migration-helper <command> --help`.

## Data that InfluxDB no longer shows

InfluxDB can lose track of fields in its index while the values are still on
disk. Queries, Grafana and therefore a normal migration then skip them without
any error. `verify --deep --read-via export-lp` reports such values as
`source_values_only_on_disk`.

`recover` copies exactly these values. It reads the storage files with
`influxd inspect export-lp`, keeps only the measurement/field pairs that a
query does not return, converts them with the same rules and writes them into
the migrated bucket. Values the migration already copied are left alone.

```sh
# what would be recovered (read-only)
loxinflux-migration-helper recover --source-bucket loxinflux_pre_migration --target-bucket loxinflux \
  --start 2020-01-01 --stop 2026-11-21 --convert-where-tag application=loxInFlux \
  --influxd "docker exec influxdb influxd" --dry-run

# then the same without --dry-run
```

Use the same rule options as for `migrate`. Like `verify --read-via export-lp`,
it must run where it can start `influxd` with access to the storage directory
(see [docs/verify-export.md](docs/verify-export.md)).

## When something goes wrong

Every run ends with a short summary and, if something failed, a hint on what to
do next. Interrupted runs (Ctrl-C, lost connection, reboot) are continued by
running the same command again.

| Exit code | Meaning                                                    |
| --------- | ---------------------------------------------------------- |
| 0         | success                                                    |
| 1         | unexpected error, please open an issue                     |
| 2         | wrong option or input                                      |
| 3         | InfluxDB not reachable                                     |
| 4         | token rejected or missing a permission                     |
| 5         | bucket, org or state file not found                        |
| 6         | InfluxDB rejected points                                   |
| 7         | `verify` found differences                                 |
| 8         | refused because it would be unsafe                         |
| 9         | stopped to protect memory (yours or InfluxDB's); rerun     |
| 10        | stopped by Ctrl-C or `docker stop`; rerun to continue      |

## Limits

* InfluxDB **2.x** only (tested with 2.9), not 1.x or 3.x.
* Values that InfluxDB already rejected under 0.3 (type conflicts) were never
  stored and cannot be recovered.
* For very dense buckets, use a smaller `--window` (e.g. `6h`) if a day takes
  longer than `--timeout-secs` (default 900) to read.

## For scripts and AI agents

Every command can print a single JSON result (`--output json`) with stable exit
codes and suggested next steps; logs go to stderr. See [AGENTS.md](AGENTS.md).

## Development

```sh
cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test
tests/e2e.sh target/release/loxinflux-migration-helper   # against a throwaway InfluxDB
```

## License

MIT, see [LICENSE](LICENSE).
