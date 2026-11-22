# Switching from loxInFlux 0.3 to 0.5, step by step

This guide takes you from a running loxInFlux 0.3 (Python) to loxInFlux 0.5
(Rust), with your whole history converted, in one bucket under the old name.
The actual interruption is a few minutes. Everything before that runs while
0.3 keeps writing as usual.

```
 prepare (hours, no downtime)              switch (minutes)                 afterwards
 ───────────────────────────────           ───────────────────────────      ─────────────
 1 back up                                 7 stop 0.3                       10 check
 2 analyze                                 8 copy today, rename buckets     11 clean up
 3 choose the rules                        9 swap tokens, start 0.5
 4 copy the history
 5 verify (+ recover, optional)
 6 prepare config and tokens
```

The examples use the bucket `loxinflux` and the org `home`. Replace them with
yours. `lmh` stands for `loxinflux-migration-helper`; see the
[README](../README.md#install) for how to run it. Set the connection once:

```sh
export INFLUX_URL=http://influx:8086 INFLUX_ORG=home INFLUX_TOKEN=<operator token>
```

## Prepare (no downtime)

### 1. Back up

```sh
influx backup --bucket loxinflux /backups/loxinflux-before-0.5
```

The tool never changes or deletes the old data, but a backup is cheap
insurance. Also pin the old image to `acidcliff/loxinflux:0.3` so 0.3 does not
update itself to 0.5 before you are ready.

### 2. Look at what is stored

```sh
lmh analyze --bucket loxinflux --classify-strings
```

This is read-only. It shows the time range, how many values there are, which
fields already have type conflicts and what would happen to the strings.

### 3. Choose the conversion rules

The history should end up looking exactly like what 0.5 will write from now on.
So pick the same rules in both places:

| What                                  | Tool flag (default: on)  | loxInFlux 0.5 `[processing]` |
| ------------------------------------- | ------------------------ | ---------------------------- |
| `"19.6°"`, `"523ppm"` become numbers  | `--no-parse-units`       | `parse_numeric_units`        |
| empty strings are dropped             | `--keep-empty`           | `drop_empty`                 |
| placeholders like `<v.1>` are dropped | `--keep-placeholders`    | `drop_placeholders`          |
| text goes to `__text__<field>`        | `--no-separate-text`     | `separate_text_fields`       |

Example: if you migrate with `--keep-placeholders`, put
`drop_placeholders = false` into the loxInFlux 0.5 config.

If other programs write into the same bucket (Home Assistant, MQTT via
Telegraf, ...), add `--convert-where-tag application=loxInFlux`. Then only
loxInFlux data is converted and everything else is copied unchanged.

### 4. Copy the history into a new bucket

Copy everything up to **today, 00:00 UTC**. Today itself is copied later, during
the switch.

```sh
lmh migrate --source-bucket loxinflux --target-bucket loxinflux_v2 --create-target \
  --start 2020-01-01 --stop 2026-11-21 \
  --convert-where-tag application=loxInFlux --keep-placeholders \
  --state state.jsonl
```

* Use dates without a time for `--start` and `--stop`. The tool works in day
  windows, and the later catch-up continues exactly at `--stop`.
* Keep `state.jsonl`. It records which days are done. If the run stops (Ctrl-C,
  reboot, error), start the same command again and it continues.
* Plan for a while: InfluxDB takes in roughly 100,000 to 200,000 values per
  second. The tool slows down by itself if InfluxDB runs short of memory.
* To rehearse first, add `--dry-run`: it reads and converts everything but
  writes nothing.

### 5. Verify

```sh
lmh verify --state state.jsonl --deep
```

`--deep` compares every single value per series: time stamp and value, after
applying the rules. Exit code 0 means the new bucket matches. It takes about as
long as the copy. Days that differ can be copied again with
`lmh migrate ... --start <day> --stop <next day> --redo`.

**Optional: values only the storage files hold.** If you can run the tool
next to InfluxDB with access to its storage directory, compare through the
storage files instead (`--read-via export-lp`, see
[verify-export.md](verify-export.md)). A warning about
`source_values_only_on_disk` means InfluxDB has values on disk that no query
returns (fields missing from its index, or values of a conflicting type). They were invisible before, so the
migration could not copy them. `recover` writes exactly those into the new
bucket:

```sh
lmh recover --source-bucket loxinflux --target-bucket loxinflux_v2 \
  --start 2020-01-01 --stop 2026-11-21 \
  --convert-where-tag application=loxInFlux --keep-placeholders \
  --influxd "docker exec influxdb influxd" --dry-run    # then without --dry-run
```

Use the same rules as in step 4. Afterwards `verify --deep --read-via export-lp`
should show no differences.

### 6. Prepare the switch

Nothing here changes the running system yet.

**Find everything that uses the bucket.** Renaming a bucket keeps its ID.
Anything that refers to the bucket by **name** follows the rename by itself:
the Telegraf output (`bucket = "loxinflux"`), Flux queries
(`from(bucket: "loxinflux")`) and InfluxQL queries (`db=loxinflux`) through the
automatic mappings InfluxDB creates per bucket. Anything bound to the **ID**
does not follow, mainly tokens. List them:

```sh
lmh cutover --source-bucket loxinflux --target-bucket loxinflux_v2
```

This is a dry run. It lists every token whose permissions name the old bucket,
and InfluxQL mappings that were created by hand.

**Create new tokens.** InfluxDB cannot change the permissions of an existing
token, so each consumer needs a new token for the new bucket. The bucket ID of
`loxinflux_v2` stays the same after the rename, so the new tokens can be
created now:

```sh
influx bucket list --name loxinflux_v2          # note the ID
influx auth create --org home --description "telegraf loxinflux" \
  --read-bucket <new id> --write-bucket <new id>
influx auth create --org home --description "grafana loxinflux" \
  --read-bucket <new id> --read-bucket <old id>   # old id = archive, optional
```

Typical consumers: Telegraf (writes), Grafana data sources, monitoring checks,
scripts. Note for each where its token is configured.

**Prepare the loxInFlux 0.5 configuration.** Copy your `config.toml` into a new
folder, so 0.3 keeps its own until the end, and:

* remove the `[paths]` section (0.5 stores nothing on disk),
* remove `round_floats` and `round_decimals` from `[telegraf]`
  (they never had an effect; rounding lives in `[general]`),
* add the `[processing]` settings that match step 3.

Check it with the new image:

```sh
docker run --rm -v ./config-0.5:/app/config:ro acidcliff/loxinflux:0.5 --check-config
```

**Check what reads the raw data downstream.** 0.5 writes numbers as floats and
genuine text as `__text__<field>`. Telegraf processors, Grafana panels or
exports that read a text field by its old name need to look at
`__text__<field>` now.

## Switch (a few minutes)

Do these steps in one go. Between step 7 and 9 nothing is recorded.

### 7. Stop loxInFlux 0.3

```sh
docker stop loxinflux
```

Wait about twice Telegraf's `flush_interval` (default 10 s), so the last points
reach InfluxDB.

### 8. Copy today, then swap the bucket names

```sh
lmh migrate --source-bucket loxinflux --target-bucket loxinflux_v2 \
  --start 2026-11-21 --stop 2026-11-22 \
  --convert-where-tag application=loxInFlux --keep-placeholders \
  --state state.jsonl

lmh cutover --source-bucket loxinflux --target-bucket loxinflux_v2 --execute --yes
```

Use the same rules and the same state file as in step 4. The catch-up takes
seconds. `cutover` renames `loxinflux` to `loxinflux_pre_migration` (your
archive) and `loxinflux_v2` to `loxinflux`.

### 9. Swap the tokens and start 0.5

* Put the new tokens into Telegraf, Grafana and your other consumers. Telegraf
  reads tokens only at start, so restart it.
* Start loxInFlux 0.5 with the new config folder. It does not need the old
  `/app/data` mount. Port 9108 is optional and gives `/health` and `/metrics`.

```sh
docker run -d --name loxinflux --restart unless-stopped \
  -v ./config-0.5:/app/config:ro -p 9108:9108 acidcliff/loxinflux:0.5
```

## Afterwards

### 10. Check

* `curl http://<host>:9108/health` returns `"status":"ok"`.
* Telegraf's log shows no `field type conflict` any more.
* New points arrive in `loxinflux` (for example the last 5 minutes in the
  InfluxDB data explorer).
* Your dashboards and monitoring checks still show data.

### 11. Clean up

* Delete the old tokens. They now only reach the archive.
* Delete the old `/app/data` folder of 0.3. Its `sps_*.xml` and `LoxAPP3.json`
  files contain your whole Miniserver program, including users and passwords.
* Keep `loxinflux_pre_migration` as long as you like. It is your way back to the
  original data.

## Going back

| Up to                 | How to undo                                                        |
| --------------------- | ------------------------------------------------------------------ |
| step 6                | delete `loxinflux_v2` and the new tokens; nothing else changed     |
| step 8 (before rename)| `docker start loxinflux`; 0.3 continues in the old bucket          |
| after step 8          | rename back (`loxinflux` → `loxinflux_v2`, `loxinflux_pre_migration` → `loxinflux`), restore the old tokens, start 0.3 |

Once 0.5 has written into the renamed bucket, starting 0.3 on it again does not
work: 0.3 writes integers into fields that are now floats, and InfluxDB rejects
those points. Use the rename-back route instead.

## Other order: run 0.5 first

You can also let 0.5 write into `loxinflux_v2` from the start (with its own
Telegraf output and token), copy the history afterwards and rename at the end.
That avoids the short gap, but you need to change the Telegraf output twice
(new bucket now, old name again after the rename). The steps above are simpler
if a few minutes without data are acceptable.
