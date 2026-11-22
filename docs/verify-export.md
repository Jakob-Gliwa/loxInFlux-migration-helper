# `verify --deep --read-via export-lp` (experimental)

`verify --deep` reads every value of the source and the target. Through the
query API (Flux) InfluxDB does most of the work: planning, cursors and
encoding annotated CSV. The tool itself mostly waits. With
`--read-via export-lp` the tool instead runs `influxd inspect export-lp`,
which decodes the storage files (TSM and WAL) directly and prints line
protocol.

The comparison is the same as with Flux: both sides go through the
migration's conversion rules. Per series and field the tool compares the
number of values and a checksum over every timestamp and value.

## Usage

The tool must be able to start `influxd` with access to the engine directory
(the one that contains `data/` and `wal/`). Source and target must be on the
same InfluxDB. Reading is read-only and needs no write access to the files.

InfluxDB in Docker, tool on the host:

```sh
loxinflux-migration-helper verify --state lmh-state-loxinflux-to-loxinflux_v2.jsonl \
  --deep --read-via export-lp --influxd "docker exec influxdb influxd"
```

Faster: run the tool in a sidecar container from the InfluxDB image. Mount
the data volume read-only; this avoids piping everything through
`docker exec`:

```sh
docker run --rm --network container:influxdb --volumes-from influxdb:ro \
  -v "$PWD:/work" -w /work --entrypoint /work/loxinflux-migration-helper \
  -e INFLUX_URL=http://localhost:8086 -e INFLUX_ORG=my-org -e INFLUX_TOKEN \
  influxdb:2 --output json verify --state lmh-state-loxinflux-to-loxinflux_v2.jsonl \
  --deep --read-via export-lp
```

| Option | Default | Meaning |
|---|---|---|
| `--read-via` | `flux` | `flux` or `export-lp`. Only with `--deep`. |
| `--influxd` | `influxd` | Command that runs influxd, split on spaces. |
| `--engine-path` | `/var/lib/influxdb2/engine` | Engine directory as `influxd` sees it. |
| `--export-chunk-days` | `0` | Days read by one export call per bucket. `0` means one shard group: the larger shard group duration of the two buckets. |

export-lp decodes whole TSM blocks, and a block can span a whole shard
group. So chunks are aligned like InfluxDB's shard groups (multiples of the
duration since 0001-01-01), and by default one call reads one shard group.
Checksums are kept per window with interned series keys. A year of day
windows needs a few tens of MB.

`--parallel` sets how many chunks run at once and how many windows of a
chunk are compared or re-checked at once. Each chunk runs two export
processes, one for the source and one for the target.

## When export-lp and Flux disagree

export-lp shows what is stored on disk. Flux shows what InfluxDB returns. The
two can differ:

* Overwritten or deleted points that compaction has not removed yet.
* Values of a field that is missing from the shard's field index. In a real
  installation this affected about 12 % of the values in some months after
  earlier type-conflict clean-ups. No query returns such values, so a Flux
  based migration cannot copy them.

The tool therefore never reports a mismatch from export-lp alone:

1. If a window differs, the tool reads the source through Flux again and
   compares it with the target from export-lp.
2. If the two now match, the window is ok (`rechecked_via_flux: true`). The
   source values that only export-lp saw are counted in
   `source_values_only_on_disk`, and the result gets a warning.
3. If the window still differs, the target is read through Flux too. That
   Flux result is what gets reported.

Each such window costs one extra Flux read of the source. That is still
cheaper than reading both sides through Flux.

So the exit code and the counts are the same as with `--read-via flux`. On a
clean source the run is faster, and as a side effect you learn whether the
source holds values that no query returns.

## Speed

One week of real data (7.8 M values per side), a throwaway InfluxDB 2.9 on
4 cores, small shards:

| Mode | Time | Values/s (both sides) |
|---|---|---|
| Flux | 54 s | 285 k |
| export-lp via `docker exec` | 30 s | 520 k |
| export-lp in a sidecar container | 17 s | 900 k |

`influxd inspect export-lp` alone prints about 2 M lines/s. Above that, the
cost is the tool's conversion and checksums: about 450 k values/s per stream.

A production installation on the same kind of host had 4 cores, 364-day
shard groups, TSM files up to 2 GB on an array disk and 800 M values per
side. The sidecar had a 2 GB memory limit and ran with `--parallel 2`:

| Range | Flux | export-lp |
|---|---|---|
| 1 shard group (335 days, 75.6 M values per side) | 525 s | 175 s |
| everything (1877 days, 800 M values per side) | about 3 h¹ | 51 min² |

¹ Before this release, with the extra count pass and `--parallel 4`.
² 331 of the windows had values only on disk (38.6 M values in one year of
the source), so they were re-read through Flux. Without them the run would,
by estimate, take about 20 minutes.

Chunks must be whole shard groups. A partial range still decodes the whole
shard group, so for a few days `--read-via flux` can be faster.

InfluxDB's own process is not involved in export-lp reads. Its heap does not
grow. The page cache of the TSM files is charged to the process that runs
export-lp.

## Also in this release

* `verify --deep` no longer runs a separate count query for the windows it
  compares. The deep comparison counts the target's values per measurement
  anyway. This saves one pass per window, and with Flux the separate count
  could trigger an InfluxDB 2.x panic (`unreachable: <nil>`) that needed a
  slower row-count fallback.
