#!/usr/bin/env bash
# End-to-end test against a real, EMPTY InfluxDB v2 (never point it at production).
#
#   docker run -d --name lmh-e2e -p 8086:8086 \
#     -e DOCKER_INFLUXDB_INIT_MODE=setup -e DOCKER_INFLUXDB_INIT_USERNAME=e2e \
#     -e DOCKER_INFLUXDB_INIT_PASSWORD=e2e-e2e-e2e -e DOCKER_INFLUXDB_INIT_ORG=o \
#     -e DOCKER_INFLUXDB_INIT_BUCKET=src -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=e2etoken \
#     -e INFLUXD_STORAGE_CACHE_SNAPSHOT_WRITE_COLD_DURATION=1s influxdb:2
#   LMH_E2E_CONTAINER=lmh-e2e tests/e2e.sh target/release/loxinflux-migration-helper
#
# Environment: INFLUX_URL (default http://localhost:8086), INFLUX_TOKEN (e2etoken), INFLUX_ORG (o),
# LMH_E2E_CONTAINER (optional: container name, enables the export-lp file test and, if the container runs
# with INFLUXD_STORAGE_CACHE_SNAPSHOT_WRITE_COLD_DURATION=1s, the recover test; that test restarts it).
set -u
BIN=${1:?usage: tests/e2e.sh path/to/loxinflux-migration-helper}
export INFLUX_URL=${INFLUX_URL:-http://localhost:8086} INFLUX_TOKEN=${INFLUX_TOKEN:-e2etoken} INFLUX_ORG=${INFLUX_ORG:-o}
WORK=$(mktemp -d); cd "$WORK" || exit 1
H="Authorization: Token $INFLUX_TOKEN"
for _ in $(seq 1 30); do curl -sf "$INFLUX_URL/health" >/dev/null && break; sleep 2; done

w(){ curl -s -o /dev/null -w '%{http_code}' -XPOST "$INFLUX_URL/api/v2/write?org=$INFLUX_ORG&bucket=$1&precision=s" -H "$H" --data-binary "$2"; }
qa(){ curl -s -XPOST "$INFLUX_URL/api/v2/query?org=$INFLUX_ORG" -H "$H" -H 'Content-Type: application/json' -H 'Accept: application/csv' \
      --data "$(printf '{"query":%s,"dialect":{"annotations":["datatype"]}}' "$(printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g; s/^/"/; s/$/"/')")"; }
id_of(){ curl -s "$INFLUX_URL/api/v2/$1" -H "$H" | grep -o '"id": *"[^"]*"' | head -1 | sed 's/.*"\([^"]*\)"$/\1/'; }
L(){ "$BIN" "$@"; }
PASS=0; FAIL=0
check(){ if eval "$2"; then PASS=$((PASS+1)); echo "PASS $1"; else FAIL=$((FAIL+1)); echo "FAIL $1"; fi; }

# loxInFlux 0.3 style data: shard A (2023-11-14) integers/strings, shard B (2024-03-09) floats.
w src 'Temperatur,application=loxInFlux,room=Bad,uuid=u1 AQ=19i,Default="19.6°" 1700000000
Temperatur,application=loxInFlux,room=Bad,uuid=u1 AQ=20i,Default="20.0°" 1700000060
Ablaufsteuerung,application=loxInFlux,uuid=u2 API="" 1700000000
Status,application=loxInFlux,uuid=u3 Default="Ein" 1700000000
Status,application=loxInFlux,uuid=u3 Default="<v.1>" 1700000060
Alarm,application=loxInFlux,uuid=u4 Default=true 1700000000
zigbee,application=z2m,device=k1 state=1i 1700000000' >/dev/null
w src 'Temperatur,application=loxInFlux,room=Bad,uuid=u1 AQ=19.5,Default=20.1 1710000000' >/dev/null
R=(--start 2023-11-01 --stop 2024-04-01)
SCOPE=(--convert-where-tag application=loxInFlux)

L --output json analyze --bucket src "${R[@]}" --classify-strings > a.json 2>/dev/null; rc=$?
check "analyze: exit 0" "[ $rc = 0 ]"
check "analyze: 2 fields with type conflicts" "grep -q '\"fields_with_type_conflicts\": 2' a.json"
check "analyze: unit strings classified" "grep -q 'string_with_unit_to_float' a.json"

L --output json migrate --source-bucket src --target-bucket dst "${R[@]}" "${SCOPE[@]}" --dry-run > d.json 2>/dev/null; rc=$?
check "dry run: exit 0, would write 9" "[ $rc = 0 ] && grep -q '\"values_written\": 9' d.json"
check "dry run: target not created" "[ -z \"\$(id_of 'buckets?name=dst&org=$INFLUX_ORG')\" ]"

L migrate --source-bucket src --target-bucket dst "${R[@]}" >/dev/null 2>&1; rc=$?
check "missing target: exit 5" "[ $rc = 5 ]"
L migrate --source-bucket src --target-bucket src "${R[@]}" >/dev/null 2>&1; rc=$?
check "source == target: exit 8" "[ $rc = 8 ]"

L --output json migrate --source-bucket src --target-bucket dst "${R[@]}" "${SCOPE[@]}" --create-target > m.json 2>/dev/null; rc=$?
check "migrate: exit 0, 9 written, 2 dropped" "[ $rc = 0 ] && grep -q '\"values_written\": 9' m.json && grep -q '\"values_dropped\": 2' m.json"
check "migrate: out-of-scope point copied" "grep -q '\"copied_unchanged\": 1' m.json"
L --output json migrate --source-bucket src --target-bucket dst "${R[@]}" "${SCOPE[@]}" > r.json 2>/dev/null
check "rerun: resumes, nothing to do" "grep -q '\"windows_migrated_now\": 0' r.json"

L --output json verify --state lmh-state-src-to-dst.jsonl > v.json 2>/dev/null; rc=$?
check "verify: exit 0" "[ $rc = 0 ] && grep -q '\"missing_total\": 0' v.json"
L --output json verify --state lmh-state-src-to-dst.jsonl --deep > vd.json 2>/dev/null; rc=$?
check "verify --deep: exit 0, every value matches" "[ $rc = 0 ] && grep -q '\"series_different\": 0' vd.json && grep -q '\"series_missing\": 0' vd.json"
w dst 'Temperatur,application=loxInFlux,room=Bad,uuid=u1 AQ=99.5 1700000000' >/dev/null
L verify --state lmh-state-src-to-dst.jsonl >/dev/null 2>&1; rc=$?
check "a changed value passes the count check" "[ $rc = 0 ]"
L --output json verify --state lmh-state-src-to-dst.jsonl --deep > vd2.json 2>/dev/null; rc=$?
check "... but verify --deep catches it: exit 7" "[ $rc = 7 ] && grep -q '\"series_different\": 1' vd2.json"
L migrate --source-bucket src --target-bucket dst --start 2023-11-14 --stop 2023-11-15 --redo "${SCOPE[@]}" >/dev/null 2>&1
L verify --state lmh-state-src-to-dst.jsonl --deep >/dev/null 2>&1; rc=$?
check "redo repairs it, deep verify exit 0" "[ $rc = 0 ]"


AQ='from(bucket:"dst")|>range(start:2023-01-01T00:00:00Z)|>filter(fn:(r)=>r._measurement=="Temperatur" and r._field=="AQ")'
check "target: all 3 AQ values readable across shards" "[ \$(qa '$AQ' | grep -c ',AQ,') = 3 ]"
check "target: unit string is now double" "qa 'from(bucket:\"dst\")|>range(start:2023-01-01T00:00:00Z)|>filter(fn:(r)=>r._measurement==\"Temperatur\" and r._field==\"Default\")' | grep -q '^#datatype.*double'"
check "target: text in __text__Default" "qa 'from(bucket:\"dst\")|>range(start:2023-01-01T00:00:00Z)|>filter(fn:(r)=>r._measurement==\"Status\")' | grep -q '__text__Default'"
check "target: out-of-scope integer kept" "qa 'from(bucket:\"dst\")|>range(start:2023-01-01T00:00:00Z)|>filter(fn:(r)=>r._measurement==\"zigbee\")' | grep -q '^#datatype.*long'"

ORG_ID=$(id_of "orgs?org=$INFLUX_ORG")
curl -s -o /dev/null -XPOST "$INFLUX_URL/api/v2/buckets" -H "$H" -H 'Content-Type: application/json' --data "{\"orgID\":\"$ORG_ID\",\"name\":\"conf\",\"retentionRules\":[]}"
w conf 'Temperatur,application=loxInFlux,room=Bad,uuid=u1 AQ=1i 1700000000' >/dev/null
L --output json migrate --source-bucket src --target-bucket conf "${R[@]}" > c.json 2>/dev/null; rc=$?
check "conflict in target: exit 6" "[ $rc = 6 ] && grep -q 'field type conflict' c.json"

curl -s -o /dev/null -XPOST "$INFLUX_URL/api/v2/delete?org=$INFLUX_ORG&bucket=dst" -H "$H" -H 'Content-Type: application/json' \
  --data '{"start":"2023-01-01T00:00:00Z","stop":"2025-01-01T00:00:00Z","predicate":"_measurement=\"Alarm\""}'
L --output json verify --state lmh-state-src-to-dst.jsonl > v2.json 2>/dev/null; rc=$?
check "verify detects deleted values: exit 7" "[ $rc = 7 ] && grep -q '\"Alarm\": 1' v2.json"
L migrate --source-bucket src --target-bucket dst --start 2023-11-09T00:00:00Z --stop 2023-11-16T00:00:00Z --redo "${SCOPE[@]}" >/dev/null 2>&1
L verify --state lmh-state-src-to-dst.jsonl >/dev/null 2>&1; rc=$?
check "redo of one window repairs it" "[ $rc = 0 ]"

SRC_ID=$(id_of "buckets?name=src&org=$INFLUX_ORG")
if [ -n "${LMH_E2E_CONTAINER:-}" ]; then
  docker exec "$LMH_E2E_CONTAINER" influxd inspect export-lp --bucket-id "$SRC_ID" --engine-path /var/lib/influxdb2/engine --output-path /tmp/src.lp --log-level error
  docker cp "$LMH_E2E_CONTAINER:/tmp/src.lp" src.lp
  L --output json migrate --from-lp src.lp --target-bucket dst2 --create-target "${SCOPE[@]}" > f.json 2>/dev/null; rc=$?
  check "export-lp file: exit 0, 9 written" "[ $rc = 0 ] && grep -q '\"values_written\": 9' f.json"
  L verify --state lmh-state-file_src_lp-to-dst2.jsonl >/dev/null 2>&1; rc=$?
  check "export-lp file: verify exit 0" "[ $rc = 0 ]"
  docker exec "$LMH_E2E_CONTAINER" influxd inspect export-lp --bucket-id "$SRC_ID" --engine-path /var/lib/influxdb2/engine --output-path - --log-level error \
    | L --output json migrate --from-lp - --target-bucket dst3 --create-target "${SCOPE[@]}" > p.json 2>/dev/null; rc=$?
  check "export-lp piped via stdin: exit 0, 9 written" "[ $rc = 0 ] && grep -q '\"values_written\": 9' p.json"
  X=(--deep --read-via export-lp --influxd "docker exec $LMH_E2E_CONTAINER influxd")
  L --output json verify --state lmh-state-src-to-dst.jsonl "${X[@]}" > vx.json 2>vx.err; rc=$?
  check "verify --deep --read-via export-lp: exit 0 (rechecked via flux: $(grep -o '"windows_rechecked_via_flux": *[0-9]*' vx.json | grep -o '[0-9]*$'))" \
    "[ $rc = 0 ] && grep -q '\"read_via\": \"export-lp\"' vx.json && grep -q '\"series_different\": 0' vx.json"
  w dst 'Temperatur,application=loxInFlux,room=Bad,uuid=u1 AQ=77.5 1700000000' >/dev/null
  L --output json verify --state lmh-state-src-to-dst.jsonl "${X[@]}" > vx2.json 2>/dev/null; rc=$?
  check "... export-lp catches a changed value: exit 7" "[ $rc = 7 ] && grep -q '\"series_different\": 1' vx2.json"
  L migrate --source-bucket src --target-bucket dst --start 2023-11-14 --stop 2023-11-15 --redo "${SCOPE[@]}" >/dev/null 2>&1
  L verify --state lmh-state-src-to-dst.jsonl "${X[@]}" >/dev/null 2>&1; rc=$?
  check "... and passes again after --redo" "[ $rc = 0 ]"
  L --output json verify --state lmh-state-src-to-dst.jsonl --deep --read-via export-lp --influxd /nonexistent/influxd > vx3.json 2>/dev/null; rc=$?
  check "export-lp without influxd: exit 2 with hint" "[ $rc = 2 ] && grep -q 'docker exec' vx3.json"

  # recover: make a field invisible to queries the way it happens in real buckets (the shard's field
  # change log is lost after the field reached a TSM file), then copy it with `recover`.
  # Needs INFLUXD_STORAGE_CACHE_SNAPSHOT_WRITE_COLD_DURATION=1s on the container (TSM files within seconds).
  C=$LMH_E2E_CONTAINER
  up(){ for _ in $(seq 1 60); do curl -sf "$INFLUX_URL/health" >/dev/null && return 0; sleep 1; done; return 1; }
  if docker exec "$C" printenv INFLUXD_STORAGE_CACHE_SNAPSHOT_WRITE_COLD_DURATION >/dev/null 2>&1; then
    curl -s -o /dev/null -XPOST "$INFLUX_URL/api/v2/buckets" -H "$H" -H 'Content-Type: application/json' --data "{\"orgID\":\"$ORG_ID\",\"name\":\"hid\",\"retentionRules\":[]}"
    HID_ID=$(id_of "buckets?name=hid&org=$INFLUX_ORG")
    w hid 'Heat,application=loxInFlux,uuid=h1 AQ=1i 1700000000' >/dev/null
    sleep 3; docker restart "$C" >/dev/null; up
    w hid 'Heat,application=loxInFlux,uuid=h1 Q=2i,Default="5 kWh" 1700000060' >/dev/null
    sleep 3; docker stop "$C" >/dev/null
    docker run --rm --volumes-from "$C" alpine:3 sh -c "rm -f /var/lib/influxdb2/engine/data/$HID_ID/autogen/*/fields.idxl" >/dev/null 2>&1
    docker start "$C" >/dev/null; up
    vis=$(qa 'from(bucket: "hid") |> range(start: 2023-11-14T00:00:00Z, stop: 2023-11-15T00:00:00Z)' | grep -c ',Heat,')
    if [ "$vis" = 1 ]; then
      L migrate --source-bucket hid --target-bucket hid2 --start 2023-11-14 --stop 2023-11-15 "${SCOPE[@]}" --create-target --state h.jsonl >/dev/null 2>&1
      RX=(--source-bucket hid --target-bucket hid2 --start 2023-11-14 --stop 2023-11-15 "${SCOPE[@]}" --influxd "docker exec $C influxd")
      L --output json recover "${RX[@]}" --dry-run > rd.json 2>/dev/null; rc=$?
      check "recover --dry-run: finds 2 hidden values in 2 fields" "[ $rc = 0 ] && grep -q '\"hidden_values\": 2' rd.json && grep -q '\"hidden_pairs\": 2' rd.json"
      L --output json recover "${RX[@]}" > rr.json 2>/dev/null; rc=$?
      check "recover: exit 0, 2 written" "[ $rc = 0 ] && grep -q '\"values_written\": 2' rr.json"
      qa 'from(bucket: "hid2") |> range(start: 2023-11-14T00:00:00Z, stop: 2023-11-15T00:00:00Z) |> filter(fn: (r) => r._field == "Q" or r._field == "Default") |> keep(columns: ["_field", "_value"])' | tr -d '\r' > hq.csv
      check "recover: hidden values readable in the target as floats (Q=2, Default=5)" \
        "[ \$(grep -c '^,_result,' hq.csv) = 2 ] && ! grep '^#datatype' hq.csv | grep -qv ',double,' && grep -q ',5,Default\$' hq.csv && grep -q ',2,Q\$' hq.csv"
      L --output json recover "${RX[@]}" > rr2.json 2>/dev/null; rc=$?
      check "recover rerun: resumes, nothing to do" "[ $rc = 0 ] && grep -q '\"windows_checked_now\": 0' rr2.json"
      L --output json verify --state h.jsonl --deep --read-via export-lp --influxd "docker exec $C influxd" > hv.json 2>/dev/null; rc=$?
      check "verify --deep --read-via export-lp after recover: exit 0" "[ $rc = 0 ] && grep -q '\"series_different\": 0' hv.json"
    else
      echo "SKIP recover test: could not make a field invisible (Flux rows: $vis)"
    fi
  else
    echo "SKIP recover test (container needs INFLUXD_STORAGE_CACHE_SNAPSHOT_WRITE_COLD_DURATION=1s)"
  fi
else
  echo "SKIP export-lp file test (set LMH_E2E_CONTAINER)"
fi

L --output json verify --state lmh-state-src-to-dst.jsonl --read-via export-lp > vu.json 2>/dev/null; rc=$?
check "--read-via export-lp without --deep: exit 2" "[ $rc = 2 ]"

L --memory-limit-mb 1 --output json migrate --source-bucket src --target-bucket dst4 "${R[@]}" --create-target --dry-run > mem.json 2>/dev/null; rc=$?
check "memory watchdog: exit 9 (resource_limit)" "[ $rc = 9 ] && grep -q 'resource_limit' mem.json"

L --log-format json verify --state lmh-state-src-to-dst.jsonl 2> j.err >/dev/null
check "json logs on stderr" "head -1 j.err | grep -q '^{\"timestamp\"'"

curl -s -o /dev/null -XPOST "$INFLUX_URL/api/v2/authorizations" -H "$H" -H 'Content-Type: application/json' \
  --data "{\"orgID\":\"$ORG_ID\",\"description\":\"grafana-read\",\"permissions\":[{\"action\":\"read\",\"resource\":{\"type\":\"buckets\",\"id\":\"$SRC_ID\",\"orgID\":\"$ORG_ID\"}}]}"
L --output json cutover --source-bucket src --target-bucket dst > k.json 2>/dev/null; rc=$?
check "cutover check: warns about bucket-bound token" "[ $rc = 0 ] && grep -q 'grafana-read' k.json && grep -q '\"executed\": false' k.json"
L cutover --source-bucket src --target-bucket dst --execute >/dev/null 2>&1; rc=$?
check "cutover --execute without --yes: exit 8" "[ $rc = 8 ]"
L cutover --source-bucket src --target-bucket dst --execute --yes >/dev/null 2>&1; rc=$?
check "cutover: names swapped" "[ $rc = 0 ] && [ \"\$(id_of 'buckets?name=src_pre_migration&org=$INFLUX_ORG')\" = $SRC_ID ] && [ \"\$(id_of 'buckets?name=src&org=$INFLUX_ORG')\" != $SRC_ID ]"

echo "RESULT: $PASS passed, $FAIL failed"
[ "$FAIL" = 0 ]
