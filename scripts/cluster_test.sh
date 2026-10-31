#!/usr/bin/env bash
# Three brokers, one topic, replication factor 2.
#
# What has to hold: clients are told the real leader for each partition, writes
# to a non-leader are refused with a code that tells the client where to go,
# followers end up byte-identical to their leader, acks=all does not return
# until the data is on the in-sync replicas, and a dead follower drops out of
# the ISR instead of freezing the partition.
#
# usage: scripts/cluster_test.sh
set -uo pipefail

BROKER=${BROKER:-./target/release/franz}
BASE_PORT=${BASE_PORT:-9790}
PARTITIONS=6
WORK=$(mktemp -d)
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null; done
  rm -rf "$WORK"
}
trap cleanup EXIT

if [ ! -x "$BROKER" ]; then
  echo "build first: cargo build --release" >&2
  exit 1
fi

PEERS=()
for id in 1 2 3; do PEERS+=(--peer "$id@127.0.0.1:$((BASE_PORT + id))"); done

failures=0
check() {
  if [ "$2" = "$3" ]; then
    printf '  ok    %s\n' "$1"
  else
    printf '  FAIL  %s (expected %s, got %s)\n' "$1" "$3" "$2"
    failures=$((failures + 1))
  fi
}

start() {
  local id=$1
  "$BROKER" --node-id "$id" --port "$((BASE_PORT + id))" \
    --data-dir "$WORK/n$id" --topic demo:$PARTITIONS \
    --replication-factor 2 --min-insync 2 "${PEERS[@]}" \
    >> "$WORK/n$id.log" 2>&1 &
  PIDS[$id]=$!
  for _ in $(seq 1 200); do
    nc -z 127.0.0.1 "$((BASE_PORT + id))" 2>/dev/null && return 0
    sleep 0.05
  done
  echo "broker $id did not come up" >&2
  cat "$WORK/n$id.log" >&2
  exit 1
}

for id in 1 2 3; do start "$id"; done
B1="-b 127.0.0.1:$((BASE_PORT + 1))"
echo "three brokers up, demo:$PARTITIONS with replication factor 2"

echo
echo "metadata names a real leader per partition"
kcat -L $B1 -t demo 2>/dev/null > "$WORK/meta.txt"
kcat -L $B1 2>/dev/null > "$WORK/all.txt"
brokers=$(grep -c 'at 127.0.0.1' "$WORK/all.txt")
leaders=$(grep -oE 'leader [0-9]+' "$WORK/meta.txt" | sort -u | wc -l | tr -d ' ')
replicas_two=$(grep -cE 'replicas: [0-9]+,[0-9]+' "$WORK/meta.txt")
check "all three brokers advertised" "$brokers" "3"
check "leadership spread over 3 brokers" "$leaders" "3"
check "every partition has 2 replicas" "$replicas_two" "$PARTITIONS"

echo "a write for a partition this broker does not lead still lands"
# librdkafka reads metadata and sends to the leader itself, so this exercises
# the redirect rather than the NOT_LEADER reply; that reply is unit-tested.
notmine=$(awk '/partition /{p=$2; sub(",","",p)} /leader [^1]/{print p; exit}' "$WORK/meta.txt")
echo redirected | kcat -P $B1 -t demo -p "${notmine:-0}" -X message.timeout.ms=8000 2>/dev/null
sleep 2
landed=$(kcat -C $B1 -t demo -p "${notmine:-0}" -o beginning -e -u 2>/dev/null | grep -c redirected)
check "record reached the partition led by another broker" "$landed" "1"

echo
echo "acks=all round-trips through the followers"
for p in $(seq 0 $((PARTITIONS - 1))); do
  seq 1 300 | kcat -P $B1 -t demo -p "$p" -X request.required.acks=-1 \
    -X message.timeout.ms=20000 2>/dev/null
done
sleep 3
want=$((300 * PARTITIONS + 1))  # plus the redirected record above
got=$(kcat -C $B1 -t demo -o beginning -e -u 2>/dev/null | wc -l | tr -d ' ')
check "all $want records readable" "$got" "$want"

echo
echo "followers hold the same bytes as their leader"
mismatch=0
compared=0
for p in $(seq 0 $((PARTITIONS - 1))); do
  copies=0
  sums=""
  for id in 1 2 3; do
    f="$WORK/n$id/demo-$p/00000000000000000000.log"
    if [ -s "$f" ]; then
      copies=$((copies + 1))
      sums="$sums $(cksum < "$f" | awk '{print $1}')"
    fi
  done
  [ "$copies" -eq 0 ] && continue
  compared=$((compared + 1))
  distinct=$(echo "$sums" | tr ' ' '\n' | grep -c '[0-9]' )
  unique=$(echo "$sums" | tr ' ' '\n' | grep '[0-9]' | sort -u | wc -l | tr -d ' ')
  if [ "$copies" -ne 2 ] || [ "$unique" -ne 1 ]; then
    echo "  partition $p: $copies copies, $unique distinct checksum(s) of $distinct"
    mismatch=$((mismatch + 1))
  fi
done
check "every partition with data is byte-identical on 2 nodes" "$mismatch" "0"
check "all $PARTITIONS partitions carried data" "$compared" "$PARTITIONS"

echo
echo "a dead follower leaves the ISR instead of freezing the partition"
# Find a partition that node 3 follows, and the broker that leads it. Only the
# leader tracks an ISR, so only the leader can be asked about it: with no
# controller, nothing propagates that state to the other brokers.
victim=$(awk -F'[ ,]+' '/^ *partition /{
  part=$3; leader=$5
  rep=""
  for (i = 1; i <= NF; i++) if ($i == "replicas:") { rep = $(i+1) "," $(i+2) }
  if (leader != 3 && rep ~ /(^|,)3(,|$)/) { print part, leader; exit }
}' "$WORK/meta.txt")
part=$(echo "$victim" | awk '{print $1}')
owner=$(echo "$victim" | awk '{print $2}')
echo "  node 3 follows partition ${part:-?}, led by broker ${owner:-?}"

kill -9 "${PIDS[3]}" 2>/dev/null
PIDS[3]=""
sleep 12

BL="-b 127.0.0.1:$((BASE_PORT + ${owner:-1}))"
kcat -L $BL -t demo 2>/dev/null > "$WORK/meta2.txt"
isr=$(awk -F'[ ,]+' -v want="$part" '/^ *partition /{
  if ($3 == want) { for (i = 1; i <= NF; i++) if ($i == "isrs:") { n = NF - i; print n; exit } }
}' "$WORK/meta2.txt")
grep -E "partition $part," "$WORK/meta2.txt" | sed 's/^/    /'
check "partition $part now reports a single-replica ISR" "${isr:-0}" "1"

# min-insync is 2, so acks=all must refuse rather than accept a write only one
# node would hold.
refused=$(seq 1 20 | kcat -P $BL -t demo -p "${part:-0}" -X request.required.acks=-1 \
  -X message.timeout.ms=6000 2>&1 | grep -ciE 'not enough|NOT_ENOUGH_REPLICAS|timed out')
check "acks=all refused while the ISR is below min-insync" \
  "$([ "$refused" -gt 0 ] && echo yes || echo no)" "yes"

# acks=1 still works: one node holding it is exactly what acks=1 means.
echo degraded | kcat -P $BL -t demo -p "${part:-0}" -X request.required.acks=1 \
  -X message.timeout.ms=6000 2>/dev/null
sleep 1
still=$(kcat -C $BL -t demo -p "${part:-0}" -o beginning -e -u 2>/dev/null | grep -c degraded)
check "acks=1 still accepted on the degraded partition" "$still" "1"

echo
if [ "$failures" -eq 0 ]; then
  echo "all checks passed"
else
  echo "$failures check(s) FAILED"
  exit 1
fi
