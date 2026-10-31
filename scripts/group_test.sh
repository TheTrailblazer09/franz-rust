#!/usr/bin/env bash
# Consumer groups, checked with two real clients.
#
# What has to hold: the partitions of a topic are split across the members of a
# group, nobody reads the same partition as anybody else, every record is read
# exactly once, and a member that dies has its partitions picked up by whoever
# is left.
#
# usage: scripts/group_test.sh
set -uo pipefail

BROKER=${BROKER:-./target/release/franz}
PORT=${PORT:-9092}
PARTITIONS=4
PER_PARTITION=100
WORK=$(mktemp -d)
BROKER_PID=""

cleanup() {
  [ -n "$BROKER_PID" ] && kill -9 "$BROKER_PID" 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

if [ ! -x "$BROKER" ]; then
  echo "build first: cargo build --release" >&2
  exit 1
fi

B="-b 127.0.0.1:$PORT"
EARLIEST="-u -X auto.offset.reset=earliest -X session.timeout.ms=6000 -X heartbeat.interval.ms=1000"
failures=0

start_broker() {
  "$BROKER" --topic demo:$PARTITIONS --data-dir "$WORK/data" --port "$PORT" \
    >> "$WORK/broker.log" 2>&1 &
  BROKER_PID=$!
  for _ in $(seq 1 200); do
    nc -z 127.0.0.1 "$PORT" 2>/dev/null && return 0
    sleep 0.05
  done
  echo "broker did not come up" >&2
  cat "$WORK/broker.log" >&2
  exit 1
}

stop_broker() {
  kill -9 "$BROKER_PID" 2>/dev/null
  for _ in $(seq 1 200); do
    nc -z 127.0.0.1 "$PORT" 2>/dev/null || return 0
    sleep 0.05
  done
}

check() {
  if [ "$2" = "$3" ]; then
    printf '  ok    %s\n' "$1"
  else
    printf '  FAIL  %s (expected %s, got %s)\n' "$1" "$3" "$2"
    failures=$((failures + 1))
  fi
}

# grep -c already prints a count and exits 1 when it is zero, so no fallback.
records() { grep -vc '^%' "$1" 2>/dev/null | head -1; }
partitions_of() { grep -v '^%' "$1" 2>/dev/null | cut -d: -f1 | sort -un | tr '\n' ',' ; }

start_broker
for p in $(seq 0 $((PARTITIONS - 1))); do
  seq $((p * PER_PARTITION + 1)) $((p * PER_PARTITION + PER_PARTITION)) \
    | kcat -P $B -t demo -p "$p" 2>/dev/null
done
total=$((PARTITIONS * PER_PARTITION))
echo "produced $total records across $PARTITIONS partitions"

echo
echo "one consumer takes every partition"
kcat $B $EARLIEST -q -c $total -G solo demo > "$WORK/solo.txt" 2>&1
check "read all $total records" "$(records "$WORK/solo.txt")" "$total"

echo
echo "the same group resumes where it left off"
kcat $B $EARLIEST -q -G solo demo > "$WORK/resume.txt" 2>&1 & pid=$!
sleep 4
kill -9 $pid 2>/dev/null
check "read nothing the second time" "$(records "$WORK/resume.txt")" "0"

echo
echo "two consumers split the partitions"
kcat $B $EARLIEST -q -f '%p:%s\n' -G pair demo > "$WORK/a.txt" 2>&1 & a=$!
kcat $B $EARLIEST -q -f '%p:%s\n' -G pair demo > "$WORK/b.txt" 2>&1 & b=$!
sleep 12
got_a=$(records "$WORK/a.txt")
got_b=$(records "$WORK/b.txt")
parts_a=$(partitions_of "$WORK/a.txt")
parts_b=$(partitions_of "$WORK/b.txt")
unique=$(cat "$WORK/a.txt" "$WORK/b.txt" | grep -v '^%' | cut -d: -f2 | sort -un | wc -l | tr -d ' ')
dupes=$(cat "$WORK/a.txt" "$WORK/b.txt" | grep -v '^%' | cut -d: -f2 | sort | uniq -d | wc -l | tr -d ' ')
overlap=$(comm -12 <(partitions_of "$WORK/a.txt" | tr ',' '\n' | sort -u) \
                   <(partitions_of "$WORK/b.txt" | tr ',' '\n' | sort -u) | grep -c '[0-9]')

echo "  consumer A: $got_a records from partitions $parts_a"
echo "  consumer B: $got_b records from partitions $parts_b"
check "every record read once" "$unique" "$total"
check "no record read twice" "$dupes" "0"
check "no partition shared" "$overlap" "0"
check "both consumers got work" "$([ "$got_a" -gt 0 ] && [ "$got_b" -gt 0 ] && echo yes || echo no)" "yes"

echo
echo "killing one consumer hands its partitions to the other"
# SIGKILL, so the member never sends LeaveGroup and has to be timed out.
kill -9 "$a" 2>/dev/null
sleep 10
for p in $(seq 0 $((PARTITIONS - 1))); do
  echo "after-crash-$p" | kcat -P $B -t demo -p "$p" 2>/dev/null
done
sleep 8
kill -9 "$b" 2>/dev/null
survivor=$(grep -c 'after-crash' "$WORK/b.txt")
survivor_parts=$(grep 'after-crash' "$WORK/b.txt" | cut -d: -f1 | sort -un | wc -l | tr -d ' ')
echo "  survivor read $survivor new record(s) across $survivor_parts partition(s)"
check "survivor picked up all $PARTITIONS partitions" "$survivor_parts" "$PARTITIONS"

echo
echo "committed offsets survive a broker restart"
stop_broker
start_broker
replayed=$(grep -o 'replayed [0-9]* committed offsets' "$WORK/broker.log" | tail -1 | grep -o '[0-9]*')
check "offsets replayed from __consumer_offsets" "$([ "${replayed:-0}" -gt 0 ] && echo yes || echo no)" "yes"
kcat $B $EARLIEST -q -G solo demo > "$WORK/after.txt" 2>&1 & pid=$!
sleep 4
kill -9 $pid 2>/dev/null
check "group 'solo' still only sees the new records" "$(records "$WORK/after.txt")" "$PARTITIONS"

echo
if [ "$failures" -eq 0 ]; then
  echo "all checks passed"
else
  echo "$failures check(s) FAILED"
  exit 1
fi
