#!/usr/bin/env bash
# SIGKILL the broker mid-produce, restart it, and check what survived.
#
# The invariant: whatever the broker serves after a crash must be a contiguous
# prefix of what was sent. Records may be lost from the tail — nothing was
# acknowledged for them — but there must be no gap, no duplicate, no torn
# record, and no offset that reads back as something else.
#
# usage: scripts/crash_test.sh [rounds] [fsync]
set -uo pipefail

ROUNDS=${1:-5}
FSYNC=${2:-never}
MESSAGES=${MESSAGES:-1000000}
BROKER=${BROKER:-./target/release/franz}
PORT=${PORT:-9092}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

if [ ! -x "$BROKER" ]; then
  echo "build first: cargo build --release" >&2
  exit 1
fi

seq 1 "$MESSAGES" | sed 's/^/m-/' > "$WORK/want.txt"

start_broker() {
  "$BROKER" --topic crash:1 --data-dir "$WORK/data" --port "$PORT" --fsync "$FSYNC" \
    > "$WORK/broker.log" 2>&1 &
  echo $!
  for _ in $(seq 1 200); do
    nc -z 127.0.0.1 "$PORT" 2>/dev/null && return 0
    sleep 0.05
  done
  echo "broker did not come up" >&2
  cat "$WORK/broker.log" >&2
  exit 1
}

failures=0

for round in $(seq 1 "$ROUNDS"); do
  rm -rf "$WORK/data"

  pid=$(start_broker)

  kcat -P -b "127.0.0.1:$PORT" -t crash < "$WORK/want.txt" >/dev/null 2>&1 &
  producer=$!

  # Somewhere in the middle of the write, not at a predictable point.
  # A full run takes roughly a second, so this lands partway through it.
  sleep "0.$(printf '%03d' $((RANDOM % 550 + 50)))"
  kill -9 "$pid" 2>/dev/null
  wait "$producer" 2>/dev/null
  wait "$pid" 2>/dev/null

  pid=$(start_broker)
  recovery=$(grep -o 'dropped [0-9]* bytes of partial write' "$WORK/broker.log" | tail -1)
  kcat -C -b "127.0.0.1:$PORT" -t crash -o beginning -e > "$WORK/got.txt" 2>/dev/null
  kcat -C -b "127.0.0.1:$PORT" -t crash -o beginning -e -f '%o\n' > "$WORK/offsets.txt" 2>/dev/null
  kill -TERM "$pid" 2>/dev/null
  wait "$pid" 2>/dev/null

  survived=$(wc -l < "$WORK/got.txt" | tr -d ' ')

  # Must be exactly the first `survived` lines of the input.
  if ! head -n "$survived" "$WORK/want.txt" | diff -q - "$WORK/got.txt" >/dev/null; then
    echo "round $round: FAIL — what came back is not a prefix of what was sent"
    failures=$((failures + 1))
    continue
  fi

  # Offsets must be 0..survived-1 with nothing missing or repeated.
  expected_offsets=$(seq 0 $((survived - 1)) | cksum)
  actual_offsets=$(cksum < "$WORK/offsets.txt")
  if [ "$survived" -gt 0 ] && [ "$expected_offsets" != "$actual_offsets" ]; then
    echo "round $round: FAIL — offsets are not contiguous from 0"
    failures=$((failures + 1))
    continue
  fi

  if [ "$survived" -eq "$MESSAGES" ]; then
    echo "round $round: inconclusive — the producer finished before the kill"
    continue
  fi

  printf 'round %d: ok — %7d of %d records survived' "$round" "$survived" "$MESSAGES"
  [ -n "$recovery" ] && printf ', %s' "$recovery"
  printf '\n'
done

echo
if [ "$failures" -eq 0 ]; then
  echo "$ROUNDS/$ROUNDS rounds passed (fsync=$FSYNC)"
else
  echo "$failures of $ROUNDS rounds FAILED (fsync=$FSYNC)"
  exit 1
fi
