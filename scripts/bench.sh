#!/usr/bin/env bash
# Produce and consume throughput at each durability setting.
#
# usage: scripts/bench.sh [messages] [record_bytes]
set -uo pipefail

MESSAGES=${1:-500000}
SIZE=${2:-200}
BROKER=${BROKER:-./target/release/franz}
PORT=${PORT:-9300}
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

if [ ! -x "$BROKER" ]; then
  echo "build first: cargo build --release" >&2
  exit 1
fi

payload=$(head -c "$SIZE" < /dev/zero | tr '\0' 'x')
seq 1 "$MESSAGES" | sed "s/\$/ $payload/" > "$WORK/input.txt"
bytes=$(wc -c < "$WORK/input.txt" | tr -d ' ')
mib=$(echo "scale=2; $bytes / 1048576" | bc)

echo "$MESSAGES records, ~${SIZE}B each, ${mib} MiB total"
echo
printf '| fsync policy | produce msg/s | produce MiB/s | consume msg/s | consume MiB/s |\n'
printf '| --- | --- | --- | --- | --- |\n'

elapsed() {
  python3 -c "import sys;print(f'{float(sys.argv[2])-float(sys.argv[1]):.4f}')" "$1" "$2"
}
rate() {
  python3 -c "import sys;print(f'{float(sys.argv[1])/float(sys.argv[2]):,.0f}')" "$1" "$2"
}
mibs() {
  python3 -c "import sys;print(f'{float(sys.argv[1])/1048576/float(sys.argv[2]):.1f}')" "$1" "$2"
}
now() { python3 -c 'import time;print(time.time())'; }

for policy in always 100 never; do
  rm -rf "$WORK/data"
  "$BROKER" --topic bench:1 --data-dir "$WORK/data" --port "$PORT" --fsync "$policy" \
    > "$WORK/broker.log" 2>&1 &
  pid=$!
  for _ in $(seq 1 200); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 0.05; done

  t0=$(now)
  kcat -P -b "127.0.0.1:$PORT" -t bench < "$WORK/input.txt" >/dev/null 2>&1
  t1=$(now)

  # Cold-ish: a fresh consumer reading the whole log back.
  kcat -C -b "127.0.0.1:$PORT" -t bench -o beginning -e >/dev/null 2>&1
  t2=$(now)
  kcat -C -b "127.0.0.1:$PORT" -t bench -o beginning -e >/dev/null 2>&1
  t3=$(now)

  kill -TERM "$pid" 2>/dev/null; wait "$pid" 2>/dev/null

  p=$(elapsed "$t0" "$t1")
  c=$(elapsed "$t2" "$t3")
  label=$([ "$policy" = "100" ] && echo "every 100ms" || echo "$policy")
  printf '| %s | %s | %s | %s | %s |\n' \
    "$label" "$(rate "$MESSAGES" "$p")" "$(mibs "$bytes" "$p")" \
    "$(rate "$MESSAGES" "$c")" "$(mibs "$bytes" "$c")"
done

echo
echo "Recovery time by log size (restart on an existing data directory):"
printf '| records on disk | log size | recovery scan |\n'
printf '| --- | --- | --- |\n'
for count in 100000 1000000; do
  rm -rf "$WORK/data"
  "$BROKER" --topic rec:1 --data-dir "$WORK/data" --port "$PORT" --fsync never \
    > "$WORK/broker.log" 2>&1 &
  pid=$!
  for _ in $(seq 1 200); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 0.05; done
  head -n "$count" "$WORK/input.txt" | kcat -P -b "127.0.0.1:$PORT" -t rec >/dev/null 2>&1
  kill -TERM "$pid" 2>/dev/null; wait "$pid" 2>/dev/null

  size=$(du -sh "$WORK/data" | cut -f1 | tr -d ' ')
  "$BROKER" --topic rec:1 --data-dir "$WORK/data" --port "$PORT" --fsync never \
    > "$WORK/broker.log" 2>&1 &
  pid=$!
  for _ in $(seq 1 400); do nc -z 127.0.0.1 "$PORT" 2>/dev/null && break; sleep 0.01; done
  recovered=$(grep -o 'recovered in [0-9.]*s' "$WORK/broker.log" | grep -o '[0-9.]*s')
  kill -TERM "$pid" 2>/dev/null; wait "$pid" 2>/dev/null

  printf '| %s | %s | %s |\n' "$count" "$size" "$recovered"
done
