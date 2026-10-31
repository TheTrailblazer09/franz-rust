#!/usr/bin/env bash
# Does the simulator actually catch anything?
#
# A crash harness that has never failed proves nothing: it could be checking an
# invariant that cannot be violated, or not checking at all. So this breaks
# recovery on purpose, three ways, and insists the simulator notices each one.
#
# Every mutation is applied to a copy, built, run, and reverted. Nothing
# deliberately broken is left behind.
#
# usage: scripts/mutation_test.sh
set -uo pipefail

SEEDS=${SEEDS:-300}
ORIGINAL=$(mktemp)
cp src/log.rs "$ORIGINAL"
trap 'cp "$ORIGINAL" src/log.rs; rm -f "$ORIGINAL"; cargo build --release -q 2>/dev/null' EXIT

failures=0

mutate() {
  python3 - "$1" "$2" <<'PY'
import pathlib, sys
p = pathlib.Path("src/log.rs")
s = p.read_text()
old, new = sys.argv[1], sys.argv[2]
if s.count(old) != 1:
    sys.exit(f"mutation anchor matched {s.count(old)} times, expected 1")
p.write_text(s.replace(old, new))
PY
}

expect_caught() {
  local name="$1"
  printf '%-44s' "$name"

  local build
  if ! build=$(cargo build --release -q 2>&1); then
    printf 'BUILD FAILED\n'
    echo "$build" | sed 's/^/      /' | head -6
    failures=$((failures + 1))
    return
  fi

  local out
  out=$(./target/release/sim --seeds "$SEEDS" 2>&1)
  if echo "$out" | grep -q VIOLATION; then
    local seed
    seed=$(echo "$out" | grep -m1 VIOLATION | sed 's/^seed \([0-9]*\).*/\1/')
    printf 'caught at seed %s\n' "$seed"
    echo "$out" | grep -m1 VIOLATION | sed 's/^/      /'
  else
    printf 'NOT CAUGHT — the simulator is not checking this\n'
    failures=$((failures + 1))
  fi

  cp "$ORIGINAL" src/log.rs
}

echo "injecting bugs into recovery and checking the simulator notices"
echo

mutate '            match batch::parse(&bytes[at..]) {' '            match batch::parse_header(&bytes[at..]) {'
expect_caught "  1. recovery skips checksum validation"

mutate '            active.log.truncate(at as u64)?;' '            let _ = at;'
expect_caught "  2. torn tail is left on disk"

mutate '        active.rebuild_index(&batches, interval)?;' '        let _ = (&batches, interval);'
expect_caught "  3. stale index kept after truncation"

echo
cp "$ORIGINAL" src/log.rs
cargo build --release -q 2>/dev/null
printf '%-44s' "  unmutated code"
if ./target/release/sim --seeds "$SEEDS" 2>&1 | grep -q VIOLATION; then
  printf 'VIOLATION on clean code\n'
  failures=$((failures + 1))
else
  printf 'no violations\n'
fi

echo
if [ "$failures" -eq 0 ]; then
  echo "the simulator catches every injected bug and passes clean code"
else
  echo "$failures problem(s)"
  exit 1
fi
