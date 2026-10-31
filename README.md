# franz

A Kafka-compatible event streaming broker, written in Rust.

Real Kafka clients — `kcat`, `librdkafka`, the Java client — can produce to it
and consume from it without knowing it isn't Kafka.

## Why build another Kafka

Not to replace it. Kafka is excellent, this is not a competitor, and nobody
should run it in production.

The reason is that Kafka is a black box exactly where you most want it open.
You cannot make its disk lie to it on demand. You cannot ask it to persist the
first 300 bytes of an unsynced write, drop the rest, and then tell you what it
believes about its own log. You cannot stop its recovery scan halfway. Those
are the behaviours that decide whether a log can be trusted, and they are
precisely the ones you cannot observe from outside — so the usual way to learn
them is to read the source and hope you understood it.

franz is small enough to instrument instead. Every byte it writes passes
through one trait, so [the simulator](#the-crash-simulator) can swap the disk
for one that loses and tears writes on command, and then assert — from a 64-bit
seed, reproducibly — that nothing acknowledged went missing. The broker exists
so that experiment can be run; the experiment is the point.

That is also why it speaks the real protocol rather than one of its own.

## Why wire compatibility

If you invent your own protocol, the only client you can test against is one you
also wrote, so a mistake in your understanding of the protocol appears in both
halves and cancels out. Speaking real Kafka means a third-party client becomes
the test: if `kcat -C` reads back exactly what `kcat -P` wrote, then the
framing, the record encoding, the checksum and the offset arithmetic are all
correct against a specification someone else wrote.

Everything below was verified with `kcat` 1.7.0 on `librdkafka` 2.16.0.

## Running it

```
cargo build --release
./target/release/franz --topic demo:3 --data-dir ./data
```

```
$ seq 1 10000 | kcat -P -b 127.0.0.1:9092 -t demo
$ kcat -C -b 127.0.0.1:9092 -t demo -o beginning -e | wc -l
10000
```

Topics are declared at startup and nothing is auto-created, so a typo fails
loudly instead of silently producing into a new topic.

```
--topic NAME:PARTITIONS   create a topic at startup, repeatable
--data-dir PATH           where logs live (default ./data)
--host HOST               address to listen on and advertise (default 127.0.0.1)
--port PORT               default 9092
--fsync always|never|MS   durability policy (default never)
--node-id N               this broker's id (default 1)
--peer ID@HOST:PORT       a broker in the cluster, repeatable, include self
--replication-factor N    replicas per partition (default 1)
--min-insync N            acks=all fails below this many in-sync replicas
--segment-bytes N         roll a segment past this size (default 64 MiB)
--index-interval N        one index entry per N bytes of log (default 4096)
--verbose                 print every frame, with a hexdump
```

`--host` is the one worth getting right. A `Metadata` response does not say
"the broker you are already talking to"; it says "connect to this host", and
the client reconnects to whatever it is told. Listening on `0.0.0.0` and
advertising it gives you a client that reads metadata once and then hangs.

## What it does

| API | Versions | Notes |
| --- | --- | --- |
| `ApiVersions` | 0–3 | Answers versions it doesn't support, so clients can downgrade |
| `Metadata` | 0–12 | Flexible encoding, topic ids, nullable names |
| `Produce` | 3–8 | Record batch v2, CRC-32C validated, `acks=0/1/all` |
| `Fetch` | 1–11 | `max_wait_ms` long-polling, `min_bytes` |
| `ListOffsets` | 1–5 | Earliest and latest |
| `FindCoordinator` | 0–2 | Group coordinator lookup |
| `JoinGroup` | 0–5 | Rebalance round one |
| `SyncGroup` | 0–3 | Assignment distribution |
| `Heartbeat` | 0–3 | Liveness, triggers rebalance |
| `LeaveGroup` | 0–3 | Clean shutdown |
| `OffsetCommit` | 0–7 | Durable consumer position |
| `OffsetFetch` | 0–5 | Resume position |

`Produce`, `Fetch` and `ListOffsets` deliberately stop one version below where
each turns flexible. That keeps one encoding per API instead of two, and costs
nothing against current clients, which negotiate down to whatever the broker
advertises. `ApiVersions` and `Metadata` do implement the flexible encoding.

## Storage

A partition is a directory of append-only segments:

```
demo-0/
  00000000000000000000.log     record batches, appended
  00000000000000000000.index   sparse offset -> byte position
  00000000000000016384.log     rolled at --segment-bytes
  00000000000000016384.index
```

Only the last segment is written to. Everything before it is immutable, which
is what lets a reader work without coordinating with the writer.

The index holds one 8-byte entry per `--index-interval` bytes of log, not one
per record — at the 4 KiB default and 200-byte records that is 8 bytes of index
per 20 records. A lookup binary-searches the entries, seeks to the byte
position, and scans batch headers forward, with the scan bounded by the index
interval rather than by the size of the log. Offsets are stored relative to the
segment's base offset so they fit in four bytes, which is the only reason the
base offset is encoded in the filename.

The broker never decodes individual records. It validates a batch's checksum,
reads how many offsets it consumes, writes the assigned base offset into the
header, and appends the bytes unchanged. The checksum covers the batch from
byte 21 onward, so stamping an offset into the first eight bytes needs no
recompute — and because records are never re-encoded, the checksum a consumer
verifies is the one the producer computed. Client-side compression needs no
support here at all.

## Durability

| `--fsync` | Behaviour | Survives |
| --- | --- | --- |
| `always` | fsync before acknowledging | machine power loss |
| `N` (ms) | fsync at most every N ms | power loss, minus up to N ms |
| `never` | leave it to the page cache | process crash, not power loss |

All three survive `SIGKILL`, because the page cache outlives the process. Only
`always` survives the machine losing power.

## Replication

```
franz --node-id 1 --port 9091 --data-dir ./n1 --topic demo:6 \
  --replication-factor 2 --min-insync 2 \
  --peer 1@127.0.0.1:9091 --peer 2@127.0.0.1:9092 --peer 3@127.0.0.1:9093
```

Each partition has one leader and the rest followers. Clients read and write
only to the leader; followers replicate by issuing ordinary `Fetch` requests
against it with `replica_id` set to their node id instead of -1.

That reuse is the point. Replication inherits the long-polling, the sparse
index and the batch handling the consumer path already had, and the leader
learns each follower's position from fetch offsets it was receiving anyway — a
follower asking for offset N is telling you it holds everything below N.

**Who holds what is a pure function of the config.** Every broker gets the same
peer list and replication factor and computes the same assignment, so there is
no controller and no consensus on metadata. The cost of that is stated below.

**The high watermark is the lowest in-sync replica's position**, and it is the
highest offset a consumer may read. A record above it exists on the leader but
is not yet on enough replicas to be safe; a consumer allowed to read it could
watch a record it had already seen disappear, which breaks the only promise a
log makes. `acks=all` waits for the high watermark to reach the offset just
written; `acks=1` returns after the leader's local append.

**A follower that stops fetching leaves the in-sync set**, which *advances* the
high watermark rather than stalling it — one slow replica must not be able to
freeze a partition for everybody. With `--min-insync 2`, a produce at
`acks=all` then fails with `NOT_ENOUGH_REPLICAS` instead of accepting a write
only one node would hold.

Replicated bytes keep the leader's offsets rather than being renumbered. The
two logs are meant to be byte-identical, so a follower that restamps has
quietly diverged; a batch that does not continue the follower's log is refused
outright.

`scripts/cluster_test.sh` runs three brokers on one machine:

```
three brokers up, demo:6 with replication factor 2

metadata names a real leader per partition
  ok    all three brokers advertised
  ok    leadership spread over 3 brokers
  ok    every partition has 2 replicas

a write for a partition this broker does not lead still lands
  ok    record reached the partition led by another broker

acks=all round-trips through the followers
  ok    all 1801 records readable

followers hold the same bytes as their leader
  ok    every partition with data is byte-identical on 2 nodes
  ok    all 6 partitions carried data

a dead follower leaves the ISR instead of freezing the partition
  node 3 follows partition 1, led by broker 2
        partition 1, leader 2, replicas: 2,3, isrs: 2
  ok    partition 1 now reports a single-replica ISR
  ok    acks=all refused while the ISR is below min-insync
  ok    acks=1 still accepted on the degraded partition
```

### What static configuration costs

Three limitations follow from having no controller, and they are the honest
price of the design rather than things left for later:

**No failover.** A leader named by config stays the leader. If that broker is
down, its partitions are unavailable for reads and writes — the surviving
replica has the data but will not serve it. Electing a new leader needs someone
to decide, and deciding needs consensus on who decided.

**Only a leader knows its ISR.** Nothing propagates in-sync state between
brokers, so asking broker 1 about a partition led by broker 2 gets the
configured replica set, not the live one. Real Kafka routes this through the
controller.

**No leader epochs, so no epoch-based truncation.** Leadership never moves, so
the epoch this broker reports is always 0. This is the piece failover would
need first: without epochs, a follower that was briefly leader can hold records
the new leader never saw, and truncating to the high watermark either keeps
divergent data or discards committed data. Knowing *that* is most of the value
of the problem; the implementation only matters once leadership can move.

Replacing the static config with Raft over the metadata log — brokers, topics,
leadership — is the upgrade path. Data replication would stay leader-follower;
only metadata needs consensus. Knowing which half needs it is the insight.

## Numbers

Apple M2, 8 cores, 16 GiB, APFS on SSD, macOS 14.5. 500,000 records of ~200
bytes, 99 MiB total, one partition, `kcat` over loopback.

| `--fsync` | produce msg/s | produce MiB/s | consume msg/s | consume MiB/s |
| --- | --- | --- | --- | --- |
| `always` | 402,059 | 79.7 | 608,347 | 120.5 |
| `100` | 825,355 | 163.5 | 582,547 | 115.4 |
| `never` | 884,330 | 175.2 | 589,553 | 116.8 |

Reproduce with `scripts/bench.sh`.

The interesting part is how small the fsync penalty is: 2.2x, not the 50x the
design notes guessed at. The reason is that `librdkafka` batches hard — tens of
thousands of records arrive in one `Produce` request, and `always` means one
fsync per *batch*, not per record. The policy that looks catastrophic on paper
is cheap precisely because the client already amortised it. A client with
`linger.ms=0` producing one record per request would show the gap the table was
expected to show.

Recovery scans only the active segment, so it is bounded by `--segment-bytes`
rather than by how much data is on disk:

| records on disk | log size | recovery scan |
| --- | --- | --- |
| 100,000 | 21 MiB | 0.097s |
| 1,000,000 | 103 MiB | 0.185s |

The second row holds 103 MiB across two segments and scans only the 39 MiB
active one. Cost is dominated by re-checksumming it.

## Crash safety

On startup the active segment is scanned, any trailing bytes that are not a
complete, checksum-valid batch are truncated, and that segment's index is
rebuilt from what survived. Sealed segments are trusted without a scan.

`scripts/crash_test.sh` sends a million records, `SIGKILL`s the broker at a
random point partway through, restarts it, and checks the invariant that
matters: **what comes back must be a contiguous prefix of what was sent.**
Records may be missing from the tail, since nothing was acknowledged for them,
but there must be no gap, no duplicate, no torn record, and the offsets must
run `0..n-1` with nothing missing or repeated.

```
$ scripts/crash_test.sh 6 never
round 1: ok —  596394 of 1000000 records survived
round 2: ok —  458900 of 1000000 records survived
round 3: ok —  284391 of 1000000 records survived
round 4: ok —   83719 of 1000000 records survived
round 5: ok —  747569 of 1000000 records survived
round 6: ok —   27082 of 1000000 records survived

6/6 rounds passed (fsync=never)
```

Passes the same way with `fsync=always`.

Worth being precise about what this does and does not prove. `SIGKILL` does not
reliably produce a torn batch, because a `write` to a regular file is not
interrupted partway through by a signal — the bytes are in the page cache or
they are not. So these rounds exercise offset continuity and clean restart, but
not the failure that actually threatens a commit log.

That is what the simulator below is for.

## The crash simulator

`SIGKILL` cannot tear a write. A `write` to a regular file is not interrupted
partway through by a signal, so the bytes reach the page cache or they do not —
which means the scariest failure for a log, *half a record batch on disk and no
record of how it got there*, is unreachable from a real crash test without
pulling the power cord.

So every byte the log reads or writes goes through a trait, and the simulator
supplies an implementation that lies the way hardware does. It models the two
crash modes separately, because they are not the same thing:

| | What survives | Can it tear a batch? |
| --- | --- | --- |
| Process killed | everything written — the page cache outlives the process | only an append still being issued |
| Power lost | everything synced, plus an arbitrary prefix of what came after | yes, and this is where torn batches come from |

Each run is a 64-bit seed that picks the fsync policy, the segment size, the
index interval, the crash mode, how many batches get appended, and where the
crash lands. Then it crashes, recovers, appends more, and crashes again — three
cycles per seed, because a single crash only tests recovery against a log a
healthy broker wrote, while the second tests it against a log that recovery
itself produced.

After every recovery it asserts:

- the log is a **contiguous prefix** of what was acknowledged: no gap, no
  reordering, no offset that reads back as a different record
- every batch passes its CRC-32C
- nothing that was synced is missing, and with `fsync=always` nothing
  acknowledged is missing at all
- data that survived one crash survives the next
- **every** offset in the log is reachable through the sparse index — a stale
  index entry left pointing past a truncation shows up here and nowhere else

```
$ cargo run --release --bin sim -- --seeds 2000
running seeds 1..2001 with 3 crash cycle(s) each

2000 seeds, 3 crash(es) each
  960 power-loss runs, 1040 process-kill runs
  982 runs had a partial batch on disk for recovery to truncate
  686 runs lost tail records that were never synced

no violations
```

A failure prints the seed, and the seed reproduces it exactly:

```
cargo run --release --bin sim -- --seed 1337
```

### Testing the test

A crash harness that has never failed proves nothing. It might be asserting
something that cannot be violated, or quietly asserting nothing at all. So
`scripts/mutation_test.sh` breaks recovery on purpose, three ways, and insists
the simulator notices each one:

```
$ scripts/mutation_test.sh
injecting bugs into recovery and checking the simulator notices

  1. recovery skips checksum validation     caught at seed 1
      cycle 2: read at 0 failed: read 117 bytes at 1808 of a 1898-byte file
  2. torn tail is left on disk              caught at seed 1
      cycle 1: lost synced data, recovered 29 but synced through 64
  3. stale index kept after truncation      caught at seed 1
      cycle 1: read at 0 returned nothing, log ends at 76

  unmutated code                            no violations

the simulator catches every injected bug and passes clean code
```

Each mutation is applied to a copy, built, run, and reverted; nothing
deliberately broken is left in the tree. All three are caught by the first seed,
and each message points at the actual defect rather than at a generic mismatch.

## Testing

```
cargo test                 # 61 unit tests
cargo run --bin sim        # deterministic crash simulator
scripts/mutation_test.sh   # checks the simulator can actually fail
scripts/crash_test.sh      # SIGKILL loop against the real broker
scripts/group_test.sh      # two real consumers sharing a topic
scripts/cluster_test.sh    # three brokers, replication factor 2
scripts/bench.sh           # throughput table above
```

The tests worth looking at are the ones using bytes captured from a real
client rather than bytes this code produced: `parses_a_real_librdkafka_handshake`
and the two `decodes_a_real_kcat_request_*` cases. A round-trip test only proves
the encoder and decoder agree with each other, which they will even when both
are wrong.

## What it does not do

| Not implemented | Why |
| --- | --- |
| Transactions, exactly-once | Needs a transaction coordinator and producer fencing |
| Log compaction | A second storage engine with its own index |
| Retention | Nothing is ever deleted; segments accumulate |
| `CreateTopics` | Topics come from `--topic` at startup |
| Timestamp offset lookup | No time index, so `ListOffsets` answers only earliest and latest |
| TLS, SASL, ACLs | Plaintext only |

Two known rough edges, stated rather than hidden:

- `Produce` copies each batch out of the request frame before stamping offsets
  into it. The append path should be able to write straight from the frame
  buffer; it does not yet.
- Fetches waiting for data wait on one broker-wide signal, so an append to any
  partition wakes every waiter. Correct, but it does more work than a
  per-partition signal would under many idle consumers.


- [x] Consumer groups
- [x] Replication across brokers

## Consumer groups

A group splits a topic's partitions across its members, nobody reads the same
partition as anybody else, and position survives a restart.

The broker does not compute the assignment. It picks the first member to join
as leader, forwards every member's subscription to that leader, and the leader
decides who gets what and sends the answer back in `SyncGroup`. The coordinator
only distributes the result. That is why franz works with clients using range,
round-robin or sticky assignors without implementing any of them — the strategy
lives in the client and travels as opaque bytes.

Offsets are stored by appending to `__consumer_offsets`, a topic like any
other, and the map is rebuilt by replaying that topic at startup. Reusing the
log costs almost nothing once the log exists. The key and value encoding is
franz's own rather than Kafka's, so a franz data directory is not readable by a
real broker.

`scripts/group_test.sh` runs two real `kcat` consumers against one topic:

```
produced 400 records across 4 partitions

one consumer takes every partition
  ok    read all 400 records

the same group resumes where it left off
  ok    read nothing the second time

two consumers split the partitions
  consumer A: 200 records from partitions 0,1
  consumer B: 200 records from partitions 2,3
  ok    every record read once
  ok    no record read twice
  ok    no partition shared
  ok    both consumers got work

killing one consumer hands its partitions to the other
  survivor read 4 new record(s) across 4 partition(s)
  ok    survivor picked up all 4 partitions

committed offsets survive a broker restart
  ok    offsets replayed from __consumer_offsets
  ok    group 'solo' still only sees the new records
```

Two things in here cost real time to get right.

**A committed offset is the next offset to read, not the last one read.** Off by
one and every restart silently replays or skips exactly one record per
partition, which no casual test catches.

**A `SyncGroup` from a generation the group has left must be rejected.** Two
consumers starting at the same moment race: the first completes a rebalance
alone and takes every partition, the second's join starts a new rebalance, and
then the first consumer's `SyncGroup` — computed for the *old* generation —
arrives and marks the group stable again. The second member ends up holding an
empty assignment and reads nothing, forever, with no error anywhere. The fix is
three lines; finding it took a packet trace.
