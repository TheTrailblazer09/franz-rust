# franz

A Kafka-compatible event streaming broker, written in Rust.

Real Kafka clients — `kcat`, `librdkafka`, the Java client — can produce to it
and consume from it without knowing it isn't Kafka.

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
they are not. So these rounds exercise offset continuity and clean restart, and
the torn-tail path is exercised directly by a unit test that writes half a batch
to a log file and reopens it. Genuinely proving the torn case end to end needs
power loss or a fault-injection layer that fails a write partway.

## Testing

```
cargo test              # 41 unit tests
scripts/crash_test.sh   # SIGKILL loop
scripts/bench.sh        # throughput table above
```

The tests worth looking at are the ones using bytes captured from a real
client rather than bytes this code produced: `parses_a_real_librdkafka_handshake`
and the two `decodes_a_real_kcat_request_*` cases. A round-trip test only proves
the encoder and decoder agree with each other, which they will even when both
are wrong.

## What it does not do

| Not implemented | Why |
| --- | --- |
| Consumer groups | The coordinator, rebalance protocol and `__consumer_offsets` are a project of their own. A simple consumer (`kcat -C`) works; `kcat -G` does not |
| Replication | Single node. Every partition reports itself as leader and sole ISR |
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

## Roadmap

- [x] A TCP server that reads length-prefixed Kafka frames
- [x] Answer `ApiVersions`, so a client finishes its handshake
- [x] Answer `Metadata`, so `kcat -L` lists topics
- [x] Append records to a segmented log on disk
- [x] Answer `Produce`, so `kcat -P` writes
- [x] Answer `Fetch`, so `kcat -C` reads it back
- [x] Survive `SIGKILL` mid-write with no acknowledged data lost
- [ ] Consumer groups
- [ ] Replication across brokers
