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

## Roadmap

Each step is something you can run and see working.

- [x] A TCP server that reads length-prefixed Kafka frames
- [x] Answer `ApiVersions`, so a client finishes its handshake
- [x] Answer `Metadata`, so `kcat -L` lists topics
- [ ] Append records to a segmented log on disk
- [ ] Answer `Produce`, so `kcat -P` writes
- [ ] Answer `Fetch`, so `kcat -C` reads it back
- [ ] Survive `SIGKILL` mid-write with no acknowledged data lost
- [ ] Consumer groups
- [ ] Replication across brokers
