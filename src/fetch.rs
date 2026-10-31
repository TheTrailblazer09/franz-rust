//! Fetch (key 1): serve record batches from a given offset.
//!
//! A fetch with `max_wait_ms` set must not spin. One that finds less than
//! `min_bytes` sleeps on the broker's append signal until data arrives or the
//! deadline passes, so an idle consumer costs nothing.
//!
//! The bytes returned are the batches exactly as the producer encoded them.
//! Nothing is decoded or re-encoded, so the checksum the consumer verifies is
//! the one the producer computed.

use std::time::{Duration, Instant};

use crate::broker::Broker;
use crate::protocol::{Reader, Result, Writer, codes};

pub struct Request {
    /// -1 from a consumer, a node id from a follower replicating.
    pub replica_id: i32,
    pub max_wait: Duration,
    pub min_bytes: i32,
    pub topics: Vec<Topic>,
}

impl Request {
    /// A follower is copying the log and may read past the high watermark; a
    /// consumer may not.
    pub fn is_follower(&self) -> bool {
        self.replica_id >= 0
    }
}

pub struct Topic {
    pub name: String,
    pub partitions: Vec<Partition>,
}

pub struct Partition {
    pub index: i32,
    pub fetch_offset: i64,
    pub max_bytes: i32,
}

pub struct TopicResult {
    pub name: String,
    pub partitions: Vec<PartitionResult>,
}

pub struct PartitionResult {
    pub index: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub log_start_offset: i64,
    pub records: Vec<u8>,
}

impl PartitionResult {
    fn failed(index: i32, error_code: i16) -> Self {
        PartitionResult {
            index,
            error_code,
            high_watermark: -1,
            log_start_offset: -1,
            records: Vec::new(),
        }
    }
}

/// Reads from `offset` but hands back only batches that end at or below
/// `limit`, so a consumer never sees past the high watermark.
fn read_up_to(
    log: &crate::log::Log,
    offset: i64,
    limit: i64,
    max_bytes: usize,
) -> std::io::Result<Vec<u8>> {
    if offset >= limit {
        return Ok(Vec::new());
    }

    let bytes = log.read(offset, max_bytes)?;
    if bytes.is_empty() {
        return Ok(bytes);
    }

    // Batches are all-or-nothing on the wire, so a batch straddling the limit
    // is dropped rather than cut.
    let Ok(batches) = crate::batch::parse_all(&bytes) else {
        return Ok(bytes);
    };
    let mut keep = 0usize;
    for parsed in &batches {
        if parsed.base_offset + parsed.offsets() > limit {
            break;
        }
        keep += parsed.total_len;
    }

    Ok(bytes[..keep].to_vec())
}

impl Request {
    pub fn decode(r: &mut Reader, version: i16) -> Result<Self> {
        let replica_id = r.i32()?;
        let max_wait = Duration::from_millis(r.i32()?.max(0) as u64);
        let min_bytes = r.i32()?;
        if version >= 3 {
            r.i32()?; // max_bytes
        }
        if version >= 4 {
            r.i8()?; // isolation_level
        }
        if version >= 7 {
            r.i32()?; // session_id
            r.i32()?; // session_epoch
        }

        let topic_count = r.array_len()?.unwrap_or(0);
        let mut topics = Vec::new();
        for _ in 0..topic_count {
            let name = r.string()?;
            let partition_count = r.array_len()?.unwrap_or(0);
            let mut partitions = Vec::new();
            for _ in 0..partition_count {
                let index = r.i32()?;
                if version >= 9 {
                    r.i32()?; // current_leader_epoch
                }
                let fetch_offset = r.i64()?;
                if version >= 5 {
                    r.i64()?; // log_start_offset, only meaningful from a follower
                }
                let max_bytes = r.i32()?;
                partitions.push(Partition {
                    index,
                    fetch_offset,
                    max_bytes,
                });
            }
            topics.push(Topic { name, partitions });
        }

        if version >= 7 {
            let forgotten = r.array_len()?.unwrap_or(0);
            for _ in 0..forgotten {
                r.string()?;
                let count = r.array_len()?.unwrap_or(0);
                for _ in 0..count {
                    r.i32()?;
                }
            }
        }
        if version >= 11 {
            r.string()?; // rack_id
        }

        Ok(Request {
            replica_id,
            max_wait,
            min_bytes,
            topics,
        })
    }
}

pub fn apply(broker: &Broker, request: &Request) -> Vec<TopicResult> {
    let deadline = Instant::now() + request.max_wait;

    loop {
        // Read the counter first: an append landing between the gather below
        // and the wait must not be missed.
        let seen = broker.appends();
        let results = gather(broker, request);

        let bytes: usize = results
            .iter()
            .flat_map(|t| &t.partitions)
            .map(|p| p.records.len())
            .sum();
        let failed = results
            .iter()
            .flat_map(|t| &t.partitions)
            .any(|p| p.error_code != codes::NONE);

        if failed || bytes as i32 >= request.min_bytes.max(1) {
            return results;
        }

        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return results;
        };
        broker.wait_for_append(seen, remaining);
    }
}

fn gather(broker: &Broker, request: &Request) -> Vec<TopicResult> {
    let mut results = Vec::new();

    for topic in &request.topics {
        let mut partitions = Vec::new();

        for partition in &topic.partitions {
            if !broker.knows(&topic.name, partition.index) {
                partitions.push(PartitionResult::failed(
                    partition.index,
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                ));
                continue;
            }

            // Consumers read from the leader, and so do followers, so a broker
            // that does not lead this partition has nothing to serve either way.
            let Some(held) = broker
                .partition(&topic.name, partition.index)
                .filter(|_| broker.cluster.leads(&topic.name, partition.index))
            else {
                partitions.push(PartitionResult::failed(
                    partition.index,
                    codes::NOT_LEADER_OR_FOLLOWER,
                ));
                continue;
            };

            let log = held.log.lock().expect("partition log mutex");
            let start = log.start_offset();
            let end = log.end_offset();
            drop(log);

            // A follower is copying the log and may read everything the leader
            // has. A consumer stops at the high watermark, because a record
            // above it is not yet on enough replicas to be safe to show.
            let high_watermark = held.high_watermark();
            let readable = if request.is_follower() {
                end
            } else {
                high_watermark.min(end)
            };

            // A follower asking for offset N has everything below N.
            if let (true, Some(state)) = (request.is_follower(), &held.replication) {
                state.lock().expect("replication mutex").follower_fetched(
                    request.replica_id,
                    partition.fetch_offset,
                    end,
                );
            }

            // Equal to the end is normal and means caught up, not out of range.
            if partition.fetch_offset < start || partition.fetch_offset > end {
                partitions.push(PartitionResult {
                    index: partition.index,
                    error_code: codes::OFFSET_OUT_OF_RANGE,
                    high_watermark,
                    log_start_offset: start,
                    records: Vec::new(),
                });
                continue;
            }

            let log = held.log.lock().expect("partition log mutex");
            let max_bytes = partition.max_bytes.max(1) as usize;
            let records = match read_up_to(&log, partition.fetch_offset, readable, max_bytes) {
                Ok(records) => records,
                Err(e) => {
                    println!("  read failed for {}-{}: {e}", topic.name, partition.index);
                    partitions.push(PartitionResult {
                        index: partition.index,
                        error_code: codes::UNKNOWN_SERVER_ERROR,
                        high_watermark,
                        log_start_offset: start,
                        records: Vec::new(),
                    });
                    continue;
                }
            };

            partitions.push(PartitionResult {
                index: partition.index,
                error_code: codes::NONE,
                high_watermark,
                log_start_offset: start,
                records,
            });
        }

        results.push(TopicResult {
            name: topic.name.clone(),
            partitions,
        });
    }

    results
}

pub fn write_response(w: &mut Writer, version: i16, results: &[TopicResult]) {
    w.i32(0); // throttle_time_ms
    if version >= 7 {
        w.i16(codes::NONE); // top-level error_code
        w.i32(0); // session_id: fetch sessions are not implemented
    }

    w.array_len(results.len());
    for topic in results {
        w.string(&topic.name);
        w.array_len(topic.partitions.len());
        for partition in &topic.partitions {
            w.i32(partition.index);
            w.i16(partition.error_code);
            w.i64(partition.high_watermark);
            if version >= 4 {
                // No transactions, so nothing is ever uncommitted.
                w.i64(partition.high_watermark); // last_stable_offset
            }
            if version >= 5 {
                w.i64(partition.log_start_offset);
            }
            if version >= 4 {
                w.array_len(0); // aborted_transactions
            }
            if version >= 11 {
                w.i32(-1); // preferred_read_replica: read from the leader
            }
            w.nullable_bytes(Some(&partition.records));
        }
    }
}
