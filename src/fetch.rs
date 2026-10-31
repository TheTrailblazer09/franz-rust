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
    pub max_wait: Duration,
    pub min_bytes: i32,
    pub topics: Vec<Topic>,
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

impl Request {
    pub fn decode(r: &mut Reader, version: i16) -> Result<Self> {
        r.i32()?; // replica_id
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
            let Some(log) = broker.log(&topic.name, partition.index) else {
                partitions.push(PartitionResult {
                    index: partition.index,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    high_watermark: -1,
                    log_start_offset: -1,
                    records: Vec::new(),
                });
                continue;
            };

            let log = log.lock().expect("partition log mutex");
            let start = log.start_offset();
            // Single node, so every append is immediately committed: the high
            // watermark is just the end of the log.
            let end = log.end_offset();

            // Equal to the end is normal and means caught up, not out of range.
            if partition.fetch_offset < start || partition.fetch_offset > end {
                partitions.push(PartitionResult {
                    index: partition.index,
                    error_code: codes::OFFSET_OUT_OF_RANGE,
                    high_watermark: end,
                    log_start_offset: start,
                    records: Vec::new(),
                });
                continue;
            }

            let max_bytes = partition.max_bytes.max(1) as usize;
            let records = match log.read(partition.fetch_offset, max_bytes) {
                Ok(records) => records,
                Err(e) => {
                    println!("  read failed for {}-{}: {e}", topic.name, partition.index);
                    partitions.push(PartitionResult {
                        index: partition.index,
                        error_code: codes::UNKNOWN_SERVER_ERROR,
                        high_watermark: end,
                        log_start_offset: start,
                        records: Vec::new(),
                    });
                    continue;
                }
            };

            partitions.push(PartitionResult {
                index: partition.index,
                error_code: codes::NONE,
                high_watermark: end,
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
