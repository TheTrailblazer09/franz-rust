//! ListOffsets (key 2): resolve earliest and latest offsets.
//!
//! `kcat -o beginning` needs this before it can fetch anything.
//!
//! Only the two sentinel timestamps are answered: -2 for earliest and -1 for
//! latest. A real timestamp needs a time index alongside the offset index,
//! which does not exist here, so those queries come back as offset -1 and the
//! client falls back to one of the sentinels.

use crate::broker::Broker;
use crate::protocol::{Reader, Result, Writer, codes};

pub const EARLIEST: i64 = -2;
pub const LATEST: i64 = -1;

pub struct Request {
    pub topics: Vec<Topic>,
}

pub struct Topic {
    pub name: String,
    pub partitions: Vec<Partition>,
}

pub struct Partition {
    pub index: i32,
    pub timestamp: i64,
}

pub struct TopicResult {
    pub name: String,
    pub partitions: Vec<PartitionResult>,
}

pub struct PartitionResult {
    pub index: i32,
    pub error_code: i16,
    pub timestamp: i64,
    pub offset: i64,
}

impl Request {
    pub fn decode(r: &mut Reader, version: i16) -> Result<Self> {
        r.i32()?; // replica_id
        if version >= 2 {
            r.i8()?; // isolation_level
        }

        let topic_count = r.array_len()?.unwrap_or(0);
        let mut topics = Vec::new();
        for _ in 0..topic_count {
            let name = r.string()?;
            let partition_count = r.array_len()?.unwrap_or(0);
            let mut partitions = Vec::new();
            for _ in 0..partition_count {
                let index = r.i32()?;
                if version >= 4 {
                    r.i32()?; // current_leader_epoch
                }
                let timestamp = r.i64()?;
                partitions.push(Partition { index, timestamp });
            }
            topics.push(Topic { name, partitions });
        }

        Ok(Request { topics })
    }
}

pub fn apply(broker: &Broker, request: &Request) -> Vec<TopicResult> {
    let mut results = Vec::new();

    for topic in &request.topics {
        let mut partitions = Vec::new();

        for partition in &topic.partitions {
            let Some(log) = broker.log(&topic.name, partition.index) else {
                partitions.push(PartitionResult {
                    index: partition.index,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    timestamp: -1,
                    offset: -1,
                });
                continue;
            };

            let log = log.lock().expect("partition log mutex");
            let offset = match partition.timestamp {
                EARLIEST => log.start_offset(),
                LATEST => log.end_offset(),
                _ => -1,
            };

            partitions.push(PartitionResult {
                index: partition.index,
                error_code: codes::NONE,
                timestamp: -1,
                offset,
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
    if version >= 2 {
        w.i32(0); // throttle_time_ms
    }

    w.array_len(results.len());
    for topic in results {
        w.string(&topic.name);
        w.array_len(topic.partitions.len());
        for partition in &topic.partitions {
            w.i32(partition.index);
            w.i16(partition.error_code);
            w.i64(partition.timestamp);
            w.i64(partition.offset);
            if version >= 4 {
                w.i32(0); // leader_epoch
            }
        }
    }
}
