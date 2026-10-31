//! Produce (key 0): append records and report where they landed.
//!
//! `acks=0` means the client wants no reply at all. Sending one desynchronises
//! the connection, because the next response it reads will be matched against
//! the wrong request.
//!
//! `throttle_time_ms` is the last field here, not the first as in Metadata.

use crate::batch;
use crate::broker::Broker;
use crate::log::AppendError;
use crate::protocol::{Reader, Result, Writer, codes};

pub struct Request {
    pub acks: i16,
    pub topics: Vec<Topic>,
}

pub struct Topic {
    pub name: String,
    pub partitions: Vec<Partition>,
}

pub struct Partition {
    pub index: i32,
    /// Owned because the assigned offsets are written into the batch headers
    /// before the bytes reach the log.
    pub records: Vec<u8>,
}

pub struct TopicResult {
    pub name: String,
    pub partitions: Vec<PartitionResult>,
}

pub struct PartitionResult {
    pub index: i32,
    pub error_code: i16,
    pub base_offset: i64,
    pub log_start_offset: i64,
}

impl Request {
    pub fn decode(r: &mut Reader, version: i16) -> Result<Self> {
        if version >= 3 {
            r.nullable_string()?; // transactional_id
        }
        let acks = r.i16()?;
        r.i32()?; // timeout_ms

        let topic_count = r.array_len()?.unwrap_or(0);
        let mut topics = Vec::new();
        for _ in 0..topic_count {
            let name = r.string()?;
            let partition_count = r.array_len()?.unwrap_or(0);
            let mut partitions = Vec::new();
            for _ in 0..partition_count {
                let index = r.i32()?;
                let records = r.nullable_bytes()?.unwrap_or_default().to_vec();
                partitions.push(Partition { index, records });
            }
            topics.push(Topic { name, partitions });
        }

        Ok(Request { acks, topics })
    }
}

/// Appends everything in the request. Mutates the records in place to stamp
/// the assigned offsets.
pub fn apply(broker: &Broker, request: &mut Request) -> Vec<TopicResult> {
    let mut results = Vec::new();
    let mut appended = false;

    for topic in &mut request.topics {
        let mut partitions = Vec::new();

        for partition in &mut topic.partitions {
            let Some(log) = broker.log(&topic.name, partition.index) else {
                partitions.push(PartitionResult {
                    index: partition.index,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    base_offset: -1,
                    log_start_offset: -1,
                });
                continue;
            };

            let mut log = log.lock().expect("partition log mutex");
            let outcome = log.append(&mut partition.records);
            let log_start_offset = log.start_offset();
            drop(log);

            let (error_code, base_offset) = match outcome {
                Ok(offset) => {
                    appended = true;
                    (codes::NONE, offset)
                }
                Err(AppendError::Batch(e)) => {
                    println!(
                        "  rejected a batch for {}-{}: {e}",
                        topic.name, partition.index
                    );
                    (codes::CORRUPT_MESSAGE, -1)
                }
                Err(AppendError::Io(e)) => {
                    println!("  write failed for {}-{}: {e}", topic.name, partition.index);
                    (codes::UNKNOWN_SERVER_ERROR, -1)
                }
            };

            partitions.push(PartitionResult {
                index: partition.index,
                error_code,
                base_offset,
                log_start_offset,
            });
        }

        results.push(TopicResult {
            name: topic.name.clone(),
            partitions,
        });
    }

    if appended {
        broker.notify_appended();
    }
    results
}

pub fn write_response(w: &mut Writer, version: i16, results: &[TopicResult]) {
    w.array_len(results.len());
    for topic in results {
        w.string(&topic.name);
        w.array_len(topic.partitions.len());
        for partition in &topic.partitions {
            w.i32(partition.index);
            w.i16(partition.error_code);
            w.i64(partition.base_offset);
            w.i64(-1); // log_append_time: the records keep the client's own
            if version >= 5 {
                w.i64(partition.log_start_offset);
            }
            if version >= 8 {
                w.array_len(0); // record_errors
                w.nullable_string(None); // error_message
            }
        }
    }
    w.i32(0); // throttle_time_ms, at the end for this API
}

/// Total records across the request, for logging.
pub fn record_count(request: &Request) -> i32 {
    request
        .topics
        .iter()
        .flat_map(|t| &t.partitions)
        .filter_map(|p| batch::parse_all(&p.records).ok())
        .flatten()
        .map(|b| b.record_count)
        .sum()
}
