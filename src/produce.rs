//! Produce (key 0): append records and report where they landed.
//!
//! `acks=0` means the client wants no reply at all. Sending one desynchronises
//! the connection, because the next response it reads will be matched against
//! the wrong request.
//!
//! `acks=all` means the record is on every in-sync replica before the producer
//! hears about it, which is the high watermark reaching the offset just
//! written. `acks=1` returns as soon as the leader's own append is done.
//!
//! `throttle_time_ms` is the last field here, not the first as in Metadata.

use std::time::{Duration, Instant};

use crate::batch;
use crate::broker::Broker;
use crate::log::AppendError;
use crate::protocol::{Reader, Result, Writer, codes};

/// How long `acks=all` waits for the in-sync replicas before handing the
/// producer a timeout it can retry.
const COMMIT_TIMEOUT: Duration = Duration::from_secs(10);

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

impl PartitionResult {
    fn failed(index: i32, error_code: i16) -> Self {
        PartitionResult {
            index,
            error_code,
            base_offset: -1,
            log_start_offset: -1,
        }
    }
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

/// Appends everything in the request, stamping the assigned offsets into the
/// batch headers as it goes.
pub fn apply(broker: &Broker, request: &mut Request) -> Vec<TopicResult> {
    let mut results = Vec::new();
    let mut appended = false;
    // Where each partition must commit to before acks=all can answer.
    let mut awaiting: Vec<(String, i32, i64)> = Vec::new();

    for topic in &mut request.topics {
        let mut partitions = Vec::new();

        for partition in &mut topic.partitions {
            if !broker.knows(&topic.name, partition.index) {
                partitions.push(PartitionResult::failed(
                    partition.index,
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                ));
                continue;
            }

            // Writes only go to the leader. This error tells the client to
            // refresh metadata and retry against the broker it names, which
            // librdkafka does without surfacing anything to the application.
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

            // An ISR too small to hold the write is refused, rather than
            // accepted on one node and called durable.
            let in_sync = held
                .replication
                .as_ref()
                .map(|state| state.lock().expect("replication mutex").in_sync().len())
                .unwrap_or(1);
            if request.acks == -1 && in_sync < broker.cluster.min_insync {
                partitions.push(PartitionResult::failed(
                    partition.index,
                    codes::NOT_ENOUGH_REPLICAS,
                ));
                continue;
            }

            let mut log = held.log.lock().expect("partition log mutex");
            let outcome = log.append(&mut partition.records);
            let log_start_offset = log.start_offset();
            let end = log.end_offset();
            drop(log);

            let (error_code, base_offset) = match outcome {
                Ok(offset) => {
                    appended = true;
                    if let Some(state) = &held.replication {
                        let mut state = state.lock().expect("replication mutex");
                        if broker.cluster.is_alone() {
                            state.commit_all(end);
                        } else {
                            state.recompute(end);
                        }
                    }
                    if request.acks == -1 && !broker.cluster.is_alone() {
                        awaiting.push((topic.name.clone(), partition.index, end));
                    }
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
                // Only a follower can hit this, and a follower never serves
                // Produce.
                Err(AppendError::Divergent { .. }) => (codes::UNKNOWN_SERVER_ERROR, -1),
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

    for (topic, index, target) in awaiting {
        if !wait_for_commit(broker, &topic, index, target) {
            mark_timed_out(&mut results, &topic, index);
        }
    }

    results
}

/// Blocks until the in-sync replicas hold the record, which is what `acks=all`
/// promises. False on timeout.
fn wait_for_commit(broker: &Broker, topic: &str, index: i32, target: i64) -> bool {
    let Some(held) = broker.partition(topic, index) else {
        return false;
    };
    let deadline = Instant::now() + COMMIT_TIMEOUT;

    loop {
        if held.high_watermark() >= target {
            return true;
        }
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            println!("  {topic}-{index}: acks=all timed out waiting for offset {target}");
            return false;
        };

        // Followers fetch on their own schedule, and a follower that has gone
        // quiet is handled by the lag timeout rather than by a signal, so this
        // polls instead of waiting to be woken.
        std::thread::sleep(remaining.min(Duration::from_millis(20)));

        if let Some(state) = &held.replication {
            let end = held.log.lock().expect("log mutex").end_offset();
            state.lock().expect("replication mutex").recompute(end);
        }
    }
}

fn mark_timed_out(results: &mut [TopicResult], topic: &str, index: i32) {
    for result in results.iter_mut().filter(|r| r.name == topic) {
        for partition in result.partitions.iter_mut().filter(|p| p.index == index) {
            partition.error_code = codes::REQUEST_TIMED_OUT;
            partition.base_offset = -1;
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{Cluster, Node};
    use crate::topics::Topics;

    fn dir(name: &str) -> std::path::PathBuf {
        let path =
            std::env::temp_dir().join(format!("franz-produce-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        path
    }

    fn three_nodes(me: i32) -> Cluster {
        Cluster {
            me,
            nodes: (1..=3)
                .map(|id| Node {
                    id,
                    host: "127.0.0.1".into(),
                    port: 9090 + id,
                })
                .collect(),
            replication_factor: 2,
            min_insync: 2,
        }
    }

    fn request(partition: i32, acks: i16) -> Request {
        Request {
            acks,
            topics: vec![Topic {
                name: "demo".into(),
                partitions: vec![Partition {
                    index: partition,
                    records: batch::fixtures::batch(0, 2),
                }],
            }],
        }
    }

    #[test]
    fn a_write_to_a_partition_this_broker_does_not_lead_is_redirected() {
        let path = dir("notleader");
        let cluster = three_nodes(1);
        let topics = Topics::from_specs(&["demo:6".to_string()]).unwrap();

        // A partition broker 1 does not lead.
        let elsewhere = (0..6)
            .find(|p| !cluster.leads("demo", *p))
            .expect("some partition is led by another broker");

        let broker = Broker::open(topics, cluster, &path, crate::log::Config::default()).unwrap();
        let results = apply(&broker, &mut request(elsewhere, 1));

        assert_eq!(
            results[0].partitions[0].error_code,
            codes::NOT_LEADER_OR_FOLLOWER
        );
        assert_eq!(results[0].partitions[0].base_offset, -1);
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn a_partition_that_does_not_exist_is_unknown_not_misrouted() {
        let path = dir("unknown");
        let topics = Topics::from_specs(&["demo:2".to_string()]).unwrap();
        let broker = Broker::open(
            topics,
            Cluster::alone(1, "127.0.0.1", 9092),
            &path,
            crate::log::Config::default(),
        )
        .unwrap();

        let results = apply(&broker, &mut request(9, 1));
        assert_eq!(
            results[0].partitions[0].error_code,
            codes::UNKNOWN_TOPIC_OR_PARTITION
        );
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn acks_all_is_refused_when_the_isr_is_too_small() {
        let path = dir("notenough");
        let cluster = three_nodes(1);
        let topics = Topics::from_specs(&["demo:6".to_string()]).unwrap();

        let mine = (0..6)
            .find(|p| cluster.leads("demo", *p))
            .expect("some partition is led here");

        let broker = Broker::open(topics, cluster, &path, crate::log::Config::default()).unwrap();

        // No follower has fetched, so the ISR is just this broker, below the
        // min-insync of 2.
        let results = apply(&broker, &mut request(mine, -1));
        assert_eq!(
            results[0].partitions[0].error_code,
            codes::NOT_ENOUGH_REPLICAS
        );

        // acks=1 only needs the local append, so it still succeeds.
        let results = apply(&broker, &mut request(mine, 1));
        assert_eq!(results[0].partitions[0].error_code, codes::NONE);
        assert_eq!(results[0].partitions[0].base_offset, 0);
        let _ = std::fs::remove_dir_all(&path);
    }

    #[test]
    fn a_lone_broker_commits_immediately_at_acks_all() {
        let path = dir("alone");
        let topics = Topics::from_specs(&["demo:1".to_string()]).unwrap();
        let broker = Broker::open(
            topics,
            Cluster::alone(1, "127.0.0.1", 9092),
            &path,
            crate::log::Config::default(),
        )
        .unwrap();

        let results = apply(&broker, &mut request(0, -1));
        assert_eq!(results[0].partitions[0].error_code, codes::NONE);
        assert_eq!(broker.partition("demo", 0).unwrap().high_watermark(), 2);
        let _ = std::fs::remove_dir_all(&path);
    }
}
