//! Everything the connection threads share: the cluster layout, the topic
//! list, and one commit log plus its replication state per partition.
//!
//! A thread per connection, so each partition's log sits behind its own
//! `Mutex` and two clients producing to different partitions never queue
//! behind each other.
//!
//! Fetches that find nothing wait on one broker-wide condition variable rather
//! than one per partition. An append anywhere wakes every waiter, which costs
//! the occasional pointless wake-up and saves holding a signal per partition;
//! a waiter that finds nothing new simply waits again.

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use crate::cluster::Cluster;
use crate::log::{self, Log};
use crate::replication::Replication;
use crate::topics::Topics;

pub struct Partition {
    pub log: Mutex<Log>,
    /// `None` on a partition this broker does not lead; only a leader tracks
    /// followers and a high watermark.
    pub replication: Option<Mutex<Replication>>,
}

impl Partition {
    /// Highest offset a consumer may read.
    pub fn high_watermark(&self) -> i64 {
        match &self.replication {
            Some(state) => state.lock().expect("replication mutex").high_watermark(),
            // A follower serves no consumers, so its own log end is the only
            // sensible answer and nothing reads it.
            None => self.log.lock().expect("log mutex").end_offset(),
        }
    }
}

pub struct Broker {
    pub topics: Topics,
    pub cluster: Cluster,
    /// Indexed by partition, `None` where this broker is not a replica.
    partitions: HashMap<String, Vec<Option<Partition>>>,
    appends: Mutex<u64>,
    appended: Condvar,
}

impl Broker {
    pub fn open(
        topics: Topics,
        cluster: Cluster,
        data_dir: &Path,
        cfg: log::Config,
    ) -> io::Result<Self> {
        let mut partitions = HashMap::new();

        for topic in topics.all() {
            let mut held = Vec::with_capacity(topic.partitions as usize);
            for index in 0..topic.partitions {
                // A broker that is not a replica stores nothing for this
                // partition, so the directory existing means this node holds
                // it. Requests for it still get an answer: see Broker::knows.
                if !cluster.holds(&topic.name, index) {
                    held.push(None);
                    continue;
                }

                let replicas = cluster.replicas(&topic.name, index);
                let leader = cluster.leader(&topic.name, index).unwrap_or(cluster.me);
                let role = if cluster.leads(&topic.name, index) {
                    "leader"
                } else {
                    "follower"
                };

                let dir = data_dir.join(format!("{}-{index}", topic.name));
                let (log, recovery) = Log::open(&dir, cfg)?;

                if recovery.truncated_bytes > 0 {
                    println!(
                        "  {}-{index} ({role}): dropped {} bytes of partial write, ends at offset {}",
                        topic.name, recovery.truncated_bytes, recovery.end_offset
                    );
                } else if cluster.is_alone() {
                    println!(
                        "  {}-{index}: offsets {}..{} in {} segment(s)",
                        topic.name,
                        log.start_offset(),
                        recovery.end_offset,
                        recovery.segments
                    );
                } else {
                    println!(
                        "  {}-{index} ({role}): offsets {}..{}, replicas {:?}",
                        topic.name,
                        log.start_offset(),
                        recovery.end_offset,
                        replicas
                    );
                }

                let end = log.end_offset();
                let replication = if cluster.leads(&topic.name, index) {
                    let mut state = Replication::new(replicas, leader);
                    // Everything already on disk was committed before the
                    // restart, or it would not have been acknowledged.
                    state.commit_all(end);
                    Some(Mutex::new(state))
                } else {
                    None
                };

                held.push(Some(Partition {
                    log: Mutex::new(log),
                    replication,
                }));
            }
            partitions.insert(topic.name.clone(), held);
        }

        Ok(Broker {
            topics,
            cluster,
            partitions,
            appends: Mutex::new(0),
            appended: Condvar::new(),
        })
    }

    /// The local copy of a partition, or `None` if this broker has none.
    pub fn partition(&self, topic: &str, index: i32) -> Option<&Partition> {
        if index < 0 {
            return None;
        }
        self.partitions.get(topic)?.get(index as usize)?.as_ref()
    }

    /// Whether the partition exists anywhere in the cluster, regardless of who
    /// holds it. A request for one this broker does not have is answered with
    /// NOT_LEADER_OR_FOLLOWER, which sends the client to the right broker,
    /// rather than UNKNOWN_TOPIC_OR_PARTITION, which tells it the data is gone.
    pub fn knows(&self, topic: &str, index: i32) -> bool {
        self.topics
            .get(topic)
            .is_some_and(|t| index >= 0 && index < t.partitions)
    }

    pub fn log(&self, topic: &str, index: i32) -> Option<&Mutex<Log>> {
        self.partition(topic, index).map(|p| &p.log)
    }

    /// A counter read before checking for data, so a fetch cannot miss an
    /// append that lands between its check and its wait.
    pub fn appends(&self) -> u64 {
        *self.appends.lock().expect("appends mutex")
    }

    pub fn notify_appended(&self) {
        let mut appends = self.appends.lock().expect("appends mutex");
        *appends += 1;
        self.appended.notify_all();
    }

    pub fn wait_for_append(&self, since: u64, timeout: Duration) {
        let appends = self.appends.lock().expect("appends mutex");
        if *appends != since {
            return;
        }
        let _ = self.appended.wait_timeout(appends, timeout);
    }

    /// Every partition this broker replicates from someone else.
    pub fn followed(&self) -> Vec<(String, i32, i32)> {
        let mut out = Vec::new();
        for topic in self.topics.all() {
            for index in 0..topic.partitions {
                if self.cluster.follows(&topic.name, index)
                    && let Some(leader) = self.cluster.leader(&topic.name, index)
                {
                    out.push((topic.name.clone(), index, leader));
                }
            }
        }
        out
    }
}
