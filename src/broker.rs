//! Everything the connection threads share: the topic list and one commit log
//! per partition.
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

use crate::log::{self, Log};
use crate::topics::Topics;

pub struct Broker {
    pub topics: Topics,
    partitions: HashMap<String, Vec<Mutex<Log>>>,
    appends: Mutex<u64>,
    appended: Condvar,
}

impl Broker {
    pub fn open(topics: Topics, data_dir: &Path, cfg: log::Config) -> io::Result<Self> {
        let mut partitions = HashMap::new();

        for topic in topics.all() {
            let mut logs = Vec::with_capacity(topic.partitions as usize);
            for index in 0..topic.partitions {
                let dir = data_dir.join(format!("{}-{index}", topic.name));
                let (log, recovery) = Log::open(&dir, cfg)?;

                if recovery.truncated_bytes > 0 {
                    println!(
                        "  {}-{index}: dropped {} bytes of partial write, ends at offset {}",
                        topic.name, recovery.truncated_bytes, recovery.end_offset
                    );
                } else {
                    println!(
                        "  {}-{index}: offsets {}..{} in {} segment(s)",
                        topic.name,
                        log.start_offset(),
                        recovery.end_offset,
                        recovery.segments
                    );
                }

                logs.push(Mutex::new(log));
            }
            partitions.insert(topic.name.clone(), logs);
        }

        Ok(Broker {
            topics,
            partitions,
            appends: Mutex::new(0),
            appended: Condvar::new(),
        })
    }

    pub fn log(&self, topic: &str, index: i32) -> Option<&Mutex<Log>> {
        if index < 0 {
            return None;
        }
        self.partitions.get(topic)?.get(index as usize)
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
}
