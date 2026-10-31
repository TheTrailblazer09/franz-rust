//! Per-partition replication state on the leader.
//!
//! The leader learns each follower's position from the fetch offsets in its
//! requests: a follower asking for offset N is telling you it already has
//! everything below N. The **high watermark** is the lowest of those across
//! the in-sync set, and it is the highest offset a consumer may read.
//!
//! That restriction is the whole point. A record above the high watermark
//! exists on the leader but is not yet on enough replicas to be safe, and if
//! leadership moved it could vanish. A consumer that could read it would watch
//! a record it had already seen disappear, which breaks the one promise a log
//! makes.
//!
//! A follower that stops fetching is dropped from the in-sync set, which
//! *advances* the high watermark rather than stalling it. One slow replica must
//! not be able to freeze the partition for everybody.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// How long a follower can go without fetching before it is out of sync.
pub const MAX_LAG: Duration = Duration::from_secs(10);

pub struct Replication {
    /// Everything this broker is willing to serve a consumer.
    high_watermark: i64,
    /// Follower id to its position and when it last asked.
    followers: HashMap<i32, Follower>,
    /// Replica ids for this partition, this broker included.
    replicas: Vec<i32>,
    leader: i32,
}

struct Follower {
    offset: i64,
    last_fetch: Instant,
}

impl Replication {
    pub fn new(replicas: Vec<i32>, leader: i32) -> Self {
        Replication {
            high_watermark: 0,
            followers: HashMap::new(),
            replicas,
            leader,
        }
    }

    pub fn high_watermark(&self) -> i64 {
        self.high_watermark
    }

    pub fn replicas(&self) -> &[i32] {
        &self.replicas
    }

    /// The leader plus every follower that has fetched recently.
    pub fn in_sync(&self) -> Vec<i32> {
        let mut isr = vec![self.leader];
        for id in &self.replicas {
            if *id == self.leader {
                continue;
            }
            if self
                .followers
                .get(id)
                .is_some_and(|f| f.last_fetch.elapsed() < MAX_LAG)
            {
                isr.push(*id);
            }
        }
        isr.sort_unstable();
        isr
    }

    /// Records where a follower has reached and recomputes the high watermark.
    ///
    /// `leader_end` is the leader's own log end offset, which caps everything:
    /// a follower cannot be ahead of the data it is copying.
    pub fn follower_fetched(&mut self, id: i32, offset: i64, leader_end: i64) {
        let entry = self.followers.entry(id).or_insert(Follower {
            offset,
            last_fetch: Instant::now(),
        });
        entry.offset = offset.min(leader_end);
        entry.last_fetch = Instant::now();
        self.recompute(leader_end);
    }

    /// Called when the leader appends, and when a follower drops out.
    pub fn recompute(&mut self, leader_end: i64) {
        let in_sync = self.in_sync();

        let mut lowest = leader_end;
        for id in &in_sync {
            if *id == self.leader {
                continue;
            }
            let at = self.followers.get(id).map_or(0, |f| f.offset);
            lowest = lowest.min(at);
        }

        // Never moves backwards: a replica rejoining with an older position
        // must not un-commit what consumers were already allowed to read.
        self.high_watermark = self.high_watermark.max(lowest);
    }

    /// A single-broker partition commits everything the moment it is written.
    pub fn commit_all(&mut self, leader_end: i64) {
        self.high_watermark = leader_end;
    }
}
