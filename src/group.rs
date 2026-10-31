//! Consumer groups: the coordinator side of the rebalance protocol.
//!
//! The broker does not compute the assignment. It picks the first member to
//! join as leader, forwards every member's subscription to that leader, and
//! the leader decides who gets which partitions and sends the answer back in
//! `SyncGroup`. The coordinator only distributes the result.
//!
//! That is why franz interoperates with clients using range, round-robin or
//! sticky assignors without implementing any of them: the strategy lives in
//! the client and travels as opaque bytes.
//!
//! A rebalance is a barrier. `JoinGroup` from one member holds open until every
//! known member has rejoined or the rebalance timeout passes, and `SyncGroup`
//! from a follower holds open until the leader's assignment arrives. With a
//! thread per connection those are plain condition-variable waits.

use std::collections::HashMap;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::protocol::codes;

/// How long a forming group waits for more members before settling on an
/// assignment, matching the idea behind Kafka's `group.initial.rebalance.delay`.
///
/// Without it, the first consumer to arrive completes a rebalance by itself and
/// takes every partition, and the second one to arrive forces another. The
/// group still converges, but it spends a few seconds with one member doing all
/// the work, and tests that start two consumers at once see it.
const SETTLE: Duration = Duration::from_millis(1500);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

pub struct Member {
    pub id: String,
    /// Protocol name to subscription bytes, passed to the leader untouched.
    pub protocols: Vec<(String, Vec<u8>)>,
    pub assignment: Vec<u8>,
    pub session_timeout: Duration,
    pub rebalance_timeout: Duration,
    pub joined_generation: i32,
    pub synced_generation: i32,
    pub last_seen: Instant,
}

pub struct Group {
    pub state: State,
    pub generation: i32,
    pub leader: Option<String>,
    pub protocol: Option<String>,
    pub members: HashMap<String, Member>,
    next_member: u64,
}

impl Group {
    fn new() -> Self {
        Group {
            state: State::Empty,
            generation: 0,
            leader: None,
            protocol: None,
            members: HashMap::new(),
            next_member: 0,
        }
    }

    /// True once every known member has rejoined at the current generation, or
    /// the slowest member's rebalance timeout has passed.
    fn join_complete(&self, since: Instant) -> bool {
        if self.members.is_empty() {
            return false;
        }
        let all_in = self
            .members
            .values()
            .all(|m| m.joined_generation == self.generation + 1);
        let longest = self
            .members
            .values()
            .map(|m| m.rebalance_timeout)
            .max()
            .unwrap_or_default();

        // A group forming from nothing holds the door open a moment longer.
        let settled = self.generation > 0 || since.elapsed() >= SETTLE;

        (all_in && settled) || since.elapsed() >= longest
    }

    fn sync_complete(&self) -> bool {
        self.state == State::Stable
    }

    /// Drops members that stopped heartbeating. Returns true if the group has
    /// to rebalance as a result.
    fn evict_dead(&mut self) -> bool {
        let generation = self.generation;
        let before = self.members.len();

        self.members.retain(|_, m| {
            // A member mid-rebalance has not had a chance to heartbeat yet.
            m.joined_generation == generation + 1 || m.last_seen.elapsed() < m.session_timeout
        });

        before != self.members.len()
    }
}

/// Where a group lives and what it is doing. One lock for all groups, since
/// group traffic is a trickle next to produce and fetch.
pub struct Coordinator {
    groups: Mutex<HashMap<String, Group>>,
    changed: Condvar,
    offsets: Mutex<HashMap<OffsetKey, Commit>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OffsetKey {
    pub group: String,
    pub topic: String,
    pub partition: i32,
}

#[derive(Debug, Clone)]
pub struct Commit {
    pub offset: i64,
    pub metadata: Option<String>,
}

pub struct JoinResult {
    pub error_code: i16,
    pub generation: i32,
    pub protocol: String,
    pub leader: String,
    pub member_id: String,
    /// Only the leader is told who else is in the group.
    pub members: Vec<(String, Vec<u8>)>,
}

pub struct SyncResult {
    pub error_code: i16,
    pub assignment: Vec<u8>,
}

impl Coordinator {
    pub fn new() -> Self {
        Coordinator {
            groups: Mutex::new(HashMap::new()),
            changed: Condvar::new(),
            offsets: Mutex::new(HashMap::new()),
        }
    }

    pub fn join(
        &self,
        group_id: &str,
        member_id: &str,
        protocol_type: &str,
        protocols: Vec<(String, Vec<u8>)>,
        session_timeout: Duration,
        rebalance_timeout: Duration,
    ) -> JoinResult {
        let mut groups = self.groups.lock().expect("groups mutex");
        let group = groups
            .entry(group_id.to_string())
            .or_insert_with(Group::new);

        if protocols.is_empty() || protocol_type.is_empty() {
            return JoinResult::failed(codes::INCONSISTENT_GROUP_PROTOCOL);
        }

        // A member with no id is new. Kafka can bounce it once to hand out an
        // id; assigning one here and carrying on is the older, simpler flow
        // and every client still supports it.
        let member_id = if member_id.is_empty() {
            group.next_member += 1;
            format!("franz-{}-{}", group_id, group.next_member)
        } else {
            if !group.members.contains_key(member_id) {
                return JoinResult::failed(codes::UNKNOWN_MEMBER_ID);
            }
            member_id.to_string()
        };

        let now = Instant::now();
        let entry = group.members.entry(member_id.clone()).or_insert(Member {
            id: member_id.clone(),
            protocols: Vec::new(),
            assignment: Vec::new(),
            session_timeout,
            rebalance_timeout,
            joined_generation: -1,
            synced_generation: -1,
            last_seen: now,
        });
        entry.protocols = protocols;
        entry.session_timeout = session_timeout;
        entry.rebalance_timeout = rebalance_timeout;
        entry.joined_generation = group.generation + 1;
        entry.last_seen = now;

        // Any join restarts the barrier: whoever already arrived waits again.
        if group.state != State::PreparingRebalance {
            group.state = State::PreparingRebalance;
        }
        println!(
            "group {group_id}: {member_id} joining at generation {}, {} member(s) known",
            group.generation + 1,
            group.members.len()
        );
        self.changed.notify_all();

        let started = Instant::now();

        // Wait for the rest of the group.
        let mut groups = groups;
        loop {
            let group = groups.get(group_id).expect("group exists");
            if group.state == State::CompletingRebalance || group.state == State::Stable {
                break;
            }
            if group.join_complete(started) {
                let group = groups.get_mut(group_id).expect("group exists");
                group.generation += 1;
                group
                    .members
                    .retain(|_, m| m.joined_generation == group.generation);
                group.protocol = common_protocol(group);
                group.leader = group.members.keys().min().cloned();
                group.state = State::CompletingRebalance;
                println!(
                    "group {group_id}: generation {} with {} member(s), leader {}",
                    group.generation,
                    group.members.len(),
                    group.leader.as_deref().unwrap_or("?")
                );
                self.changed.notify_all();
                break;
            }
            let (next, _) = self
                .changed
                .wait_timeout(groups, Duration::from_millis(50))
                .expect("groups mutex");
            groups = next;
        }

        let group = groups.get(group_id).expect("group exists");
        if !group.members.contains_key(&member_id) {
            return JoinResult::failed(codes::UNKNOWN_MEMBER_ID);
        }
        let Some(protocol) = group.protocol.clone() else {
            return JoinResult::failed(codes::INCONSISTENT_GROUP_PROTOCOL);
        };
        let leader = group.leader.clone().unwrap_or_default();

        let members = if leader == member_id {
            group
                .members
                .values()
                .map(|m| (m.id.clone(), subscription(m, &protocol)))
                .collect()
        } else {
            Vec::new()
        };

        JoinResult {
            error_code: codes::NONE,
            generation: group.generation,
            protocol,
            leader,
            member_id,
            members,
        }
    }

    pub fn sync(
        &self,
        group_id: &str,
        member_id: &str,
        generation: i32,
        assignments: Vec<(String, Vec<u8>)>,
    ) -> SyncResult {
        let mut groups = self.groups.lock().expect("groups mutex");

        let Some(group) = groups.get_mut(group_id) else {
            return SyncResult::failed(codes::UNKNOWN_MEMBER_ID);
        };
        if !group.members.contains_key(member_id) {
            return SyncResult::failed(codes::UNKNOWN_MEMBER_ID);
        }
        if generation != group.generation {
            return SyncResult::failed(codes::ILLEGAL_GENERATION);
        }

        // A rebalance started after this member was told to sync, so its
        // assignment is for a generation the group has left. Accepting it here
        // marks the group stable again and strands whoever triggered the
        // rebalance with an empty assignment.
        if group.state == State::PreparingRebalance {
            println!("group {group_id}: {member_id} synced into a restarted rebalance");
            return SyncResult::failed(codes::REBALANCE_IN_PROGRESS);
        }

        let is_leader = group.leader.as_deref() == Some(member_id);
        if is_leader {
            if group.state != State::CompletingRebalance {
                return SyncResult::failed(codes::REBALANCE_IN_PROGRESS);
            }
            for (member, assignment) in assignments {
                if let Some(m) = group.members.get_mut(&member) {
                    m.assignment = assignment;
                }
            }
            group.state = State::Stable;
            println!("group {group_id}: stable at generation {generation}");
            self.changed.notify_all();
        }

        if let Some(m) = group.members.get_mut(member_id) {
            m.synced_generation = generation;
            m.last_seen = Instant::now();
        }

        let timeout = group
            .members
            .get(member_id)
            .map(|m| m.rebalance_timeout)
            .unwrap_or(Duration::from_secs(60));
        let started = Instant::now();

        // Followers wait here until the leader's assignment lands.
        let mut groups = groups;
        while !groups.get(group_id).is_some_and(|g| g.sync_complete()) {
            match groups.get(group_id) {
                Some(g) if g.generation != generation => {
                    return SyncResult::failed(codes::ILLEGAL_GENERATION);
                }
                Some(g) if g.state == State::PreparingRebalance => {
                    return SyncResult::failed(codes::REBALANCE_IN_PROGRESS);
                }
                None => return SyncResult::failed(codes::UNKNOWN_MEMBER_ID),
                Some(_) => {}
            }
            if started.elapsed() >= timeout {
                return SyncResult::failed(codes::REBALANCE_IN_PROGRESS);
            }
            let (next, _) = self
                .changed
                .wait_timeout(groups, Duration::from_millis(50))
                .expect("groups mutex");
            groups = next;
        }

        let group = groups.get(group_id).expect("group exists");
        if generation != group.generation {
            return SyncResult::failed(codes::ILLEGAL_GENERATION);
        }
        match group.members.get(member_id) {
            Some(m) => SyncResult {
                error_code: codes::NONE,
                assignment: m.assignment.clone(),
            },
            None => SyncResult::failed(codes::UNKNOWN_MEMBER_ID),
        }
    }

    pub fn heartbeat(&self, group_id: &str, member_id: &str, generation: i32) -> i16 {
        let mut groups = self.groups.lock().expect("groups mutex");

        let Some(group) = groups.get_mut(group_id) else {
            return codes::UNKNOWN_MEMBER_ID;
        };
        if !group.members.contains_key(member_id) {
            return codes::UNKNOWN_MEMBER_ID;
        }
        if generation != group.generation {
            return codes::ILLEGAL_GENERATION;
        }
        if group.state == State::PreparingRebalance {
            return codes::REBALANCE_IN_PROGRESS;
        }

        if let Some(m) = group.members.get_mut(member_id) {
            m.last_seen = Instant::now();
        }
        codes::NONE
    }

    pub fn leave(&self, group_id: &str, member_ids: &[String]) {
        let mut groups = self.groups.lock().expect("groups mutex");
        let Some(group) = groups.get_mut(group_id) else {
            return;
        };

        for id in member_ids {
            group.members.remove(id);
        }
        group.state = if group.members.is_empty() {
            State::Empty
        } else {
            State::PreparingRebalance
        };
        self.changed.notify_all();
    }

    /// Evicts members that stopped heartbeating, so their partitions get
    /// reassigned. Called from a background thread.
    pub fn expire_members(&self) -> Vec<String> {
        let mut groups = self.groups.lock().expect("groups mutex");
        let mut rebalanced = Vec::new();

        for (id, group) in groups.iter_mut() {
            if group.state == State::Stable && group.evict_dead() {
                group.state = if group.members.is_empty() {
                    State::Empty
                } else {
                    State::PreparingRebalance
                };
                rebalanced.push(id.clone());
            }
        }

        if !rebalanced.is_empty() {
            self.changed.notify_all();
        }
        rebalanced
    }

    pub fn commit(&self, key: OffsetKey, commit: Commit) {
        self.offsets
            .lock()
            .expect("offsets mutex")
            .insert(key, commit);
    }

    pub fn committed(&self, key: &OffsetKey) -> Option<Commit> {
        self.offsets
            .lock()
            .expect("offsets mutex")
            .get(key)
            .cloned()
    }

    /// Every commit, for persisting or inspecting.
    pub fn all_commits(&self) -> Vec<(OffsetKey, Commit)> {
        self.offsets
            .lock()
            .expect("offsets mutex")
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

impl Default for Coordinator {
    fn default() -> Self {
        Self::new()
    }
}

/// The protocol every member supports. Clients send a preference list; the
/// first one they all share wins.
fn common_protocol(group: &Group) -> Option<String> {
    let first = group.members.values().next()?;
    for (name, _) in &first.protocols {
        if group
            .members
            .values()
            .all(|m| m.protocols.iter().any(|(n, _)| n == name))
        {
            return Some(name.clone());
        }
    }
    None
}

fn subscription(member: &Member, protocol: &str) -> Vec<u8> {
    member
        .protocols
        .iter()
        .find(|(name, _)| name == protocol)
        .map(|(_, bytes)| bytes.clone())
        .unwrap_or_default()
}

impl JoinResult {
    fn failed(error_code: i16) -> Self {
        JoinResult {
            error_code,
            generation: -1,
            protocol: String::new(),
            leader: String::new(),
            member_id: String::new(),
            members: Vec::new(),
        }
    }
}

impl SyncResult {
    fn failed(error_code: i16) -> Self {
        SyncResult {
            error_code,
            assignment: Vec::new(),
        }
    }
}
