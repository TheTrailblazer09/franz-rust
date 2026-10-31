//! The seven APIs a consumer group needs, and the internal topic its offsets
//! live in.
//!
//! Offsets are stored by appending to `__consumer_offsets`, a topic like any
//! other, and the map is rebuilt by replaying that topic at startup. Reusing
//! the log costs almost nothing once the log exists, and it is the part of
//! Kafka's design most worth copying.
//!
//! The key/value encoding is franz's own, not Kafka's, so a franz data
//! directory is not readable by a real broker. Kafka's layout is a versioned
//! struct that would have to be matched byte for byte to be worth claiming.
//!
//! The off-by-one worth stating: a committed offset is the **next** offset to
//! read, not the last one read. Getting it wrong silently replays or skips
//! exactly one record per partition per restart, which no casual test catches.

use std::time::Duration;

use crate::batch;
use crate::broker::Broker;
use crate::group::{Commit, Coordinator, OffsetKey};
use crate::protocol::{Reader, Result, Writer, codes};

pub const OFFSETS_TOPIC: &str = "__consumer_offsets";

/// Which offsets partition a group's commits live in. Always this broker on a
/// single node, but the mapping is what a cluster would route on.
pub fn partition_for(group: &str, partitions: i32) -> i32 {
    let mut hash: u32 = 2166136261;
    for b in group.as_bytes() {
        hash ^= *b as u32;
        hash = hash.wrapping_mul(16777619);
    }
    (hash % partitions as u32) as i32
}

// --- FindCoordinator (10) ---------------------------------------------------

pub fn find_coordinator(
    r: &mut Reader,
    w: &mut Writer,
    version: i16,
    node_id: i32,
    host: &str,
    port: i32,
) -> Result<()> {
    let key = r.string()?;
    if version >= 1 {
        r.i8()?; // key_type: 0 group, 1 transaction
    }

    if version >= 1 {
        w.i32(0); // throttle_time_ms
    }
    let error_code = if key.is_empty() {
        codes::INVALID_GROUP_ID
    } else {
        codes::NONE
    };
    w.i16(error_code);
    if version >= 1 {
        w.nullable_string(None); // error_message
    }
    w.i32(node_id);
    w.string(host);
    w.i32(port);
    Ok(())
}

// --- JoinGroup (11) ---------------------------------------------------------

pub fn join_group(r: &mut Reader, w: &mut Writer, version: i16, co: &Coordinator) -> Result<()> {
    let group_id = r.string()?;
    let session_timeout = Duration::from_millis(r.i32()?.max(0) as u64);
    let rebalance_timeout = if version >= 1 {
        Duration::from_millis(r.i32()?.max(0) as u64)
    } else {
        session_timeout
    };
    let member_id = r.string()?;
    if version >= 5 {
        r.nullable_string()?; // group_instance_id
    }
    let protocol_type = r.string()?;

    let count = r.array_len()?.unwrap_or(0);
    let mut protocols = Vec::new();
    for _ in 0..count {
        let name = r.string()?;
        let metadata = r.nullable_bytes()?.unwrap_or_default().to_vec();
        protocols.push((name, metadata));
    }

    let result = co.join(
        &group_id,
        &member_id,
        &protocol_type,
        protocols,
        session_timeout,
        rebalance_timeout,
    );

    if version >= 2 {
        w.i32(0); // throttle_time_ms
    }
    w.i16(result.error_code);
    w.i32(result.generation);
    w.string(&result.protocol);
    w.string(&result.leader);
    w.string(&result.member_id);
    w.array_len(result.members.len());
    for (id, metadata) in &result.members {
        w.string(id);
        if version >= 5 {
            w.nullable_string(None); // group_instance_id
        }
        w.nullable_bytes(Some(metadata));
    }
    Ok(())
}

// --- SyncGroup (14) ---------------------------------------------------------

pub fn sync_group(r: &mut Reader, w: &mut Writer, version: i16, co: &Coordinator) -> Result<()> {
    let group_id = r.string()?;
    let generation = r.i32()?;
    let member_id = r.string()?;
    if version >= 3 {
        r.nullable_string()?; // group_instance_id
    }

    let count = r.array_len()?.unwrap_or(0);
    let mut assignments = Vec::new();
    for _ in 0..count {
        let member = r.string()?;
        let assignment = r.nullable_bytes()?.unwrap_or_default().to_vec();
        assignments.push((member, assignment));
    }

    let result = co.sync(&group_id, &member_id, generation, assignments);

    if version >= 1 {
        w.i32(0); // throttle_time_ms
    }
    w.i16(result.error_code);
    w.nullable_bytes(Some(&result.assignment));
    Ok(())
}

// --- Heartbeat (12) --------------------------------------------------------

pub fn heartbeat(r: &mut Reader, w: &mut Writer, version: i16, co: &Coordinator) -> Result<()> {
    let group_id = r.string()?;
    let generation = r.i32()?;
    let member_id = r.string()?;
    if version >= 3 {
        r.nullable_string()?; // group_instance_id
    }

    let error_code = co.heartbeat(&group_id, &member_id, generation);

    if version >= 1 {
        w.i32(0); // throttle_time_ms
    }
    w.i16(error_code);
    Ok(())
}

// --- LeaveGroup (13) -------------------------------------------------------

pub fn leave_group(r: &mut Reader, w: &mut Writer, version: i16, co: &Coordinator) -> Result<()> {
    let group_id = r.string()?;

    let members = if version >= 3 {
        let count = r.array_len()?.unwrap_or(0);
        let mut ids = Vec::new();
        for _ in 0..count {
            ids.push(r.string()?);
            r.nullable_string()?; // group_instance_id
        }
        ids
    } else {
        vec![r.string()?]
    };

    co.leave(&group_id, &members);

    if version >= 1 {
        w.i32(0); // throttle_time_ms
    }
    w.i16(codes::NONE);
    if version >= 3 {
        w.array_len(members.len());
        for id in &members {
            w.string(id);
            w.nullable_string(None); // group_instance_id
            w.i16(codes::NONE);
        }
    }
    Ok(())
}

// --- OffsetCommit (8) ------------------------------------------------------

pub fn offset_commit(
    r: &mut Reader,
    w: &mut Writer,
    version: i16,
    broker: &Broker,
    co: &Coordinator,
) -> Result<()> {
    let group_id = r.string()?;
    let generation = if version >= 1 { r.i32()? } else { -1 };
    let member_id = if version >= 1 {
        r.string()?
    } else {
        String::new()
    };
    if version >= 7 {
        r.nullable_string()?; // group_instance_id
    }
    if (2..=4).contains(&version) {
        r.i64()?; // retention_time_ms
    }

    let topic_count = r.array_len()?.unwrap_or(0);
    let mut commits = Vec::new();
    for _ in 0..topic_count {
        let name = r.string()?;
        let partition_count = r.array_len()?.unwrap_or(0);
        let mut partitions = Vec::new();
        for _ in 0..partition_count {
            let index = r.i32()?;
            let offset = r.i64()?;
            if version >= 6 {
                r.i32()?; // committed_leader_epoch
            }
            if version == 1 {
                r.i64()?; // commit_timestamp, only ever in v1
            }
            let metadata = r.nullable_string()?;
            partitions.push((index, offset, metadata));
        }
        commits.push((name, partitions));
    }

    // A commit from a member the group has moved past must not land, or a
    // consumer evicted mid-rebalance rewinds the new owner's position.
    let stale = if generation >= 0 {
        co.heartbeat(&group_id, &member_id, generation)
    } else {
        codes::NONE
    };

    let mut durable = Vec::new();
    if stale == codes::NONE {
        for (topic, partitions) in &commits {
            for (index, offset, metadata) in partitions {
                let key = OffsetKey {
                    group: group_id.clone(),
                    topic: topic.clone(),
                    partition: *index,
                };
                let commit = Commit {
                    offset: *offset,
                    metadata: metadata.clone(),
                };
                durable.push((encode_key(&key), Some(encode_value(&commit))));
                co.commit(key, commit);
            }
        }
    }

    // Written after the in-memory map so a failed append cannot leave the two
    // disagreeing in the direction that loses a commit.
    let mut write_error = codes::NONE;
    if !durable.is_empty()
        && let Err(e) = append_offsets(broker, &group_id, durable)
    {
        println!("  could not persist offsets for {group_id}: {e}");
        write_error = codes::UNKNOWN_SERVER_ERROR;
    }

    let error_code = if stale != codes::NONE {
        stale
    } else {
        write_error
    };

    if version >= 3 {
        w.i32(0); // throttle_time_ms
    }
    w.array_len(commits.len());
    for (topic, partitions) in &commits {
        w.string(topic);
        w.array_len(partitions.len());
        for (index, _, _) in partitions {
            w.i32(*index);
            w.i16(error_code);
        }
    }
    Ok(())
}

// --- OffsetFetch (9) -------------------------------------------------------

pub fn offset_fetch(r: &mut Reader, w: &mut Writer, version: i16, co: &Coordinator) -> Result<()> {
    let group_id = r.string()?;

    let topic_count = r.array_len()?;
    let requested: Option<Vec<(String, Vec<i32>)>> = match topic_count {
        None => None,
        Some(count) => {
            let mut topics = Vec::new();
            for _ in 0..count {
                let name = r.string()?;
                let partition_count = r.array_len()?.unwrap_or(0);
                let mut partitions = Vec::new();
                for _ in 0..partition_count {
                    partitions.push(r.i32()?);
                }
                topics.push((name, partitions));
            }
            Some(topics)
        }
    };

    // Null means every topic this group has committed for.
    let topics = requested.unwrap_or_else(|| {
        let mut by_topic: Vec<(String, Vec<i32>)> = Vec::new();
        for (key, _) in co.all_commits() {
            if key.group != group_id {
                continue;
            }
            match by_topic.iter_mut().find(|(name, _)| *name == key.topic) {
                Some((_, partitions)) => partitions.push(key.partition),
                None => by_topic.push((key.topic.clone(), vec![key.partition])),
            }
        }
        by_topic
    });

    if version >= 3 {
        w.i32(0); // throttle_time_ms
    }
    w.array_len(topics.len());
    for (topic, partitions) in &topics {
        w.string(topic);
        w.array_len(partitions.len());
        for index in partitions {
            let commit = co.committed(&OffsetKey {
                group: group_id.clone(),
                topic: topic.clone(),
                partition: *index,
            });
            w.i32(*index);
            // -1 means "no committed offset", which sends the consumer to its
            // auto.offset.reset rather than to the start of the log.
            w.i64(commit.as_ref().map_or(-1, |c| c.offset));
            if version >= 5 {
                w.i32(-1); // committed_leader_epoch
            }
            w.nullable_string(commit.as_ref().and_then(|c| c.metadata.as_deref()));
            w.i16(codes::NONE);
        }
    }
    if version >= 2 {
        w.i16(codes::NONE); // top-level error_code
    }
    Ok(())
}

// --- the internal topic ----------------------------------------------------

fn encode_key(key: &OffsetKey) -> Vec<u8> {
    let mut w = Writer::new();
    w.i16(0); // key format version
    w.string(&key.group);
    w.string(&key.topic);
    w.i32(key.partition);
    w.into_inner()
}

fn encode_value(commit: &Commit) -> Vec<u8> {
    let mut w = Writer::new();
    w.i16(0); // value format version
    w.i64(commit.offset);
    w.nullable_string(commit.metadata.as_deref());
    w.into_inner()
}

fn decode(key: &[u8], value: Option<&[u8]>) -> Option<(OffsetKey, Commit)> {
    let mut r = Reader::new(key);
    if r.i16().ok()? != 0 {
        return None;
    }
    let key = OffsetKey {
        group: r.string().ok()?,
        topic: r.string().ok()?,
        partition: r.i32().ok()?,
    };

    let value = value?;
    let mut r = Reader::new(value);
    if r.i16().ok()? != 0 {
        return None;
    }
    let commit = Commit {
        offset: r.i64().ok()?,
        metadata: r.nullable_string().ok()?,
    };
    Some((key, commit))
}

fn append_offsets(
    broker: &Broker,
    group: &str,
    records: Vec<batch::Record>,
) -> std::io::Result<()> {
    let partitions = broker.topics.get(OFFSETS_TOPIC).map_or(1, |t| t.partitions);
    let index = partition_for(group, partitions);

    let Some(log) = broker.log(OFFSETS_TOPIC, index) else {
        return Err(std::io::Error::other(format!(
            "{OFFSETS_TOPIC}-{index} does not exist"
        )));
    };

    let mut blob = batch::build(&records);
    let mut log = log.lock().expect("offsets log mutex");
    log.append(&mut blob).map_err(std::io::Error::other)?;
    log.sync()
}

/// Replays the whole internal topic to rebuild the offset map.
///
/// Later commits for a key overwrite earlier ones, so a straight replay in
/// offset order lands on the newest. Real Kafka compacts the topic so this
/// does not grow without bound; franz keeps every version.
pub fn load_offsets(broker: &Broker, co: &Coordinator) -> std::io::Result<usize> {
    let Some(topic) = broker.topics.get(OFFSETS_TOPIC) else {
        return Ok(0);
    };
    let mut loaded = 0;

    for index in 0..topic.partitions {
        let Some(log) = broker.log(OFFSETS_TOPIC, index) else {
            continue;
        };
        let log = log.lock().expect("offsets log mutex");

        let mut offset = log.start_offset();
        let end = log.end_offset();
        while offset < end {
            let bytes = log.read(offset, 1 << 20)?;
            if bytes.is_empty() {
                break;
            }
            let batches = match batch::parse_all(&bytes) {
                Ok(batches) => batches,
                Err(e) => {
                    println!("  {OFFSETS_TOPIC}-{index} unreadable at {offset}: {e}");
                    break;
                }
            };
            if let Ok(records) = batch::records(&bytes) {
                for (key, value) in records {
                    if let Some((key, commit)) = decode(&key, value.as_deref()) {
                        co.commit(key, commit);
                        loaded += 1;
                    }
                }
            }
            offset = batches
                .last()
                .map(|b| b.base_offset + b.offsets())
                .unwrap_or(end);
        }
    }

    Ok(loaded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_commit_round_trips_through_the_record_encoding() {
        let key = OffsetKey {
            group: "readers".into(),
            topic: "demo".into(),
            partition: 2,
        };
        let commit = Commit {
            offset: 4096,
            metadata: Some("note".into()),
        };

        let records = vec![(encode_key(&key), Some(encode_value(&commit)))];
        let blob = batch::build(&records);
        let back = batch::records(&blob).unwrap();

        assert_eq!(back.len(), 1);
        let (got_key, got_commit) = decode(&back[0].0, back[0].1.as_deref()).unwrap();
        assert_eq!(got_key, key);
        assert_eq!(got_commit.offset, 4096);
        assert_eq!(got_commit.metadata.as_deref(), Some("note"));
    }

    #[test]
    fn several_commits_fit_in_one_batch() {
        let records: Vec<_> = (0..5)
            .map(|p| {
                let key = OffsetKey {
                    group: "g".into(),
                    topic: "t".into(),
                    partition: p,
                };
                (
                    encode_key(&key),
                    Some(encode_value(&Commit {
                        offset: p as i64 * 10,
                        metadata: None,
                    })),
                )
            })
            .collect();

        let blob = batch::build(&records);
        let back = batch::records(&blob).unwrap();
        assert_eq!(back.len(), 5);

        for (i, (key, value)) in back.iter().enumerate() {
            let (key, commit) = decode(key, value.as_deref()).unwrap();
            assert_eq!(key.partition, i as i32);
            assert_eq!(commit.offset, i as i64 * 10);
        }
    }

    #[test]
    fn groups_land_on_a_stable_partition() {
        assert_eq!(partition_for("readers", 8), partition_for("readers", 8));
        let spread: std::collections::HashSet<i32> = (0..50)
            .map(|i| partition_for(&format!("g{i}"), 8))
            .collect();
        assert!(spread.len() > 1, "every group hashed to one partition");
    }
}
