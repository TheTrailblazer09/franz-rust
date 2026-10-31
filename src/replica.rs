//! The follower side: a thread per partition that copies from the leader.
//!
//! Followers replicate by issuing ordinary `Fetch` requests, the same API a
//! consumer uses, with `replica_id` set to their node id instead of -1. That
//! reuse is deliberate: replication inherits the long-polling, the index and
//! the batch handling that the consumer path already has, and the leader
//! learns each follower's position from the fetch offsets it already receives.
//!
//! It also means this module is the only place franz speaks the protocol as a
//! *client*, so it is the only place that encodes a request and decodes a
//! response rather than the other way round.

use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use crate::broker::Broker;
use crate::cluster::Node;
use crate::protocol::{ApiKey, Reader, Writer, codes};

/// Fetch v11: the newest version franz implements, and the last before the
/// API turns flexible.
const FETCH_VERSION: i16 = 11;

const MAX_WAIT: i32 = 500;
const MAX_BYTES: i32 = 1024 * 1024;
const RETRY: Duration = Duration::from_millis(500);

/// Starts one fetcher thread per partition this broker follows.
pub fn start(broker: Arc<Broker>) {
    for (topic, partition, leader) in broker.followed() {
        let Some(node) = broker.cluster.node(leader).cloned() else {
            println!("no address for node {leader}, cannot follow {topic}-{partition}");
            continue;
        };

        let broker = Arc::clone(&broker);
        std::thread::spawn(move || follow(broker, topic, partition, node));
    }
}

fn follow(broker: Arc<Broker>, topic: String, partition: i32, leader: Node) {
    let me = broker.cluster.me;
    println!(
        "following {topic}-{partition} from node {} at {}",
        leader.id,
        leader.address()
    );

    let mut stream: Option<TcpStream> = None;
    let mut correlation = 0i32;

    loop {
        let Some(held) = broker.partition(&topic, partition) else {
            return;
        };

        if stream.is_none() {
            match connect(&leader) {
                Ok(fresh) => stream = Some(fresh),
                Err(e) => {
                    println!("{topic}-{partition}: cannot reach node {}: {e}", leader.id);
                    std::thread::sleep(RETRY);
                    continue;
                }
            }
        }
        let socket = stream.as_mut().expect("connected");

        let from = held.log.lock().expect("log mutex").end_offset();
        correlation += 1;

        match exchange(socket, me, &topic, partition, from, correlation) {
            Ok(Fetched {
                error_code,
                records,
            }) => {
                if error_code != codes::NONE {
                    // NOT_LEADER_OR_FOLLOWER here means the config disagrees
                    // across the cluster, which is worth saying out loud since
                    // nothing will fix it on its own.
                    println!("{topic}-{partition}: leader returned error {error_code}, retrying");
                    std::thread::sleep(RETRY);
                    continue;
                }

                if records.is_empty() {
                    continue;
                }

                let mut log = held.log.lock().expect("log mutex");
                let mut bytes = records;
                match log.append_replicated(&mut bytes) {
                    Ok(_) => {
                        drop(log);
                        broker.notify_appended();
                    }
                    Err(e) => {
                        drop(log);
                        // A follower that cannot apply the leader's bytes has
                        // diverged, and without leader epochs there is nothing
                        // safe to do about it automatically.
                        println!("{topic}-{partition}: cannot apply replicated bytes: {e}");
                        std::thread::sleep(RETRY);
                    }
                }
            }
            Err(e) => {
                println!(
                    "{topic}-{partition}: fetch from node {} failed: {e}",
                    leader.id
                );
                stream = None;
                std::thread::sleep(RETRY);
            }
        }
    }
}

fn connect(leader: &Node) -> std::io::Result<TcpStream> {
    let stream = match leader.socket() {
        Some(address) => TcpStream::connect_timeout(&address, Duration::from_secs(2))?,
        None => TcpStream::connect(leader.address())?,
    };
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    Ok(stream)
}

struct Fetched {
    error_code: i16,
    records: Vec<u8>,
}

/// One Fetch round trip for a single partition.
fn exchange(
    socket: &mut TcpStream,
    me: i32,
    topic: &str,
    partition: i32,
    from: i64,
    correlation: i32,
) -> std::io::Result<Fetched> {
    let mut w = Writer::new();
    w.i16(ApiKey::Fetch as i16);
    w.i16(FETCH_VERSION);
    w.i32(correlation);
    w.nullable_string(Some("franz-replica"));

    w.i32(me); // replica_id, which is what marks this a follower
    w.i32(MAX_WAIT);
    w.i32(1); // min_bytes
    w.i32(MAX_BYTES); // v3+
    w.i8(0); // isolation_level, v4+
    w.i32(0); // session_id, v7+
    w.i32(-1); // session_epoch, v7+

    w.array_len(1);
    w.string(topic);
    w.array_len(1);
    w.i32(partition);
    w.i32(-1); // current_leader_epoch, v9+
    w.i64(from);
    w.i64(0); // log_start_offset, v5+
    w.i32(MAX_BYTES);

    w.array_len(0); // forgotten_topics, v7+
    w.string(""); // rack_id, v11+

    let body = w.into_inner();
    socket.write_all(&(body.len() as u32).to_be_bytes())?;
    socket.write_all(&body)?;
    socket.flush()?;

    let mut prefix = [0u8; 4];
    socket.read_exact(&mut prefix)?;
    let len = u32::from_be_bytes(prefix) as usize;
    if len == 0 || len > 128 * 1024 * 1024 {
        return Err(std::io::Error::new(
            ErrorKind::InvalidData,
            format!("leader replied with a {len}-byte frame"),
        ));
    }
    let mut frame = vec![0u8; len];
    socket.read_exact(&mut frame)?;

    decode(&frame, correlation).map_err(|e| std::io::Error::new(ErrorKind::InvalidData, e))
}

fn decode(frame: &[u8], correlation: i32) -> Result<Fetched, String> {
    let mut r = Reader::new(frame);
    let bad = |e: crate::protocol::Error| e.to_string();

    let echoed = r.i32().map_err(bad)?;
    if echoed != correlation {
        return Err(format!("correlation_id {echoed}, expected {correlation}"));
    }

    r.i32().map_err(bad)?; // throttle_time_ms
    let top_level = r.i16().map_err(bad)?; // v7+
    r.i32().map_err(bad)?; // session_id
    if top_level != codes::NONE {
        return Ok(Fetched {
            error_code: top_level,
            records: Vec::new(),
        });
    }

    let topics = r.array_len().map_err(bad)?.unwrap_or(0);
    for _ in 0..topics {
        r.string().map_err(bad)?;
        let partitions = r.array_len().map_err(bad)?.unwrap_or(0);
        // One partition is requested, so the first one in the reply is it.
        if partitions > 0 {
            r.i32().map_err(bad)?; // partition_index
            let error_code = r.i16().map_err(bad)?;
            r.i64().map_err(bad)?; // high_watermark
            r.i64().map_err(bad)?; // last_stable_offset, v4+
            r.i64().map_err(bad)?; // log_start_offset, v5+
            let aborted = r.array_len().map_err(bad)?.unwrap_or(0);
            for _ in 0..aborted {
                r.i64().map_err(bad)?;
                r.i64().map_err(bad)?;
            }
            r.i32().map_err(bad)?; // preferred_read_replica, v11+
            let records = r.nullable_bytes().map_err(bad)?.unwrap_or_default();

            // One partition per request, so the first is the only one.
            return Ok(Fetched {
                error_code,
                records: records.to_vec(),
            });
        }
    }

    Ok(Fetched {
        error_code: codes::NONE,
        records: Vec::new(),
    })
}
