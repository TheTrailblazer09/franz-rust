//! Metadata (key 3): topic and partition discovery.
//!
//! Thirteen versions, and most of them insert a field into the middle of an
//! existing structure. Both halves below are written as a flat sequence of
//! version gates in the schema's field order, not a tidier one — reordering is
//! how a field ends up in the wrong place.

use crate::protocol::{ApiKey, Reader, Result, Writer, codes};
use crate::topics::{Topic, Topics};

const KEY: ApiKey = ApiKey::Metadata;

/// Who to connect to, as this broker describes itself.
pub struct BrokerInfo<'a> {
    pub node_id: i32,
    pub host: &'a str,
    pub port: i32,
    pub cluster_id: &'a str,
}

/// `topics: None` means every topic.
pub struct Request {
    pub topics: Option<Vec<String>>,
}

impl Request {
    pub fn decode(r: &mut Reader, version: i16) -> Result<Self> {
        let flexible = KEY.is_flexible(version);

        let count = if flexible {
            r.compact_array_len()?
        } else {
            r.array_len()?
        };

        let topics = match count {
            None => None,
            Some(n) => {
                let mut names = Vec::new();
                for _ in 0..n {
                    if version >= 10 {
                        r.uuid()?; // topic_id: we look topics up by name
                    }
                    let name = if flexible {
                        r.compact_nullable_string()?
                    } else {
                        Some(r.string()?)
                    };
                    if flexible {
                        r.tagged_fields()?;
                    }
                    names.extend(name);
                }
                Some(names)
            }
        };

        if version >= 4 {
            r.bool()?; // allow_auto_topic_creation
        }
        if (8..=10).contains(&version) {
            r.bool()?; // include_cluster_authorized_operations
        }
        if version >= 8 {
            r.bool()?; // include_topic_authorized_operations
        }
        if flexible {
            r.tagged_fields()?;
        }

        Ok(Request { topics })
    }
}

/// How a partition is laid out across the cluster.
pub struct Placement {
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub in_sync: Vec<i32>,
}

pub fn write_response(
    w: &mut Writer,
    version: i16,
    broker: &BrokerInfo,
    brokers: &[BrokerInfo],
    topics: &Topics,
    wanted: Option<&[String]>,
    placement: &dyn Fn(&str, i32) -> Placement,
) {
    let flexible = KEY.is_flexible(version);

    if version >= 3 {
        w.i32(0); // throttle_time_ms
    }

    array_len(w, flexible, brokers.len());
    for node in brokers {
        w.i32(node.node_id);
        if flexible {
            w.compact_string(node.host);
        } else {
            w.string(node.host);
        }
        w.i32(node.port);
        if version >= 1 {
            write_nullable_string(w, flexible, None); // rack
        }
        if flexible {
            w.empty_tagged_fields();
        }
    }

    if version >= 2 {
        write_nullable_string(w, flexible, Some(broker.cluster_id));
    }
    if version >= 1 {
        w.i32(broker.node_id); // controller_id
    }

    let listed: Vec<(&str, Option<&Topic>)> = match wanted {
        None => topics
            .all()
            .iter()
            .map(|t| (t.name.as_str(), Some(t)))
            .collect(),
        Some(names) => names.iter().map(|n| (n.as_str(), topics.get(n))).collect(),
    };

    array_len(w, flexible, listed.len());
    for (name, topic) in &listed {
        w.i16(match topic {
            Some(_) => codes::NONE,
            None => codes::UNKNOWN_TOPIC_OR_PARTITION,
        });

        // The name only became nullable at v12, and only so a response can
        // identify a topic by id alone.
        match (flexible, version >= 12) {
            (true, true) => w.compact_nullable_string(Some(name)),
            (true, false) => w.compact_string(name),
            (false, _) => w.string(name),
        }
        if version >= 10 {
            w.uuid(&topic.map_or([0u8; 16], |t| t.id));
        }
        if version >= 1 {
            w.bool(false); // is_internal
        }

        let partitions = topic.map_or(0, |t| t.partitions);
        array_len(w, flexible, partitions as usize);
        for index in 0..partitions {
            let placed = placement(name, index);

            w.i16(codes::NONE);
            w.i32(index);
            w.i32(placed.leader);
            if version >= 7 {
                // Leadership never moves without a controller, so the epoch
                // this broker reports never changes.
                w.i32(0); // leader_epoch
            }
            array_len(w, flexible, placed.replicas.len());
            for id in &placed.replicas {
                w.i32(*id);
            }
            array_len(w, flexible, placed.in_sync.len());
            for id in &placed.in_sync {
                w.i32(*id);
            }
            if version >= 5 {
                let offline: Vec<i32> = placed
                    .replicas
                    .iter()
                    .filter(|id| !placed.in_sync.contains(id))
                    .copied()
                    .collect();
                array_len(w, flexible, offline.len());
                for id in &offline {
                    w.i32(*id);
                }
            }
            if flexible {
                w.empty_tagged_fields();
            }
        }

        if version >= 8 {
            w.i32(i32::MIN); // topic_authorized_operations: not requested
        }
        if flexible {
            w.empty_tagged_fields();
        }
    }

    if (8..=10).contains(&version) {
        w.i32(i32::MIN); // cluster_authorized_operations
    }
    if flexible {
        w.empty_tagged_fields();
    }
}

fn array_len(w: &mut Writer, flexible: bool, n: usize) {
    if flexible {
        w.compact_array_len(n);
    } else {
        w.array_len(n);
    }
}

fn write_nullable_string(w: &mut Writer, flexible: bool, s: Option<&str>) {
    if flexible {
        w.compact_nullable_string(s);
    } else {
        w.nullable_string(s);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topics() -> Topics {
        Topics::from_specs(&["demo:2".to_string(), "logs:1".to_string()]).unwrap()
    }

    fn broker() -> BrokerInfo<'static> {
        BrokerInfo {
            node_id: 1,
            host: "127.0.0.1",
            port: 9092,
            cluster_id: "franz-test",
        }
    }

    fn encode(version: i16, wanted: Option<&[String]>) -> Vec<u8> {
        let mut w = Writer::new();
        let me = broker();
        let placement = |_: &str, _: i32| Placement {
            leader: 1,
            replicas: vec![1],
            in_sync: vec![1],
        };
        write_response(
            &mut w,
            version,
            &me,
            std::slice::from_ref(&me),
            &topics(),
            wanted,
            &placement,
        );
        w.into_inner()
    }

    #[test]
    fn null_topics_asks_for_everything() {
        let mut w = Writer::new();
        w.uvarint(0); // a null compact array, not an empty one
        w.bool(false);
        w.bool(false);
        w.empty_tagged_fields();
        let body = w.into_inner();

        let req = Request::decode(&mut Reader::new(&body), 12).unwrap();
        assert!(req.topics.is_none());
    }

    #[test]
    fn empty_topics_is_not_null() {
        let mut w = Writer::new();
        w.compact_array_len(0);
        w.bool(false);
        w.bool(false);
        w.empty_tagged_fields();
        let body = w.into_inner();

        let req = Request::decode(&mut Reader::new(&body), 12).unwrap();
        assert_eq!(req.topics.as_deref(), Some(&[][..]));
    }

    /// Captured from kcat 1.7.0, header stripped. A client that has never seen
    /// this code is the only way to find out whether the decoder matches the
    /// protocol or only matches our encoder.
    const KCAT_ALL_TOPICS: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00];

    const KCAT_ONE_TOPIC: &[u8] = &[
        0x02, // compact array: one topic
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // topic_id, all zero:
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // unknown to the client
        0x05, b'd', b'e', b'm', b'o', // compact "demo"
        0x00, // the topic's tagged fields
        0x01, // allow_auto_topic_creation
        0x00, // include_topic_authorized_operations
        0x00, // the request's tagged fields
    ];

    #[test]
    fn decodes_a_real_kcat_request_for_every_topic() {
        let req = Request::decode(&mut Reader::new(KCAT_ALL_TOPICS), 12).unwrap();
        assert!(req.topics.is_none());
    }

    #[test]
    fn decodes_a_real_kcat_request_for_one_topic() {
        let mut r = Reader::new(KCAT_ONE_TOPIC);
        let req = Request::decode(&mut r, 12).unwrap();
        assert_eq!(req.topics.unwrap(), ["demo"]);
        assert_eq!(r.remaining(), 0, "the whole body should be consumed");
    }

    #[test]
    fn names_round_trip_at_v12() {
        let mut w = Writer::new();
        w.compact_array_len(1);
        w.uuid(&[0u8; 16]);
        w.compact_nullable_string(Some("demo"));
        w.empty_tagged_fields();
        w.bool(true);
        w.bool(false);
        w.empty_tagged_fields();
        let body = w.into_inner();

        let req = Request::decode(&mut Reader::new(&body), 12).unwrap();
        assert_eq!(req.topics.unwrap(), ["demo"]);
    }

    #[test]
    fn a_classic_request_has_no_uuid_or_tagged_fields() {
        let mut w = Writer::new();
        w.array_len(1);
        w.string("demo");
        w.bool(true);
        let body = w.into_inner();

        let req = Request::decode(&mut Reader::new(&body), 4).unwrap();
        assert_eq!(req.topics.unwrap(), ["demo"]);
    }

    #[test]
    fn flexible_is_smaller_than_classic_for_the_same_content() {
        assert!(encode(12, None).len() < encode(8, None).len());
    }

    #[test]
    fn topic_id_appears_only_from_v10() {
        let id = topics().get("demo").unwrap().id;
        assert!(!encode(9, None).windows(16).any(|c| c == id));
        assert!(encode(10, None).windows(16).any(|c| c == id));
    }

    #[test]
    fn an_unknown_topic_is_an_error_code_with_no_partitions() {
        let wanted = vec!["missing".to_string()];
        let body = encode(12, Some(&wanted));

        let mut r = Reader::new(&body);
        r.i32().unwrap(); // throttle_time_ms
        r.compact_array_len().unwrap();
        r.i32().unwrap();
        r.compact_nullable_string().unwrap();
        r.i32().unwrap();
        r.compact_nullable_string().unwrap();
        r.tagged_fields().unwrap();
        r.compact_nullable_string().unwrap(); // cluster_id
        r.i32().unwrap(); // controller_id

        assert_eq!(r.compact_array_len().unwrap(), Some(1));
        assert_eq!(r.i16().unwrap(), codes::UNKNOWN_TOPIC_OR_PARTITION);
        assert_eq!(
            r.compact_nullable_string().unwrap().as_deref(),
            Some("missing")
        );
        assert_eq!(r.uuid().unwrap(), [0u8; 16]);
        assert!(!r.bool().unwrap());
        assert_eq!(r.compact_array_len().unwrap(), Some(0), "no partitions");
    }
}
