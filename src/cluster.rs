//! Who holds which partition.
//!
//! The assignment is a pure function of the config: every broker is told the
//! same peer list and the same replication factor, and they all compute the
//! same answer. That removes the need for a controller, consensus on metadata,
//! and the whole question of who decides — at the cost of the thing a
//! controller buys you, which is failover. A leader named by config stays the
//! leader; if that broker is down, its partitions are unavailable.
//!
//! That is a real limitation, not a detail. It is called out in the README,
//! because the honest version of "static cluster config" is "no failover".

use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

impl Node {
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn socket(&self) -> Option<SocketAddr> {
        self.address().parse().ok()
    }
}

#[derive(Debug, Clone)]
pub struct Cluster {
    pub me: i32,
    pub nodes: Vec<Node>,
    pub replication_factor: usize,
    /// A produce with `acks=all` fails rather than accepting a write the ISR
    /// is too small to hold.
    pub min_insync: usize,
}

impl Cluster {
    /// A single broker that leads everything, which is what Phase 1 and 2 were.
    pub fn alone(id: i32, host: &str, port: i32) -> Self {
        Cluster {
            me: id,
            nodes: vec![Node {
                id,
                host: host.to_string(),
                port,
            }],
            replication_factor: 1,
            min_insync: 1,
        }
    }

    pub fn node(&self, id: i32) -> Option<&Node> {
        self.nodes.iter().find(|n| n.id == id)
    }

    pub fn is_alone(&self) -> bool {
        self.nodes.len() <= 1
    }

    /// Replicas for a partition, leader first.
    ///
    /// Spread by topic name as well as partition index, so two topics with the
    /// same partition count do not pile every partition 0 onto one broker.
    pub fn replicas(&self, topic: &str, partition: i32) -> Vec<i32> {
        let count = self.nodes.len();
        if count == 0 {
            return Vec::new();
        }

        let wanted = self.replication_factor.min(count);
        let start = (name_hash(topic) as usize + partition as usize) % count;

        (0..wanted)
            .map(|i| self.nodes[(start + i) % count].id)
            .collect()
    }

    pub fn leader(&self, topic: &str, partition: i32) -> Option<i32> {
        self.replicas(topic, partition).first().copied()
    }

    pub fn leads(&self, topic: &str, partition: i32) -> bool {
        self.leader(topic, partition) == Some(self.me)
    }

    /// True when this broker holds the partition at all, leader or follower.
    pub fn holds(&self, topic: &str, partition: i32) -> bool {
        self.replicas(topic, partition).contains(&self.me)
    }

    pub fn follows(&self, topic: &str, partition: i32) -> bool {
        self.holds(topic, partition) && !self.leads(topic, partition)
    }
}

fn name_hash(name: &str) -> u32 {
    let mut hash: u32 = 2166136261;
    for b in name.as_bytes() {
        hash ^= *b as u32;
        hash = hash.wrapping_mul(16777619);
    }
    hash
}

/// Parses `ID@HOST:PORT`.
pub fn parse_peer(spec: &str) -> Result<Node, String> {
    let (id, address) = spec
        .split_once('@')
        .ok_or_else(|| format!("expected ID@HOST:PORT, got `{spec}`"))?;
    let id: i32 = id.parse().map_err(|_| format!("`{id}` is not a node id"))?;
    let (host, port) = address
        .rsplit_once(':')
        .ok_or_else(|| format!("`{address}` has no port"))?;
    let port: i32 = port
        .parse()
        .map_err(|_| format!("`{port}` is not a port"))?;

    if host.is_empty() {
        return Err(format!("`{spec}` has no host"));
    }

    Ok(Node {
        id,
        host: host.to_string(),
        port,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn three(factor: usize) -> Cluster {
        Cluster {
            me: 1,
            nodes: (1..=3)
                .map(|id| Node {
                    id,
                    host: "127.0.0.1".into(),
                    port: 9090 + id,
                })
                .collect(),
            replication_factor: factor,
            min_insync: 2,
        }
    }

    #[test]
    fn every_broker_computes_the_same_assignment() {
        let mut a = three(2);
        let mut b = three(2);
        a.me = 1;
        b.me = 3;

        for partition in 0..12 {
            assert_eq!(
                a.replicas("demo", partition),
                b.replicas("demo", partition),
                "partition {partition}"
            );
        }
    }

    #[test]
    fn leadership_is_spread_over_the_brokers() {
        let cluster = three(2);
        let leaders: Vec<i32> = (0..9).map(|p| cluster.leader("demo", p).unwrap()).collect();
        let distinct: std::collections::HashSet<i32> = leaders.iter().copied().collect();
        assert_eq!(distinct.len(), 3, "got {leaders:?}");
    }

    /// Two given names can collide — with three brokers that happens a third
    /// of the time. What matters is that names shift the layout at all, so
    /// many topics do not all stack their partition 0 on one broker.
    #[test]
    fn the_topic_name_shifts_the_layout() {
        let cluster = three(1);
        let layouts: std::collections::HashSet<Vec<i32>> = (0..12)
            .map(|t| {
                let name = format!("topic-{t}");
                (0..3).map(|p| cluster.leader(&name, p).unwrap()).collect()
            })
            .collect();

        assert_eq!(
            layouts.len(),
            3,
            "expected all three rotations, got {layouts:?}"
        );
    }

    #[test]
    fn replication_factor_is_capped_at_the_cluster_size() {
        let cluster = three(9);
        assert_eq!(cluster.replicas("demo", 0).len(), 3);
    }

    #[test]
    fn a_replica_set_has_no_duplicates() {
        let cluster = three(3);
        for partition in 0..12 {
            let replicas = cluster.replicas("demo", partition);
            let distinct: std::collections::HashSet<i32> = replicas.iter().copied().collect();
            assert_eq!(distinct.len(), replicas.len(), "partition {partition}");
        }
    }

    #[test]
    fn leads_and_follows_are_exclusive() {
        let cluster = three(2);
        for partition in 0..12 {
            assert!(!(cluster.leads("demo", partition) && cluster.follows("demo", partition)));
            if cluster.holds("demo", partition) {
                assert!(cluster.leads("demo", partition) || cluster.follows("demo", partition));
            }
        }
    }

    #[test]
    fn peer_specs_parse_or_explain_themselves() {
        assert_eq!(
            parse_peer("2@localhost:9093").unwrap(),
            Node {
                id: 2,
                host: "localhost".into(),
                port: 9093
            }
        );
        assert!(parse_peer("localhost:9093").is_err());
        assert!(parse_peer("x@localhost:9093").is_err());
        assert!(parse_peer("2@localhost").is_err());
        assert!(parse_peer("2@:9093").is_err());
    }

    #[test]
    fn a_lone_broker_leads_everything() {
        let cluster = Cluster::alone(1, "127.0.0.1", 9092);
        assert!(cluster.is_alone());
        for partition in 0..8 {
            assert!(cluster.leads("demo", partition));
            assert_eq!(cluster.replicas("demo", partition), vec![1]);
        }
    }
}
