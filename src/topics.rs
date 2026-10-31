//! Which topics exist and how many partitions each has.
//!
//! Topics come from the command line and vanish on exit. CreateTopics and
//! on-disk state come later; Metadata needs something to describe first.

pub struct Topic {
    pub name: String,
    pub id: [u8; 16],
    pub partitions: i32,
}

pub struct Topics {
    topics: Vec<Topic>,
}

impl Topics {
    /// Each spec is `name:partitions`.
    pub fn from_specs(specs: &[String]) -> Result<Self, String> {
        let mut topics: Vec<Topic> = Vec::new();

        for spec in specs {
            let (name, count) = spec
                .split_once(':')
                .ok_or_else(|| format!("expected NAME:PARTITIONS, got `{spec}`"))?;

            if name.is_empty() {
                return Err(format!("`{spec}` has no topic name"));
            }
            let partitions: i32 = count
                .parse()
                .map_err(|_| format!("`{count}` is not a partition count"))?;
            if partitions < 1 {
                return Err(format!(
                    "{name} needs at least one partition, got {partitions}"
                ));
            }
            if topics.iter().any(|t| t.name == name) {
                return Err(format!("{name} was given twice"));
            }

            topics.push(Topic {
                name: name.to_owned(),
                id: topic_id(name),
                partitions,
            });
        }

        // Sorted so `kcat -L` output does not shuffle between runs.
        topics.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(Topics { topics })
    }

    pub fn get(&self, name: &str) -> Option<&Topic> {
        self.topics.iter().find(|t| t.name == name)
    }

    pub fn all(&self) -> &[Topic] {
        &self.topics
    }
}

/// Sixteen bytes derived from the name, so the id survives a restart.
///
/// Kafka generates a random id per topic and persists it. Nothing is persisted
/// here, and a fresh random id each boot looks to a client like the topic was
/// deleted and recreated. Not a real UUID, but the protocol treats it as
/// opaque bytes.
fn topic_id(name: &str) -> [u8; 16] {
    let mut id = [0u8; 16];
    for (i, half) in id.chunks_mut(8).enumerate() {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ i as u64;
        for b in name.as_bytes() {
            hash ^= *b as u64;
            hash = hash.wrapping_mul(0x100_0000_01b3);
        }
        half.copy_from_slice(&hash.to_be_bytes());
    }
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    fn specs(args: &[&str]) -> Result<Topics, String> {
        Topics::from_specs(&args.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    #[test]
    fn parses_and_sorts() {
        let t = specs(&["logs:1", "demo:3"]).unwrap();
        let names: Vec<&str> = t.all().iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["demo", "logs"]);
        assert_eq!(t.get("demo").unwrap().partitions, 3);
        assert!(t.get("missing").is_none());
    }

    #[test]
    fn rejects_bad_specs() {
        assert!(specs(&["demo"]).is_err());
        assert!(specs(&["demo:0"]).is_err());
        assert!(specs(&["demo:x"]).is_err());
        assert!(specs(&[":3"]).is_err());
        assert!(specs(&["demo:1", "demo:2"]).is_err());
    }

    #[test]
    fn ids_are_stable_and_distinct() {
        let a = specs(&["demo:1"]).unwrap();
        let b = specs(&["demo:9"]).unwrap();
        assert_eq!(a.get("demo").unwrap().id, b.get("demo").unwrap().id);
        assert_ne!(
            a.get("demo").unwrap().id,
            specs(&["other:1"]).unwrap().get("other").unwrap().id
        );
        assert_ne!(a.get("demo").unwrap().id, [0u8; 16]);
    }
}
