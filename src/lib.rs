//! franz: the Apache Kafka wire protocol, a commit log, and a broker built on
//! both.
//!
//! Split into a library so the broker and the fault-injection simulator can
//! drive the same storage code.

pub mod api_versions;
pub mod batch;
pub mod broker;
pub mod crc32c;
pub mod fetch;
pub mod files;
pub mod group;
pub mod group_api;
pub mod list_offsets;
pub mod log;
pub mod metadata;
pub mod produce;
pub mod protocol;
pub mod sim;
pub mod topics;
