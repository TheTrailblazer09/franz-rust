//! ApiVersions (key 18): the first request every client sends.
//!
//! A broker must answer even a version it does not support. A client that gets
//! a closed connection has no way to discover what it should have asked for.

use crate::protocol::{ApiKey, Writer, codes};

pub struct Range {
    pub api_key: i16,
    pub min: i16,
    pub max: i16,
}

/// Built from the API table, so we cannot advertise something unimplemented.
pub fn advertised() -> Vec<Range> {
    ApiKey::ALL
        .iter()
        .filter_map(|key| {
            key.supported().map(|(min, max)| Range {
                api_key: *key as i16,
                min,
                max,
            })
        })
        .collect()
}

/// Encodes the body. The header is written by the caller.
pub fn write_response(w: &mut Writer, version: i16, error_code: i16, ranges: &[Range]) {
    let flexible = ApiKey::ApiVersions.is_flexible(version);

    w.i16(error_code);

    if flexible {
        w.compact_array_len(ranges.len());
    } else {
        w.array_len(ranges.len());
    }
    for range in ranges {
        w.i16(range.api_key);
        w.i16(range.min);
        w.i16(range.max);
        if flexible {
            w.empty_tagged_fields();
        }
    }

    if version >= 1 {
        w.i32(0); // throttle_time_ms
    }
    if flexible {
        w.empty_tagged_fields();
    }
}

/// Answered in the v0 shape, the only one every client can read.
pub fn write_unsupported(w: &mut Writer) {
    write_response(w, 0, codes::UNSUPPORTED_VERSION, &advertised());
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fixed list, so these stay byte-exact as real APIs are added.
    fn one_range() -> Vec<Range> {
        vec![Range {
            api_key: 18,
            min: 0,
            max: 3,
        }]
    }

    fn encode(version: i16) -> Vec<u8> {
        let mut w = Writer::new();
        write_response(&mut w, version, codes::NONE, &one_range());
        w.into_inner()
    }

    #[test]
    fn v0_is_an_error_code_and_a_classic_array() {
        assert_eq!(
            encode(0),
            vec![
                0x00, 0x00, // error_code 0
                0x00, 0x00, 0x00, 0x01, // one entry
                0x00, 0x12, 0x00, 0x00, 0x00, 0x03, // ApiVersions, v0 to v3
            ]
        );
    }

    #[test]
    fn v1_adds_throttle_time_and_nothing_else() {
        let v0 = encode(0);
        let v1 = encode(1);
        assert_eq!(v1[..v0.len()], v0[..]);
        assert_eq!(v1[v0.len()..], [0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn v3_uses_compact_lengths_and_tagged_fields() {
        assert_eq!(
            encode(3),
            vec![
                0x00, 0x00, // error_code 0
                0x02, // compact array: one entry, as length + 1
                0x00, 0x12, 0x00, 0x00, 0x00, 0x03, // ApiVersions, v0 to v3
                0x00, // the entry's tagged fields
                0x00, 0x00, 0x00, 0x00, // throttle_time_ms
                0x00, // the response's tagged fields
            ]
        );
    }

    #[test]
    fn an_unsupported_version_still_reports_the_real_ranges() {
        let mut w = Writer::new();
        write_unsupported(&mut w);
        let body = w.into_inner();

        assert_eq!(&body[..2], &codes::UNSUPPORTED_VERSION.to_be_bytes());
        let count = i32::from_be_bytes([body[2], body[3], body[4], body[5]]);
        assert_eq!(count as usize, advertised().len());
    }

    #[test]
    fn we_advertise_every_api_we_implement_and_nothing_else() {
        let keys: Vec<i16> = advertised().iter().map(|r| r.api_key).collect();
        assert_eq!(keys, [0, 1, 2, 3, 18], "ascending api_key order");

        // Produce, Fetch and ListOffsets deliberately stop below the version
        // where each turns flexible.
        assert!(!ApiKey::Produce.supports(9));
        assert!(!ApiKey::Fetch.supports(12));
        assert!(!ApiKey::ListOffsets.supports(6));
        assert!(ApiKey::Produce.supports(8));
    }
}
