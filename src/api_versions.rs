//! ApiVersions (key 18): the first request every client sends.
//!
//! The rule that shapes this API: a broker must answer *even a version it does
//! not support*. A client that opens with v9 and gets a closed connection has
//! no way to discover that v3 would have worked, so the reply to an
//! out-of-range version is a v0-shaped body carrying `UNSUPPORTED_VERSION`
//! together with the real range. librdkafka then retries at the version it was
//! told about.
//!
//! The body is a list of (api_key, min_version, max_version), and it is built
//! from [`ApiKey::supported`] rather than written out separately. Advertising
//! something the broker does not implement is a lie a client will act on, and
//! the failure arrives later, somewhere else.

use crate::protocol::{ApiKey, Writer, codes};

/// Encodes the response body. The header is written by the caller.
pub fn write_response(w: &mut Writer, version: i16, error_code: i16) {
    let flexible = ApiKey::ApiVersions.is_flexible(version);

    let supported: Vec<(ApiKey, i16, i16)> = ApiKey::ALL
        .iter()
        .filter_map(|key| key.supported().map(|(lo, hi)| (*key, lo, hi)))
        .collect();

    w.i16(error_code);

    if flexible {
        w.compact_array_len(supported.len());
    } else {
        w.array_len(supported.len());
    }
    for (key, lo, hi) in &supported {
        w.i16(*key as i16);
        w.i16(*lo);
        w.i16(*hi);
        if flexible {
            w.empty_tagged_fields();
        }
    }

    if version >= 1 {
        w.i32(0); // throttle_time_ms: no quotas, so never delay a client
    }
    if flexible {
        w.empty_tagged_fields();
    }
}

/// Answers a version outside our range. Deliberately encoded as v0: it is the
/// only shape every client can read.
pub fn write_unsupported(w: &mut Writer) {
    write_response(w, 0, codes::UNSUPPORTED_VERSION);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(version: i16) -> Vec<u8> {
        let mut w = Writer::new();
        write_response(&mut w, version, codes::NONE);
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
                0x02, // compact array: one entry, encoded as length + 1
                0x00, 0x12, 0x00, 0x00, 0x00, 0x03, // ApiVersions, v0 to v3
                0x00, // the entry's tagged fields
                0x00, 0x00, 0x00, 0x00, // throttle_time_ms
                0x00, // the response's tagged fields
            ]
        );
    }

    #[test]
    fn an_unsupported_version_still_reports_the_whole_range() {
        let mut w = Writer::new();
        write_unsupported(&mut w);
        let body = w.into_inner();

        assert_eq!(&body[..2], &codes::UNSUPPORTED_VERSION.to_be_bytes());
        // Same list as a successful v0 reply, so the client can choose again.
        assert_eq!(&body[2..], &encode(0)[2..]);
    }

    #[test]
    fn we_only_advertise_what_we_implement() {
        assert!(ApiKey::ApiVersions.supports(3));
        assert!(!ApiKey::ApiVersions.supports(4));
        assert!(!ApiKey::Metadata.supports(0), "not built yet");
    }
}
