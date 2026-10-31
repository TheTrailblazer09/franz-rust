//! Parsing Kafka requests: bytes in, meaning out.
//!
//! Two halves. [`Reader`] is a cursor over a byte slice that knows how to pull
//! out the primitive types the protocol is built from, and fails rather than
//! panicking when a frame is shorter than it claims. [`RequestHeader`] is the
//! fixed preamble every request begins with, whatever the operation.
//!
//! The rule worth stating once: whether a field uses the compact encoding is
//! decided per API by [`ApiKey::flexible_since`], never by a flag on the
//! reader. `Metadata` became flexible at v9 while `Fetch` waited until v12, so
//! a reader that tracked "flexible" as connection state would be right for one
//! and wrong for the other. When it is wrong the frame still decodes — the
//! fields just land a byte off, and the client hangs with no error to read.

use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// Asked for more bytes than the frame holds.
    Truncated { want: usize, have: usize, at: usize },
    /// A varint ran past its maximum width, so these bytes are not a varint.
    BadVarint { at: usize },
    /// A string field was not valid UTF-8.
    BadUtf8 { at: usize },
    /// An api_key this broker has never heard of.
    UnknownApiKey(i16),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Truncated { want, have, at } => {
                write!(
                    f,
                    "frame ended early: wanted {want} bytes at offset {at}, {have} remain"
                )
            }
            Error::BadVarint { at } => write!(
                f,
                "malformed varint at offset {at}: usually means the previous field was \
                 read with the wrong encoding for this API version"
            ),
            Error::BadUtf8 { at } => write!(f, "string at offset {at} is not valid UTF-8"),
            Error::UnknownApiKey(k) => write!(f, "api_key {k} is not one this broker knows"),
        }
    }
}

impl std::error::Error for Error {}

// --- reading bytes ----------------------------------------------------------

pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(Error::Truncated {
                want: n,
                have: self.remaining(),
                at: self.pos,
            });
        }
        let bytes = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(bytes)
    }

    pub fn i16(&mut self) -> Result<i16> {
        let b = self.take(2)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    pub fn i32(&mut self) -> Result<i32> {
        let b = self.take(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Unsigned varint: seven bits of value per byte, low groups first, with
    /// the high bit set on every byte but the last. Compact lengths and tag
    /// numbers use these, which is how a short string costs one byte of
    /// overhead instead of two.
    pub fn uvarint(&mut self) -> Result<u32> {
        let at = self.pos;
        let mut out: u32 = 0;
        for shift in [0u32, 7, 14, 21, 28] {
            let b = self.take(1)?[0];
            out |= ((b & 0x7f) as u32) << shift;
            if b & 0x80 == 0 {
                return Ok(out);
            }
        }
        Err(Error::BadVarint { at })
    }

    /// An int16 length followed by that many bytes, where -1 means null.
    pub fn nullable_string(&mut self) -> Result<Option<String>> {
        let at = self.pos;
        let len = self.i16()?;
        if len < 0 {
            return Ok(None);
        }
        let bytes = self.take(len as usize)?;
        match std::str::from_utf8(bytes) {
            Ok(s) => Ok(Some(s.to_owned())),
            Err(_) => Err(Error::BadUtf8 { at }),
        }
    }

    /// Tagged fields, skipped wholesale.
    ///
    /// This is Kafka's forward-compatibility escape hatch: a count, then that
    /// many (tag, length, bytes) triples. Nothing here reads a tag yet, but the
    /// bytes still have to be consumed or every field after them is read from
    /// the wrong offset.
    pub fn tagged_fields(&mut self) -> Result<()> {
        let count = self.uvarint()?;
        for _ in 0..count {
            let _tag = self.uvarint()?;
            let size = self.uvarint()? as usize;
            self.take(size)?;
        }
        Ok(())
    }
}

// --- what the client asked for ----------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    ApiVersions = 18,
}

impl ApiKey {
    pub fn from_i16(key: i16) -> Result<Self> {
        match key {
            0 => Ok(ApiKey::Produce),
            1 => Ok(ApiKey::Fetch),
            2 => Ok(ApiKey::ListOffsets),
            3 => Ok(ApiKey::Metadata),
            18 => Ok(ApiKey::ApiVersions),
            other => Err(Error::UnknownApiKey(other)),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            ApiKey::Produce => "Produce",
            ApiKey::Fetch => "Fetch",
            ApiKey::ListOffsets => "ListOffsets",
            ApiKey::Metadata => "Metadata",
            ApiKey::ApiVersions => "ApiVersions",
        }
    }

    /// First version of this API encoded with compact types and tagged fields.
    fn flexible_since(self) -> i16 {
        match self {
            ApiKey::Produce => 9,
            ApiKey::Fetch => 12,
            ApiKey::ListOffsets => 6,
            ApiKey::Metadata => 9,
            ApiKey::ApiVersions => 3,
        }
    }

    pub fn is_flexible(self, version: i16) -> bool {
        version >= self.flexible_since()
    }
}

/// The preamble on every request, whatever the operation.
///
/// `client_id` is the trap. The header gained its tagged-field buffer when it
/// went flexible, but `client_id` stayed an int16-prefixed string rather than
/// becoming compact — so a single flexible request carries both encodings, and
/// assuming otherwise puts every later field one byte out.
#[derive(Debug)]
pub struct RequestHeader {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

impl RequestHeader {
    pub fn decode(r: &mut Reader) -> Result<Self> {
        let api_key = ApiKey::from_i16(r.i16()?)?;
        let api_version = r.i16()?;
        let correlation_id = r.i32()?;
        let client_id = r.nullable_string()?;

        if api_key.is_flexible(api_version) {
            r.tagged_fields()?;
        }

        Ok(RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Captured from `kcat -L` 1.7.0 against this broker. Not written by hand:
    /// a client that has never seen this code is the only way to find out
    /// whether the parsing matches the real protocol or only matches itself.
    const KCAT_API_VERSIONS: &[u8] = &[
        0x00, 0x12, // api_key 18, ApiVersions
        0x00, 0x03, // api_version 3
        0x00, 0x00, 0x00, 0x01, // correlation_id 1
        0x00, 0x07, b'r', b'd', b'k', b'a', b'f', b'k', b'a', // client_id "rdkafka"
        0x00, // header tagged fields: none
        0x0b, b'l', b'i', b'b', b'r', b'd', b'k', b'a', b'f', b'k',
        b'a', // compact "librdkafka"
        0x07, b'2', b'.', b'1', b'6', b'.', b'0', // compact "2.16.0"
        0x00, // body tagged fields: none
    ];

    #[test]
    fn parses_a_real_librdkafka_handshake() {
        let mut r = Reader::new(KCAT_API_VERSIONS);
        let h = RequestHeader::decode(&mut r).unwrap();

        assert_eq!(h.api_key, ApiKey::ApiVersions);
        assert_eq!(h.api_version, 3);
        assert_eq!(h.correlation_id, 1);
        assert_eq!(h.client_id.as_deref(), Some("rdkafka"));
        assert_eq!(r.remaining(), 19, "the body is left for the API to decode");
    }

    #[test]
    fn a_non_flexible_request_has_no_tagged_fields_to_skip() {
        // Same shape as the frame built by hand in step 1.
        let frame = &[
            0x00, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x04, b't', b'e', b's', b't',
        ];
        let mut r = Reader::new(frame);
        let h = RequestHeader::decode(&mut r).unwrap();

        assert_eq!(h.api_version, 0);
        assert_eq!(h.client_id.as_deref(), Some("test"));
        assert_eq!(r.remaining(), 0, "ApiVersions v0 has an empty body");
    }

    #[test]
    fn flexible_thresholds_are_per_api_not_per_connection() {
        assert!(ApiKey::Metadata.is_flexible(9));
        assert!(
            !ApiKey::Fetch.is_flexible(9),
            "Fetch is flexible only from v12"
        );
        assert!(ApiKey::Fetch.is_flexible(12));
    }

    #[test]
    fn a_truncated_frame_says_where_it_stopped() {
        let mut r = Reader::new(&[0x00, 0x12, 0x00]);
        let err = RequestHeader::decode(&mut r).unwrap_err();
        assert_eq!(
            err,
            Error::Truncated {
                want: 2,
                have: 1,
                at: 2
            }
        );
    }

    #[test]
    fn an_unknown_api_key_is_named_as_such() {
        // api_key 99, and deliberately nothing after the correlation_id: the
        // unknown key is the real problem and should be what gets reported.
        let frame = &[0x00, 0x63, 0x00, 0x01, 0x00, 0x00, 0x00, 0x05];
        let err = RequestHeader::decode(&mut Reader::new(frame)).unwrap_err();
        assert_eq!(err, Error::UnknownApiKey(99));
    }

    #[test]
    fn a_null_client_id_is_allowed() {
        let frame = &[0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x09, 0xff, 0xff];
        let h = RequestHeader::decode(&mut Reader::new(frame)).unwrap();
        assert_eq!(h.api_key, ApiKey::Metadata);
        assert_eq!(h.client_id, None);
    }
}
