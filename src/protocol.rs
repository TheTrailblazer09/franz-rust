//! Parsing Kafka requests: bytes in, meaning out.
//!
//! Two halves. [`Reader`] is a cursor over a byte slice that knows how to pull
//! out the primitive types the protocol is built from, and fails rather than
//! panicking when a frame is shorter than it claims. [`RequestHeader`] is the
//! fixed preamble every request begins with, whatever the operation.

use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    Truncated {
        want: usize,
        have: usize,
        at: usize,
    },
    /// A varint ran past its maximum width, so these bytes are not a varint.
    BadVarint {
        at: usize,
    },
    BadUtf8 {
        at: usize,
    },
    UnexpectedNull {
        field: &'static str,
        at: usize,
    },
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
            Error::UnexpectedNull { field, at } => {
                write!(
                    f,
                    "{field} at offset {at} is null, which this version forbids"
                )
            }
            Error::UnknownApiKey(k) => write!(f, "api_key {k} is not one this broker knows"),
        }
    }
}

impl std::error::Error for Error {}

/// Kafka error codes, as they appear on the wire.
///
/// These belong to the protocol, not to this implementation: clients branch on
/// them, and several are instructions rather than complaints. Returning a
/// wrong-but-plausible code turns a recoverable situation into a stuck client.
pub mod codes {
    pub const NONE: i16 = 0;
    pub const OFFSET_OUT_OF_RANGE: i16 = 1;
    pub const CORRUPT_MESSAGE: i16 = 2;
    pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
    pub const ILLEGAL_GENERATION: i16 = 22;
    pub const INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
    pub const INVALID_GROUP_ID: i16 = 24;
    pub const UNKNOWN_MEMBER_ID: i16 = 25;
    pub const REBALANCE_IN_PROGRESS: i16 = 27;
    pub const UNSUPPORTED_VERSION: i16 = 35;
    pub const UNKNOWN_SERVER_ERROR: i16 = -1;
}

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

    /// Unsigned varint
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

    /// Exactly `n` bytes, for fields whose length came from somewhere else.
    pub fn take_bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        self.take(n)
    }

    pub fn bool(&mut self) -> Result<bool> {
        Ok(self.take(1)?[0] != 0)
    }

    pub fn i8(&mut self) -> Result<i8> {
        Ok(self.take(1)?[0] as i8)
    }

    pub fn i64(&mut self) -> Result<i64> {
        let b = self.take(8)?;
        Ok(i64::from_be_bytes(b.try_into().expect("took eight bytes")))
    }

    pub fn varint(&mut self) -> Result<i32> {
        let n = self.uvarint()?;
        Ok(((n >> 1) as i32) ^ -((n & 1) as i32))
    }

    pub fn varlong(&mut self) -> Result<i64> {
        let at = self.pos;
        let mut out: u64 = 0;
        for shift in (0..64).step_by(7) {
            let b = self.take(1)?[0];
            out |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(((out >> 1) as i64) ^ -((out & 1) as i64));
            }
        }
        Err(Error::BadVarint { at })
    }

    /// An int32 length then that many bytes, where -1 means null. Record
    /// batches arrive in one of these.
    pub fn nullable_bytes(&mut self) -> Result<Option<&'a [u8]>> {
        let at = self.pos;
        let len = self.i32()?;
        if len < 0 {
            return Ok(None);
        }
        let len = len as usize;
        if len > self.remaining() {
            return Err(Error::Truncated {
                want: len,
                have: self.remaining(),
                at,
            });
        }
        Ok(Some(self.take(len)?))
    }

    pub fn uuid(&mut self) -> Result<[u8; 16]> {
        let mut id = [0u8; 16];
        id.copy_from_slice(self.take(16)?);
        Ok(id)
    }

    pub fn string(&mut self) -> Result<String> {
        let at = self.pos;
        self.nullable_string()?.ok_or(Error::UnexpectedNull {
            field: "string",
            at,
        })
    }

    /// Unsigned varint of length + 1, so zero means null.
    pub fn compact_nullable_string(&mut self) -> Result<Option<String>> {
        let at = self.pos;
        let len = self.uvarint()?;
        if len == 0 {
            return Ok(None);
        }
        let bytes = self.take(len as usize - 1)?;
        match std::str::from_utf8(bytes) {
            Ok(s) => Ok(Some(s.to_owned())),
            Err(_) => Err(Error::BadUtf8 { at }),
        }
    }

    /// `None` is a null array, which is not an empty one: null topics in a
    /// Metadata request means every topic.
    ///
    /// Callers loop and push rather than reserving the count, which the client
    /// controls.
    pub fn array_len(&mut self) -> Result<Option<usize>> {
        let n = self.i32()?;
        if n < 0 {
            return Ok(None);
        }
        Ok(Some(n as usize))
    }

    pub fn compact_array_len(&mut self) -> Result<Option<usize>> {
        let n = self.uvarint()?;
        if n == 0 {
            return Ok(None);
        }
        Ok(Some(n as usize - 1))
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

// --- writing bytes ----------------------------------------------------------

/// The mirror of [`Reader`]. Appends to a buffer and cannot fail, which is why
/// none of these return a `Result` and encoding a response needs no error
/// handling at all.
#[derive(Default)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Self {
        Writer::default()
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.buf
    }

    pub fn i16(&mut self, n: i16) {
        self.buf.extend_from_slice(&n.to_be_bytes());
    }

    pub fn i32(&mut self, n: i32) {
        self.buf.extend_from_slice(&n.to_be_bytes());
    }

    pub fn uvarint(&mut self, mut n: u32) {
        while n >= 0x80 {
            self.buf.push((n as u8) | 0x80);
            n >>= 7;
        }
        self.buf.push(n as u8);
    }

    pub fn bool(&mut self, b: bool) {
        self.buf.push(b as u8);
    }

    pub fn i8(&mut self, n: i8) {
        self.buf.push(n as u8);
    }

    pub fn i64(&mut self, n: i64) {
        self.buf.extend_from_slice(&n.to_be_bytes());
    }

    /// Zigzag varint: record fields inside a batch use these, so -1 costs one
    /// byte rather than five.
    pub fn varint(&mut self, n: i32) {
        self.uvarint(((n << 1) ^ (n >> 31)) as u32);
    }

    pub fn varlong(&mut self, n: i64) {
        let zigzag = ((n << 1) ^ (n >> 63)) as u64;
        let mut n = zigzag;
        while n >= 0x80 {
            self.buf.push((n as u8) | 0x80);
            n >>= 7;
        }
        self.buf.push(n as u8);
    }

    pub fn raw(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    pub fn nullable_bytes(&mut self, bytes: Option<&[u8]>) {
        match bytes {
            Some(b) => {
                self.i32(b.len() as i32);
                self.raw(b);
            }
            None => self.i32(-1),
        }
    }

    pub fn uuid(&mut self, id: &[u8; 16]) {
        self.buf.extend_from_slice(id);
    }

    pub fn string(&mut self, s: &str) {
        self.i16(s.len() as i16);
        self.buf.extend_from_slice(s.as_bytes());
    }

    pub fn nullable_string(&mut self, s: Option<&str>) {
        match s {
            Some(s) => self.string(s),
            None => self.i16(-1),
        }
    }

    pub fn compact_string(&mut self, s: &str) {
        self.uvarint(s.len() as u32 + 1);
        self.buf.extend_from_slice(s.as_bytes());
    }

    pub fn compact_nullable_string(&mut self, s: Option<&str>) {
        match s {
            Some(s) => self.compact_string(s),
            None => self.uvarint(0),
        }
    }

    /// Array length in the classic encoding: an int32, where -1 means null.
    pub fn array_len(&mut self, n: usize) {
        self.i32(n as i32);
    }

    /// Array length in the compact encoding: an unsigned varint of length plus
    /// one, which leaves zero free to mean null.
    pub fn compact_array_len(&mut self, n: usize) {
        self.uvarint(n as u32 + 1);
    }

    pub fn empty_tagged_fields(&mut self) {
        self.uvarint(0);
    }
}

/// Writes the response preamble: the correlation_id the client matches this
/// reply against, and a tagged-field buffer if this API is flexible here.
///
/// `ApiVersions` is the exception, and it has to be special-cased or no client
/// gets past the handshake: its response always uses header v0, with no tagged
/// fields, even at v3 where the body *is* flexible. The client must parse this
/// response before it knows anything about the broker, so the one reply whose
/// shape it cannot negotiate is pinned forever.
pub fn write_response_header(w: &mut Writer, api_key: ApiKey, version: i16, correlation_id: i32) {
    w.i32(correlation_id);
    if api_key != ApiKey::ApiVersions && api_key.is_flexible(version) {
        w.empty_tagged_fields();
    }
}

// --- what the client asked for ----------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    ApiVersions = 18,
}

impl ApiKey {
    /// Rust has no equivalent of Java's `values()`, so the list is written out.
    /// `all_lists_every_api_key_we_recognise` keeps it honest.
    pub const ALL: &'static [ApiKey] = &[
        ApiKey::Produce,
        ApiKey::Fetch,
        ApiKey::ListOffsets,
        ApiKey::Metadata,
        ApiKey::OffsetCommit,
        ApiKey::OffsetFetch,
        ApiKey::FindCoordinator,
        ApiKey::JoinGroup,
        ApiKey::Heartbeat,
        ApiKey::LeaveGroup,
        ApiKey::SyncGroup,
        ApiKey::ApiVersions,
    ];

    pub fn from_i16(key: i16) -> Result<Self> {
        match key {
            0 => Ok(ApiKey::Produce),
            1 => Ok(ApiKey::Fetch),
            2 => Ok(ApiKey::ListOffsets),
            3 => Ok(ApiKey::Metadata),
            8 => Ok(ApiKey::OffsetCommit),
            9 => Ok(ApiKey::OffsetFetch),
            10 => Ok(ApiKey::FindCoordinator),
            11 => Ok(ApiKey::JoinGroup),
            12 => Ok(ApiKey::Heartbeat),
            13 => Ok(ApiKey::LeaveGroup),
            14 => Ok(ApiKey::SyncGroup),
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
            ApiKey::OffsetCommit => "OffsetCommit",
            ApiKey::OffsetFetch => "OffsetFetch",
            ApiKey::FindCoordinator => "FindCoordinator",
            ApiKey::JoinGroup => "JoinGroup",
            ApiKey::Heartbeat => "Heartbeat",
            ApiKey::LeaveGroup => "LeaveGroup",
            ApiKey::SyncGroup => "SyncGroup",
            ApiKey::ApiVersions => "ApiVersions",
        }
    }

    /// Inclusive version range this broker implements, or `None` if it does
    /// not implement the API at all. This is exactly what `ApiVersions`
    /// advertises, so a client that read that response will never ask for
    /// anything outside it.
    ///
    /// Spelled out one variant at a time rather than with a catch-all, so
    /// adding an API to the enum stops compiling until its range is declared.
    pub fn supported(self) -> Option<(i16, i16)> {
        match self {
            ApiKey::Produce => Some((3, 8)),
            ApiKey::Fetch => Some((1, 11)),
            ApiKey::ListOffsets => Some((1, 5)),
            ApiKey::Metadata => Some((0, 12)),
            ApiKey::OffsetCommit => Some((0, 7)),
            ApiKey::OffsetFetch => Some((0, 5)),
            ApiKey::FindCoordinator => Some((0, 2)),
            ApiKey::JoinGroup => Some((0, 5)),
            ApiKey::Heartbeat => Some((0, 3)),
            ApiKey::LeaveGroup => Some((0, 3)),
            ApiKey::SyncGroup => Some((0, 3)),
            ApiKey::ApiVersions => Some((0, 3)),
        }
    }

    pub fn supports(self, version: i16) -> bool {
        matches!(self.supported(), Some((lo, hi)) if version >= lo && version <= hi)
    }

    /// First version of this API encoded with compact types and tagged fields.
    fn flexible_since(self) -> i16 {
        match self {
            ApiKey::Produce => 9,
            ApiKey::Fetch => 12,
            ApiKey::ListOffsets => 6,
            ApiKey::Metadata => 9,
            ApiKey::OffsetCommit => 8,
            ApiKey::OffsetFetch => 6,
            ApiKey::FindCoordinator => 3,
            ApiKey::JoinGroup => 6,
            ApiKey::Heartbeat => 4,
            ApiKey::LeaveGroup => 4,
            ApiKey::SyncGroup => 4,
            ApiKey::ApiVersions => 3,
        }
    }

    pub fn is_flexible(self, version: i16) -> bool {
        version >= self.flexible_since()
    }
}

/// The preamble on every request, whatever the operation.

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
    fn all_lists_every_api_key_we_recognise() {
        let recognised: Vec<ApiKey> = (0..=100i16)
            .filter_map(|k| ApiKey::from_i16(k).ok())
            .collect();
        assert_eq!(
            recognised,
            ApiKey::ALL,
            "ApiKey::ALL has drifted from from_i16"
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
