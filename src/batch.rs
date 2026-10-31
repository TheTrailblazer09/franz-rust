//! Record batch v2: the block of bytes a client sends and expects back.
//!
//! The broker never decodes individual records. It checks the batch is intact,
//! reads how many offsets it consumes, writes the assigned base offset into the
//! header, and appends the bytes unchanged. That is both less code and faster,
//! and it means client-side compression needs no support here at all.
//!
//! The header is fixed-width:
//!
//! ```text
//!  0  base_offset            int64
//!  8  batch_length           int32   bytes after this field
//! 12  partition_leader_epoch int32
//! 16  magic                  int8    always 2
//! 17  crc                    uint32  CRC-32C of byte 21 onward
//! 21  attributes             int16
//! 23  last_offset_delta      int32
//! 27  base_timestamp         int64
//! 35  max_timestamp          int64
//! 43  producer_id            int64
//! 51  producer_epoch         int16
//! 53  base_sequence          int32
//! 57  record_count           int32
//! 61  records...
//! ```
//!
//! Two details that matter: the crc covers from byte 21, not from the start,
//! which is why stamping a new base offset needs no recompute. And
//! `last_offset_delta` rather than `record_count` decides how many offsets the
//! batch consumes.

use std::fmt;

use crate::crc32c;

/// Bytes before the first record.
pub const HEADER_LEN: usize = 61;

/// `base_offset` and `batch_length` sit outside the length they describe.
const LENGTH_PREFIX: usize = 12;

/// Smallest legal `batch_length`: everything in the header after it.
const MIN_BATCH_LENGTH: i32 = (HEADER_LEN - LENGTH_PREFIX) as i32;

const CRC_FROM: usize = 21;

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// Fewer bytes present than the batch claims.
    Truncated {
        want: usize,
        have: usize,
    },
    /// Not record batch v2.
    BadMagic(i8),
    BadLength(i32),
    BadCrc {
        stored: u32,
        computed: u32,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Truncated { want, have } => {
                write!(f, "batch claims {want} bytes, {have} present")
            }
            Error::BadMagic(m) => write!(f, "magic byte {m}, expected 2"),
            Error::BadLength(n) => write!(f, "batch_length {n} is too small to be a header"),
            Error::BadCrc { stored, computed } => {
                write!(f, "crc {stored:#010x} does not match {computed:#010x}")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Batch {
    pub base_offset: i64,
    /// Including the length prefix, so this is how far to the next batch.
    pub total_len: usize,
    pub last_offset_delta: i32,
    pub record_count: i32,
}

impl Batch {
    /// How many offsets this batch consumes.
    pub fn offsets(&self) -> i64 {
        self.last_offset_delta as i64 + 1
    }
}

/// Reads the header fields without checking the checksum.
///
/// The read path walks batch to batch with this. Verifying the crc there would
/// mean pulling in every byte of a batch just to decide where the next one
/// starts, and the consumer checks it anyway.
pub fn parse_header(bytes: &[u8]) -> Result<Batch, Error> {
    if bytes.len() < HEADER_LEN {
        return Err(Error::Truncated {
            want: HEADER_LEN,
            have: bytes.len(),
        });
    }

    let batch_length = i32(bytes, 8);
    if batch_length < MIN_BATCH_LENGTH {
        return Err(Error::BadLength(batch_length));
    }

    let magic = bytes[16] as i8;
    if magic != 2 {
        return Err(Error::BadMagic(magic));
    }

    Ok(Batch {
        base_offset: i64(bytes, 0),
        total_len: LENGTH_PREFIX + batch_length as usize,
        last_offset_delta: i32(bytes, 23),
        record_count: i32(bytes, 57),
    })
}

/// Validates the batch at the start of `bytes`, checksum included. Needs the
/// whole batch present, so a short buffer is reported as truncated.
pub fn parse(bytes: &[u8]) -> Result<Batch, Error> {
    if bytes.len() < LENGTH_PREFIX {
        return Err(Error::Truncated {
            want: LENGTH_PREFIX,
            have: bytes.len(),
        });
    }

    let batch_length = i32(bytes, 8);
    if batch_length < MIN_BATCH_LENGTH {
        return Err(Error::BadLength(batch_length));
    }
    let total_len = LENGTH_PREFIX + batch_length as usize;
    if bytes.len() < total_len {
        return Err(Error::Truncated {
            want: total_len,
            have: bytes.len(),
        });
    }

    let header = parse_header(bytes)?;

    let stored = u32::from_be_bytes([bytes[17], bytes[18], bytes[19], bytes[20]]);
    let computed = crc32c::checksum(&bytes[CRC_FROM..total_len]);
    if stored != computed {
        return Err(Error::BadCrc { stored, computed });
    }

    Ok(header)
}

/// Walks every batch in a blob, validating each.
pub fn parse_all(bytes: &[u8]) -> Result<Vec<Batch>, Error> {
    let mut batches = Vec::new();
    let mut at = 0;

    while at < bytes.len() {
        let batch = parse(&bytes[at..])?;
        at += batch.total_len;
        batches.push(batch);
    }

    Ok(batches)
}

/// Writes the offset the broker assigned. Outside the checksum's range, so
/// nothing has to be recomputed.
pub fn stamp_base_offset(bytes: &mut [u8], offset: i64) {
    bytes[..8].copy_from_slice(&offset.to_be_bytes());
}

fn i32(bytes: &[u8], at: usize) -> i32 {
    i32::from_be_bytes(bytes[at..at + 4].try_into().expect("four bytes"))
}

fn i64(bytes: &[u8], at: usize) -> i64 {
    i64::from_be_bytes(bytes[at..at + 8].try_into().expect("eight bytes"))
}

#[cfg(test)]
pub(crate) mod fixtures {
    use super::*;

    /// Builds a valid batch with `count` zero-length records.
    pub fn batch(base_offset: i64, count: i32) -> Vec<u8> {
        let mut records = Vec::new();
        for i in 0..count {
            // length, attributes, timestamp_delta, offset_delta, key, value, headers
            let body = [0u8, 0, (i as u8) << 1, 0xff, 0xff, 0];
            records.push(((body.len() as u8) << 1) as u8); // zigzag length
            records.extend_from_slice(&body);
        }

        let mut out = Vec::new();
        out.extend_from_slice(&base_offset.to_be_bytes());
        out.extend_from_slice(&0i32.to_be_bytes()); // batch_length, filled in below
        out.extend_from_slice(&0i32.to_be_bytes()); // partition_leader_epoch
        out.push(2); // magic
        out.extend_from_slice(&0u32.to_be_bytes()); // crc, filled in below
        out.extend_from_slice(&0i16.to_be_bytes()); // attributes
        out.extend_from_slice(&(count - 1).to_be_bytes()); // last_offset_delta
        out.extend_from_slice(&0i64.to_be_bytes()); // base_timestamp
        out.extend_from_slice(&0i64.to_be_bytes()); // max_timestamp
        out.extend_from_slice(&(-1i64).to_be_bytes()); // producer_id
        out.extend_from_slice(&(-1i16).to_be_bytes()); // producer_epoch
        out.extend_from_slice(&(-1i32).to_be_bytes()); // base_sequence
        out.extend_from_slice(&count.to_be_bytes());
        out.extend_from_slice(&records);

        let batch_length = (out.len() - LENGTH_PREFIX) as i32;
        out[8..12].copy_from_slice(&batch_length.to_be_bytes());
        let crc = crc32c::checksum(&out[CRC_FROM..]);
        out[17..21].copy_from_slice(&crc.to_be_bytes());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fixtures::batch;

    #[test]
    fn parses_what_it_builds() {
        let bytes = batch(0, 3);
        let parsed = parse(&bytes).unwrap();

        assert_eq!(parsed.base_offset, 0);
        assert_eq!(parsed.total_len, bytes.len());
        assert_eq!(parsed.record_count, 3);
        assert_eq!(parsed.offsets(), 3);
    }

    #[test]
    fn stamping_an_offset_keeps_the_checksum_valid() {
        let mut bytes = batch(0, 2);
        stamp_base_offset(&mut bytes, 4096);

        let parsed = parse(&bytes).unwrap();
        assert_eq!(parsed.base_offset, 4096);
    }

    #[test]
    fn one_corrupt_byte_in_the_records_is_caught() {
        let mut bytes = batch(0, 2);
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;

        assert!(matches!(parse(&bytes), Err(Error::BadCrc { .. })));
    }

    #[test]
    fn a_half_written_batch_is_truncated_not_corrupt() {
        let bytes = batch(0, 2);
        let err = parse(&bytes[..bytes.len() - 5]).unwrap_err();
        assert!(matches!(err, Error::Truncated { .. }));
    }

    #[test]
    fn rejects_older_message_formats() {
        let mut bytes = batch(0, 1);
        bytes[16] = 1;
        assert_eq!(parse(&bytes), Err(Error::BadMagic(1)));
    }

    #[test]
    fn walks_several_batches() {
        let mut blob = batch(0, 2);
        blob.extend_from_slice(&batch(0, 3));
        blob.extend_from_slice(&batch(0, 1));

        let batches = parse_all(&blob).unwrap();
        assert_eq!(batches.len(), 3);
        assert_eq!(batches.iter().map(|b| b.offsets()).sum::<i64>(), 6);
    }
}
