//! CRC-32C, the checksum on every record batch.
//!
//! Castagnoli, not the CRC-32 in zip files and PNGs. Different polynomial,
//! different results, and a mismatch here looks like corrupt data from the
//! client rather than a wrong checksum function.

/// The Castagnoli polynomial, bit-reversed for a table that shifts right.
const POLY: u32 = 0x82F6_3B78;

static TABLE: [u32; 256] = build_table();

const fn build_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut byte = 0;

    while byte < 256 {
        let mut crc = byte as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[byte] = crc;
        byte += 1;
    }

    table
}

pub fn checksum(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for b in bytes {
        crc = TABLE[((crc ^ *b as u32) & 0xff) as usize] ^ (crc >> 8);
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_published_check_values() {
        assert_eq!(checksum(b""), 0x0000_0000);
        assert_eq!(checksum(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn is_not_plain_crc32() {
        // 0xCBF43926 is what CRC-32 gives for this input.
        assert_ne!(checksum(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn one_flipped_bit_changes_it() {
        assert_ne!(checksum(b"hello world"), checksum(b"hello worle"));
    }
}
