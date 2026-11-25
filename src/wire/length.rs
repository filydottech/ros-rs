//! `RouterOS` length words.
//!
//! Five tiers: one byte below 0x80, two below 0x4000, three below 0x200000,
//! four below 0x10000000, and 0xf0 followed by a big-endian `u32`.

use super::WireError;

/// The largest value a length word can carry.
pub(crate) const MAX_WIRE_LENGTH: u64 = u32::MAX as u64;

/// One decoded length word: its value and the bytes it occupied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Prefix {
    pub(crate) length: u32,
    pub(crate) size: usize,
}

/// Append the shortest length prefix for `n`.
///
/// Every branch masks the value to exactly the bits its prefix carries, so the
/// narrowing casts are intended and cannot truncate meaning.
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn encode_length(dst: &mut Vec<u8>, n: u32) {
    if n < 0x80 {
        dst.push(n as u8);
    } else if n < 0x4000 {
        dst.push((n >> 8) as u8 | 0x80);
        dst.push(n as u8);
    } else if n < 0x20_0000 {
        dst.push((n >> 16) as u8 | 0xc0);
        dst.push((n >> 8) as u8);
        dst.push(n as u8);
    } else if n < 0x1000_0000 {
        dst.push((n >> 24) as u8 | 0xe0);
        dst.push((n >> 16) as u8);
        dst.push((n >> 8) as u8);
        dst.push(n as u8);
    } else {
        dst.push(0xf0);
        dst.extend_from_slice(&n.to_be_bytes());
    }
}

/// Read the prefix at the start of `bytes`.
///
/// `Ok(None)` means `bytes` holds only part of a prefix; the caller feeds more.
/// A byte in the reserved `0xf1..=0xff` range is a protocol error.
pub(crate) fn decode_length(bytes: &[u8]) -> Result<Option<Prefix>, WireError> {
    let Some(&first) = bytes.first() else {
        return Ok(None);
    };
    let prefix = if first < 0x80 {
        Prefix {
            length: u32::from(first),
            size: 1,
        }
    } else if first & 0xc0 == 0x80 {
        if bytes.len() < 2 {
            return Ok(None);
        }
        Prefix {
            length: (u32::from(first & 0x3f) << 8) | u32::from(bytes[1]),
            size: 2,
        }
    } else if first & 0xe0 == 0xc0 {
        if bytes.len() < 3 {
            return Ok(None);
        }
        Prefix {
            length: (u32::from(first & 0x1f) << 16)
                | (u32::from(bytes[1]) << 8)
                | u32::from(bytes[2]),
            size: 3,
        }
    } else if first & 0xf0 == 0xe0 {
        if bytes.len() < 4 {
            return Ok(None);
        }
        Prefix {
            length: (u32::from(first & 0x0f) << 24)
                | (u32::from(bytes[1]) << 16)
                | (u32::from(bytes[2]) << 8)
                | u32::from(bytes[3]),
            size: 4,
        }
    } else if first == 0xf0 {
        if bytes.len() < 5 {
            return Ok(None);
        }
        Prefix {
            length: u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]),
            size: 5,
        }
    } else {
        return Err(WireError::protocol(format!(
            "reserved control byte 0x{first:02x}"
        )));
    };
    Ok(Some(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_tier_round_trips() {
        let values = [
            0u32,
            1,
            0x7f,
            0x80,
            0x3fff,
            0x4000,
            0x1f_ffff,
            0x20_0000,
            0x0fff_ffff,
            0x1000_0000,
            u32::MAX,
        ];
        for value in values {
            let mut bytes = Vec::new();
            encode_length(&mut bytes, value);
            let prefix = decode_length(&bytes).unwrap().unwrap();
            assert_eq!(prefix.length, value);
            assert_eq!(prefix.size, bytes.len());
        }
    }

    #[test]
    fn tiers_use_the_smallest_form() {
        let cases: [(u32, &[u8]); 5] = [
            (0x7f, &[0x7f]),
            (0x80, &[0x80, 0x80]),
            (0x4000, &[0xc0, 0x40, 0x00]),
            (0x20_0000, &[0xe0, 0x20, 0x00, 0x00]),
            (0x1000_0000, &[0xf0, 0x10, 0x00, 0x00, 0x00]),
        ];
        for (value, expected) in cases {
            let mut bytes = Vec::new();
            encode_length(&mut bytes, value);
            assert_eq!(bytes, expected, "value {value:#x}");
        }
    }

    #[test]
    fn incomplete_prefixes_report_none() {
        let mut full = Vec::new();
        encode_length(&mut full, 0x1234_5678);
        assert_eq!(full.len(), 5);
        for cut in 0..full.len() {
            assert_eq!(decode_length(&full[..cut]).unwrap(), None, "cut {cut}");
        }
    }

    #[test]
    fn reserved_control_bytes_are_rejected() {
        for byte in 0xf1u8..=0xff {
            let err = decode_length(&[byte]).unwrap_err();
            assert!(matches!(err, WireError::Protocol { .. }), "byte {byte:#x}");
        }
    }

    #[test]
    fn extended_tier_preserves_the_full_u32() {
        let prefix = decode_length(&[0xf0, 0xff, 0xff, 0xff, 0xff])
            .unwrap()
            .unwrap();
        assert_eq!(prefix.length, u32::MAX);
        assert_eq!(prefix.size, 5);
    }
}
