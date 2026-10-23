//! 144-bit bitvector utilities for daily 10-minute slot tracking.
//!
//! 144 slots = 24 hours × 6 slots/hour (one per 10 minutes).
//! Stored as `[u64; 3]`: words 0–1 hold 64 bits each, word 2 holds bits 128–143.
//! Serialized as 36 hex chars (little-endian byte order within each word).

use std::fmt::Write as FmtWrite;

/// Encode a 144-bit bitvector as 36 hex chars (little-endian byte order within each u64).
/// Words 0-1: 8 bytes each, word 2: 2 bytes (only bits 128-143 used).
pub fn bitvec_to_hex(bits: &[u64; 3]) -> String {
    let mut hex = String::with_capacity(36);
    for i in 0..3 {
        let byte_count = if i < 2 { 8 } else { 2 };
        for b in 0..byte_count {
            let byte = ((bits[i] >> (b * 8)) & 0xFF) as u8;
            write!(hex, "{:02x}", byte).unwrap();
        }
    }
    hex
}

/// Count set bits in a 144-bit bitvector (only the low 144 bits).
pub fn popcount_144(bits: &[u64; 3]) -> u32 {
    bits[0].count_ones() + bits[1].count_ones() + (bits[2] & 0xFFFF).count_ones()
}

/// Decode 36 hex chars back to a 144-bit bitvector.
pub fn hex_to_bitvec(hex: &str) -> Option<[u64; 3]> {
    if hex.len() != 36 {
        return None;
    }
    let bytes: Vec<u8> = (0..18)
        .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16))
        .collect::<Result<_, _>>()
        .ok()?;
    let mut bits = [0u64; 3];
    for j in 0..8 {
        bits[0] |= (bytes[j] as u64) << (j * 8);
    }
    for j in 0..8 {
        bits[1] |= (bytes[8 + j] as u64) << (j * 8);
    }
    for j in 0..2 {
        bits[2] |= (bytes[16 + j] as u64) << (j * 8);
    }
    Some(bits)
}

/// Compute the 10-minute slot index (0–143) for a given UTC timestamp.
pub fn slot_from_timestamp(timestamp: u32) -> usize {
    let secs_in_day = (timestamp % 86400) as usize;
    let hour = secs_in_day / 3600;
    let minute = (secs_in_day % 3600) / 60;
    hour * 6 + minute / 10
}

/// Compute uptime as a percentage of active slots relative to elapsed slots.
/// Returns `None` if the activity data is missing or stale.
pub fn compute_uptime(
    beacon_activity: &Option<String>,
    beacon_activity_date: &Option<String>,
    today: &str,
    current_slot: u32,
) -> Option<f32> {
    let hex = beacon_activity.as_deref()?;
    let date = beacon_activity_date.as_deref()?;
    if date != today || current_slot == 0 {
        return None;
    }
    let bits = hex_to_bitvec(hex)?;
    let set = popcount_144(&bits);
    let elapsed = current_slot.min(144);
    Some(((set as f32 / elapsed as f32) * 1000.0).round() / 10.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitvec_roundtrip_zeros() {
        let bits = [0u64; 3];
        let hex = bitvec_to_hex(&bits);
        assert_eq!(hex, "000000000000000000000000000000000000");
        assert_eq!(hex.len(), 36);
        assert_eq!(hex_to_bitvec(&hex), Some(bits));
    }

    #[test]
    fn test_bitvec_roundtrip_values() {
        let bits = [0xFF, 0x00, 0x00];
        let hex = bitvec_to_hex(&bits);
        assert_eq!(&hex[..16], "ff00000000000000"); // word 0 LE
        let decoded = hex_to_bitvec(&hex).unwrap();
        assert_eq!(decoded, bits);
    }

    #[test]
    fn test_bitvec_roundtrip_all_words() {
        let bits = [0x0123456789ABCDEFu64, 0xFEDCBA9876543210, 0xBEEF];
        let hex = bitvec_to_hex(&bits);
        let decoded = hex_to_bitvec(&hex).unwrap();
        assert_eq!(decoded, bits);
    }

    #[test]
    fn test_bitvec_slot_boundaries() {
        // Slot 0 → word 0, bit 0
        let mut bits = [0u64; 3];
        bits[0] |= 1u64 << 0;
        let hex = bitvec_to_hex(&bits);
        let decoded = hex_to_bitvec(&hex).unwrap();
        assert_ne!(decoded[0] & 1, 0);

        // Slot 63 → word 0, bit 63
        let mut bits = [0u64; 3];
        bits[0] |= 1u64 << 63;
        let hex = bitvec_to_hex(&bits);
        let decoded = hex_to_bitvec(&hex).unwrap();
        assert_ne!(decoded[0] & (1u64 << 63), 0);

        // Slot 64 → word 1, bit 0
        let mut bits = [0u64; 3];
        bits[1] |= 1u64 << 0;
        let hex = bitvec_to_hex(&bits);
        let decoded = hex_to_bitvec(&hex).unwrap();
        assert_ne!(decoded[1] & 1, 0);

        // Slot 143 → word 2, bit 15
        let mut bits = [0u64; 3];
        bits[2] |= 1u64 << 15;
        let hex = bitvec_to_hex(&bits);
        let decoded = hex_to_bitvec(&hex).unwrap();
        assert_ne!(decoded[2] & (1u64 << 15), 0);
    }

    #[test]
    fn test_hex_to_bitvec_bad_input() {
        assert_eq!(hex_to_bitvec(""), None);
        assert_eq!(hex_to_bitvec("too_short"), None);
        assert_eq!(hex_to_bitvec("zz0000000000000000000000000000000000"), None);
    }

    #[test]
    fn test_slot_calculation() {
        // 00:00 UTC → slot 0
        let ts = 1710460800u32; // 2024-03-15 00:00:00 UTC
        assert_eq!(slot_from_timestamp(ts), 0);

        // 12:30 UTC → slot 75 (12*6 + 3)
        let ts2 = ts + 12 * 3600 + 30 * 60;
        assert_eq!(slot_from_timestamp(ts2), 75);

        // 23:50 UTC → slot 143 (23*6 + 5)
        let ts3 = ts + 23 * 3600 + 50 * 60;
        assert_eq!(slot_from_timestamp(ts3), 143);
    }

    #[test]
    fn test_compute_uptime() {
        // All 144 slots set, 144 elapsed → 100%
        let bits = [u64::MAX, u64::MAX, 0xFFFF];
        let hex = bitvec_to_hex(&bits);
        let result = compute_uptime(&Some(hex), &Some("2026-03-16".to_string()), "2026-03-16", 144);
        assert_eq!(result, Some(100.0));

        // 10 of 10 slots set → 100%
        let bits = [0b1111111111, 0, 0]; // slots 0-9
        let hex = bitvec_to_hex(&bits);
        let result = compute_uptime(&Some(hex), &Some("2026-03-16".to_string()), "2026-03-16", 10);
        assert_eq!(result, Some(100.0));

        // 5 of 10 slots → 50%
        let bits = [0b11111, 0, 0]; // slots 0-4
        let hex = bitvec_to_hex(&bits);
        let result = compute_uptime(&Some(hex), &Some("2026-03-16".to_string()), "2026-03-16", 10);
        assert_eq!(result, Some(50.0));

        // Stale date → None
        let result = compute_uptime(&Some("000000000000000000000000000000000000".to_string()), &Some("2026-03-15".to_string()), "2026-03-16", 10);
        assert_eq!(result, None);

        // No data → None
        let result = compute_uptime(&None, &None, "2026-03-16", 10);
        assert_eq!(result, None);
    }
}
