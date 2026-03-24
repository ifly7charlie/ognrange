//! Global system uptime tracking via APRS-IS server keepalives.
//!
//! Each keepalive from the upstream APRS-IS server (e.g. aprsc) sets a bit
//! in a 144-slot daily bitvector (one bit per 10-minute UTC window).
//! The result is exported to `stats/global-uptime.json.gz` (and `.json`
//! if `UNCOMPRESSED_ARROW_FILES` is set).

use std::sync::Mutex;

use chrono::Utc;
use tracing::info;

use crate::bitvec::{bitvec_to_hex, hex_to_bitvec, popcount_144, slot_from_timestamp};
use crate::config::{OUTPUT_PATH, UNCOMPRESSED_ARROW_FILES};
use crate::json_io::{read_json_or_gz, write_atomic, write_gz_atomic};
use crate::symlinks::symlink_atomic;

/// Snapshot of parsed server keepalive fields
struct ParsedKeepalive {
    software: String,
    version: String,
    alias: String,
    address: String,
}

struct Inner {
    bits: [u64; 3],
    date: String,
    server: String,
    server_software: String,
    server_address: String,
}

/// Cloned snapshot of Inner for use outside the lock (e.g. file I/O).
struct Snapshot {
    bits: [u64; 3],
    date: String,
    server: String,
    server_software: String,
    server_address: String,
}

impl Inner {
    fn snapshot(&self) -> Snapshot {
        Snapshot {
            bits: self.bits,
            date: self.date.clone(),
            server: self.server.clone(),
            server_software: self.server_software.clone(),
            server_address: self.server_address.clone(),
        }
    }
}

/// Tracks global system uptime from APRS-IS keepalives.
pub struct GlobalUptime {
    inner: Mutex<Inner>,
}

impl GlobalUptime {
    /// Create a new tracker, restoring today's state from disk if available.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(load_state()),
        }
    }

    /// Clear the current slot's bit and rewrite the JSON.
    /// Called during shutdown so a partial slot isn't counted as fully covered.
    pub fn clear_current_slot(&self) {
        let now = Utc::now();
        let today = now.format("%Y-%m-%d").to_string();
        let ts = now.timestamp() as u32;
        let slot = slot_from_timestamp(ts);

        let snap = {
            let mut inner = self.inner.lock().unwrap();
            if inner.date != today {
                return; // nothing to clear
            }
            inner.bits[slot / 64] &= !(1u64 << (slot % 64));
            inner.snapshot()
        };

        info!("Global uptime: cleared current slot on shutdown");
        write_live(now, &snap);
    }

    /// Record receipt of an APRS-IS server message.
    /// Only aprsc keepalive lines set a bit; other server messages are ignored.
    pub fn record_keepalive(&self, msg: &str) {
        let parsed = match parse_server_keepalive(msg) {
            Some(p) => p,
            None => return,
        };

        let now = Utc::now();
        let today = now.format("%Y-%m-%d").to_string();
        let ts = now.timestamp() as u32;
        let slot = slot_from_timestamp(ts);

        let snap = {
            let mut inner = self.inner.lock().unwrap();

            // Reset bitvector on date change
            if inner.date != today {
                info!("Global uptime: new day {}", today);
                inner.bits = [0u64; 3];
                inner.date = today;
            }

            inner.bits[slot / 64] |= 1u64 << (slot % 64);
            inner.server = parsed.alias;
            inner.server_software = format!("{} {}", parsed.software, parsed.version);
            inner.server_address = parsed.address;
            inner.snapshot()
        };

        write_live(now, &snap);
    }

    /// Write a dated snapshot during rollup (e.g. `global-uptime.2026-03-16.json.gz`)
    /// and update symlinks to point to it.
    pub fn write_snapshot(&self, day_file: &str) {
        let now = Utc::now();
        let snap = {
            let inner = self.inner.lock().unwrap();
            if inner.date.is_empty() {
                return; // no keepalives received yet
            }
            inner.snapshot()
        };

        // Use 144 only for completed (previous) days; for today use the actual elapsed slot count.
        let today = now.format("%Y-%m-%d").to_string();
        let elapsed_override = if snap.date != today { Some(144) } else { None };
        let content = build_json(now, &snap, elapsed_override);
        let stats_dir = format!("{}stats", *OUTPUT_PATH);

        // Always write .json.gz
        let dated_gz_name = format!("global-uptime.{}.json.gz", day_file);
        write_gz_atomic(&stats_dir, &dated_gz_name, &content);
        symlink_atomic(&dated_gz_name, &format!("{}/global-uptime.json.gz", stats_dir));

        // Conditionally write .json
        if *UNCOMPRESSED_ARROW_FILES {
            let dated_name = format!("global-uptime.{}.json", day_file);
            write_atomic(&stats_dir, &dated_name, &content);
            symlink_atomic(&dated_name, &format!("{}/global-uptime.json", stats_dir));
        }

        info!("Wrote global uptime snapshot: {}", dated_gz_name);
    }
}

/// Build the JSON string from a snapshot.
/// `elapsed_override`: if Some, use this as the elapsed slot count (e.g. 144 for a completed day).
/// If None, derive elapsed from the current time (for the live file).
fn build_json(now: chrono::DateTime<Utc>, snap: &Snapshot, elapsed_override: Option<u32>) -> String {
    let hex = bitvec_to_hex(&snap.bits);
    let elapsed = elapsed_override.unwrap_or_else(|| {
        let ts = now.timestamp() as u32;
        (slot_from_timestamp(ts) as u32 + 1).min(144)
    });
    let set = popcount_144(&snap.bits);
    let uptime = if elapsed > 0 {
        ((set as f32 / elapsed as f32) * 1000.0).round() / 10.0
    } else {
        0.0
    };

    let json = serde_json::json!({
        "generated": now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "date": snap.date,
        "server": snap.server,
        "serverSoftware": snap.server_software,
        "serverAddress": snap.server_address,
        "activity": hex,
        "uptime": uptime,
        "slot": elapsed,
    });

    serde_json::to_string_pretty(&json).unwrap_or_else(|_| "{}".to_string())
}

/// Write the live (non-dated) global-uptime files directly.
fn write_live(now: chrono::DateTime<Utc>, snap: &Snapshot) {
    let content = build_json(now, snap, None);
    let stats_dir = format!("{}stats", *OUTPUT_PATH);
    write_gz_atomic(&stats_dir, "global-uptime.json.gz", &content);
    if *UNCOMPRESSED_ARROW_FILES {
        write_atomic(&stats_dir, "global-uptime.json", &content);
    }
}

/// Parse an aprsc server keepalive line.
/// Expected format: `# aprsc <version> <DD Mon YYYY HH:MM:SS GMT> <alias> <address>`
fn parse_server_keepalive(msg: &str) -> Option<ParsedKeepalive> {
    let content = msg.strip_prefix("# ")?.trim();

    if !content.starts_with("aprsc ") {
        return None;
    }

    // aprsc version DD Mon YYYY HH:MM:SS GMT alias address
    // 0     1       2  3   4    5        6   7     8
    let parts: Vec<&str> = content.split_whitespace().collect();
    if parts.len() < 9 {
        return None;
    }

    Some(ParsedKeepalive {
        software: parts[0].to_string(),
        version: parts[1].to_string(),
        alias: parts[7].to_string(),
        address: parts[8].to_string(),
    })
}

/// Load today's state from `stats/global-uptime.json` (or `.json.gz`), or return defaults.
fn load_state() -> Inner {
    let path = format!("{}stats/global-uptime.json", *OUTPUT_PATH);
    let parsed = match read_json_or_gz(&path) {
        Some(v) => v,
        None => return default_inner(),
    };

    let today = Utc::now().format("%Y-%m-%d").to_string();
    let date = parsed["date"].as_str().unwrap_or("");
    if date != today {
        return default_inner();
    }

    let hex = parsed["activity"].as_str().unwrap_or("");
    let bits = hex_to_bitvec(hex).unwrap_or([0u64; 3]);

    info!(
        "Restored global uptime for {}: {}/{} slots",
        date,
        popcount_144(&bits),
        parsed["slot"].as_u64().unwrap_or(0)
    );

    Inner {
        bits,
        date: date.to_string(),
        server: parsed["server"].as_str().unwrap_or("").to_string(),
        server_software: parsed["serverSoftware"].as_str().unwrap_or("").to_string(),
        server_address: parsed["serverAddress"].as_str().unwrap_or("").to_string(),
    }
}

fn default_inner() -> Inner {
    Inner {
        bits: [0u64; 3],
        date: String::new(),
        server: String::new(),
        server_software: String::new(),
        server_address: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_server_keepalive() {
        let msg = "# aprsc 2.1.19-g730c5c0 16 Mar 2026 10:31:00 GMT GLIDERN5 148.251.228.229:14580";
        let parsed = parse_server_keepalive(msg).unwrap();
        assert_eq!(parsed.software, "aprsc");
        assert_eq!(parsed.version, "2.1.19-g730c5c0");
        assert_eq!(parsed.alias, "GLIDERN5");
        assert_eq!(parsed.address, "148.251.228.229:14580");
    }

    #[test]
    fn test_parse_not_aprsc() {
        // Our own keepalive echo
        assert!(parse_server_keepalive("# ognrange https://example.com v1").is_none());
        // Login response
        assert!(parse_server_keepalive("# logresp OGNRANGE unverified").is_none());
    }

    #[test]
    fn test_parse_too_short() {
        assert!(parse_server_keepalive("# aprsc 2.1.19").is_none());
    }

    #[test]
    fn test_parse_no_hash_prefix() {
        assert!(parse_server_keepalive("aprsc 2.1.19-g730c5c0 16 Mar 2026 10:31:00 GMT GLIDERN5 148.251.228.229:14580").is_none());
    }

    #[test]
    fn test_record_keepalive_sets_bit() {
        let uptime = GlobalUptime {
            inner: Mutex::new(default_inner()),
        };
        uptime.record_keepalive("# aprsc 2.1.19-g730c5c0 16 Mar 2026 10:31:00 GMT GLIDERN5 148.251.228.229:14580");

        let inner = uptime.inner.lock().unwrap();
        assert_eq!(inner.server, "GLIDERN5");
        assert_eq!(inner.server_software, "aprsc 2.1.19-g730c5c0");
        assert_eq!(inner.server_address, "148.251.228.229:14580");
        // At least one bit should be set
        assert!(popcount_144(&inner.bits) >= 1);
    }

    #[test]
    fn test_clear_current_slot() {
        let now = Utc::now();
        let today = now.format("%Y-%m-%d").to_string();
        let ts = now.timestamp() as u32;
        let slot = slot_from_timestamp(ts);

        // Pre-fill: set the current slot and one other
        let other_slot = if slot > 0 { slot - 1 } else { slot + 1 };
        let mut bits = [0u64; 3];
        bits[slot / 64] |= 1u64 << (slot % 64);
        bits[other_slot / 64] |= 1u64 << (other_slot % 64);
        assert_eq!(popcount_144(&bits), 2);

        let uptime = GlobalUptime {
            inner: Mutex::new(Inner {
                bits,
                date: today,
                server: String::new(),
                server_software: String::new(),
                server_address: String::new(),
            }),
        };

        uptime.clear_current_slot();

        let inner = uptime.inner.lock().unwrap();
        // Current slot cleared, other slot still set
        assert_eq!(popcount_144(&inner.bits), 1);
        assert_eq!(inner.bits[slot / 64] & (1u64 << (slot % 64)), 0);
        assert_ne!(inner.bits[other_slot / 64] & (1u64 << (other_slot % 64)), 0);
    }
}
