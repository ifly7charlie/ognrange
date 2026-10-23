//! Global APRS packet statistics accumulator.
//!
//! `StationGlobalStats` wraps `DailyAccumulator<AprsPacketStats>` for the
//! daily file output, and exposes `with_data` so callers use the same
//! recording methods defined on `AprsPacketStats`.
//!
//! Pre-station counters (packets rejected before a station is identified) are
//! tracked as bare atomics since they don't belong in per-station stats.
//!
//! Output file: `{OUTPUT_PATH}/stats/station-stats.{date}.json.gz`
//! Symlink:     `{OUTPUT_PATH}/stats/station-stats.json.gz` → latest dated file
//! State file:  `{OUTPUT_PATH}/stats/station-stats.state.json`

use std::sync::atomic::{AtomicU64, Ordering};

use tracing::error;

use crate::accumulators::Accumulators;
use crate::config::OUTPUT_PATH;
use crate::packet_stats::AprsPacketStats;
use crate::stats_accumulator::DailyAccumulator;

const FILE_PREFIX: &str = "station-stats";

/// Snapshot of pre-station counters for logging.
pub struct PreStationStats {
    pub raw_count: u64,
    pub invalid_packet: u64,
    pub ignored_station: u64,
    pub ignored_protocol: u64,
}

impl std::fmt::Display for PreStationStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        if self.ignored_station > 0 {
            parts.push(format!("station:{}", self.ignored_station));
        }
        if self.ignored_protocol > 0 {
            parts.push(format!("protocol:{}", self.ignored_protocol));
        }
        if self.invalid_packet > 0 {
            parts.push(format!("invalid:{}", self.invalid_packet));
        }
        if parts.is_empty() {
            write!(f, "")
        } else {
            write!(f, ", {}", parts.join(", "))
        }
    }
}

pub struct StationGlobalStats {
    acc: DailyAccumulator<AprsPacketStats>,

    // Pre-station counters - packets rejected before a station is identified.
    // These are global-only (not in per-station AprsPacketStats).
    pub raw_count: AtomicU64,
    pub invalid_packet: AtomicU64,
    pub ignored_station: AtomicU64,
    pub ignored_protocol: AtomicU64,
}

impl StationGlobalStats {
    /// Create fresh global stats starting now.
    pub fn new() -> Self {
        Self {
            acc: DailyAccumulator::new(),
            raw_count: AtomicU64::new(0),
            invalid_packet: AtomicU64::new(0),
            ignored_station: AtomicU64::new(0),
            ignored_protocol: AtomicU64::new(0),
        }
    }

    /// Load from state file, falling back to fresh if missing or stale.
    pub fn load() -> Self {
        let state_path = state_path();
        Self {
            acc: DailyAccumulator::load(&state_path),
            raw_count: AtomicU64::new(0),
            invalid_packet: AtomicU64::new(0),
            ignored_station: AtomicU64::new(0),
            ignored_protocol: AtomicU64::new(0),
        }
    }

    /// Mutate the shared `AprsPacketStats` under the lock.
    /// Callers use the same recording methods as per-station:
    ///   `global.with_data(|d| d.record_ignored_stationary())`
    pub fn with_data<F: FnOnce(&mut AprsPacketStats)>(&self, f: F) {
        self.acc.with_data(f);
    }

    /// Snapshot for periodic logging.
    pub fn snapshot(&self) -> (AprsPacketStats, PreStationStats) {
        let mut stats = AprsPacketStats::default();
        self.acc.with_data(|d| stats = d.clone());
        let pre = PreStationStats {
            raw_count: self.raw_count.load(Ordering::Relaxed),
            invalid_packet: self.invalid_packet.load(Ordering::Relaxed),
            ignored_station: self.ignored_station.load(Ordering::Relaxed),
            ignored_protocol: self.ignored_protocol.load(Ordering::Relaxed),
        };
        (stats, pre)
    }

    /// Write daily stats file. Called each rollup alongside `protocol_stats.write_stats()`.
    pub fn write_and_maybe_reset(&self, old_acc: &Accumulators, new_acc: &Accumulators) {
        let stats_dir = stats_dir();
        if let Err(e) = std::fs::create_dir_all(&stats_dir) {
            error!("Failed to create stats directory: {}", e);
            return;
        }
        self.acc
            .write_and_maybe_reset(old_acc, new_acc, &stats_dir, FILE_PREFIX);
    }

    /// Persist state for crash recovery. Called on graceful shutdown.
    pub fn save_state(&self) {
        let stats_dir = stats_dir();
        if let Err(e) = std::fs::create_dir_all(&stats_dir) {
            error!("Failed to create stats directory: {}", e);
            return;
        }

        // Also write the current snapshot
        use crate::accumulators::initialise_accumulators;
        let accs = initialise_accumulators();
        self.acc.write_current(&stats_dir, FILE_PREFIX, &accs.day);

        self.acc.save_state(&state_path());
    }
}

fn stats_dir() -> String {
    format!("{}stats", *OUTPUT_PATH)
}

fn state_path() -> String {
    format!("{}stats/{}.state.json", *OUTPUT_PATH, FILE_PREFIX)
}
