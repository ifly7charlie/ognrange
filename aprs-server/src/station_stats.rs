//! Global APRS packet statistics accumulator.
//!
//! `StationGlobalStats` accumulates the same `AprsPacketStats` as per-station
//! stats, but as a single live in-memory counter incremented with every packet
//! regardless of which station received it.
//!
//! Output file: `{OUTPUT_PATH}/stats/station-stats.{date}.json.gz`
//! Symlink:     `{OUTPUT_PATH}/stats/station-stats.json.gz` → latest dated file
//! State file:  `{OUTPUT_PATH}/stats/station-stats.state.json`

use tracing::error;

use crate::accumulators::Accumulators;
use crate::config::OUTPUT_PATH;
use crate::stats_accumulator::DailyAccumulator;
use crate::station::AprsPacketStats;

const FILE_PREFIX: &str = "station-stats";

pub struct StationGlobalStats {
    acc: DailyAccumulator<AprsPacketStats>,
}

impl StationGlobalStats {
    /// Create fresh global stats starting now.
    pub fn new() -> Self {
        Self {
            acc: DailyAccumulator::new(),
        }
    }

    /// Load from state file, falling back to fresh if missing or stale.
    pub fn load() -> Self {
        let state_path = state_path();
        Self {
            acc: DailyAccumulator::load(&state_path),
        }
    }

    /// Increment the raw packet counter.
    pub fn record_raw(&self) {
        self.acc.with_data(|d| d.count += 1);
    }

    /// Increment the accepted counter and the hourly per-layer bucket.
    pub fn record_accepted(&self, layer: &str, hour: usize) {
        let layer = layer.to_string();
        self.acc.with_data(move |d| {
            d.accepted += 1;
            d.hourly.entry(layer).or_insert([0u64; 24])[hour % 24] += 1;
        });
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
