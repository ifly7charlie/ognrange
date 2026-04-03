//! APRS packet statistics - shared between per-station and global contexts.
//!
//! `AprsPacketStats` tracks raw/accepted packet counts, exception counters,
//! and per-layer hourly breakdowns. Recording methods are defined once here
//! and used identically for both per-station (`StationDetails.stats`) and the
//! global aggregate (`StationGlobalStats` via `with_data`).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::stats_accumulator::DailyStatsData;

/// Per-station and global APRS packet statistics.
///
/// Resets daily: stats in the status DB are replaced with a fresh default each
/// time the day rolls over. Historical data is preserved in dated daily files.
///
/// Backward-compat: old DB/JSON entries may have `count` instead of `accepted`;
/// the API normalises this on read.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct AprsPacketStats {
    /// All packets seen by this station before any filtering
    pub count: u64,
    /// Packets that passed all filters and were written to coverage data
    pub accepted: u64,
    /// Sum of packet ages at receipt (server receive time − packet timestamp) in seconds,
    /// for accepted packets only. Divide by `accepted` to get mean packet age.
    pub delay_sum_secs: u64,
    pub ignored_tracker: u32,
    pub invalid_tracker: u32,
    pub invalid_timestamp: u32,
    pub ignored_stationary: u32,
    pub ignored_signal0: u32,
    pub ignored_h3stationary: u32,
    pub ignored_elevation: u32,
    pub ignored_future_timestamp: u32,
    pub ignored_stale_timestamp: u32,
    /// Accepted packet counts by layer and hour-of-day (0–23).
    /// e.g. `{"flarm": [0, 12, 5, ...], "adsb": [...]}`
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub hourly: HashMap<String, [u64; 24]>,
    /// Max H3 cell counts per accumulator type per layer, observed across rollup cycles.
    /// Outer key: accumulator type name ("day", "month", "year", "yearnz")
    /// Inner key: layer name ("combined", "flarm", etc.)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub max_h3_cells: HashMap<String, HashMap<String, usize>>,
}

// ---------------------------------------------------------------------------
// Recording methods - used by both per-station (&mut self) and global
// (via StationGlobalStats::with_data)
// ---------------------------------------------------------------------------

impl AprsPacketStats {
    pub fn record_raw(&mut self) {
        self.count += 1;
    }

    pub fn record_accepted(&mut self, layer: &str, hour: usize) {
        self.accepted += 1;
        self.hourly
            .entry(layer.to_string())
            .or_insert([0u64; 24])[hour % 24] += 1;
    }

    pub fn record_delay(&mut self, secs: u64) {
        self.delay_sum_secs = self.delay_sum_secs.saturating_add(secs);
    }

    pub fn record_invalid_timestamp(&mut self) {
        self.invalid_timestamp += 1;
    }

    pub fn record_invalid_tracker(&mut self) {
        self.invalid_tracker += 1;
    }

    pub fn record_ignored_future_timestamp(&mut self) {
        self.ignored_future_timestamp += 1;
    }

    pub fn record_ignored_stale_timestamp(&mut self) {
        self.ignored_stale_timestamp += 1;
    }

    pub fn record_ignored_tracker(&mut self) {
        self.ignored_tracker += 1;
    }

    pub fn record_ignored_stationary(&mut self) {
        self.ignored_stationary += 1;
    }

    pub fn record_ignored_signal0(&mut self) {
        self.ignored_signal0 += 1;
    }

    pub fn record_ignored_h3stationary(&mut self) {
        self.ignored_h3stationary += 1;
    }

    pub fn record_ignored_elevation(&mut self) {
        self.ignored_elevation += 1;
    }

    /// Record an H3 cell count for a (accumulator_type, layer) pair, keeping the max.
    pub fn record_h3_count(&mut self, acc_type: &str, layer: &str, count: usize) {
        let entry = self
            .max_h3_cells
            .entry(acc_type.to_string())
            .or_default()
            .entry(layer.to_string())
            .or_insert(0);
        *entry = (*entry).max(count);
    }
}

// ---------------------------------------------------------------------------
// Display - used for periodic log output
// ---------------------------------------------------------------------------

impl std::fmt::Display for AprsPacketStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        if self.ignored_stationary > 0 {
            parts.push(format!("stationary:{}", self.ignored_stationary));
        }
        if self.ignored_h3stationary > 0 {
            parts.push(format!("h3stationary:{}", self.ignored_h3stationary));
        }
        if self.ignored_signal0 > 0 {
            parts.push(format!("signal0:{}", self.ignored_signal0));
        }
        if self.ignored_tracker > 0 {
            parts.push(format!("relayed:{}", self.ignored_tracker));
        }
        if self.ignored_elevation > 0 {
            parts.push(format!("elevation:{}", self.ignored_elevation));
        }
        if self.ignored_future_timestamp > 0 {
            parts.push(format!("future_ts:{}", self.ignored_future_timestamp));
        }
        if self.ignored_stale_timestamp > 0 {
            parts.push(format!("stale_ts:{}", self.ignored_stale_timestamp));
        }
        if self.invalid_tracker > 0 {
            parts.push(format!("bad_tracker:{}", self.invalid_tracker));
        }
        if self.invalid_timestamp > 0 {
            parts.push(format!("bad_ts:{}", self.invalid_timestamp));
        }
        if parts.is_empty() {
            write!(f, "no rejects")
        } else {
            write!(f, "rejected: {}", parts.join(", "))
        }
    }
}

// ---------------------------------------------------------------------------
// DailyStatsData - JSON output for daily files
// ---------------------------------------------------------------------------

impl DailyStatsData for AprsPacketStats {
    fn to_output_json(&self, start_time: &chrono::DateTime<chrono::Utc>) -> String {
        let now = chrono::Utc::now();
        let uptime = (now - *start_time).num_seconds().max(0) as u64;

        // Build hourly map sorted by layer name
        let mut hourly = serde_json::Map::new();
        let mut hourly_sorted: Vec<_> = self.hourly.iter().collect();
        hourly_sorted.sort_by_key(|(k, _)| (*k).clone());
        for (layer, counts) in hourly_sorted {
            let arr: Vec<serde_json::Value> =
                counts.iter().map(|&c| serde_json::Value::from(c)).collect();
            hourly.insert(layer.clone(), serde_json::Value::Array(arr));
        }

        let output = serde_json::json!({
            "generated": now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            "startTime": start_time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            "uptimeSeconds": uptime,
            "count": self.count,
            "accepted": self.accepted,
            "delaySumSecs": self.delay_sum_secs,
            "ignoredTracker": self.ignored_tracker,
            "invalidTracker": self.invalid_tracker,
            "invalidTimestamp": self.invalid_timestamp,
            "ignoredStationary": self.ignored_stationary,
            "ignoredSignal0": self.ignored_signal0,
            "ignoredH3stationary": self.ignored_h3stationary,
            "ignoredElevation": self.ignored_elevation,
            "ignoredFutureTimestamp": self.ignored_future_timestamp,
            "ignoredStaleTimestamp": self.ignored_stale_timestamp,
            "hourly": hourly,
            "maxH3Cells": self.max_h3_cells,
        });

        serde_json::to_string_pretty(&output).unwrap_or_else(|_| "{}".to_string())
    }
}
