//! Generic single-period (day-only) stats accumulator.
//!
//! `DailyAccumulator<T>` tracks one day of data in memory. The frontend
//! aggregates monthly/yearly views from daily files, so only the current
//! day needs to be kept in memory here.
//!
//! Used by both per-station stats (via `StationDetails.stats`) and the
//! global station aggregate (`StationGlobalStats`).

use std::sync::Mutex;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::accumulators::{AccumulatorEntry, Accumulators};
use crate::config::UNCOMPRESSED_ARROW_FILES;
use crate::coverage::header::AccumulatorBucket;
use crate::json_io::{read_json, write_atomic, write_gz_atomic};
use crate::symlinks::symlink_atomic;

/// Trait implemented by stats data structs stored in a `DailyAccumulator`.
pub trait DailyStatsData:
    Clone + Default + Serialize + for<'de> Deserialize<'de> + Send + 'static
{
    /// Build the JSON string written to disk each rollup.
    fn to_output_json(&self, start_time: &DateTime<Utc>) -> String;
}

struct DailyAccumulatorInner<T> {
    data: T,
    start_time: DateTime<Utc>,
    day_bucket: AccumulatorBucket,
}

/// Single-period in-memory accumulator. Tracks the current day only.
pub struct DailyAccumulator<T: DailyStatsData> {
    inner: Mutex<DailyAccumulatorInner<T>>,
}

impl<T: DailyStatsData> DailyAccumulator<T> {
    /// Create a fresh accumulator starting now.
    pub fn new() -> Self {
        let now = Utc::now();
        let day_bucket = day_bucket_for(&now);
        Self {
            inner: Mutex::new(DailyAccumulatorInner {
                data: T::default(),
                start_time: now,
                day_bucket,
            }),
        }
    }

    /// Load from a state file, falling back to fresh if missing or from a different day.
    pub fn load(state_path: &str) -> Self {
        let parsed = match read_json(state_path) {
            Some(v) => v,
            None => return Self::new(),
        };

        let start_str = match parsed["startTime"].as_str() {
            Some(s) => s,
            None => return Self::new(),
        };
        let start_time = match start_str.parse::<DateTime<Utc>>() {
            Ok(t) => t,
            Err(_) => return Self::new(),
        };

        // Validate: reject state from a different day
        let now = Utc::now();
        let current_bucket = day_bucket_for(&now);
        let stored_bucket = AccumulatorBucket(parsed["dayBucket"].as_u64().unwrap_or(0) as u16);
        if stored_bucket != current_bucket {
            info!("DailyAccumulator state is from a different day, starting fresh");
            return Self::new();
        }

        let data: T = match serde_json::from_value(parsed["data"].clone()) {
            Ok(d) => d,
            Err(e) => {
                error!("Failed to deserialize DailyAccumulator state data: {}", e);
                return Self::new();
            }
        };

        info!("Restored DailyAccumulator state from {}", start_str);
        Self {
            inner: Mutex::new(DailyAccumulatorInner {
                data,
                start_time,
                day_bucket: stored_bucket,
            }),
        }
    }

    /// Mutate the stats data under the lock.
    pub fn with_data<F: FnOnce(&mut T)>(&self, f: F) {
        let mut inner = self.inner.lock().unwrap();
        f(&mut inner.data);
    }

    /// Write output file for the old day. If the day bucket rotated, reset internal state.
    ///
    /// File naming: `{stats_dir}/{file_prefix}.{old_acc.day.file}.json.gz`
    /// Symlink: `{stats_dir}/{file_prefix}.json.gz` → latest dated file
    pub fn write_and_maybe_reset(
        &self,
        old_acc: &Accumulators,
        new_acc: &Accumulators,
        stats_dir: &str,
        file_prefix: &str,
    ) {
        let old_day = &old_acc.day;
        let new_day = &new_acc.day;
        let rotated = old_day.bucket != new_day.bucket;

        // Build JSON while holding lock, then release for I/O
        let json = {
            let inner = self.inner.lock().unwrap();
            inner.data.to_output_json(&inner.start_time)
        };

        write_output_file(stats_dir, file_prefix, old_day, &json);

        // Reset if day rotated
        if rotated {
            let now = Utc::now();
            let mut inner = self.inner.lock().unwrap();
            inner.data = T::default();
            inner.start_time = now;
            inner.day_bucket = new_day.bucket;
            info!("Reset DailyAccumulator ({}) for new day: {}", file_prefix, new_day.file);
        }
    }

    /// Write output file using the current accumulator state (for shutdown / save_state).
    pub fn write_current(&self, stats_dir: &str, file_prefix: &str, day_entry: &AccumulatorEntry) {
        let json = {
            let inner = self.inner.lock().unwrap();
            inner.data.to_output_json(&inner.start_time)
        };
        write_output_file(stats_dir, file_prefix, day_entry, &json);
    }

    /// Persist state for crash recovery.
    pub fn save_state(&self, state_path: &str) {
        let inner = self.inner.lock().unwrap();
        let data_val = match serde_json::to_value(&inner.data) {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to serialize DailyAccumulator state: {}", e);
                return;
            }
        };
        let state = serde_json::json!({
            "startTime": inner.start_time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            "dayBucket": inner.day_bucket.0,
            "data": data_val,
        });
        let json = serde_json::to_string_pretty(&state).unwrap_or_else(|_| "{}".to_string());
        if let Err(e) = std::fs::write(state_path, json.as_bytes()) {
            error!("Failed to write DailyAccumulator state to {}: {}", state_path, e);
        }
    }
}

/// Write a daily stats file and update the symlink.
fn write_output_file(
    stats_dir: &str,
    file_prefix: &str,
    day_entry: &AccumulatorEntry,
    json: &str,
) {
    let file_name = format!("{}.{}.json.gz", file_prefix, day_entry.file);
    write_gz_atomic(stats_dir, &file_name, json);

    if *UNCOMPRESSED_ARROW_FILES {
        let plain_name = format!("{}.{}.json", file_prefix, day_entry.file);
        write_atomic(stats_dir, &plain_name, json);
    }

    // Update the "latest" symlink
    symlink_atomic(&file_name, &format!("{}/{}.json.gz", stats_dir, file_prefix));
    if *UNCOMPRESSED_ARROW_FILES {
        let plain_name = format!("{}.{}.json", file_prefix, day_entry.file);
        symlink_atomic(
            &plain_name,
            &format!("{}/{}.json", stats_dir, file_prefix),
        );
    }
}

/// Compute the day-level AccumulatorBucket for a given timestamp.
/// Matches the formula in `what_accumulators()`.
fn day_bucket_for(dt: &DateTime<Utc>) -> AccumulatorBucket {
    use chrono::Datelike;
    let y = dt.year() as u32;
    let m0 = dt.month() - 1; // 0-indexed
    let d = dt.day();
    AccumulatorBucket(
        (((y as u16) & 0x07) << 9) | (((m0 as u16) & 0x0f) << 5) | ((d as u16) & 0x1f),
    )
}
