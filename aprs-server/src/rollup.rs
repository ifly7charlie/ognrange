//! Rollup - merging current accumulators into day/month/year archives
//! and exporting to Apache Arrow files.
//!
//! This module handles the periodic aggregation of coverage data from
//! the "current" accumulator into longer-term day, month, year, and
//! year-nz (Southern Hemisphere season) accumulators.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use rusty_leveldb::LdbIterator;

/// Global shutdown flag - checked by long-running DB iterations.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Signal all rollup tasks to stop iterating.
pub fn request_shutdown() {
    SHUTDOWN.store(true, Ordering::Relaxed);
}

fn is_shutdown() -> bool {
    SHUTDOWN.load(Ordering::Relaxed)
}

/// Warn when a station rollup task exceeds this fraction of the rollup period.
const SLOW_TASK_WARN_FRACTION: f64 = 0.75;
/// After the first slow-task warning, repeat with progress every this many seconds.
const SLOW_TASK_REPEAT_WARN_SECS: u64 = 300;
/// Startup rollup tasks: first warning and repeat interval.
const STARTUP_TASK_WARN_SECS: u64 = 300;

/// True while a rollup (periodic or startup) is running. The rollup timer
/// checks this before swapping accumulators so a slow rollup causes the
/// boundary to be skipped (and later collapsed into one catch-up rollup)
/// rather than two rollups overlapping.
static ROLLUP_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

pub fn rollup_in_progress() -> bool {
    ROLLUP_IN_PROGRESS.load(Ordering::Acquire)
}

/// RAII guard for ROLLUP_IN_PROGRESS - Drop-based release so an aborted
/// rollup future still clears the flag.
struct RollupGuard;

impl RollupGuard {
    fn try_acquire() -> Option<Self> {
        ROLLUP_IN_PROGRESS
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
            .then_some(RollupGuard)
    }
}

impl Drop for RollupGuard {
    fn drop(&mut self) {
        ROLLUP_IN_PROGRESS.store(false, Ordering::Release);
    }
}

use arrow::array::{
    ArrayRef, StringArray, UInt16Array, UInt32Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use flate2::write::GzEncoder;
use flate2::Compression;
use tracing::{error, info, warn};

use crate::accumulators::{AccumulatorEntry, Accumulators};
use crate::config::{
    MAX_SIMULTANEOUS_ROLLUPS, ROLLUP_PERIOD_MINUTES, STATION_EXPIRY_TIME_SECS,
    UNCOMPRESSED_ARROW_FILES,
};
use crate::coverage::activity::{update_activity, RollupActivity};
use crate::coverage::header::{
    AccumulatorBucket, AccumulatorType, CoverageHeader,
};
use crate::coverage::record::{ArrowGlobal, ArrowStation, CoverageRecord};
use crate::layers::{is_layer_prefixed, Layer};
use crate::packet_stats::AprsPacketStats;
use crate::station::{StationDetails, StationManager};
use crate::db::{self, Storage, TrackedDb};
use crate::types::{Epoch, H3Index, StationId, StationName};

#[derive(Debug, Default)]
pub struct RollupStats {
    pub stations_processed: usize,
    pub stations_skipped: usize,
    pub records_read: usize,
    pub records_written: usize,
    pub records_deleted: usize,
    pub arrow_records: usize,
    pub elapsed_ms: u64,
    /// Per-(layer, acc_type) H3 cell counts from the global station rollup.
    pub global_h3_counts: Vec<(String, String, usize)>,
}

/// Live progress for a single rollup task, readable from outside the blocking thread.
#[derive(Debug, Default)]
struct RollupProgress {
    layer: String,
    phase: String,
    detail: String,
    records_read: usize,
    records_written: usize,
    records_deleted: usize,
    arrow_records: usize,
}

impl std::fmt::Display for RollupProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "layer={}, phase={}, read={}, written={}, deleted={}, arrow={}",
            self.layer, self.phase, self.records_read, self.records_written,
            self.records_deleted, self.arrow_records
        )?;
        if !self.detail.is_empty() {
            write!(f, ", detail={}", self.detail)?;
        }
        Ok(())
    }
}

/// Await a rollup task with no timeout, logging a warning (with live progress)
/// when it runs long. First warning fires at `first_warn` after `started`,
/// then repeats every `repeat_warn`. Only returns when the task completes
/// or panics - tasks are never abandoned (spawn_blocking threads cannot be
/// aborted; abandoning one leaves it writing the DB concurrently with the
/// next rollup).
async fn await_with_warnings<T>(
    mut handle: tokio::task::JoinHandle<T>,
    label: &str,
    progress: &std::sync::Mutex<RollupProgress>,
    started: std::time::Instant,
    first_warn: std::time::Duration,
    repeat_warn: std::time::Duration,
) -> Result<T, tokio::task::JoinError> {
    let mut next_warn = started + first_warn;
    loop {
        let sleep_for = next_warn.saturating_duration_since(std::time::Instant::now());
        tokio::select! {
            res = &mut handle => return res,
            _ = tokio::time::sleep(sleep_for) => {
                let prog = progress.lock().map(|p| p.to_string()).unwrap_or_default();
                warn!(
                    "rollup task {} still running after {:.0}s - progress: {}",
                    label, started.elapsed().as_secs_f64(), prog
                );
                next_warn = std::time::Instant::now() + repeat_warn;
            }
        }
    }
}

/// Perform a full rollup: merge current → day/month/year/yearnz.
/// Caller must flush the H3 cache before calling this.
pub async fn rollup_all(
    storage: &Storage,
    station_manager: &StationManager,
    old_accumulators: &Accumulators,
    new_accumulators: Option<&Accumulators>,
    write_json: bool,
) -> RollupStats {
    // Defense in depth: the rollup timer is strictly sequential and checks
    // rollup_in_progress() before swapping accumulators, so this should never
    // fire - but if a second entry point ever appears, skip rather than overlap.
    let Some(_rollup_guard) = RollupGuard::try_acquire() else {
        warn!("rollup already in progress - skipping this rollup cycle");
        return RollupStats::default();
    };

    let start = std::time::Instant::now();

    info!("--------[ accumulator rotation ]--------");
    let (old_text, old_files) = old_accumulators.describe();
    if let Some(new_acc) = new_accumulators {
        let (new_text, new_files) = new_acc.describe();
        info!("{}/{} => {}/{}", old_text, old_files, new_text, new_files);
    }

    // --- Station expiry check ---
    let now_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32;
    let expiry_epoch = now_epoch.saturating_sub(*STATION_EXPIRY_TIME_SECS as u32);

    let all_station_details = station_manager.all_stations_with_global();
    let mut valid_stations: HashSet<StationId> = HashSet::new();
    let mut invalid_count = 0usize;
    let mut moved_count = 0usize;
    let mut need_purge = false;
    let mut confirmed_moves: HashSet<StationId> = HashSet::new();
    // Tracks stations transitioning valid→invalid THIS rollup.
    // Only these get their databases purged - already-invalid stations (from
    // previous rollups) are excluded so a single new expiry doesn't sweep up
    // a large backlog of orphaned databases.
    let mut newly_invalid: HashSet<StationId> = HashSet::new();

    let move_confirm_secs = *crate::config::STATION_MOVE_CONFIRM_SECS as u32;

    for station in &all_station_details {
        let was_valid = station.valid;
        let validity_ts = station
            .last_packet
            .or(station.last_beacon)
            .map(|e| e.0)
            .unwrap_or(now_epoch); // no timestamp yet → assume valid

        // Confirm moves: station is bouncing and the previous (original) location
        // hasn't been seen for STATION_MOVE_CONFIRM_DAYS
        let confirmed_move = if station.bouncing {
            let prev_age = station.last_seen_at_previous
                .map(|e| now_epoch.saturating_sub(e.0))
                .unwrap_or(0); // no timestamp → treat as recent (legacy data)
            if prev_age >= move_confirm_secs {
                info!(
                    "station {} confirming move - previous location last seen {} days ago",
                    station.station,
                    prev_age / 86400
                );
                let mut updated = station.clone();
                updated.moved = true;
                updated.bouncing = false;
                station_manager.update(&updated);
                confirmed_moves.insert(station.id);
                true
            } else {
                false
            }
        } else {
            false
        };

        if station.moved || confirmed_move {
            if was_valid {
                moved_count += 1;
            } else {
                // stale moved flag from before station was already purged
                let mut updated = station.clone();
                updated.moved = false;
                station_manager.update(&updated);
            }
        } else if validity_ts > expiry_epoch {
            valid_stations.insert(station.id);
        } else if was_valid {
            invalid_count += 1;
            newly_invalid.insert(station.id);
            info!(
                "station {} now invalid: expired, last activity {}",
                station.station,
                chrono::DateTime::from_timestamp(validity_ts as i64, 0)
                    .map(|d| d.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                    .unwrap_or_else(|| validity_ts.to_string())
            );
        }
        // already-invalid stations are left out of valid_stations (correct for
        // global rollup filtering) and are NOT added to newly_invalid, so they
        // won't have their databases purged just because need_purge is set by
        // a different station expiring.
    }

    // Safety: if >2% of stations became invalid, don't purge data (something is wrong).
    // Add all back to valid_stations so global rollup keeps their coverage, and clear
    // newly_invalid so their metadata is NOT updated - they'll be re-evaluated next rollup
    // instead of being permanently orphaned with valid=false but intact databases.
    if invalid_count as f64 / (valid_stations.len().max(1) as f64) > 0.02 {
        warn!(
            "Too many invalid stations ({}), not purging any",
            invalid_count
        );
        for station in &all_station_details {
            valid_stations.insert(station.id);
        }
        newly_invalid.clear();
    } else {
        need_purge = invalid_count > 0 || moved_count > 0;
    }

    // Update station validity in the station manager.
    // newly_invalid is empty when the safety valve fired, so expiring stations
    // retain valid=true and will be re-evaluated on the next rollup.
    for station in &all_station_details {
        let is_valid = valid_stations.contains(&station.id);
        let was_moved = station.moved || confirmed_moves.contains(&station.id);
        if station.valid != is_valid || was_moved {
            let mut updated = if confirmed_moves.contains(&station.id) {
                station_manager.get_or_create(&station.station)
                    .expect("station must exist during rollup")
            } else {
                station.clone()
            };
            updated.valid = is_valid;
            if was_moved {
                updated.moved = false;
                updated.purged_at = Some(crate::types::Epoch(now_epoch));
                updated.purge_reason = Some("moved".into());
            } else if !is_valid && station.valid {
                updated.purged_at = Some(crate::types::Epoch(now_epoch));
                updated.purge_reason = Some("expired".into());
            }
            station_manager.update(&updated);
        }
    }

    info!(
        "performing rollup of {} valid stations + global, {} invalid, {} moved",
        valid_stations.len(),
        invalid_count,
        moved_count,
    );

    // Determine which layers to process, in rollup order (lowest traffic first)
    let layers = crate::layers::rollup_layers(crate::config::ENABLED_LAYERS.as_ref());

    // Which non-current accumulators retired (bucket changed)?
    let retired_accumulators: Vec<(AccumulatorType, AccumulatorBucket)> = if let Some(new_acc) = new_accumulators {
        let mut retired = Vec::new();
        if old_accumulators.day.bucket != new_acc.day.bucket {
            retired.push((AccumulatorType::Day, old_accumulators.day.bucket));
        }
        if old_accumulators.month.bucket != new_acc.month.bucket {
            retired.push((AccumulatorType::Month, old_accumulators.month.bucket));
        }
        if old_accumulators.year.bucket != new_acc.year.bucket {
            retired.push((AccumulatorType::Year, old_accumulators.year.bucket));
        }
        if old_accumulators.yearnz.bucket != new_acc.yearnz.bucket {
            retired.push((AccumulatorType::YearNz, old_accumulators.yearnz.bucket));
        }
        retired
    } else {
        Vec::new()
    };
    let has_retired = !retired_accumulators.is_empty();

    // Earliest effective_start of any accumulator whose bucket changed (including current)
    let update_cutoff: Option<Epoch> = new_accumulators.map(|new_acc| {
        let mut min_start = old_accumulators.current.effective_start.0; // current always changes
        if old_accumulators.day.bucket != new_acc.day.bucket {
            min_start = min_start.min(old_accumulators.day.effective_start.0);
        }
        if old_accumulators.month.bucket != new_acc.month.bucket {
            min_start = min_start.min(old_accumulators.month.effective_start.0);
        }
        if old_accumulators.year.bucket != new_acc.year.bucket {
            min_start = min_start.min(old_accumulators.year.effective_start.0);
        }
        if old_accumulators.yearnz.bucket != new_acc.yearnz.bucket {
            min_start = min_start.min(old_accumulators.yearnz.effective_start.0);
        }
        Epoch(min_start)
    });

    // Build the list of stations to process (global is already first from all_stations_with_global)
    let mut station_entries: Vec<(String, bool, Option<StationDetails>)> = Vec::new();
    for station in &all_station_details {
        station_entries.push((
            station.station.as_str().to_string(),
            station.station.as_str() == "global",
            Some(station.clone()),
        ));
    }

    // --- Concurrent rollup with MAX_SIMULTANEOUS_ROLLUPS ---
    let max_concurrent = *MAX_SIMULTANEOUS_ROLLUPS;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
    let valid_stations = Arc::new(valid_stations);
    let accumulators = old_accumulators.clone();
    let layers = Arc::new(layers);
    let retired_accumulators = Arc::new(retired_accumulators);

    let mut tasks: Vec<(String, String, Arc<std::sync::Mutex<RollupProgress>>, std::time::Instant, tokio::task::JoinHandle<RollupStats>)> = Vec::new();
    let mut skipped_no_traffic: usize = 0;

    for (station_name, _is_global_hint, station_meta) in station_entries {
        let station_path = storage.station_path(&station_name).to_string_lossy().to_string();
        let is_global = station_name == "global";

        // Purge invalid/moved stations instead of rolling them up.
        // Only purge stations that became invalid THIS rollup (newly_invalid) or
        // were just confirmed as moved.  Already-invalid stations are skipped to
        // prevent a single new expiry from sweeping up a large backlog of
        // previously-undeleted databases.
        if !is_global && need_purge {
            let is_newly_invalid = station_meta
                .as_ref()
                .map(|s| newly_invalid.contains(&s.id))
                .unwrap_or(false);
            let was_moved = station_meta.as_ref().map(|s| s.moved).unwrap_or(false)
                || station_meta.as_ref().map(|s| confirmed_moves.contains(&s.id)).unwrap_or(false);
            if is_newly_invalid || was_moved {
                let reason = if was_moved {
                    "moved".to_string()
                } else {
                    let last = station_meta
                        .as_ref()
                        .and_then(|s| s.last_packet.or(s.last_beacon));
                    match last {
                        Some(e) => {
                            let dt = chrono::DateTime::from_timestamp(e.0 as i64, 0)
                                .map(|d| d.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                                .unwrap_or_else(|| "invalid".to_string());
                            format!("expired, last activity {} ({})", dt, e.0)
                        }
                        None => "no activity recorded".to_string(),
                    }
                };
                info!("clearing database for {}: {}", station_name, reason);
                storage.purge_station(&station_name);
                continue;
            }
        }

        // Skip invalid stations entirely -- no rollup, no arrow output
        if !is_global {
            let is_invalid = station_meta
                .as_ref()
                .map(|s| !valid_stations.contains(&s.id))
                .unwrap_or(false);
            if is_invalid {
                continue;
            }
        }

        // Skip DB rollup for stations with no new traffic since last output
        // but still write station JSON so metadata stays current
        if !is_global && !has_retired {
            if let Some(meta) = &station_meta {
                if let Some(out_epoch) = meta.output_epoch {
                    if !meta.moved {
                        let last = meta.last_packet.map(|e| e.0).unwrap_or(0);
                        let cutoff = update_cutoff.map(|e| e.0).unwrap_or(out_epoch.0);
                        if last < cutoff {
                            if write_json {
                                let output_dir = crate::config::output_dir(&station_name);
                                write_station_json(&output_dir, &station_name, meta, &accumulators, None, 0);
                            }
                            skipped_no_traffic += 1;
                            continue;
                        }
                    }
                }
            }
        }

        if !std::path::Path::new(&station_path).exists() {
            continue;
        }

        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let valid = valid_stations.clone();
        let acc = accumulators.clone();
        let layers = layers.clone();
        let retired = retired_accumulators.clone();
        let task_station_name = station_name.clone();
        let task_station_path = station_path.clone();
        let progress = Arc::new(std::sync::Mutex::new(RollupProgress::default()));
        let task_progress = progress.clone();
        let spawned_at = std::time::Instant::now();

        let new_current_bucket = new_accumulators.map(|na| na.current.bucket);
        let handle = tokio::task::spawn_blocking(move || {
            let _permit = permit; // released when task completes

            let valid_ref = if is_global { Some(valid.as_ref()) } else { None };
            let station_stats = match rollup_station_all_layers(
                &station_path,
                &station_name,
                &acc,
                &layers,
                valid_ref,
                is_global,
                station_meta.as_ref(),
                &task_progress,
                &retired,
                &SHUTDOWN, // cancel only on process exit
                new_current_bucket,
            ) {
                Ok(stats) => stats,
                Err(e) => {
                    error!("Rollup failed for {}: {}", station_name, e);
                    RollupStats::default()
                }
            };
            station_stats
        });
        tasks.push((task_station_name, task_station_path, progress, spawned_at, handle));
    }

    // Produce the master stations list (runs while station rollups are in progress)
    crate::stationfile::produce_station_file(station_manager, old_accumulators);

    // Collect results - no timeout: tasks are awaited to completion, with a
    // warning (including live progress) when one runs past 75% of the rollup
    // period, repeated every SLOW_TASK_REPEAT_WARN_SECS so a hang stays
    // visible in the logs.
    let mut total_stats = RollupStats::default();
    total_stats.stations_processed = tasks.len();
    let first_warn = std::time::Duration::from_secs_f64(
        *ROLLUP_PERIOD_MINUTES * 60.0 * SLOW_TASK_WARN_FRACTION,
    );
    let repeat_warn = std::time::Duration::from_secs(SLOW_TASK_REPEAT_WARN_SECS);
    let total_tasks = tasks.len();
    let mut completed_count = 0usize;

    for (station_name, station_path, progress, spawned_at, handle) in tasks {
        match await_with_warnings(handle, &station_name, &progress, spawned_at, first_warn, repeat_warn).await {
            Ok(stats) => {
                total_stats.records_read += stats.records_read;
                total_stats.records_written += stats.records_written;
                total_stats.records_deleted += stats.records_deleted;
                total_stats.arrow_records += stats.arrow_records;
                total_stats.global_h3_counts.extend(stats.global_h3_counts);
                // Update output_epoch/output_date (mirrors TS rollup.ts:207-208)
                // Also reset per-station stats on day rotation (after write_station_json captured them).
                if station_name != "global" {
                    if let Some(mut details) = station_manager.get(&StationName(station_name.clone())) {
                        details.output_epoch = Some(Epoch(now_epoch));
                        details.output_date = Some(
                            chrono::DateTime::from_timestamp(now_epoch as i64, 0)
                                .map(|d| d.to_rfc3339())
                                .unwrap_or_default()
                        );
                        let day_rotated = new_accumulators
                            .map(|na| na.day.bucket != old_accumulators.day.bucket)
                            .unwrap_or(false);
                        if day_rotated {
                            details.stats = AprsPacketStats::default();
                        }
                        station_manager.update(&details);
                    }
                }
            }
            Err(e) => {
                let prog = progress.lock().map(|p| p.to_string()).unwrap_or_default();
                error!("Rollup task panicked for {} ({}): {} - progress: {}", station_name, station_path, e, prog);
                total_stats.stations_skipped += 1;
            }
        }
        completed_count += 1;
        if total_tasks == 0 || completed_count * 10 / total_tasks != (completed_count - 1) * 10 / total_tasks {
            let pct = if total_tasks > 0 { completed_count * 100 / total_tasks } else { 100 };
            info!("rollup {}% [{}/{}]", pct, completed_count, total_tasks);
        }
    }

    total_stats.stations_skipped += skipped_no_traffic;
    total_stats.elapsed_ms = start.elapsed().as_millis() as u64;

    // On day rotation, clear beacon_activity for stations whose bitvector is still from the
    // previous day.  Stations that already received a beacon in the new day (beacon_activity_date
    // == new day file) are left untouched.  Without this sweep, stations that go silent at night
    // would carry yesterday's bitvector into today's day files indefinitely.
    if let Some(new_acc) = new_accumulators {
        if new_acc.day.bucket != old_accumulators.day.bucket {
            let new_day = &new_acc.day.file;
            for mut details in station_manager.all_stations() {
                let is_new_day = details.beacon_activity_date.as_deref() == Some(new_day.as_str());
                if !is_new_day && (details.beacon_activity.is_some() || details.beacon_activity_date.is_some()) {
                    details.beacon_activity = None;
                    details.beacon_activity_date = None;
                    station_manager.update(&details);
                }
            }
        }
    }

    // Persist all in-memory station state (output_epoch, stats resets, validity) to the DB
    station_manager.flush_all();

    info!(
        "Rollup complete in {}ms: {} stations ({} skipped no-traffic, {} concurrent), {} records read, {} written, {} deleted, {} arrow records",
        total_stats.elapsed_ms,
        total_stats.stations_processed,
        skipped_no_traffic,
        max_concurrent,
        total_stats.records_read,
        total_stats.records_written,
        total_stats.records_deleted,
        total_stats.arrow_records,
    );
    total_stats
}

/// Rollup state for one destination accumulator
struct RollupAccumulator {
    acc_type: AccumulatorType,
    bucket: AccumulatorBucket,
    file: String,
    /// DB key range for this destination
    range_start: String,
    range_end: String,
    /// Arrow rows collected
    arrow_station_rows: Vec<ArrowStation>,
    arrow_global_rows: Vec<ArrowGlobal>,
    /// Activity tracking (loaded from DB meta, updated after rollup)
    activity: RollupActivity,
    /// Number of destination records seen during merge (for logging)
    dest_record_count: usize,
}

/// Per-station rollup: open the DB once, roll up all layers, flush and close.
#[allow(clippy::too_many_arguments)]
fn rollup_station_all_layers(
    station_path: &str,
    station_name: &str,
    accumulators: &Accumulators,
    layers: &[Layer],
    valid_stations: Option<&HashSet<StationId>>,
    is_global: bool,
    station_meta: Option<&crate::station::StationDetails>,
    progress: &std::sync::Mutex<RollupProgress>,
    retired_accumulators: &[(AccumulatorType, AccumulatorBucket)],
    cancel: &AtomicBool,
    new_current_bucket: Option<AccumulatorBucket>,
) -> Result<RollupStats, String> {
    let station_start = std::time::Instant::now();
    let mut db = match TrackedDb::open(station_path, true) {
        Ok(db) => db,
        Err(e) => {
            return Err(format!("Failed to open DB {}: {}", station_path, e));
        }
    };
    let open_elapsed = station_start.elapsed();

    let mut total_stats = RollupStats::default();
    let cancelled = || cancel.load(Ordering::Relaxed);
    let mut combined_day_activity: Option<RollupActivity> = None;
    let mut combined_day_arrow_count: usize = 0;

    // Self-healing: scan the DB's Current metas (cheap seek-only pass) so any
    // bucket stranded by an earlier failed cycle is rolled up with this one.
    // Requires knowing the NEW live bucket - it legitimately has data without
    // meta mid-cycle (the boundary flush drains the mixed-bucket cache but
    // writes meta for the old accumulators only) and must never be touched.
    let mut hanging_by_layer: HashMap<Layer, Vec<(AccumulatorBucket, Accumulators)>> =
        HashMap::new();
    let mut all_dest_files: HashSet<String> = HashSet::new();
    let mut scan_elapsed = std::time::Duration::ZERO;
    if let Some(new_bucket) = new_current_bucket {
        let scan_start = std::time::Instant::now();
        if let Ok(mut p) = progress.lock() {
            p.phase = "scan".to_string();
        }
        if let Some(scan) = scan_station_db(&mut db, layers) {
            let old_bucket = accumulators.current.bucket;

            for (bucket, layer, acc) in scan.current_metas {
                if bucket != old_bucket && bucket != new_bucket {
                    hanging_by_layer.entry(layer).or_default().push((bucket, acc));
                } else if bucket == old_bucket
                    && (acc.day.bucket != accumulators.day.bucket
                        || acc.month.bucket != accumulators.month.bucket
                        || acc.year.bucket != accumulators.year.bucket
                        || acc.yearnz.bucket != accumulators.yearnz.bucket)
                {
                    // Current bucket ids encode (day_of_month << 7) | period and
                    // recur monthly. A live-bucket meta pointing at different
                    // destinations means a month-old stranded bucket has
                    // collided with the live one - its stale data is about to
                    // merge into the live rollup. Healing should keep hangs far
                    // younger than a month, so this firing means healing failed.
                    warn!(
                        "{}/{}: live current bucket {:04x} meta has stale destinations \
                         (meta day={:04x}/month={:04x} vs live day={:04x}/month={:04x}) - \
                         month-old stranded data is merging into the live rollup",
                        station_name, layer.name(), bucket.0,
                        acc.day.bucket.0, acc.month.bucket.0,
                        accumulators.day.bucket.0, accumulators.month.bucket.0
                    );
                }
            }

            // Purge Current-type leftovers with no usable meta (orphaned data
            // or unparseable meta - no destination provenance, can't be rolled
            // up). Destination-type anomalies are left to the startup scan.
            let to_purge: Vec<&(AccumulatorType, AccumulatorBucket, Layer, String)> =
                scan.to_purge.iter()
                    .filter(|(t, b, _, _)| {
                        *t == AccumulatorType::Current && *b != old_bucket && *b != new_bucket
                    })
                    .collect();
            if !to_purge.is_empty() {
                let desc: Vec<String> = to_purge.iter().map(|(_, _, _, d)| d.clone()).collect();
                let ranges: Vec<(String, String)> = to_purge.iter()
                    .map(|(t, b, l, _)| CoverageHeader::db_search_range_with_meta(*t, *b, *l))
                    .collect();
                // Permanent destruction of unrecoverable data - warn level,
                // matching the DROPPING path. purged == 0 means the delete
                // failed (error-logged by db) and the purge will repeat.
                let purged = db::delete_ranges(&mut db, &ranges);
                warn!("{}: purged {} keys from {} unrecoverable current accumulators: {}",
                    station_name, purged, to_purge.len(), desc.join(", "));
            }

            if !hanging_by_layer.is_empty() {
                // Destination files known to the DB, plus the live ones (which
                // may not exist in the DB yet) - used by the DROP check.
                all_dest_files = scan.dest_metas.into_iter().map(|(_, _, _, f)| f)
                    .chain(
                        [&accumulators.day, &accumulators.month, &accumulators.year, &accumulators.yearnz]
                            .iter()
                            .filter(|e| !e.file.is_empty())
                            .map(|e| e.file.clone()),
                    )
                    .collect();
            }
        } else {
            // Without the scan, stranded buckets stay invisible this cycle
            warn!("{}: accumulator meta scan failed - self-healing skipped this cycle", station_name);
        }
        scan_elapsed = scan_start.elapsed();
    }

    let mut layers_elapsed = std::time::Duration::ZERO;
    for layer in layers {
        if cancelled() {
            break;
        }
        if let Ok(mut p) = progress.lock() {
            p.layer = layer.name().to_string();
            p.phase = "rollup".to_string();
        }
        let layer_start = std::time::Instant::now();
        let hangs = hanging_by_layer.get(layer).map(|v| v.as_slice()).unwrap_or(&[]);
        let result = rollup_current_buckets(
            &mut db, station_name, *layer, Some(accumulators), hangs,
            &all_dest_files, valid_stations, is_global, station_meta,
            retired_accumulators, cancel, progress, "heal",
        );
        let layer_elapsed = layer_start.elapsed();
        layers_elapsed += layer_elapsed;
        match result {
            Ok((stats, day_activity, day_arrow_count)) => {
                if layer_elapsed.as_secs() >= 20 {
                    warn!("{}: layer {} took {:?} (read={}, written={}, deleted={}, arrow={})",
                        station_name, layer.name(), layer_elapsed,
                        stats.records_read, stats.records_written,
                        stats.records_deleted, stats.arrow_records);
                }
                total_stats.records_read += stats.records_read;
                total_stats.records_written += stats.records_written;
                total_stats.records_deleted += stats.records_deleted;
                total_stats.arrow_records += stats.arrow_records;
                total_stats.global_h3_counts.extend(stats.global_h3_counts);
                if let Some(act) = day_activity {
                    combined_day_activity = Some(act);
                }
                if let Some(count) = day_arrow_count {
                    combined_day_arrow_count = count;
                }
                if let Ok(mut p) = progress.lock() {
                    p.records_read = total_stats.records_read;
                    p.records_written = total_stats.records_written;
                    p.records_deleted = total_stats.records_deleted;
                    p.arrow_records = total_stats.arrow_records;
                }
            }
            Err(e) => {
                error!("Rollup failed for {}/{}: {}", station_name, layer.name(), e);
            }
        }
    }

    // Write per-station JSON for every active-station rollup (not gated by write_json so that
    // beaconActivity, stats, and arrowRecords are always current, not just on hourly writes).
    if !is_global && !cancelled() {
        if let Some(meta) = station_meta {
            let output_dir = crate::config::output_dir(station_name);
            write_station_json(
                &output_dir,
                station_name,
                meta,
                accumulators,
                combined_day_activity.as_ref(),
                combined_day_arrow_count,
            );
        }
    }

    // Always flush - ensures memtable is written to SSTables so next open
    // doesn't pay WAL replay cost (cheap if few/no layers completed)
    if let Ok(mut p) = progress.lock() {
        p.phase = "flush".to_string();
    }
    let flush_start = std::time::Instant::now();
    db.flush().map_err(|e| format!("flush failed for {}: {}", station_name, e))?;
    let flush_elapsed = flush_start.elapsed();

    // Skip compaction on cancel - it's expensive and not needed for correctness
    let compact_elapsed;
    if !cancelled() {
        if let Ok(mut p) = progress.lock() {
            p.phase = "compact".to_string();
        }
        // Log the level structure for the global DB: compact_range only cascades
        // down to the highest non-empty level among L1..L5, so if L5 is empty here
        // the L5->L6 merge (the pass that reclaims the bottom level) never runs.
        let fmt_levels = |db: &TrackedDb| -> String {
            db.level_file_sizes().iter()
                .map(|(l, f, b)| format!("L{}:{}f/{}MB", l, f, b / 1_000_000))
                .collect::<Vec<_>>().join(" ")
        };
        if is_global {
            info!("{}: pre-compact levels: {}", station_name, fmt_levels(&db));
        }
        let compact_start = std::time::Instant::now();
        db.compact_range(b"!", b"~").map_err(|e| format!("compact failed for {}: {}", station_name, e))?;
        db.flush().map_err(|e| format!("flush after compact failed for {}: {}", station_name, e))?;
        compact_elapsed = compact_start.elapsed();
        if is_global {
            info!("{}: post-compact levels: {} ({:?})", station_name, fmt_levels(&db), compact_elapsed);
        }
    } else {
        compact_elapsed = std::time::Duration::ZERO;
    }

    let total_elapsed = station_start.elapsed();
    if total_elapsed.as_secs() >= 40 {
        warn!("{}: slow station rollup {:?} - open={:?}, scan={:?}, layers={:?}, flush={:?}, compact={:?}, \
               read={}, written={}, deleted={}, arrow={}",
            station_name, total_elapsed, open_elapsed, scan_elapsed, layers_elapsed,
            flush_elapsed, compact_elapsed,
            total_stats.records_read, total_stats.records_written,
            total_stats.records_deleted, total_stats.arrow_records);
    }
    Ok(total_stats)
}

/// Pre-read, normalized current-accumulator data feeding a single rollup merge:
/// records sorted by H3 (duplicate H3s already merged), plus the DB keys to
/// delete once the merge commits. Allows multiple hanging current buckets that
/// share a destination set to be rolled up in one destination walk (startup).
struct CurrentSource {
    /// (h3 hex, record), sorted by H3, duplicate H3s pre-merged
    records: Vec<(String, CoverageRecord)>,
    /// raw current data keys to delete on commit
    delete_data_keys: Vec<String>,
    /// current meta keys to delete on commit
    delete_meta_keys: Vec<String>,
    /// raw record count read from the DB (before any dedup)
    records_read: usize,
    /// earliest period start across the source buckets
    period_start: Epoch,
    /// latest period end across the source buckets
    period_end: Epoch,
}

/// Per-layer rollup within an already-open DB.
/// Single-bucket convenience wrapper over rollup_current_buckets (test-only;
/// production paths go through rollup_current_buckets with hanging detection).
/// Returns (stats, day_activity) where day_activity is the combined-layer Day RollupActivity
/// (if this layer is Combined and has a Day accumulator).
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn rollup_station_layer(
    db: &mut rusty_leveldb::DB,
    _station_path: &str,
    station_name: &str,
    accumulators: &Accumulators,
    layer: Layer,
    _layer_suffix: &str,
    valid_stations: Option<&HashSet<StationId>>,
    is_global: bool,
    station_meta: Option<&crate::station::StationDetails>,
    retired_accumulators: &[(AccumulatorType, AccumulatorBucket)],
    cancel: &AtomicBool,
    progress: &std::sync::Mutex<RollupProgress>,
) -> Result<(RollupStats, Option<RollupActivity>, Option<usize>), String> {
    rollup_current_buckets(
        db, station_name, layer, Some(accumulators), &[], &HashSet::new(),
        valid_stations, is_global, station_meta, retired_accumulators,
        cancel, progress, "rollup",
    )
}

/// Roll up a layer's current buckets - the live one being retired (if any)
/// plus hanging buckets recovered from their metas - grouped by destination
/// set so each group needs only one destination walk + arrow rewrite.
///
/// `active`: the live accumulators whose current bucket is being retired this
/// cycle. Its destinations are authoritative (no stored meta needed), it is
/// exempt from the missing-destination DROP check, and `retired_accumulators`
/// are purged with its group. Hanging buckets whose destinations match the
/// active set join its group and are healed in the same walk.
///
/// Used by the periodic rollup (active + hanging) and the startup mop-up
/// (hanging only).
#[allow(clippy::too_many_arguments)]
fn rollup_current_buckets(
    db: &mut rusty_leveldb::DB,
    station_name: &str,
    layer: Layer,
    active: Option<&Accumulators>,
    hanging: &[(AccumulatorBucket, Accumulators)],
    all_dest_files: &HashSet<String>,
    valid_stations: Option<&HashSet<StationId>>,
    is_global: bool,
    station_meta: Option<&crate::station::StationDetails>,
    retired_accumulators: &[(AccumulatorType, AccumulatorBucket)],
    cancel: &AtomicBool,
    progress: &std::sync::Mutex<RollupProgress>,
    context: &str,
) -> Result<(RollupStats, Option<RollupActivity>, Option<usize>), String> {
    let dest_key = |acc: &Accumulators| {
        format!(
            "{:04x}/{}|{:04x}/{}|{:04x}/{}|{:04x}/{}",
            acc.day.bucket.0, acc.day.file,
            acc.month.bucket.0, acc.month.file,
            acc.year.bucket.0, acc.year.file,
            acc.yearnz.bucket.0, acc.yearnz.file,
        )
    };

    // Group members by destination set. The active bucket (if any) anchors
    // its group and supplies the descriptor; hanging buckets join it when
    // their destinations match, otherwise the newest member describes the group.
    struct Group {
        descriptor: Accumulators,
        members: Vec<(AccumulatorBucket, Epoch)>, // (bucket, effective_start)
        has_active: bool,
    }
    let mut groups: HashMap<String, Group> = HashMap::new();
    if let Some(acc) = active {
        groups.insert(dest_key(acc), Group {
            descriptor: acc.clone(),
            members: vec![(acc.current.bucket, acc.current.effective_start)],
            has_active: true,
        });
    }
    for (bucket, acc) in hanging {
        let entry = groups.entry(dest_key(acc)).or_insert_with(|| Group {
            descriptor: acc.clone(),
            members: Vec::new(),
            has_active: false,
        });
        entry.members.push((*bucket, acc.current.effective_start));
        if !entry.has_active
            && acc.current.effective_start.0 > entry.descriptor.current.effective_start.0
        {
            entry.descriptor = acc.clone();
        }
    }

    let mut total = RollupStats::default();
    let mut day_activity: Option<RollupActivity> = None;
    let mut day_arrow_count: Option<usize> = None;

    'groups: for group in groups.values() {
        // Shutdown: leave remaining groups intact - they'll be re-detected
        // and rolled up on a later pass.
        if is_shutdown() || cancel.load(Ordering::Relaxed) {
            break;
        }

        let descriptor = &group.descriptor;
        let (_, dest_files) = descriptor.describe();
        let buckets_desc: Vec<String> = group.members.iter()
            .map(|(b, _)| format!("{:04x}", b.0))
            .collect();

        // Missing-destination check for pure-hang groups: a destination whose
        // file isn't known would have its complete arrow files on disk
        // overwritten by a partial rollup. The active group is exempt - its
        // destinations are the live accumulators.
        if !group.has_active {
            let missing: Vec<&str> = [
                ("day", &descriptor.day, AccumulatorType::Day),
                ("month", &descriptor.month, AccumulatorType::Month),
                ("year", &descriptor.year, AccumulatorType::Year),
                ("yearnz", &descriptor.yearnz, AccumulatorType::YearNz),
            ].iter()
                .filter(|(_, entry, acc_type)| {
                    !entry.file.is_empty()
                        && crate::layers::should_produce(layer, *acc_type)
                        && !all_dest_files.contains(&entry.file)
                })
                .map(|(name, _, _)| *name)
                .collect();

            if !missing.is_empty() {
                warn!(
                    "{}: DROPPING {} hanging current accumulator(s) [{}] for {} [{}]: {} missing -\
                     rolling up would overwrite complete arrow files on disk",
                    station_name, group.members.len(), buckets_desc.join(","), dest_files,
                    layer.name(), missing.join(",")
                );
                // Delete the current meta keys AND data so they won't hang again
                let mut purged = 0usize;
                for (bucket, _) in &group.members {
                    let (start, end) = CoverageHeader::db_search_range_with_meta(
                        AccumulatorType::Current, *bucket, layer,
                    );
                    purged += db::delete_range(db, &start, &end);
                }
                // purged == 0 with this DROPPING warn repeating each cycle
                // means the deletes aren't sticking (see error log from db).
                info!(
                    "{}/{}: purged {} keys for dropped current bucket(s) [{}]",
                    station_name, layer.name(), purged, buckets_desc.join(",")
                );
                continue;
            }
        }

        // Read every member's current records and merge them by H3. Keys
        // within a single bucket range are already H3-sorted and unique, so
        // a single-member group (the normal periodic path) collects straight
        // into a Vec; only multi-member groups need the BTreeMap to merge
        // duplicate H3s and keep the combined records H3-sorted.
        let single_member = group.members.len() == 1;
        let mut sorted: Vec<(String, CoverageRecord)> = Vec::new();
        let mut merged: std::collections::BTreeMap<String, CoverageRecord> =
            std::collections::BTreeMap::new();
        let mut delete_data_keys: Vec<String> = Vec::new();
        let mut delete_meta_keys: Vec<String> = Vec::new();
        let mut records_read = 0usize;
        let mut unparseable = 0usize;
        let mut period_start = u32::MAX;
        let mut period_end = 0u32;

        for (bucket, effective_start) in &group.members {
            let (start, end) = CoverageHeader::db_search_range(
                AccumulatorType::Current, *bucket, layer,
            );
            // A failed read (None) is indistinguishable from an empty bucket;
            // acting on it would purge the meta and orphan the unread data.
            let Some(records) = db::read_range(db, &start, &end, Some(cancel)) else {
                warn!(
                    "{}/{}: read of current bucket {:04x} failed - leaving group [{}] untouched for retry",
                    station_name, layer.name(), bucket.0, buckets_desc.join(",")
                );
                continue 'groups;
            };
            if is_shutdown() || cancel.load(Ordering::Relaxed) {
                // Possibly-partial read: leave this group untouched.
                continue 'groups;
            }
            records_read += records.len();
            delete_data_keys.reserve(records.len());
            if single_member {
                sorted.reserve(records.len());
            }
            for (key, value) in records {
                if let (Some(h3), Some(record)) =
                    (extract_h3_from_db_key(&key), CoverageRecord::from_bytes(&value))
                {
                    if single_member {
                        sorted.push((h3, record));
                    } else {
                        merged.entry(h3)
                            .and_modify(|existing| {
                                if let Some(m) = existing.rollup(&record, valid_stations) {
                                    *existing = m;
                                }
                            })
                            .or_insert(record);
                    }
                } else {
                    unparseable += 1;
                }
                delete_data_keys.push(key);
            }
            delete_meta_keys.push(CoverageHeader::accumulator_meta(
                AccumulatorType::Current, *bucket, layer,
            ).db_key());
            let es = effective_start.0;
            period_start = period_start.min(es);
            period_end = period_end.max(es + (*ROLLUP_PERIOD_MINUTES * 60.0) as u32);
        }
        total.records_read += records_read;

        // Unparseable records are excluded from the merge but their keys are
        // still deleted with the bucket - corrupt data being destroyed must
        // be visible.
        if unparseable > 0 {
            warn!(
                "{}/{}: discarding {} of {} current record(s) as unparseable from bucket(s) [{}]",
                station_name, layer.name(), unparseable, records_read,
                buckets_desc.join(",")
            );
        }

        let merged_records: Vec<(String, CoverageRecord)> =
            if single_member { sorted } else { merged.into_iter().collect() };

        if merged_records.is_empty() {
            // Nothing mergeable (no traffic this period, meta-only hangs, or
            // unparseable records) - delete the member keys so stale metas
            // can't linger as hanging accumulators.
            let key_count = delete_data_keys.len() + delete_meta_keys.len();
            let mut batch = rusty_leveldb::WriteBatch::default();
            for key in delete_data_keys.iter().chain(delete_meta_keys.iter()) {
                batch.delete(key.as_bytes());
            }
            if let Err(e) = db.write(batch, true) {
                error!(
                    "{}/{}: failed to purge {} keys for empty current bucket(s) [{}]: {}",
                    station_name, layer.name(), key_count, buckets_desc.join(","), e
                );
            } else if !group.has_active || group.members.len() > 1 {
                // Hanging buckets resolving as empty would otherwise vanish
                // without trace; the plain empty active bucket stays quiet.
                info!(
                    "{}/{}: purged {} keys for {} empty hanging current bucket(s) [{}]",
                    station_name, layer.name(), key_count, group.members.len(),
                    buckets_desc.join(",")
                );
            }
            continue;
        }

        // Per-group log only when hanging buckets are involved - the plain
        // active-bucket rollup is the normal path and stays quiet.
        if !group.has_active || group.members.len() > 1 {
            info!(
                "{}/{}: rolling up {} current accumulator(s) [{}] ({} records) into {}",
                station_name, layer.name(), group.members.len(), buckets_desc.join(","),
                merged_records.len(), dest_files
            );
        }

        let source = CurrentSource {
            records: merged_records,
            delete_data_keys,
            delete_meta_keys,
            records_read,
            period_start: Epoch(period_start),
            period_end: Epoch(period_end),
        };

        // Retired-accumulator purging belongs to the live rotation only
        let retired = if group.has_active { retired_accumulators } else { &[] };

        let group_t0 = std::time::Instant::now();
        match rollup_layer_core(
            db, station_name, descriptor, layer, layer.file_suffix(),
            valid_stations, is_global, station_meta, retired,
            cancel, progress, &source, group_t0, std::time::Duration::ZERO,
        ) {
            Ok((stats, da, dac)) => {
                if group.has_active {
                    // Station JSON fields come from the live rotation's group
                    day_activity = da;
                    day_arrow_count = dac;
                }
                // Completion mirrors the "rolling up" intent line: logged
                // whenever hanging buckets were involved, so draining a hang
                // is confirmable from the log rather than by its absence.
                if !group.has_active || group.members.len() > 1 {
                    info!(
                        "{}: {} rollup complete {} [{}] - {} written, {} arrow, {} deleted",
                        station_name, context, layer.name(), buckets_desc.join(","),
                        stats.records_written, stats.arrow_records, stats.records_deleted
                    );
                }
                total.records_written += stats.records_written;
                total.records_deleted += stats.records_deleted;
                total.arrow_records += stats.arrow_records;
                total.global_h3_counts.extend(stats.global_h3_counts);
            }
            Err(e) => {
                error!(
                    "{}: {} rollup failed for {} [{}]: {}",
                    station_name, context, layer.name(), buckets_desc.join(","), e
                );
            }
        }
    }

    Ok((total, day_activity, day_arrow_count))
}

/// Core of the per-layer rollup: merge a CurrentSource into the destination
/// accumulators of `accumulators`, write arrow output, then commit the DB batch.
///
/// Ordering is deliberate (crash consistency): arrow files are written and
/// atomically renamed BEFORE the destructive WriteBatch that deletes the
/// current accumulator. An abort or crash anywhere before the commit leaves
/// the current accumulator intact, so the next rollup (or startup mop-up)
/// reproduces the identical merge and rewrites the same arrow files.
/// "DB cleared but arrow never written" cannot happen.
#[allow(clippy::too_many_arguments)]
fn rollup_layer_core(
    db: &mut rusty_leveldb::DB,
    station_name: &str,
    accumulators: &Accumulators,
    layer: Layer,
    layer_suffix: &str,
    valid_stations: Option<&HashSet<StationId>>,
    is_global: bool,
    station_meta: Option<&crate::station::StationDetails>,
    retired_accumulators: &[(AccumulatorType, AccumulatorBucket)],
    cancel: &AtomicBool,
    progress: &std::sync::Mutex<RollupProgress>,
    source: &CurrentSource,
    layer_t0: std::time::Instant,
    t_read_current: std::time::Duration,
) -> Result<(RollupStats, Option<RollupActivity>, Option<usize>), String> {
    let mut stats = RollupStats { records_read: source.records_read, ..Default::default() };

    // Helper to update progress phase + detail
    let set_phase = |phase: &str, detail: &str| {
        if let Ok(mut p) = progress.lock() {
            p.phase = phase.to_string();
            p.detail = detail.to_string();
        }
    };

    // Set up rollup destination accumulators
    let dest_entries: Vec<(AccumulatorType, &AccumulatorEntry)> = vec![
        (AccumulatorType::Day, &accumulators.day),
        (AccumulatorType::Month, &accumulators.month),
        (AccumulatorType::Year, &accumulators.year),
        (AccumulatorType::YearNz, &accumulators.yearnz),
    ]
    .into_iter()
    .filter(|(acc_type, _)| crate::layers::should_produce(layer, *acc_type))
    .collect();

    let mut destinations: Vec<RollupAccumulator> = Vec::with_capacity(dest_entries.len());
    for (acc_type, entry) in &dest_entries {
        let (start, end) = CoverageHeader::db_search_range(*acc_type, entry.bucket, layer);
        let meta_key = CoverageHeader::accumulator_meta(*acc_type, entry.bucket, layer).db_key();
        let activity = load_activity_from_db(db, &meta_key);

        destinations.push(RollupAccumulator {
            acc_type: *acc_type,
            bucket: entry.bucket,
            file: entry.file.clone(),
            range_start: start,
            range_end: end,
            arrow_station_rows: Vec::new(),
            arrow_global_rows: Vec::new(),
            activity,
            dest_record_count: 0,
        });
    }

    // Batch of DB put/delete operations to apply at the end
    let mut puts: Vec<(String, Vec<u8>)> = Vec::new();
    let mut deletes: Vec<String> = Vec::new();

    // Walk the current accumulator and merge into each destination.
    // Everything in this phase is in-memory (puts/deletes/arrow rows are
    // buffered) - any early return here is non-destructive.
    set_phase("merge", &format!("{}cur", source.records.len()));
    let t_merge_start = std::time::Instant::now();
    let h3source = source.records.len() as u32;
    let now = Epoch(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32);

    // Non-destructive bail-out for shutdown mid-merge: nothing was committed,
    // so zero the counters that only reflect buffered (uncommitted) work.
    macro_rules! cancel_bail {
        () => {
            if cancel.load(Ordering::Relaxed) {
                stats.records_written = 0;
                stats.global_h3_counts.clear();
                return Ok((stats, None, None));
            }
        };
    }

    // Process each destination accumulator one at a time, iterating the DB directly
    for dest in &mut destinations {
        let mut iter = db.new_iter().map_err(|e| format!("new_iter failed: {}", e))?;
        iter.seek(dest.range_start.as_bytes());
        let range_end = dest.range_end.clone();

        // Read current iterator position if within range
        fn read_iter(iter: &rusty_leveldb::DBIterator, end: &[u8]) -> Option<(String, Vec<u8>)> {
            iter.current().and_then(|(k, v)| {
                if k.as_ref() >= end { return None; }
                std::str::from_utf8(&k).ok().map(|s| (s.to_string(), v.to_vec()))
            })
        }

        let mut iter_entry = read_iter(&iter, range_end.as_bytes());

        for (current_h3, current_record) in &source.records {
            cancel_bail!();

            // Emit destination records whose H3 is before the current H3
            while let Some((ref dest_key, ref dest_value)) = iter_entry {
                cancel_bail!();
                let dest_h3 = match extract_h3_from_db_key(dest_key) {
                    Some(h3) => h3,
                    None => { iter.advance(); iter_entry = read_iter(&iter, range_end.as_bytes()); continue; }
                };
                if dest_h3.as_str() >= current_h3.as_str() { break; }

                dest.dest_record_count += 1;
                emit_dest_record(dest_key, dest_value, dest, is_global, valid_stations, &mut puts, &mut deletes);
                iter.advance();
                iter_entry = read_iter(&iter, range_end.as_bytes());
            }

            // Check if destination has a matching H3
            let dest_key_for_h3 = make_dest_key(dest.acc_type, dest.bucket, layer, current_h3);

            let merged = if let Some((ref dest_key, ref dest_value)) = iter_entry {
                let matches = extract_h3_from_db_key(dest_key).as_deref() == Some(current_h3.as_str());
                if matches {
                    dest.dest_record_count += 1;
                    let dest_record = CoverageRecord::from_bytes(dest_value);
                    iter.advance();
                    iter_entry = read_iter(&iter, range_end.as_bytes());
                    match dest_record {
                        Some(dr) => dr.rollup(current_record, valid_stations),
                        None => Some(current_record.clone()),
                    }
                } else {
                    Some(current_record.clone())
                }
            } else {
                Some(current_record.clone())
            };

            if let Some(ref merged_record) = merged {
                puts.push((dest_key_for_h3.clone(), merged_record.to_bytes()));
                stats.records_written += 1;

                let (lo, hi) = H3Index(current_h3.clone()).split_long();
                if is_global {
                    dest.arrow_global_rows.push(merged_record.to_arrow_global(lo, hi));
                } else {
                    dest.arrow_station_rows.push(merged_record.to_arrow_station(lo, hi));
                }
            }
        }

        // Drain remaining destination records (past end of current)
        while let Some((ref dest_key, ref dest_value)) = iter_entry {
            cancel_bail!();
            dest.dest_record_count += 1;
            emit_dest_record(dest_key, dest_value, dest, is_global, valid_stations, &mut puts, &mut deletes);
            iter.advance();
            iter_entry = read_iter(&iter, range_end.as_bytes());
        }

        drop(iter);
        update_activity(&mut dest.activity, h3source, source.period_start, source.period_end, now);
    }

    let t_merge = t_merge_start.elapsed();

    // --- Arrow phase: write output files BEFORE any destructive DB mutation.
    // The files are written to .working and atomically renamed, so an abort
    // never damages the previous file; and because the current accumulator is
    // still in the DB, a crash from here until the batch commit is fully
    // recoverable (the next rollup/startup reproduces the identical merge).
    set_phase("arrow", "");
    let t_arrow_start = std::time::Instant::now();
    let output_dir = crate::config::output_dir(station_name);
    if let Err(e) = std::fs::create_dir_all(&output_dir) {
        return Err(format!("Failed to create output dir: {}", e));
    }

    // Extract the combined-layer day RollupActivity before writing (for per-station JSON)
    let day_activity = if layer == Layer::Combined {
        destinations.iter()
            .find(|d| d.acc_type == AccumulatorType::Day)
            .map(|d| d.activity.clone())
    } else {
        None
    };

    // Arrow count for the combined-layer Day dest, returned to caller so write_station_json
    // can include it. We skip write_metadata_json for this specific dest to avoid overwriting
    // the full station JSON (which includes beaconActivity, stats, etc.) with stripped metadata.
    let mut combined_day_arrow_count: Option<usize> = None;

    for dest in &destinations {
        // Shutdown mid-arrow: nothing destructive has happened yet. Files
        // already written this iteration get idempotently rewritten next time.
        cancel_bail!();

        if dest.file.is_empty() {
            continue;
        }

        let arrow_count = if is_global {
            write_arrow_global(
                &output_dir,
                station_name,
                dest.acc_type.name(),
                &dest.file,
                layer_suffix,
                &dest.arrow_global_rows,
            )?
        } else {
            write_arrow_station(
                &output_dir,
                station_name,
                dest.acc_type.name(),
                &dest.file,
                layer_suffix,
                &dest.arrow_station_rows,
            )?
        };
        stats.arrow_records += arrow_count;

        // Track per-layer per-accumulator H3 counts for global station
        if is_global {
            stats.global_h3_counts.push((
                layer.name().to_string(),
                dest.acc_type.name().to_string(),
                arrow_count,
            ));
        }

        // For the combined-layer Day dest on non-global stations, skip write_metadata_json:
        // write_station_json (called by the parent) writes the same filename with full details
        // including beaconActivity and stats. Return the arrow count to the parent instead.
        if !is_global && layer == Layer::Combined && dest.acc_type == AccumulatorType::Day {
            combined_day_arrow_count = Some(arrow_count);
            continue;
        }

        // Write metadata JSON (includes activity)
        write_metadata_json(
            &output_dir,
            station_name,
            dest.acc_type.name(),
            &dest.file,
            layer_suffix,
            station_meta,
            arrow_count,
            Some(&dest.activity),
        );
    }

    let t_arrow = t_arrow_start.elapsed();

    // --- Destructive phase: single WriteBatch for all DB mutations.
    // Arrow output is durably on disk; from here the merge is committed.
    let mut batch = rusty_leveldb::WriteBatch::default();

    // Puts: merged destination records
    for (key, value) in &puts {
        batch.put(key.as_bytes(), value);
    }

    // Update accumulator metadata (matching saveAccumulatorMetadata in TypeScript)
    let acc_json = serde_json::to_value(accumulators).unwrap_or_default();
    for dest in &destinations {
        let meta_key = CoverageHeader::accumulator_meta(dest.acc_type, dest.bucket, layer).db_key();
        let existing = db.get(meta_key.as_bytes());
        let mut meta_bytes = crate::db::build_accumulator_meta(
            existing.as_deref(),
            &acc_json,
            accumulators.current.bucket.0,
        );
        // Merge activity into the meta
        if let Ok(mut meta) = serde_json::from_slice::<serde_json::Value>(&meta_bytes) {
            meta["activity"] = serde_json::to_value(&dest.activity).unwrap_or_default();
            if let Ok(bytes) = serde_json::to_vec(&meta) {
                meta_bytes = bytes;
            }
        }
        batch.put(meta_key.as_bytes(), &meta_bytes);
    }

    // Deletes: station-filtered records that were emptied or updated
    for key in &deletes {
        batch.delete(key.as_bytes());
    }

    // Delete the current accumulator records we already read, and their meta keys
    for key in &source.delete_data_keys {
        batch.delete(key.as_bytes());
    }
    for key in &source.delete_meta_keys {
        batch.delete(key.as_bytes());
    }
    stats.records_deleted = source.delete_data_keys.len();

    // Last chance to bail non-destructively: after this the batch commits.
    cancel_bail!();

    set_phase("write_batch", &format!("{}puts/{}dels", puts.len(), deletes.len() + source.delete_data_keys.len()));
    let t_write_start = std::time::Instant::now();
    db.write(batch, true).map_err(|e| format!("write batch failed for {}: {}", station_name, e))?;
    let t_write = t_write_start.elapsed();

    // Purge retired accumulators (matching TypeScript rollupdatabase.ts:407-416).
    // When a bucket changes (e.g. day rolls over), purge old bucket's data and meta.
    set_phase("purge", &format!("{} retired", retired_accumulators.len()));
    let t_purge_start = std::time::Instant::now();
    for (acc_type, old_bucket) in retired_accumulators {
        let (start, end) = CoverageHeader::db_search_range_with_meta(*acc_type, *old_bucket, layer);
        db::delete_range(db, &start, &end);
    }
    let t_purge = t_purge_start.elapsed();

    let layer_total = layer_t0.elapsed();
    if layer_total.as_secs() >= 20 {
        let dest_counts: Vec<(&str, usize)> = destinations.iter()
            .map(|d| (d.acc_type.name(), d.dest_record_count))
            .collect();
        warn!("{}/{}: slow layer {:?} - read_current={}recs/{:?}, \
               merge={:?}(dest={:?}), arrow={:?}({}recs), write={:?}({}puts/{}dels), purge={:?}",
            station_name, layer.name(), layer_total,
            source.records_read, t_read_current,
            t_merge, dest_counts,
            t_arrow, stats.arrow_records,
            t_write, puts.len(), deletes.len() + source.delete_data_keys.len(),
            t_purge);
    }

    Ok((stats, day_activity, combined_day_arrow_count))
}

/// Emit a destination record to arrow output, optionally filtering stations.
/// Pushes DB mutations to puts/deletes if station filtering changed the record.
fn emit_dest_record(
    key: &str,
    value: &[u8],
    dest: &mut RollupAccumulator,
    is_global: bool,
    valid_stations: Option<&HashSet<StationId>>,
    puts: &mut Vec<(String, Vec<u8>)>,
    deletes: &mut Vec<String>,
) {
    let record = match CoverageRecord::from_bytes(value) {
        Some(r) => r,
        None => return,
    };

    let h3 = match extract_h3_from_db_key(key) {
        Some(h3) => h3,
        None => return,
    };

    if let Some(valid) = valid_stations {
        match record.remove_invalid_stations(valid) {
            Some(filtered) => {
                let filtered_bytes = filtered.to_bytes();
                let changed = filtered_bytes.as_slice() != value;
                let (lo, hi) = H3Index(h3).split_long();
                if is_global {
                    dest.arrow_global_rows.push(filtered.to_arrow_global(lo, hi));
                } else {
                    dest.arrow_station_rows.push(filtered.to_arrow_station(lo, hi));
                }
                if changed {
                    puts.push((key.to_string(), filtered_bytes));
                }
            }
            None => {
                // All stations removed - delete from DB
                deletes.push(key.to_string());
            }
        }
    } else {
        let (lo, hi) = H3Index(h3).split_long();
        if is_global {
            dest.arrow_global_rows.push(record.to_arrow_global(lo, hi));
        } else {
            dest.arrow_station_rows.push(record.to_arrow_station(lo, hi));
        }
    }
}

/// Extract H3 index string from a db key like "c/0042/8828308283fffff"
fn extract_h3_from_db_key(key: &str) -> Option<String> {
    key.rsplit('/').next().map(|s| s.to_string())
}

/// Build a destination db key
fn make_dest_key(
    acc_type: AccumulatorType,
    bucket: AccumulatorBucket,
    layer: Layer,
    h3: &str,
) -> String {
    let header = CoverageHeader::new(
        StationId(0),
        acc_type,
        bucket,
        H3Index(h3.to_string()),
        layer,
    );
    header.db_key()
}

// ---- Activity persistence ----

/// Load existing activity from a DB meta key using an open DB handle.
fn load_activity_from_db(db: &mut rusty_leveldb::DB, meta_key: &str) -> RollupActivity {
    if let Some(value) = db.get(meta_key.as_bytes()) {
        if let Ok(meta) = serde_json::from_slice::<serde_json::Value>(&value) {
            if let Some(activity_val) = meta.get("activity") {
                if let Ok(activity) = serde_json::from_value::<RollupActivity>(activity_val.clone()) {
                    return activity;
                }
            }
        }
    }
    RollupActivity::default()
}

// ---- Arrow writing ----

fn station_schema() -> Schema {
    Schema::new(vec![
        Field::new("h3lo", DataType::UInt32, false),
        Field::new("h3hi", DataType::UInt32, false),
        Field::new("minAgl", DataType::UInt16, false),
        Field::new("minAlt", DataType::UInt16, false),
        Field::new("minAltSig", DataType::UInt8, false),
        Field::new("maxSig", DataType::UInt8, false),
        Field::new("avgSig", DataType::UInt8, false),
        Field::new("avgCrc", DataType::UInt8, false),
        Field::new("count", DataType::UInt32, false),
        Field::new("avgGap", DataType::UInt8, false),
    ])
}

fn global_schema() -> Schema {
    Schema::new(vec![
        Field::new("h3lo", DataType::UInt32, false),
        Field::new("h3hi", DataType::UInt32, false),
        Field::new("minAgl", DataType::UInt16, false),
        Field::new("minAlt", DataType::UInt16, false),
        Field::new("minAltSig", DataType::UInt8, false),
        Field::new("maxSig", DataType::UInt8, false),
        Field::new("avgSig", DataType::UInt8, false),
        Field::new("avgCrc", DataType::UInt8, false),
        Field::new("count", DataType::UInt32, false),
        Field::new("avgGap", DataType::UInt8, false),
        Field::new("stations", DataType::Utf8, false),
        Field::new("expectedGap", DataType::UInt8, false),
        Field::new("numStations", DataType::UInt8, false),
    ])
}

fn write_arrow_station(
    output_dir: &str,
    station_name: &str,
    acc_type: &str,
    file_id: &str,
    layer_suffix: &str,
    rows: &[ArrowStation],
) -> Result<usize, String> {
    if rows.is_empty() {
        return Ok(0);
    }

    let schema = std::sync::Arc::new(station_schema());
    let columns: Vec<ArrayRef> = vec![
        std::sync::Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.h3lo))),
        std::sync::Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.h3hi))),
        std::sync::Arc::new(UInt16Array::from_iter_values(rows.iter().map(|r| r.min_agl))),
        std::sync::Arc::new(UInt16Array::from_iter_values(rows.iter().map(|r| r.min_alt))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.min_alt_sig))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.max_sig))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.avg_sig))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.avg_crc))),
        std::sync::Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.count))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.avg_gap))),
    ];

    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| format!("RecordBatch error: {}", e))?;

    write_arrow_file(output_dir, station_name, acc_type, file_id, layer_suffix, &schema, &[batch])?;
    Ok(rows.len())
}

fn write_arrow_global(
    output_dir: &str,
    station_name: &str,
    acc_type: &str,
    file_id: &str,
    layer_suffix: &str,
    rows: &[ArrowGlobal],
) -> Result<usize, String> {
    if rows.is_empty() {
        return Ok(0);
    }

    let schema = std::sync::Arc::new(global_schema());
    let columns: Vec<ArrayRef> = vec![
        std::sync::Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.h3lo))),
        std::sync::Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.h3hi))),
        std::sync::Arc::new(UInt16Array::from_iter_values(rows.iter().map(|r| r.min_agl))),
        std::sync::Arc::new(UInt16Array::from_iter_values(rows.iter().map(|r| r.min_alt))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.min_alt_sig))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.max_sig))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.avg_sig))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.avg_crc))),
        std::sync::Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.count))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.avg_gap))),
        std::sync::Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.stations.as_str()))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.expected_gap))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.num_stations))),
    ];

    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| format!("RecordBatch error: {}", e))?;

    write_arrow_file(output_dir, station_name, acc_type, file_id, layer_suffix, &schema, &[batch])?;
    Ok(rows.len())
}

fn write_arrow_file(
    output_dir: &str,
    station_name: &str,
    acc_type: &str,
    file_id: &str,
    layer_suffix: &str,
    schema: &std::sync::Arc<Schema>,
    batches: &[RecordBatch],
) -> Result<(), String> {
    let base_name = format!("{}.{}.{}{}", station_name, acc_type, file_id, layer_suffix);

    // Write gzip-compressed .arrow.gz
    let gz_working = format!("{}/{}.arrow.gz.working", output_dir, base_name);
    let gz_final = format!("{}/{}.arrow.gz", output_dir, base_name);
    {
        let file = std::fs::File::create(&gz_working)
            .map_err(|e| format!("Failed to create {}: {}", gz_working, e))?;
        let encoder = GzEncoder::new(file, Compression::default());
        let mut writer = StreamWriter::try_new(encoder, schema)
            .map_err(|e| format!("StreamWriter error: {}", e))?;
        for batch in batches {
            writer.write(batch).map_err(|e| format!("Write error: {}", e))?;
        }
        writer.finish().map_err(|e| format!("Finish error: {}", e))?;
    }

    // Shrink guard: accumulator outputs are monotonic - day/month/year files
    // only ever gain coverage, so a replacement smaller than what is already on
    // disk can only be a partial/orphaned rollup clobbering a complete file
    // (e.g. a hanging current drained into an already-closed period). Refuse the
    // swap, keep the larger existing file, and skip the uncompressed twin too so
    // the two stay in sync. (The one legitimate shrink - global station-validity
    // cleanup - is intentionally blocked by this invariant.)
    if let Ok(existing) = std::fs::metadata(&gz_final) {
        let new_size = std::fs::metadata(&gz_working).map(|m| m.len()).unwrap_or(0);
        if new_size < existing.len() {
            // Keep the rejected partial on disk (as .rejected.<epoch>, not
            // .working which the next write truncates, and timestamped so repeat
            // rejections don't clobber each other) for inspection/recovery.
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let rejected = format!("{}/{}.arrow.gz.rejected.{}", output_dir, base_name, ts);
            let kept = std::fs::rename(&gz_working, &rejected).is_ok();
            warn!(
                "{}: REFUSING arrow shrink {}.arrow.gz: new {} bytes < existing {} bytes \
                 (partial/orphaned rollup?) - keeping existing file{}",
                station_name, base_name, new_size, existing.len(),
                if kept { ", partial saved as .rejected" } else { "" }
            );
            return Ok(());
        }
    }

    std::fs::rename(&gz_working, &gz_final)
        .map_err(|e| format!("Rename error: {}", e))?;

    // Create symlink for latest: station.type.layer.arrow.gz -> station.type.file.layer.arrow.gz
    symlink_atomic(
        &format!("{}.arrow.gz", base_name),
        &format!("{}/{}.{}{}.arrow.gz", output_dir, station_name, acc_type, layer_suffix),
    );

    // Optionally write uncompressed
    if *UNCOMPRESSED_ARROW_FILES {
        let raw_working = format!("{}/{}.arrow.working", output_dir, base_name);
        let raw_final = format!("{}/{}.arrow", output_dir, base_name);
        {
            let file = std::fs::File::create(&raw_working)
                .map_err(|e| format!("Failed to create {}: {}", raw_working, e))?;
            let mut writer = StreamWriter::try_new(file, schema)
                .map_err(|e| format!("StreamWriter error: {}", e))?;
            for batch in batches {
                writer.write(batch).map_err(|e| format!("Write error: {}", e))?;
            }
            writer.finish().map_err(|e| format!("Finish error: {}", e))?;
        }
        std::fs::rename(&raw_working, &raw_final)
            .map_err(|e| format!("Rename error: {}", e))?;

        symlink_atomic(
            &format!("{}.arrow", base_name),
            &format!("{}/{}.{}{}.arrow", output_dir, station_name, acc_type, layer_suffix),
        );
    }

    Ok(())
}

// NOTE: changes to output fields must be reflected in docs/STATIONS.md and docs/STATION.md
/// Write per-station JSON containing station details, beacon bitvector, uptime, layers, and activity.
/// Written once per rollup cycle for non-global stations with traffic.
fn write_station_json(
    output_dir: &str,
    station_name: &str,
    station_meta: &crate::station::StationDetails,
    accumulators: &Accumulators,
    day_activity: Option<&RollupActivity>,
    arrow_records: usize,
) {
    use chrono::{Datelike, Timelike, Utc};

    let now = Utc::now();
    let today = format!("{:04}-{:02}-{:02}", now.year(), now.month(), now.day());
    let current_slot = now.hour() * 6 + now.minute() / 10 + 1; // 1-144
    let day_file = &accumulators.day.file;

    // Compute uptime from beacon activity.
    // If the beacon activity covers the day being rolled up and that day is already
    // complete (not today), use elapsed=144 so the uptime is accurate for the full day.
    let uptime = if station_meta.beacon_activity_date.as_deref() == Some(day_file.as_str())
        && day_file.as_str() != today.as_str()
    {
        station_meta.beacon_activity.as_deref().and_then(|hex| {
            let bits = crate::bitvec::hex_to_bitvec(hex)?;
            let set = crate::bitvec::popcount_144(&bits);
            Some(((set as f32 / 144.0) * 1000.0).round() / 10.0)
        })
    } else {
        crate::station::compute_uptime(
            &station_meta.beacon_activity,
            &station_meta.beacon_activity_date,
            &today,
            current_slot,
        )
    };

    // Build the JSON: serialize StationDetails then merge in extra fields
    let mut json = match serde_json::to_value(station_meta) {
        Ok(v) => v,
        Err(e) => {
            error!("Failed to serialize station details for {}: {}", station_name, e);
            return;
        }
    };

    let now_epoch = now.timestamp() as u64;
    if let Some(obj) = json.as_object_mut() {
        obj.insert("uptime".to_string(), serde_json::json!(uptime));
        obj.insert("arrowRecords".to_string(), serde_json::json!(arrow_records));
        obj.insert("exportedAt".to_string(), serde_json::json!(now_epoch));
        if let Some(act) = day_activity {
            obj.insert("activity".to_string(), serde_json::to_value(act).unwrap_or_default());
        }
    }

    let json_str = match serde_json::to_string_pretty(&json) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to format station JSON for {}: {}", station_name, e);
            return;
        }
    };

    // Write the dated day file
    let day_file = &accumulators.day.file;
    let base_name = format!("{}.day.{}", station_name, day_file);
    let json_path = format!("{}/{}.json", output_dir, base_name);
    if let Err(e) = std::fs::write(&json_path, &json_str) {
        error!("Failed to write station JSON {}: {}", json_path, e);
        return;
    }

    // Create symlinks (bare + day + month + year + yearnz)
    let target = format!("{}.json", base_name);
    create_accumulator_symlinks(output_dir, station_name, "json", accumulators, &target, true);
}

use crate::symlinks::{create_accumulator_symlinks, symlink_atomic};

/// Write per-accumulator metadata JSON
fn write_metadata_json(
    output_dir: &str,
    station_name: &str,
    acc_type: &str,
    file_id: &str,
    layer_suffix: &str,
    station_meta: Option<&crate::station::StationDetails>,
    arrow_count: usize,
    activity: Option<&RollupActivity>,
) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut meta = serde_json::json!({
        "station": station_meta.map(|s| &s.station),
        "lat": station_meta.and_then(|s| s.lat),
        "lng": station_meta.and_then(|s| s.lng),
        "lastOutputFile": now,
        "arrowRecords": arrow_count,
    });
    if let Some(act) = activity {
        meta["activity"] = serde_json::to_value(act).unwrap_or_default();
    }

    let base_name = format!("{}.{}.{}{}", station_name, acc_type, file_id, layer_suffix);
    let json_path = format!("{}/{}.json", output_dir, base_name);
    if let Err(e) = std::fs::write(&json_path, serde_json::to_string_pretty(&meta).unwrap_or_default()) {
        error!("Failed to write metadata {}: {}", json_path, e);
    }

    // Symlink for latest
    symlink_atomic(
        &format!("{}.json", base_name),
        &format!("{}/{}.{}{}.json", output_dir, station_name, acc_type, layer_suffix),
    );
}

/// Startup rollup: check for any unflushed accumulators from a previous run.
///
/// Scans each station DB for "current" accumulator meta keys. If the stored
/// accumulators match the expected buckets (current + all destinations), the
/// accumulator is still active and left in place. Otherwise, the hanging
/// current data is rolled up into day/month/year/yearnz.
/// Also purges orphaned data and old accumulators that don't match expected buckets.
/// Result of scanning a station DB's accumulator metas (seek-based, cheap -
/// only meta keys are read; data ranges are skipped over).
#[derive(Default)]
struct StationDbScan {
    /// Parsed Current metas: (bucket, layer, accumulators-from-meta).
    /// Unfiltered - the caller decides which are hanging.
    current_metas: Vec<(AccumulatorBucket, Layer, Accumulators)>,
    /// Anomalies: data-without-meta (orphaned) and unparseable metas.
    /// (type, bucket, layer, description)
    to_purge: Vec<(AccumulatorType, AccumulatorBucket, Layer, String)>,
    /// Destination (non-current) metas found: (type, bucket, layer, file)
    dest_metas: Vec<(AccumulatorType, AccumulatorBucket, Layer, String)>,
}

/// Scan a station DB for accumulator metas using a seeking iterator (like the
/// TypeScript implementation) - reads only meta keys, seeking past each
/// accumulator's data range. Current metas for layers not in `layers` are
/// skipped (left untouched). Shared by the startup mop-up and the per-cycle
/// self-healing pass.
fn scan_station_db(db: &mut rusty_leveldb::DB, layers: &[Layer]) -> Option<StationDbScan> {
    let mut scan = StationDbScan::default();

    let mut iter = db.new_iter().ok()?;
    iter.seek(&[]);

    while let Some((key_bytes, val_bytes)) = iter.current() {
        let key_str = match std::str::from_utf8(&key_bytes) {
            Ok(s) => s,
            Err(_) => { if !iter.advance() { break; } continue; }
        };

        let header = match CoverageHeader::from_db_key(key_str) {
            Some(h) => h,
            None => { if !iter.advance() { break; } continue; }
        };

        let acc_type = header.accumulator_type();
        let bucket = header.bucket();
        let layer = header.layer;

        // Calculate the end of this accumulator's range for seeking
        let (_, seek_end) = CoverageHeader::db_search_range(acc_type, bucket, layer);

        if !header.is_meta() {
            // Data entry without meta - orphaned, mark for purge
            scan.to_purge.push((acc_type, bucket, layer,
                format!("{}/{}/{:04x}(orphaned)", layer.name(), acc_type.name(), bucket.0)));
            iter.seek(seek_end.as_bytes());
            continue;
        }

        // Process meta entry
        if acc_type == AccumulatorType::Current {
            if layers.contains(&layer) {
                match serde_json::from_slice::<serde_json::Value>(&val_bytes)
                    .ok()
                    .and_then(|meta| parse_accumulators_from_meta(&meta))
                {
                    Some(acc) => {
                        scan.current_metas.push((bucket, layer, acc));
                    }
                    None => {
                        // Unparseable current meta: destinations are unknowable,
                        // so it can never be rolled up - purge like an orphan.
                        scan.to_purge.push((acc_type, bucket, layer,
                            format!("{}/current/{:04x}(invalid current meta)", layer.name(), bucket.0)));
                    }
                }
            }
        } else {
            let meta_ok = serde_json::from_slice::<serde_json::Value>(&val_bytes)
                .ok()
                .and_then(|meta| {
                    let type_name = acc_type.name();
                    meta.get("accumulators")
                        .and_then(|a| a.get(type_name))
                        .and_then(|e| e.get("file"))
                        .and_then(|f| f.as_str())
                        .filter(|f| !f.is_empty())
                        .map(|f| f.to_string())
                });

            match meta_ok {
                Some(file) => {
                    scan.dest_metas.push((acc_type, bucket, layer, file));
                }
                None => {
                    scan.to_purge.push((acc_type, bucket, layer,
                        format!("{}/{}/{:04x}(invalid meta)", layer.name(), acc_type.name(), bucket.0)));
                }
            }
        }

        // Seek past this accumulator's data range
        iter.seek(seek_end.as_bytes());
    }

    Some(scan)
}

pub async fn rollup_startup(
    storage: &Storage,
    station_manager: &StationManager,
    expected_accumulators: &Accumulators,
) {
    info!("Startup rollup: checking for unflushed accumulators...");

    // Defensive only: main awaits startup before spawning the rollup timer.
    let _rollup_guard = RollupGuard::try_acquire();
    if _rollup_guard.is_none() {
        warn!("startup rollup: another rollup appears to be in progress");
    }

    let all_station_details = station_manager.all_stations_with_global();

    let layers = crate::layers::rollup_layers(crate::config::ENABLED_LAYERS.as_ref());

    let total_stations = all_station_details.len();
    let startup_start = std::time::Instant::now();
    let max_concurrent = *crate::config::MAX_SIMULTANEOUS_ROLLUPS;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
    let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let mut tasks = Vec::new();

    for detail in &all_station_details {
        let station_name = detail.station.as_str().to_string();
        let station_path = storage.station_path(&station_name).to_string_lossy().to_string();
        if !std::path::Path::new(&station_path).exists() {
            completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            continue;
        }

        let layers = layers.clone();
        let all_details = all_station_details.clone();
        let completed = completed.clone();
        let total = total_stations;
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let expected = expected_accumulators.clone();
        let task_station_name = station_name.clone();
        let progress = Arc::new(std::sync::Mutex::new(RollupProgress::default()));
        let task_progress = progress.clone();
        let spawned_at = std::time::Instant::now();

        let handle = tokio::task::spawn_blocking(move || {
            let _permit = permit;

            let log_progress = |completed: &std::sync::atomic::AtomicUsize| {
                let done = completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                if total == 0 || done * 10 / total != (done - 1) * 10 / total {
                    let pct = if total > 0 { done * 100 / total } else { 100 };
                    let elapsed = startup_start.elapsed().as_secs_f64();
                    let speed = if elapsed > 0.0 { done as f64 / elapsed } else { 0.0 };
                    info!("startup:{}% [{}/{}] {:.0}s elapsed, {:.1}/s", pct, done, total, elapsed, speed);
                }
            };

            // Open DB once for all operations on this station
            let mut db = match TrackedDb::open(&station_path, true) {
                Ok(db) => db,
                Err(e) => {
                    error!("Startup rollup: failed to open DB {}: {}", station_path, e);
                    log_progress(&completed);
                    return (0, 0, 0, 0);
                }
            };

            // Migrate all legacy unprefixed keys to prefixed format before scanning
            let migrated = migrate_legacy_keys(&mut db);

            // Scan DB for accumulator metas (shared with the per-cycle
            // self-healing pass in rollup_station_all_layers)
            let Some(scan) = scan_station_db(&mut db, &layers) else {
                warn!("{}: accumulator meta scan failed - startup mop-up skipped for this station", station_name);
                log_progress(&completed);
                return (0, 0, 0, 0);
            };
            let mut to_purge = scan.to_purge;
            let all_accumulators = scan.dest_metas;

            // A current meta is hanging unless it is the still-active
            // accumulator (same current bucket AND same destination buckets -
            // the current bucket alone isn't unique, it encodes
            // (day_of_month << 7) | period, which repeats across months).
            let mut hanging_buckets: HashMap<(AccumulatorBucket, Layer), Accumulators> = HashMap::new();
            for (bucket, layer, acc) in scan.current_metas {
                let matches_expected = bucket == expected.current.bucket
                    && acc.day.bucket == expected.day.bucket
                    && acc.month.bucket == expected.month.bucket
                    && acc.year.bucket == expected.year.bucket
                    && acc.yearnz.bucket == expected.yearnz.bucket;

                if !matches_expected {
                    hanging_buckets.insert((bucket, layer), acc);
                }
            }

            if !hanging_buckets.is_empty() || !to_purge.is_empty() || migrated > 0 {
                let found: Vec<String> = all_accumulators.iter()
                    .map(|(t, b, l, f)| format!("{}/{}={:04x}({})", l.name(), t.name(), b.0, f))
                    .collect();
                let hanging: Vec<String> = hanging_buckets.keys()
                    .map(|(b, l)| format!("{}/{:04x}", l.name(), b.0))
                    .collect();
                let orphaned: Vec<String> = to_purge.iter()
                    .map(|(_, _, _, desc)| desc.clone())
                    .collect();
                info!(
                    "{}: scan: found=[{}] hanging=[{}] orphaned=[{}] migrated={} expected day={:04x} month={:04x} year={:04x} yearnz={:04x}",
                    station_name,
                    found.join(", "), hanging.join(", "), orphaned.join(", "),
                    migrated,
                    expected.day.bucket.0, expected.month.bucket.0,
                    expected.year.bucket.0, expected.yearnz.bucket.0
                );
            }

            // Purge old accumulators whose buckets don't match expected
            // (matching TypeScript rollupdatabase.ts:638-640)
            let expected_buckets: HashMap<(AccumulatorType, Layer), AccumulatorBucket> = {
                let types = [
                    (AccumulatorType::Day, expected.day.bucket),
                    (AccumulatorType::Month, expected.month.bucket),
                    (AccumulatorType::Year, expected.year.bucket),
                    (AccumulatorType::YearNz, expected.yearnz.bucket),
                ];
                let mut m = HashMap::new();
                for layer in &layers {
                    for (t, b) in &types {
                        m.insert((*t, *layer), *b);
                    }
                }
                m
            };

            for (acc_type, bucket, layer, file) in &all_accumulators {
                if let Some(expected_bucket) = expected_buckets.get(&(*acc_type, *layer)) {
                    if bucket != expected_bucket {
                        to_purge.push((*acc_type, *bucket, *layer,
                            format!("{}/{}/{:04x}(expected {:04x})", layer.name(), file, bucket.0, expected_bucket.0)));
                    }
                }
            }

            // Execute purges - single iterator pass for all ranges
            if !to_purge.is_empty() {
                let purge_desc: Vec<String> = to_purge.iter()
                    .map(|(_, _, _, desc)| desc.clone())
                    .collect();
                let ranges: Vec<(String, String)> = to_purge.iter()
                    .map(|(acc_type, bucket, layer, _)| {
                        CoverageHeader::db_search_range_with_meta(*acc_type, *bucket, *layer)
                    })
                    .collect();
                let purged = db::delete_ranges(&mut db, &ranges);
                info!("{}: purged {} keys from {} stale accumulators: {}",
                    station_name, purged, to_purge.len(), purge_desc.join(", "));
            }

            if hanging_buckets.is_empty() {
                log_progress(&completed);
                return (migrated, 0usize, 0usize, 0usize);
            }

            let mut rolled_up = 0usize;
            let mut arrow = 0usize;
            let mut deleted = 0usize;

            let is_global = station_name == "global";
            let station_meta: Option<&crate::station::StationDetails> = all_details
                .iter()
                .find(|s| s.station.as_str() == station_name);

            // Build set of existing destination files from the all_accumulators we already collected
            // Also include expected accumulator files - a destination may not exist in the DB yet
            // (e.g. first bucket of a new day) but is still valid to roll up into.
            let all_dest_files: HashSet<String> = all_accumulators.iter()
                .map(|(_, _, _, file)| file.clone())
                .chain([&expected.day, &expected.month, &expected.year, &expected.yearnz]
                    .iter()
                    .filter(|e| !e.file.is_empty())
                    .map(|e| e.file.clone()))
                .collect();

            // Group hanging currents per layer and roll each destination-set
            // group up in a single walk (shared with the per-cycle
            // self-healing pass; startup has no active bucket).
            let mut hanging_by_layer: HashMap<Layer, Vec<(AccumulatorBucket, Accumulators)>> =
                HashMap::new();
            for ((bucket, layer), acc) in hanging_buckets {
                hanging_by_layer.entry(layer).or_default().push((bucket, acc));
            }

            for layer in &layers {
                if is_shutdown() {
                    // Leave remaining layers intact - they'll be re-detected
                    // and rolled up on the next startup.
                    break;
                }
                let Some(hangs) = hanging_by_layer.get(layer) else { continue };

                match rollup_current_buckets(
                    &mut db,
                    &station_name,
                    *layer,
                    None, // no active bucket at startup
                    hangs,
                    &all_dest_files,
                    None,
                    is_global,
                    station_meta,
                    &[], // no retired accumulators during startup
                    &SHUTDOWN, // cancel only on process exit
                    &task_progress,
                    "startup",
                ) {
                    Ok((stats, _day_activity, _day_arrow_count)) => {
                        rolled_up += stats.records_written;
                        arrow += stats.arrow_records;
                        deleted += stats.records_deleted;
                    }
                    Err(e) => {
                        error!(
                            "{}: startup rollup failed for {}: {}",
                            station_name, layer.name(), e
                        );
                    }
                }
            }

            if let Err(e) = db.flush() {
                error!("Failed to flush DB for {}: {}", station_name, e);
            }

            log_progress(&completed);

            (migrated, rolled_up, arrow, deleted)
        });

        tasks.push((task_station_name, progress, spawned_at, handle));
    }

    let mut total_migrated = 0usize;
    let mut total_rolled_up = 0usize;
    let mut total_arrow = 0usize;
    let mut total_deleted = 0usize;

    // No timeout: every task is awaited to completion so normal operation
    // never starts while a startup task is still writing (spawn_blocking
    // threads cannot be aborted - abandoning one leaves it running
    // concurrently with the rollup timer). Long tasks warn periodically.
    let warn_interval = std::time::Duration::from_secs(STARTUP_TASK_WARN_SECS);
    for (station_name, progress, spawned_at, handle) in tasks {
        match await_with_warnings(handle, &station_name, &progress, spawned_at, warn_interval, warn_interval).await {
            Ok((migrated, rolled_up, arrow, deleted)) => {
                total_migrated += migrated;
                total_rolled_up += rolled_up;
                total_arrow += arrow;
                total_deleted += deleted;
            }
            Err(e) => {
                error!("Startup rollup task panicked for {}: {}", station_name, e);
            }
        }
    }

    let elapsed = startup_start.elapsed().as_secs_f64();
    let speed = if elapsed > 0.0 { total_stations as f64 / elapsed } else { 0.0 };
    info!(
        "startup:100% [{}/{}] {:.0}s elapsed, {:.1}/s - {} written, {} arrow, {} deleted, {} legacy migrated",
        total_stations, total_stations, elapsed, speed,
        total_rolled_up, total_arrow, total_deleted, total_migrated
    );
}

/// Parse accumulators from a stored meta JSON value.
fn parse_accumulators_from_meta(meta: &serde_json::Value) -> Option<Accumulators> {
    let acc = meta.get("accumulators")?;
    Some(Accumulators {
        current: parse_acc_entry(acc.get("current")?)?,
        day: parse_acc_entry(acc.get("day")?)?,
        month: parse_acc_entry(acc.get("month")?)?,
        year: parse_acc_entry(acc.get("year")?)?,
        yearnz: parse_acc_entry(acc.get("yearnz")?)?,
    })
}

fn parse_acc_entry(v: &serde_json::Value) -> Option<AccumulatorEntry> {
    Some(AccumulatorEntry {
        bucket: AccumulatorBucket(v.get("bucket")?.as_u64()? as u16),
        file: v.get("file")?.as_str()?.to_string(),
        effective_start: crate::types::Epoch(
            v.get("effectiveStart")
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u32,
        ),
    })
}

/// Migrate all legacy unprefixed keys in the DB to layer-prefixed format.
///
/// Legacy keys: "0042/8828308283fffff" → prefixed: "c/0042/8828308283fffff"
/// Checks the first key to determine if migration is needed; a DB is either
/// entirely legacy or entirely prefixed.
fn migrate_legacy_keys(db: &mut rusty_leveldb::DB) -> usize {
    use rusty_leveldb::LdbIterator;

    let mut batch = rusty_leveldb::WriteBatch::default();
    let mut migrated = 0;

    let mut iter = match db.new_iter() {
        Ok(iter) => iter,
        Err(_) => return 0,
    };
    iter.seek(&[]);

    // Check the first key - if it's already layer-prefixed the DB is migrated
    if let Some((first_key, _)) = iter.current() {
        if let Ok(s) = std::str::from_utf8(&first_key) {
            if is_layer_prefixed(s) {
                return 0;
            }
        }
    } else {
        return 0;
    }

    while let Some((key_bytes, val_bytes)) = iter.current() {
        let key_str = match std::str::from_utf8(&key_bytes) {
            Ok(s) => s.to_string(),
            Err(_) => { if !iter.advance() { break; } continue; }
        };

        // All keys should be legacy - if we hit a prefixed key something is wrong
        if is_layer_prefixed(&key_str) {
            error!("Legacy migration: unexpected prefixed key '{}' in legacy DB after migrating {} keys", key_str, migrated);
            break;
        }

        // Parse the legacy key to get accumulator type/bucket and H3
        let header = match CoverageHeader::from_db_key(&key_str) {
            Some(h) => h,
            None => { if !iter.advance() { break; } continue; }
        };

        // Build the prefixed key - same transform for data and meta keys
        let prefixed_key = header.db_key();

        batch.put(prefixed_key.as_bytes(), &val_bytes);
        batch.delete(key_str.as_bytes());
        migrated += 1;

        if !iter.advance() { break; }
    }

    if migrated > 0 {
        if let Err(e) = db.write(batch, true) {
            error!("Legacy migration batch failed: {}", e);
        }
    }

    migrated
}

/// Export station data to Apache Arrow format (standalone, outside rollup)
pub async fn export_arrow(
    station_name: &str,
    accumulator_file: &str,
    _storage: &Storage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let output_dir = crate::config::output_dir(station_name);
    let _output_path = format!("{}/{}.arrow.gz", output_dir, accumulator_file);

    // Ensure output directory exists
    std::fs::create_dir_all(&output_dir)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::accumulators::AccumulatorEntry;
    use crate::coverage::header::AccumulatorBucket;
    use crate::coverage::record::BufferType;
    use crate::types::Epoch;

    #[test]
    fn test_extract_h3_from_db_key() {
        assert_eq!(
            extract_h3_from_db_key("c/0042/8828308283fffff"),
            Some("8828308283fffff".to_string())
        );
        assert_eq!(
            extract_h3_from_db_key("f/1123/882830deadbeef7"),
            Some("882830deadbeef7".to_string())
        );
    }

    #[test]
    fn test_rollup_station_empty_db() {
        let tmp = tempfile::tempdir().unwrap();
        let station_path = tmp.path().to_string_lossy().to_string();

        let opts = rusty_leveldb::Options { create_if_missing: true, ..Default::default() };
        let mut db = rusty_leveldb::DB::open(&station_path, opts).unwrap();

        let accumulators = Accumulators {
            current: AccumulatorEntry { bucket: AccumulatorBucket(0x042), file: String::new(), effective_start: Epoch(0) },
            day: AccumulatorEntry { bucket: AccumulatorBucket(0x1001), file: "2026-03-12".into(), effective_start: Epoch(0) },
            month: AccumulatorEntry { bucket: AccumulatorBucket(0x3003), file: "2026-03".into(), effective_start: Epoch(0) },
            year: AccumulatorEntry { bucket: AccumulatorBucket(0x4000), file: "2026".into(), effective_start: Epoch(0) },
            yearnz: AccumulatorEntry { bucket: AccumulatorBucket(0x5000), file: "2025nz".into(), effective_start: Epoch(0) },
        };

        let (stats, _, _) = rollup_station_layer(
            &mut db, &station_path, "test_station", &accumulators,
            Layer::Combined, ".combined", None, false, None, &[], &SHUTDOWN,
            &std::sync::Mutex::new(RollupProgress::default()),
        ).unwrap();

        assert_eq!(stats.records_written, 0);
        assert_eq!(stats.records_deleted, 0);
    }

    #[test]
    fn test_rollup_station_with_current_records() {
        let tmp = tempfile::tempdir().unwrap();
        let station_path = tmp.path().join("station_db").to_string_lossy().to_string();

        // Populate a DB with "current" accumulator records
        {
            let opts = rusty_leveldb::Options { create_if_missing: true, ..Default::default() };
            let mut db = rusty_leveldb::DB::open(&station_path, opts).unwrap();

            let mut rec = CoverageRecord::new(BufferType::Station);
            rec.update(1000, 500, 2, 28, 5);
            rec.update(900, 400, 1, 32, 3);

            let key = "c/0042/8828308283fffff";
            let _ = db.put(key.as_bytes(), &rec.to_bytes());
            let _ = db.flush();
        }

        let accumulators = Accumulators {
            current: AccumulatorEntry { bucket: AccumulatorBucket(0x042), file: String::new(), effective_start: Epoch(0) },
            day: AccumulatorEntry { bucket: AccumulatorBucket(0x1001), file: "2026-03-12".into(), effective_start: Epoch(0) },
            month: AccumulatorEntry { bucket: AccumulatorBucket(0x3003), file: "2026-03".into(), effective_start: Epoch(0) },
            year: AccumulatorEntry { bucket: AccumulatorBucket(0x4000), file: "2026".into(), effective_start: Epoch(0) },
            yearnz: AccumulatorEntry { bucket: AccumulatorBucket(0x5000), file: "2025nz".into(), effective_start: Epoch(0) },
        };

        let mut db = {
            let opts = rusty_leveldb::Options { create_if_missing: false, ..Default::default() };
            rusty_leveldb::DB::open(&station_path, opts).unwrap()
        };

        let (stats, _, _) = rollup_station_layer(
            &mut db, &station_path, "test_station", &accumulators,
            Layer::Combined, ".combined", None, false, None, &[], &SHUTDOWN,
            &std::sync::Mutex::new(RollupProgress::default()),
        ).unwrap();

        // Should have merged into 4 destinations (day, month, year, yearnz)
        assert_eq!(stats.records_written, 4);
        assert_eq!(stats.records_deleted, 1);

        // Verify the current record was deleted and dest records were written
        assert!(db.get("c/0042/8828308283fffff".as_bytes()).is_none());
        let day_key = make_dest_key(
            AccumulatorType::Day, AccumulatorBucket(0x1001),
            Layer::Combined, "8828308283fffff",
        );
        let day_data = db.get(day_key.as_bytes()).expect("day record should exist");
        let day_rec = CoverageRecord::from_bytes(&day_data).unwrap();
        assert_eq!(day_rec.count(), 2);
    }

    fn test_accumulators() -> Accumulators {
        Accumulators {
            current: AccumulatorEntry { bucket: AccumulatorBucket(0x042), file: String::new(), effective_start: Epoch(0) },
            day: AccumulatorEntry { bucket: AccumulatorBucket(0x1001), file: "2026-03-12".into(), effective_start: Epoch(0) },
            month: AccumulatorEntry { bucket: AccumulatorBucket(0x3003), file: "2026-03".into(), effective_start: Epoch(0) },
            year: AccumulatorEntry { bucket: AccumulatorBucket(0x4000), file: "2026".into(), effective_start: Epoch(0) },
            yearnz: AccumulatorEntry { bucket: AccumulatorBucket(0x5000), file: "2025nz".into(), effective_start: Epoch(0) },
        }
    }

    #[test]
    fn test_rollup_cancelled_preserves_current() {
        // A cancelled rollup must leave the current accumulator fully intact -
        // no destination records, no deletions - so it can be retried later.
        let tmp = tempfile::tempdir().unwrap();
        let station_path = tmp.path().join("station_db").to_string_lossy().to_string();
        let key = "c/0042/8828308283fffff";

        let mut db = {
            let opts = rusty_leveldb::Options { create_if_missing: true, ..Default::default() };
            let mut db = rusty_leveldb::DB::open(&station_path, opts).unwrap();
            let mut rec = CoverageRecord::new(BufferType::Station);
            rec.update(1000, 500, 2, 28, 5);
            db.put(key.as_bytes(), &rec.to_bytes()).unwrap();
            db.flush().unwrap();
            db
        };

        let accumulators = test_accumulators();
        let cancel = AtomicBool::new(true);
        let (stats, _, _) = rollup_station_layer(
            &mut db, &station_path, "test_station", &accumulators,
            Layer::Combined, ".combined", None, false, None, &[], &cancel,
            &std::sync::Mutex::new(RollupProgress::default()),
        ).unwrap();

        assert_eq!(stats.records_written, 0);
        assert_eq!(stats.records_deleted, 0);
        // Current record untouched, no destination records created
        assert!(db.get(key.as_bytes()).is_some());
        let day_key = make_dest_key(
            AccumulatorType::Day, AccumulatorBucket(0x1001),
            Layer::Combined, "8828308283fffff",
        );
        assert!(db.get(day_key.as_bytes()).is_none());
    }

    #[test]
    fn test_rollup_layer_core_merges_multiple_current_buckets() {
        // Simulates the startup grouping path: two hanging current buckets that
        // share a destination set are pre-merged in memory and rolled up with a
        // single destination walk; both buckets' records are deleted on commit.
        let tmp = tempfile::tempdir().unwrap();
        let station_path = tmp.path().join("station_db").to_string_lossy().to_string();

        let h3 = "8828308283fffff";
        let key_a = "c/0042/8828308283fffff";
        let key_b = "c/0043/8828308283fffff";

        let mut rec_a = CoverageRecord::new(BufferType::Station);
        rec_a.update(1000, 500, 2, 28, 5);
        rec_a.update(900, 400, 1, 32, 3);
        let mut rec_b = CoverageRecord::new(BufferType::Station);
        rec_b.update(800, 300, 1, 30, 4);

        let mut db = {
            let opts = rusty_leveldb::Options { create_if_missing: true, ..Default::default() };
            let mut db = rusty_leveldb::DB::open(&station_path, opts).unwrap();
            db.put(key_a.as_bytes(), &rec_a.to_bytes()).unwrap();
            db.put(key_b.as_bytes(), &rec_b.to_bytes()).unwrap();
            db.flush().unwrap();
            db
        };

        // Pre-merge the duplicate H3 as the startup group loop does
        let merged = rec_a.rollup(&rec_b, None).expect("merge should produce a record");
        let source = CurrentSource {
            records: vec![(h3.to_string(), merged)],
            delete_data_keys: vec![key_a.to_string(), key_b.to_string()],
            delete_meta_keys: Vec::new(),
            records_read: 2,
            period_start: Epoch(0),
            period_end: Epoch(3600),
        };

        let accumulators = test_accumulators();
        // Distinct station name: arrow output paths are derived from it, and a
        // name shared with other tests would race on the .working temp files.
        let (stats, _, _) = rollup_layer_core(
            &mut db, "test_station_multi", &accumulators, Layer::Combined, ".combined",
            None, false, None, &[], &SHUTDOWN,
            &std::sync::Mutex::new(RollupProgress::default()),
            &source, std::time::Instant::now(), std::time::Duration::ZERO,
        ).unwrap();

        // Merged into 4 destinations; both source buckets' records deleted
        assert_eq!(stats.records_written, 4);
        assert_eq!(stats.records_deleted, 2);
        assert!(db.get(key_a.as_bytes()).is_none());
        assert!(db.get(key_b.as_bytes()).is_none());

        let day_key = make_dest_key(
            AccumulatorType::Day, AccumulatorBucket(0x1001),
            Layer::Combined, h3,
        );
        let day_rec = CoverageRecord::from_bytes(&db.get(day_key.as_bytes()).unwrap()).unwrap();
        assert_eq!(day_rec.count(), 3);
    }

    #[test]
    fn test_heal_hanging_bucket_joins_active_group() {
        // A hanging current bucket whose destinations match the live
        // accumulators joins the active group and is healed in the same
        // destination walk - one merge, both buckets deleted.
        let tmp = tempfile::tempdir().unwrap();
        let station_path = tmp.path().join("station_db").to_string_lossy().to_string();

        let h3 = "8828308283fffff";
        let active_key = "c/0042/8828308283fffff";
        let hanging_key = "c/0041/8828308283fffff";

        let mut active_rec = CoverageRecord::new(BufferType::Station);
        active_rec.update(1000, 500, 2, 28, 5);
        active_rec.update(900, 400, 1, 32, 3);
        let mut hanging_rec = CoverageRecord::new(BufferType::Station);
        hanging_rec.update(800, 300, 1, 30, 4);

        let mut db = {
            let opts = rusty_leveldb::Options { create_if_missing: true, ..Default::default() };
            let mut db = rusty_leveldb::DB::open(&station_path, opts).unwrap();
            db.put(active_key.as_bytes(), &active_rec.to_bytes()).unwrap();
            db.put(hanging_key.as_bytes(), &hanging_rec.to_bytes()).unwrap();
            db.flush().unwrap();
            db
        };

        let active = test_accumulators(); // current bucket 0x042
        // Hanging accumulator from an earlier period, same destination set
        let mut hanging_acc = test_accumulators();
        hanging_acc.current.bucket = AccumulatorBucket(0x041);
        let hanging = vec![(AccumulatorBucket(0x041), hanging_acc)];

        let (stats, _, _) = rollup_current_buckets(
            &mut db, "test_station_heal", Layer::Combined, Some(&active), &hanging,
            &HashSet::new(), None, false, None, &[], &AtomicBool::new(false),
            &std::sync::Mutex::new(RollupProgress::default()), "heal",
        ).unwrap();

        // One merged H3 into 4 destinations, both source buckets deleted
        assert_eq!(stats.records_written, 4);
        assert_eq!(stats.records_deleted, 2);
        assert!(db.get(active_key.as_bytes()).is_none());
        assert!(db.get(hanging_key.as_bytes()).is_none());

        let day_key = make_dest_key(
            AccumulatorType::Day, AccumulatorBucket(0x1001),
            Layer::Combined, h3,
        );
        let day_rec = CoverageRecord::from_bytes(&db.get(day_key.as_bytes()).unwrap()).unwrap();
        assert_eq!(day_rec.count(), 3);
    }

    #[test]
    fn test_heal_drops_hang_with_missing_destinations() {
        // A pure-hang group whose destination files are unknown is DROPPED
        // (purged) rather than rolled up - rolling would overwrite complete
        // arrow files on disk with partial data.
        let tmp = tempfile::tempdir().unwrap();
        let station_path = tmp.path().join("station_db").to_string_lossy().to_string();

        let hanging_key = "c/0041/8828308283fffff";
        let mut rec = CoverageRecord::new(BufferType::Station);
        rec.update(800, 300, 1, 30, 4);

        let mut db = {
            let opts = rusty_leveldb::Options { create_if_missing: true, ..Default::default() };
            let mut db = rusty_leveldb::DB::open(&station_path, opts).unwrap();
            db.put(hanging_key.as_bytes(), &rec.to_bytes()).unwrap();
            db.flush().unwrap();
            db
        };

        let mut hanging_acc = test_accumulators();
        hanging_acc.current.bucket = AccumulatorBucket(0x041);
        let hanging = vec![(AccumulatorBucket(0x041), hanging_acc)];

        // all_dest_files empty -> every destination is "missing"
        let (stats, _, _) = rollup_current_buckets(
            &mut db, "test_station_drop", Layer::Combined, None, &hanging,
            &HashSet::new(), None, false, None, &[], &AtomicBool::new(false),
            &std::sync::Mutex::new(RollupProgress::default()), "startup",
        ).unwrap();

        assert_eq!(stats.records_written, 0);
        // Hanging data purged, no destination records created
        assert!(db.get(hanging_key.as_bytes()).is_none());
        let day_key = make_dest_key(
            AccumulatorType::Day, AccumulatorBucket(0x1001),
            Layer::Combined, "8828308283fffff",
        );
        assert!(db.get(day_key.as_bytes()).is_none());
    }

    #[test]
    fn test_write_arrow_station_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let output_dir = tmp.path().to_string_lossy().to_string();

        let rows = vec![
            ArrowStation {
                h3lo: 0xAABBCCDD, h3hi: 0x88,
                min_agl: 400, min_alt: 900,
                min_alt_sig: 28, max_sig: 32,
                avg_sig: 30, avg_crc: 15, count: 2, avg_gap: 16,
            },
        ];

        let count = write_arrow_station(
            &output_dir, "TEST", "day", "2026-03-12", ".combined", &rows,
        ).unwrap();
        assert_eq!(count, 1);

        // Verify file exists
        let gz_path = format!("{}/TEST.day.2026-03-12.combined.arrow.gz", output_dir);
        assert!(std::path::Path::new(&gz_path).exists());
    }
}
