//! Rollup — merging current accumulators into day/month/year archives
//! and exporting to Apache Arrow files.
//!
//! This module handles the periodic aggregation of coverage data from
//! the "current" accumulator into longer-term day, month, year, and
//! year-nz (Southern Hemisphere season) accumulators.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use rusty_leveldb::LdbIterator;

/// Global shutdown flag — checked by long-running DB iterations.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Signal all rollup tasks to stop iterating.
pub fn request_shutdown() {
    SHUTDOWN.store(true, Ordering::Relaxed);
}

fn is_shutdown() -> bool {
    SHUTDOWN.load(Ordering::Relaxed)
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
    MAX_SIMULTANEOUS_ROLLUPS, OUTPUT_PATH, ROLLUP_PERIOD_MINUTES, STATION_EXPIRY_TIME_SECS,
    UNCOMPRESSED_ARROW_FILES,
};
use crate::coverage::activity::{update_activity, RollupActivity};
use crate::coverage::header::{
    AccumulatorBucket, AccumulatorType, AccumulatorTypeAndBucket, CoverageHeader,
};
use crate::coverage::record::{ArrowGlobal, ArrowStation, CoverageRecord};
use crate::h3cache::H3Cache;
use crate::layers::Layer;
use crate::station::{StationDetails, StationManager};
use crate::db::{self, Storage, TrackedDb};
use crate::types::{Epoch, H3Index, StationId, StationName};

#[derive(Debug, Default)]
pub struct RollupStats {
    pub stations_processed: usize,
    pub stations_skipped: usize,
    pub records_written: usize,
    pub records_deleted: usize,
    pub arrow_records: usize,
    pub elapsed_ms: u64,
}

/// Live progress for a single rollup task, readable from outside the blocking thread.
#[derive(Debug, Default)]
struct RollupProgress {
    layer: String,
    phase: String,
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
        )
    }
}

/// Perform a full rollup: merge current → day/month/year/yearnz.
/// Caller must flush the H3 cache before calling this.
pub async fn rollup_all(
    storage: &Storage,
    station_manager: &StationManager,
    old_accumulators: &Accumulators,
    new_accumulators: Option<&Accumulators>,
) -> RollupStats {
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

    for station in &all_station_details {
        let was_valid = station.valid;
        let validity_ts = station
            .last_packet
            .or(station.last_beacon)
            .map(|e| e.0)
            .unwrap_or(now_epoch); // no timestamp yet → assume valid

        if station.moved {
            moved_count += 1;
            // moved flag is reset below after purge
        } else if validity_ts > expiry_epoch {
            valid_stations.insert(station.id);
        } else {
            // expired
            if was_valid {
                invalid_count += 1;
                info!(
                    "station {} now invalid: expired, last activity {}",
                    station.station,
                    chrono::DateTime::from_timestamp(validity_ts as i64, 0)
                        .map(|d| d.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                        .unwrap_or_else(|| validity_ts.to_string())
                );
            }
        }
    }

    // Safety: if >2% of stations became invalid, don't purge (something is wrong)
    if invalid_count as f64 / (valid_stations.len().max(1) as f64) > 0.02 {
        warn!(
            "Too many invalid stations ({}), not purging any",
            invalid_count
        );
        for station in &all_station_details {
            valid_stations.insert(station.id);
        }
    } else {
        need_purge = invalid_count > 0 || moved_count > 0;
    }

    info!(
        "performing rollup of {} valid stations + global, {} invalid, {} moved",
        valid_stations.len(),
        invalid_count,
        moved_count,
    );

    // Update station validity in the station manager
    for station in &all_station_details {
        let is_valid = valid_stations.contains(&station.id);
        let was_moved = station.moved;
        if station.valid != is_valid || was_moved {
            let mut updated = station.clone();
            updated.valid = is_valid;
            if was_moved {
                updated.moved = false;
            }
            station_manager.update(&updated);
        }
    }

    // Determine which layers to process
    let layers: Vec<Layer> = if let Some(ref enabled) = *crate::config::ENABLED_LAYERS {
        enabled.iter().copied().collect()
    } else {
        crate::layers::ALL_LAYERS.to_vec()
    };

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

    let total_stations = station_entries.len();

    // --- Concurrent rollup with MAX_SIMULTANEOUS_ROLLUPS ---
    let max_concurrent = *MAX_SIMULTANEOUS_ROLLUPS;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
    let valid_stations = Arc::new(valid_stations);
    let accumulators = old_accumulators.clone();
    let layers = Arc::new(layers);
    let retired_accumulators = Arc::new(retired_accumulators);

    let mut tasks: Vec<(String, String, Arc<std::sync::Mutex<RollupProgress>>, tokio::task::JoinHandle<RollupStats>)> = Vec::new();
    let mut skipped_no_traffic: usize = 0;

    for (station_name, _is_global_hint, station_meta) in station_entries {
        let station_path = storage.station_path(&station_name).to_string_lossy().to_string();
        let is_global = station_name == "global";

        // Purge invalid/moved stations instead of rolling them up
        if !is_global && need_purge {
            let is_invalid = station_meta
                .as_ref()
                .map(|s| !valid_stations.contains(&s.id))
                .unwrap_or(false);
            let was_moved = station_meta.as_ref().map(|s| s.moved).unwrap_or(false);
            if is_invalid || was_moved {
                let reason = if was_moved {
                    "moved".to_string()
                } else {
                    let last = station_meta
                        .as_ref()
                        .and_then(|s| s.last_packet.or(s.last_beacon));
                    match last {
                        Some(e) => format!("expired, last activity epoch {}", e.0),
                        None => "no activity recorded".to_string(),
                    }
                };
                info!("clearing database for {}: {}", station_name, reason);
                storage.purge_station(&station_name);
                continue;
            }
        }

        // Skip stations with no new traffic since last output (mirrors TS rollup.ts:180)
        if !is_global && !has_retired {
            if let Some(meta) = &station_meta {
                if let Some(out_epoch) = meta.output_epoch {
                    if !meta.moved {
                        let last = meta.last_packet.map(|e| e.0).unwrap_or(0);
                        let cutoff = update_cutoff.map(|e| e.0).unwrap_or(out_epoch.0);
                        if last < cutoff {
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
            ) {
                Ok(stats) => stats,
                Err(e) => {
                    error!("Rollup failed for {}: {}", station_name, e);
                    RollupStats::default()
                }
            };
            station_stats
        });
        tasks.push((task_station_name, task_station_path, progress, handle));
    }

    // Produce the master stations list (runs while station rollups are in progress)
    crate::stationfile::produce_station_file(station_manager, old_accumulators);

    // Collect results with per-station timeout
    let mut total_stats = RollupStats::default();
    total_stats.stations_processed = total_stations;
    let station_timeout = std::time::Duration::from_secs(300); // 5 minutes per station

    for (station_name, station_path, progress, handle) in tasks {
        match tokio::time::timeout(station_timeout, handle).await {
            Ok(Ok(stats)) => {
                total_stats.records_written += stats.records_written;
                total_stats.records_deleted += stats.records_deleted;
                total_stats.arrow_records += stats.arrow_records;
                // Update output_epoch/output_date (mirrors TS rollup.ts:207-208)
                if station_name != "global" {
                    if let Some(mut details) = station_manager.get(&StationName(station_name.clone())) {
                        details.output_epoch = Some(Epoch(now_epoch));
                        details.output_date = Some(
                            chrono::DateTime::from_timestamp(now_epoch as i64, 0)
                                .map(|d| d.to_rfc3339())
                                .unwrap_or_default()
                        );
                        station_manager.update(&details);
                    }
                }
            }
            Ok(Err(e)) => {
                let prog = progress.lock().map(|p| p.to_string()).unwrap_or_default();
                error!("Rollup task panicked for {} ({}): {} — progress: {}", station_name, station_path, e, prog);
                total_stats.stations_skipped += 1;
            }
            Err(_) => {
                let prog = progress.lock().map(|p| p.to_string()).unwrap_or_default();
                error!("Rollup task timed out after {}s for {} ({}) — progress: {}",
                    station_timeout.as_secs(), station_name, station_path, prog);
                total_stats.stations_skipped += 1;
            }
        }
    }

    total_stats.stations_skipped += skipped_no_traffic;
    total_stats.elapsed_ms = start.elapsed().as_millis() as u64;

    info!(
        "Rollup complete in {}ms: {} stations ({} skipped no-traffic, {} concurrent), {} records written, {} deleted, {} arrow records",
        total_stats.elapsed_ms,
        total_stats.stations_processed,
        skipped_no_traffic,
        max_concurrent,
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
    /// Current records from this accumulator, sorted by key
    records: Vec<(String, Vec<u8>)>,
    /// Current position in the records iterator
    pos: usize,
    /// Arrow rows collected
    arrow_station_rows: Vec<ArrowStation>,
    arrow_global_rows: Vec<ArrowGlobal>,
    /// Activity tracking (loaded from DB meta, updated after rollup)
    activity: RollupActivity,
}

impl RollupAccumulator {
    fn current(&self) -> Option<(&str, &[u8])> {
        if self.pos < self.records.len() {
            Some((&self.records[self.pos].0, &self.records[self.pos].1))
        } else {
            None
        }
    }

    fn advance(&mut self) {
        self.pos += 1;
    }
}

/// Per-station rollup: open the DB once, roll up all layers, flush and close.
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
) -> Result<RollupStats, String> {
    let mut db = match TrackedDb::open(station_path, true, "rollup") {
        Ok(db) => db,
        Err(e) => {
            return Err(format!("Failed to open DB {}: {}", station_path, e));
        }
    };

    let mut total_stats = RollupStats::default();
    for layer in layers {
        if let Ok(mut p) = progress.lock() {
            p.layer = layer.name().to_string();
            p.phase = "rollup".to_string();
        }
        let layer_suffix = layer.file_suffix();
        match rollup_station_layer(
            &mut db, station_path, station_name, accumulators,
            *layer, layer_suffix, valid_stations, is_global, station_meta,
            retired_accumulators,
        ) {
            Ok(stats) => {
                total_stats.records_written += stats.records_written;
                total_stats.records_deleted += stats.records_deleted;
                total_stats.arrow_records += stats.arrow_records;
                if let Ok(mut p) = progress.lock() {
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

    if let Ok(mut p) = progress.lock() {
        p.phase = "flush".to_string();
    }
    db.flush().map_err(|e| format!("flush failed for {}: {}", station_name, e))?;
    if let Ok(mut p) = progress.lock() {
        p.phase = "compact".to_string();
    }
    db.compact_range(b"0", b"Z").map_err(|e| format!("compact failed for {}: {}", station_name, e))?;
    db.flush().map_err(|e| format!("flush after compact failed for {}: {}", station_name, e))?;
    Ok(total_stats)
}

/// Per-layer rollup within an already-open DB.
fn rollup_station_layer(
    db: &mut rusty_leveldb::DB,
    _station_path: &str,
    station_name: &str,
    accumulators: &Accumulators,
    layer: Layer,
    layer_suffix: &str,
    valid_stations: Option<&HashSet<StationId>>,
    is_global: bool,
    station_meta: Option<&crate::station::StationDetails>,
    retired_accumulators: &[(AccumulatorType, AccumulatorBucket)],
) -> Result<RollupStats, String> {
    let mut stats = RollupStats::default();

    // Read all "current" accumulator records for this layer
    let (current_start, current_end) = CoverageHeader::db_search_range(
        AccumulatorType::Current,
        accumulators.current.bucket,
        layer,
    );
    let current_records = db::read_range(db, &current_start, &current_end, Some(&SHUTDOWN));

    if is_shutdown() || current_records.is_empty() {
        return Ok(stats);
    }

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
        let records = db::read_range(db, &start, &end, Some(&SHUTDOWN));

        let meta_key = CoverageHeader::accumulator_meta(*acc_type, entry.bucket, layer).db_key();
        let activity = load_activity_from_db(db, &meta_key);

        destinations.push(RollupAccumulator {
            acc_type: *acc_type,
            bucket: entry.bucket,
            file: entry.file.clone(),
            records,
            pos: 0,
            arrow_station_rows: Vec::new(),
            arrow_global_rows: Vec::new(),
            activity,
        });
    }

    // Batch of DB put/delete operations to apply at the end
    let mut puts: Vec<(String, Vec<u8>)> = Vec::new();
    let mut deletes: Vec<String> = Vec::new();

    // Walk the current accumulator and merge into each destination
    for (current_key, current_value) in &current_records {
        let current_record = match CoverageRecord::from_bytes(current_value) {
            Some(r) => r,
            None => continue,
        };

        // Extract the H3 index from the current key
        let current_h3 = match extract_h3_from_db_key(current_key) {
            Some(h3) => h3,
            None => continue,
        };

        // Process each destination accumulator
        for dest in &mut destinations {
            // Advance past keys that are before the current H3
            loop {
                let should_emit = match dest.current() {
                    Some((dest_key, _)) => {
                        let dest_h3 = extract_h3_from_db_key(dest_key).unwrap_or_default();
                        dest_h3 < current_h3
                    }
                    None => false,
                };
                if !should_emit { break; }
                if let Some((key, value)) = emit_at_pos(dest, is_global, valid_stations) {
                    match value {
                        Some(v) => puts.push((key, v)),
                        None => deletes.push(key),
                    }
                }
                dest.advance();
            }

            // Check if destination has a matching H3
            let dest_key_for_h3 = make_dest_key(dest.acc_type, dest.bucket, layer, &current_h3);

            // Check for matching H3 — copy values out before advancing
            let merged = {
                let matches = match dest.current() {
                    Some((dk, _)) => extract_h3_from_db_key(dk) == Some(current_h3.clone()),
                    None => false,
                };
                if matches {
                    let dest_value = dest.records[dest.pos].1.clone();
                    dest.advance();
                    let dest_record = CoverageRecord::from_bytes(&dest_value);
                    match dest_record {
                        Some(dr) => dr.rollup(&current_record, valid_stations),
                        None => Some(current_record.clone()),
                    }
                } else {
                    Some(current_record.clone())
                }
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

    }

    // Drain remaining destination records (past end of current)
    for dest in &mut destinations {
        while dest.pos < dest.records.len() {
            if let Some((key, value)) = emit_at_pos(dest, is_global, valid_stations) {
                match value {
                    Some(v) => puts.push((key, v)),
                    None => deletes.push(key),
                }
            }
            dest.advance();
        }
    }

    // Update activity for each destination
    let h3source = current_records.len() as u32;
    let period_start = Epoch(accumulators.current.effective_start.0);
    let period_end = Epoch(period_start.0 + (*ROLLUP_PERIOD_MINUTES * 60.0) as u32);
    let now = Epoch(std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32);

    for dest in &mut destinations {
        update_activity(&mut dest.activity, h3source, period_start, period_end, now);
    }

    // Build a single WriteBatch for all DB mutations
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

    // Delete the current accumulator records we already read
    for (key, _) in &current_records {
        batch.delete(key.as_bytes());
    }
    stats.records_deleted = current_records.len();

    // Delete the current meta key
    let current_meta_key = CoverageHeader::accumulator_meta(
        AccumulatorType::Current, accumulators.current.bucket, layer
    ).db_key();
    batch.delete(current_meta_key.as_bytes());

    db.write(batch, true).map_err(|e| format!("write batch failed for {}: {}", station_name, e))?;

    // Purge retired accumulators (matching TypeScript rollupdatabase.ts:407-416).
    // When a bucket changes (e.g. day rolls over), purge old bucket's data and meta.
    for (acc_type, old_bucket) in retired_accumulators {
        let (start, end) = CoverageHeader::db_search_range_with_meta(*acc_type, *old_bucket, layer);
        db::delete_range(db, &start, &end);
    }

    // Write arrow files and metadata for each destination
    let output_dir = if station_name == "global" {
        format!("{}global", *OUTPUT_PATH)
    } else {
        format!("{}stations/{}", *OUTPUT_PATH, station_name)
    };
    if let Err(e) = std::fs::create_dir_all(&output_dir) {
        return Err(format!("Failed to create output dir: {}", e));
    }

    for dest in &destinations {
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

    Ok(stats)
}

/// Emit the record at dest.pos to arrow output, optionally filtering stations.
/// Also writes back to DB if station filtering changed the record (matching TypeScript behaviour).
/// Returns a DB mutation if the record was changed or deleted by station filtering.
fn emit_at_pos(
    dest: &mut RollupAccumulator,
    is_global: bool,
    valid_stations: Option<&HashSet<StationId>>,
) -> Option<(String, Option<Vec<u8>>)> {
    let (ref key, ref value) = dest.records[dest.pos];
    let record = match CoverageRecord::from_bytes(value) {
        Some(r) => r,
        None => return None,
    };

    let h3 = match extract_h3_from_db_key(key) {
        Some(h3) => h3,
        None => return None,
    };

    let db_key = key.clone();

    if let Some(valid) = valid_stations {
        match record.remove_invalid_stations(valid) {
            Some(filtered) => {
                let changed = filtered.to_bytes() != record.to_bytes();
                let (lo, hi) = H3Index(h3).split_long();
                if is_global {
                    dest.arrow_global_rows.push(filtered.to_arrow_global(lo, hi));
                } else {
                    dest.arrow_station_rows.push(filtered.to_arrow_station(lo, hi));
                }
                if changed {
                    Some((db_key, Some(filtered.to_bytes())))
                } else {
                    None
                }
            }
            None => {
                // All stations removed — delete from DB
                Some((db_key, None))
            }
        }
    } else {
        let (lo, hi) = H3Index(h3).split_long();
        if is_global {
            dest.arrow_global_rows.push(record.to_arrow_global(lo, hi));
        } else {
            dest.arrow_station_rows.push(record.to_arrow_station(lo, hi));
        }
        None
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
    std::fs::rename(&gz_working, &gz_final)
        .map_err(|e| format!("Rename error: {}", e))?;

    // Create symlink for latest: station.type.layer.arrow.gz -> station.type.file.layer.arrow.gz
    let symlink_name = format!("{}/{}.{}{}.arrow.gz", output_dir, station_name, acc_type, layer_suffix);
    let _ = std::fs::remove_file(&symlink_name);
    let target = format!("{}.arrow.gz", base_name);
    #[cfg(unix)]
    {
        let _ = std::os::unix::fs::symlink(&target, &symlink_name);
    }

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

        let raw_symlink = format!("{}/{}.{}{}.arrow", output_dir, station_name, acc_type, layer_suffix);
        let _ = std::fs::remove_file(&raw_symlink);
        let raw_target = format!("{}.arrow", base_name);
        #[cfg(unix)]
        {
            let _ = std::os::unix::fs::symlink(&raw_target, &raw_symlink);
        }
    }

    Ok(())
}

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
    let symlink_name = format!("{}/{}.{}{}.json", output_dir, station_name, acc_type, layer_suffix);
    let _ = std::fs::remove_file(&symlink_name);
    let target = format!("{}.json", base_name);
    #[cfg(unix)]
    {
        let _ = std::os::unix::fs::symlink(&target, &symlink_name);
    }
}

/// Startup rollup: check for any unflushed accumulators from a previous run.
///
/// Scans each station DB for "current" accumulator meta keys. If found,
/// the metadata contains the accumulator buckets that were active when the
/// process last shut down. We migrate any legacy unprefixed keys, then
/// roll up the hanging current data into day/month/year/yearnz.
/// Also purges orphaned data and old accumulators that don't match expected buckets.
pub async fn rollup_startup(
    storage: &Storage,
    station_manager: &StationManager,
    expected_accumulators: &Accumulators,
) {
    info!("Startup rollup: checking for unflushed accumulators...");

    let all_station_details = station_manager.all_stations_with_global();

    let layers: Vec<Layer> = if let Some(ref enabled) = *crate::config::ENABLED_LAYERS {
        enabled.iter().copied().collect()
    } else {
        crate::layers::ALL_LAYERS.to_vec()
    };

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

        let handle = tokio::task::spawn_blocking(move || {
            let _permit = permit;

            let log_progress = |completed: &std::sync::atomic::AtomicUsize| {
                let done = completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                if done % 100 == 0 || done == total {
                    let pct = done * 100 / total;
                    let elapsed = startup_start.elapsed().as_secs_f64();
                    let speed = if elapsed > 0.0 { done as f64 / elapsed } else { 0.0 };
                    info!("startup:{}% [{}/{}] {:.0}s elapsed, {:.1}/s", pct, done, total, elapsed, speed);
                }
            };

            // Open DB once for all operations on this station
            let mut db = match TrackedDb::open(&station_path, true, "startup_rollup") {
                Ok(db) => db,
                Err(e) => {
                    error!("Startup rollup: failed to open DB {}: {}", station_path, e);
                    log_progress(&completed);
                    return (0, 0, 0, 0);
                }
            };

            // Scan DB using seeking iterator (like TypeScript) — only reads meta keys,
            // skips past data ranges to avoid reading all H3 records into memory.
            let mut hanging_buckets: HashMap<(AccumulatorBucket, Layer), Accumulators> = HashMap::new();
            let mut to_purge: Vec<(AccumulatorType, AccumulatorBucket, Layer)> = Vec::new();
            let mut all_accumulators: Vec<(AccumulatorType, AccumulatorBucket, Layer, String)> = Vec::new();

            {
                let mut iter = match db.new_iter() {
                    Ok(iter) => iter,
                    Err(_) => {
                        log_progress(&completed);
                        return (0, 0, 0, 0);
                    }
                };
                iter.seek(&[]);
                let mut key_buf = Vec::new();
                let mut val_buf = Vec::new();

                while iter.current(&mut key_buf, &mut val_buf) {
                    let key_str = match std::str::from_utf8(&key_buf) {
                        Ok(s) => s.to_string(),
                        Err(_) => { if !iter.advance() { break; } continue; }
                    };

                    let header = match CoverageHeader::from_db_key(&key_str) {
                        Some(h) => h,
                        None => { if !iter.advance() { break; } continue; }
                    };

                    let acc_type = header.accumulator_type();
                    let bucket = header.bucket();
                    let layer = header.layer;

                    // Calculate the end of this accumulator's range for seeking
                    let (_, seek_end) = CoverageHeader::db_search_range(acc_type, bucket, layer);

                    if !header.is_meta() {
                        // Data entry without meta — orphaned, mark for purge
                        info!("{}: purging entry without metadata {}", station_name, key_str);
                        to_purge.push((acc_type, bucket, layer));
                        iter.seek(seek_end.as_bytes());
                        continue;
                    }

                    // Process meta entry
                    if acc_type == AccumulatorType::Current {
                        if layers.contains(&layer) {
                            if let Ok(meta) = serde_json::from_slice::<serde_json::Value>(&val_buf) {
                                if let Some(acc) = parse_accumulators_from_meta(&meta) {
                                    hanging_buckets.insert((bucket, layer), acc);
                                }
                            }
                        }
                    } else {
                        let meta_ok = serde_json::from_slice::<serde_json::Value>(&val_buf)
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
                                all_accumulators.push((acc_type, bucket, layer, file));
                            }
                            None => {
                                info!("{}: invalid accumulator or missing metadata for {}, purging",
                                    station_name, key_str);
                                to_purge.push((acc_type, bucket, layer));
                            }
                        }
                    }

                    // Seek past this accumulator's data range
                    iter.seek(seek_end.as_bytes());
                }
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

            for (acc_type, bucket, layer, _file) in &all_accumulators {
                if let Some(expected_bucket) = expected_buckets.get(&(*acc_type, *layer)) {
                    if bucket != expected_bucket {
                        to_purge.push((*acc_type, *bucket, *layer));
                    }
                }
            }

            // Execute purges
            if !to_purge.is_empty() {
                let purge_desc: Vec<String> = to_purge.iter()
                    .map(|(t, b, l)| format!("{}/{:04x}/{}", l.db_prefix(), b.0, t.name()))
                    .collect();
                let mut purged = 0usize;
                for (acc_type, bucket, layer) in &to_purge {
                    let (start, end) = CoverageHeader::db_search_range_with_meta(*acc_type, *bucket, *layer);
                    purged += db::delete_range(&mut db, &start, &end);
                }
                info!("{}: purged {} keys from {} stale accumulators: {}",
                    station_name, purged, to_purge.len(), purge_desc.join(", "));
            }

            if hanging_buckets.is_empty() {
                log_progress(&completed);
                return (0usize, 0usize, 0usize, 0usize);
            }

            let mut migrated = 0usize;
            let mut rolled_up = 0usize;
            let mut arrow = 0usize;
            let mut deleted = 0usize;

            let is_global = station_name == "global";
            let station_meta: Option<&crate::station::StationDetails> = all_details
                .iter()
                .find(|s| s.station.as_str() == station_name);

            // Build set of existing destination files from the all_accumulators we already collected
            let all_dest_files: HashSet<String> = all_accumulators.iter()
                .map(|(_, _, _, file)| file.clone())
                .collect();

            // Migrate legacy keys first
            for ((bucket, _layer), acc) in &hanging_buckets {
                migrated += migrate_legacy_keys(&mut db, acc, *bucket);
            }

            for ((_bucket, layer), acc) in &hanging_buckets {
                let (current_start, dest_files) = acc.describe();

                // Check which destination buckets are missing
                let missing: Vec<&str> = [
                    ("day", &acc.day),
                    ("month", &acc.month),
                    ("year", &acc.year),
                    ("yearnz", &acc.yearnz),
                ].iter()
                    .filter(|(_, entry)| !entry.file.is_empty() && !all_dest_files.contains(&entry.file))
                    .map(|(name, _)| *name)
                    .collect();

                if missing.len() >= 3 {
                    warn!(
                        "{}: DROPPING hanging current accumulator {:04x}({}) for {}: {} were missing",
                        station_name, acc.current.bucket.0, current_start, dest_files,
                        missing.join(",")
                    );
                    // Delete the current meta key so it won't hang again
                    let meta_key = CoverageHeader::accumulator_meta(
                        AccumulatorType::Current, acc.current.bucket, *layer,
                    ).db_key();
                    let _ = db.delete(meta_key.as_bytes());
                    continue;
                }

                info!(
                    "{}: rolling up hanging current accumulator {:04x}({}) into {}{}",
                    station_name, acc.current.bucket.0, current_start, dest_files,
                    if missing.is_empty() { String::new() } else { format!(", missing: {}", missing.join(",")) }
                );

                let layer_suffix = layer.file_suffix();
                match rollup_station_layer(
                    &mut db,
                    &station_path,
                    &station_name,
                    acc,
                    *layer,
                    layer_suffix,
                    None,
                    is_global,
                    station_meta,
                    &[], // no retired accumulators during startup
                ) {
                    Ok(stats) => {
                        info!(
                            "{}: startup rollup complete {} — {} written, {} arrow, {} deleted",
                            station_name, layer.name(),
                            stats.records_written, stats.arrow_records, stats.records_deleted
                        );
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

            // Delete the current meta keys now that rollup is done
            let mut batch = rusty_leveldb::WriteBatch::default();
            for (bucket, layer) in hanging_buckets.keys() {
                let meta_key = CoverageHeader::accumulator_meta(
                    AccumulatorType::Current, *bucket, *layer,
                ).db_key();
                batch.delete(meta_key.as_bytes());
            }
            if let Err(e) = db.write(batch, true) {
                error!("Failed to delete meta keys for {}: {}", station_name, e);
            }
            if let Err(e) = db.flush() {
                error!("Failed to flush DB for {}: {}", station_name, e);
            }

            log_progress(&completed);

            (migrated, rolled_up, arrow, deleted)
        });

        tasks.push(handle);
    }

    let mut total_migrated = 0usize;
    let mut total_rolled_up = 0usize;
    let mut total_arrow = 0usize;
    let mut total_deleted = 0usize;

    let station_timeout = std::time::Duration::from_secs(300);
    for handle in tasks {
        match tokio::time::timeout(station_timeout, handle).await {
            Ok(Ok((migrated, rolled_up, arrow, deleted))) => {
                total_migrated += migrated;
                total_rolled_up += rolled_up;
                total_arrow += arrow;
                total_deleted += deleted;
            }
            Ok(Err(e)) => {
                error!("Startup rollup task panicked: {}", e);
            }
            Err(_) => {
                error!("Startup rollup task timed out after {}s — possible corrupt DB",
                    station_timeout.as_secs());
            }
        }
    }

    let elapsed = startup_start.elapsed().as_secs_f64();
    let speed = if elapsed > 0.0 { total_stations as f64 / elapsed } else { 0.0 };
    info!(
        "startup:100% [{}/{}] {:.0}s elapsed, {:.1}/s — {} written, {} arrow, {} deleted, {} legacy migrated",
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

/// Migrate legacy unprefixed keys to layer-prefixed format.
///
/// Legacy keys: "0042/8828308283fffff" → prefixed: "c/0042/8828308283fffff"
/// If a prefixed key already exists, merge via rollup.
fn migrate_legacy_keys(
    db: &mut rusty_leveldb::DB,
    accumulators: &Accumulators,
    _current_bucket: AccumulatorBucket,
) -> usize {
    let acc_entries = [
        (AccumulatorType::Current, &accumulators.current),
        (AccumulatorType::Day, &accumulators.day),
        (AccumulatorType::Month, &accumulators.month),
        (AccumulatorType::Year, &accumulators.year),
        (AccumulatorType::YearNz, &accumulators.yearnz),
    ];

    let mut batch = rusty_leveldb::WriteBatch::default();
    let mut migrated = 0;

    for (acc_type, entry) in &acc_entries {
        let tb = AccumulatorTypeAndBucket::new(*acc_type, entry.bucket);
        let legacy_prefix = format!("{}/8", tb.to_hex());
        let legacy_end = format!("{}/9", tb.to_hex());

        let legacy_records = db::read_range(db, &legacy_prefix, &legacy_end, None);

        // Delete legacy meta key
        let legacy_meta = CoverageHeader::legacy_meta_key(*acc_type, entry.bucket);
        batch.delete(legacy_meta.as_bytes());

        for (legacy_key, legacy_value) in &legacy_records {
            // Skip meta keys
            if legacy_key.contains("00_meta") {
                batch.delete(legacy_key.as_bytes());
                continue;
            }

            // Extract H3 from legacy key
            let h3 = match legacy_key.rsplit('/').next() {
                Some(h3) => h3,
                None => continue,
            };

            // Build prefixed key (combined layer for legacy)
            let prefixed_key = make_dest_key(*acc_type, entry.bucket, Layer::Combined, h3);

            // Check DB for existing prefixed key
            let final_value = if let Some(existing_bytes) = db.get(prefixed_key.as_bytes()) {
                if let (Some(existing_rec), Some(legacy_rec)) = (
                    CoverageRecord::from_bytes(&existing_bytes),
                    CoverageRecord::from_bytes(legacy_value),
                ) {
                    existing_rec
                        .rollup(&legacy_rec, None)
                        .map(|r| r.to_bytes())
                        .unwrap_or_else(|| legacy_value.clone())
                } else {
                    legacy_value.clone()
                }
            } else {
                legacy_value.clone()
            };

            batch.put(prefixed_key.as_bytes(), &final_value);
            batch.delete(legacy_key.as_bytes());
            migrated += 1;
        }
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
    let output_dir = if station_name == "global" {
        format!("{}global", *OUTPUT_PATH)
    } else {
        format!("{}stations/{}", *OUTPUT_PATH, station_name)
    };
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

        let stats = rollup_station_layer(
            &mut db, &station_path, "test_station", &accumulators,
            Layer::Combined, ".combined", None, false, None, &[],
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

        let stats = rollup_station_layer(
            &mut db, &station_path, "test_station", &accumulators,
            Layer::Combined, ".combined", None, false, None, &[],
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
