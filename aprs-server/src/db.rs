//! LevelDB abstraction layer.
//!
//! Provides `TrackedDb` (RAII wrapper with open-count limiting), shared iteration
//! helpers, and `Storage` for per-station database paths and batch writes.

use rusty_leveldb::LdbIterator;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex};
use tracing::error;

use crate::config::{DB_PATH, MAX_STATION_DBS};
use crate::coverage::record::CoverageRecord;

// ---- Global concurrent-open limit ----
//
// Each open LevelDB holds file handles for its WAL log, MANIFEST, and up to
// `max_open_files - 10` SST table files. Letting too many databases open
// simultaneously exhausts the OS file-descriptor limit, which can corrupt
// databases that are mid-write when the error occurs.
//
// The counter is incremented in TrackedDb::open (after the limit check) and
// decremented in Drop, so it naturally limits concurrent opens across rollup,
// H3 cache flush, and any other caller without needing per-call coordination.

static DB_OPEN: std::sync::LazyLock<(Mutex<usize>, Condvar)> =
    std::sync::LazyLock::new(|| (Mutex::new(0), Condvar::new()));

// ---- TrackedDb: RAII wrapper for LevelDB with open-count limiting ----

/// A LevelDB handle that blocks on open when the global limit is reached and
/// automatically decrements the count on drop.
pub struct TrackedDb {
    db: rusty_leveldb::DB,
}

impl TrackedDb {
    /// Open a LevelDB database, blocking if the global open limit is reached.
    pub fn open(path: &str, create_if_missing: bool) -> Result<Self, rusty_leveldb::Status> {
        let (lock, condvar) = &*DB_OPEN;
        let mut count = lock.lock().unwrap();
        while *count >= *MAX_STATION_DBS {
            tracing::warn!("DB open limit ({}) reached — waiting", *MAX_STATION_DBS);
            count = condvar.wait(count).unwrap();
        }
        *count += 1;
        drop(count);

        let mut opts = rusty_leveldb::Options::default();
        opts.create_if_missing = create_if_missing;
        opts.max_open_files = 40;
        match rusty_leveldb::DB::open(path, opts) {
            Ok(db) => Ok(TrackedDb { db }),
            Err(e) => {
                *DB_OPEN.0.lock().unwrap() -= 1;
                DB_OPEN.1.notify_one();
                Err(e)
            }
        }
    }
}

impl std::ops::Deref for TrackedDb {
    type Target = rusty_leveldb::DB;
    fn deref(&self) -> &Self::Target { &self.db }
}

impl std::ops::DerefMut for TrackedDb {
    fn deref_mut(&mut self) -> &mut Self::Target { &mut self.db }
}

impl Drop for TrackedDb {
    fn drop(&mut self) {
        *DB_OPEN.0.lock().unwrap() -= 1;
        DB_OPEN.1.notify_one();
    }
}

// ---- Shared iteration helpers ----

/// Read key-value pairs within a key range from an open DB.
/// Checks `shutdown` flag (if provided) to exit early.
/// Guards against stuck iterators.
pub fn read_range(
    db: &mut rusty_leveldb::DB,
    start_key: &str,
    end_key: &str,
    shutdown: Option<&AtomicBool>,
) -> Vec<(String, Vec<u8>)> {
    let mut iter = match db.new_iter() {
        Ok(iter) => iter,
        Err(_) => return Vec::new(),
    };
    iter.seek(start_key.as_bytes());
    let end_bytes = end_key.as_bytes();
    let mut results = Vec::new();
    let mut prev_key: Option<Vec<u8>> = None;
    let mut scanned: usize = 0;
    while let Some((key_bytes, val_bytes)) = iter.current() {
        if let Some(flag) = shutdown {
            if flag.load(Ordering::Relaxed) {
                tracing::warn!(
                    "db_read_range: shutdown requested after scanning {} keys ({}..{}), returning {} results",
                    scanned, start_key, end_key, results.len()
                );
                break;
            }
        }
        if key_bytes.as_ref() >= end_bytes {
            break;
        }
        scanned += 1;
        if let Some(ref pk) = prev_key {
            if pk.as_slice() == key_bytes.as_ref() {
                error!(
                    "db_read_range: iterator stuck at key {:?} after {} keys, aborting",
                    String::from_utf8_lossy(&key_bytes), scanned
                );
                break;
            }
        }
        prev_key = Some(key_bytes.to_vec());
        if let Ok(key_str) = std::str::from_utf8(&key_bytes) {
            if key_str >= start_key {
                results.push((key_str.to_string(), val_bytes.to_vec()));
            }
        }
        if !iter.advance() {
            break;
        }
    }
    results
}

/// Delete all keys within a range from an open DB (data + meta).
/// Returns the number of keys deleted.
pub fn delete_range(
    db: &mut rusty_leveldb::DB,
    start_key: &str,
    end_key: &str,
) -> usize {
    delete_ranges(db, &[(start_key.to_string(), end_key.to_string())])
}

/// Delete all keys within multiple ranges using a single iterator pass.
/// Ranges must be non-overlapping. They will be sorted internally.
/// Returns the total number of keys deleted.
pub fn delete_ranges(
    db: &mut rusty_leveldb::DB,
    ranges: &[(String, String)],
) -> usize {
    if ranges.is_empty() {
        return 0;
    }

    let mut sorted_ranges: Vec<&(String, String)> = ranges.iter().collect();
    sorted_ranges.sort_by(|a, b| a.0.cmp(&b.0));

    let mut iter = match db.new_iter() {
        Ok(iter) => iter,
        Err(_) => return 0,
    };

    let mut keys_to_delete: Vec<Vec<u8>> = Vec::new();

    for (start_key, end_key) in &sorted_ranges {
        iter.seek(start_key.as_bytes());
        let end_bytes = end_key.as_bytes();
        let mut prev_key: Option<Vec<u8>> = None;

        while let Some((key_bytes, _val_bytes)) = iter.current() {
            if key_bytes.as_ref() >= end_bytes {
                break;
            }
            if let Some(ref pk) = prev_key {
                if pk.as_slice() == key_bytes.as_ref() {
                    error!(
                        "delete_ranges: iterator stuck at key {:?}, aborting range",
                        String::from_utf8_lossy(&key_bytes)
                    );
                    break;
                }
            }
            prev_key = Some(key_bytes.to_vec());
            if key_bytes.as_ref() >= start_key.as_bytes() {
                keys_to_delete.push(key_bytes.to_vec());
            }
            if !iter.advance() {
                break;
            }
        }
    }
    drop(iter);

    let count = keys_to_delete.len();
    if count > 0 {
        let mut batch = rusty_leveldb::WriteBatch::default();
        for key in &keys_to_delete {
            batch.delete(key);
        }
        if let Err(e) = db.write(batch, true) {
            error!("delete_ranges failed: {}", e);
        }
    }
    count
}

/// Read all key-value pairs from an already-open database (full scan).
pub fn read_all(db: &mut rusty_leveldb::DB) -> Vec<(String, Vec<u8>)> {
    let mut iter = match db.new_iter() {
        Ok(iter) => iter,
        Err(_) => return Vec::new(),
    };

    let mut results = Vec::new();

    iter.seek(&[]);
    while let Some((key_bytes, val_bytes)) = iter.current() {
        if let Ok(key_str) = std::str::from_utf8(&key_bytes) {
            results.push((key_str.to_string(), val_bytes.to_vec()));
        }
        if !iter.advance() {
            break;
        }
    }

    results
}

// ---- Storage struct ----

/// Storage manages station database paths and provides convenience methods.
pub struct Storage {
    db_path: PathBuf,
}

impl Storage {
    pub fn new() -> Self {
        let db_path = PathBuf::from(format!("{}stations", *DB_PATH));

        if let Err(e) = std::fs::create_dir_all(&db_path) {
            error!("Failed to create stations directory {:?}: {}", db_path, e);
        }

        Storage { db_path }
    }

    /// Write a batch of key-value pairs to a station's database.
    /// For each (db_key, record_bytes) pair, merges with existing data if present.
    pub async fn write_batch(
        &self,
        station_name: &str,
        records: &[(String, Vec<u8>)],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if records.is_empty() {
            return Ok(());
        }

        let station_path_str = self.station_path(station_name).to_string_lossy().to_string();
        let records_owned: Vec<(String, Vec<u8>)> = records.to_vec();

        tokio::task::spawn_blocking(move || {
            let mut db = match TrackedDb::open(&station_path_str, true) {
                Ok(db) => db,
                Err(e) => {
                    error!(
                        "Failed to open DB for {}: {} — {} databases currently open",
                        station_path_str, e, *DB_OPEN.0.lock().unwrap()
                    );
                    return;
                }
            };

            for (key, new_data) in &records_owned {
                let existing = db.get(key.as_bytes());

                let merged = if key.contains("00_meta") {
                    merge_meta_json(existing.as_deref(), new_data)
                } else if let Some(existing_bytes) = existing {
                    if let Some(existing_record) = CoverageRecord::from_bytes(&existing_bytes) {
                        if let Some(new_record) = CoverageRecord::from_bytes(new_data) {
                            match existing_record.rollup(&new_record, None) {
                                Some(merged) => merged.to_bytes(),
                                None => new_data.clone(),
                            }
                        } else {
                            new_data.clone()
                        }
                    } else {
                        new_data.clone()
                    }
                } else {
                    new_data.clone()
                };

                if let Err(e) = db.put(key.as_bytes(), &merged) {
                    error!("Failed to write key {} to {}: {}", key, station_path_str, e);
                }
            }

            if let Err(e) = db.flush() {
                error!("Failed to flush DB for {}: {}", station_path_str, e);
            }
        })
        .await?;

        Ok(())
    }

    /// Get the path for a station's database directory.
    /// Global lives at {DB_PATH}global, stations at {DB_PATH}stations/{name}.
    pub fn station_path(&self, station_name: &str) -> PathBuf {
        if station_name == "global" {
            PathBuf::from(format!("{}global", *DB_PATH))
        } else {
            self.db_path.join(station_name)
        }
    }

    /// Purge a station's database by removing its directory.
    pub fn purge_station(&self, station_name: &str) {
        let station_path = self.station_path(station_name);
        if station_path.exists() {
            if let Err(e) = std::fs::remove_dir_all(&station_path) {
                error!("Failed to purge station DB {:?}: {}", station_path, e);
            }
        }
    }
}

const MAX_ALL_STARTS: usize = 250;

/// Merge a new meta JSON with existing meta JSON, appending to allStarts
/// and capping at MAX_ALL_STARTS entries. Matches saveAccumulatorMetadata in TypeScript.
fn merge_meta_json(existing: Option<&[u8]>, new_data: &[u8]) -> Vec<u8> {
    let mut new_meta: serde_json::Value = match serde_json::from_slice(new_data) {
        Ok(v) => v,
        Err(_) => return new_data.to_vec(),
    };

    if let Some(existing_bytes) = existing {
        if let Ok(existing_meta) = serde_json::from_slice::<serde_json::Value>(existing_bytes) {
            if let Some(existing_starts) = existing_meta.get("allStarts").and_then(|v| v.as_array()) {
                if let Some(new_starts) = new_meta.get("allStarts").and_then(|v| v.as_array()).cloned() {
                    let mut merged: Vec<serde_json::Value> = existing_starts.clone();
                    merged.extend(new_starts);
                    if merged.len() > MAX_ALL_STARTS {
                        merged.drain(..merged.len() - MAX_ALL_STARTS);
                    }
                    new_meta["allStarts"] = serde_json::Value::Array(merged);
                }
            }
        }
    }

    serde_json::to_vec(&new_meta).unwrap_or_else(|_| new_data.to_vec())
}

/// Build and merge accumulator metadata JSON, matching saveAccumulatorMetadata in TypeScript.
/// Used by rollup to update destination accumulator meta entries.
pub fn build_accumulator_meta(
    existing: Option<&[u8]>,
    accumulators: &serde_json::Value,
    current_bucket: u16,
) -> Vec<u8> {
    let now = chrono::Utc::now();
    let now_secs = now.timestamp() as u32;
    let now_utc = now.to_rfc3339();

    let new_start = serde_json::json!({"start": now_secs, "startUtc": now_utc});

    let mut all_starts: Vec<serde_json::Value> = if let Some(existing_bytes) = existing {
        if let Ok(existing_meta) = serde_json::from_slice::<serde_json::Value>(existing_bytes) {
            existing_meta.get("allStarts")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    all_starts.push(new_start);
    if all_starts.len() > MAX_ALL_STARTS {
        all_starts.drain(..all_starts.len() - MAX_ALL_STARTS);
    }

    let mut meta = if let Some(existing_bytes) = existing {
        serde_json::from_slice::<serde_json::Value>(existing_bytes)
            .unwrap_or_else(|_| serde_json::json!({}))
    } else {
        serde_json::json!({})
    };

    meta["start"] = serde_json::json!(now_secs);
    meta["startUtc"] = serde_json::json!(now_utc);
    meta["accumulators"] = accumulators.clone();
    meta["currentAccumulator"] = serde_json::json!(current_bucket);
    meta["allStarts"] = serde_json::Value::Array(all_starts);

    serde_json::to_vec(&meta).unwrap_or_default()
}
