//! LevelDB storage layer for station H3 databases.
//!
//! Each station has its own LevelDB database under DB_PATH/stations/STATION_NAME/.
//! Mirrors the worker thread storage from the TypeScript codebase.

use rusty_leveldb::LdbIterator;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Mutex;
use tracing::error;

use crate::config::DB_PATH;
use crate::coverage::record::CoverageRecord;

/// Track which station DB paths are currently open (for debugging LockErrors)
static OPEN_DBS: std::sync::LazyLock<Mutex<std::collections::HashMap<String, &'static str>>> =
    std::sync::LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));

pub fn track_db_open(path: &str, operation: &'static str) {
    let mut open = OPEN_DBS.lock().unwrap();
    if let Some(existing_op) = open.insert(path.to_string(), operation) {
        tracing::error!(
            "BUG: DB {} opened for '{}' but already open for '{}'",
            path, operation, existing_op
        );
    }
}

pub fn track_db_close(path: &str) {
    OPEN_DBS.lock().unwrap().remove(path);
}

pub fn open_dbs_summary() -> Vec<(String, &'static str)> {
    OPEN_DBS.lock().unwrap().iter().map(|(k, v)| (k.clone(), *v)).collect()
}

/// Storage manages station database paths and provides convenience methods.
pub struct Storage {
    db_path: PathBuf,
}

impl Storage {
    pub fn new() -> Self {
        let db_path = PathBuf::from(format!("{}stations", *DB_PATH));

        // Ensure base directory exists
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
            track_db_open(&station_path_str, "write_batch");

            let mut opts = rusty_leveldb::Options::default();
            opts.create_if_missing = true;

            let mut db = match rusty_leveldb::DB::open(&station_path_str, opts) {
                Ok(db) => db,
                Err(e) => {
                    error!(
                        "Failed to open DB for {}: {} — currently open: {:?}",
                        station_path_str, e, open_dbs_summary()
                    );
                    track_db_close(&station_path_str);
                    return;
                }
            };

            for (key, new_data) in &records_owned {
                let existing = db.get(key.as_bytes());

                let merged = if key.contains("00_meta") {
                    // Meta keys are JSON — merge allStarts from existing
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

                let _ = db.put(key.as_bytes(), &merged);
            }

            if let Err(e) = db.flush() {
                error!("Failed to flush DB for {}: {}", station_path_str, e);
            }
            drop(db);
            OPEN_DBS.lock().unwrap().remove(&station_path_str);
        })
        .await?;

        Ok(())
    }

    /// Read a single record from a station's database
    pub async fn read(
        &self,
        station_name: &str,
        db_key: &str,
    ) -> Option<CoverageRecord> {
        let station_path = self.station_path(station_name).to_string_lossy().to_string();
        let key = db_key.to_string();

        tokio::task::spawn_blocking(move || {
            let mut opts = rusty_leveldb::Options::default();
            opts.create_if_missing = false;

            let mut db = rusty_leveldb::DB::open(&station_path, opts).ok()?;
            let data = db.get(key.as_bytes())?;
            CoverageRecord::from_bytes(&data)
        })
        .await
        .ok()
        .flatten()
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

    /// Read all key-value pairs in a range from a station's database.
    /// Returns (db_key, raw_bytes) pairs sorted by key.
    pub fn read_range_sync(
        station_path: &str,
        start_key: &str,
        end_key: &str,
    ) -> Vec<(String, Vec<u8>)> {
        let mut opts = rusty_leveldb::Options::default();
        opts.create_if_missing = false;

        let mut db = match rusty_leveldb::DB::open(station_path, opts) {
            Ok(db) => db,
            Err(e) => {
                error!("Failed to open DB for range read {}: {}", station_path, e);
                return Vec::new();
            }
        };

        let mut iter = match db.new_iter() {
            Ok(iter) => iter,
            Err(_) => return Vec::new(),
        };

        // Seek to start
        iter.seek(start_key.as_bytes());

        let mut results = Vec::new();
        let mut key_buf = Vec::new();
        let mut val_buf = Vec::new();
        let end_bytes = end_key.as_bytes();

        while iter.current(&mut key_buf, &mut val_buf) {
            if key_buf.as_slice() >= end_bytes {
                break;
            }
            if let Ok(key_str) = std::str::from_utf8(&key_buf) {
                if key_str >= start_key {
                    results.push((key_str.to_string(), val_buf.clone()));
                }
            }
            if !iter.advance() {
                break;
            }
        }

        results
    }

    /// Read all key-value pairs from a database (full scan).
    pub fn read_all_sync(station_path: &str) -> Vec<(String, Vec<u8>)> {
        let mut opts = rusty_leveldb::Options::default();
        opts.create_if_missing = false;

        track_db_open(station_path, "read_all_sync");
        let mut db = match rusty_leveldb::DB::open(station_path, opts) {
            Ok(db) => db,
            Err(_) => {
                track_db_close(station_path);
                return Vec::new();
            }
        };

        let mut iter = match db.new_iter() {
            Ok(iter) => iter,
            Err(_) => return Vec::new(),
        };

        let mut results = Vec::new();
        let mut key_buf = Vec::new();
        let mut val_buf = Vec::new();

        iter.seek(&[]);
        while iter.current(&mut key_buf, &mut val_buf) {
            if let Ok(key_str) = std::str::from_utf8(&key_buf) {
                results.push((key_str.to_string(), val_buf.clone()));
            }
            if !iter.advance() {
                break;
            }
        }

        drop(iter);
        drop(db);
        track_db_close(station_path);
        results
    }

    /// Apply a batch of put/delete operations to a station database.
    pub fn apply_batch_sync(
        station_path: &str,
        puts: &[(String, Vec<u8>)],
        deletes: &[String],
    ) -> Result<(), String> {
        let mut opts = rusty_leveldb::Options::default();
        opts.create_if_missing = true;

        let mut db = rusty_leveldb::DB::open(station_path, opts)
            .map_err(|e| format!("Failed to open DB: {}", e))?;

        for key in deletes {
            let _ = db.delete(key.as_bytes());
        }
        for (key, value) in puts {
            let _ = db.put(key.as_bytes(), value);
        }
        db.flush()
            .map_err(|e| format!("Failed to flush DB: {}", e))?;
        Ok(())
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
