//! Diagnostic tool: scans all station DBs and reports
//! hanging/orphaned/stale accumulators using the same logic as rollup_startup.
//!
//! Usage: dbscan [--rollup] [station_name]
//!   --rollup   Scan, run startup rollup, then scan again to compare
//!   station    Only scan this station (optional)

// Share all modules with the main binary
#[path = "../accumulators.rs"] mod accumulators;
#[path = "../config.rs"] mod config;
#[path = "../coverage/mod.rs"] mod coverage;
#[path = "../db.rs"] mod db;
#[path = "../layers.rs"] mod layers;
#[path = "../types.rs"] mod types;
#[path = "../station.rs"] mod station;
#[path = "../packet_stats.rs"] mod packet_stats;
#[path = "../stats_accumulator.rs"] mod stats_accumulator;
#[path = "../bitvec.rs"] mod bitvec;
#[path = "../json_io.rs"] mod json_io;
#[path = "../symlinks.rs"] mod symlinks;
#[path = "../rollup.rs"] mod rollup;
#[path = "../stationfile.rs"] mod stationfile;
#[path = "../ignore_station.rs"] mod ignore_station;
#[path = "../elevation.rs"] mod elevation;
#[path = "../h3cache.rs"] mod h3cache;

use std::collections::HashMap;
use coverage::header::{AccumulatorBucket, AccumulatorType, AccumulatorTypeAndBucket, CoverageHeader};
use db::TrackedDb;
use layers::Layer;
use rusty_leveldb::LdbIterator;

#[tokio::main]
async fn main() {
    let _ = dotenvy::from_filename(".env.local");

    let args: Vec<String> = std::env::args().collect();
    let do_rollup = args.iter().any(|a| a == "--rollup");
    let filter_station: Option<String> = args.iter()
        .filter(|a| !a.starts_with('-') && *a != &args[0])
        .next()
        .cloned();

    let stations_dir = format!("{}stations", *config::DB_PATH);
    println!("DB_PATH={} stations_dir={}", *config::DB_PATH, stations_dir);

    let expected = accumulators::initialise_accumulators();
    let (exp_text, exp_files) = expected.describe();
    println!("Expected accumulators: {}/{}", exp_text, exp_files);
    println!("  day={:04x} month={:04x} year={:04x} yearnz={:04x}",
        expected.day.bucket.0, expected.month.bucket.0,
        expected.year.bucket.0, expected.yearnz.bucket.0);
    println!();

    let station_dirs = enumerate_stations(&stations_dir, &filter_station);

    println!("=== BEFORE ===");
    scan_all(&station_dirs, &stations_dir, &expected);

    if do_rollup {
        println!();
        println!("=== RUNNING STARTUP ROLLUP ===");
        // Init tracing so rollup logs are visible
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
            .with_target(false)
            .init();

        let case_insensitive = std::path::Path::new("CARGO.TOML").exists()
            && std::path::Path::new("Cargo.toml").exists();
        let station_manager = station::StationManager::new(case_insensitive);
        if let Some(ref name) = filter_station {
            let n = name.clone();
            station_manager.retain(|s| s.as_str() == n);
            println!("Filtered station manager to: {}", name);
        }
        let storage = db::Storage::new();
        let acc = accumulators::initialise_accumulators();
        rollup::rollup_startup(&storage, &station_manager, &acc).await;

        println!();
        println!("=== AFTER ===");
        let expected_after = accumulators::initialise_accumulators();
        scan_all(&station_dirs, &stations_dir, &expected_after);
    }
}

fn enumerate_stations(stations_dir: &str, filter: &Option<String>) -> Vec<String> {
    let mut station_dirs: Vec<String> = Vec::new();

    let global_path = format!("{}global", *config::DB_PATH);
    if std::path::Path::new(&global_path).exists() {
        station_dirs.push("global".to_string());
    }

    if let Ok(entries) = std::fs::read_dir(stations_dir) {
        for entry in entries.flatten() {
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                if let Some(name) = entry.file_name().to_str() {
                    station_dirs.push(name.to_string());
                }
            }
        }
    }
    station_dirs.sort();

    if let Some(ref f) = filter {
        station_dirs.retain(|s| s == f);
    }

    station_dirs
}

fn scan_all(station_dirs: &[String], stations_dir: &str, expected: &accumulators::Accumulators) {
    let layers: Vec<Layer> = if let Some(ref enabled) = *config::ENABLED_LAYERS {
        enabled.iter().copied().collect()
    } else {
        layers::ALL_LAYERS.to_vec()
    };

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

    println!("Scanning {} station DBs...", station_dirs.len());

    let mut total_hanging = 0usize;
    let mut total_orphaned = 0usize;
    let mut total_stale = 0usize;

    for station_name in station_dirs {
        let station_path = if station_name == "global" {
            format!("{}global", *config::DB_PATH)
        } else {
            format!("{}/{}", stations_dir, station_name)
        };

        let mut db = match TrackedDb::open(&station_path, false, 4 * 1024 * 1024) {
            Ok(db) => db,
            Err(e) => {
                eprintln!("{}: ERROR opening: {}", station_name, e);
                continue;
            }
        };

        let mut hanging_buckets: HashMap<(AccumulatorBucket, Layer), accumulators::Accumulators> = HashMap::new();
        let mut orphaned: Vec<String> = Vec::new();
        let mut stale: Vec<(AccumulatorType, AccumulatorBucket, Layer, String)> = Vec::new();
        let mut all_accumulators: Vec<(AccumulatorType, AccumulatorBucket, Layer, String, usize)> = Vec::new();
        let mut all_keys: Vec<String> = Vec::new();

        let mut iter = match db.new_iter() {
            Ok(iter) => iter,
            Err(e) => {
                eprintln!("{}: ERROR creating iterator: {:?}", station_name, e);
                continue;
            }
        };
        iter.seek(&[]);

        while let Some((key_bytes, val_bytes)) = iter.current() {
            let key_str = match std::str::from_utf8(&key_bytes) {
                Ok(s) => s.to_string(),
                Err(_) => {
                    if !iter.advance() { break; }
                    continue;
                }
            };

            let header = match CoverageHeader::from_db_key(&key_str) {
                Some(h) => h,
                None => {
                    if !iter.advance() { break; }
                    continue;
                }
            };

            let acc_type = header.accumulator_type();
            let bucket = header.bucket();
            let layer = header.layer;
            let (_, seek_end) = CoverageHeader::db_search_range(acc_type, bucket, layer);

            if !header.is_meta() {
                let mut key_count = 0usize;
                loop {
                    key_count += 1;
                    if !iter.advance() { break; }
                    match iter.current() {
                        Some((k, _)) if k.as_ref() < seek_end.as_bytes() => continue,
                        _ => break,
                    }
                }
                orphaned.push(format!(
                    "{}: ORPHANED data: key={} decoded_as={}/{}/{:04x} key_count={}",
                    station_name, key_str, layer.name(), acc_type.name(), bucket.0, key_count
                ));
                continue;
            }

            // Count data keys after this meta
            let key_count = {
                let mut count = 0usize;
                let (data_start, _) = CoverageHeader::db_search_range(acc_type, bucket, layer);
                let mut tmp_iter = db.new_iter().ok();
                if let Some(ref mut it) = tmp_iter {
                    it.seek(data_start.as_bytes());
                    while let Some((k, _)) = it.current() {
                        if k.as_ref() >= seek_end.as_bytes() { break; }
                        count += 1;
                        if !it.advance() { break; }
                    }
                }
                count
            };

            all_keys.push(key_str.clone());

            if acc_type == AccumulatorType::Current {
                if layers.contains(&layer) {
                    if let Ok(meta) = serde_json::from_slice::<serde_json::Value>(&val_bytes) {
                        if let Some(acc) = rollup_parse_accumulators_from_meta(&meta) {
                            let matches_expected = bucket == expected.current.bucket
                                && acc.day.bucket == expected.day.bucket
                                && acc.month.bucket == expected.month.bucket
                                && acc.year.bucket == expected.year.bucket
                                && acc.yearnz.bucket == expected.yearnz.bucket;

                            if !matches_expected {
                                let (desc, files) = acc.describe();
                                println!("{}: HANGING current: key={} bucket={:04x} layer={} key_count={} acc={}/{}",
                                    station_name, key_str, bucket.0, layer.name(), key_count, desc, files);
                                println!("{}:   expected: current={:04x} day={:04x} month={:04x} year={:04x} yearnz={:04x}",
                                    station_name,
                                    expected.current.bucket.0, expected.day.bucket.0,
                                    expected.month.bucket.0, expected.year.bucket.0, expected.yearnz.bucket.0);
                                println!("{}:   stored:   current={:04x} day={:04x} month={:04x} year={:04x} yearnz={:04x}",
                                    station_name, bucket.0, acc.day.bucket.0, acc.month.bucket.0,
                                    acc.year.bucket.0, acc.yearnz.bucket.0);
                                hanging_buckets.insert((bucket, layer), acc);
                            }
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
                        all_accumulators.push((acc_type, bucket, layer, file, key_count));
                    }
                    None => {
                        let meta_debug = serde_json::from_slice::<serde_json::Value>(&val_bytes)
                            .ok()
                            .map(|meta| {
                                let type_name = acc_type.name();
                                format!("has_accumulators={} has_type={} keys={:?}",
                                    meta.get("accumulators").is_some(),
                                    meta.get("accumulators").and_then(|a| a.get(type_name)).is_some(),
                                    meta.get("accumulators").map(|a| {
                                        if let Some(obj) = a.as_object() {
                                            obj.keys().cloned().collect::<Vec<_>>()
                                        } else {
                                            vec!["NOT_OBJECT".to_string()]
                                        }
                                    })
                                )
                            })
                            .unwrap_or_else(|| "unparseable".to_string());
                        println!("{}: INVALID META: key={} decoded_as={}/{}/{:04x} {}",
                            station_name, key_str, layer.name(), acc_type.name(), bucket.0, meta_debug);
                    }
                }
            }

            iter.seek(seek_end.as_bytes());
        }
        drop(iter);

        // Check non-current accumulators against expected buckets
        for (acc_type, bucket, layer, file, key_count) in &all_accumulators {
            if let Some(expected_bucket) = expected_buckets.get(&(*acc_type, *layer)) {
                if bucket != expected_bucket {
                    let tb = AccumulatorTypeAndBucket::new(*acc_type, *bucket);
                    let actual_key = format!("{}{}/00_meta", layer.db_prefix(), tb.to_hex());
                    let (range_start, range_end) = CoverageHeader::db_search_range_with_meta(*acc_type, *bucket, *layer);
                    stale.push((*acc_type, *bucket, *layer, file.clone()));
                    println!("{}: STALE dest: {}/{}/{:04x}(expected {:04x}) key_count={} actual_key={} delete_range={}..{}",
                        station_name, layer.name(), file, bucket.0, expected_bucket.0,
                        key_count, actual_key, range_start, range_end);
                    let key_exists = all_keys.contains(&actual_key);
                    println!("{}:   key_in_scan={} all_meta_keys_for_layer=[{}]",
                        station_name, key_exists,
                        all_keys.iter()
                            .filter(|k| k.starts_with(layer.db_prefix()))
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(", "));
                }
            }
        }

        let has_issues = !hanging_buckets.is_empty() || !orphaned.is_empty() || !stale.is_empty();
        if has_issues {
            total_hanging += hanging_buckets.len();
            for o in &orphaned {
                println!("{}", o);
                total_orphaned += 1;
            }
            total_stale += stale.len();
        }
    }

    println!("Stations scanned: {} | Hanging: {} | Orphaned: {} | Stale: {}",
        station_dirs.len(), total_hanging, total_orphaned, total_stale);
}

fn rollup_parse_accumulators_from_meta(meta: &serde_json::Value) -> Option<accumulators::Accumulators> {
    let acc = meta.get("accumulators")?;
    Some(accumulators::Accumulators {
        current: parse_acc_entry(acc.get("current")?)?,
        day: parse_acc_entry(acc.get("day")?)?,
        month: parse_acc_entry(acc.get("month")?)?,
        year: parse_acc_entry(acc.get("year")?)?,
        yearnz: parse_acc_entry(acc.get("yearnz")?)?,
    })
}

fn parse_acc_entry(v: &serde_json::Value) -> Option<accumulators::AccumulatorEntry> {
    Some(accumulators::AccumulatorEntry {
        bucket: AccumulatorBucket(v.get("bucket")?.as_u64()? as u16),
        file: v.get("file")?.as_str().unwrap_or("").to_string(),
        effective_start: types::Epoch(v.get("effectiveStart")?.as_u64().unwrap_or(0) as u32),
    })
}
