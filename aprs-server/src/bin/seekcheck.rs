//! Read-only blast-radius detector for the `TableIterator::seek` gap bug.
//!
//! For each station DB it reads the truth ONCE via a full linear scan
//! (seek-to-start + advance — the path NOT affected by the bug, which correctly
//! suppresses tombstoned keys), then re-reads every accumulator range using the
//! vulnerable boundary `seek(range_start)` + forward scan that the rollup uses
//! (current-bucket read, destination merge, delete_ranges). Any divergence is
//! the bug's actual impact:
//!
//!   RESURRECTED = key returned by the boundary seek but absent from the linear
//!                 truth -> a deleted row read as live (rolled forward / merged).
//!   DROPPED     = key in the linear truth but missing from the boundary seek
//!                 -> a live row silently skipped (under-read / under-delete).
//!
//! Run this against the CURRENT (pre-fix) binary to measure historical
//! exposure; once the seek fix is deployed it reports zero.
//!
//! Usage: seekcheck [station_name] [--ghost-data] [--deep] [--skip-global] [--limit=N]
//!   --ghost-data     CHEAP fleet census: hop-scan metas only (skips bulk data)
//!                    and report stale accumulators (bucket != expected) that
//!                    still carry data keys - the only place the bug could
//!                    resurrect real coverage right now. Skips the truth scan.
//!   (default = fast) one linear pass per DB + first-key-after-seek divergence;
//!                    catches RESURRECTED (deleted-read-as-live) at every
//!                    boundary the rollup seeks to, and DROPPED keys at those
//!                    boundaries.
//!   --deep           adds a second pass that re-reads each accumulator window
//!                    to count the full magnitude of DROPPED/RESURRECTED keys
//!                    inside a single read (roughly doubles runtime).
//!   --skip-global    skip the (huge) global DB, which dominates runtime.
//!   --limit=N        only check the first N DBs (sampling).
//!   station_name     only check one station.
//!
//! Progress (station counter, rate, ETA, key count) prints to stderr; results
//! go to stdout.

// Shared modules included via #[path]; suppress dead-code/unused noise.
#![allow(unused)]

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

use std::collections::{BTreeSet, HashSet};
use std::io::Write;
use std::time::Instant;
use coverage::header::{AccumulatorBucket, AccumulatorType, CoverageHeader};
use db::TrackedDb;
use layers::Layer;
use rusty_leveldb::LdbIterator;

/// Whether a db key is a metadata key (vs a real H3 coverage record).
fn key_is_meta(key: &str) -> bool {
    CoverageHeader::from_db_key(key).map(|h| h.is_meta()).unwrap_or(false)
}

/// Boundary-seek read of [start, end): the exact pattern read_range /
/// destination merge / delete_ranges use (seek to a synthetic boundary, then
/// advance forward).
fn boundary_read(db: &mut TrackedDb, start: &str, end: &str) -> Option<BTreeSet<String>> {
    let mut iter = db.new_iter().ok()?;
    iter.seek(start.as_bytes());
    let end_b = end.as_bytes();
    let mut out = BTreeSet::new();
    let mut prev: Option<Vec<u8>> = None;
    while let Some((k, _)) = iter.current() {
        if k.as_ref() >= end_b {
            break;
        }
        if let Some(ref p) = prev {
            if p.as_slice() == k.as_ref() {
                break; // stuck-iterator guard, mirrors read_range
            }
        }
        prev = Some(k.to_vec());
        if let Ok(s) = std::str::from_utf8(&k) {
            if s >= start {
                out.insert(s.to_string());
            }
        }
        if !iter.advance() {
            break;
        }
    }
    Some(out)
}

/// Cheap accumulator enumeration via the scan-skip hop pattern (seek to start,
/// then hop by seeking to each accumulator's `.../9000...` end). This is the
/// buggy seek path, so it surfaces resurrected ghosts; it reads only metas and
/// skips bulk data, so it's fast even on the 100GB global DB.
fn hop_accumulators(db: &mut TrackedDb) -> Vec<(Layer, AccumulatorType, AccumulatorBucket)> {
    let mut out = Vec::new();
    let Ok(mut iter) = db.new_iter() else { return out };
    iter.seek(&[]);
    let mut prev: Option<Vec<u8>> = None;
    while let Some((kb, _)) = iter.current() {
        // Progress guard: the hop must move strictly forward.
        if let Some(ref p) = prev {
            if p.as_slice() >= kb.as_ref() {
                break;
            }
        }
        prev = Some(kb.to_vec());

        let Ok(key) = std::str::from_utf8(&kb) else {
            if !iter.advance() { break; }
            continue;
        };
        let Some(h) = CoverageHeader::from_db_key(key) else {
            if !iter.advance() { break; }
            continue;
        };
        let (t, b, l) = (h.accumulator_type(), h.bucket(), h.layer);
        if h.is_meta() {
            out.push((l, t, b));
        }
        // Hop past this accumulator's data range (the scan-skip seek).
        let (_, seek_end) = CoverageHeader::db_search_range(t, b, l);
        iter.seek(seek_end.as_bytes());
    }
    out
}

/// --ghost-data: cheap fleet-wide census of stale accumulators (bucket != the
/// expected current bucket for their type) that still carry data keys. Those
/// are the only ones where the seek bug could resurrect *real* coverage; a
/// stale accumulator with key_count==0 is harmless meta cruft.
fn run_ghost_data(stations: &[(String, String)]) {
    let expected = accumulators::initialise_accumulators();
    let expected_bucket = |t: AccumulatorType| -> Option<AccumulatorBucket> {
        match t {
            AccumulatorType::Day => Some(expected.day.bucket),
            AccumulatorType::Month => Some(expected.month.bucket),
            AccumulatorType::Year => Some(expected.year.bucket),
            AccumulatorType::YearNz => Some(expected.yearnz.bucket),
            _ => None, // Current / unknown: not a dest-rollup target, skip
        }
    };

    let total = stations.len();
    let run_start = Instant::now();
    let mut last_tick = Instant::now();
    let mut affected = 0usize;
    let mut total_accs = 0usize;
    let mut total_keys = 0usize;

    for (i, (name, path)) in stations.iter().enumerate() {
        if i == 0 || last_tick.elapsed().as_secs_f64() >= 2.0 {
            let el = run_start.elapsed().as_secs_f64().max(1e-6);
            let rate = i as f64 / el;
            let eta = if rate > 0.0 { (total - i) as f64 / rate } else { 0.0 };
            eprint!("\r[{}/{}] {:.0}s  {:.0} st/s  ETA {:.0}s  {:<24}", i, total, el, rate, eta, name);
            let _ = std::io::stderr().flush();
            last_tick = Instant::now();
        }

        let mut db = match TrackedDb::open(path, false) {
            Ok(d) => d,
            Err(_) => continue,
        };
        let accs = hop_accumulators(&mut db);
        let mut hits: Vec<(String, usize)> = Vec::new();
        for (l, t, b) in accs {
            let Some(eb) = expected_bucket(t) else { continue };
            if b == eb {
                continue; // current/expected bucket -> not stale
            }
            let (ds, de) = CoverageHeader::db_search_range(t, b, l);
            let cnt = boundary_read(&mut db, &ds, &de).map(|s| s.len()).unwrap_or(0);
            if cnt > 0 {
                hits.push((format!("{}/{}/{:04x}", l.name(), t.name(), b.0), cnt));
            }
        }
        if !hits.is_empty() {
            affected += 1;
            print!("\n{}:", name);
            for (desc, cnt) in &hits {
                print!("  {}={}", desc, cnt);
                total_accs += 1;
                total_keys += cnt;
            }
            println!();
        }
    }

    eprint!("\r{:80}\r", " ");
    println!("\n=== GHOST-DATA SUMMARY ({:.0}s) ===", run_start.elapsed().as_secs_f64());
    println!("Stations checked:                 {}", total);
    println!("Stations with stale data:         {}", affected);
    println!("Stale accumulators carrying data: {}", total_accs);
    println!("Total at-risk data keys:          {}", total_keys);
    if total_keys == 0 {
        println!("\nNo surviving stale accumulator carries data: live resurrection risk is meta-only.");
    } else {
        println!("\nThese stale accumulators still hold real coverage the buggy seek could resurrect.");
    }
}

fn main() {
    let _ = dotenvy::from_filename(".env.local");
    let args: Vec<String> = std::env::args().collect();
    let filter: Option<String> = args.iter().skip(1).find(|a| !a.starts_with('-')).cloned();
    // --deep also runs the (slower) second-pass window magnitude diff that
    // counts DROPPED (under-read) keys; default does the one-pass resurrection
    // check only.
    let deep = args.iter().any(|a| a == "--deep");
    // --skip-global: the global DB can be ~100GB and dominates runtime.
    let skip_global = args.iter().any(|a| a == "--skip-global");
    let limit: Option<usize> = args
        .iter()
        .find_map(|a| a.strip_prefix("--limit=").and_then(|n| n.parse().ok()));

    let stations_dir = format!("{}stations", *config::DB_PATH);
    println!("DB_PATH={}  mode={}", *config::DB_PATH, if deep { "deep" } else { "fast" });

    let mut station_dirs: Vec<(String, String)> = Vec::new();
    let global = format!("{}global", *config::DB_PATH);
    if !skip_global && std::path::Path::new(&global).exists() {
        station_dirs.push(("global".into(), global));
    }
    if let Ok(entries) = std::fs::read_dir(&stations_dir) {
        for e in entries.flatten() {
            if e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                if let Some(n) = e.file_name().to_str() {
                    station_dirs.push((n.to_string(), format!("{}/{}", stations_dir, n)));
                }
            }
        }
    }
    station_dirs.sort();
    if let Some(ref f) = filter {
        station_dirs.retain(|(n, _)| n == f);
    }
    if let Some(n) = limit {
        station_dirs.truncate(n);
    }

    // --ghost-data: cheap stale-accumulator-with-data census; skips the full
    // truth scan entirely.
    if args.iter().any(|a| a == "--ghost-data") {
        println!("Checking {} DBs for stale accumulators carrying data...\n", station_dirs.len());
        run_ghost_data(&station_dirs);
        return;
    }

    let total = station_dirs.len();
    println!("Checking {} station DBs...\n", total);

    let (mut tot_res_data, mut tot_res_meta) = (0usize, 0usize);
    let (mut tot_drop_data, mut tot_drop_meta) = (0usize, 0usize);
    let mut affected_stations = 0usize;
    let mut checked_ranges = 0usize;
    let mut total_keys: u64 = 0;
    let run_start = Instant::now();
    let mut last_tick = Instant::now();

    for (i, (name, path)) in station_dirs.iter().enumerate() {
        // Throttled progress line on stderr (so stdout stays results-only).
        if i == 0 || last_tick.elapsed().as_secs_f64() >= 2.0 {
            let el = run_start.elapsed().as_secs_f64().max(1e-6);
            let rate = i as f64 / el;
            let eta = if rate > 0.0 { (total - i) as f64 / rate } else { 0.0 };
            eprint!(
                "\r[{}/{}] {:.0}s  {:.0} st/s  ETA {:.0}s  {:.1}M keys  {:<24}",
                i, total, el, rate, eta, total_keys as f64 / 1e6, name
            );
            let _ = std::io::stderr().flush();
            last_tick = Instant::now();
        }

        let mut db = match TrackedDb::open(path, false) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("\n{}: open failed: {}", name, e);
                continue;
            }
        };

        // 1) Truth: one full linear scan (unaffected by the seek bug).
        let mut truth: Vec<String> = Vec::new();
        {
            let Ok(mut it) = db.new_iter() else { continue };
            it.seek(&[]);
            while let Some((k, _)) = it.current() {
                if let Ok(s) = std::str::from_utf8(&k) {
                    truth.push(s.to_string());
                }
                // Within-scan progress for large DBs (the global one).
                if truth.len() % 2_000_000 == 0 {
                    eprint!(
                        "\r[{}/{}] scanning {} ... {:.1}M keys        ",
                        i, total, name, truth.len() as f64 / 1e6
                    );
                    let _ = std::io::stderr().flush();
                    last_tick = Instant::now();
                }
                if !it.advance() {
                    break;
                }
            }
        }
        total_keys += truth.len() as u64;

        // truth is in sorted (leveldb byte) order; ASCII keys => str order matches.
        let truth_set: HashSet<&str> = truth.iter().map(|s| s.as_str()).collect();

        // 2) Distinct accumulators present (Layer/Type/Bucket aren't Ord -> HashSet).
        let mut accs: HashSet<(Layer, AccumulatorType, AccumulatorBucket)> = HashSet::new();
        for k in &truth {
            if let Some(h) = CoverageHeader::from_db_key(k) {
                accs.insert((h.layer, h.accumulator_type(), h.bucket()));
            }
        }

        let mut resurrected: BTreeSet<String> = BTreeSet::new();
        let mut dropped: BTreeSet<String> = BTreeSet::new();

        // First real key >= target in the (sorted) linear truth.
        let truth_first_ge = |target: &str| -> Option<&str> {
            let idx = truth.partition_point(|k| k.as_str() < target);
            truth.get(idx).map(|s| s.as_str())
        };
        // Key the buggy boundary seek actually lands on at `target`.
        let buggy_first_at = |db: &mut TrackedDb, target: &str| -> Option<String> {
            let mut it = db.new_iter().ok()?;
            it.seek(target.as_bytes());
            it.current()
                .and_then(|(k, _)| String::from_utf8(k.to_vec()).ok())
        };

        for (layer, t, bucket) in &accs {
            let (data_start, data_end) = CoverageHeader::db_search_range(*t, *bucket, *layer);
            let (meta_start, _) =
                CoverageHeader::db_search_range_with_meta(*t, *bucket, *layer);

            // (A) First-key-after-seek divergence for every boundary the code
            //     seeks to, including the scan-skip end `.../9000...` that hops
            //     to the *next* accumulator (this is what surfaces ghosts in
            //     fully-deleted accumulators, invisible to a live-key scan).
            for target in [&data_start, &meta_start, &data_end] {
                checked_ranges += 1;
                let buggy = buggy_first_at(&mut db, target);
                let truth_next = truth_first_ge(target);
                if buggy.as_deref() != truth_next {
                    if let Some(ref bk) = buggy {
                        if !truth_set.contains(bk.as_str()) {
                            resurrected.insert(bk.clone()); // landed on a deleted key
                        }
                    }
                    // truth's next live key was skipped over.
                    let skipped = match &buggy {
                        None => true,
                        Some(bk) => truth_next.map(|tn| bk.as_str() > tn).unwrap_or(false),
                    };
                    if let (true, Some(tn)) = (skipped, truth_next) {
                        dropped.insert(tn.to_string());
                    }
                }
            }

            // (B) Magnitude check (--deep only): full set diff over the
            //     read/delete windows [start, end) — a second pass over the
            //     data — to count how many keys within a single read are
            //     mis-handled (under-read of live data / over-read of deleted).
            if !deep {
                continue;
            }
            for start in [&data_start, &meta_start] {
                let Some(got) = boundary_read(&mut db, start, &data_end) else { continue };
                let want: BTreeSet<String> = truth
                    .iter()
                    .filter(|k| k.as_str() >= start.as_str() && k.as_str() < data_end.as_str())
                    .cloned()
                    .collect();
                for k in got.difference(&want) {
                    resurrected.insert(k.clone());
                }
                for k in want.difference(&got) {
                    dropped.insert(k.clone());
                }
            }
        }

        let (mut rd, mut rm, mut dd, mut dm) = (0usize, 0usize, 0usize, 0usize);
        for k in &resurrected {
            if key_is_meta(k) { rm += 1 } else { rd += 1 }
        }
        for k in &dropped {
            if key_is_meta(k) { dm += 1 } else { dd += 1 }
        }

        if rd + rm + dd + dm > 0 {
            affected_stations += 1;
            println!(
                "\n{}: resurrected data={} meta={} | dropped data={} meta={}  ({} keys, {} accumulators)",
                name, rd, rm, dd, dm, truth.len(), accs.len()
            );
            for k in resurrected.iter().take(4) {
                println!("    RESURRECTED {}", k);
            }
            for k in dropped.iter().take(4) {
                println!("    DROPPED     {}", k);
            }
            tot_res_data += rd;
            tot_res_meta += rm;
            tot_drop_data += dd;
            tot_drop_meta += dm;
        }
    }

    eprint!("\r{:80}\r", " "); // clear progress line
    println!("\n=== SUMMARY ({:.0}s, {:.1}M keys) ===", run_start.elapsed().as_secs_f64(), total_keys as f64 / 1e6);
    println!("Stations checked:   {}", total);
    println!("Accumulator ranges: {}", checked_ranges);
    println!("Stations affected:  {}", affected_stations);
    println!("RESURRECTED (deleted read as live):  data={}  meta={}", tot_res_data, tot_res_meta);
    println!("DROPPED     (live not returned):     data={}  meta={}", tot_drop_data, tot_drop_meta);
    if tot_res_data == 0 && tot_drop_data == 0 {
        println!("\nNo DATA-key divergence: real H3 coverage reads are clean (metas only, if any).");
    } else {
        println!("\nDATA-key divergence present: real coverage was mis-read by the boundary seek.");
    }
    if !deep {
        println!("(fast mode: DROPPED is boundary-only; run --deep for full under-read magnitude.)");
    }
}
