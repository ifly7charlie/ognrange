//! Diagnostic tool: force a full rusty-leveldb compaction over a DB and report
//! the on-disk size / SST-file count before and after.
//!
//! This is the rusty-leveldb counterpart to the classic-level `compactdb`
//! Node tool. It runs `compact_range_full(b"!", b"~")`, which cascades into the
//! bottom level and reclaims the shadowed versions and tombstones that the
//! routine `compact_range` leaves stranded there - so it answers directly: how
//! much superseded data does a full reclaim drop? (The rollup runs the same
//! pass periodically, gated by GLOBAL_FULL_COMPACT_WRITE_THRESHOLD.)
//!
//! Usage: compactdb [path] [--chunked]
//!   path        DB directory to compact (default: ${DB_PATH}global)
//!   --chunked   Compact in narrow key slices instead of one full-range pass.
//!               Use after `repair_db` has dumped every SST into L0: a single
//!               full-range compaction would do one ~N-way L0 merge (O(files)
//!               per key in rusty-leveldb) and crawl; slices keep each merge small.
//!
//! Opens read-write and takes the LevelDB LOCK - the aprs-server must NOT be
//! holding the DB. For an apples-to-apples comparison against the classic-level
//! tool, run both against fresh copies of the SAME original DB directory.

use rusty_leveldb::{Options, DB};

/// Total bytes of all files plus the count of `.ldb` SST files in `path`.
fn dir_stats(path: &str) -> (u64, usize) {
    let mut bytes = 0u64;
    let mut ldb_files = 0usize;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                if meta.is_file() {
                    bytes += meta.len();
                    if entry.file_name().to_string_lossy().ends_with(".ldb") {
                        ldb_files += 1;
                    }
                }
            }
        }
    }
    (bytes, ldb_files)
}

fn main() {
    let _ = dotenvy::from_filename(".env.local");

    // First non-flag arg is the DB path; default to ${DB_PATH}global.
    let path = std::env::args()
        .skip(1)
        .find(|a| !a.starts_with('-'))
        .unwrap_or_else(|| {
            let base = std::env::var("DB_PATH").unwrap_or_else(|_| "./db".to_string());
            let base = if base.ends_with('/') { base } else { format!("{}/", base) };
            format!("{}global", base)
        });

    let (before_bytes, before_files) = dir_stats(&path);
    println!("--- {} ---", path);
    println!(
        "BEFORE: {} .ldb files, {:.1} MB on disk",
        before_files,
        before_bytes as f64 / 1_000_000.0
    );

    // Match the global-DB sizing the server now uses (see config.rs / db.rs).
    let mut opts = Options::default();
    opts.create_if_missing = false;
    opts.max_open_files = 2000;
    opts.max_file_size = 16 * 1024 * 1024;
    opts.block_cache_capacity_bytes = 256 * 1024 * 1024;

    let mut db = match DB::open(&path, opts) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Failed to open {}: {}", path, e);
            std::process::exit(1);
        }
    };

    let chunked = std::env::args().any(|a| a == "--chunked");
    let t0 = std::time::Instant::now();
    if chunked {
        // Recovery mode for a DB whose SSTs have all been dumped into L0 (e.g.
        // after `repair_db`): a single full-range compaction would merge every L0
        // file at once, and rusty-leveldb's merging iterator is O(files) per key,
        // so a ~2000-way merge crawls. Instead compact narrow key slices so each
        // merge only touches the handful of L0 files overlapping that slice. The
        // dedup/reclaim happens at L0->L1 regardless, so plain compact_range (not
        // _full) is enough here - the bottom level is empty in this state.
        let layers = [b'a', b'c', b'd', b'f', b'n', b'p', b's', b't'];
        let mut bounds: Vec<Vec<u8>> = vec![b"!".to_vec()];
        for &l in &layers {
            for &n in b"0123456789abcdef" {
                bounds.push(vec![l, b'/', n]);
            }
        }
        bounds.push(b"~".to_vec());

        println!("Chunked compaction in {} slices (avoids a wide L0 merge)...", bounds.len() - 1);
        for (i, w) in bounds.windows(2).enumerate() {
            if let Err(e) = db.compact_range(&w[0], &w[1]) {
                eprintln!("compact_range({:?}..) failed: {}", String::from_utf8_lossy(&w[0]), e);
                std::process::exit(1);
            }
            let (cur_bytes, cur_files) = dir_stats(&path);
            println!(
                "  slice {}/{} [{}..] done @ {:.0}s - {} files, {:.1} MB",
                i + 1, bounds.len() - 1, String::from_utf8_lossy(&w[0]),
                t0.elapsed().as_secs_f64(), cur_files, cur_bytes as f64 / 1_000_000.0
            );
        }
    } else {
        println!("Compacting full range [b\"!\"..b\"~\"] with rusty-leveldb - may take a while...");
        // compact_range_full cascades into the bottom level, reclaiming the shadowed
        // versions and tombstones that the routine compact_range leaves stranded there.
        if let Err(e) = db.compact_range_full(b"!", b"~") {
            eprintln!("compact_range_full failed: {}", e);
            std::process::exit(1);
        }
    }
    if let Err(e) = db.flush() {
        eprintln!("flush failed: {}", e);
    }
    // Drop closes the DB, finalising obsolete-file deletion before we measure.
    drop(db);
    let secs = t0.elapsed().as_secs_f64();

    let (after_bytes, after_files) = dir_stats(&path);
    println!("Compaction finished in {:.1}s", secs);
    println!(
        "AFTER:  {} .ldb files, {:.1} MB on disk",
        after_files,
        after_bytes as f64 / 1_000_000.0
    );
    let size_x = before_bytes as f64 / after_bytes.max(1) as f64;
    let file_x = before_files as f64 / after_files.max(1) as f64;
    println!("Reduction: {:.1}x size, {:.1}x files", size_x, file_x);
}
