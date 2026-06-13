//! Diagnostic tool: force a full rusty-leveldb compaction over a DB and report
//! the on-disk size / SST-file count before and after.
//!
//! This is the rusty-leveldb counterpart to the classic-level `compactdb`
//! Node tool. It uses the SAME engine and the SAME `compact_range(b"!", b"~")`
//! call the rollup uses, so it answers directly: does rusty-leveldb's own
//! compaction reclaim superseded key versions, and by how much?
//!
//! Usage: compactdb [path]
//!   path   DB directory to compact (default: ${DB_PATH}global)
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

    println!("Compacting full range [b\"!\"..b\"~\"] with rusty-leveldb - may take a while...");
    let t0 = std::time::Instant::now();
    if let Err(e) = db.compact_range(b"!", b"~") {
        eprintln!("compact_range failed: {}", e);
        std::process::exit(1);
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
