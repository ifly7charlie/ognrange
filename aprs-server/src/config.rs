use crate::layers::{parse_enabled_layers, Layer};
use once_cell::sync::Lazy;
use std::collections::HashSet;
use std::env;

fn fix_trailing_slash(s: &str) -> String {
    if s.ends_with('/') {
        s.to_string()
    } else {
        format!("{}/", s)
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// Site identity
pub static NEXT_PUBLIC_SITEURL: Lazy<String> = Lazy::new(|| env_or("NEXT_PUBLIC_SITEURL", "unknown"));

// Terrarium-encoded PNG DEM tiles. AWS Open Data public bucket is the default;
// override via NEXT_PUBLIC_DEM_TILE_URL to front it through your own CDN.
pub static DEM_TILE_URL: Lazy<String> = Lazy::new(|| {
    env_or(
        "NEXT_PUBLIC_DEM_TILE_URL",
        "https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png",
    )
});

// Database and output paths
pub static DB_PATH: Lazy<String> = Lazy::new(|| fix_trailing_slash(&env_or("DB_PATH", "./db")));
pub static OUTPUT_PATH: Lazy<String> = Lazy::new(|| fix_trailing_slash(&env_or("OUTPUT_PATH", "./data")));
pub static UNCOMPRESSED_ARROW_FILES: Lazy<bool> = Lazy::new(|| env_parse("UNCOMPRESSED_ARROW_FILES", 1) != 0);

/// Arrow output directory for a station: {OUTPUT_PATH}{name}/
pub fn output_dir(station_name: &str) -> String {
    format!("{}{}", *OUTPUT_PATH, station_name)
}

// APRS connection
pub static APRS_CALLSIGN: Lazy<String> = Lazy::new(|| {
    let cs = env_or("APRS_CALLSIGN", "OGNRANGE");
    assert!(cs.len() <= 8, "APRS_CALLSIGN must be at most 8 characters, got '{}'", cs);
    cs
});
pub static APRS_KEEPALIVE_PERIOD_MS: Lazy<u64> =
    Lazy::new(|| env_parse::<u64>("APRS_KEEPALIVE_PERIOD_SECONDS", 45) * 1000);
pub static APRS_TRAFFIC_FILTER: Lazy<String> =
    Lazy::new(|| env_or("APRS_TRAFFIC_FILTER", "t/spuoimnwt"));
pub static APRS_SERVER: Lazy<String> =
    Lazy::new(|| env_or("APRS_SERVER", "aprs.glidernet.org:14580"));

// Database handle limits
pub static MAX_STATION_DBS: Lazy<usize> = Lazy::new(|| env_parse("MAX_STATION_DBS", 800));

// Target SST file size, applied to every DB. LevelDB's 2MB default produces
// tens of thousands of tiny files on the larger DBs (the global one, plus the
// busier stations), which makes every scan/merge a storm of cold file opens.
// A larger target means far fewer, bigger files. Compaction work bounds scale
// with this (grandparent-overlap and expansion limits are multiples of it), so
// keep it moderate.
pub static DB_MAX_FILE_SIZE_BYTES: Lazy<usize> =
    Lazy::new(|| env_parse::<usize>("DB_MAX_FILE_SIZE_MB", 16) * 1024 * 1024);

// Per-station DBs get a small open-file budget so total FDs stay bounded across
// MAX_STATION_DBS concurrent opens. The global DB is a singleton holding the
// world's coverage (millions of H3 cells, thousands of SST files), so it gets a
// much larger table cache (open files) and block cache - the small per-station
// limits make traversing it pathologically slow (every file-boundary crossing
// is a cold open through a tiny cache).
pub static STATION_MAX_OPEN_FILES: Lazy<usize> = Lazy::new(|| env_parse("STATION_MAX_OPEN_FILES", 40));
pub static GLOBAL_MAX_OPEN_FILES: Lazy<usize> = Lazy::new(|| env_parse("GLOBAL_MAX_OPEN_FILES", 2000));

// Block cache holds decompressed 4KB data blocks - it tracks the hot working
// set, not file size. The per-station value is multiplied by up to
// MAX_STATION_DBS concurrent opens, so keep it modest; the global singleton can
// afford a large cache for the price of one.
pub static STATION_BLOCK_CACHE_BYTES: Lazy<usize> =
    Lazy::new(|| env_parse::<usize>("STATION_BLOCK_CACHE_MB", 16) * 1024 * 1024);
pub static GLOBAL_BLOCK_CACHE_BYTES: Lazy<usize> =
    Lazy::new(|| env_parse::<usize>("GLOBAL_BLOCK_CACHE_MB", 256) * 1024 * 1024);

// Bottom-level reclaim: routine per-rollup compaction (compact_range) only tidies
// the upper levels, so superseded versions and tombstones pile up in a DB's bottom
// level (L6) in proportion to write volume - for the global DB and for busy station
// DBs alike. After this many (written + deleted) records have been applied to a
// given DB, the next rollup runs the expensive compact_range_full pass that cascades
// to L6 and reclaims them. At the default 3h cadence (~0.7M writes/cycle on global)
// ~5M is roughly daily; small/quiet stations rarely reach it (and barely bloat).
// 0 disables.
pub static FULL_COMPACT_WRITE_THRESHOLD: Lazy<u64> =
    Lazy::new(|| env_parse::<u64>("FULL_COMPACT_WRITE_THRESHOLD", 5_000_000));

// Rollup configuration
pub static ROLLUP_PERIOD_MINUTES: Lazy<f64> = Lazy::new(|| {
    if let Ok(v) = env::var("ROLLUP_PERIOD_MINUTES") {
        if let Ok(f) = v.parse::<f64>() {
            return f;
        }
    }
    env_parse::<f64>("ROLLUP_PERIOD_HOURS", 3.0) * 60.0
});

pub static MAX_SIMULTANEOUS_ROLLUPS: Lazy<usize> = Lazy::new(|| {
    let configured = env_parse("MAX_SIMULTANEOUS_ROLLUPS", 100usize);
    configured.min(*MAX_STATION_DBS / 2)
});

// How often to write server/station status files for the frontend, independent of rollup.
// Set lower than ROLLUP_PERIOD_HOURS when you want fresher status without more frequent rollups.
pub static STATUS_WRITE_PERIOD_MINUTES: Lazy<f64> =
    Lazy::new(|| env_parse::<f64>("STATUS_WRITE_PERIOD_MINUTES", 60.0));

// H3 cell levels - DO NOT CHANGE without resetting all data
pub static H3_STATION_CELL_LEVEL: Lazy<u8> = Lazy::new(|| env_parse("H3_STATION_CELL_LEVEL", 8));
pub static H3_GLOBAL_CELL_LEVEL: Lazy<u8> = Lazy::new(|| env_parse("H3_GLOBAL_CELL_LEVEL", 7));

// Aircraft tracking
pub static FORGET_AIRCRAFT_AFTER_SECS: Lazy<u64> =
    Lazy::new(|| env_parse::<u64>("FORGET_AIRCRAFT_AFTER_HOURS", 12) * 3600);

// Station management
/// Maximum station ID before refusing new allocations (u16::MAX - 1, since 0 = global)
pub const MAX_STATION_ID: u16 = u16::MAX - 1;
/// Warn when station ID allocation reaches this percentage of MAX_STATION_ID
pub const STATION_ID_WARN_PERCENT: u16 = 90;

pub static STATION_MOVE_THRESHOLD_KM: Lazy<f64> =
    Lazy::new(|| env_parse("STATION_MOVE_THRESHOLD_KM", 0.2));
pub static STATION_MOVE_CONFIRM_SECS: Lazy<u64> =
    Lazy::new(|| env_parse::<u64>("STATION_MOVE_CONFIRM_DAYS", 7) * 3600 * 24);
pub static STATION_EXPIRY_TIME_SECS: Lazy<u64> =
    Lazy::new(|| env_parse::<u64>("STATION_EXPIRY_TIME_DAYS", 31) * 3600 * 24);

// H3 cache timing
pub static H3_CACHE_FLUSH_PERIOD_MS: Lazy<u64> =
    Lazy::new(|| env_parse::<u64>("H3_CACHE_FLUSH_PERIOD_MINUTES", 5) * 60 * 1000);
pub static H3_CACHE_MAXIMUM_DIRTY_PERIOD_MS: Lazy<u64> =
    Lazy::new(|| env_parse::<u64>("H3_CACHE_MAXIMUM_DIRTY_PERIOD_MINUTES", 30) * 60 * 1000);

// Elevation tile cache
pub static MAX_ELEVATION_TILES: Lazy<usize> = Lazy::new(|| env_parse("MAX_ELEVATION_TILES", 32000));
pub static ELEVATION_TILE_RESOLUTION: Lazy<u32> =
    Lazy::new(|| env_parse("ELEVATION_TILE_RESOLUTION", 11));

/// Disable the background ground-horizon generation queue entirely
/// (operational kill-switch; replaces MAX_GROUND_HORIZONS_PER_ROLLUP - the
/// queue is serial and coarse-to-fine, so pacing is no longer needed)
pub static GROUND_HORIZON_PAUSED: Lazy<bool> =
    Lazy::new(|| env_parse("GROUND_HORIZON_PAUSED", 0) != 0);

/// Default antenna height above ground (m) when a station's beaconed altitude
/// is missing or fails the sanity window against the DEM ground. Used as the
/// viewpoint for both the ground and receive horizons
pub static GROUND_STATION_AGL_M: Lazy<f64> = Lazy::new(|| env_parse("GROUND_STATION_AGL_M", 10.0));

// Reject log rotation
pub static REJECT_LOG_MAX_MB: Lazy<u64> = Lazy::new(|| env_parse("REJECT_LOG_MAX_MB", 50));

/// Count (but still process) packets with timestamps more than this many seconds in the future (default: 300 = 5 minutes)
pub static FUTURE_PACKET_CUTOFF_SECS: Lazy<u32> =
    Lazy::new(|| env_parse("FUTURE_PACKET_CUTOFF_SECS", 300u32));

/// Count (but still process) packets with timestamps older than this many seconds (default: 3600 = 1 hour)
pub static STALE_PACKET_CUTOFF_SECS: Lazy<u32> =
    Lazy::new(|| env_parse("STALE_PACKET_CUTOFF_SECS", 3600u32));

/// Reject packets claiming a position further than this from the station's
/// known location - beyond real reception range the position is corrupt
/// (default: 500, comfortably above the ~450km mountain/ducting records)
pub static MAX_PACKET_DISTANCE_KM: Lazy<f64> =
    Lazy::new(|| env_parse("MAX_PACKET_DISTANCE_KM", 500.0f64));

// Layer configuration
pub static ENABLED_LAYERS: Lazy<Option<HashSet<Layer>>> =
    Lazy::new(|| parse_enabled_layers(env::var("ENABLED_LAYERS").ok().as_deref()));

// Logging
pub static LOG_SYSLOG: Lazy<bool> = Lazy::new(|| env_parse("LOG_SYSLOG", 0) != 0);
pub static LOG_STDOUT: Lazy<bool> = Lazy::new(|| env_parse("LOG_STDOUT", 1) != 0);

// Git version
pub static GIT_REF: Lazy<Option<String>> = Lazy::new(|| {
    env::var("GIT_REF")
        .or_else(|_| env::var("NEXT_PUBLIC_GIT_REF"))
        .ok()
});

pub fn git_version() -> String {
    let raw = if let Some(ref r) = *GIT_REF {
        r.clone()
    } else {
        std::process::Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_string())
            .unwrap_or_default()
    };

    // aprsc tokenizes the login line by whitespace and expects `vers <app> <version>`
    // as two separate tokens. Whitespace inside the version would consume the next
    // keyword (e.g. `filter`) and cause the server to reject the connection.
    let sanitized: String = raw
        .split_whitespace()
        .collect::<Vec<_>>()
        .join("-");
    if sanitized.is_empty() {
        "unknown".to_string()
    } else {
        sanitized
    }
}
