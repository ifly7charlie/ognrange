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
pub static NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN: Lazy<String> =
    Lazy::new(|| env_or("NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN", ""));

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

// H3 cell levels — DO NOT CHANGE without resetting all data
pub static H3_STATION_CELL_LEVEL: Lazy<u8> = Lazy::new(|| env_parse("H3_STATION_CELL_LEVEL", 8));
pub static H3_GLOBAL_CELL_LEVEL: Lazy<u8> = Lazy::new(|| env_parse("H3_GLOBAL_CELL_LEVEL", 7));

// Aircraft tracking
pub static FORGET_AIRCRAFT_AFTER_SECS: Lazy<u64> =
    Lazy::new(|| env_parse::<u64>("FORGET_AIRCRAFT_AFTER_HOURS", 12) * 3600);

// Station management
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

// Reject log rotation
pub static REJECT_LOG_MAX_MB: Lazy<u64> = Lazy::new(|| env_parse("REJECT_LOG_MAX_MB", 50));

// Layer configuration
pub static ENABLED_LAYERS: Lazy<Option<HashSet<Layer>>> =
    Lazy::new(|| parse_enabled_layers(env::var("ENABLED_LAYERS").ok().as_deref()));

// Git version
pub static GIT_REF: Lazy<Option<String>> = Lazy::new(|| {
    env::var("GIT_REF")
        .or_else(|_| env::var("NEXT_PUBLIC_GIT_REF"))
        .ok()
});

pub fn git_version() -> String {
    if let Some(ref r) = *GIT_REF {
        return r.clone();
    }
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}
