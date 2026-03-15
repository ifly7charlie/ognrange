//! Protocol usage statistics — tracks per-TOCALL message counts,
//! unique devices, and geographic distribution.
//!
//! Writes JSON stats files alongside station data during each rollup,
//! persists state across restarts via a state file.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::Mutex;

use chrono::{Datelike, Utc};
use flate2::write::GzEncoder;
use flate2::Compression;
use tracing::{error, info, warn};

use crate::config::{OUTPUT_PATH, UNCOMPRESSED_ARROW_FILES};

/// Infrastructure TOCALLs excluded from protocol stats
const INFRASTRUCTURE_TOCALLS: &[&str] = &[
    "OGNSDR", "OGNSXR", "OGNDELAY", "OGNDVS", "OGNTTN", "OGMSHT", "OGNHEL", "OGNDSX", "OGAVZ",
];

fn is_infrastructure(tocall: &str) -> bool {
    INFRASTRUCTURE_TOCALLS.contains(&tocall)
}

/// Map lat/lon to a region short code
fn region_code(lat: f64, lon: f64) -> &'static str {
    // Checked in order; first match wins. Overlaps are intentional —
    // the order prioritises the more specific region.
    if lat >= 35.0 && lat <= 72.0 && lon >= -25.0 && lon <= 45.0 {
        "eu"
    } else if lat >= 15.0 && lat <= 72.0 && lon >= -170.0 && lon <= -50.0 {
        "na"
    } else if lat >= -60.0 && lat <= 15.0 && lon >= -90.0 && lon <= -30.0 {
        "sa"
    } else if lat >= -35.0 && lat <= 37.0 && lon >= -20.0 && lon <= 55.0 {
        "af"
    } else if lat >= 0.0 && lat <= 75.0 && lon >= 45.0 && lon <= 180.0 {
        "as"
    } else if lat >= -50.0 && lat <= 5.0 && lon >= 100.0 && lon <= 180.0 {
        "oc"
    } else {
        "ot"
    }
}

/// Altitude band short code from AGL in meters.
/// Bands: 0–3000m ("low"), 3000–4500m ("mid"), 4500m+ ("high")
fn altitude_band(agl_m: u16) -> &'static str {
    if agl_m < 3000 {
        "low"
    } else if agl_m < 4500 {
        "mid"
    } else {
        "high"
    }
}

/// Per-TOCALL counters
#[derive(Debug, Clone)]
struct TocallStats {
    raw_count: u64,
    accepted_count: u64,
    devices: HashSet<u32>,
    regions: HashMap<&'static str, u64>,
    altitudes: HashMap<&'static str, u64>,
}

impl TocallStats {
    fn new() -> Self {
        Self {
            raw_count: 0,
            accepted_count: 0,
            devices: HashSet::new(),
            regions: HashMap::new(),
            altitudes: HashMap::new(),
        }
    }
}

/// Internal mutable state
#[derive(Debug, Clone)]
struct StatsInner {
    start_time: chrono::DateTime<Utc>,
    tocalls: HashMap<String, TocallStats>,
    /// Accepted packet counts by layer and hour-of-day (0–23)
    hourly: HashMap<String, [u64; 24]>,
    /// Number of process restarts since start_time (0 = no restarts)
    restarts: u32,
}

/// Thread-safe protocol statistics tracker
pub struct ProtocolStats {
    inner: Mutex<StatsInner>,
}

impl ProtocolStats {
    /// Create fresh stats starting now
    fn new() -> Self {
        Self {
            inner: Mutex::new(StatsInner {
                start_time: Utc::now(),
                tocalls: HashMap::new(),
                hourly: HashMap::new(),
                restarts: 0,
            }),
        }
    }

    /// Load from state file, falling back to fresh if stale/missing
    pub fn load() -> Self {
        let state_path = format!("{}stats/protocol-stats.state.json", *OUTPUT_PATH);
        let data = match std::fs::read_to_string(&state_path) {
            Ok(d) => d,
            Err(_) => return Self::new(),
        };

        let parsed: serde_json::Value = match serde_json::from_str(&data) {
            Ok(v) => v,
            Err(e) => {
                warn!("Failed to parse protocol stats state: {}", e);
                return Self::new();
            }
        };

        // Validate start_time is in current month
        let start_str = parsed["startTime"].as_str().unwrap_or("");
        let start_time = match start_str.parse::<chrono::DateTime<Utc>>() {
            Ok(dt) => dt,
            Err(_) => return Self::new(),
        };

        let now = Utc::now();
        if start_time.year() != now.year() || start_time.month() != now.month() {
            info!("Protocol stats state is from a different month, starting fresh");
            return Self::new();
        }

        // Restore tocall data
        let mut tocalls = HashMap::new();
        if let Some(obj) = parsed["tocalls"].as_object() {
            for (tocall, val) in obj {
                let mut devices = HashSet::new();
                if let Some(arr) = val["devices"].as_array() {
                    for d in arr {
                        if let Some(s) = d.as_str() {
                            if let Ok(n) = u32::from_str_radix(s, 16) {
                                devices.insert(n);
                            }
                        }
                    }
                }
                let mut regions = HashMap::new();
                if let Some(robj) = val["regions"].as_object() {
                    for (region, count) in robj {
                        let code: &'static str = match region.as_str() {
                            "eu" => "eu",
                            "na" => "na",
                            "sa" => "sa",
                            "af" => "af",
                            "as" => "as",
                            "oc" => "oc",
                            _ => "ot",
                        };
                        *regions.entry(code).or_insert(0) += count.as_u64().unwrap_or(0);
                    }
                }
                let mut altitudes = HashMap::new();
                if let Some(aobj) = val["altitudes"].as_object() {
                    for (band, count) in aobj {
                        let code: &'static str = match band.as_str() {
                            "low" => "low",
                            "mid" => "mid",
                            "high" => "high",
                            _ => continue,
                        };
                        *altitudes.entry(code).or_insert(0) += count.as_u64().unwrap_or(0);
                    }
                }
                tocalls.insert(
                    tocall.clone(),
                    TocallStats {
                        raw_count: val["rawCount"].as_u64().unwrap_or(0),
                        accepted_count: val["acceptedCount"].as_u64().unwrap_or(0),
                        devices,
                        regions,
                        altitudes,
                    },
                );
            }
        }

        // Restore hourly data
        let mut hourly = HashMap::new();
        if let Some(obj) = parsed["hourly"].as_object() {
            for (layer, arr) in obj {
                if let Some(hours) = arr.as_array() {
                    let mut counts = [0u64; 24];
                    for (i, v) in hours.iter().take(24).enumerate() {
                        counts[i] = v.as_u64().unwrap_or(0);
                    }
                    hourly.insert(layer.clone(), counts);
                }
            }
        }

        let restarts = parsed["restarts"].as_u64().unwrap_or(0) as u32 + 1;

        info!(
            "Restored protocol stats from {}: {} tocalls, restart #{}",
            start_str,
            tocalls.len(),
            restarts
        );

        Self {
            inner: Mutex::new(StatsInner {
                start_time,
                tocalls,
                hourly,
                restarts,
            }),
        }
    }

    /// Record a raw packet (before filtering)
    pub fn record_raw(&self, tocall: &str, flarm_num: u32, lat: f64, lon: f64) {
        if is_infrastructure(tocall) {
            return;
        }
        let region = region_code(lat, lon);
        let mut inner = self.inner.lock().unwrap();
        let entry = inner
            .tocalls
            .entry(tocall.to_string())
            .or_insert_with(TocallStats::new);
        entry.raw_count += 1;
        entry.devices.insert(flarm_num);
        *entry.regions.entry(region).or_insert(0) += 1;
    }

    /// Record an accepted packet for hourly-by-layer tracking
    pub fn record_hourly(&self, layer: &str, hour: u32) {
        let mut inner = self.inner.lock().unwrap();
        let hours = inner.hourly.entry(layer.to_string()).or_insert([0u64; 24]);
        hours[hour as usize % 24] += 1;
    }

    /// Record an accepted packet (passed all filters) with AGL altitude in meters
    pub fn record_accepted(&self, tocall: &str, agl_m: u16) {
        if is_infrastructure(tocall) {
            return;
        }
        let band = altitude_band(agl_m);
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.tocalls.get_mut(tocall) {
            entry.accepted_count += 1;
            *entry.altitudes.entry(band).or_insert(0) += 1;
        }
    }

    /// Write stats during rollup. `day_rotation` resets hourly counters.
    /// `month_rotation` triggers a monthly final file + full reset.
    pub fn write_stats(&self, day_file: &str, day_rotation: bool, month_rotation: Option<&str>) {
        let stats_dir = format!("{}stats", *OUTPUT_PATH);
        if let Err(e) = std::fs::create_dir_all(&stats_dir) {
            error!("Failed to create stats directory: {}", e);
            return;
        }

        // Build JSON from current state
        let json = {
            let inner = self.inner.lock().unwrap();
            build_output_json(&inner)
        };

        // Write daily .json.gz (always)
        let daily_gz_name = format!("protocol-stats.{}.json.gz", day_file);
        write_gz_atomic(&stats_dir, &daily_gz_name, &json);

        // Write daily .json (if uncompressed enabled)
        let daily_json_name = format!("protocol-stats.{}.json", day_file);
        if *UNCOMPRESSED_ARROW_FILES {
            write_atomic(&stats_dir, &daily_json_name, &json);
        }

        // Symlinks
        {
            use crate::symlinks::symlink_atomic;
            symlink_atomic(&daily_gz_name, &format!("{}/protocol-stats.json.gz", stats_dir));
            if *UNCOMPRESSED_ARROW_FILES {
                symlink_atomic(
                    &daily_json_name,
                    &format!("{}/protocol-stats.json", stats_dir),
                );
            }
        }

        // Reset hourly counters, device sets, and restart counter on day rotation
        // (after daily file is written)
        if day_rotation {
            let mut inner = self.inner.lock().unwrap();
            inner.hourly.clear();
            inner.restarts = 0;
            for stats in inner.tocalls.values_mut() {
                stats.devices.clear();
            }
        }

        // Monthly final file on month rotation
        if let Some(month_file) = month_rotation {
            let monthly_gz_name = format!("protocol-stats.{}.json.gz", month_file);
            write_gz_atomic(&stats_dir, &monthly_gz_name, &json);
            if *UNCOMPRESSED_ARROW_FILES {
                let monthly_json_name = format!("protocol-stats.{}.json", month_file);
                write_atomic(&stats_dir, &monthly_json_name, &json);
            }
            info!("Wrote monthly protocol stats: {}", monthly_gz_name);

            // Reset for new month (month rotation implies day rotation, so
            // restarts/hourly/devices are already cleared above)
            let mut inner = self.inner.lock().unwrap();
            inner.tocalls.clear();
            inner.hourly.clear();
            inner.start_time = Utc::now();
        }

        // Write state file (always uncompressed)
        self.save_state_inner();

        info!("Wrote protocol stats: {}", daily_gz_name);
    }

    /// Save state file only (for shutdown)
    pub fn save_state(&self) {
        let stats_dir = format!("{}stats", *OUTPUT_PATH);
        if let Err(e) = std::fs::create_dir_all(&stats_dir) {
            error!("Failed to create stats directory: {}", e);
            return;
        }
        self.save_state_inner();
        info!("Saved protocol stats state");
    }

    fn save_state_inner(&self) {
        let inner = self.inner.lock().unwrap();
        let state_json = build_state_json(&inner);
        let state_path = format!("{}stats/protocol-stats.state.json", *OUTPUT_PATH);
        if let Err(e) = std::fs::write(&state_path, state_json.as_bytes()) {
            error!("Failed to write protocol stats state: {}", e);
        }
    }
}

/// Build the public-facing JSON output
fn build_output_json(inner: &StatsInner) -> String {
    let now = Utc::now();
    let uptime = (now - inner.start_time).num_seconds().max(0) as u64;

    let mut protocols = serde_json::Map::new();
    // Sort by raw_count descending for consistent output
    let mut sorted: Vec<_> = inner.tocalls.iter().collect();
    sorted.sort_by(|a, b| b.1.raw_count.cmp(&a.1.raw_count));

    for (tocall, stats) in sorted {
        let mut regions = serde_json::Map::new();
        let mut region_sorted: Vec<_> = stats.regions.iter().collect();
        region_sorted.sort_by(|a, b| b.1.cmp(a.1));
        for (code, count) in region_sorted {
            if *count > 0 {
                regions.insert(code.to_string(), serde_json::Value::from(*count));
            }
        }

        let mut entry = serde_json::Map::new();
        entry.insert("raw".to_string(), serde_json::Value::from(stats.raw_count));
        entry.insert(
            "accepted".to_string(),
            serde_json::Value::from(stats.accepted_count),
        );
        entry.insert(
            "devices".to_string(),
            serde_json::Value::from(stats.devices.len() as u64),
        );
        entry.insert("regions".to_string(), serde_json::Value::Object(regions));

        let mut altitudes = serde_json::Map::new();
        for &band in &["low", "mid", "high"] {
            if let Some(&count) = stats.altitudes.get(band) {
                if count > 0 {
                    altitudes.insert(band.to_string(), serde_json::Value::from(count));
                }
            }
        }
        entry.insert("altitudes".to_string(), serde_json::Value::Object(altitudes));

        protocols.insert(tocall.clone(), serde_json::Value::Object(entry));
    }

    // Build hourly map sorted by layer name
    let mut hourly = serde_json::Map::new();
    let mut hourly_sorted: Vec<_> = inner.hourly.iter().collect();
    hourly_sorted.sort_by_key(|(k, _)| (*k).clone());
    for (layer, counts) in hourly_sorted {
        let arr: Vec<serde_json::Value> = counts.iter().map(|&c| serde_json::Value::from(c)).collect();
        hourly.insert(layer.clone(), serde_json::Value::Array(arr));
    }

    let output = serde_json::json!({
        "generated": now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "startTime": inner.start_time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "uptimeSeconds": uptime,
        "restarts": inner.restarts,
        "protocols": protocols,
        "hourly": hourly,
    });

    serde_json::to_string_pretty(&output).unwrap_or_else(|_| "{}".to_string())
}

/// Build the state JSON (includes device sets for unique count accuracy)
fn build_state_json(inner: &StatsInner) -> String {
    let mut tocalls = serde_json::Map::new();

    for (tocall, stats) in &inner.tocalls {
        let mut regions = serde_json::Map::new();
        for (code, count) in &stats.regions {
            if *count > 0 {
                regions.insert(code.to_string(), serde_json::Value::from(*count));
            }
        }

        let devices: Vec<serde_json::Value> = stats
            .devices
            .iter()
            .map(|&d| serde_json::Value::from(format!("{:06X}", d)))
            .collect();

        let mut entry = serde_json::Map::new();
        entry.insert(
            "rawCount".to_string(),
            serde_json::Value::from(stats.raw_count),
        );
        entry.insert(
            "acceptedCount".to_string(),
            serde_json::Value::from(stats.accepted_count),
        );
        let mut altitudes = serde_json::Map::new();
        for (band, count) in &stats.altitudes {
            if *count > 0 {
                altitudes.insert(band.to_string(), serde_json::Value::from(*count));
            }
        }

        entry.insert("devices".to_string(), serde_json::Value::Array(devices));
        entry.insert("regions".to_string(), serde_json::Value::Object(regions));
        entry.insert("altitudes".to_string(), serde_json::Value::Object(altitudes));

        tocalls.insert(tocall.clone(), serde_json::Value::Object(entry));
    }

    let mut hourly = serde_json::Map::new();
    for (layer, counts) in &inner.hourly {
        let arr: Vec<serde_json::Value> = counts.iter().map(|&c| serde_json::Value::from(c)).collect();
        hourly.insert(layer.clone(), serde_json::Value::Array(arr));
    }

    let state = serde_json::json!({
        "startTime": inner.start_time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "restarts": inner.restarts,
        "tocalls": tocalls,
        "hourly": hourly,
    });

    serde_json::to_string_pretty(&state).unwrap_or_else(|_| "{}".to_string())
}

/// Write gzipped content atomically (via .working temp file + rename)
fn write_gz_atomic(dir: &str, filename: &str, content: &str) {
    let working = format!("{}/{}.working", dir, filename);
    let final_path = format!("{}/{}", dir, filename);

    let result = (|| -> Result<(), String> {
        let file = std::fs::File::create(&working)
            .map_err(|e| format!("create {}: {}", working, e))?;
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder
            .write_all(content.as_bytes())
            .map_err(|e| format!("write {}: {}", working, e))?;
        encoder
            .finish()
            .map_err(|e| format!("finish {}: {}", working, e))?;
        std::fs::rename(&working, &final_path)
            .map_err(|e| format!("rename {} -> {}: {}", working, final_path, e))?;
        Ok(())
    })();

    if let Err(e) = result {
        error!("Failed to write gz stats: {}", e);
        let _ = std::fs::remove_file(&working);
    }
}

/// Write uncompressed content atomically
fn write_atomic(dir: &str, filename: &str, content: &str) {
    let working = format!("{}/{}.working", dir, filename);
    let final_path = format!("{}/{}", dir, filename);

    if let Err(e) = std::fs::write(&working, content.as_bytes()) {
        error!("Failed to write stats {}: {}", working, e);
        return;
    }
    if let Err(e) = std::fs::rename(&working, &final_path) {
        error!("Failed to rename stats {} -> {}: {}", working, final_path, e);
        let _ = std::fs::remove_file(&working);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_code() {
        // Europe
        assert_eq!(region_code(48.0, 11.0), "eu"); // Munich
        assert_eq!(region_code(51.5, -0.1), "eu"); // London
        // North America
        assert_eq!(region_code(40.7, -74.0), "na"); // New York
        // South America
        assert_eq!(region_code(-23.5, -46.6), "sa"); // São Paulo
        // Africa
        assert_eq!(region_code(-33.9, 18.4), "af"); // Cape Town
        // Asia
        assert_eq!(region_code(35.7, 139.7), "as"); // Tokyo
        // Oceania
        assert_eq!(region_code(-33.9, 151.2), "oc"); // Sydney
        // Other (Antarctica)
        assert_eq!(region_code(-80.0, 0.0), "ot");
    }

    #[test]
    fn test_is_infrastructure() {
        assert!(is_infrastructure("OGNSDR"));
        assert!(is_infrastructure("OGNSXR"));
        assert!(!is_infrastructure("OGFLR"));
        assert!(!is_infrastructure("OGNTRK"));
    }

    #[test]
    fn test_altitude_band() {
        assert_eq!(altitude_band(0), "low");
        assert_eq!(altitude_band(2999), "low");
        assert_eq!(altitude_band(3000), "mid");
        assert_eq!(altitude_band(4499), "mid");
        assert_eq!(altitude_band(4500), "high");
        assert_eq!(altitude_band(10000), "high");
    }

    #[test]
    fn test_record_and_output() {
        let stats = ProtocolStats::new();

        // Record some raw packets (flarm_num is the parsed 24-bit hex ID)
        stats.record_raw("OGFLR", 0x123456, 48.0, 11.0);
        stats.record_raw("OGFLR", 0x123456, 48.0, 11.0);
        stats.record_raw("OGFLR", 0xABCDEF, 40.7, -74.0);
        stats.record_raw("OGADSB", 0x3E1234, 48.0, 11.0);

        // Record accepted with altitude bands
        stats.record_accepted("OGFLR", 1500); // low
        stats.record_accepted("OGFLR", 3500); // mid

        // Infrastructure should be ignored
        stats.record_raw("OGNSDR", 0x123456, 48.0, 11.0);
        stats.record_accepted("OGNSDR", 1000);

        let inner = stats.inner.lock().unwrap();
        assert_eq!(inner.tocalls.len(), 2); // OGFLR + OGADSB, not OGNSDR
        assert_eq!(inner.tocalls["OGFLR"].raw_count, 3);
        assert_eq!(inner.tocalls["OGFLR"].accepted_count, 2);
        assert_eq!(inner.tocalls["OGFLR"].devices.len(), 2);
        assert_eq!(inner.tocalls["OGFLR"].regions["eu"], 2);
        assert_eq!(inner.tocalls["OGFLR"].regions["na"], 1);
        assert_eq!(inner.tocalls["OGFLR"].altitudes["low"], 1);
        assert_eq!(inner.tocalls["OGFLR"].altitudes["mid"], 1);
        assert_eq!(inner.tocalls["OGADSB"].raw_count, 1);
        assert!(!inner.tocalls.contains_key("OGNSDR"));
    }

    #[test]
    fn test_output_json_format() {
        let stats = ProtocolStats::new();
        stats.record_raw("OGFLR", 0x123456, 48.0, 11.0);
        stats.record_accepted("OGFLR", 2000);
        stats.record_hourly("flarm", 14);
        stats.record_hourly("flarm", 14);
        stats.record_hourly("flarm", 15);

        let inner = stats.inner.lock().unwrap();
        let json = build_output_json(&inner);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert!(parsed["generated"].is_string());
        assert!(parsed["startTime"].is_string());
        assert!(parsed["uptimeSeconds"].is_number());
        assert_eq!(parsed["restarts"], 0);
        assert_eq!(parsed["protocols"]["OGFLR"]["raw"], 1);
        assert_eq!(parsed["protocols"]["OGFLR"]["accepted"], 1);
        assert_eq!(parsed["protocols"]["OGFLR"]["devices"], 1);
        assert_eq!(parsed["protocols"]["OGFLR"]["regions"]["eu"], 1);
        assert_eq!(parsed["protocols"]["OGFLR"]["altitudes"]["low"], 1);

        let hourly = parsed["hourly"]["flarm"].as_array().unwrap();
        assert_eq!(hourly.len(), 24);
        assert_eq!(hourly[14], 2);
        assert_eq!(hourly[15], 1);
        assert_eq!(hourly[0], 0);
    }

    #[test]
    fn test_state_json_roundtrip() {
        let stats = ProtocolStats::new();
        stats.record_raw("OGFLR", 0x123456, 48.0, 11.0);
        stats.record_raw("OGFLR", 0xABCDEF, 40.7, -74.0);
        stats.record_accepted("OGFLR", 5000);
        stats.record_hourly("flarm", 10);
        stats.record_hourly("flarm", 10);
        stats.record_hourly("adsb", 22);

        let inner = stats.inner.lock().unwrap();
        let state_json = build_state_json(&inner);
        let parsed: serde_json::Value = serde_json::from_str(&state_json).unwrap();

        assert!(parsed["startTime"].is_string());
        assert_eq!(parsed["restarts"], 0);
        assert_eq!(parsed["tocalls"]["OGFLR"]["rawCount"], 2);
        assert_eq!(parsed["tocalls"]["OGFLR"]["acceptedCount"], 1);
        let devices = parsed["tocalls"]["OGFLR"]["devices"].as_array().unwrap();
        assert_eq!(devices.len(), 2);
        assert!(parsed["tocalls"]["OGFLR"]["regions"]["eu"].as_u64().unwrap() > 0);
        assert_eq!(parsed["tocalls"]["OGFLR"]["altitudes"]["high"], 1);

        let flarm_hours = parsed["hourly"]["flarm"].as_array().unwrap();
        assert_eq!(flarm_hours[10], 2);
        assert_eq!(flarm_hours[0], 0);
        let adsb_hours = parsed["hourly"]["adsb"].as_array().unwrap();
        assert_eq!(adsb_hours[22], 1);
    }
}
