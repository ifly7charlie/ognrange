//! Protocol usage statistics - tracks per-TOCALL message counts,
//! unique devices, and geographic distribution.
//!
//! Accumulates data independently in four periods (day, month, year, yearnz),
//! mirroring how arrow files are structured. Each period resets only
//! when its boundary is crossed during rollup.
//!
//! Writes JSON stats files alongside station data during each rollup,
//! persists state across restarts via a state file.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use chrono::Utc;
use tracing::{error, info};

use crate::accumulators::{initialise_accumulators, AccumulatorEntry, Accumulators};
use crate::config::{OUTPUT_PATH, UNCOMPRESSED_ARROW_FILES};
use crate::coverage::header::AccumulatorType;
use crate::json_io::{read_json, write_atomic, write_gz_atomic};

/// Infrastructure TOCALLs excluded from protocol stats
const INFRASTRUCTURE_TOCALLS: &[&str] = &[
    "OGNSDR", "OGNSXR", "OGNDELAY", "OGNDVS", "OGNTTN", "OGMSHT", "OGNHEL", "OGNDSX", "OGAVZ",
];

fn is_infrastructure(tocall: &str) -> bool {
    INFRASTRUCTURE_TOCALLS.contains(&tocall)
}

/// Map lat/lon to a region short code
fn region_code(lat: f64, lon: f64) -> &'static str {
    // Checked in order; first match wins. Overlaps are intentional -
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

/// Map AccumulatorType to the JSON key used in state and output files.
/// Current is excluded from protocol stats.
fn period_key(acc_type: AccumulatorType) -> &'static str {
    match acc_type {
        AccumulatorType::Day => "day",
        AccumulatorType::Month => "month",
        AccumulatorType::Year => "year",
        AccumulatorType::YearNz => "yearnz",
        _ => "unknown",
    }
}

/// The accumulator types tracked by protocol stats (Current is excluded)
const PERIOD_TYPES: &[AccumulatorType] = &[
    AccumulatorType::Day,
    AccumulatorType::Month,
    AccumulatorType::Year,
    AccumulatorType::YearNz,
];

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

/// Accumulator for a single time period (day, month, year, or yearnz).
/// Each instance holds only that period's data and resets independently.
#[derive(Debug, Clone)]
struct PeriodAccumulator {
    start_time: chrono::DateTime<Utc>,
    tocalls: HashMap<String, TocallStats>,
    /// Accepted packet counts by layer and hour-of-day (0–23)
    hourly: HashMap<String, [u64; 24]>,
    /// Number of process restarts since start_time (meaningful only for day)
    restarts: u32,
}

impl PeriodAccumulator {
    fn new() -> Self {
        Self {
            start_time: Utc::now(),
            tocalls: HashMap::new(),
            hourly: HashMap::new(),
            restarts: 0,
        }
    }

    /// Record a raw packet with pre-computed region code
    fn record_raw(&mut self, tocall: &str, flarm_num: u32, region: &'static str) {
        let entry = self.tocalls.entry(tocall.to_string()).or_insert_with(TocallStats::new);
        entry.raw_count += 1;
        entry.devices.insert(flarm_num);
        *entry.regions.entry(region).or_insert(0) += 1;
    }

    /// Record an accepted packet with pre-computed altitude band
    fn record_accepted(&mut self, tocall: &str, band: &'static str) {
        // Only update if the tocall was already seen in record_raw
        if let Some(entry) = self.tocalls.get_mut(tocall) {
            entry.accepted_count += 1;
            *entry.altitudes.entry(band).or_insert(0) += 1;
        }
    }

    /// Record an accepted packet for hourly-by-layer tracking
    fn record_hourly(&mut self, layer: &str, hour: u32) {
        let hours = self.hourly.entry(layer.to_string()).or_insert([0u64; 24]);
        hours[hour as usize % 24] += 1;
    }
}

/// Internal mutable state - per-period accumulators keyed by type (Day, Month, Year, YearNz).
/// Current is excluded - protocol stats don't track per-rollup periods.
#[derive(Debug, Clone)]
struct StatsInner {
    periods: HashMap<AccumulatorType, PeriodAccumulator>,
}

impl StatsInner {
    fn new() -> Self {
        let mut periods = HashMap::new();
        for &acc_type in PERIOD_TYPES {
            periods.insert(acc_type, PeriodAccumulator::new());
        }
        Self { periods }
    }
}

/// Thread-safe protocol statistics tracker
pub struct ProtocolStats {
    inner: Mutex<StatsInner>,
}

impl ProtocolStats {
    /// Create fresh stats starting now
    fn new() -> Self {
        Self {
            inner: Mutex::new(StatsInner::new()),
        }
    }

    /// Load from state file, falling back to fresh if stale/missing.
    ///
    /// Detects old single-period format (top-level `startTime`) and starts fresh.
    /// Validates each period against the current accumulator effective_start;
    /// resets any period whose stored start_time predates the current period boundary.
    pub fn load() -> Self {
        let state_path = format!("{}stats/protocol-stats.state.json", *OUTPUT_PATH);
        let parsed = match read_json(&state_path) {
            Some(v) => v,
            None => return Self::new(),
        };

        // Detect old format (top-level startTime) - start fresh to avoid confusion
        if parsed["startTime"].is_string() {
            info!("Protocol stats state is in old single-period format, starting fresh");
            return Self::new();
        }

        // Get current accumulator boundaries for validation
        let current_accs = initialise_accumulators();

        let mut periods = HashMap::new();
        let mut day_restarts = 0u32;
        let mut any_restored = false;

        for &acc_type in PERIOD_TYPES {
            let key = period_key(acc_type);
            let entry = match_acc_entry(&current_accs, acc_type);

            let acc = match parsed[key].as_object() {
                Some(_) => restore_period(&parsed[key]),
                None => None,
            };

            let effective_start = chrono::DateTime::from_timestamp(entry.effective_start.0 as i64, 0)
                .unwrap_or(chrono::DateTime::UNIX_EPOCH);

            let period_acc = match acc {
                Some(a) if a.start_time >= effective_start => {
                    any_restored = true;
                    if acc_type == AccumulatorType::Day {
                        day_restarts = a.restarts;
                    }
                    a
                }
                Some(_) => {
                    info!("Protocol stats {} period is stale, starting fresh for this period", key);
                    PeriodAccumulator::new()
                }
                None => PeriodAccumulator::new(),
            };

            periods.insert(acc_type, period_acc);
        }

        if !any_restored {
            return Self::new();
        }

        // Increment day restarts on successful state restore
        if let Some(day) = periods.get_mut(&AccumulatorType::Day) {
            day.restarts = day_restarts + 1;
        }

        let day_tocalls = periods.get(&AccumulatorType::Day).map(|d| d.tocalls.len()).unwrap_or(0);
        let month_tocalls = periods.get(&AccumulatorType::Month).map(|d| d.tocalls.len()).unwrap_or(0);
        let year_tocalls = periods.get(&AccumulatorType::Year).map(|d| d.tocalls.len()).unwrap_or(0);
        let yearnz_tocalls = periods.get(&AccumulatorType::YearNz).map(|d| d.tocalls.len()).unwrap_or(0);
        let restarts = periods.get(&AccumulatorType::Day).map(|d| d.restarts).unwrap_or(0);
        let start_str = parsed["day"]["startTime"].as_str().unwrap_or("(unknown)");

        info!(
            "Restored protocol stats from {}: day={} tocalls, month={} tocalls, year={} tocalls, yearnz={} tocalls, restart #{}",
            start_str, day_tocalls, month_tocalls, year_tocalls, yearnz_tocalls, restarts
        );

        Self {
            inner: Mutex::new(StatsInner { periods }),
        }
    }

    /// Record a raw packet (before filtering) - region computed once, applied to all periods
    pub fn record_raw(&self, tocall: &str, flarm_num: u32, lat: f64, lon: f64) {
        if is_infrastructure(tocall) {
            return;
        }
        let region = region_code(lat, lon);
        let mut inner = self.inner.lock().unwrap();
        for acc in inner.periods.values_mut() {
            acc.record_raw(tocall, flarm_num, region);
        }
    }

    /// Record an accepted packet for hourly-by-layer tracking - applied to all periods
    pub fn record_hourly(&self, layer: &str, hour: u32) {
        let mut inner = self.inner.lock().unwrap();
        for acc in inner.periods.values_mut() {
            acc.record_hourly(layer, hour);
        }
    }

    /// Record an accepted packet (passed all filters) - band computed once, applied to all periods
    pub fn record_accepted(&self, tocall: &str, agl_m: u16) {
        if is_infrastructure(tocall) {
            return;
        }
        let band = altitude_band(agl_m);
        let mut inner = self.inner.lock().unwrap();
        for acc in inner.periods.values_mut() {
            acc.record_accepted(tocall, band);
        }
    }

    /// Write stats during rollup.
    ///
    /// For each tracked period (Day, Month, Year, YearNz):
    /// - Writes `protocol-stats.{old_entry.file}.json.gz` every rollup
    /// - If Day: updates the `protocol-stats.json.gz` symlink
    /// - If rotated (old_entry.bucket != new_entry.bucket): resets that accumulator
    ///
    /// All JSONs are built in a single lock hold, then I/O is done lock-free,
    /// then the lock is re-acquired to reset stale accumulators.
    pub fn write_stats(&self, old_acc: &Accumulators, new_acc: &Accumulators) {
        let stats_dir = format!("{}stats", *OUTPUT_PATH);
        if let Err(e) = std::fs::create_dir_all(&stats_dir) {
            error!("Failed to create stats directory: {}", e);
            return;
        }

        let period_pairs: &[(AccumulatorType, &AccumulatorEntry, &AccumulatorEntry)] = &[
            (AccumulatorType::Day, &old_acc.day, &new_acc.day),
            (AccumulatorType::Month, &old_acc.month, &new_acc.month),
            (AccumulatorType::Year, &old_acc.year, &new_acc.year),
            (AccumulatorType::YearNz, &old_acc.yearnz, &new_acc.yearnz),
        ];

        // Build all needed JSONs while holding the lock, then release before I/O
        let jsons: Vec<(AccumulatorType, &AccumulatorEntry, &AccumulatorEntry, String)> = {
            let inner = self.inner.lock().unwrap();
            period_pairs
                .iter()
                .map(|&(acc_type, old_entry, new_entry)| {
                    let acc = inner.periods.get(&acc_type).map(build_output_json).unwrap_or_default();
                    (acc_type, old_entry, new_entry, acc)
                })
                .collect()
        };

        // Write files and update symlinks (lock-free)
        for (acc_type, old_entry, new_entry, json) in &jsons {
            let rotated = old_entry.bucket != new_entry.bucket;
            let file_name = format!("protocol-stats.{}.json.gz", old_entry.file);

            write_gz_atomic(&stats_dir, &file_name, json);
            if *UNCOMPRESSED_ARROW_FILES {
                let plain_name = format!("protocol-stats.{}.json", old_entry.file);
                write_atomic(&stats_dir, &plain_name, json);
            }

            if *acc_type == AccumulatorType::Day {
                // Update symlinks to point at today's daily file
                use crate::symlinks::symlink_atomic;
                symlink_atomic(&file_name, &format!("{}/protocol-stats.json.gz", stats_dir));
                if *UNCOMPRESSED_ARROW_FILES {
                    let plain_name = format!("protocol-stats.{}.json", old_entry.file);
                    symlink_atomic(&plain_name, &format!("{}/protocol-stats.json", stats_dir));
                }
            }

            if rotated {
                info!("Wrote {} protocol stats archive: {}", period_key(*acc_type), file_name);
            }
        }

        // Re-acquire lock to reset stale accumulators
        {
            let mut inner = self.inner.lock().unwrap();
            for (acc_type, old_entry, new_entry, _) in &jsons {
                if old_entry.bucket != new_entry.bucket {
                    inner.periods.insert(*acc_type, PeriodAccumulator::new());
                }
            }
        }

        // Persist current state for crash recovery
        self.save_state_inner();

        let day_file = &old_acc.day.file;
        info!("Wrote protocol stats: protocol-stats.{}.json.gz", day_file);
    }

    /// Save state file and write all period snapshot files (called on graceful shutdown)
    pub fn save_state(&self) {
        let stats_dir = format!("{}stats", *OUTPUT_PATH);
        if let Err(e) = std::fs::create_dir_all(&stats_dir) {
            error!("Failed to create stats directory: {}", e);
            return;
        }

        // Write snapshot files for all periods using current accumulator filenames
        let current_accs = initialise_accumulators();
        write_period_files(&self.inner.lock().unwrap(), &current_accs, &stats_dir);

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

/// Write period snapshot files for all tracked periods.
/// Used by both save_state (shutdown) and could be extended for other uses.
fn write_period_files(inner: &StatsInner, accs: &Accumulators, stats_dir: &str) {
    let period_entries: &[(AccumulatorType, &AccumulatorEntry)] = &[
        (AccumulatorType::Day, &accs.day),
        (AccumulatorType::Month, &accs.month),
        (AccumulatorType::Year, &accs.year),
        (AccumulatorType::YearNz, &accs.yearnz),
    ];

    for &(acc_type, entry) in period_entries {
        if let Some(acc) = inner.periods.get(&acc_type) {
            let json = build_output_json(acc);
            let file_name = format!("protocol-stats.{}.json.gz", entry.file);
            write_gz_atomic(stats_dir, &file_name, &json);
            if *UNCOMPRESSED_ARROW_FILES {
                let plain_name = format!("protocol-stats.{}.json", entry.file);
                write_atomic(stats_dir, &plain_name, &json);
            }
        }
    }
}

/// Return the AccumulatorEntry from an Accumulators struct for a given AccumulatorType
fn match_acc_entry<'a>(accs: &'a Accumulators, acc_type: AccumulatorType) -> &'a AccumulatorEntry {
    match acc_type {
        AccumulatorType::Day => &accs.day,
        AccumulatorType::Month => &accs.month,
        AccumulatorType::Year => &accs.year,
        AccumulatorType::YearNz => &accs.yearnz,
        AccumulatorType::Current => &accs.current,
    }
}

/// Build the public-facing JSON output for a single period accumulator
fn build_output_json(acc: &PeriodAccumulator) -> String {
    let now = Utc::now();
    let uptime = (now - acc.start_time).num_seconds().max(0) as u64;

    let mut protocols = serde_json::Map::new();
    // Sort by raw_count descending for consistent output
    let mut sorted: Vec<_> = acc.tocalls.iter().collect();
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
    let mut hourly_sorted: Vec<_> = acc.hourly.iter().collect();
    hourly_sorted.sort_by_key(|(k, _)| (*k).clone());
    for (layer, counts) in hourly_sorted {
        let arr: Vec<serde_json::Value> = counts.iter().map(|&c| serde_json::Value::from(c)).collect();
        hourly.insert(layer.clone(), serde_json::Value::Array(arr));
    }

    let output = serde_json::json!({
        "generated": now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "startTime": acc.start_time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "uptimeSeconds": uptime,
        "restarts": acc.restarts,
        "protocols": protocols,
        "hourly": hourly,
    });

    serde_json::to_string_pretty(&output).unwrap_or_else(|_| "{}".to_string())
}

/// Build the state JSON (includes device sets for unique count accuracy across restarts).
/// Format: `{ "day": {...}, "month": {...}, "year": {...}, "yearnz": {...} }`
fn build_state_json(inner: &StatsInner) -> String {
    let mut state = serde_json::Map::new();
    for &acc_type in PERIOD_TYPES {
        let key = period_key(acc_type);
        let val = inner.periods.get(&acc_type)
            .map(serialize_period)
            .unwrap_or(serde_json::Value::Null);
        state.insert(key.to_string(), val);
    }
    serde_json::to_string_pretty(&serde_json::Value::Object(state))
        .unwrap_or_else(|_| "{}".to_string())
}

/// Serialize a single period accumulator to a JSON value (for the state file)
fn serialize_period(acc: &PeriodAccumulator) -> serde_json::Value {
    let mut tocalls = serde_json::Map::new();

    for (tocall, stats) in &acc.tocalls {
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

        let mut altitudes = serde_json::Map::new();
        for (band, count) in &stats.altitudes {
            if *count > 0 {
                altitudes.insert(band.to_string(), serde_json::Value::from(*count));
            }
        }

        let mut entry = serde_json::Map::new();
        entry.insert(
            "rawCount".to_string(),
            serde_json::Value::from(stats.raw_count),
        );
        entry.insert(
            "acceptedCount".to_string(),
            serde_json::Value::from(stats.accepted_count),
        );
        entry.insert("devices".to_string(), serde_json::Value::Array(devices));
        entry.insert("regions".to_string(), serde_json::Value::Object(regions));
        entry.insert("altitudes".to_string(), serde_json::Value::Object(altitudes));

        tocalls.insert(tocall.clone(), serde_json::Value::Object(entry));
    }

    let mut hourly = serde_json::Map::new();
    for (layer, counts) in &acc.hourly {
        let arr: Vec<serde_json::Value> = counts.iter().map(|&c| serde_json::Value::from(c)).collect();
        hourly.insert(layer.clone(), serde_json::Value::Array(arr));
    }

    serde_json::json!({
        "startTime": acc.start_time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "restarts": acc.restarts,
        "tocalls": tocalls,
        "hourly": hourly,
    })
}

/// Restore a single period accumulator from a JSON value (from the state file).
/// Returns None if startTime is missing or unparseable.
fn restore_period(val: &serde_json::Value) -> Option<PeriodAccumulator> {
    let start_str = val["startTime"].as_str()?;
    let start_time = start_str.parse::<chrono::DateTime<Utc>>().ok()?;

    let mut tocalls = HashMap::new();
    if let Some(obj) = val["tocalls"].as_object() {
        for (tocall, entry) in obj {
            let mut devices = HashSet::new();
            if let Some(arr) = entry["devices"].as_array() {
                for d in arr {
                    if let Some(s) = d.as_str() {
                        if let Ok(n) = u32::from_str_radix(s, 16) {
                            devices.insert(n);
                        }
                    }
                }
            }
            let mut regions = HashMap::new();
            if let Some(robj) = entry["regions"].as_object() {
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
            if let Some(aobj) = entry["altitudes"].as_object() {
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
                    raw_count: entry["rawCount"].as_u64().unwrap_or(0),
                    accepted_count: entry["acceptedCount"].as_u64().unwrap_or(0),
                    devices,
                    regions,
                    altitudes,
                },
            );
        }
    }

    let mut hourly = HashMap::new();
    if let Some(obj) = val["hourly"].as_object() {
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

    let restarts = val["restarts"].as_u64().unwrap_or(0) as u32;

    Some(PeriodAccumulator {
        start_time,
        tocalls,
        hourly,
        restarts,
    })
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
        let day = &inner.periods[&AccumulatorType::Day];
        let month = &inner.periods[&AccumulatorType::Month];
        let year = &inner.periods[&AccumulatorType::Year];
        let yearnz = &inner.periods[&AccumulatorType::YearNz];

        // All four periods accumulate the same data
        assert_eq!(day.tocalls.len(), 2); // OGFLR + OGADSB, not OGNSDR
        assert_eq!(day.tocalls["OGFLR"].raw_count, 3);
        assert_eq!(day.tocalls["OGFLR"].accepted_count, 2);
        assert_eq!(day.tocalls["OGFLR"].devices.len(), 2);
        assert_eq!(day.tocalls["OGFLR"].regions["eu"], 2);
        assert_eq!(day.tocalls["OGFLR"].regions["na"], 1);
        assert_eq!(day.tocalls["OGFLR"].altitudes["low"], 1);
        assert_eq!(day.tocalls["OGFLR"].altitudes["mid"], 1);
        assert_eq!(day.tocalls["OGADSB"].raw_count, 1);
        assert!(!day.tocalls.contains_key("OGNSDR"));

        // Month, year, and yearnz should mirror day data
        assert_eq!(month.tocalls["OGFLR"].raw_count, 3);
        assert_eq!(year.tocalls["OGFLR"].raw_count, 3);
        assert_eq!(yearnz.tocalls["OGFLR"].raw_count, 3);
        assert_eq!(month.tocalls["OGFLR"].devices.len(), 2);
        assert_eq!(year.tocalls["OGADSB"].raw_count, 1);
        assert_eq!(yearnz.tocalls["OGADSB"].raw_count, 1);
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
        let json = build_output_json(&inner.periods[&AccumulatorType::Day]);
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

        // New format: four period objects
        assert!(parsed["day"].is_object());
        assert!(parsed["month"].is_object());
        assert!(parsed["year"].is_object());
        assert!(parsed["yearnz"].is_object());

        assert!(parsed["day"]["startTime"].is_string());
        assert_eq!(parsed["day"]["restarts"], 0);
        assert_eq!(parsed["day"]["tocalls"]["OGFLR"]["rawCount"], 2);
        assert_eq!(parsed["day"]["tocalls"]["OGFLR"]["acceptedCount"], 1);
        let devices = parsed["day"]["tocalls"]["OGFLR"]["devices"].as_array().unwrap();
        assert_eq!(devices.len(), 2);
        assert!(parsed["day"]["tocalls"]["OGFLR"]["regions"]["eu"].as_u64().unwrap() > 0);
        assert_eq!(parsed["day"]["tocalls"]["OGFLR"]["altitudes"]["high"], 1);

        let flarm_hours = parsed["day"]["hourly"]["flarm"].as_array().unwrap();
        assert_eq!(flarm_hours[10], 2);
        assert_eq!(flarm_hours[0], 0);
        let adsb_hours = parsed["day"]["hourly"]["adsb"].as_array().unwrap();
        assert_eq!(adsb_hours[22], 1);

        // Month, year, and yearnz should mirror day (all accumulated from same calls)
        assert_eq!(parsed["month"]["tocalls"]["OGFLR"]["rawCount"], 2);
        assert_eq!(parsed["year"]["tocalls"]["OGFLR"]["rawCount"], 2);
        assert_eq!(parsed["yearnz"]["tocalls"]["OGFLR"]["rawCount"], 2);
    }

    #[test]
    fn test_period_accumulators_independent() {
        // Verify that resetting day does not affect month, year, or yearnz
        let stats = ProtocolStats::new();
        stats.record_raw("OGFLR", 0x111111, 48.0, 11.0);
        stats.record_accepted("OGFLR", 1000);
        stats.record_hourly("flarm", 8);

        {
            let mut inner = stats.inner.lock().unwrap();
            // Simulate a day rotation - reset only the day accumulator
            inner.periods.insert(AccumulatorType::Day, PeriodAccumulator::new());
        }

        let inner = stats.inner.lock().unwrap();
        let day = &inner.periods[&AccumulatorType::Day];
        let month = &inner.periods[&AccumulatorType::Month];
        let year = &inner.periods[&AccumulatorType::Year];
        let yearnz = &inner.periods[&AccumulatorType::YearNz];

        // Day is fresh
        assert_eq!(day.tocalls.len(), 0);
        assert_eq!(day.hourly.len(), 0);
        // Month, year, and yearnz still have the data
        assert_eq!(month.tocalls["OGFLR"].raw_count, 1);
        assert_eq!(year.tocalls["OGFLR"].raw_count, 1);
        assert_eq!(yearnz.tocalls["OGFLR"].raw_count, 1);
        assert_eq!(month.hourly["flarm"][8], 1);
        assert_eq!(year.hourly["flarm"][8], 1);
        assert_eq!(yearnz.hourly["flarm"][8], 1);
    }

    #[test]
    fn test_period_accumulator_independence() {
        // After a simulated day reset, the month still retains previously recorded data
        let stats = ProtocolStats::new();
        stats.record_raw("OGFLR", 0x222222, 48.0, 11.0);
        stats.record_hourly("combined", 6);

        // Simulate day rotation (month/year/yearnz keep their data)
        {
            let mut inner = stats.inner.lock().unwrap();
            inner.periods.insert(AccumulatorType::Day, PeriodAccumulator::new());
        }

        // Record more data - goes to all periods including the fresh day
        stats.record_raw("OGFLR", 0x333333, 48.0, 11.0);

        let inner = stats.inner.lock().unwrap();
        let day = &inner.periods[&AccumulatorType::Day];
        let month = &inner.periods[&AccumulatorType::Month];

        // Day only has the post-reset record
        assert_eq!(day.tocalls["OGFLR"].raw_count, 1);
        // Month has both records (pre- and post-reset)
        assert_eq!(month.tocalls["OGFLR"].raw_count, 2);
    }
}
