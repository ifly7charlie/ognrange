//! Station metadata management.
//!
//! Tracks station details (location, ID, stats) in memory with LevelDB persistence.
//! The LevelDB handle is NOT held in the struct (it's not Send/Sync).
//! Instead, all DB operations go through `spawn_blocking`.

use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::RwLock;
use tracing::{error, info, warn};

use crate::bitvec::{bitvec_to_hex, hex_to_bitvec, slot_from_timestamp};
use crate::config::{DB_PATH, STATION_MOVE_THRESHOLD_KM};
use crate::db::TrackedDb;
use crate::stats_accumulator::DailyStatsData;
use crate::types::{Epoch, StationId, StationName};

/// Message sent to the DB writer thread
enum DbWrite {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
    Shutdown,
}

// Re-export compute_uptime for external callers (rollup etc.)
pub use crate::bitvec::compute_uptime;

/// Per-station and global APRS packet statistics.
///
/// Tracks raw/accepted packet counts, exception counters, and per-layer hourly
/// breakdowns. Used identically for both per-station (`StationDetails.stats`)
/// and the global aggregate (`StationGlobalStats`).
///
/// Resets daily: stats in the status DB are replaced with a fresh default each
/// time the day rolls over. Historical data is preserved in dated daily files.
///
/// Backward-compat: old DB/JSON entries may have `count` instead of `accepted`;
/// the API normalises this on read.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct AprsPacketStats {
    /// All packets seen by this station before any filtering
    pub count: u64,
    /// Packets that passed all filters and were written to coverage data
    pub accepted: u64,
    /// Sum of packet ages at receipt (server receive time − packet timestamp) in seconds,
    /// for accepted packets only. Divide by `accepted` to get mean packet age.
    pub delay_sum_secs: u64,
    pub ignored_tracker: u32,
    pub invalid_tracker: u32,
    pub invalid_timestamp: u32,
    pub ignored_stationary: u32,
    pub ignored_signal0: u32,
    #[serde(rename = "ignoredPAW")]
    pub ignored_paw: u32,
    pub ignored_h3stationary: u32,
    pub ignored_elevation: u32,
    pub ignored_future_timestamp: u32,
    pub ignored_stale_timestamp: u32,
    /// Accepted packet counts by layer and hour-of-day (0–23).
    /// e.g. `{"flarm": [0, 12, 5, ...], "adsb": [...]}`
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub hourly: HashMap<String, [u64; 24]>,
}

impl DailyStatsData for AprsPacketStats {
    fn to_output_json(&self, start_time: &chrono::DateTime<chrono::Utc>) -> String {
        let now = chrono::Utc::now();
        let uptime = (now - *start_time).num_seconds().max(0) as u64;

        // Build hourly map sorted by layer name
        let mut hourly = serde_json::Map::new();
        let mut hourly_sorted: Vec<_> = self.hourly.iter().collect();
        hourly_sorted.sort_by_key(|(k, _)| (*k).clone());
        for (layer, counts) in hourly_sorted {
            let arr: Vec<serde_json::Value> =
                counts.iter().map(|&c| serde_json::Value::from(c)).collect();
            hourly.insert(layer.clone(), serde_json::Value::Array(arr));
        }

        let output = serde_json::json!({
            "generated": now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            "startTime": start_time.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            "uptimeSeconds": uptime,
            "count": self.count,
            "accepted": self.accepted,
            "delaySumSecs": self.delay_sum_secs,
            "ignoredTracker": self.ignored_tracker,
            "invalidTracker": self.invalid_tracker,
            "invalidTimestamp": self.invalid_timestamp,
            "ignoredStationary": self.ignored_stationary,
            "ignoredSignal0": self.ignored_signal0,
            "ignoredPAW": self.ignored_paw,
            "ignoredH3stationary": self.ignored_h3stationary,
            "ignoredElevation": self.ignored_elevation,
            "ignoredFutureTimestamp": self.ignored_future_timestamp,
            "ignoredStaleTimestamp": self.ignored_stale_timestamp,
            "hourly": hourly,
        });

        serde_json::to_string_pretty(&output).unwrap_or_else(|_| "{}".to_string())
    }
}

/// Deserialize an `Option<[f64; 2]>` that tolerates nulls inside the array.
/// JSON like `[null, null]` or `[52.1, null]` becomes `None` instead of a parse error.
fn deserialize_coord_pair<'de, D>(deserializer: D) -> Result<Option<[f64; 2]>, D::Error>
where
    D: Deserializer<'de>,
{
    let v: Option<[Option<f64>; 2]> = Option::deserialize(deserializer)?;
    Ok(v.and_then(|[a, b]| Some([a?, b?])))
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct StationDetails {
    pub id: StationId,
    pub station: StationName,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lat: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lng: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "primary_location",
            deserialize_with = "deserialize_coord_pair")]
    pub primary_location: Option<[f64; 2]>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "previous_location",
            deserialize_with = "deserialize_coord_pair")]
    pub previous_location: Option<[f64; 2]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_packet: Option<Epoch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_location: Option<Epoch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_beacon: Option<Epoch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notice: Option<String>,
    #[serde(default)]
    pub moved: bool,
    #[serde(default)]
    pub bouncing: bool,
    #[serde(default)]
    pub mobile: bool,
    /// Consecutive packets at locations matching neither primary nor previous
    #[serde(default)]
    pub new_location_count: u16,
    /// Epoch when the station's coverage data was last purged
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purged_at: Option<Epoch>,
    /// Reason for the last data purge (e.g. "moved", "expired")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purge_reason: Option<String>,
    /// Last time a packet was received near `primary_location`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_at_primary: Option<Epoch>,
    /// Last time a packet was received near `previous_location`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_at_previous: Option<Epoch>,
    #[serde(default)]
    pub valid: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub layer_mask: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_epoch: Option<Epoch>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_output_file: Option<Epoch>,
    #[serde(default)]
    pub stats: AprsPacketStats,
    // NOTE: changes to StationDetails fields must be reflected in docs/STATIONS.md and docs/STATION.md
    /// Daily beacon activity bitvector: 144 bits (one per 10-min UTC slot), hex-encoded
    #[serde(skip_serializing_if = "Option::is_none")]
    pub beacon_activity: Option<String>,
    /// UTC date (YYYY-MM-DD) the beacon activity bitvector covers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub beacon_activity_date: Option<String>,
    /// Station uptime today as a percentage (0.0–100.0). Computed at output time, not persisted.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uptime: Option<f32>,
    /// Human-readable layer names derived from layer_mask. Computed at output time, not persisted.
    #[serde(skip_deserializing)]
    pub layers: Vec<String>,
}

/// Station status manager with thread-safe access.
/// DB writes are serialized through a channel to a dedicated writer thread.
pub struct StationManager {
    stations: RwLock<HashMap<StationName, StationDetails>>,
    station_ids: RwLock<HashMap<StationId, StationName>>,
    next_id: AtomicU16,
    db_path: String,
    write_tx: std::sync::mpsc::Sender<DbWrite>,
    case_insensitive: bool,
}

impl StationManager {
    /// Create and load station status from the database
    pub fn new(case_insensitive: bool) -> Self {
        let db_path = format!("{}status", *DB_PATH);

        // Channel for the writer thread (created now, thread spawned after load)
        let (write_tx, write_rx) = std::sync::mpsc::channel::<DbWrite>();

        let mut manager = StationManager {
            stations: RwLock::new(HashMap::new()),
            station_ids: RwLock::new(HashMap::new()),
            next_id: AtomicU16::new(1),
            db_path: db_path.clone(),
            write_tx,
            case_insensitive,
        };

        // Load synchronously first — opens and closes the DB
        if let Err(e) = manager.load_sync() {
            error!("Fatal: failed to open station database {}: {}", db_path, e);
            error!("Is another instance already running?");
            error!("If the database is corrupt, consider restoring from a backup.");
            error!("To rebuild from the last exported data, run: yarn tsx bin/recover_station_db.ts");
            std::process::exit(1);
        }

        // Spawn the writer thread which takes over the DB lock
        let writer_db_path = db_path;
        std::thread::spawn(move || {
            let mut db = match TrackedDb::open(&writer_db_path, true) {
                Ok(db) => db,
                Err(e) => {
                    error!("Fatal: writer thread failed to open status DB {}: {}", writer_db_path, e);
                    return;
                }
            };
            while let Ok(msg) = write_rx.recv() {
                match msg {
                    DbWrite::Put { key, value } => {
                        let _ = db.put(key.as_bytes(), &value);
                        let _ = db.flush();
                    }
                    DbWrite::Delete { key } => {
                        let _ = db.delete(key.as_bytes());
                        let _ = db.flush();
                    }
                    DbWrite::Shutdown => {
                        let _ = db.flush();
                        break;
                    }
                }
            }
        });

        manager
    }

    fn load_sync(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut db = TrackedDb::open(&self.db_path, true)?;
        let entries = crate::db::read_all(&mut db);

        let mut max_id: u16 = 0;
        let mut has_global = false;

        for (raw_name, val_buf) in &entries {
            if raw_name == "global" {
                has_global = true;
                continue;
            }
            let name = if self.case_insensitive {
                raw_name.to_uppercase()
            } else {
                raw_name.clone()
            };
            match serde_json::from_slice::<StationDetails>(val_buf) {
                Ok(mut details) => {
                    if details.id.0 > max_id {
                        max_id = details.id.0;
                    }
                    let station_name = StationName(name);
                    details.station = station_name.clone();
                    details.layers = crate::layers::layer_names_from_mask(details.layer_mask.unwrap_or(0));
                    self.station_ids
                        .write()
                        .unwrap()
                        .insert(details.id, station_name.clone());
                    self.stations
                        .write()
                        .unwrap()
                        .insert(station_name, details);
                }
                Err(e) => {
                    warn!("Failed to parse station {}: {}", name, e);
                }
            }
        }

        if has_global {
            info!("Removing stale 'global' entry from station status DB");
            let _ = db.delete(b"global");
            let _ = db.flush();
        }

        let next = max_id + 1;
        self.next_id.store(next, Ordering::SeqCst);
        let count = self.stations.read().unwrap().len();
        info!("Loaded {} stations, next ID: {}", count, next);

        Ok(())
    }

    pub fn next_station_id(&self) -> u16 {
        self.next_id.load(Ordering::SeqCst)
    }

    /// Get or create station details.
    /// Returns None if the station ID limit has been reached.
    pub fn get_or_create(&self, name: &StationName) -> Option<StationDetails> {
        // Fast path: read lock
        {
            let stations = self.stations.read().unwrap();
            if let Some(details) = stations.get(name) {
                return Some(details.clone());
            }
        }

        // Slow path: write lock, re-check to avoid duplicate ID allocation
        let mut stations = self.stations.write().unwrap();
        if let Some(details) = stations.get(name) {
            return Some(details.clone());
        }

        // Hard cap: prevent u16 wrap-around
        use crate::config::{MAX_STATION_ID, STATION_ID_WARN_PERCENT};
        let current = self.next_id.load(Ordering::SeqCst);
        if current > MAX_STATION_ID {
            error!(
                "Station ID limit reached ({}/{}), rejecting new station: {}",
                current, MAX_STATION_ID, name
            );
            return None;
        }
        let warn_threshold = (MAX_STATION_ID as u32 * STATION_ID_WARN_PERCENT as u32 / 100) as u16;
        if current >= warn_threshold {
            warn!(
                "Station ID allocation at {:.0}% ({}/{}), new station: {}",
                current as f64 / MAX_STATION_ID as f64 * 100.0,
                current,
                MAX_STATION_ID,
                name
            );
        }

        let new_id = StationId(self.next_id.fetch_add(1, Ordering::SeqCst));
        let details = StationDetails {
            id: new_id,
            station: name.clone(),
            lat: None,
            lng: None,
            primary_location: None,
            previous_location: None,
            last_packet: None,
            last_location: None,
            last_beacon: None,
            status: None,
            notice: None,
            moved: false,
            bouncing: false,
            mobile: false,
            new_location_count: 0,
            purged_at: None,
            purge_reason: None,
            last_seen_at_primary: None,
            last_seen_at_previous: None,
            valid: false,
            layer_mask: None,
            output_epoch: None,
            output_date: None,
            last_output_file: None,
            stats: AprsPacketStats::default(),
            beacon_activity: None,
            beacon_activity_date: None,
            uptime: None,
            layers: Vec::new(),
        };

        info!(
            "Allocated ID {} to {}, {} stations total",
            new_id.0, name, stations.len() + 1
        );

        stations.insert(name.clone(), details.clone());
        self.station_ids
            .write()
            .unwrap()
            .insert(new_id, name.clone());

        self.persist(&details);

        Some(details)
    }

    /// Get station details without creating
    pub fn get(&self, name: &StationName) -> Option<StationDetails> {
        self.stations.read().unwrap().get(name).cloned()
    }

    /// Update station details in memory only. Call flush_all() to persist to the DB.
    pub fn update(&self, details: &StationDetails) {
        self.stations
            .write()
            .unwrap()
            .insert(details.station.clone(), details.clone());
    }

    /// Persist all in-memory station state to the DB.
    pub fn flush_all(&self) {
        let stations: Vec<StationDetails> = self.stations.read().unwrap().values().cloned().collect();
        for details in &stations {
            self.persist(&details);
        }
    }

    /// Send a station update to the writer thread
    fn persist(&self, details: &StationDetails) {
        if details.station.as_str() == "global" {
            return;
        }
        let key = details.station.as_str().to_string();
        let json = match serde_json::to_vec(details) {
            Ok(j) => j,
            Err(e) => {
                error!("Failed to serialize station {}: {}", details.station, e);
                return;
            }
        };
        let _ = self.write_tx.send(DbWrite::Put { key, value: json });
    }

    /// Get station name by ID
    pub fn get_name(&self, id: StationId) -> Option<StationName> {
        if id.0 == 0 {
            return Some(StationName("global".to_string()));
        }
        self.station_ids.read().unwrap().get(&id).cloned()
    }

    /// Create a StationManager for tests (no database, no writer thread)
    #[cfg(test)]
    fn new_for_test() -> Self {
        let (write_tx, _write_rx) = std::sync::mpsc::channel::<DbWrite>();
        StationManager {
            stations: RwLock::new(HashMap::new()),
            station_ids: RwLock::new(HashMap::new()),
            next_id: AtomicU16::new(1),
            db_path: String::new(),
            write_tx,
            case_insensitive: false,
        }
    }

    /// Check if a station has moved and update its location.
    ///
    /// Unified model for three scenarios:
    /// - **Bouncing**: two stations sharing a callsign — both locations stay fresh
    /// - **Moved**: test→production relocation — old location decays (handled in rollup)
    /// - **Mobile**: receiver on a vehicle — consecutive new locations exceed threshold
    pub fn check_station_moved(
        &self,
        name: &StationName,
        lat: f64,
        lng: f64,
        timestamp: Epoch,
        raw_packet: &str,
    ) {
        let mut details = match self.get(name) {
            Some(d) => d,
            None => return,
        };

        if details.primary_location.is_none() {
            details.primary_location = Some([lat, lng]);
        }
        if details.previous_location.is_none() {
            details.previous_location = details
                .lat
                .zip(details.lng)
                .map(|(la, lo)| [la, lo])
                .or(details.primary_location);
        }

        let primary = details.primary_location.unwrap_or([lat, lng]);
        let previous = details.previous_location.unwrap_or(primary);
        let dist_primary = great_circle_distance(primary[0], primary[1], lat, lng);
        let dist_previous = great_circle_distance(previous[0], previous[1], lat, lng);
        let threshold = *STATION_MOVE_THRESHOLD_KM;

        // 1. At primary location — station is where we expect it
        if dist_primary <= threshold {
            details.last_seen_at_primary = Some(timestamp);
            details.new_location_count = 0;
            if details.mobile {
                info!("{} appears to have stopped moving", name);
                details.mobile = false;
                details.bouncing = false;
                // Settle: previous = primary, with matching timestamp
                details.previous_location = details.primary_location;
                details.last_seen_at_previous = details.last_seen_at_primary;
            }
        }
        // 2. At previous location — bouncing between two known locations
        else if dist_previous <= threshold {
            details.last_seen_at_previous = Some(timestamp);
            details.bouncing = true;
            details.new_location_count = 0;
            if details.mobile {
                info!("{} appears to have stopped moving", name);
                details.mobile = false;
            }
        }
        // 3. Neither location — new location
        else {
            details.new_location_count += 1;

            if details.new_location_count >= 3 {
                if !details.mobile {
                    info!("{} appears to be mobile ({:.1}km from primary)", name, dist_primary);
                }
                details.mobile = true;
            } else if details.new_location_count == 1 {
                // First new location: log once
                warn!("{}", raw_packet);
                warn!(
                    "Station {} at {},{} ({:.1}km from primary) — pending confirmation",
                    name, lat, lng, dist_primary
                );
            }

            // Rotate: new → primary, old primary → previous
            // Timestamps follow their coordinates
            details.previous_location = details.primary_location;
            details.last_seen_at_previous = details.last_seen_at_primary;
            details.primary_location = Some([lat, lng]);
            details.last_seen_at_primary = Some(timestamp);
            details.bouncing = true;
        }

        details.lat = Some(lat);
        details.lng = Some(lng);
        details.last_location = Some(timestamp);
        self.update(&details);
    }

    /// Update station beacon status
    pub fn update_station_beacon(&self, name: &StationName, body: &str, timestamp: Epoch) {
        let mut details = match self.get(name) {
            Some(d) => d,
            None => return,
        };
        details.last_beacon = Some(timestamp);
        details.status = Some(body.to_string());
        self.update(&details);
    }

    /// Record a station beacon in the daily beacon activity bitvector.
    /// Sets the bit for the 10-minute UTC slot corresponding to `timestamp`.
    pub fn record_beacon(&self, name: &StationName, timestamp: u32) {
        let utc_date = match chrono::DateTime::from_timestamp(timestamp as i64, 0) {
            Some(dt) => dt.format("%Y-%m-%d").to_string(),
            None => return,
        };

        let slot = slot_from_timestamp(timestamp);

        let mut details = match self.get(name) {
            Some(d) => d,
            None => return,
        };

        // Reset if date changed
        let mut bits = match (&details.beacon_activity, &details.beacon_activity_date) {
            (Some(hex), Some(date)) if date == &utc_date => {
                hex_to_bitvec(hex).unwrap_or([0u64; 3])
            }
            _ => [0u64; 3],
        };

        bits[slot / 64] |= 1u64 << (slot % 64);

        details.beacon_activity = Some(bitvec_to_hex(&bits));
        details.beacon_activity_date = Some(utc_date);
        self.update(&details);
    }

    /// Close the database, persisting final state via the writer thread
    pub fn close(&self) {
        let all_stations: Vec<StationDetails> =
            self.stations.read().unwrap().values().cloned().collect();

        for details in &all_stations {
            self.persist(details);
        }
        let _ = self.write_tx.send(DbWrite::Shutdown);
    }

    /// Get all station details
    pub fn all_stations(&self) -> Vec<StationDetails> {
        self.stations.read().unwrap().values().cloned().collect()
    }

    /// All stations with global first (id=0), matching TypeScript's allStationsDetails({includeGlobal: true})
    pub fn all_stations_with_global(&self) -> Vec<StationDetails> {
        let mut result = vec![StationDetails {
            id: StationId(0),
            station: StationName("global".to_string()),
            lat: None,
            lng: None,
            primary_location: None,
            previous_location: None,
            last_packet: None,
            last_location: None,
            last_beacon: None,
            status: None,
            notice: None,
            moved: false,
            bouncing: false,
            mobile: false,
            new_location_count: 0,
            purged_at: None,
            purge_reason: None,
            last_seen_at_primary: None,
            last_seen_at_previous: None,
            valid: false,
            layer_mask: None,
            output_epoch: None,
            output_date: None,
            last_output_file: None,
            stats: AprsPacketStats::default(),
            beacon_activity: None,
            beacon_activity_date: None,
            uptime: None,
            layers: Vec::new(),
        }];
        result.extend(
            self.stations.read().unwrap().values()
                .filter(|d| d.station.as_str() != "global")
                .cloned()
        );
        result
    }
}

/// Haversine great-circle distance in kilometers
fn great_circle_distance(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    let r = 6371.0;
    let d_lat = (lat2 - lat1).to_radians();
    let d_lng = (lng2 - lng1).to_radians();
    let a = (d_lat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (d_lng / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    r * c
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitvec::bitvec_to_hex;

    fn make_test_details() -> StationDetails {
        StationDetails {
            id: StationId(1),
            station: StationName("TEST".to_string()),
            lat: None,
            lng: None,
            primary_location: None,
            previous_location: None,
            last_packet: None,
            last_location: None,
            last_beacon: None,
            status: None,
            notice: None,
            moved: false,
            bouncing: false,
            mobile: false,
            new_location_count: 0,
            purged_at: None,
            purge_reason: None,
            last_seen_at_primary: None,
            last_seen_at_previous: None,
            valid: false,
            layer_mask: None,
            output_epoch: None,
            output_date: None,
            last_output_file: None,
            stats: AprsPacketStats::default(),
            beacon_activity: None,
            beacon_activity_date: None,
            uptime: None,
            layers: Vec::new(),
        }
    }

    #[test]
    fn test_last_seen_timestamps_serde() {
        let mut details = make_test_details();

        // Without timestamps
        let json = serde_json::to_string(&details).unwrap();
        assert!(!json.contains("lastSeenAtPrimary"));
        assert!(!json.contains("lastSeenAtPrevious"));
        let restored: StationDetails = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.last_seen_at_primary, None);
        assert_eq!(restored.last_seen_at_previous, None);

        // With timestamps
        details.last_seen_at_primary = Some(Epoch(1000000));
        details.last_seen_at_previous = Some(Epoch(900000));
        let json = serde_json::to_string(&details).unwrap();
        let restored: StationDetails = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.last_seen_at_primary, Some(Epoch(1000000)));
        assert_eq!(restored.last_seen_at_previous, Some(Epoch(900000)));
    }

    // Locations for movement tests
    // Victoria, Australia (-37.308, 142.988)
    const LOC_A: (f64, f64) = (-37.308, 142.988);
    // Sydney, Australia (-33.918, 151.099) — ~824km from LOC_A
    const LOC_B: (f64, f64) = (-33.918, 151.099);
    // Perth, Australia (-31.95, 115.86) — far from both A and B
    const LOC_C: (f64, f64) = (-31.95, 115.86);
    // 500m north of LOC_A
    const LOC_D: (f64, f64) = (-37.3035, 142.988);

    fn get_station(mgr: &StationManager, name: &str) -> StationDetails {
        mgr.get_or_create(&StationName(name.to_string())).expect("test station allocation failed")
    }

    #[test]
    fn test_stationary_station() {
        let mgr = StationManager::new_for_test();
        let name = StationName("TEST".to_string());

        // Repeated packets at same location
        for i in 0..5 {
            mgr.check_station_moved(&name, LOC_A.0, LOC_A.1, Epoch(1000 + i * 1000), "raw");
        }
        let s = get_station(&mgr, "TEST");
        assert_eq!(s.primary_location, Some([LOC_A.0, LOC_A.1]));
        assert!(!s.moved);
        assert!(!s.bouncing);
        assert!(!s.mobile);
        assert_eq!(s.new_location_count, 0);
        assert_eq!(s.last_seen_at_primary, Some(Epoch(5000)));
    }

    #[test]
    fn test_bouncing_two_locations() {
        let mgr = StationManager::new_for_test();
        let name = StationName("TEST".to_string());

        // Establish at LOC_A
        mgr.check_station_moved(&name, LOC_A.0, LOC_A.1, Epoch(1000), "raw");

        // Packet from LOC_B — new location, bouncing starts
        mgr.check_station_moved(&name, LOC_B.0, LOC_B.1, Epoch(2000), "raw2");
        let s = get_station(&mgr, "TEST");
        assert!(s.bouncing);
        assert!(!s.mobile);
        assert_eq!(s.primary_location, Some([LOC_B.0, LOC_B.1]));
        assert_eq!(s.previous_location, Some([LOC_A.0, LOC_A.1]));
        assert_eq!(s.last_seen_at_primary, Some(Epoch(2000)));
        assert_eq!(s.last_seen_at_previous, Some(Epoch(1000)));

        // Back to LOC_A — hits "at previous" branch, both timestamps fresh
        mgr.check_station_moved(&name, LOC_A.0, LOC_A.1, Epoch(3000), "raw3");
        let s = get_station(&mgr, "TEST");
        assert!(s.bouncing);
        assert!(!s.mobile);
        assert_eq!(s.last_seen_at_previous, Some(Epoch(3000)));
        assert_eq!(s.new_location_count, 0);

        // Many more alternating packets — all silently accepted
        for i in 0..10 {
            let (loc, ts) = if i % 2 == 0 {
                (LOC_B, 4000 + i * 100)
            } else {
                (LOC_A, 4000 + i * 100)
            };
            mgr.check_station_moved(&name, loc.0, loc.1, Epoch(ts), "raw_repeat");
        }
        let s = get_station(&mgr, "TEST");
        assert!(s.bouncing);
        assert!(!s.moved);
        assert!(!s.mobile);
    }

    #[test]
    fn test_relocation() {
        let mgr = StationManager::new_for_test();
        let name = StationName("TEST".to_string());

        // Establish at LOC_A
        mgr.check_station_moved(&name, LOC_A.0, LOC_A.1, Epoch(1000), "raw");

        // Move to LOC_B
        mgr.check_station_moved(&name, LOC_B.0, LOC_B.1, Epoch(2000), "raw2");
        let s = get_station(&mgr, "TEST");
        assert!(s.bouncing);
        assert_eq!(s.last_seen_at_previous, Some(Epoch(1000)));

        // Only packets from LOC_B — last_seen_at_previous stays stale
        for i in 1..5 {
            mgr.check_station_moved(&name, LOC_B.0, LOC_B.1, Epoch(2000 + i * 1000), "raw_b");
        }
        let s = get_station(&mgr, "TEST");
        assert!(s.bouncing);
        assert!(!s.mobile);
        assert_eq!(s.last_seen_at_previous, Some(Epoch(1000)));
        assert_eq!(s.last_seen_at_primary, Some(Epoch(6000)));
    }

    #[test]
    fn test_mobile_fast() {
        let mgr = StationManager::new_for_test();
        let name = StationName("TEST".to_string());

        // Establish at LOC_A
        mgr.check_station_moved(&name, LOC_A.0, LOC_A.1, Epoch(1000), "raw");

        // Consecutive packets at new locations: B, C, D — all far from each other
        mgr.check_station_moved(&name, LOC_B.0, LOC_B.1, Epoch(2000), "raw2");
        let s = get_station(&mgr, "TEST");
        assert!(!s.mobile);
        assert_eq!(s.new_location_count, 1);

        mgr.check_station_moved(&name, LOC_C.0, LOC_C.1, Epoch(3000), "raw3");
        let s = get_station(&mgr, "TEST");
        assert!(!s.mobile);
        assert_eq!(s.new_location_count, 2);

        mgr.check_station_moved(&name, LOC_D.0, LOC_D.1, Epoch(4000), "raw4");
        let s = get_station(&mgr, "TEST");
        assert!(s.mobile);
        assert_eq!(s.new_location_count, 3);

        // Further new locations keep mobile=true
        mgr.check_station_moved(&name, LOC_A.0 + 1.0, LOC_A.1 + 1.0, Epoch(5000), "raw5");
        let s = get_station(&mgr, "TEST");
        assert!(s.mobile);
    }

    #[test]
    fn test_mobile_slow() {
        let mgr = StationManager::new_for_test();
        let name = StationName("TEST".to_string());

        // Establish at origin
        let base_lat = -37.0;
        let base_lng = 143.0;
        mgr.check_station_moved(&name, base_lat, base_lng, Epoch(1000), "raw");

        // Drift 300m per step (well above 200m threshold)
        // ~0.003 degrees latitude ≈ 333m
        for i in 1..=4 {
            let lat = base_lat + (i as f64) * 0.003;
            mgr.check_station_moved(&name, lat, base_lng, Epoch(1000 + i * 1000), "raw");
        }
        let s = get_station(&mgr, "TEST");
        assert!(s.mobile);
        assert!(s.new_location_count >= 3);
    }

    #[test]
    fn test_mobile_stops() {
        let mgr = StationManager::new_for_test();
        let name = StationName("TEST".to_string());

        // Establish, then become mobile
        mgr.check_station_moved(&name, LOC_A.0, LOC_A.1, Epoch(1000), "raw");
        mgr.check_station_moved(&name, LOC_B.0, LOC_B.1, Epoch(2000), "raw");
        mgr.check_station_moved(&name, LOC_C.0, LOC_C.1, Epoch(3000), "raw");
        mgr.check_station_moved(&name, LOC_D.0, LOC_D.1, Epoch(4000), "raw");
        let s = get_station(&mgr, "TEST");
        assert!(s.mobile);

        // Now the primary is LOC_D. Send 2 packets from LOC_D — station stops
        mgr.check_station_moved(&name, LOC_D.0, LOC_D.1, Epoch(5000), "raw");
        let s = get_station(&mgr, "TEST");
        // First packet at primary clears mobile
        assert!(!s.mobile);
        assert!(!s.bouncing);
        // previous = primary (settled)
        assert_eq!(s.previous_location, s.primary_location);
        assert_eq!(s.last_seen_at_previous, s.last_seen_at_primary);
        assert_eq!(s.new_location_count, 0);
    }

    #[test]
    fn test_mobile_stops_at_previous() {
        let mgr = StationManager::new_for_test();
        let name = StationName("TEST".to_string());

        // Establish at LOC_A, move around, become mobile
        mgr.check_station_moved(&name, LOC_A.0, LOC_A.1, Epoch(1000), "raw");
        mgr.check_station_moved(&name, LOC_B.0, LOC_B.1, Epoch(2000), "raw");
        mgr.check_station_moved(&name, LOC_C.0, LOC_C.1, Epoch(3000), "raw");
        mgr.check_station_moved(&name, LOC_D.0, LOC_D.1, Epoch(4000), "raw");
        let s = get_station(&mgr, "TEST");
        assert!(s.mobile);

        // After LOC_D as primary, previous is LOC_C
        // Now send a packet from LOC_C (previous location)
        mgr.check_station_moved(&name, LOC_C.0, LOC_C.1, Epoch(5000), "raw");
        let s = get_station(&mgr, "TEST");
        // Stops mobile, but bouncing=true because two locations remain
        assert!(!s.mobile);
        assert!(s.bouncing);
        assert_eq!(s.new_location_count, 0);
    }

    #[test]
    fn test_mobile_serde() {
        let mut details = make_test_details();
        details.mobile = true;
        details.new_location_count = 5;

        let json = serde_json::to_string(&details).unwrap();
        let restored: StationDetails = serde_json::from_str(&json).unwrap();
        assert!(restored.mobile);
        assert_eq!(restored.new_location_count, 5);

        // Default deserialization (missing fields)
        let minimal = r#"{"id":1,"station":"X","moved":false,"bouncing":false,"valid":false,"stats":{}}"#;
        let restored: StationDetails = serde_json::from_str(minimal).unwrap();
        assert!(!restored.mobile);
        assert_eq!(restored.new_location_count, 0);
    }

    #[test]
    fn test_timestamp_follows_coordinates() {
        let mgr = StationManager::new_for_test();
        let name = StationName("TEST".to_string());

        // Establish at LOC_A with timestamp 1000
        mgr.check_station_moved(&name, LOC_A.0, LOC_A.1, Epoch(1000), "raw");
        let s = get_station(&mgr, "TEST");
        assert_eq!(s.primary_location, Some([LOC_A.0, LOC_A.1]));
        assert_eq!(s.last_seen_at_primary, Some(Epoch(1000)));

        // Move to LOC_B with timestamp 2000
        // Old primary (LOC_A, ts=1000) becomes previous
        mgr.check_station_moved(&name, LOC_B.0, LOC_B.1, Epoch(2000), "raw");
        let s = get_station(&mgr, "TEST");
        assert_eq!(s.primary_location, Some([LOC_B.0, LOC_B.1]));
        assert_eq!(s.last_seen_at_primary, Some(Epoch(2000)));
        assert_eq!(s.previous_location, Some([LOC_A.0, LOC_A.1]));
        assert_eq!(s.last_seen_at_previous, Some(Epoch(1000)));

        // Move to LOC_C with timestamp 3000
        // Old primary (LOC_B, ts=2000) becomes previous
        mgr.check_station_moved(&name, LOC_C.0, LOC_C.1, Epoch(3000), "raw");
        let s = get_station(&mgr, "TEST");
        assert_eq!(s.primary_location, Some([LOC_C.0, LOC_C.1]));
        assert_eq!(s.last_seen_at_primary, Some(Epoch(3000)));
        assert_eq!(s.previous_location, Some([LOC_B.0, LOC_B.1]));
        assert_eq!(s.last_seen_at_previous, Some(Epoch(2000)));
    }

    #[test]
    fn test_first_packet_sets_primary_and_last_seen() {
        let mgr = StationManager::new_for_test();
        let name = StationName("TEST".to_string());
        mgr.check_station_moved(&name, LOC_A.0, LOC_A.1, Epoch(1000), "raw");
        let s = get_station(&mgr, "TEST");
        assert_eq!(s.primary_location, Some([LOC_A.0, LOC_A.1]));
        assert!(!s.moved);
        assert!(!s.bouncing);
        assert!(!s.mobile);
        assert_eq!(s.last_seen_at_primary, Some(Epoch(1000)));
    }

    #[test]
    fn test_activity_serde_roundtrip() {
        let mut details = make_test_details();

        // Without beacon activity
        let json = serde_json::to_string(&details).unwrap();
        assert!(!json.contains("beaconActivity"));

        // With beacon activity
        let bits = [0x01u64, 0x00, 0x00];
        details.beacon_activity = Some(bitvec_to_hex(&bits));
        details.beacon_activity_date = Some("2026-03-15".to_string());
        let json = serde_json::to_string(&details).unwrap();
        let restored: StationDetails = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.beacon_activity, details.beacon_activity);
        assert_eq!(restored.beacon_activity_date, details.beacon_activity_date);
    }
}
