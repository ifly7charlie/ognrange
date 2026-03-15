//! Station metadata management.
//!
//! Tracks station details (location, ID, stats) in memory with LevelDB persistence.
//! The LevelDB handle is NOT held in the struct (it's not Send/Sync).
//! Instead, all DB operations go through `spawn_blocking`.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::RwLock;
use tracing::{error, info, warn};

use crate::config::{DB_PATH, STATION_MOVE_THRESHOLD_KM};
use crate::db::TrackedDb;
use crate::types::{Epoch, StationId, StationName};

/// Message sent to the DB writer thread
enum DbWrite {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
    Shutdown,
}

/// Encode a 144-bit bitvector as 36 hex chars (little-endian byte order within each u64).
/// Words 0-1: 8 bytes each, word 2: 2 bytes (only bits 128-143 used).
fn bitvec_to_hex(bits: &[u64; 3]) -> String {
    let mut hex = String::with_capacity(36);
    for i in 0..3 {
        let byte_count = if i < 2 { 8 } else { 2 };
        for b in 0..byte_count {
            let byte = ((bits[i] >> (b * 8)) & 0xFF) as u8;
            write!(hex, "{:02x}", byte).unwrap();
        }
    }
    hex
}

/// Count set bits in a 144-bit bitvector (only the low 144 bits).
fn popcount_144(bits: &[u64; 3]) -> u32 {
    bits[0].count_ones() + bits[1].count_ones() + (bits[2] & 0xFFFF).count_ones()
}

/// Compute station uptime as a percentage of active slots relative to elapsed slots today.
/// Returns `None` if the activity data is missing or stale.
pub fn compute_uptime(beacon_activity: &Option<String>, beacon_activity_date: &Option<String>, today: &str, current_slot: u32) -> Option<f32> {
    let hex = beacon_activity.as_deref()?;
    let date = beacon_activity_date.as_deref()?;
    if date != today || current_slot == 0 {
        return None;
    }
    let bits = hex_to_bitvec(hex)?;
    let set = popcount_144(&bits);
    let elapsed = current_slot.min(144);
    Some(((set as f32 / elapsed as f32) * 1000.0).round() / 10.0)
}

/// Decode 36 hex chars back to a 144-bit bitvector.
fn hex_to_bitvec(hex: &str) -> Option<[u64; 3]> {
    if hex.len() != 36 {
        return None;
    }
    let bytes: Vec<u8> = (0..18)
        .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16))
        .collect::<Result<_, _>>()
        .ok()?;
    let mut bits = [0u64; 3];
    for j in 0..8 {
        bits[0] |= (bytes[j] as u64) << (j * 8);
    }
    for j in 0..8 {
        bits[1] |= (bytes[8 + j] as u64) << (j * 8);
    }
    for j in 0..2 {
        bits[2] |= (bytes[16 + j] as u64) << (j * 8);
    }
    Some(bits)
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct StationStats {
    pub ignored_tracker: u32,
    pub invalid_tracker: u32,
    pub invalid_timestamp: u32,
    pub ignored_stationary: u32,
    pub ignored_signal0: u32,
    #[serde(rename = "ignoredPAW")]
    pub ignored_paw: u32,
    pub ignored_h3stationary: u32,
    pub ignored_elevation: u32,
    pub count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StationDetails {
    pub id: StationId,
    pub station: StationName,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lat: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lng: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "primary_location")]
    pub primary_location: Option<[f64; 2]>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "previous_location")]
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
    pub stats: StationStats,
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
            std::process::exit(1);
        }

        // Spawn the writer thread which takes over the DB lock
        let writer_db_path = db_path;
        std::thread::spawn(move || {
            let mut db = match TrackedDb::open(&writer_db_path, true, "station_writer") {
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
        let mut db = TrackedDb::open(&self.db_path, true, "station_load")?;
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

    /// Get or create station details
    pub fn get_or_create(&self, name: &StationName) -> StationDetails {
        // Fast path: read lock
        {
            let stations = self.stations.read().unwrap();
            if let Some(details) = stations.get(name) {
                return details.clone();
            }
        }

        // Slow path: write lock, re-check to avoid duplicate ID allocation
        let mut stations = self.stations.write().unwrap();
        if let Some(details) = stations.get(name) {
            return details.clone();
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
            valid: false,
            layer_mask: None,
            output_epoch: None,
            output_date: None,
            last_output_file: None,
            stats: StationStats::default(),
            beacon_activity: None,
            beacon_activity_date: None,
            uptime: None,
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

        details
    }

    /// Get station details without creating
    pub fn get(&self, name: &StationName) -> Option<StationDetails> {
        self.stations.read().unwrap().get(name).cloned()
    }

    /// Update station details in memory and persist
    pub fn update(&self, details: &StationDetails) {
        self.stations
            .write()
            .unwrap()
            .insert(details.station.clone(), details.clone());
        self.persist(details);
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

    /// Check if a station has moved and update its location
    pub fn check_station_moved(
        &self,
        name: &StationName,
        lat: f64,
        lng: f64,
        timestamp: Epoch,
        raw_packet: &str,
    ) {
        let mut details = self.get_or_create(name);

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
        let distance = great_circle_distance(primary[0], primary[1], lat, lng);
        let threshold = *STATION_MOVE_THRESHOLD_KM;

        if distance > threshold {
            let previous = details.previous_location.unwrap_or(primary);
            let prev_distance = great_circle_distance(previous[0], previous[1], lat, lng);

            if prev_distance > threshold {
                details.notice = Some(format!(
                    "{:.0}km move detected {} resetting history",
                    distance,
                    chrono::DateTime::from_timestamp(timestamp.0 as i64, 0)
                        .map(|d| d.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                        .unwrap_or_default()
                ));
                warn!("{}", raw_packet);
                warn!(
                    "Station {} moved to {},{} ({:.1}km)",
                    name, lat, lng, distance
                );
                details.moved = true;
                details.previous_location = details.primary_location;
                details.bouncing = false;
                details.primary_location = Some([lat, lng]);
            } else if prev_distance > 0.1 {
                details.notice = Some("station appears to be in motion, resetting history".into());
                info!("{} {}", name, details.notice.as_ref().unwrap());
                details.moved = true;
                details.bouncing = false;
                details.primary_location = Some([lat, lng]);
            } else {
                info!("{} bouncing between two locations (merging)", name);
                details.notice =
                    Some("station appears to be bouncing between two locations, merging data".into());
                details.moved = false;
                details.bouncing = true;
                details.primary_location = Some([lat, lng]);
            }
        } else {
            if distance > 0.1 {
                details.notice = Some(format!(
                    "small {:.1}km move detected, keeping history",
                    distance
                ));
            } else {
                details.notice = Some(String::new());
            }
            details.bouncing = false;
        }

        details.lat = Some(lat);
        details.lng = Some(lng);
        details.last_location = Some(timestamp);
        self.update(&details);
    }

    /// Update station beacon status
    pub fn update_station_beacon(&self, name: &StationName, body: &str, timestamp: Epoch) {
        let mut details = self.get_or_create(name);
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

        let secs_in_day = (timestamp % 86400) as usize;
        let hour = secs_in_day / 3600;
        let minute = (secs_in_day % 3600) / 60;
        let slot = hour * 6 + minute / 10; // 0–143

        let mut details = self.get_or_create(name);

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
            valid: false,
            layer_mask: None,
            output_epoch: None,
            output_date: None,
            last_output_file: None,
            stats: StationStats::default(),
            beacon_activity: None,
            beacon_activity_date: None,
            uptime: None,
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

    #[test]
    fn test_bitvec_roundtrip_zeros() {
        let bits = [0u64; 3];
        let hex = bitvec_to_hex(&bits);
        assert_eq!(hex, "000000000000000000000000000000000000");
        assert_eq!(hex.len(), 36);
        assert_eq!(hex_to_bitvec(&hex), Some(bits));
    }

    #[test]
    fn test_bitvec_roundtrip_values() {
        let bits = [0xFF, 0x00, 0x00];
        let hex = bitvec_to_hex(&bits);
        assert_eq!(&hex[..16], "ff00000000000000"); // word 0 LE
        let decoded = hex_to_bitvec(&hex).unwrap();
        assert_eq!(decoded, bits);
    }

    #[test]
    fn test_bitvec_roundtrip_all_words() {
        let bits = [0x0123456789ABCDEFu64, 0xFEDCBA9876543210, 0xBEEF];
        let hex = bitvec_to_hex(&bits);
        let decoded = hex_to_bitvec(&hex).unwrap();
        assert_eq!(decoded, bits);
    }

    #[test]
    fn test_bitvec_slot_boundaries() {
        // Slot 0 → word 0, bit 0
        let mut bits = [0u64; 3];
        bits[0] |= 1u64 << 0;
        let hex = bitvec_to_hex(&bits);
        let decoded = hex_to_bitvec(&hex).unwrap();
        assert_ne!(decoded[0] & 1, 0);

        // Slot 63 → word 0, bit 63
        let mut bits = [0u64; 3];
        bits[0] |= 1u64 << 63;
        let hex = bitvec_to_hex(&bits);
        let decoded = hex_to_bitvec(&hex).unwrap();
        assert_ne!(decoded[0] & (1u64 << 63), 0);

        // Slot 64 → word 1, bit 0
        let mut bits = [0u64; 3];
        bits[1] |= 1u64 << 0;
        let hex = bitvec_to_hex(&bits);
        let decoded = hex_to_bitvec(&hex).unwrap();
        assert_ne!(decoded[1] & 1, 0);

        // Slot 143 → word 2, bit 15
        let mut bits = [0u64; 3];
        bits[2] |= 1u64 << 15;
        let hex = bitvec_to_hex(&bits);
        let decoded = hex_to_bitvec(&hex).unwrap();
        assert_ne!(decoded[2] & (1u64 << 15), 0);
    }

    #[test]
    fn test_hex_to_bitvec_bad_input() {
        assert_eq!(hex_to_bitvec(""), None);
        assert_eq!(hex_to_bitvec("too_short"), None);
        assert_eq!(hex_to_bitvec("zz0000000000000000000000000000000000"), None);
    }

    #[test]
    fn test_slot_calculation() {
        // 00:00 UTC → slot 0
        let ts = 1710460800u32; // 2024-03-15 00:00:00 UTC
        let secs_in_day = (ts % 86400) as usize;
        assert_eq!(secs_in_day / 3600 * 6 + (secs_in_day % 3600) / 60 / 10, 0);

        // 12:30 UTC → slot 75 (12*6 + 3)
        let ts2 = ts + 12 * 3600 + 30 * 60;
        let secs2 = (ts2 % 86400) as usize;
        assert_eq!(secs2 / 3600 * 6 + (secs2 % 3600) / 60 / 10, 75);

        // 23:50 UTC → slot 143 (23*6 + 5)
        let ts3 = ts + 23 * 3600 + 50 * 60;
        let secs3 = (ts3 % 86400) as usize;
        assert_eq!(secs3 / 3600 * 6 + (secs3 % 3600) / 60 / 10, 143);
    }

    #[test]
    fn test_activity_serde_roundtrip() {
        let mut details = StationDetails {
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
            valid: false,
            layer_mask: None,
            output_epoch: None,
            output_date: None,
            last_output_file: None,
            stats: StationStats::default(),
            beacon_activity: None,
            beacon_activity_date: None,
            uptime: None,
        };

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
