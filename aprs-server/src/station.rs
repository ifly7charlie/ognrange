//! Station metadata management.
//!
//! Tracks station details (location, ID, stats) in memory with LevelDB persistence.
//! The LevelDB handle is NOT held in the struct (it's not Send/Sync).
//! Instead, all DB operations go through `spawn_blocking`.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::RwLock;
use rusty_leveldb::LdbIterator;
use tracing::{error, info, warn};

use crate::config::{DB_PATH, STATION_MOVE_THRESHOLD_KM};
use crate::types::{Epoch, StationId, StationName};

/// Message sent to the DB writer thread
enum DbWrite {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
    Shutdown,
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

        // Now spawn the writer thread which takes over the DB lock
        let writer_db_path = db_path;
        std::thread::spawn(move || {
            let mut opts = rusty_leveldb::Options::default();
            opts.create_if_missing = true;
            let mut db = match rusty_leveldb::DB::open(&writer_db_path, opts) {
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
        let mut opts = rusty_leveldb::Options::default();
        opts.create_if_missing = true;

        let mut db = rusty_leveldb::DB::open(&self.db_path, opts)?;
        let mut iter = db.new_iter()?;

        let mut max_id: u16 = 0;
        let mut key_buf = Vec::new();
        let mut val_buf = Vec::new();
        let mut has_global = false;

        while iter.advance() {
            if iter.current(&mut key_buf, &mut val_buf) {
                let raw_name = String::from_utf8_lossy(&key_buf).to_string();
                if raw_name == "global" {
                    has_global = true;
                    continue;
                }
                let name = if self.case_insensitive {
                    raw_name.to_uppercase()
                } else {
                    raw_name
                };
                match serde_json::from_slice::<StationDetails>(&val_buf) {
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
        }

        drop(iter);

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
