mod accumulators;
mod aprs;
mod config;
mod coverage;
mod elevation;
mod h3cache;
mod ignore_station;
mod layers;
mod reject_log;
mod rollup;
mod station;
mod stationfile;
mod db;
mod types;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{error, info, warn};

use aprs::parser::{self, extract_crc, extract_rotation, extract_signal_db, extract_vertical_speed};
use aprs::{AprsConnection, AprsPacket, PacketType};
use config::*;
use h3cache::H3Cache;
use layers::{
    get_write_layers, is_presence_only, layer_from_dest_callsign, layer_mask_from_set, Layer,
    PRESENCE_SIGNAL,
};
use station::StationManager;
use db::Storage;
use types::{Epoch, H3Index, StationId, StationName};

/// Global packet statistics
#[derive(Debug, Default)]
struct PacketStats {
    invalid_packet: AtomicU64,
    ignored_station: AtomicU64,
    ignored_paw: AtomicU64,
    ignored_tracker: AtomicU64,
    ignored_protocol: AtomicU64,
    invalid_tracker: AtomicU64,
    invalid_timestamp: AtomicU64,
    ignored_stationary: AtomicU64,
    ignored_signal0: AtomicU64,
    ignored_h3stationary: AtomicU64,
    ignored_elevation: AtomicU64,
    count: AtomicU64,
    raw_count: AtomicU64,
}

impl PacketStats {
    fn snapshot(&self) -> PacketStatsSnapshot {
        PacketStatsSnapshot {
            invalid_packet: self.invalid_packet.load(Ordering::Relaxed),
            ignored_station: self.ignored_station.load(Ordering::Relaxed),
            ignored_paw: self.ignored_paw.load(Ordering::Relaxed),
            ignored_tracker: self.ignored_tracker.load(Ordering::Relaxed),
            ignored_protocol: self.ignored_protocol.load(Ordering::Relaxed),
            invalid_tracker: self.invalid_tracker.load(Ordering::Relaxed),
            invalid_timestamp: self.invalid_timestamp.load(Ordering::Relaxed),
            ignored_stationary: self.ignored_stationary.load(Ordering::Relaxed),
            ignored_signal0: self.ignored_signal0.load(Ordering::Relaxed),
            ignored_h3stationary: self.ignored_h3stationary.load(Ordering::Relaxed),
            ignored_elevation: self.ignored_elevation.load(Ordering::Relaxed),
            count: self.count.load(Ordering::Relaxed),
            raw_count: self.raw_count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, serde::Serialize)]
struct PacketStatsSnapshot {
    invalid_packet: u64,
    ignored_station: u64,
    ignored_paw: u64,
    ignored_tracker: u64,
    ignored_protocol: u64,
    invalid_tracker: u64,
    invalid_timestamp: u64,
    ignored_stationary: u64,
    ignored_signal0: u64,
    ignored_h3stationary: u64,
    ignored_elevation: u64,
    count: u64,
    raw_count: u64,
}

impl std::fmt::Display for PacketStatsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        if self.ignored_station > 0 { parts.push(format!("station:{}", self.ignored_station)); }
        if self.ignored_protocol > 0 { parts.push(format!("protocol:{}", self.ignored_protocol)); }
        if self.ignored_stationary > 0 { parts.push(format!("stationary:{}", self.ignored_stationary)); }
        if self.ignored_h3stationary > 0 { parts.push(format!("h3stationary:{}", self.ignored_h3stationary)); }
        if self.ignored_signal0 > 0 { parts.push(format!("signal0:{}", self.ignored_signal0)); }
        if self.ignored_tracker > 0 { parts.push(format!("relayed:{}", self.ignored_tracker)); }
        if self.ignored_paw > 0 { parts.push(format!("paw:{}", self.ignored_paw)); }
        if self.ignored_elevation > 0 { parts.push(format!("elevation:{}", self.ignored_elevation)); }
        if self.invalid_packet > 0 { parts.push(format!("invalid:{}", self.invalid_packet)); }
        if self.invalid_tracker > 0 { parts.push(format!("bad_tracker:{}", self.invalid_tracker)); }
        if self.invalid_timestamp > 0 { parts.push(format!("bad_ts:{}", self.invalid_timestamp)); }
        if parts.is_empty() {
            write!(f, "no rejects")
        } else {
            write!(f, "rejected: {}", parts.join(", "))
        }
    }
}

/// Aircraft tracking for gap calculation and stationary detection
struct AircraftState {
    /// H3 cells at resolution 10 — kept in insertion order (oldest first) for FIFO eviction
    h3s: Vec<String>,
    first: u32,
    packets: u32,
    seen: u32,
}

struct AppState {
    station_manager: StationManager,
    h3_cache: H3Cache,
    storage: Storage,
    elevation: elevation::ElevationService,
    packet_stats: PacketStats,
    accumulators: RwLock<accumulators::Accumulators>,
    all_aircraft: Mutex<HashMap<String, AircraftState>>,
    aircraft_station: Mutex<HashMap<String, u32>>,
    case_insensitive: bool,
    /// Mutex to serialize cache flushes and rollups — rollup acquires this,
    /// does a full flush, then rolls up, ensuring no concurrent DB access.
    flush_lock: Mutex<()>,
}

impl AppState {
    fn normalise_case(&self, s: &str) -> String {
        if self.case_insensitive && s != "global" {
            s.to_uppercase()
        } else {
            s.to_string()
        }
    }
}

#[tokio::main]
async fn main() {
    // Load .env.local
    let _ = dotenvy::from_filename(".env.local");

    // Set up logging
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(false)
        .init();

    let gv = config::git_version();
    info!("ognrange-rs v{}", gv);

    if *ROLLUP_PERIOD_MINUTES < 12.0 {
        warn!("ROLLUP_PERIOD_MINUTES is too short, it must be more than 12 minutes");
    }

    info!(
        "Configuration: DB@{} Output@{}",
        *DB_PATH, *OUTPUT_PATH
    );

    // Create directories
    for dir in &[
        format!("{}stations", *DB_PATH),
        format!("{}stations", *OUTPUT_PATH),
    ] {
        if let Err(e) = std::fs::create_dir_all(dir) {
            error!("Error creating directory {}: {}", dir, e);
        }
    }

    // Detect case-insensitive filesystem
    let case_insensitive = (std::path::Path::new("CARGO.TOML").exists()
        && std::path::Path::new("Cargo.toml").exists())
        || (std::path::Path::new("PACKAGE.JSON").exists()
            && std::path::Path::new("package.json").exists());
    if case_insensitive {
        warn!("*** Case insensitive file system — data may be merged unexpectedly");
    }

    // Initialise core state
    let acc = accumulators::initialise_accumulators();
    info!(
        "Accumulators: {}/{}",
        acc.describe().0,
        acc.describe().1
    );

    let state = Arc::new(AppState {
        station_manager: StationManager::new(case_insensitive),
        h3_cache: H3Cache::new(),
        storage: Storage::new(),
        elevation: elevation::ElevationService::new(),
        packet_stats: PacketStats::default(),
        accumulators: RwLock::new(acc),
        all_aircraft: Mutex::new(HashMap::new()),
        aircraft_station: Mutex::new(HashMap::new()),
        case_insensitive,
        flush_lock: Mutex::new(()),
    });

    // Initialise reject log (logs if active)
    reject_log::init();

    // Probe elevation API before starting
    state.elevation.probe().await;

    // Startup rollup
    info!("Performing startup rollup...");
    rollup::rollup_startup(&state.storage, &state.station_manager).await;

    // Start APRS listener
    info!("Starting APRS...");
    let (event_tx, event_rx) = mpsc::channel(10_000);
    let _aprs_conn = AprsConnection::start(event_tx, gv.clone());

    // Spawn packet processor
    let state_clone = state.clone();
    let processor = tokio::spawn(packet_processor(state_clone, event_rx));

    // Spawn periodic tasks
    let state_clone = state.clone();
    let periodic = tokio::spawn(periodic_tasks(state_clone));

    // Spawn rollup timer
    let state_clone = state.clone();
    let rollup_timer_handle = tokio::spawn(rollup_timer(state_clone));

    // Wait for shutdown signal
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("SIGINT received, shutting down...");
        }
        _ = signal_term() => {
            info!("SIGTERM received, shutting down...");
        }
    }

    // Signal rollup iterations to stop, then abort background tasks
    rollup::request_shutdown();
    processor.abort();
    periodic.abort();
    rollup_timer_handle.abort();

    // Wait for any in-flight spawn_blocking DB writes to complete
    let _flush_guard = state.flush_lock.lock().await;

    // Graceful shutdown
    info!("Flushing data...");
    let acc = state.accumulators.read().await.clone();
    state
        .h3_cache
        .flush(&state.storage, &state.station_manager, &acc, true)
        .await;
    drop(_flush_guard);

    state.station_manager.close();
    info!("Shutdown complete");
}

/// Listen for SIGTERM (Unix only)
#[cfg(unix)]
async fn signal_term() {
    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to install SIGTERM handler")
        .recv()
        .await;
}

#[cfg(not(unix))]
async fn signal_term() {
    // On non-Unix, just wait forever (ctrl_c will trigger)
    std::future::pending::<()>().await;
}

/// Process incoming APRS events from the connection
async fn packet_processor(state: Arc<AppState>, mut event_rx: mpsc::Receiver<aprs::connection::AprsEvent>) {
    while let Some(event) = event_rx.recv().await {
        match event {
            aprs::connection::AprsEvent::Packet(raw) => {
                state
                    .packet_stats
                    .raw_count
                    .fetch_add(1, Ordering::Relaxed);

                if let Some(packet) = parser::parse_aprs(&raw) {
                    if !packet.source_callsign.is_empty() && packet.timestamp.is_some() {
                        if packet.latitude.is_some()
                            && packet.longitude.is_some()
                            && packet
                                .comment
                                .as_ref()
                                .map(|c| c.starts_with("id") || c.contains(" id"))
                                .unwrap_or(false)
                        {
                            // Aircraft position report
                            process_packet(&state, &packet, &raw).await;
                        } else {
                            // Station beacon or status
                            let station_name =
                                state.normalise_case(&packet.source_callsign);
                            let sn = StationName(station_name);

                            let is_station = packet.dest_callsign == "OGNSDR"
                                || raw.contains("qAC");

                            if is_station && !ignore_station::ignore_station(sn.as_str()) {
                                match packet.packet_type {
                                    PacketType::Location => {
                                        if let (Some(lat), Some(lng), Some(ts)) =
                                            (packet.latitude, packet.longitude, packet.timestamp)
                                        {
                                            state.station_manager.check_station_moved(
                                                &sn,
                                                lat,
                                                lng,
                                                Epoch(ts),
                                                &raw,
                                            );
                                        }
                                    }
                                    PacketType::Status => {
                                        if let (Some(body), Some(ts)) =
                                            (&packet.body, packet.timestamp)
                                        {
                                            state.station_manager.update_station_beacon(
                                                &sn,
                                                body,
                                                Epoch(ts),
                                            );
                                        }
                                    }
                                    _ => {
                                        state
                                            .packet_stats
                                            .invalid_packet
                                            .fetch_add(1, Ordering::Relaxed);
                                        reject_log::log_reject("invalid_packet_type", &raw);
                                    }
                                }
                            } else {
                                reject_log::log_reject("not_station_or_ignored", &raw);
                            }
                        }
                    } else {
                        reject_log::log_reject("no_callsign_or_timestamp", &raw);
                    }
                } else {
                    reject_log::log_reject("parse_failed", &raw);
                }
            }
            aprs::connection::AprsEvent::ServerMessage(msg) => {
                let raw_count = state.packet_stats.raw_count.load(Ordering::Relaxed);
                info!("{} # {}", msg, raw_count);
            }
            aprs::connection::AprsEvent::Disconnected(reason) => {
                warn!("APRS disconnected: {}", reason);
            }
        }
    }
}

/// Process a single aircraft position packet
async fn process_packet(state: &AppState, packet: &AprsPacket, raw: &str) {
    // Extract station from last digipeater
    let station_str = packet
        .digipeaters
        .last()
        .map(|d| d.callsign.as_str())
        .unwrap_or("unknown");
    let station_name = StationName(state.normalise_case(station_str));

    // Check ignore list
    if ignore_station::ignore_station(station_name.as_str()) {
        state
            .packet_stats
            .ignored_station
            .fetch_add(1, Ordering::Relaxed);
        reject_log::log_reject("ignored_station", raw);
        return;
    }

    // Determine protocol layer
    let layer = match layer_from_dest_callsign(&packet.dest_callsign) {
        Some(l) => l,
        None => {
            state
                .packet_stats
                .ignored_protocol
                .fetch_add(1, Ordering::Relaxed);
            reject_log::log_reject("ignored_protocol", raw);
            return;
        }
    };

    // Check if layer is enabled
    if let Some(ref enabled) = *ENABLED_LAYERS {
        if !enabled.contains(&layer) {
            state
                .packet_stats
                .ignored_protocol
                .fetch_add(1, Ordering::Relaxed);
            reject_log::log_reject("disabled_layer", raw);
            return;
        }
    }

    // Get or create station
    let mut station_details = state.station_manager.get_or_create(&station_name);

    // Extract flarm ID (last 6 chars of source callsign)
    let source = &packet.source_callsign;
    if source.len() < 6 {
        state
            .packet_stats
            .invalid_tracker
            .fetch_add(1, Ordering::Relaxed);
        station_details.stats.invalid_tracker += 1;
        state.station_manager.update(&station_details);
        reject_log::log_reject("invalid_tracker", raw);
        return;
    }
    let flarm_id = &source[source.len() - 6..];

    let timestamp = match packet.timestamp {
        Some(ts) => ts,
        None => {
            state
                .packet_stats
                .invalid_timestamp
                .fetch_add(1, Ordering::Relaxed);
            station_details.stats.invalid_timestamp += 1;
            state.station_manager.update(&station_details);
            reject_log::log_reject("invalid_timestamp", raw);
            return;
        }
    };

    // OGNTRK relay filter
    if layer == Layer::Ogntrk {
        if let Some(first_digi) = packet.digipeaters.first() {
            if !first_digi.callsign.starts_with("qA") {
                state
                    .packet_stats
                    .ignored_tracker
                    .fetch_add(1, Ordering::Relaxed);
                station_details.stats.ignored_tracker += 1;
                state.station_manager.update(&station_details);
                reject_log::log_reject("ogntrk_relay", raw);
                return;
            }
        }
    }

    let altitude = (packet.altitude.unwrap_or(0.0).floor().clamp(0.0, 55000.0)) as u16;
    let lat = packet.latitude.unwrap();
    let lng = packet.longitude.unwrap();
    let comment = packet.comment.as_deref().unwrap_or("");

    // Ensure aircraft entry exists before stationary check (matches TS: aircraft
    // is always created before stationary filter so seen time is always tracked)
    {
        let mut all_aircraft = state.all_aircraft.lock().await;
        all_aircraft.entry(flarm_id.to_string()).or_insert_with(|| AircraftState {
            h3s: Vec::new(),
            first: timestamp,
            packets: 0,
            seen: 0,
        });
    }

    // Check if moving
    let speed = packet.speed.unwrap_or(99.0);
    if speed < 1.0 {
        let raw_rot = extract_rotation(comment);
        let raw_vc = extract_vertical_speed(comment);
        if raw_rot == 0.0 && raw_vc < 30.0 {
            state
                .packet_stats
                .ignored_stationary
                .fetch_add(1, Ordering::Relaxed);
            station_details.stats.ignored_stationary += 1;
            // Update aircraft seen time (always succeeds — entry created above)
            let mut all_aircraft = state.all_aircraft.lock().await;
            if let Some(aircraft) = all_aircraft.get_mut(flarm_id) {
                aircraft.seen = timestamp;
            }
            state.station_manager.update(&station_details);
            return;
        }
    }

    // Gap calculation
    let gap: u8;
    let first: bool;
    {
        let gs = format!("{}/{}", station_name, flarm_id);
        let mut aircraft_station = state.aircraft_station.lock().await;
        let mut all_aircraft = state.all_aircraft.lock().await;

        // Entry guaranteed to exist from above
        let aircraft = all_aircraft.get_mut(flarm_id).unwrap();

        let seen = aircraft.seen;
        let when = aircraft_station.get(&gs).copied();
        // last_seen = when ?? seen ?? timestamp
        let last = when.unwrap_or(if seen > 0 { seen } else { timestamp });
        gap = (timestamp.abs_diff(last)).min(60).max(1) as u8;

        aircraft_station.insert(gs, timestamp);
        first = aircraft.seen < timestamp;
        if first {
            aircraft.seen = timestamp;
        }

        // H3 stationary detection
        if first {
            // Use h3o for cell calculation at resolution 10 (~65m edge)
            let h3_key_10 = if let Ok(coord10) = h3o::LatLng::from_radians(lat.to_radians(), lng.to_radians()) {
                format!("{:x}", coord10.to_cell(h3o::Resolution::Ten))
            } else {
                format!("{:.4},{:.4}", lat, lng) // fallback
            };

            // Insert only if not already present (Vec used for insertion-order FIFO)
            if !aircraft.h3s.contains(&h3_key_10) {
                aircraft.h3s.push(h3_key_10);
            }

            if aircraft.h3s.len() > 4 {
                // Remove oldest (first in Vec is always the earliest added)
                aircraft.h3s.remove(0);
                aircraft.packets = 0;
            } else {
                aircraft.packets += 1;
            }

            let s = aircraft.h3s.len() as u32;
            if s > 0 && aircraft.packets / s > 90 {
                state
                    .packet_stats
                    .ignored_h3stationary
                    .fetch_add(1, Ordering::Relaxed);
                station_details.stats.ignored_stationary += 1;
                state.station_manager.update(&station_details);
                return;
            }
        }
    }

    // Signal handling
    let is_presence_only = is_presence_only(layer);
    let signal: u8;
    let crc: u8;

    if is_presence_only {
        signal = PRESENCE_SIGNAL;
        crc = 0;
    } else {
        crc = extract_crc(comment);

        if let Some(raw_signal) = extract_signal_db(comment) {
            signal = ((raw_signal.max(0.0) * 4.0).round() as u16).min(63).max(1) as u8;
        } else {
            signal = 0;
        }

        if signal == 0 {
            state
                .packet_stats
                .ignored_signal0
                .fetch_add(1, Ordering::Relaxed);
            station_details.stats.ignored_signal0 += 1;
            state.station_manager.update(&station_details);
            reject_log::log_reject("signal_zero", raw);
            return;
        }
    }

    // Count valid packet
    state.packet_stats.count.fetch_add(1, Ordering::Relaxed);
    station_details.stats.count += 1;

    // Determine write layers (dual-write for FLARM/OGNTRK)
    let write_layers = get_write_layers(layer);
    station_details.layer_mask =
        Some(station_details.layer_mask.unwrap_or(0) | layer_mask_from_set(&write_layers));

    // Update last packet time
    station_details.last_packet = Some(Epoch(
        station_details
            .last_packet
            .map(|e| e.0.max(timestamp))
            .unwrap_or(timestamp),
    ));
    state.station_manager.update(&station_details);

    // Async elevation lookup and H3 update
    let elevation_service = &state.elevation;
    let h3_cache = &state.h3_cache;
    let station_id = station_details.id;

    let gl = elevation_service.get_elevation(lat, lng).await;
    let agl = (altitude as f64 - gl).clamp(0.0, 55000.0).round() as u16;

    // Get current accumulator bucket
    let current_bucket = state.accumulators.read().await.current.bucket;

    // Calculate H3 cells
    let station_cell_level = *H3_STATION_CELL_LEVEL;
    let global_cell_level = *H3_GLOBAL_CELL_LEVEL;

    // Use h3o for cell computation
    let coord = match h3o::LatLng::from_radians(lat.to_radians(), lng.to_radians()) {
        Ok(c) => c,
        Err(e) => {
            error!("Invalid coordinates {},{}: {}", lat, lng, e);
            state
                .packet_stats
                .ignored_elevation
                .fetch_add(1, Ordering::Relaxed);
            reject_log::log_reject("invalid_coordinates", raw);
            return;
        }
    };

    let station_resolution = h3o::Resolution::try_from(station_cell_level).unwrap_or(h3o::Resolution::Eight);
    let global_resolution = h3o::Resolution::try_from(global_cell_level).unwrap_or(h3o::Resolution::Seven);

    let station_cell = coord.to_cell(station_resolution);
    let global_cell = station_cell.parent(global_resolution).unwrap_or(station_cell);

    let h3_station = H3Index(format!("{:x}", station_cell));
    let h3_global = H3Index(format!("{:x}", global_cell));

    // Write to each target layer
    for write_layer in &write_layers {
        // Station database: (station_name, 0) — h3 at station cell level
        h3_cache
            .update(
                &h3_station,
                altitude,
                agl,
                crc,
                signal,
                gap,
                StationId(0),
                station_id,
                *write_layer,
                current_bucket,
            )
            .await;

        // Global database: (global, station_id) — h3 at global cell level
        h3_cache
            .update(
                &h3_global,
                altitude,
                agl,
                crc,
                signal,
                gap,
                station_id,
                StationId(0),
                *write_layer,
                current_bucket,
            )
            .await;
    }
}

/// Periodic maintenance tasks: cache flush, statistics, aircraft purge
async fn periodic_tasks(state: Arc<AppState>) {
    let flush_period = Duration::from_millis(*H3_CACHE_FLUSH_PERIOD_MS);
    let mut flush_interval = tokio::time::interval(flush_period);
    flush_interval.tick().await; // skip immediate first tick

    let mut last_count = 0u64;
    let mut last_raw_count = 0u64;
    let mut last_h3_total = 0usize;
    let flush_secs = *H3_CACHE_FLUSH_PERIOD_MS as f64 / 1000.0;

    // Aircraft purge: first one after FORGET_AIRCRAFT_AFTER_SECS, then every hour
    let forget_delay = Duration::from_secs(*FORGET_AIRCRAFT_AFTER_SECS + 60);
    let _purge_handle = {
        let state = state.clone();
        tokio::spawn(async move {
            tokio::time::sleep(forget_delay).await;
            let mut purge_interval = tokio::time::interval(Duration::from_secs(3600));
            loop {
                purge_interval.tick().await;
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as u32;
                let purge_before = now_secs - *FORGET_AIRCRAFT_AFTER_SECS as u32;

                let mut all_aircraft = state.all_aircraft.lock().await;
                let before = all_aircraft.len();
                all_aircraft.retain(|_, v| v.seen >= purge_before);
                let purged = before - all_aircraft.len();
                info!(
                    "Purged {} aircraft from gap map, {} remaining",
                    purged,
                    all_aircraft.len()
                );

                let mut aircraft_station = state.aircraft_station.lock().await;
                aircraft_station.retain(|_, &mut v| v >= purge_before);
            }
        })
    };

    loop {
        flush_interval.tick().await;

        let acc = state.accumulators.read().await.clone();
        let _flush_guard = state.flush_lock.lock().await;
        let flush_stats = state
            .h3_cache
            .flush(&state.storage, &state.station_manager, &acc, false)
            .await;
        drop(_flush_guard);

        let stats = state.packet_stats.snapshot();
        let packets = stats.count - last_count;
        let raw_packets = stats.raw_count - last_raw_count;
        let pps = packets as f64 / flush_secs;
        let raw_pps = raw_packets as f64 / flush_secs;
        let h3_total = flush_stats.total;
        let h3_delta = h3_total as i64 - last_h3_total as i64;
        let elevation_cache_size = state.elevation.cache_size_async().await;

        info!(
            "elevation cache: {}, total stations: {}",
            elevation_cache_size,
            state.station_manager.next_station_id() - 1
        );
        info!(
            "valid: {} ({:.1}/s), total: {} ({:.1}/s), {}",
            packets, pps, raw_packets, raw_pps, stats
        );
        info!(
            "h3s: {} delta {} ({:.0}%): expired {} ({:.0}%), written {} ({:.0}%)[{} stations] {:.1}% {:.1}/s {}:1",
            h3_total,
            h3_delta,
            if h3_total > 0 { h3_delta as f64 * 100.0 / h3_total as f64 } else { 0.0 },
            flush_stats.expired,
            if h3_total > 0 { flush_stats.expired as f64 * 100.0 / h3_total as f64 } else { 0.0 },
            flush_stats.written,
            if h3_total > 0 { flush_stats.written as f64 * 100.0 / h3_total as f64 } else { 0.0 },
            flush_stats.databases,
            if packets > 0 { flush_stats.written as f64 * 100.0 / packets as f64 } else { 0.0 },
            flush_stats.written as f64 / flush_secs,
            if flush_stats.written > 0 { packets / flush_stats.written as u64 } else { 0 }
        );

        last_count = stats.count;
        last_raw_count = stats.raw_count;
        last_h3_total = h3_total;
    }
}

/// Rollup timer: triggers accumulator rotation at period boundaries.
/// Acquires flush_lock to flush all cached H3 data, then rolls up.
async fn rollup_timer(state: Arc<AppState>) {
    loop {
        let delay = accumulators::next_rollup_delay();
        tokio::time::sleep(delay).await;

        let now = chrono::Utc::now();
        let new_acc = accumulators::what_accumulators(now);

        let old_acc = {
            let current = state.accumulators.read().await;
            if current.current.bucket == new_acc.current.bucket {
                continue; // bucket hasn't changed
            }
            current.clone()
        };

        // Update live accumulators so new packets use the new bucket
        {
            let mut acc = state.accumulators.write().await;
            *acc = new_acc.clone();
        }

        // Flush all cached H3 data under the lock so no periodic flush
        // can interleave. Uses old accumulators so data lands in the
        // current bucket before rollup moves it.
        let flush_guard = state.flush_lock.lock().await;
        state
            .h3_cache
            .flush(&state.storage, &state.station_manager, &old_acc, true)
            .await;

        // Rollup — all cached data is on disk. Hold flush_lock through
        // rollup so periodic flushes cannot open station DBs that rollup
        // already has open (which causes LockErrors).
        rollup::rollup_all(
            &state.storage,
            &state.station_manager,
            &old_acc,
            Some(&new_acc),
        )
        .await;
        drop(flush_guard);
    }
}
