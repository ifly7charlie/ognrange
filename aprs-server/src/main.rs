mod accumulators;
mod aprs;
mod bitvec;
mod config;
mod coverage;
mod elevation;
mod global_uptime;
mod ground_horizon;
mod h3cache;
mod horizon;
mod json_io;
mod ignore_station;
mod ntfy;
mod layers;
mod protocol_stats;
mod reject_log;
mod rollup;
mod packet_stats;
mod stats_accumulator;
mod station;
mod station_stats;
mod stationfile;
mod symlinks;
mod db;
mod types;
#[cfg(unix)]
mod syslog;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{debug, error, info, warn};

use aprs::parser::{self, extract_crc, extract_rotation, extract_signal_db, extract_vertical_speed, quantise_signal_db};
use aprs::{AprsConnection, AprsPacket, PacketType};
use config::*;
use h3cache::H3Cache;
use layers::{
    get_write_layers, is_presence_only, layer_from_dest_callsign, layer_mask_from_set, Layer,
    PRESENCE_SIGNAL,
};
use station::{StationDetails, StationManager};
use db::Storage;
use types::{Epoch, H3Index, StationId, StationName};

/// Event queue between the APRS connection and the packet processor.
/// ~75s of headroom at peak rates; the connection sheds packets (with a
/// warning) rather than blocking when this fills - see connection.rs
const EVENT_CHANNEL_CAPACITY: usize = 50_000;

/// Aircraft tracking for gap calculation and stationary detection
struct AircraftState {
    /// H3 cells at resolution 10 - kept in insertion order (oldest first) for FIFO eviction
    h3s: Vec<String>,
    packets: u32,
    seen: u32,
}

struct AppState {
    station_manager: StationManager,
    h3_cache: H3Cache,
    storage: Storage,
    elevation: elevation::ElevationService,
    protocol_stats: protocol_stats::ProtocolStats,
    global_stats: station_stats::StationGlobalStats,
    global_uptime: global_uptime::GlobalUptime,
    accumulators: RwLock<accumulators::Accumulators>,
    all_aircraft: Mutex<HashMap<(Layer, u32), AircraftState>>,
    aircraft_station: Mutex<HashMap<(Layer, u16, u32), u32>>,
    case_insensitive: bool,
    /// Mutex to serialize cache flushes and rollups - rollup acquires this,
    /// does a full flush, then rolls up, ensuring no concurrent DB access.
    flush_lock: Mutex<()>,
    /// Backlog monitoring, written by the packet processor: peak event-queue
    /// depth and max packet age at dequeue since the last periodic log tick
    /// (which resets both), plus the age of the most recent packet.
    backlog_peak: AtomicU64,
    lag_max_ms: AtomicU64,
    lag_last_ms: AtomicU64,
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
    init_logging();

    let gv = config::git_version();
    info!("ognrange-rs v{}", gv);

    if *ROLLUP_PERIOD_MINUTES < 12.0 {
        warn!("ROLLUP_PERIOD_MINUTES is too short, it must be more than 12 minutes");
    }

    if std::env::var("MAX_GROUND_HORIZONS_PER_ROLLUP").is_ok() {
        warn!(
            "MAX_GROUND_HORIZONS_PER_ROLLUP is no longer used - ground horizons run as a \
             serial background queue; set GROUND_HORIZON_PAUSED=1 to disable it"
        );
    }

    info!(
        "Configuration: DB@{} Output@{}",
        *DB_PATH, *OUTPUT_PATH
    );

    // Create directories
    for dir in &[
        format!("{}stations", *DB_PATH),
        format!("{}stations", *OUTPUT_PATH),
        format!("{}stats", *OUTPUT_PATH),
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
        warn!("*** Case insensitive file system - data may be merged unexpectedly");
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
        protocol_stats: protocol_stats::ProtocolStats::load(),
        global_stats: station_stats::StationGlobalStats::load(),
        global_uptime: global_uptime::GlobalUptime::new(),
        accumulators: RwLock::new(acc),
        all_aircraft: Mutex::new(HashMap::new()),
        aircraft_station: Mutex::new(HashMap::new()),
        case_insensitive,
        flush_lock: Mutex::new(()),
        backlog_peak: AtomicU64::new(0),
        lag_max_ms: AtomicU64::new(0),
        lag_last_ms: AtomicU64::new(0),
    });

    // Initialise reject log (logs if active)
    reject_log::init();

    // Probe the DEM tile endpoint before starting (log-only health check)
    state.elevation.probe().await;

    // Startup rollup - migrate legacy keys and process hanging current accumulators
    info!("Performing startup rollup...");
    {
        let acc = state.accumulators.read().await;
        rollup::rollup_startup(&state.storage, &state.station_manager, &acc).await;
    }

    // Start APRS listener
    info!("Starting APRS...");
    let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
    let _aprs_conn = AprsConnection::start(event_tx, gv.clone());

    // Spawn packet processor
    let state_clone = state.clone();
    let mut processor = tokio::spawn(packet_processor(state_clone, event_rx));

    // Spawn periodic tasks
    let state_clone = state.clone();
    let mut periodic = tokio::spawn(periodic_tasks(state_clone));

    // Spawn rollup timer
    let state_clone = state.clone();
    let mut rollup_timer_handle = tokio::spawn(rollup_timer(state_clone));

    // Spawn status writer (server stats + per-station JSON, independent of rollup)
    let state_clone = state.clone();
    let mut status_writer_handle = tokio::spawn(status_writer(state_clone));

    // Spawn ground-horizon queue (terrain file backfill between rollups)
    let state_clone = state.clone();
    let mut ground_horizon_handle = tokio::spawn(ground_horizon_task(state_clone));

    // Spawn outage monitor (per-station ntfy topics + outage notifications)
    let state_clone = state.clone();
    let mut outage_monitor_handle = tokio::spawn(outage_monitor(state_clone));

    // Wait for shutdown signal. The background tasks never return in normal
    // operation - one ending (a panic) would otherwise leave a half-dead daemon
    // that keeps the APRS connection alive but processes nothing, so treat it
    // as fatal: flush, then exit non-zero so systemd restarts us.
    let mut fatal: Option<&str> = None;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("SIGINT received, shutting down...");
        }
        _ = signal_term() => {
            info!("SIGTERM received, shutting down...");
        }
        res = &mut processor => {
            error!("Packet processor exited unexpectedly: {:?}", res);
            fatal = Some("packet processor died");
        }
        res = &mut periodic => {
            error!("Periodic tasks exited unexpectedly: {:?}", res);
            fatal = Some("periodic tasks died");
        }
        res = &mut rollup_timer_handle => {
            error!("Rollup timer exited unexpectedly: {:?}", res);
            fatal = Some("rollup timer died");
        }
        res = &mut status_writer_handle => {
            error!("Status writer exited unexpectedly: {:?}", res);
            fatal = Some("status writer died");
        }
        res = &mut ground_horizon_handle => {
            error!("Ground horizon task exited unexpectedly: {:?}", res);
            fatal = Some("ground horizon task died");
        }
        res = &mut outage_monitor_handle => {
            error!("Outage monitor exited unexpectedly: {:?}", res);
            fatal = Some("outage monitor died");
        }
    }

    // Signal rollup iterations to stop, then abort background tasks
    rollup::request_shutdown();
    processor.abort();
    periodic.abort();
    rollup_timer_handle.abort();
    status_writer_handle.abort();
    ground_horizon_handle.abort();
    outage_monitor_handle.abort();

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

    state.protocol_stats.save_state();
    state.global_stats.save_state();
    state.global_uptime.clear_current_slot();
    state.station_manager.close();
    if let Some(reason) = fatal {
        error!("Exiting after fatal error: {}", reason);
        std::process::exit(1);
    }
    info!("Shutdown complete");
}

/// Initialise the tracing subscriber with optional stdout and syslog layers.
#[cfg(unix)]
fn init_logging() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let stdout_layer = if *LOG_STDOUT {
        Some(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_thread_ids(false),
        )
    } else {
        None
    };

    let syslog_layer = if *LOG_SYSLOG {
        Some(syslog::SyslogLayer::new("ognrange"))
    } else {
        None
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(stdout_layer)
        .with(syslog_layer)
        .init();
}

#[cfg(not(unix))]
fn init_logging() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let stdout_layer = if *LOG_STDOUT {
        Some(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_thread_ids(false),
        )
    } else {
        None
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(stdout_layer)
        .init();
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
            aprs::connection::AprsEvent::Packet { raw, received } => {
                // Backlog telemetry: queue depth behind this packet and how
                // long it sat in the channel. Peaks are reset by the
                // periodic stats log.
                let lag_ms = received.elapsed().as_millis() as u64;
                state.lag_last_ms.store(lag_ms, Ordering::Relaxed);
                state.lag_max_ms.fetch_max(lag_ms, Ordering::Relaxed);
                state
                    .backlog_peak
                    .fetch_max(event_rx.len() as u64, Ordering::Relaxed);

                state
                    .global_stats
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
                            // Extract and validate flarm ID (last 6 hex chars of source callsign)
                            let source = &packet.source_callsign;
                            if source.len() < 6 {
                                state.global_stats.with_data(|d| d.record_invalid_tracker());
                                reject_log::log_reject("invalid_tracker", &raw);
                                continue;
                            }
                            let flarm_hex = &source[source.len() - 6..];
                            let flarm_num = match u32::from_str_radix(flarm_hex, 16) {
                                Ok(n) => n,
                                Err(_) => {
                                    state.global_stats.with_data(|d| d.record_invalid_tracker());
                                    reject_log::log_reject("invalid_flarm_hex", &raw);
                                    continue;
                                }
                            };

                            // FLARM and OGN trackers can be set to broadcast a random
                            // address that changes over time (privacy mode), flagged as
                            // address type 0 in the id field. Excluded from unique-device
                            // stats so the churn doesn't inflate aircraft counts. Scoped
                            // to these tocalls because other networks (SafeSky, WeGlide,
                            // FlyingNeurons...) use address type 0 with stable IDs.
                            let random_id = matches!(
                                packet.dest_callsign.as_str(),
                                "OGFLR" | "OGFLR6" | "OGFLR7" | "APRS" | "OGNTRK"
                            ) && packet
                                .comment
                                .as_deref()
                                .and_then(parser::extract_address_type)
                                == Some(0);

                            // Record protocol stats before filtering
                            state.protocol_stats.record_raw(
                                &packet.dest_callsign,
                                flarm_num,
                                random_id,
                                packet.latitude.unwrap(),
                                packet.longitude.unwrap(),
                            );
                            // Aircraft position report
                            process_packet(&state, &packet, &raw, flarm_num).await;
                        } else {
                            // Station beacon or status
                            let station_name =
                                state.normalise_case(&packet.source_callsign);
                            let sn = StationName(station_name);

                            let is_station = packet.dest_callsign == "OGNSDR"
                                || raw.contains("qAC");

                            if is_station && !ignore_station::ignore_station(sn.as_str()) {
                                if state.station_manager.get(&sn).is_none() {
                                    reject_log::log_reject("beacon_no_traffic", &raw);
                                } else {
                                if let Some(ts) = packet.timestamp {
                                    state
                                        .station_manager
                                        .record_beacon(&sn, ts);
                                }
                                match packet.packet_type {
                                    PacketType::Location => {
                                        if let (Some(lat), Some(lng), Some(ts)) =
                                            (packet.latitude, packet.longitude, packet.timestamp)
                                        {
                                            state.station_manager.check_station_moved(
                                                &sn,
                                                lat,
                                                lng,
                                                packet.altitude,
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
                                            .global_stats
                                            .invalid_packet
                                            .fetch_add(1, Ordering::Relaxed);
                                        reject_log::log_reject("invalid_packet_type", &raw);
                                    }
                                }
                                } // station exists
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
                let raw_count = state.global_stats.raw_count.load(Ordering::Relaxed);
                let queued = event_rx.len();
                if queued > 0 {
                    let lag = state.lag_last_ms.load(Ordering::Relaxed) as f64 / 1000.0;
                    info!("{} # {} backlog {} lag {:.1}s", msg, raw_count, queued, lag);
                } else {
                    info!("{} # {}", msg, raw_count);
                }
                state.global_uptime.record_keepalive(&msg);
            }
            aprs::connection::AprsEvent::Disconnected(reason) => {
                warn!("APRS disconnected: {}", reason);
            }
        }
    }
}

/// Process a single aircraft position packet
async fn process_packet(state: &AppState, packet: &AprsPacket, raw: &str, flarm_num: u32) {
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
            .global_stats
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
                .global_stats
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
                .global_stats
                .ignored_protocol
                .fetch_add(1, Ordering::Relaxed);
            reject_log::log_reject("disabled_layer", raw);
            return;
        }
    }

    // Allocate (or retrieve) the station entry and count this packet once
    let mut station_details = match state.station_manager.get_or_create(&station_name) {
        Some(d) => d,
        None => return,
    };
    station_details.stats.record_raw();
    state.global_stats.with_data(|d| d.record_raw());

    let timestamp = match packet.timestamp {
        Some(ts) => ts,
        None => {
            station_details.stats.record_invalid_timestamp();
            state.global_stats.with_data(|d| d.record_invalid_timestamp());
            state.station_manager.update(&station_details);
            reject_log::log_reject("invalid_timestamp", raw);
            return;
        }
    };

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;

    // Count packets with timestamps far in the future - logged for diagnostics,
    // but still processed (hourly stats use server receive time so no chart pollution).
    if timestamp > now_secs + *FUTURE_PACKET_CUTOFF_SECS {
        station_details.stats.record_ignored_future_timestamp();
        state.global_stats.with_data(|d| d.record_ignored_future_timestamp());
        reject_log::log_reject("future_timestamp", raw);
    }

    // Count packets with timestamps older than STALE_PACKET_CUTOFF_SECS - logged for
    // diagnostics, but still processed (hourly stats use server receive time so no chart pollution).
    if timestamp < now_secs.saturating_sub(*STALE_PACKET_CUTOFF_SECS) {
        station_details.stats.record_ignored_stale_timestamp();
        state.global_stats.with_data(|d| d.record_ignored_stale_timestamp());
        reject_log::log_reject("stale_timestamp", raw);
    }

    // OGNTRK relay filter
    if layer == Layer::Ogntrk {
        if let Some(first_digi) = packet.digipeaters.first() {
            if !first_digi.callsign.starts_with("qA") {
                station_details.stats.record_ignored_tracker();
                state.global_stats.with_data(|d| d.record_ignored_tracker());
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

    // Filter positions implausibly far from the receiver - a corrupted packet
    // (e.g. a mangled longitude) otherwise plants a stray cell in the coverage
    // map and, via min-angle selection, wrecks the horizon chart. Checked before
    // the elevation lookups so garbage positions don't pull in far-away DEM
    // tiles. Skipped until the station's own position is known.
    if let (Some(slat), Some(slng)) = (station_details.lat, station_details.lng) {
        if (slat != 0.0 || slng != 0.0)
            && station::great_circle_distance(slat, slng, lat, lng) > *MAX_PACKET_DISTANCE_KM
        {
            station_details.stats.record_ignored_distance();
            state.global_stats.with_data(|d| d.record_ignored_distance());
            state.station_manager.update(&station_details);
            reject_log::log_reject("distance_too_far", raw);
            return;
        }
    }

    // Ensure aircraft entry exists before stationary check (matches TS: aircraft
    // is always created before stationary filter so seen time is always tracked)
    {
        let mut all_aircraft = state.all_aircraft.lock().await;
        all_aircraft.entry((layer, flarm_num)).or_insert_with(|| AircraftState {
            h3s: Vec::new(),
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
            station_details.stats.record_ignored_stationary();
            state.global_stats.with_data(|d| d.record_ignored_stationary());
            state.station_manager.update(&station_details);
            // Update aircraft seen time (always succeeds - entry created above)
            let mut all_aircraft = state.all_aircraft.lock().await;
            if let Some(aircraft) = all_aircraft.get_mut(&(layer, flarm_num)) {
                aircraft.seen = timestamp;
            }
            return;
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
            signal = quantise_signal_db(raw_signal);
        } else {
            signal = 0;
        }

        if signal == 0 {
            station_details.stats.record_ignored_signal0();
            state.global_stats.with_data(|d| d.record_ignored_signal0());
            state.station_manager.update(&station_details);
            reject_log::log_reject("signal_zero", raw);
            return;
        }
    }

    // Gap calculation
    let gap: u8;
    let first: bool;
    {
        let gs_key = (layer, station_details.id.0, flarm_num);
        let mut aircraft_station = state.aircraft_station.lock().await;
        let mut all_aircraft = state.all_aircraft.lock().await;

        // Entry guaranteed to exist from above
        let aircraft = all_aircraft.get_mut(&(layer, flarm_num)).unwrap();

        let seen = aircraft.seen;
        let when = aircraft_station.get(&gs_key).copied();
        // last_seen = when ?? seen ?? timestamp
        let last = when.unwrap_or(if seen > 0 { seen } else { timestamp });
        gap = (timestamp.abs_diff(last)).min(60).max(1) as u8;

        aircraft_station.insert(gs_key, timestamp);
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
                station_details.stats.record_ignored_h3stationary();
                state.global_stats.with_data(|d| d.record_ignored_h3stationary());
                state.station_manager.update(&station_details);
                return;
            }
        }
    }

    // Determine write layers (dual-write for FLARM/OGNTRK)
    let write_layers = get_write_layers(layer);
    let hour = ((now_secs / 3600) % 24) as usize;

    let new_mask = station_details.layer_mask.unwrap_or(0) | layer_mask_from_set(&write_layers);
    station_details.layer_mask = Some(new_mask);
    station_details.layers = layers::layer_names_from_mask(new_mask);

    // Update last packet time — use receive time (wall clock) not packet timestamp,
    // because rollup's "no traffic" skip compares this against accumulator boundaries
    // which are wall-clock-based. Using the APRS timestamp (which can lag) would cause
    // stations to be falsely skipped when packets arrive just after a boundary with
    // timestamps from just before it.
    station_details.last_packet = Some(Epoch(
        station_details
            .last_packet
            .map(|e| e.0.max(now_secs))
            .unwrap_or(now_secs),
    ));
    // Also track the APRS packet timestamp for display/export purposes
    station_details.last_packet_packet_time = Some(Epoch(
        station_details
            .last_packet_packet_time
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

    // Coarse AGL using max ground elevation in ~10km cell
    let coarse_gl = elevation_service.get_max_elevation_coarse(lat, lng).await;
    let coarse_agl = (altitude as f64 - coarse_gl).clamp(0.0, 55000.0).round() as u16;

    // Filter bogus altitude data
    if (layer == Layer::Adsb && coarse_agl > 4500) || coarse_agl > 10000 {
        station_details.stats.record_ignored_elevation();
        state.global_stats.with_data(|d| d.record_ignored_elevation());
        state.station_manager.update(&station_details);
        if layer != Layer::Adsb {
            reject_log::log_reject("altitude_too_high", raw);
        }
        return;
    }

    // Packet passed all filters - count as accepted
    let delay = now_secs.saturating_sub(timestamp) as u64;
    station_details.stats.record_delay(delay);
    for wl in &write_layers {
        station_details.stats.record_accepted(wl.name(), hour);
    }
    state.global_stats.with_data(|d| {
        d.record_delay(delay);
        for wl in &write_layers {
            d.record_accepted(wl.name(), hour);
        }
    });
    state.station_manager.update(&station_details);

    state.protocol_stats.record_accepted(&packet.dest_callsign, coarse_agl);
    for wl in &write_layers {
        state.protocol_stats.record_hourly(wl.name(), hour as u32);
    }

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
            state.global_stats.with_data(|d| d.record_ignored_elevation());
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
        // Station database: (station_name, 0) - h3 at station cell level
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

        // Global database: (global, station_id) - h3 at global cell level
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
    let mut last_tile = elevation::TileCacheStats::default();
    let mut last_dropped = 0u64;
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

                // Take these locks one at a time: process_packet's gap calculation
                // acquires aircraft_station then all_aircraft, so holding all_aircraft
                // while waiting for aircraft_station here is a lock-order inversion
                // that deadlocks the packet processor (observed 2026-07-25).
                {
                    let mut all_aircraft = state.all_aircraft.lock().await;
                    let before = all_aircraft.len();
                    all_aircraft.retain(|_, v| v.seen >= purge_before);
                    let purged = before - all_aircraft.len();
                    info!(
                        "Purged {} aircraft from gap map, {} remaining",
                        purged,
                        all_aircraft.len()
                    );
                }

                {
                    let mut aircraft_station = state.aircraft_station.lock().await;
                    aircraft_station.retain(|_, &mut v| v >= purge_before);
                }
            }
        })
    };

    // DEM disk-tile prune: immediately at startup (cleans up after previous
    // runs), then daily. Drops tiles whose mtime hasn't been refreshed in 10 days.
    let _dem_prune_handle = {
        let state = state.clone();
        tokio::spawn(async move {
            let mut prune_interval = tokio::time::interval(Duration::from_secs(24 * 3600));
            loop {
                prune_interval.tick().await;
                let state = state.clone();
                let _ = tokio::task::spawn_blocking(move || state.elevation.prune_disk_cache()).await;
            }
        })
    };

    let mut last_station_flush: i64 = chrono::Utc::now().timestamp() / 3600;
    loop {
        flush_interval.tick().await;

        let _flush_guard = state.flush_lock.lock().await;
        let acc = state.accumulators.read().await.clone();
        let flush_stats = state
            .h3_cache
            .flush(&state.storage, &state.station_manager, &acc, false)
            .await;
        drop(_flush_guard);

        // Flush station status DB once per hour
        let current_hour = chrono::Utc::now().timestamp() / 3600;
        if current_hour != last_station_flush {
            last_station_flush = current_hour;
            state.station_manager.flush_all();
        }

        let (stats, pre) = state.global_stats.snapshot();
        // The day-rotation rollup resets the global stats (write_and_maybe_reset),
        // so these cumulative counters can go backwards between ticks; a plain
        // subtraction underflows (panics in debug builds, observed 2026-08-07).
        // Saturate: the first tick after a reset reports 0/s for that interval.
        let packets = stats.accepted.saturating_sub(last_count);
        let raw_count = pre.raw_count;
        let raw_packets = raw_count.saturating_sub(last_raw_count);
        let pps = packets as f64 / flush_secs;
        let raw_pps = raw_packets as f64 / flush_secs;
        let h3_total = flush_stats.total;
        let h3_delta = h3_total as i64 - last_h3_total as i64;

        // Backlog since last tick: peak queue depth and max packet age at
        // dequeue (both reset here), plus packets shed by the connection
        let backlog_peak = state.backlog_peak.swap(0, Ordering::Relaxed);
        let lag_max_ms = state.lag_max_ms.swap(0, Ordering::Relaxed);
        let dropped_total = aprs::connection::DROPPED_PACKETS.load(Ordering::Relaxed);
        let dropped = dropped_total.saturating_sub(last_dropped);
        last_dropped = dropped_total;
        info!(
            "backlog: peak {}/{} queued, max lag {:.1}s, dropped {} ({} total)",
            backlog_peak,
            EVENT_CHANNEL_CAPACITY,
            lag_max_ms as f64 / 1000.0,
            dropped,
            dropped_total
        );

        // Tile cache: inventory now, lookup counters as deltas since last tick
        let tile = state.elevation.stats().await;
        let ram_d = tile.ram_hits.saturating_sub(last_tile.ram_hits);
        let disk_d = tile.disk_hits.saturating_sub(last_tile.disk_hits);
        let net_d = tile.net_fetches.saturating_sub(last_tile.net_fetches);
        let fail_d = tile.net_failures.saturating_sub(last_tile.net_failures);
        let net_secs = tile.net_time_ms.saturating_sub(last_tile.net_time_ms) as f64 / 1000.0;
        let lookups = ram_d + disk_d + net_d + fail_d;
        let per_zoom = tile
            .per_zoom
            .iter()
            .map(|(z, n)| format!("z{}:{}", z, n))
            .collect::<Vec<_>>()
            .join(" ");
        info!(
            "tile cache: {}/{} tiles ({}) {:.0}MB, total stations: {}",
            tile.tiles,
            tile.max_tiles,
            per_zoom,
            tile.bytes as f64 / (1024.0 * 1024.0),
            state.station_manager.next_station_id() - 1
        );
        info!(
            "tile lookups: {} ({:.1}% ram, {} disk, {} net, {} failed, {:.1}s fetching)",
            lookups,
            if lookups > 0 { ram_d as f64 * 100.0 / lookups as f64 } else { 0.0 },
            disk_d,
            net_d,
            fail_d,
            net_secs
        );
        last_tile = tile;
        info!(
            "valid: {} ({:.1}/s), total: {} ({:.1}/s), {}{}",
            packets, pps, raw_packets, raw_pps, stats, pre
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

        last_count = stats.accepted;
        last_raw_count = raw_count;
        last_h3_total = h3_total;
    }
}

/// Rollup timer: triggers accumulator rotation at period boundaries.
/// Acquires flush_lock to flush all cached H3 data, then rolls up.
/// Write server-level stats files and per-station status JSONs on a timer
/// independent of rollup. Keeps the frontend up-to-date between long rollup
/// periods without the cost of a full accumulator swap + Arrow aggregation.
async fn status_writer(state: Arc<AppState>) {
    let period = Duration::from_secs((*STATUS_WRITE_PERIOD_MINUTES * 60.0) as u64).max(Duration::from_secs(60));
    let mut interval = tokio::time::interval(period);
    interval.tick().await; // skip first tick so startup rollup goes first

    loop {
        interval.tick().await;

        let acc = state.accumulators.read().await.clone();

        // Write server-level stats. Passing acc for both old and new means
        // bucket == new_bucket everywhere, so no accumulator resets fire -
        // this is a pure snapshot write.
        state.protocol_stats.write_stats(&acc, &acc);
        state.global_stats.write_and_maybe_reset(&acc, &acc);
        state.global_uptime.write_snapshot(&acc.day.file);

        // Write per-station status JSONs from live in-memory data
        state.station_manager.write_status_snapshots(&acc);
    }
}

/// Outage monitor: evaluates every station's beacon/traffic freshness and
/// publishes outage / back-online notifications to the station's persistent
/// ntfy topic (see ntfy.rs for the outage definition and local-time window).
///
/// Topic URLs are minted (and persisted immediately - regenerating one would
/// strand subscribers) the first time a station is seen, and re-minted when
/// NTFY_BASE_URL / NTFY_TOPIC_PREFIX no longer match the stored URL (the old
/// server's subscribers are stranded by the config change regardless).
/// Minting is purely local; the network publishes are what's throttled - at
/// most NTFY_SENDS_PER_CYCLE per cycle, 5s apart, staying inside ntfy.sh's
/// rate budget. A station already in outage at minting time is marked
/// notified without sending: nobody can have subscribed to a topic that never
/// existed, and this stops the first run after deploy from announcing every
/// long-dead station.
///
/// Transitions outside the station's 10:00-17:00 approximate local time are
/// simply held - the pending state is re-evaluated each tick and announced
/// when the window opens (an overnight outage is announced next morning).
async fn outage_monitor(state: Arc<AppState>) {
    use chrono::Timelike;

    let period = Duration::from_secs((*OUTAGE_CHECK_PERIOD_MINUTES * 60.0) as u64)
        .max(Duration::from_secs(60));
    let mut interval = tokio::time::interval(period);
    interval.tick().await; // skip the immediate tick - let startup settle

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("reqwest client build failed");

    let epoch_str = |e: Option<Epoch>| {
        e.and_then(|e| chrono::DateTime::from_timestamp(e.0 as i64, 0))
            .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
            .unwrap_or_else(|| "unknown".to_string())
    };

    loop {
        interval.tick().await;

        let now = chrono::Utc::now();
        let now_epoch = now.timestamp() as u32;
        let utc_hour = now.hour();
        let beacon_secs = *OUTAGE_BEACON_SECS;
        let traffic_secs = *OUTAGE_TRAFFIC_SECS;
        let expected_prefix = format!("{}/{}-", &*NTFY_BASE_URL, &*NTFY_TOPIC_PREFIX);

        let mut generated = 0usize;
        let mut deferred = 0usize;
        let mut offline_sent = 0usize;
        let mut online_sent = 0usize;
        let mut online = 0usize;
        let mut outage_beacons = 0usize;
        let mut outage_traffic = 0usize;
        let mut notified_count = 0usize;
        let mut send_budget = *NTFY_SENDS_PER_CYCLE;

        for details in state.station_manager.all_stations() {
            if details.station.as_str() == "global" || details.last_packet.is_none() {
                continue;
            }

            let outage = ntfy::evaluate_outage(&details, now_epoch, beacon_secs, traffic_secs);
            match outage {
                None => online += 1,
                Some(ntfy::OutageReason::Beacons) => outage_beacons += 1,
                Some(ntfy::OutageReason::Traffic) => outage_traffic += 1,
            }
            if details.outage_notified_at.is_some() {
                notified_count += 1;
            }

            let needs_mint = match &details.ntfy_url {
                None => true,
                // Base URL / prefix config changed: the stored topic lives on
                // the old server, so a new one has to be minted
                Some(url) => !url.starts_with(&expected_prefix),
            };
            if needs_mint {
                // Re-get so the packet processor's concurrent updates to this
                // station aren't clobbered by our stale clone
                if let Some(mut fresh) = state.station_manager.get(&details.station) {
                    fresh.ntfy_url = Some(ntfy::generate_url(fresh.station.as_str()));
                    fresh.outage_notified_at = outage.map(|_| Epoch(now_epoch));
                    fresh.outage_reason = outage.map(|r| r.as_str().to_string());
                    state.station_manager.update_and_persist(&fresh);
                    generated += 1;
                }
                continue;
            }

            let notified = details.outage_notified_at.is_some();
            if outage.is_some() == notified
                || !*NTFY_ENABLED
                || !ntfy::in_notify_window(utc_hour, details.lng)
            {
                continue;
            }

            // Cap publishes per cycle so a mass transition (e.g. an APRS-IS
            // or server outage on our side) doesn't blow ntfy.sh's rate
            // budget; the rest go out on later cycles
            if send_budget == 0 {
                deferred += 1;
                continue;
            }
            send_budget -= 1;

            let url = details.ntfy_url.clone().unwrap();
            let name = details.station.as_str();

            let ok = match outage {
                Some(ntfy::OutageReason::Beacons) => {
                    ntfy::send(
                        &client,
                        &url,
                        &format!("{} offline", name),
                        &format!(
                            "No status beacons received from {} since {}. The receiver appears to be down.",
                            name,
                            epoch_str(details.last_beacon)
                        ),
                        ntfy::Priority::Default,
                        "warning",
                    )
                    .await
                }
                Some(ntfy::OutageReason::Traffic) => {
                    ntfy::send(
                        &client,
                        &url,
                        &format!("{} not receiving", name),
                        &format!(
                            "{} is beaconing but has received no aircraft traffic since {}.",
                            name,
                            epoch_str(details.last_packet)
                        ),
                        ntfy::Priority::Default,
                        "warning",
                    )
                    .await
                }
                None => {
                    let was = match details.outage_reason.as_deref() {
                        Some("beacons") => "no beacons",
                        Some("traffic") => "no traffic",
                        _ => "outage",
                    };
                    ntfy::send(
                        &client,
                        &url,
                        &format!("{} back online", name),
                        &format!(
                            "{} restarted and is reporting again ({} since {}).",
                            name,
                            was,
                            epoch_str(details.outage_notified_at)
                        ),
                        ntfy::Priority::Low,
                        "white_check_mark",
                    )
                    .await
                }
            };

            if ok {
                if let Some(mut fresh) = state.station_manager.get(&details.station) {
                    if let Some(reason) = outage {
                        fresh.outage_notified_at = Some(Epoch(now_epoch));
                        fresh.outage_reason = Some(reason.as_str().to_string());
                        offline_sent += 1;
                    } else {
                        fresh.outage_notified_at = None;
                        fresh.outage_reason = None;
                        online_sent += 1;
                    }
                    state.station_manager.update_and_persist(&fresh);
                }
            }
            // Pace sends at ntfy.sh's replenish rate. Applies to failed
            // sends too: a failure usually means the server is
            // rate-limiting, the worst time to keep hammering
            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        info!(
            "outage monitor: {} online, {} offline ({} no beacons, {} no traffic), {} notified",
            online,
            outage_beacons + outage_traffic,
            outage_beacons,
            outage_traffic,
            notified_count
        );
        if generated > 0 || deferred > 0 || offline_sent > 0 || online_sent > 0 {
            info!(
                "outage monitor: {} topic URLs generated, {} offline, {} back-online notifications, {} sends deferred to later cycles",
                generated, offline_sent, online_sent, deferred
            );
        }
    }
}

/// A station the ground-horizon queue should generate a terrain file for:
/// real, non-mobile, valid, positioned, and missing/stale file (moved, or
/// the beaconed antenna altitude changed since generation). Returns the
/// position to generate at: the primary location, not the live fix - a
/// bouncing station (two receivers sharing a callsign) toggles lat/lng
/// between sites on every packet, but the primary only changes on a
/// genuine rotation, so the file doesn't churn
fn ground_horizon_candidate(details: &StationDetails) -> Option<(f64, f64)> {
    if details.station.as_str() == "global" || details.mobile || !details.valid {
        return None;
    }
    // A just-rotated primary (a single packet at a brand-new location) is
    // still pending confirmation - it may be a mobile blip or the start of a
    // bounce, so wait for a second packet before sampling 120km of terrain
    if details.new_location_count > 0 {
        return None;
    }
    // Records that predate primary-location tracking fall back to the fix
    let [lat, lng] = details
        .primary_location
        .or_else(|| details.lat.zip(details.lng).map(|(la, lo)| [la, lo]))?;
    if !lat.is_finite() || !lng.is_finite() || (lat == 0.0 && lng == 0.0) {
        return None;
    }
    let output_dir = config::output_dir(details.station.as_str());
    ground_horizon::needs_regeneration(
        &output_dir,
        details.station.as_str(),
        details.ground_horizon_pos,
        details.ground_horizon_beacon,
        lat,
        lng,
        details.beacon_altitude,
    )
    .then_some((lat, lng))
}

/// Background ground-horizon generation: a serial FIFO of stations needing a
/// terrain file, processed one at a time strictly between rollups. Serial is
/// the throttle - coarse-to-fine sampling makes each station a handful of
/// tile fetches - and newly seen or moved stations join the back of the
/// queue as the periodic rescan finds them.
async fn ground_horizon_task(state: Arc<AppState>) {
    use std::collections::{HashSet, VecDeque};

    if *GROUND_HORIZON_PAUSED {
        info!("ground-horizon generation paused (GROUND_HORIZON_PAUSED)");
        // Park rather than return - a returned task trips the fatal watchdog
        std::future::pending::<()>().await;
    }

    let mut queue: VecDeque<StationName> = VecDeque::new();
    let mut queued: HashSet<StationName> = HashSet::new();
    let mut written = 0usize;
    let mut failed = 0usize;

    loop {
        // Strictly between rollups: wait out any in-progress rollup
        if rollup::rollup_in_progress() {
            tokio::time::sleep(Duration::from_secs(10)).await;
            continue;
        }

        // Rescan for candidates; newcomers join the back of the queue.
        // Sorted so a fleet backfill proceeds in a predictable order
        let mut discovered: Vec<StationName> = state
            .station_manager
            .all_stations()
            .iter()
            .filter(|d| ground_horizon_candidate(d).is_some())
            .map(|d| d.station.clone())
            .filter(|name| !queued.contains(name))
            .collect();
        discovered.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        for name in discovered {
            queued.insert(name.clone());
            queue.push_back(name);
        }

        let Some(name) = queue.pop_front() else {
            tokio::time::sleep(Duration::from_secs(60)).await;
            continue;
        };
        queued.remove(&name);

        // Re-check at pop time - the station may have moved, gone invalid or
        // been generated for while it sat in the queue
        let Some(details) = state.station_manager.get(&name) else {
            continue;
        };
        let Some((lat, lng)) = ground_horizon_candidate(&details) else {
            continue;
        };

        let started = std::time::Instant::now();
        let beacon_used = details.beacon_altitude;
        let generated = match ground_horizon::compute(
            &state.elevation,
            lat,
            lng,
            beacon_used,
        )
        .await
        {
            Some(gh) => {
                let output_dir = config::output_dir(name.as_str());
                match ground_horizon::write_arrow(&output_dir, name.as_str(), &gh) {
                    Ok(_) => true,
                    Err(e) => {
                        warn!("{}: ground-horizon write failed: {} - requeued", name, e);
                        false
                    }
                }
            }
            None => {
                warn!("{}: ground-horizon DEM sampling failed - requeued at the back", name);
                false
            }
        };

        if generated {
            written += 1;
            if let Some(mut details) = state.station_manager.get(&name) {
                details.ground_horizon_pos = Some([lat, lng]);
                // The beacon the file was actually computed with - a beacon
                // arriving mid-generation is picked up by the next rescan
                details.ground_horizon_beacon = beacon_used;
                // Persisted by the next rollup's flush_all; worst case after
                // an unclean shutdown is one redundant regeneration
                state.station_manager.update(&details);
            }
            debug!(
                "{}: ground-horizon written in {:.1}s ({} queued)",
                name,
                started.elapsed().as_secs_f64(),
                queue.len()
            );
            if written % 25 == 0 || queue.is_empty() {
                info!(
                    "ground-horizon backfill: {} written, {} failed attempts, {} queued",
                    written,
                    failed,
                    queue.len()
                );
            }
        } else {
            failed += 1;
            queued.insert(name.clone());
            queue.push_back(name);
            // Back off so a dead tile endpoint cycles the queue slowly
            // instead of hammering it (each failure already ate the HTTP
            // timeouts)
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    }
}

async fn rollup_timer(state: Arc<AppState>) {
    let mut last_hourly_write: i64 = chrono::Utc::now().timestamp() / 3600;
    loop {
        // Never start (or swap accumulators for) a new rollup while one is
        // still running. Checked BEFORE the swap below so a skipped boundary
        // never strands an already-swapped-out current bucket. With the
        // strictly sequential loop this only fires if a rollup is launched
        // from elsewhere (e.g. a future second entry point).
        if rollup::rollup_in_progress() {
            warn!("rollup still in progress - delaying boundary check; missed boundaries collapse into one catch-up rollup");
            tokio::time::sleep(Duration::from_secs(60)).await;
            continue;
        }

        // Check if the bucket is already stale (e.g. startup crossed a boundary,
        // or the previous rollup overran one or more boundaries). If so, rollup
        // immediately instead of sleeping to the next boundary - otherwise data
        // from the new period gets merged into the old bucket, which is wrong
        // at day boundaries.
        //
        // Catch-up collapses: buckets only change when this loop swaps them, so
        // however many boundaries were missed, all data since the last swap is
        // in the single old current bucket and one old->now rollup covers it.
        // retired_accumulators (computed in rollup_all from old vs new) handles
        // any day/month/year rotations that happened during the gap.
        let now = chrono::Utc::now();
        let new_acc = accumulators::what_accumulators(now);
        let needs_rollup = {
            let current = state.accumulators.read().await;
            current.current.bucket != new_acc.current.bucket
        };

        if !needs_rollup {
            let delay = accumulators::next_rollup_delay();
            tokio::time::sleep(delay).await;
            continue;
        }

        let old_acc = state.accumulators.read().await.clone();

        // Write JSON files and stats once per hour (or always if rollup period >= 1h)
        let current_hour = now.timestamp() / 3600;
        let write_outputs = current_hour != last_hourly_write;
        if write_outputs {
            last_hourly_write = current_hour;
        }

        // Acquire flush_lock first, then swap accumulators, so periodic
        // flushes always see consistent accumulators.
        let flush_guard = state.flush_lock.lock().await;

        // Update live accumulators so new packets use the new bucket
        {
            let mut acc = state.accumulators.write().await;
            *acc = new_acc.clone();
        }

        // Let any in-flight packet finish writing to cache.
        // One packet processor task; bucket-read to last cache update
        // is pure CPU + uncontended mutex - well under 1ms. 5ms is generous.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        // Flush all cached H3 data. Uses old accumulators so data lands
        // in the correct bucket before rollup moves it.
        state
            .h3_cache
            .flush(&state.storage, &state.station_manager, &old_acc, true)
            .await;

        // Rollup - all cached data is on disk. Hold flush_lock through
        // rollup so periodic flushes cannot open station DBs that rollup
        // already has open (which causes LockErrors).
        let rollup_stats = rollup::rollup_all(
            &state.storage,
            &state.station_manager,
            &old_acc,
            Some(&new_acc),
            write_outputs,
            &state.elevation,
        )
        .await;
        drop(flush_guard);

        // Record global H3 cell counts from this rollup cycle
        if !rollup_stats.global_h3_counts.is_empty() {
            state.global_stats.with_data(|d| {
                for (layer, acc_type, count) in &rollup_stats.global_h3_counts {
                    d.record_h3_count(acc_type, layer, *count);
                }
            });
        }

        // Write protocol and station stats after rollup completes (hourly)
        state.protocol_stats.write_stats(&old_acc, &new_acc);
        if write_outputs {
            state.global_stats.write_and_maybe_reset(&old_acc, &new_acc);
            state.global_uptime.write_snapshot(&old_acc.day.file);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ground_horizon_candidate_uses_primary_and_waits_for_confirmation() {
        let mgr = station::StationManager::new_for_test();
        let name = StationName("TEST-GH".to_string());
        let mut d = mgr.get_or_create(&name).unwrap();
        d.valid = true;
        // Live fix at a bouncing partner's site, primary elsewhere: the file
        // is generated for (and staleness compared against) the primary only
        d.lat = Some(47.0);
        d.lng = Some(8.0);
        d.primary_location = Some([46.0, 7.0]);
        assert_eq!(ground_horizon_candidate(&d), Some((46.0, 7.0)));

        // A just-rotated primary awaiting a confirming packet is skipped
        d.new_location_count = 1;
        assert_eq!(ground_horizon_candidate(&d), None);
        d.new_location_count = 0;

        // Records from before primary-location tracking fall back to the fix
        d.primary_location = None;
        assert_eq!(ground_horizon_candidate(&d), Some((47.0, 8.0)));

        // Mobile and invalid stations are excluded
        d.mobile = true;
        assert_eq!(ground_horizon_candidate(&d), None);
        d.mobile = false;
        d.valid = false;
        assert_eq!(ground_horizon_candidate(&d), None);
    }
}
