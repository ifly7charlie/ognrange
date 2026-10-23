//! In-memory H3 coverage data cache.
//!
//! Buffers coverage record updates in memory before flushing to LevelDB.
//! Mirrors the TypeScript `h3cache.ts`.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::accumulators::Accumulators;
use crate::config::{H3_CACHE_FLUSH_PERIOD_MS, H3_CACHE_MAXIMUM_DIRTY_PERIOD_MS};
use crate::coverage::header::{AccumulatorType, CoverageHeader};
use crate::coverage::record::{BufferType, CoverageRecord};
use crate::layers::Layer;
use crate::station::StationManager;
use crate::db::Storage;
use crate::types::{H3Index, H3LockKey, StationId};

struct CacheEntry {
    record: CoverageRecord,
    last_access: u64,  // ms since epoch
    first_access: u64, // ms since epoch
}

#[derive(Debug, Clone, Default)]
pub struct FlushStats {
    pub total: usize,
    pub expired: usize,
    pub written: usize,
    pub databases: usize,
    pub elapsed_ms: u64,
}

pub struct H3Cache {
    entries: Arc<Mutex<BTreeMap<H3LockKey, CacheEntry>>>,
}

impl H3Cache {
    pub fn new() -> Self {
        H3Cache {
            entries: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Update a cached H3 cell with a new observation
    pub async fn update(
        &self,
        h3: &H3Index,
        altitude: u16,
        agl: u16,
        crc: u8,
        signal: u8,
        gap: u8,
        packet_station_id: StationId,
        db_station_id: StationId,
        layer: Layer,
        current_bucket: crate::coverage::header::AccumulatorBucket,
    ) {
        let header = CoverageHeader::new(
            db_station_id,
            AccumulatorType::Current,
            current_bucket,
            h3.clone(),
            layer,
        );

        let now = now_ms();
        let lock_key = header.lock_key().clone();

        let mut entries = self.entries.lock().await;
        if let Some(entry) = entries.get_mut(&lock_key) {
            entry.last_access = now;
            if db_station_id.0 == 0 {
                entry
                    .record
                    .update_with_station(altitude, agl, crc, signal, gap, packet_station_id);
            } else {
                entry.record.update(altitude, agl, crc, signal, gap);
            }
        } else {
            let buf_type = if db_station_id.0 != 0 {
                BufferType::Station
            } else {
                BufferType::Global
            };
            let mut record = CoverageRecord::new(buf_type);
            if db_station_id.0 == 0 {
                record.update_with_station(altitude, agl, crc, signal, gap, packet_station_id);
            } else {
                record.update(altitude, agl, crc, signal, gap);
            }
            entries.insert(
                lock_key,
                CacheEntry {
                    record,
                    last_access: now,
                    first_access: now,
                },
            );
        }
    }

    /// Flush dirty/expired entries to storage.
    /// If `all_unwritten` is true, flushes everything regardless of age.
    pub async fn flush(
        &self,
        storage: &Storage,
        station_manager: &StationManager,
        accumulators: &Accumulators,
        all_unwritten: bool,
    ) -> FlushStats {
        let start = now_ms();
        let now = start;
        let flush_time = now.saturating_sub(*H3_CACHE_FLUSH_PERIOD_MS);
        let max_dirty_time = now.saturating_sub(*H3_CACHE_MAXIMUM_DIRTY_PERIOD_MS);

        let mut stats = FlushStats::default();

        // Extract entries to flush
        let to_flush: BTreeMap<H3LockKey, CacheEntry>;
        {
            let mut entries = self.entries.lock().await;
            stats.total = entries.len();

            if all_unwritten {
                to_flush = std::mem::take(&mut *entries);
            } else {
                let mut flush_keys = Vec::new();
                for (k, v) in entries.iter() {
                    if v.last_access < flush_time || v.first_access < max_dirty_time {
                        flush_keys.push(k.clone());
                        if v.last_access < flush_time {
                            stats.expired += 1;
                        }
                    }
                }
                to_flush = flush_keys
                    .into_iter()
                    .filter_map(|k| entries.remove(&k).map(|v| (k, v)))
                    .collect();
            }
        }

        // Group by station, track which layers each station has data for
        let mut by_station: BTreeMap<String, Vec<(String, Vec<u8>)>> = BTreeMap::new();
        let mut station_layers: BTreeMap<String, HashSet<Layer>> = BTreeMap::new();

        for (lock_key, entry) in &to_flush {
            if let Some(header) = CoverageHeader::from_lock_key(lock_key.as_str()) {
                let station_name = station_manager
                    .get_name(header.dbid)
                    .map(|n| n.0.clone())
                    .unwrap_or_else(|| {
                        tracing::error!("Unknown station for lock key {}", lock_key);
                        String::new()
                    });

                if !station_name.is_empty() {
                    station_layers
                        .entry(station_name.clone())
                        .or_default()
                        .insert(header.layer);
                    by_station
                        .entry(station_name)
                        .or_default()
                        .push((header.db_key(), entry.record.to_bytes()));
                    stats.written += 1;
                }
            }
        }

        // Build accumulator metadata JSON for each station's layers
        let now_secs = (now / 1000) as u32;
        let now_utc = chrono::DateTime::from_timestamp(now_secs as i64, 0)
            .map(|d| d.to_rfc3339())
            .unwrap_or_default();

        let meta_json = serde_json::json!({
            "start": now_secs,
            "startUtc": now_utc,
            "accumulators": accumulators,
            "currentAccumulator": accumulators.current.bucket,
        });
        let meta_bytes = serde_json::to_vec(&meta_json).unwrap_or_default();

        // Write metadata for all accumulator types (not just Current), matching
        // TypeScript's saveAccumulatorMetadata called from h3storage.ts flush.
        // This ensures day/month/year/yearnz meta keys exist before the first
        // rollup, so startup rollup doesn't see them as "missing".
        let all_acc_types = [
            (AccumulatorType::Current, accumulators.current.bucket),
            (AccumulatorType::Day, accumulators.day.bucket),
            (AccumulatorType::Month, accumulators.month.bucket),
            (AccumulatorType::Year, accumulators.year.bucket),
            (AccumulatorType::YearNz, accumulators.yearnz.bucket),
        ];

        for (station_name, layers) in &station_layers {
            let records = by_station.get_mut(station_name).unwrap();
            for layer in layers {
                for (acc_type, bucket) in &all_acc_types {
                    if !crate::layers::should_produce(*layer, *acc_type) {
                        continue;
                    }
                    let meta_header = CoverageHeader::accumulator_meta(
                        *acc_type,
                        *bucket,
                        *layer,
                    );
                    records.push((meta_header.db_key(), meta_bytes.clone()));
                }
            }
        }

        // Write to storage
        stats.databases = by_station.len();
        for (station_name, records) in by_station {
            if let Err(e) = storage.write_batch(&station_name, &records).await {
                tracing::error!("Failed to flush H3s for {}: {}", station_name, e);
            }
        }

        stats.elapsed_ms = now_ms() - start;
        if stats.written > 0 {
            tracing::info!(
                "H3 cache flushed: {} records for {} stations in {}ms",
                stats.written, stats.databases, stats.elapsed_ms
            );
        }
        stats
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
