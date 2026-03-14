//! Master station list output (mirrors TS produceStationFile).
//!
//! Writes stations.json, stations-complete.json, and stations.arrow
//! files during each rollup cycle.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, StringArray, UInt32Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use flate2::write::GzEncoder;
use flate2::Compression;
use tracing::{error, info};

use crate::accumulators::Accumulators;
use crate::config::{OUTPUT_PATH, UNCOMPRESSED_ARROW_FILES};
use crate::station::{StationDetails, StationManager};

/// Produce the master stations list files.
///
/// Writes:
/// - stations.json / stations.json.gz — stations with last_packet
/// - stations.day.{date}.arrow[.gz] — Arrow file with station metadata + symlinks
/// - stations-complete.json / stations-complete.json.gz — all stations
pub fn produce_station_file(
    station_manager: &StationManager,
    accumulators: &Accumulators,
) {
    use std::io::Write;

    let all_stations = station_manager.all_stations();
    let active_stations: Vec<&StationDetails> = all_stations
        .iter()
        .filter(|s| s.last_packet.is_some())
        .collect();

    let output_path = &*OUTPUT_PATH;
    let stations_dir = format!("{}stations", output_path);
    let _ = std::fs::create_dir_all(&stations_dir);

    // 1. stations.json / stations.json.gz
    match serde_json::to_string(&active_stations) {
        Ok(json) => {
            if let Err(e) = std::fs::write(format!("{}stations.json", output_path), &json) {
                error!("stations.json write error: {}", e);
            }
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            let _ = encoder.write_all(json.as_bytes());
            if let Ok(compressed) = encoder.finish() {
                if let Err(e) =
                    std::fs::write(format!("{}stations.json.gz", output_path), &compressed)
                {
                    error!("stations.json.gz write error: {}", e);
                }
            }
        }
        Err(e) => error!("stations.json serialization error: {}", e),
    }

    // 2. Arrow file — columns: id, name, lat, lng, valid, lastPacket, layerMask
    let mut sorted: Vec<&StationDetails> = active_stations;
    sorted.sort_by_key(|s| s.id.0);

    let ids: Vec<u32> = sorted.iter().map(|s| s.id.0 as u32).collect();
    let names: Vec<&str> = sorted.iter().map(|s| s.station.as_str()).collect();
    let lats: Vec<f32> = sorted.iter().map(|s| s.lat.unwrap_or(0.0) as f32).collect();
    let lngs: Vec<f32> = sorted.iter().map(|s| s.lng.unwrap_or(0.0) as f32).collect();
    let valids: Vec<bool> = sorted.iter().map(|s| s.valid).collect();
    let last_packets: Vec<u32> = sorted
        .iter()
        .map(|s| s.last_packet.map(|e| e.0).unwrap_or(0))
        .collect();
    let layer_masks: Vec<u8> = sorted.iter().map(|s| s.layer_mask.unwrap_or(0)).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("lat", DataType::Float32, false),
        Field::new("lng", DataType::Float32, false),
        Field::new("valid", DataType::Boolean, false),
        Field::new("lastPacket", DataType::UInt32, false),
        Field::new("layerMask", DataType::UInt8, false),
    ]));

    let batch = match RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(Float32Array::from(lats)) as ArrayRef,
            Arc::new(Float32Array::from(lngs)) as ArrayRef,
            Arc::new(BooleanArray::from(valids)) as ArrayRef,
            Arc::new(UInt32Array::from(last_packets)) as ArrayRef,
            Arc::new(UInt8Array::from(layer_masks)) as ArrayRef,
        ],
    ) {
        Ok(b) => b,
        Err(e) => {
            error!("Failed to create station arrow batch: {}", e);
            write_stations_complete_json(output_path, &all_stations);
            return;
        }
    };

    let day_file = &accumulators.day.file;

    // Write compressed arrow
    let gz_working = format!("{}/stations.day.{}.working.arrow.gz", stations_dir, day_file);
    let gz_final = format!("{}/stations.day.{}.arrow.gz", stations_dir, day_file);
    match (|| -> Result<(), String> {
        let file = std::fs::File::create(&gz_working)
            .map_err(|e| format!("Failed to create {}: {}", gz_working, e))?;
        let encoder = GzEncoder::new(file, Compression::default());
        let mut writer = StreamWriter::try_new(encoder, &schema)
            .map_err(|e| format!("StreamWriter error: {}", e))?;
        writer
            .write(&batch)
            .map_err(|e| format!("Write error: {}", e))?;
        writer
            .finish()
            .map_err(|e| format!("Finish error: {}", e))?;
        Ok(())
    })() {
        Ok(()) => {
            if let Err(e) = std::fs::rename(&gz_working, &gz_final) {
                error!("Rename error for stations arrow.gz: {}", e);
            } else {
                info!("output compressed station file stations.day.{}.arrow.gz", day_file);
            }
        }
        Err(e) => error!("stations arrow.gz write error: {}", e),
    }

    // Write uncompressed arrow if configured
    if *UNCOMPRESSED_ARROW_FILES {
        let raw_working = format!(
            "{}/stations.day.{}.working.arrow",
            stations_dir, day_file
        );
        let raw_final = format!("{}/stations.day.{}.arrow", stations_dir, day_file);
        match (|| -> Result<(), String> {
            let file = std::fs::File::create(&raw_working)
                .map_err(|e| format!("Failed to create {}: {}", raw_working, e))?;
            let mut writer = StreamWriter::try_new(file, &schema)
                .map_err(|e| format!("StreamWriter error: {}", e))?;
            writer
                .write(&batch)
                .map_err(|e| format!("Write error: {}", e))?;
            writer
                .finish()
                .map_err(|e| format!("Finish error: {}", e))?;
            Ok(())
        })() {
            Ok(()) => {
                if let Err(e) = std::fs::rename(&raw_working, &raw_final) {
                    error!("Rename error for stations arrow: {}", e);
                } else {
                    info!("output uncompressed station file stations.day.{}.arrow", day_file);
                }
            }
            Err(e) => error!("stations arrow write error: {}", e),
        }
    }

    // Symlinks for arrow files
    #[cfg(unix)]
    {
        let source_gz = format!("stations.day.{}.arrow.gz", day_file);
        let source_raw = format!("stations.day.{}.arrow", day_file);

        // day latest
        symlink_file(
            &source_gz,
            &format!("{}/stations.day.arrow.gz", stations_dir),
        );
        if *UNCOMPRESSED_ARROW_FILES {
            symlink_file(
                &source_raw,
                &format!("{}/stations.day.arrow", stations_dir),
            );
        }

        // month, year, yearnz symlinks
        for (name, entry) in [
            ("month", &accumulators.month),
            ("year", &accumulators.year),
            ("yearnz", &accumulators.yearnz),
        ] {
            symlink_file(
                &source_gz,
                &format!("{}/stations.{}.{}.arrow.gz", stations_dir, name, entry.file),
            );
            symlink_file(
                &source_gz,
                &format!("{}/stations.{}.arrow.gz", stations_dir, name),
            );
            if *UNCOMPRESSED_ARROW_FILES {
                symlink_file(
                    &source_raw,
                    &format!("{}/stations.{}.{}.arrow", stations_dir, name, entry.file),
                );
                symlink_file(
                    &source_raw,
                    &format!("{}/stations.{}.arrow", stations_dir, name),
                );
            }
        }

        // Legacy symlink at OUTPUT_PATH root
        symlink_file(
            &format!("stations/{}", source_gz),
            &format!("{}stations.arrow.gz", output_path),
        );
        if *UNCOMPRESSED_ARROW_FILES {
            symlink_file(
                &format!("stations/{}", source_raw),
                &format!("{}stations.arrow", output_path),
            );
        }
    }

    // 3. stations-complete.json / stations-complete.json.gz
    write_stations_complete_json(output_path, &all_stations);
}

fn write_stations_complete_json(output_path: &str, all_stations: &[StationDetails]) {
    use std::io::Write;
    match serde_json::to_string(all_stations) {
        Ok(json) => {
            if let Err(e) =
                std::fs::write(format!("{}stations-complete.json", output_path), &json)
            {
                error!("stations-complete.json write error: {}", e);
            }
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            let _ = encoder.write_all(json.as_bytes());
            if let Ok(compressed) = encoder.finish() {
                if let Err(e) = std::fs::write(
                    format!("{}stations-complete.json.gz", output_path),
                    &compressed,
                ) {
                    error!("stations-complete.json.gz write error: {}", e);
                }
            }
        }
        Err(e) => error!("stations-complete.json serialization error: {}", e),
    }
}

/// Create/overwrite a symlink (unix only)
#[cfg(unix)]
fn symlink_file(src: &str, dest: &str) {
    let _ = std::fs::remove_file(dest);
    if let Err(e) = std::os::unix::fs::symlink(src, dest) {
        error!("error symlinking {} to {}: {}", src, dest, e);
    }
}
