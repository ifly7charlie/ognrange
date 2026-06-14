//! Offline recovery: rebuild a corrupted accumulator Arrow file by re-merging
//! the intact lower-level Arrow outputs. "Arrow only" - never opens the DB
//! (the live server holds its lock); station ids come from the registry
//! (`stations-complete.json`, the same file the frontend uses), falling back to
//! per-station `{name}/{name}.json` sidecars.
//!
//! Modes:
//!   station-month <STATION> <YYYY-MM>
//!       Rebuild a station's monthly file from its own daily files (same H3
//!       resolution, plain station schema). Drive a fleet repair from a shell
//!       loop over the corrupted stations.
//!
//!   global-day <YYYY-MM-DD>
//!       Rebuild the global daily file from every station's daily file for that
//!       date. Station cells (res H3_STATION_CELL_LEVEL) are reparented to the
//!       global resolution (H3_GLOBAL_CELL_LEVEL) and merged into per-cell
//!       nested-station records.
//!
//!   global-month <YYYY-MM> [--from=month|day]
//!       Rebuild the global monthly file. --from=month (default) merges each
//!       station's monthly file; --from=day merges every station daily file in
//!       the month (use this when some station monthlies are themselves
//!       corrupt - the dailies are the trusted source).
//!
//! Options: --layer=a,b (default: all layers present), --force (overwrite even
//! a larger existing file; by default a rebuild only replaces a file that has
//! FEWER rows than the rebuild - the corruption signature - so good files like
//! adsb are left alone). The replaced file is kept as `.corrupt.<epoch>`.
//!
//! Fidelity: counts, min/max altitude and per-cell station attribution
//! (num_stations, percentages) are exact; only the signal-quality averages
//! (avgSig/avgCrc/avgGap/expectedGap) are approximate, recovered from the
//! u8-quantised per-station averages. See CoverageRecord::station_from_arrow.

// Shared modules are included via #[path]; each bin only uses part of them, so
// suppress the resulting dead-code/unused noise crate-wide for this tool.
#![allow(unused)]

#[path = "../types.rs"] mod types;
#[path = "../layers.rs"] mod layers;
#[path = "../coverage/mod.rs"] mod coverage;
#[path = "../config.rs"] mod config;

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Write};

use arrow::array::{Array, ArrayRef, StringArray, UInt16Array, UInt32Array, UInt8Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

use coverage::record::{ArrowGlobal, ArrowStation, CoverageRecord};
use layers::Layer;
use types::H3Index;

const ALL_LAYERS: [&str; 8] = [
    "combined", "flarm", "adsb", "adsl", "fanet", "ogntrk", "paw", "safesky",
];

fn out_base() -> String {
    config::OUTPUT_PATH.clone()
}

fn epoch() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Parse `*.{day|month|year}.{file_id}{.layer}.arrow.gz` (station name is the
/// arbitrary prefix; file_id never contains a dot).
fn parse_arrow(fname: &str) -> Option<(String, String, String)> {
    let stem = fname.strip_suffix(".arrow.gz")?;
    for acc in ["day", "month", "year"] {
        let needle = format!(".{}.", acc);
        if let Some(pos) = stem.find(&needle) {
            let rest = &stem[pos + needle.len()..];
            let mut parts = rest.splitn(2, '.');
            let fid = parts.next()?.to_string();
            if fid.is_empty() {
                return None;
            }
            let layer = parts.next().unwrap_or("combined").to_string();
            return Some((acc.to_string(), fid, layer));
        }
    }
    None
}

/// Read a station Arrow file into (h3 index, reconstructed record) pairs.
fn read_station_file(path: &str) -> Option<Vec<(u64, CoverageRecord)>> {
    let file = File::open(path).ok()?;
    let reader = StreamReader::try_new(GzDecoder::new(BufReader::new(file)), None).ok()?;
    let col = |b: &RecordBatch, n: &str| -> Option<ArrayRef> { b.column_by_name(n).cloned() };
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.ok()?;
        let lo = col(&batch, "h3lo")?; let lo = lo.as_any().downcast_ref::<UInt32Array>()?;
        let hi = col(&batch, "h3hi")?; let hi = hi.as_any().downcast_ref::<UInt32Array>()?;
        let min_agl = col(&batch, "minAgl")?; let min_agl = min_agl.as_any().downcast_ref::<UInt16Array>()?;
        let min_alt = col(&batch, "minAlt")?; let min_alt = min_alt.as_any().downcast_ref::<UInt16Array>()?;
        let min_alt_sig = col(&batch, "minAltSig")?; let min_alt_sig = min_alt_sig.as_any().downcast_ref::<UInt8Array>()?;
        let max_sig = col(&batch, "maxSig")?; let max_sig = max_sig.as_any().downcast_ref::<UInt8Array>()?;
        let avg_sig = col(&batch, "avgSig")?; let avg_sig = avg_sig.as_any().downcast_ref::<UInt8Array>()?;
        let avg_crc = col(&batch, "avgCrc")?; let avg_crc = avg_crc.as_any().downcast_ref::<UInt8Array>()?;
        let count = col(&batch, "count")?; let count = count.as_any().downcast_ref::<UInt32Array>()?;
        let avg_gap = col(&batch, "avgGap")?; let avg_gap = avg_gap.as_any().downcast_ref::<UInt8Array>()?;
        for i in 0..batch.num_rows() {
            let row = ArrowStation {
                h3lo: lo.value(i), h3hi: hi.value(i),
                min_agl: min_agl.value(i), min_alt: min_alt.value(i),
                min_alt_sig: min_alt_sig.value(i), max_sig: max_sig.value(i),
                avg_sig: avg_sig.value(i), avg_crc: avg_crc.value(i),
                count: count.value(i), avg_gap: avg_gap.value(i),
            };
            let h3 = ((row.h3hi as u64) << 32) | row.h3lo as u64;
            out.push((h3, CoverageRecord::station_from_arrow(&row)));
        }
    }
    Some(out)
}

/// Count rows in an existing Arrow.gz (for the shrink guard); None if absent.
fn count_rows(path: &str) -> Option<usize> {
    let file = File::open(path).ok()?;
    let reader = StreamReader::try_new(GzDecoder::new(BufReader::new(file)), None).ok()?;
    let mut n = 0;
    for batch in reader {
        n += batch.ok()?.num_rows();
    }
    Some(n)
}

fn station_schema() -> Schema {
    Schema::new(vec![
        Field::new("h3lo", DataType::UInt32, false),
        Field::new("h3hi", DataType::UInt32, false),
        Field::new("minAgl", DataType::UInt16, false),
        Field::new("minAlt", DataType::UInt16, false),
        Field::new("minAltSig", DataType::UInt8, false),
        Field::new("maxSig", DataType::UInt8, false),
        Field::new("avgSig", DataType::UInt8, false),
        Field::new("avgCrc", DataType::UInt8, false),
        Field::new("count", DataType::UInt32, false),
        Field::new("avgGap", DataType::UInt8, false),
    ])
}

fn global_schema() -> Schema {
    let mut f = station_schema().fields().to_vec();
    f.push(Field::new("stations", DataType::Utf8, false).into());
    f.push(Field::new("expectedGap", DataType::UInt8, false).into());
    f.push(Field::new("numStations", DataType::UInt8, false).into());
    Schema::new(f)
}

fn station_batch(rows: &[ArrowStation]) -> RecordBatch {
    let cols: Vec<ArrayRef> = vec![
        std::sync::Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.h3lo))),
        std::sync::Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.h3hi))),
        std::sync::Arc::new(UInt16Array::from_iter_values(rows.iter().map(|r| r.min_agl))),
        std::sync::Arc::new(UInt16Array::from_iter_values(rows.iter().map(|r| r.min_alt))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.min_alt_sig))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.max_sig))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.avg_sig))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.avg_crc))),
        std::sync::Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.count))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.avg_gap))),
    ];
    RecordBatch::try_new(std::sync::Arc::new(station_schema()), cols).expect("station batch")
}

fn global_batch(rows: &[ArrowGlobal]) -> RecordBatch {
    let cols: Vec<ArrayRef> = vec![
        std::sync::Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.h3lo))),
        std::sync::Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.h3hi))),
        std::sync::Arc::new(UInt16Array::from_iter_values(rows.iter().map(|r| r.min_agl))),
        std::sync::Arc::new(UInt16Array::from_iter_values(rows.iter().map(|r| r.min_alt))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.min_alt_sig))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.max_sig))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.avg_sig))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.avg_crc))),
        std::sync::Arc::new(UInt32Array::from_iter_values(rows.iter().map(|r| r.count))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.avg_gap))),
        std::sync::Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.stations.as_str()))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.expected_gap))),
        std::sync::Arc::new(UInt8Array::from_iter_values(rows.iter().map(|r| r.num_stations))),
    ];
    RecordBatch::try_new(std::sync::Arc::new(global_schema()), cols).expect("global batch")
}

/// Atomically install `working` as `target`, preserving any existing target as
/// `.corrupt.<epoch>` so a replaced file is never lost.
fn install(working: &str, target: &str, ts: u64) -> Result<(), String> {
    if std::fs::metadata(target).is_ok() {
        let _ = std::fs::rename(target, &format!("{}.corrupt.{}", target, ts));
    }
    std::fs::rename(working, target).map_err(|e| format!("rename {}: {}", target, e))
}

/// Write a rebuilt batch to `target` (an .arrow.gz path), plus its uncompressed
/// `.arrow` twin ONLY if one already exists on disk (so a stale corrupt twin
/// isn't left to be served, but no new uncompressed files are introduced where
/// the deployment doesn't use them). Honours the shrink guard: unless `force`,
/// refuses to replace a file with >= rows. Replaced files are preserved as
/// `.corrupt.<epoch>`.
fn write_target(target: &str, schema: &Schema, batch: &RecordBatch, force: bool) -> String {
    let new_rows = batch.num_rows();
    if let Some(existing) = count_rows(target) {
        if !force && existing >= new_rows {
            return format!("SKIP  {} (existing {} rows >= rebuilt {} rows; --force to override)", target, existing, new_rows);
        }
    }
    let schema = std::sync::Arc::new(schema.clone());
    let ts = epoch();

    // gzip output (always).
    let gz_working = format!("{}.working", target);
    let gz_res = (|| -> Result<(), String> {
        let file = File::create(&gz_working).map_err(|e| format!("create {}: {}", gz_working, e))?;
        let mut w = StreamWriter::try_new(GzEncoder::new(file, Compression::default()), &schema)
            .map_err(|e| format!("writer: {}", e))?;
        w.write(batch).map_err(|e| format!("write: {}", e))?;
        let enc = w.into_inner().map_err(|e| format!("into_inner: {}", e))?;
        enc.finish().map_err(|e| format!("gz finish: {}", e))?; // explicit footer flush
        Ok(())
    })();
    if let Err(e) = gz_res {
        let _ = std::fs::remove_file(&gz_working);
        return format!("FAIL  {}: {}", target, e);
    }

    // uncompressed twin (when the deployment uses one).
    let raw_target = target.strip_suffix(".gz").map(|s| s.to_string());
    let raw_working = raw_target.as_ref().map(|r| format!("{}.working", r));
    // Mirror existing layout only: rewrite the uncompressed twin iff one is
    // already present; never create a new .arrow where the deployment has none.
    let want_raw = raw_target.as_ref().map(|r| std::fs::metadata(r).is_ok()).unwrap_or(false);
    if want_raw {
        let (rt, rw) = (raw_target.as_ref().unwrap(), raw_working.as_ref().unwrap());
        let raw_res = (|| -> Result<(), String> {
            let file = File::create(rw).map_err(|e| format!("create {}: {}", rw, e))?;
            let mut w = StreamWriter::try_new(file, &schema).map_err(|e| format!("raw writer: {}", e))?;
            w.write(batch).map_err(|e| format!("raw write: {}", e))?;
            w.finish().map_err(|e| format!("raw finish: {}", e))?;
            Ok(())
        })();
        if let Err(e) = raw_res {
            let _ = std::fs::remove_file(rw);
            let _ = std::fs::remove_file(&gz_working);
            return format!("FAIL  {}: {}", rt, e);
        }
    }

    if let Err(e) = install(&gz_working, target, ts) {
        return format!("FAIL  {}", e);
    }
    if want_raw {
        if let Err(e) = install(raw_working.as_ref().unwrap(), raw_target.as_ref().unwrap(), ts) {
            return format!("WROTE {} ({} rows) but raw twin FAIL {}", target, new_rows, e);
        }
        return format!("WROTE {} (+.arrow) ({} rows)", target, new_rows);
    }
    format!("WROTE {} ({} rows)", target, new_rows)
}

/// Merge same-resolution station records by H3 into a sorted ArrowStation set.
fn merge_station(records: Vec<(u64, CoverageRecord)>) -> Vec<ArrowStation> {
    let mut map: HashMap<u64, CoverageRecord> = HashMap::new();
    for (h3, rec) in records {
        map.entry(h3)
            .and_modify(|e| { if let Some(m) = e.rollup(&rec, None) { *e = m; } })
            .or_insert(rec);
    }
    let mut keys: Vec<u64> = map.keys().copied().collect();
    keys.sort_unstable();
    keys.into_iter()
        .map(|h3| {
            let (lo, hi) = ((h3 & 0xffff_ffff) as u32, (h3 >> 32) as u32);
            map[&h3].to_arrow_station(lo, hi)
        })
        .collect()
}

/// Merge station records (with owning station id) into global cells at the
/// global resolution, returning a sorted ArrowGlobal set.
fn merge_global(records: Vec<(u64, u16, CoverageRecord)>, gres: h3o::Resolution) -> Vec<ArrowGlobal> {
    // First collapse a station's many child cells under one global cell.
    let mut per_station: HashMap<(u64, u16), CoverageRecord> = HashMap::new();
    let mut reparent_fail = 0u64;
    for (h3, id, rec) in records {
        let Some(cell) = h3o::CellIndex::try_from(h3).ok() else { reparent_fail += 1; continue };
        let Some(parent) = cell.parent(gres) else { reparent_fail += 1; continue };
        let gh3 = u64::from(parent);
        per_station.entry((gh3, id))
            .and_modify(|e| { if let Some(m) = e.rollup(&rec, None) { *e = m; } })
            .or_insert(rec);
    }
    if reparent_fail > 0 {
        eprintln!("  warning: {} rows failed reparenting (bad H3 index)", reparent_fail);
    }
    // Then merge stations into each global cell.
    let mut per_cell: HashMap<u64, CoverageRecord> = HashMap::new();
    for ((gh3, id), st) in per_station {
        let g = CoverageRecord::global_single(id, &st);
        per_cell.entry(gh3)
            .and_modify(|e| { if let Some(m) = e.rollup(&g, None) { *e = m; } })
            .or_insert(g);
    }
    let mut keys: Vec<u64> = per_cell.keys().copied().collect();
    keys.sort_unstable();
    keys.into_iter()
        .map(|gh3| {
            let (lo, hi) = ((gh3 & 0xffff_ffff) as u32, (gh3 >> 32) as u32);
            per_cell[&gh3].to_arrow_global(lo, hi)
        })
        .collect()
}

/// Build the name -> id map from the station registry written for the frontend
/// (`stations-complete.json` = all stations; falls back to `stations.json` =
/// active only). This is the authoritative id source and covers every station,
/// unlike the per-station `{name}/{name}.json` sidecars (many are absent).
fn load_id_map(base: &str) -> HashMap<String, u16> {
    let mut m = HashMap::new();
    for fname in ["stations-complete.json", "stations.json"] {
        let path = format!("{}{}", base, fname);
        let Ok(txt) = std::fs::read_to_string(&path) else { continue };
        let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(&txt) else { continue };
        for s in arr {
            if let (Some(name), Some(id)) =
                (s.get("station").and_then(|v| v.as_str()), s.get("id").and_then(|v| v.as_u64()))
            {
                m.entry(name.to_string()).or_insert(id as u16);
            }
        }
        if !m.is_empty() {
            break; // prefer the complete registry
        }
    }
    m
}

/// Fallback: read a station's id from its own `{station}.json` sidecar.
fn station_id_sidecar(base: &str, name: &str) -> Option<u16> {
    let path = format!("{}{}/{}.json", base, name, name);
    let txt = std::fs::read_to_string(&path).ok()?;
    let v: serde_json::Value = serde_json::from_str(&txt).ok()?;
    v.get("id")?.as_u64().map(|n| n as u16)
}

fn list_station_dirs(base: &str) -> Vec<String> {
    let mut v = Vec::new();
    if let Ok(entries) = std::fs::read_dir(base) {
        for e in entries.flatten() {
            if e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                if let Some(n) = e.file_name().to_str() {
                    if n != "global" {
                        v.push(n.to_string());
                    }
                }
            }
        }
    }
    v.sort();
    v
}

/// Files in `dir` matching acc type, a file_id predicate, and layer.
fn matching_files(dir: &str, acc: &str, layer: &str, fid_ok: &dyn Fn(&str) -> bool) -> Vec<String> {
    let mut v = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            if e.file_type().map(|t| t.is_symlink()).unwrap_or(false) {
                continue;
            }
            let fname = e.file_name();
            let Some(fname) = fname.to_str() else { continue };
            let Some((a, fid, l)) = parse_arrow(fname) else { continue };
            if a == acc && l == layer && fid_ok(&fid) {
                v.push(e.path().to_string_lossy().to_string());
            }
        }
    }
    v
}

fn layers_arg(args: &[String]) -> Vec<String> {
    args.iter()
        .find_map(|a| a.strip_prefix("--layer="))
        .map(|s| s.split(',').map(|x| x.trim().to_string()).collect())
        .unwrap_or_else(|| ALL_LAYERS.iter().map(|s| s.to_string()).collect())
}

fn suffix(layer: &str) -> String {
    Layer::from_name(layer).map(|l| l.file_suffix().to_string()).unwrap_or_default()
}

fn main() {
    let _ = dotenvy::from_filename(".env.local");
    let args: Vec<String> = std::env::args().collect();
    let force = args.iter().any(|a| a == "--force");
    let base = out_base();
    let gres = h3o::Resolution::try_from(*config::H3_GLOBAL_CELL_LEVEL)
        .unwrap_or(h3o::Resolution::Seven);

    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("");
    match mode {
        "station-month" => {
            let (Some(station), Some(ym)) = (args.get(2), args.get(3)) else {
                eprintln!("usage: arrowrebuild station-month <STATION> <YYYY-MM> [--layer=..] [--force]");
                std::process::exit(2);
            };
            let dir = format!("{}{}", base, station);
            let day_prefix = format!("{}-", ym); // day file_id "YYYY-MM-DD"
            for layer in layers_arg(&args) {
                let files = matching_files(&dir, "day", &layer, &|fid| fid.starts_with(&day_prefix));
                if files.is_empty() {
                    continue;
                }
                let mut recs = Vec::new();
                for f in &files {
                    if let Some(mut r) = read_station_file(f) {
                        recs.append(&mut r);
                    }
                }
                let rows = merge_station(recs);
                let target = format!("{}/{}.month.{}{}.arrow.gz", dir, station, ym, suffix(&layer));
                let batch = station_batch(&rows);
                println!("{}  [{} from {} day files]", write_target(&target, &station_schema(), &batch, force), layer, files.len());
            }
        }
        "global-day" | "global-month" => {
            let Some(period) = args.get(2) else {
                eprintln!("usage: arrowrebuild {} <PERIOD> [--from=month|day] [--layer=..] [--force]", mode);
                std::process::exit(2);
            };
            let is_month = mode == "global-month";
            let from_day = is_month
                && args.iter().find_map(|a| a.strip_prefix("--from=")).map(|s| s == "day").unwrap_or(false);
            let (src_acc, fid_ok): (&str, Box<dyn Fn(&str) -> bool>) = if !is_month {
                ("day", { let p = period.clone(); Box::new(move |fid: &str| fid == p) })
            } else if from_day {
                ("day", { let p = format!("{}-", period); Box::new(move |fid: &str| fid.starts_with(&p)) })
            } else {
                ("month", { let p = period.clone(); Box::new(move |fid: &str| fid == p) })
            };

            let stations = list_station_dirs(&base);
            let id_map = load_id_map(&base);
            eprintln!("loaded {} station ids from registry", id_map.len());
            let mut missing_id = 0usize;
            let global_dir = format!("{}global", base);
            let _ = std::fs::create_dir_all(&global_dir);

            for layer in layers_arg(&args) {
                let mut recs: Vec<(u64, u16, CoverageRecord)> = Vec::new();
                let mut used_stations = 0usize;
                for (i, name) in stations.iter().enumerate() {
                    if i % 500 == 0 {
                        eprint!("\r  {} layer {}: {}/{} stations", mode, layer, i, stations.len());
                        let _ = std::io::stderr().flush();
                    }
                    let id = id_map.get(name).copied()
                        .or_else(|| id_map.get(&name.to_uppercase()).copied())
                        .or_else(|| station_id_sidecar(&base, name));
                    let Some(id) = id else { missing_id += 1; continue };
                    let dir = format!("{}{}", base, name);
                    let files = matching_files(&dir, src_acc, &layer, &fid_ok);
                    if files.is_empty() {
                        continue;
                    }
                    used_stations += 1;
                    for f in &files {
                        if let Some(rows) = read_station_file(f) {
                            for (h3, rec) in rows {
                                recs.push((h3, id, rec));
                            }
                        }
                    }
                }
                eprint!("\r{:60}\r", " ");
                if recs.is_empty() {
                    continue;
                }
                let rows = merge_global(recs, gres);
                let fname = if is_month {
                    format!("global.month.{}{}.arrow.gz", period, suffix(&layer))
                } else {
                    format!("global.day.{}{}.arrow.gz", period, suffix(&layer))
                };
                let target = format!("{}/{}", global_dir, fname);
                let batch = global_batch(&rows);
                println!("{}  [{} from {} stations, {} src={}]", write_target(&target, &global_schema(), &batch, force), layer, used_stations, recs_label(is_month, from_day), src_acc);
            }
            if missing_id > 0 {
                eprintln!("note: {} station dirs had no readable id sidecar and were skipped", missing_id);
            }
        }
        _ => {
            eprintln!("usage: arrowrebuild <station-month|global-day|global-month> ...");
            eprintln!("  station-month <STATION> <YYYY-MM>");
            eprintln!("  global-day    <YYYY-MM-DD>");
            eprintln!("  global-month  <YYYY-MM> [--from=month|day]");
            eprintln!("  common: [--layer=a,b] [--force]");
            std::process::exit(2);
        }
    }
}

fn recs_label(is_month: bool, from_day: bool) -> &'static str {
    if is_month && from_day { "month-from-day" } else if is_month { "month" } else { "day" }
}
