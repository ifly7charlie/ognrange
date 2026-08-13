//! Per-station ground (terrain) horizon.
//!
//! Samples the DEM along 720 half-degree bearing rays out to 120km and writes
//! a single non-accumulator `{station}.ground-horizon.arrow` file: terrain is
//! static, so the file is generated once per station location and only
//! regenerated when it is missing or the station has moved (see
//! `needs_regeneration`; `StationDetails.ground_horizon_pos` records the
//! position the file was generated for). Sample 0 of every ray is the
//! station's own ground elevation - the rotation point - which also gives the
//! frontend the station ground level it otherwise has no access to. Angles
//! are computed from the antenna viewpoint (ground + `antenna_agl_m`), the
//! same reference the receive horizon uses.
//!
//! Sampling is coarse-to-fine: each ray is first walked end-to-end at
//! COARSE_ZOOM (z7, ~1km pixels already cached by the packet path's coarse
//! AGL check) to find the provisional skyline, then only the buckets whose
//! coarse angle plus a worst-case under-read margin could still beat that
//! skyline are re-read at the fine zoom (z11), supersampled at ~pixel pitch
//! and stored as the bucket max. Once terrain occludes the horizon, nothing
//! beyond it is fetched at z11 unless it could be materially higher - the
//! fine tiles go exactly where the horizon is decided.
//!
//! Unlike the receive horizon (horizon.rs) this samples the DEM directly at
//! each ray coordinate - no H3 cells, no cell-centre quantisation.
//!
//! Self-contained on purpose (depends only on `elevation` and `config`): the
//! `groundhorizon` bin tool includes this module via `#[path]` and should not
//! drag in the DB/coverage stack. The small geometry helpers are commented
//! copies of their originals rather than imports for the same reason.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, FixedSizeListArray, Float32Array, Int16Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use flate2::write::GzEncoder;
use flate2::Compression;

use crate::config;
use crate::elevation::{ElevationService, COARSE_ZOOM};

/// Bearing bins per full circle (0.5 degree), matching the receive horizon
pub const GROUND_BINS: usize = 720;
/// Sample spacing along each bearing ray
pub const GROUND_STEP_KM: f64 = 0.5;
/// Ray length; matches HORIZON_MAX_DISTANCE_KM (horizon.rs)
pub const GROUND_MAX_KM: f64 = 120.0;
/// Samples per ray: 0..=120km inclusive, sample 0 is the station itself
pub const GROUND_SAMPLES: usize = 241;

const BIN_DEG: f64 = 360.0 / GROUND_BINS as f64;
const EARTH_RADIUS_KM: f64 = 6371.0;
const EARTH_RADIUS_M: f64 = 6_371_000.0;
/// Standard-refraction effective earth radius factor
const REFRACTION_K: f64 = 4.0 / 3.0;

/// Floor for the refinement margin: even where the coarse ray reads dead
/// flat, grant this much possible hidden height (DEM downsampling smooths
/// small features below its own noise signature)
const REFINE_MARGIN_FLOOR_M: f64 = 50.0;
/// How much height the local coarse relief could be hiding. A ~150m-wide
/// knife ridge carries ~1/6 weight in a ~900m z7 pixel, so a crest of height
/// H leaves only ~H/6 of signature in the coarse samples; steep faces
/// (pixel-internal range ~ local relief) are covered with room to spare.
/// Correctness bound for the refinement rule, not a tuning knob: a coarse
/// sample is only skipped for fine re-reading when even the margin could not
/// lift it above the provisional skyline
const REFINE_RELIEF_FACTOR: f64 = 6.0;
/// Cap on fine-pass supersampling steps per 0.5km bucket
const MAX_SUBSTEPS: usize = 16;
/// Equatorial web-mercator circumference, for pixel ground-size derivation
const MERCATOR_CIRCUMFERENCE_M: f64 = 40_075_016.686;

/// Ground size of one 256px-tile pixel at a latitude and zoom
fn pixel_size_m(zoom: u32, lat: f64) -> f64 {
    MERCATOR_CIRCUMFERENCE_M * lat.to_radians().cos() / (256.0 * 2f64.powi(zoom as i32))
}

/// Supersampling steps per bucket so the fine pass reads roughly every pixel
/// along the ray (1 when the pixel is coarser than the 0.5km step)
fn substeps(zoom: u32, lat: f64) -> usize {
    ((GROUND_STEP_KM * 1000.0 / pixel_size_m(zoom, lat)).ceil() as usize).clamp(1, MAX_SUBSTEPS)
}

/// Distance of substep k (1..=n) within bucket s: the last substep lands
/// exactly on the bucket's nominal distance, so n=1 degenerates to the plain
/// one-sample-per-bucket scheme
fn substep_distance_km(s: usize, k: usize, n: usize) -> f64 {
    (s as f64 - 1.0 + k as f64 / n as f64) * GROUND_STEP_KM
}

/// Metres of hidden height the coarse ray could be concealing around bucket
/// `s`: scaled from the max-min relief of the neighbouring coarse samples
/// (+-2 buckets ~ +-1km, comfortably covering a ~900m z7 pixel). Flat coarse
/// terrain cannot hide a mountain - its signature would show - so the margin
/// collapses toward the floor there, while cliffs and steep faces (where a
/// single pixel spans hundreds of metres of height) get a margin to match.
/// `coarse` is the whole ray; index 0 (the station point sample) is excluded
fn refine_margin_m(coarse: &[i16], s: usize) -> f64 {
    let lo = s.saturating_sub(2).max(1);
    let hi = (s + 2).min(coarse.len() - 1);
    let window = &coarse[lo..=hi];
    let relief = (*window.iter().max().unwrap() - *window.iter().min().unwrap()) as f64;
    (REFINE_RELIEF_FACTOR * relief).max(REFINE_MARGIN_FLOOR_M)
}

/// Whether a coarse sample could still beat the provisional skyline once the
/// worst-case pixel under-read (`margin_m`, see refine_margin_m) is granted.
/// The angular margin is enormous close-in and shrinks with distance, so
/// near terrain is effectively always refined while occluded or flat far
/// field prunes away
fn is_candidate(coarse_angle_deg: f64, distance_km: f64, margin_m: f64, skyline_deg: f64) -> bool {
    let margin_deg = margin_m.atan2(distance_km * 1000.0).to_degrees();
    coarse_angle_deg + margin_deg >= skyline_deg
}

/// Antenna height above ground from the beaconed MSL altitude against the
/// DEM ground, when the difference is sane (0..=300m - OGN station altitude
/// configs are often 0, site ground level, or feet-as-metres). None when the
/// beacon is missing or implausible - the caller falls back to the
/// configured default
pub fn beacon_agl_m(beacon_altitude: Option<f64>, ground_m: f64) -> Option<f64> {
    beacon_altitude
        .map(|alt| alt - ground_m)
        .filter(|agl| (0.0..=300.0).contains(agl))
}

/// The height actually used as the angle viewpoint: beacon-derived AGL or
/// the configured default. Shared with the receive horizon (horizon.rs) so
/// both charts keep the same viewpoint reference
pub fn antenna_agl_m(beacon_altitude: Option<f64>, ground_m: f64) -> f64 {
    beacon_agl_m(beacon_altitude, ground_m).unwrap_or(*config::GROUND_STATION_AGL_M)
}

/// Elevation angle to a point `delta_h_m` above station ground at
/// `distance_m`, with the k=4/3 curvature dip. Copy of
/// horizon.rs::elevation_angle_deg (and horizondata.ts::elevationAngleDeg) -
/// the three must stay identical so ground and receive angles are comparable
fn elevation_angle_deg(delta_h_m: f64, distance_m: f64) -> f64 {
    let geometric = delta_h_m.atan2(distance_m);
    let curvature_dip = distance_m / (2.0 * REFRACTION_K * EARTH_RADIUS_M);
    (geometric - curvature_dip).to_degrees()
}

/// Forward great-circle destination from (lat, lng) along a bearing.
/// Mirror of horizondata.ts::destinationPoint (which returns [lng, lat] for
/// deck.gl; this returns (lat, lng))
pub fn destination_point(lat: f64, lng: f64, bearing_deg: f64, distance_km: f64) -> (f64, f64) {
    let d = distance_km / EARTH_RADIUS_KM;
    let brng = bearing_deg.to_radians();
    let lat1 = lat.to_radians();
    let lng1 = lng.to_radians();
    let lat2 = (lat1.sin() * d.cos() + lat1.cos() * d.sin() * brng.cos()).asin();
    let lng2 = lng1 + (brng.sin() * d.sin() * lat1.cos()).atan2(d.cos() - lat1.sin() * lat2.sin());
    (lat2.to_degrees(), (lng2.to_degrees() + 540.0).rem_euclid(360.0) - 180.0)
}

/// Haversine distance in km. Copy of station.rs::great_circle_distance (kept
/// local so the bin tool's include set stays small)
fn great_circle_distance_km(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    let d_lat = (lat2 - lat1).to_radians();
    let d_lng = (lng2 - lng1).to_radians();
    let a = (d_lat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (d_lng / 2.0).sin().powi(2);
    EARTH_RADIUS_KM * 2.0 * a.sqrt().asin()
}

fn clamp_i16(elevation_m: f64) -> i16 {
    elevation_m.round().clamp(i16::MIN as f64, i16::MAX as f64) as i16
}

pub struct GroundHorizon {
    pub lat: f64,
    pub lng: f64,
    /// Beacon-derived antenna height above ground; None when the beacon was
    /// missing or implausible and the configured default height was used
    /// instead (persisted as stationAgl=NaN so the frontend can tell)
    pub agl_m: Option<f64>,
    /// GROUND_BINS x GROUND_SAMPLES row-major terrain samples, m MSL.
    /// Sample 0 of every row is the station ground elevation; refined buckets
    /// hold the max over their fine-pass substeps, pruned buckets the coarse
    /// sample
    pub elevations: Vec<i16>,
    /// Max occlusion angle per bin over samples 1..GROUND_SAMPLES.
    /// Not serialised - the frontend derives the skyline (and the visible
    /// foreground crests) from the elevations; kept for tests and the
    /// groundhorizon bin tool's summary output
    pub horizon_angle: Vec<f32>,
    /// Distance (km, rounded) of that max-angle sample per bin (not serialised)
    pub horizon_distance_km: Vec<u16>,
}

impl GroundHorizon {
    /// Pure assembler: derive the per-bin horizon from raw ray samples,
    /// viewed from `agl_m` (or the configured default when None) above the
    /// station ground (sample 0)
    pub fn from_samples(lat: f64, lng: f64, agl_m: Option<f64>, elevations: Vec<i16>) -> GroundHorizon {
        assert_eq!(elevations.len(), GROUND_BINS * GROUND_SAMPLES);
        let viewpoint_m = elevations[0] as f64 + agl_m.unwrap_or(*config::GROUND_STATION_AGL_M);
        let mut horizon_angle = Vec::with_capacity(GROUND_BINS);
        let mut horizon_distance_km = Vec::with_capacity(GROUND_BINS);
        for bin in 0..GROUND_BINS {
            let ray = &elevations[bin * GROUND_SAMPLES..(bin + 1) * GROUND_SAMPLES];
            let mut best = f64::NEG_INFINITY;
            let mut best_km = 0u16;
            for (s, &sample) in ray.iter().enumerate().skip(1) {
                let distance_km = s as f64 * GROUND_STEP_KM;
                let angle = elevation_angle_deg(sample as f64 - viewpoint_m, distance_km * 1000.0);
                if angle > best {
                    best = angle;
                    best_km = distance_km.round() as u16;
                }
            }
            horizon_angle.push(best as f32);
            horizon_distance_km.push(best_km);
        }
        GroundHorizon { lat, lng, agl_m, elevations, horizon_angle, horizon_distance_km }
    }
}

/// Sample the DEM along every bearing ray, coarse-to-fine (see module docs).
/// All-or-nothing: any failed tile fetch returns None and the whole
/// computation is retried next cycle - a partial file would otherwise persist
/// until the station moves
pub async fn compute(
    elevation: &ElevationService,
    lat: f64,
    lng: f64,
    beacon_altitude: Option<f64>,
) -> Option<GroundHorizon> {
    let fine_zoom = elevation.fine_zoom();
    let fine_substeps = substeps(fine_zoom, lat);
    let mut cursor = elevation.cursor();

    let station_m = clamp_i16(cursor.sample(lat, lng, fine_zoom).await?);
    let agl_m = beacon_agl_m(beacon_altitude, station_m as f64);
    let viewpoint_m = station_m as f64 + agl_m.unwrap_or(*config::GROUND_STATION_AGL_M);

    let mut elevations = Vec::with_capacity(GROUND_BINS * GROUND_SAMPLES);
    let mut ray = [0i16; GROUND_SAMPLES];
    let mut coarse_angles = [0f64; GROUND_SAMPLES];
    for bin in 0..GROUND_BINS {
        let bearing = bin as f64 * BIN_DEG;
        ray[0] = station_m;

        // Coarse pass: the whole ray at z7 establishes the provisional skyline
        let mut skyline = f64::NEG_INFINITY;
        for s in 1..GROUND_SAMPLES {
            let distance_km = s as f64 * GROUND_STEP_KM;
            let (slat, slng) = destination_point(lat, lng, bearing, distance_km);
            let sample_m = cursor.sample(slat, slng, COARSE_ZOOM).await?;
            ray[s] = clamp_i16(sample_m);
            let angle = elevation_angle_deg(sample_m - viewpoint_m, distance_km * 1000.0);
            coarse_angles[s] = angle;
            if angle > skyline {
                skyline = angle;
            }
        }

        // Fine pass: re-read only the buckets that could beat the skyline,
        // supersampled at ~pixel pitch, keeping the max (a crest between two
        // 500m samples no longer slips through)
        for s in 1..GROUND_SAMPLES {
            let distance_km = s as f64 * GROUND_STEP_KM;
            if !is_candidate(coarse_angles[s], distance_km, refine_margin_m(&ray, s), skyline) {
                continue;
            }
            let mut bucket_max = i16::MIN;
            for k in 1..=fine_substeps {
                let d = substep_distance_km(s, k, fine_substeps);
                let (slat, slng) = destination_point(lat, lng, bearing, d);
                bucket_max = bucket_max.max(clamp_i16(cursor.sample(slat, slng, fine_zoom).await?));
            }
            ray[s] = bucket_max;
        }
        elevations.extend_from_slice(&ray);
    }
    Some(GroundHorizon::from_samples(lat, lng, agl_m, elevations))
}

fn gz_path(output_dir: &str, station_name: &str) -> String {
    format!("{}/{}.ground-horizon.arrow.gz", output_dir, station_name)
}

fn raw_path(output_dir: &str, station_name: &str) -> String {
    format!("{}/{}.ground-horizon.arrow", output_dir, station_name)
}

/// Beacon changes below this don't trigger a rebuild - absorbs the /A=
/// feet->metres rounding while still catching a reconfigured antenna altitude
const BEACON_REGEN_THRESHOLD_M: f64 = 2.0;

/// Whether the live beaconed altitude differs enough from the one the file
/// was generated with to change the viewpoint materially. A beacon appearing
/// or disappearing always counts; the recorded value is the raw /A= input
/// (even one compute() rejected as implausible) so a stable beacon compares
/// stable regardless of the sanity-window outcome
pub fn beacon_changed(current: Option<f64>, recorded: Option<f64>) -> bool {
    match (current, recorded) {
        (Some(c), Some(r)) => (c - r).abs() >= BEACON_REGEN_THRESHOLD_M,
        (None, None) => false,
        _ => true,
    }
}

/// Whether the terrain file must be (re)generated: missing file, no recorded
/// generation position, the station has moved beyond the move threshold, or
/// the beaconed antenna altitude has materially changed since the file was
/// written (an operator correcting their configured altitude gets a fresh
/// viewpoint without having to move the station)
pub fn needs_regeneration(
    output_dir: &str,
    station_name: &str,
    recorded_pos: Option<[f64; 2]>,
    recorded_beacon: Option<f64>,
    lat: f64,
    lng: f64,
    beacon_altitude: Option<f64>,
) -> bool {
    if !std::path::Path::new(&gz_path(output_dir, station_name)).exists() {
        return true;
    }
    let Some([rlat, rlng]) = recorded_pos else {
        return true;
    };
    if great_circle_distance_km(rlat, rlng, lat, lng) > *config::STATION_MOVE_THRESHOLD_KM {
        return true;
    }
    beacon_changed(beacon_altitude, recorded_beacon)
}

fn ground_schema(gh: &GroundHorizon, item_field: &Arc<Field>) -> Schema {
    let generated_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let metadata: HashMap<String, String> = [
        ("stationLat".to_string(), format!("{:.6}", gh.lat)),
        ("stationLng".to_string(), format!("{:.6}", gh.lng)),
        // NaN = the configured default height was assumed (beacon missing or
        // implausible); the frontend treats it like pre-capture files that
        // lack the key entirely
        ("stationAgl".to_string(), gh.agl_m.map_or("NaN".to_string(), |a| format!("{:.1}", a))),
        ("stepKm".to_string(), GROUND_STEP_KM.to_string()),
        ("maxKm".to_string(), (GROUND_MAX_KM as u32).to_string()),
        ("samples".to_string(), GROUND_SAMPLES.to_string()),
        ("generatedAt".to_string(), generated_at.to_string()),
    ]
    .into();
    Schema::new_with_metadata(
        vec![
            Field::new("bearing", DataType::Float32, false),
            Field::new_fixed_size_list("elevations", item_field.clone(), GROUND_SAMPLES as i32, false),
        ],
        metadata,
    )
}

/// Write {station}.ground-horizon.arrow.gz (plus the uncompressed twin when
/// UNCOMPRESSED_ARROW_FILES). Same .working-then-rename pattern as the rollup
/// arrow writers; no symlink (the name is already stable - no accumulator or
/// file id) and no shrink guard (always exactly GROUND_BINS rows)
pub fn write_arrow(output_dir: &str, station_name: &str, gh: &GroundHorizon) -> Result<usize, String> {
    std::fs::create_dir_all(output_dir)
        .map_err(|e| format!("Failed to create {}: {}", output_dir, e))?;

    let item_field = Arc::new(Field::new("item", DataType::Int16, false));
    let schema = Arc::new(ground_schema(gh, &item_field));

    let values: ArrayRef = Arc::new(Int16Array::from_iter_values(gh.elevations.iter().copied()));
    let elevations = FixedSizeListArray::try_new(item_field, GROUND_SAMPLES as i32, values, None)
        .map_err(|e| format!("FixedSizeList error: {}", e))?;
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Float32Array::from_iter_values(
            (0..GROUND_BINS).map(|i| (i as f64 * BIN_DEG) as f32),
        )),
        Arc::new(elevations),
    ];
    let batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| format!("RecordBatch error: {}", e))?;

    let gz_final = gz_path(output_dir, station_name);
    let gz_working = format!("{}.working", gz_final);
    {
        let file = std::fs::File::create(&gz_working)
            .map_err(|e| format!("Failed to create {}: {}", gz_working, e))?;
        let encoder = GzEncoder::new(file, Compression::default());
        let mut writer = StreamWriter::try_new(encoder, &schema)
            .map_err(|e| format!("StreamWriter error: {}", e))?;
        writer.write(&batch).map_err(|e| format!("Write error: {}", e))?;
        writer.finish().map_err(|e| format!("Finish error: {}", e))?;
    }
    std::fs::rename(&gz_working, &gz_final).map_err(|e| format!("Rename error: {}", e))?;

    if *config::UNCOMPRESSED_ARROW_FILES {
        let raw_final = raw_path(output_dir, station_name);
        let raw_working = format!("{}.working", raw_final);
        {
            let file = std::fs::File::create(&raw_working)
                .map_err(|e| format!("Failed to create {}: {}", raw_working, e))?;
            let mut writer = StreamWriter::try_new(file, &schema)
                .map_err(|e| format!("StreamWriter error: {}", e))?;
            writer.write(&batch).map_err(|e| format!("Write error: {}", e))?;
            writer.finish().map_err(|e| format!("Finish error: {}", e))?;
        }
        std::fs::rename(&raw_working, &raw_final).map_err(|e| format!("Rename error: {}", e))?;
    }

    Ok(GROUND_BINS)
}

/// Delete the terrain files (both variants), for confirmed move purges.
/// Returns how many files were removed
pub fn remove_files(output_dir: &str, station_name: &str) -> usize {
    [gz_path(output_dir, station_name), raw_path(output_dir, station_name)]
        .iter()
        .filter(|p| std::fs::remove_file(p).is_ok())
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::ipc::reader::StreamReader;
    use flate2::read::GzDecoder;

    /// Degrees of latitude per km, approximately
    const DEG_PER_KM: f64 = 1.0 / 111.195;

    #[test]
    fn destination_cardinals() {
        // ~111.195km north = one degree of latitude
        let (lat, lng) = destination_point(47.0, 8.0, 0.0, 1.0 / DEG_PER_KM);
        assert!((lat - 48.0).abs() < 1e-3, "lat {}", lat);
        assert!((lng - 8.0).abs() < 1e-6, "lng {}", lng);
        let (lat, lng) = destination_point(47.0, 8.0, 180.0, 1.0 / DEG_PER_KM);
        assert!((lat - 46.0).abs() < 1e-3, "lat {}", lat);
        assert!((lng - 8.0).abs() < 1e-6, "lng {}", lng);
        // East along the equator = one degree of longitude
        let (lat, lng) = destination_point(0.0, 8.0, 90.0, 1.0 / DEG_PER_KM);
        assert!(lat.abs() < 1e-6, "lat {}", lat);
        assert!((lng - 9.0).abs() < 1e-3, "lng {}", lng);
    }

    #[test]
    fn destination_wraps_antimeridian() {
        let (lat, lng) = destination_point(0.0, 179.5, 90.0, 1.0 / DEG_PER_KM);
        assert!(lat.abs() < 1e-6, "lat {}", lat);
        assert!((lng - (-179.5)).abs() < 1e-3, "lng {}", lng);
    }

    /// Flat samples for every bin at `elevation_m`, station included
    fn flat_samples(elevation_m: i16) -> Vec<i16> {
        vec![elevation_m; GROUND_BINS * GROUND_SAMPLES]
    }

    #[test]
    fn flat_terrain_horizon_is_nearest_curvature_dip() {
        let gh = GroundHorizon::from_samples(47.0, 8.0, Some(0.0), flat_samples(500));
        assert_eq!(gh.horizon_angle.len(), GROUND_BINS);
        // Level terrain: every angle is a pure curvature dip, which grows with
        // distance, so the max is the nearest sample (0.5km, ~-0.0017 deg)
        for (i, &angle) in gh.horizon_angle.iter().enumerate() {
            assert!((angle - (-0.0017)).abs() < 0.0005, "bin {} angle {}", i, angle);
        }
        assert!(gh.horizon_distance_km.iter().all(|&d| d <= 1));
    }

    #[test]
    fn ridge_sets_horizon() {
        let mut samples = flat_samples(500);
        // +300m ridge 20km out on bearing 0: same reference values as the
        // receive-horizon angle_curvature_dip test
        samples[(20.0 / GROUND_STEP_KM) as usize] = 800;
        let gh = GroundHorizon::from_samples(47.0, 8.0, Some(0.0), samples);
        assert!((gh.horizon_angle[0] - 0.792).abs() < 0.01, "angle {}", gh.horizon_angle[0]);
        assert_eq!(gh.horizon_distance_km[0], 20);
        // Other bins stay flat
        assert!(gh.horizon_angle[1] < 0.0);
    }

    #[test]
    fn negative_elevations_preserved() {
        // The assembler and i16 encoding must pass negatives through: the DEM
        // service clamps bathymetry to sea level, but genuine below-sea-level
        // land regions (this station is on the Dead Sea shore) keep their
        // negative elevations via the regional floor table
        let gh = GroundHorizon::from_samples(31.5, 35.5, Some(0.0), flat_samples(-400));
        assert!(gh.elevations.iter().all(|&e| e == -400));
        assert!(gh.horizon_angle.iter().all(|&a| a < 0.0));
    }

    #[test]
    fn clamp_i16_bounds() {
        assert_eq!(clamp_i16(8848.4), 8848);
        assert_eq!(clamp_i16(-430.6), -431);
        assert_eq!(clamp_i16(40000.0), i16::MAX);
        assert_eq!(clamp_i16(-40000.0), i16::MIN);
    }

    #[test]
    fn substeps_track_pixel_size() {
        // z7 pixels (~900m at 45N) are coarser than the 500m step: no supersampling
        assert_eq!(substeps(7, 45.0), 1);
        // z11 at 45N: ~54m pixels, 500/54 -> 10 substeps
        assert_eq!(substeps(11, 45.0), 10);
        // z11 at the equator: ~76m pixels -> 7
        assert_eq!(substeps(11, 0.0), 7);
        // Very fine pixels hit the cap
        assert_eq!(substeps(13, 60.0), MAX_SUBSTEPS);
    }

    #[test]
    fn substep_schedule_ends_on_nominal() {
        for n in [1, 4, 10] {
            // Last substep is exactly the bucket's nominal distance...
            assert!((substep_distance_km(20, n, n) - 10.0).abs() < 1e-12);
            assert!((substep_distance_km(1, n, n) - 0.5).abs() < 1e-12);
        }
        // ...and the first lies beyond the previous bucket's nominal, so
        // buckets partition the ray without re-reading shared points
        assert!(substep_distance_km(20, 1, 10) > 9.5);
    }

    #[test]
    fn refine_margin_tracks_relief() {
        // Dead-flat coarse ray: only the floor remains
        let flat = vec![500i16; GROUND_SAMPLES];
        assert_eq!(refine_margin_m(&flat, 100), REFINE_MARGIN_FLOOR_M);
        // A cliff face: 400m of relief within the window scales the margin
        // far past what a fixed crest bound would grant
        let mut cliff = vec![500i16; GROUND_SAMPLES];
        cliff[10] = 900;
        assert_eq!(refine_margin_m(&cliff, 9), 6.0 * 400.0);
        // Outside the +-2 window the cliff is invisible again
        assert_eq!(refine_margin_m(&cliff, 20), REFINE_MARGIN_FLOOR_M);
        // The station point sample (index 0) never joins the window
        let mut spiky_station = vec![500i16; GROUND_SAMPLES];
        spiky_station[0] = 2000;
        assert_eq!(refine_margin_m(&spiky_station, 1), REFINE_MARGIN_FLOOR_M);
    }

    #[test]
    fn candidate_rule_prunes_and_keeps() {
        // Flat ray: skyline is the nearest sample's curvature dip; the far
        // field is provably below it even granting the floor margin
        let skyline = elevation_angle_deg(0.0, 500.0);
        let floor = REFINE_MARGIN_FLOOR_M;
        assert!(!is_candidate(elevation_angle_deg(0.0, 100_000.0), 100.0, floor, skyline));
        // ...while near flat terrain stays a candidate (the angular margin is
        // large close-in) - the rule is deliberately conservative there
        assert!(is_candidate(elevation_angle_deg(0.0, 5_000.0), 5.0, floor, skyline));
        // A 300m crest at 40km that z7 averaging flattened to 80m must stay a
        // candidate: its own ~80m of visible relief scales the margin to
        // cover the hidden height
        let margin = 6.0 * 80.0;
        assert!(is_candidate(elevation_angle_deg(80.0, 40_000.0), 40.0, margin, skyline));
        // Terrain far below a real mountain skyline prunes even with a
        // generous margin
        assert!(!is_candidate(-2.0, 40.0, 250.0, 1.0));
    }

    #[test]
    fn antenna_agl_beacon_window() {
        let default = *crate::config::GROUND_STATION_AGL_M;
        // Sane mast height: beacon 510m over 500m ground
        assert_eq!(beacon_agl_m(Some(510.0), 500.0), Some(10.0));
        assert_eq!(antenna_agl_m(Some(510.0), 500.0), 10.0);
        // Beacon equals ground (site elevation configured): 0m accepted
        assert_eq!(beacon_agl_m(Some(500.0), 500.0), Some(0.0));
        // Below ground or absurdly high (feet-as-metres etc): no beacon AGL,
        // viewpoint falls back to the configured default
        assert_eq!(beacon_agl_m(Some(490.0), 500.0), None);
        assert_eq!(beacon_agl_m(Some(900.0), 500.0), None);
        assert_eq!(beacon_agl_m(None, 500.0), None);
        assert_eq!(antenna_agl_m(Some(490.0), 500.0), default);
        assert_eq!(antenna_agl_m(None, 500.0), default);
    }

    #[test]
    fn agl_lowers_ridge_angle() {
        let mut samples = flat_samples(500);
        samples[(20.0 / GROUND_STEP_KM) as usize] = 800;
        let ground = GroundHorizon::from_samples(47.0, 8.0, Some(0.0), samples.clone());
        let mast = GroundHorizon::from_samples(47.0, 8.0, Some(10.0), samples);
        // A 10m mast viewpoint lowers the 300m/20km ridge angle by
        // atan(300/20km) - atan(290/20km) ~= 0.0287 deg
        let drop = ground.horizon_angle[0] - mast.horizon_angle[0];
        assert!((drop as f64 - 0.0287).abs() < 0.002, "drop {}", drop);
        assert_eq!(mast.horizon_distance_km[0], 20);
    }

    #[test]
    fn write_arrow_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("TEST");
        let out = out.to_str().unwrap();

        let mut samples = flat_samples(500);
        samples[40] = 800;
        let gh = GroundHorizon::from_samples(47.0, 8.0, Some(0.0), samples);
        assert_eq!(write_arrow(out, "TEST", &gh).unwrap(), GROUND_BINS);

        let gz = gz_path(out, "TEST");
        assert!(std::path::Path::new(&gz).exists());
        assert!(!std::path::Path::new(&format!("{}.working", gz)).exists());
        // Uncompressed twin (UNCOMPRESSED_ARROW_FILES defaults on)
        assert!(std::path::Path::new(&raw_path(out, "TEST")).exists());

        let file = std::fs::File::open(&gz).unwrap();
        let reader =
            StreamReader::try_new(GzDecoder::new(std::io::BufReader::new(file)), None).unwrap();
        let schema = reader.schema();
        assert_eq!(schema.metadata().get("stationLat").unwrap(), "47.000000");
        assert_eq!(schema.metadata().get("stationAgl").unwrap(), "0.0");
        assert_eq!(schema.metadata().get("stepKm").unwrap(), "0.5");
        assert_eq!(schema.metadata().get("samples").unwrap(), "241");

        let batches: Vec<_> = reader.map(|b| b.unwrap()).collect();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, GROUND_BINS);

        let batch = &batches[0];
        let bearing = batch.column(0).as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(bearing.value(0), 0.0);
        assert_eq!(bearing.value(GROUND_BINS - 1), 359.5);
        // Just bearing + elevations: the frontend derives skyline and crests
        assert_eq!(batch.num_columns(), 2);
        let lists = batch.column(1).as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        assert_eq!(lists.value_length(), GROUND_SAMPLES as i32);
        let ray0 = lists.value(0);
        let ray0 = ray0.as_any().downcast_ref::<Int16Array>().unwrap();
        assert_eq!(ray0.value(0), 500);
        assert_eq!(ray0.value(40), 800);
    }

    #[test]
    fn default_agl_persists_as_nan() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("TEST");
        let out = out.to_str().unwrap();

        let gh = GroundHorizon::from_samples(47.0, 8.0, None, flat_samples(500));
        assert_eq!(write_arrow(out, "TEST", &gh).unwrap(), GROUND_BINS);

        let file = std::fs::File::open(gz_path(out, "TEST")).unwrap();
        let reader =
            StreamReader::try_new(GzDecoder::new(std::io::BufReader::new(file)), None).unwrap();
        assert_eq!(reader.schema().metadata().get("stationAgl").unwrap(), "NaN");
    }

    #[test]
    fn needs_regeneration_branches() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().to_str().unwrap();
        let pos = Some([47.0, 8.0]);

        // No file yet
        assert!(needs_regeneration(out, "TEST", pos, None, 47.0, 8.0, None));

        std::fs::write(gz_path(out, "TEST"), b"stub").unwrap();
        // File present but no recorded position (e.g. wiped station DB)
        assert!(needs_regeneration(out, "TEST", None, None, 47.0, 8.0, None));
        // Unmoved
        assert!(!needs_regeneration(out, "TEST", pos, None, 47.0, 8.0, None));
        // Within the move threshold (0.2km default): ~100m north
        assert!(!needs_regeneration(out, "TEST", pos, None, 47.0009, 8.0, None));
        // Beyond it: ~1km north
        assert!(needs_regeneration(out, "TEST", pos, None, 47.009, 8.0, None));

        // Unmoved but the beaconed altitude changed materially (the
        // Lennrtsns case: operator reconfigured /A= from 79ft to 138ft)
        assert!(needs_regeneration(out, "TEST", pos, Some(24.1), 47.0, 8.0, Some(42.1)));
        // Stable beacon: no churn
        assert!(!needs_regeneration(out, "TEST", pos, Some(42.1), 47.0, 8.0, Some(42.1)));
    }

    #[test]
    fn beacon_change_window() {
        // Stable states
        assert!(!beacon_changed(None, None));
        assert!(!beacon_changed(Some(42.06), Some(42.06)));
        // Sub-threshold drift (feet rounding) doesn't churn
        assert!(!beacon_changed(Some(42.06), Some(41.0)));
        // Material change, appearance and disappearance all rebuild
        assert!(beacon_changed(Some(42.06), Some(24.08)));
        assert!(beacon_changed(Some(42.06), None));
        assert!(beacon_changed(None, Some(42.06)));
    }

    #[test]
    fn remove_files_removes_both() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().to_str().unwrap();
        std::fs::write(gz_path(out, "TEST"), b"stub").unwrap();
        std::fs::write(raw_path(out, "TEST"), b"stub").unwrap();
        assert_eq!(remove_files(out, "TEST"), 2);
        assert_eq!(remove_files(out, "TEST"), 0);
    }
}
