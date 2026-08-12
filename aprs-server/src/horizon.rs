//! Per-station receive-horizon aggregation.
//!
//! For each bearing bin around a station, tracks the minimum elevation angle
//! at which anything was received (overall and per distance band). Nothing is
//! received below the skyline, so the lowest observed angle is the tightest
//! bound on the horizon occlusion in that direction. The angle is computed
//! from the cell's lowest received MSL altitude, the station's ground
//! elevation and the great-circle distance, with a standard-refraction
//! earth-curvature dip.
//!
//! Layers are merged into RF frequency groups (see `Layer::frequency_group`):
//! the horizon is an antenna/frequency property, not a protocol one. Merging
//! is pure min-selection so feeding the same rows twice never changes angles.

use std::collections::HashMap;

use crate::coverage::header::AccumulatorType;
use crate::coverage::record::ArrowStation;
use crate::layers::{FrequencyGroup, Layer};
use crate::station::{great_circle_distance, StationDetails};

/// Bearing bins per full circle (0.5 degree)
pub const HORIZON_BINS: usize = 720;
/// Cells nearer than this are excluded: close to the station the signal is
/// strong enough to be received well below the true terrain skyline, so
/// min-angle selection there reads an artificially low horizon (and the
/// cell-centre position quantisation alone is ~0.5km at H3 res 8)
pub const HORIZON_MIN_DISTANCE_KM: f64 = 5.0;
/// Cells further than this are excluded entirely. Min-angle selection is
/// extremely sensitive to corrupted positions already in the coverage data
/// (a single garbage cell thousands of km out shows as a huge negative
/// angle - the curvature dip alone is ~-23 degrees at 6800km)
pub const HORIZON_MAX_DISTANCE_KM: f64 = 120.0;
/// Valid elevation-angle window (degrees). Below the floor the point would
/// sit under any plausible terrain even after the curvature dip - a corrupted
/// altitude; above the ceiling the cell is nearly overhead and says nothing
/// about the horizon
pub const HORIZON_MIN_ANGLE_DEG: f32 = -3.0;
pub const HORIZON_MAX_ANGLE_DEG: f32 = 50.0;
/// Distance band upper edges (km). The first band starts at
/// HORIZON_MIN_DISTANCE_KM; the top band ends at the cell-match distance
/// where a 0.5 degree bin arc equals the cell width:
/// 0.8km / (0.5 degree in radians) ~= 91km
pub const HORIZON_DISTANCE_BANDS_KM: [f64; 5] = [10.0, 20.0, 30.0, 50.0, 90.0];
/// Half the across-flats width of an H3 res-8 cell (√3·461m/2). If
/// H3_STATION_CELL_LEVEL is ever made variable this should derive from it
const CELL_HALF_WIDTH_KM: f64 = 0.4;
const EARTH_RADIUS_M: f64 = 6_371_000.0;
/// Standard-refraction effective earth radius factor
const REFRACTION_K: f64 = 4.0 / 3.0;

const BIN_DEG: f64 = 360.0 / HORIZON_BINS as f64;

/// Standard initial great-circle bearing from point 1 to point 2, degrees [0, 360)
fn initial_bearing_deg(lat1: f64, lng1: f64, lat2: f64, lng2: f64) -> f64 {
    let (p1, p2) = (lat1.to_radians(), lat2.to_radians());
    let dl = (lng2 - lng1).to_radians();
    let y = dl.sin() * p2.cos();
    let x = p1.cos() * p2.sin() - p1.sin() * p2.cos() * dl.cos();
    y.atan2(x).to_degrees().rem_euclid(360.0)
}

/// Elevation angle from the station to a point `delta_h_m` above station
/// ground at `distance_m`, corrected for earth curvature with the k=4/3
/// effective radius (standard atmospheric refraction)
fn elevation_angle_deg(delta_h_m: f64, distance_m: f64) -> f64 {
    let geometric = delta_h_m.atan2(distance_m);
    let curvature_dip = distance_m / (2.0 * REFRACTION_K * EARTH_RADIUS_M);
    (geometric - curvature_dip).to_degrees()
}

/// Bin span subtended by a cell's ~0.8km width at `distance_km`, as
/// (first_bin, bin_count) with wraparound; always at least one bin
fn bin_span(bearing_deg: f64, distance_km: f64) -> (u16, u16) {
    let half_deg = CELL_HALF_WIDTH_KM.atan2(distance_km).to_degrees();
    let first = ((bearing_deg - half_deg) / BIN_DEG).floor() as i64;
    let last = ((bearing_deg + half_deg) / BIN_DEG).floor() as i64;
    let count = (last - first + 1).clamp(1, HORIZON_BINS as i64) as u16;
    (first.rem_euclid(HORIZON_BINS as i64) as u16, count)
}

/// First band whose upper edge contains the distance; None beyond the last
fn band_index(distance_km: f64) -> Option<usize> {
    HORIZON_DISTANCE_BANDS_KM.iter().position(|&edge| distance_km <= edge)
}

#[derive(Clone, Copy)]
struct CellGeom {
    first_bin: u16,
    bin_count: u16,
    distance_km: f32,
}

fn compute_geom(station_lat: f64, station_lng: f64, h3: u64) -> Option<CellGeom> {
    let cell = h3o::CellIndex::try_from(h3).ok()?;
    let centre = h3o::LatLng::from(cell);
    let distance_km = great_circle_distance(station_lat, station_lng, centre.lat(), centre.lng());
    if !distance_km.is_finite()
        || distance_km < HORIZON_MIN_DISTANCE_KM
        || distance_km > HORIZON_MAX_DISTANCE_KM
    {
        return None;
    }
    let bearing = initial_bearing_deg(station_lat, station_lng, centre.lat(), centre.lng());
    let (first_bin, bin_count) = bin_span(bearing, distance_km);
    Some(CellGeom { first_bin, bin_count, distance_km: distance_km as f32 })
}

#[derive(Clone, Copy)]
struct BandEntry {
    min_agl: u16,
    angle: f32,
}

#[derive(Clone, Copy)]
struct HorizonBin {
    /// Lowest-angle cell across the whole distance window
    lowest: BandEntry,
    lowest_distance_km: f32,
    /// Furthest contributing cell in this bearing
    max_distance_km: f32,
    /// Lowest-angle cell per distance band
    bands: [Option<BandEntry>; HORIZON_DISTANCE_BANDS_KM.len()],
    /// Cell-arc contributions (a near cell counts once per bin it subtends)
    count: u32,
}

impl HorizonBin {
    fn new(entry: BandEntry, distance_km: f32, band: Option<usize>) -> Self {
        let mut bands = [None; HORIZON_DISTANCE_BANDS_KM.len()];
        if let Some(i) = band {
            bands[i] = Some(entry);
        }
        HorizonBin {
            lowest: entry,
            lowest_distance_km: distance_km,
            max_distance_km: distance_km,
            bands,
            count: 1,
        }
    }
}

pub struct HorizonRow {
    pub frequency: u16,
    /// Degrees, bin start: 0.0, 0.5, ... 359.5
    pub bearing: f32,
    /// Minimum elevation angle across the whole distance window
    pub lowest_angle: f32,
    /// AGL metres of that lowest-angle cell's lowest point
    pub lowest_agl: u16,
    /// km to the lowest-angle cell, rounded
    pub lowest_distance: u16,
    /// km to the furthest contributing cell, rounded
    pub max_distance: u16,
    /// Minimum angle per distance band; None = no cells in band
    pub band_angles: [Option<f32>; HORIZON_DISTANCE_BANDS_KM.len()],
    pub count: u32,
}

pub struct HorizonFile {
    pub acc_type: AccumulatorType,
    pub file_id: String,
    pub rows: Vec<HorizonRow>,
}

type BinKey = (AccumulatorType, String, FrequencyGroup);
type BinArray = Box<[Option<HorizonBin>]>;

/// Accumulates horizon bins for one station across a rollup cycle.
/// Fed once per (layer, destination accumulator) with the full merged cell
/// set; merging is idempotent min-selection so repeated walks of the same
/// destination (hanging-bucket healing) never skew angles.
pub struct HorizonCollector {
    lat: f64,
    lng: f64,
    elevation_m: f64,
    bins: HashMap<BinKey, BinArray>,
    /// Bearing/distance never change for a cell - computed once per rollup
    geom_cache: HashMap<u64, Option<CellGeom>>,
}

impl HorizonCollector {
    /// None when the station has no usable position or no resolved ground
    /// elevation - the horizon is skipped entirely for this cycle
    pub fn from_station(meta: &StationDetails) -> Option<Self> {
        let (lat, lng) = (meta.lat?, meta.lng?);
        let ground_m = meta.elevation?;
        if !lat.is_finite() || !lng.is_finite() || (lat == 0.0 && lng == 0.0) {
            return None;
        }
        // The viewpoint is the antenna, not the ground - same reference as
        // the ground horizon so the two charts stay directly comparable
        let elevation_m =
            ground_m + crate::ground_horizon::antenna_agl_m(meta.beacon_altitude, ground_m);
        Some(HorizonCollector {
            lat,
            lng,
            elevation_m,
            bins: HashMap::new(),
            geom_cache: HashMap::new(),
        })
    }

    /// Feed one layer's complete destination-accumulator cell set. No-op for
    /// Day/Current accumulators and for layers with no frequency group.
    pub fn feed(&mut self, acc_type: AccumulatorType, file_id: &str, layer: Layer, rows: &[ArrowStation]) {
        let Some(freq) = layer.frequency_group() else {
            return;
        };
        if !matches!(
            acc_type,
            AccumulatorType::Month | AccumulatorType::Year | AccumulatorType::YearNz
        ) {
            return;
        }

        let (lat, lng, elevation_m) = (self.lat, self.lng, self.elevation_m);
        let geom_cache = &mut self.geom_cache;
        let bins = self
            .bins
            .entry((acc_type, file_id.to_string(), freq))
            .or_insert_with(|| vec![None; HORIZON_BINS].into_boxed_slice());

        for row in rows {
            let h3 = ((row.h3hi as u64) << 32) | row.h3lo as u64;
            let geom = *geom_cache
                .entry(h3)
                .or_insert_with(|| compute_geom(lat, lng, h3));
            let Some(geom) = geom else {
                continue;
            };

            let angle = elevation_angle_deg(
                row.min_alt as f64 - elevation_m,
                geom.distance_km as f64 * 1000.0,
            ) as f32;
            if !(HORIZON_MIN_ANGLE_DEG..=HORIZON_MAX_ANGLE_DEG).contains(&angle) {
                continue;
            }
            let entry = BandEntry { min_agl: row.min_agl, angle };
            let band = band_index(geom.distance_km as f64);

            for i in 0..geom.bin_count as usize {
                let bin = &mut bins[(geom.first_bin as usize + i) % HORIZON_BINS];
                match bin {
                    None => *bin = Some(HorizonBin::new(entry, geom.distance_km, band)),
                    Some(b) => {
                        if entry.angle < b.lowest.angle {
                            b.lowest = entry;
                            b.lowest_distance_km = geom.distance_km;
                        }
                        b.max_distance_km = b.max_distance_km.max(geom.distance_km);
                        if let Some(bi) = band {
                            match &mut b.bands[bi] {
                                Some(e) if entry.angle >= e.angle => {}
                                slot => *slot = Some(entry),
                            }
                        }
                        b.count += 1;
                    }
                }
            }
        }
    }

    /// One file per (accumulator, file id) holding both frequency groups,
    /// rows sorted by (frequency, bearing), only non-empty bins
    pub fn build_files(&self) -> Vec<HorizonFile> {
        let mut file_keys: Vec<(AccumulatorType, &String)> =
            self.bins.keys().map(|(a, f, _)| (*a, f)).collect();
        file_keys.sort_by_key(|(a, f)| (*a as u8, f.as_str().to_string()));
        file_keys.dedup();

        let mut out = Vec::new();
        for (acc_type, file_id) in file_keys {
            let mut rows = Vec::new();
            for freq in FrequencyGroup::ALL {
                let Some(bins) = self.bins.get(&(acc_type, file_id.clone(), *freq)) else {
                    continue;
                };
                for (idx, bin) in bins.iter().enumerate() {
                    let Some(b) = bin else {
                        continue;
                    };
                    rows.push(HorizonRow {
                        frequency: freq.mhz(),
                        bearing: (idx as f64 * BIN_DEG) as f32,
                        lowest_angle: b.lowest.angle,
                        lowest_agl: b.lowest.min_agl,
                        lowest_distance: b.lowest_distance_km.round() as u16,
                        max_distance: b.max_distance_km.round() as u16,
                        band_angles: b.bands.map(|e| e.map(|e| e.angle)),
                        count: b.count,
                    });
                }
            }
            if !rows.is_empty() {
                out.push(HorizonFile { acc_type, file_id: file_id.clone(), rows });
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::StationName;

    const STATION_LAT: f64 = 47.0;
    const STATION_LNG: f64 = 8.0;
    const STATION_ELEVATION: f64 = 500.0;
    /// Degrees of latitude per km, approximately
    const DEG_PER_KM: f64 = 1.0 / 111.195;

    fn test_station() -> StationDetails {
        StationDetails {
            station: StationName("TEST".to_string()),
            lat: Some(STATION_LAT),
            lng: Some(STATION_LNG),
            elevation: Some(STATION_ELEVATION),
            // Antenna at ground level so angle expectations stay in ground
            // terms; without this the default AGL (10m) shifts them all
            beacon_altitude: Some(STATION_ELEVATION),
            ..Default::default()
        }
    }

    /// ArrowStation row for the res-8 cell containing (lat, lng)
    fn cell_row(lat: f64, lng: f64, min_alt: u16, min_agl: u16) -> ArrowStation {
        let cell = h3o::LatLng::new(lat, lng).unwrap().to_cell(h3o::Resolution::Eight);
        let h3 = u64::from(cell);
        ArrowStation {
            h3lo: (h3 & 0xffff_ffff) as u32,
            h3hi: (h3 >> 32) as u32,
            min_agl,
            min_alt,
            min_alt_sig: 0,
            max_sig: 0,
            avg_sig: 0,
            avg_crc: 0,
            count: 1,
            avg_gap: 0,
        }
    }

    /// A row roughly `distance_km` north of the test station
    fn row_north(distance_km: f64, min_alt: u16, min_agl: u16) -> ArrowStation {
        cell_row(STATION_LAT + distance_km * DEG_PER_KM, STATION_LNG, min_alt, min_agl)
    }

    #[test]
    fn bearing_cardinals() {
        assert!(initial_bearing_deg(47.0, 8.0, 48.0, 8.0).abs() < 1e-9);
        assert!((initial_bearing_deg(47.0, 8.0, 46.0, 8.0) - 180.0).abs() < 1e-9);
        // East/west along a parallel converge slightly off 90/270
        let east = initial_bearing_deg(47.0, 8.0, 47.0, 9.0);
        assert!((89.0..90.0).contains(&east), "east bearing {}", east);
        let west = initial_bearing_deg(47.0, 8.0, 47.0, 7.0);
        assert!((270.0..271.0).contains(&west), "west bearing {}", west);
    }

    #[test]
    fn angle_curvature_dip() {
        // Flat terrain at 10km: pure curvature dip of d/(2kR) radians
        let angle = elevation_angle_deg(0.0, 10_000.0);
        assert!((angle - (-0.0337)).abs() < 0.001, "angle {}", angle);
        // 300m above station ground at 20km, minus the dip
        let angle = elevation_angle_deg(300.0, 20_000.0);
        assert!((angle - 0.792).abs() < 0.01, "angle {}", angle);
    }

    #[test]
    fn bin_span_widths() {
        // 5km: cell subtends ~9.1 degrees -> ~18 half-degree bins
        let (_, count) = bin_span(180.0, 5.0);
        assert!((17..=20).contains(&count), "count {}", count);
        // 100km: ~0.46 degrees -> 1-2 bins
        let (_, count) = bin_span(180.0, 100.0);
        assert!((1..=2).contains(&count), "count {}", count);
    }

    #[test]
    fn bin_span_wraps_north() {
        let (first, count) = bin_span(0.1, 5.0);
        assert!(count >= 17);
        // Span starts west of north and wraps through bin 0
        assert!(first as usize > HORIZON_BINS / 2, "first {}", first);
        assert!((first as usize + count as usize) % HORIZON_BINS < HORIZON_BINS / 2);
    }

    #[test]
    fn band_boundaries() {
        assert_eq!(band_index(7.0), Some(0));
        assert_eq!(band_index(10.0), Some(0));
        assert_eq!(band_index(10.1), Some(1));
        assert_eq!(band_index(90.0), Some(4));
        assert_eq!(band_index(90.1), None);
    }

    #[test]
    fn from_station_requires_position_and_elevation() {
        assert!(HorizonCollector::from_station(&test_station()).is_some());
        let mut s = test_station();
        s.elevation = None;
        assert!(HorizonCollector::from_station(&s).is_none());
        let mut s = test_station();
        s.lat = None;
        assert!(HorizonCollector::from_station(&s).is_none());
        let mut s = test_station();
        s.lat = Some(0.0);
        s.lng = Some(0.0);
        assert!(HorizonCollector::from_station(&s).is_none());
        let mut s = test_station();
        s.lat = Some(f64::NAN);
        assert!(HorizonCollector::from_station(&s).is_none());
    }

    #[test]
    fn antenna_viewpoint_raises_reference() {
        // Sane beaconed altitude: antenna 10m over ground lifts the viewpoint
        let mut s = test_station();
        s.beacon_altitude = Some(STATION_ELEVATION + 10.0);
        let c = HorizonCollector::from_station(&s).unwrap();
        assert_eq!(c.elevation_m, STATION_ELEVATION + 10.0);
        // Insane altitude (feet-as-metres etc) falls back to the default AGL
        let mut s = test_station();
        s.beacon_altitude = Some(STATION_ELEVATION + 5000.0);
        let c = HorizonCollector::from_station(&s).unwrap();
        assert_eq!(c.elevation_m, STATION_ELEVATION + *crate::config::GROUND_STATION_AGL_M);
    }

    #[test]
    fn near_cells_excluded() {
        let mut c = HorizonCollector::from_station(&test_station()).unwrap();
        c.feed(AccumulatorType::Month, "2026-07", Layer::Combined, &[row_north(1.0, 600, 100)]);
        // Under HORIZON_MIN_DISTANCE_KM the strong near-field signal is
        // received below the true skyline - excluded despite being plausible
        c.feed(AccumulatorType::Month, "2026-07", Layer::Combined, &[row_north(4.0, 700, 200)]);
        assert!(c.build_files().is_empty());
    }

    #[test]
    fn far_cells_excluded() {
        let mut c = HorizonCollector::from_station(&test_station()).unwrap();
        // Beyond HORIZON_MAX_DISTANCE_KM: a corrupted-position cell must not
        // reach the lowest/any-distance selection
        c.feed(AccumulatorType::Month, "2026-07", Layer::Combined, &[row_north(150.0, 600, 4)]);
        assert!(c.build_files().is_empty());
        // Just inside the window still counts
        c.feed(AccumulatorType::Month, "2026-07", Layer::Combined, &[row_north(110.0, 3000, 500)]);
        let files = c.build_files();
        assert_eq!(files.len(), 1);
        assert!(files[0].rows.iter().all(|r| r.max_distance <= 120));
    }

    #[test]
    fn day_current_and_unmapped_layers_ignored() {
        let mut c = HorizonCollector::from_station(&test_station()).unwrap();
        let rows = [row_north(20.0, 800, 100)];
        c.feed(AccumulatorType::Day, "2026-07-20", Layer::Combined, &rows);
        c.feed(AccumulatorType::Current, "", Layer::Combined, &rows);
        c.feed(AccumulatorType::Month, "2026-07", Layer::Flarm, &rows);
        c.feed(AccumulatorType::Month, "2026-07", Layer::Ogntrk, &rows);
        c.feed(AccumulatorType::Month, "2026-07", Layer::Safesky, &rows);
        assert!(c.build_files().is_empty());
    }

    #[test]
    fn frequency_groups_split_and_merge() {
        let mut c = HorizonCollector::from_station(&test_station()).unwrap();
        // combined + adsl merge into 868; adsb is separate 1090
        c.feed(AccumulatorType::Month, "2026-07", Layer::Combined, &[row_north(20.0, 800, 100)]);
        c.feed(AccumulatorType::Month, "2026-07", Layer::Adsl, &[row_north(20.0, 700, 50)]);
        c.feed(AccumulatorType::Month, "2026-07", Layer::Adsb, &[row_north(20.0, 900, 200)]);

        let files = c.build_files();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].acc_type, AccumulatorType::Month);
        assert_eq!(files[0].file_id, "2026-07");

        let f868: Vec<_> = files[0].rows.iter().filter(|r| r.frequency == 868).collect();
        let f1090: Vec<_> = files[0].rows.iter().filter(|r| r.frequency == 1090).collect();
        assert!(!f868.is_empty() && !f1090.is_empty());
        // The adsl row has the lower altitude, hence lower angle: wins the 868 bins
        assert!(f868.iter().all(|r| r.lowest_agl == 50));
        assert!(f1090.iter().all(|r| r.lowest_agl == 200));

        // ~300m above station ground at ~20km, minus curvature dip
        let angle = f868[0].lowest_angle;
        assert!((0.5..1.1).contains(&angle), "angle {}", angle);
    }

    #[test]
    fn lowest_angle_wins_over_lower_agl() {
        let mut c = HorizonCollector::from_station(&test_station()).unwrap();
        // Same bearing: cell hugging terrain nearby (agl 50, steep ~4 degrees)
        // vs a barely-above-station cell further out (agl 300, ~0.15 degrees)
        c.feed(
            AccumulatorType::Year,
            "2026",
            Layer::Combined,
            &[row_north(20.0, 2000, 50), row_north(40.0, 700, 300)],
        );
        let files = c.build_files();
        let row = files[0].rows.iter().find(|r| (r.bearing - 0.0).abs() < 0.01).unwrap();
        assert_eq!(row.lowest_agl, 300);
        assert_eq!(row.lowest_distance, 40);
        assert_eq!(row.max_distance, 40);
        // Both cells land in different bands: (10,20] and (30,50]
        let b20 = row.band_angles[1].unwrap();
        let b50 = row.band_angles[3].unwrap();
        assert!(b50 < b20, "b50 {} b20 {}", b50, b20);
        // Any-distance is the lower envelope of the bands
        assert_eq!(row.lowest_angle, b50);
        assert!(row.band_angles[0].is_none());
    }

    #[test]
    fn angle_window_excluded() {
        let mut c = HorizonCollector::from_station(&test_station()).unwrap();
        // Below the floor: claims 0m MSL, 500m under the station at 8km (~-3.6 deg)
        c.feed(AccumulatorType::Month, "2026-07", Layer::Combined, &[row_north(8.0, 0, 0)]);
        // Above the ceiling: ~9500m above the station at 6km (~57 deg even
        // after cell-centre snapping stretches the distance)
        c.feed(AccumulatorType::Month, "2026-07", Layer::Combined, &[row_north(6.0, 10000, 100)]);
        assert!(c.build_files().is_empty());
        // A normal cell still passes
        c.feed(AccumulatorType::Month, "2026-07", Layer::Combined, &[row_north(20.0, 800, 100)]);
        assert!(!c.build_files().is_empty());
    }

    #[test]
    fn refeed_is_idempotent_for_angles() {
        let mut c = HorizonCollector::from_station(&test_station()).unwrap();
        let rows = [row_north(20.0, 800, 100)];
        c.feed(AccumulatorType::Month, "2026-07", Layer::Combined, &rows);
        let first: Vec<(f32, f32)> = c.build_files()[0]
            .rows
            .iter()
            .map(|r| (r.bearing, r.lowest_angle))
            .collect();
        c.feed(AccumulatorType::Month, "2026-07", Layer::Combined, &rows);
        let second: Vec<(f32, f32)> = c.build_files()[0]
            .rows
            .iter()
            .map(|r| (r.bearing, r.lowest_angle))
            .collect();
        assert_eq!(first, second);
    }

    #[test]
    fn files_split_by_accumulator() {
        let mut c = HorizonCollector::from_station(&test_station()).unwrap();
        let rows = [row_north(20.0, 800, 100)];
        c.feed(AccumulatorType::Month, "2026-07", Layer::Combined, &rows);
        c.feed(AccumulatorType::Year, "2026", Layer::Combined, &rows);
        c.feed(AccumulatorType::YearNz, "2025nz", Layer::Combined, &rows);
        let files = c.build_files();
        assert_eq!(files.len(), 3);
        assert_eq!(files[0].acc_type, AccumulatorType::Month);
        assert_eq!(files[1].acc_type, AccumulatorType::Year);
        assert_eq!(files[2].acc_type, AccumulatorType::YearNz);
    }

    #[test]
    fn arc_spreading_fills_adjacent_bins() {
        let mut c = HorizonCollector::from_station(&test_station()).unwrap();
        // A single cell at 6km (safely inside the 5km minimum - the cell
        // centre lands slightly off the nominal distance) should fill ~15
        // bins around north
        c.feed(AccumulatorType::Month, "2026-07", Layer::Combined, &[row_north(6.0, 700, 100)]);
        let files = c.build_files();
        let n = files[0].rows.len();
        assert!((12..=19).contains(&n), "rows {}", n);
    }
}
