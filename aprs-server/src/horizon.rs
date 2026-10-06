//! Per-station receive-horizon aggregation.
//!
//! For each bearing bin around a station, tracks the monotone envelope of the
//! lowest elevation angle anything was received at, as a Pareto frontier of
//! (distance, angle) breakpoints: a cell survives iff no other cell is both
//! farther and lower-angle. Reception proven at an angle holds for every
//! closer distance too (same angle, stronger signal), so the frontier reads
//! as a staircase "proven down to angle a out to distance d" that only rises
//! with distance. The angle is computed from the cell's lowest received MSL
//! altitude, the station's antenna viewpoint and the great-circle distance,
//! with a standard-refraction earth-curvature dip.
//!
//! Cells claiming reception materially below the terrain skyline (from the
//! station's ground-horizon file, when present) are rejected: nothing is
//! received below the skyline, so such cells are corrupted positions or
//! altitudes, and the min-selection frontier is extremely sensitive to them.
//! Single-packet cells are rejected for the same reason - one corrupt packet
//! must not set a breakpoint.
//!
//! Layers are merged into RF frequency groups (see `Layer::frequency_group`):
//! the horizon is an antenna/frequency property, not a protocol one. Merging
//! is pure frontier insertion so feeding the same rows twice never changes
//! the envelope.

use std::collections::HashMap;

use crate::coverage::header::AccumulatorType;
use crate::coverage::record::ArrowStation;
use crate::ground_horizon::TerrainSkyline;
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
/// about the horizon. Backstop only - the skyline filter below is the real
/// terrain-aware check when a ground-horizon file exists
pub const HORIZON_MIN_ANGLE_DEG: f32 = -3.0;
pub const HORIZON_MAX_ANGLE_DEG: f32 = 50.0;
/// Breakpoints kept per bin after thinning (schema columns bp0..bp4)
pub const HORIZON_MAX_BREAKPOINTS: usize = 5;
/// Angle steps smaller than this are thinned out of the stored frontier -
/// thinning only ever raises the envelope, never lowers it
pub const HORIZON_EPSILON_DEG: f32 = 0.05;
/// How far below the terrain skyline a cell's angle may sit before it is
/// rejected as physically implausible (diffraction and DEM noise allowance).
/// Keep in sync with SKYLINE_TOLERANCE_DEG in horizondata.ts - the frontend
/// mirrors this filter for files written before a ground horizon existed
pub const SKYLINE_TOLERANCE_DEG: f32 = 0.25;
/// Cells with fewer packets than this never feed the envelope: a single
/// corrupt packet alone must not set a breakpoint
pub const HORIZON_MIN_CELL_PACKETS: u32 = 2;
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

/// One envelope breakpoint: the lowest angle proven at or beyond this
/// distance is this angle
#[derive(Clone, Copy, PartialEq, Debug)]
pub struct FrontierPoint {
    pub distance_km: f32,
    pub angle: f32,
}

/// Pareto frontier of (distance, angle) for one bearing bin, sorted by
/// distance; the sort invariant makes angles ascend too (any out-of-order
/// pair would be a domination)
#[derive(Clone)]
struct HorizonBin {
    frontier: Vec<FrontierPoint>,
    /// Accepted cell-arc contributions (a near cell counts once per bin it
    /// subtends; rejected cells never count)
    count: u32,
}

impl HorizonBin {
    fn new(p: FrontierPoint) -> Self {
        HorizonBin { frontier: vec![p], count: 1 }
    }

    /// Insert preserving the frontier: discard the point if some kept point
    /// is at least as far with at most its angle; otherwise insert it and
    /// drop the points it dominates. Set-determined, so feed order and
    /// re-feeding cannot change the result
    fn insert(&mut self, p: FrontierPoint) {
        // First point at >= p's distance: by the invariant it carries the
        // minimum angle of that whole suffix, so one probe decides domination
        let idx = self.frontier.partition_point(|q| q.distance_km < p.distance_km);
        if idx < self.frontier.len() && self.frontier[idx].angle <= p.angle {
            return;
        }
        // Points p dominates: nearer-with-higher-angle is the angle >= p tail
        // of the prefix, plus an equal-distance point (its angle must be
        // higher or the probe above would have discarded p)
        let k = self.frontier[..idx].partition_point(|q| q.angle < p.angle);
        let end = if idx < self.frontier.len() && self.frontier[idx].distance_km == p.distance_km {
            idx + 1
        } else {
            idx
        };
        self.frontier.splice(k..end, [p]);
    }
}

/// Thin a frontier for storage: drop steps smaller than HORIZON_EPSILON_DEG,
/// then enforce the breakpoint cap by dropping the interior points whose
/// removal raises the envelope least (the smallest step to the next kept
/// angle). The first point (the overall lowest angle) and the last (the
/// furthest proven distance) always survive. Thinning is conservative: every
/// removal makes the stored envelope equal or higher than the exact one
fn thin_frontier(frontier: &[FrontierPoint]) -> Vec<FrontierPoint> {
    let Some((&first, rest)) = frontier.split_first() else {
        return Vec::new();
    };
    let mut kept = vec![first];
    if let Some((&last, interior)) = rest.split_last() {
        for &p in interior {
            if p.angle >= kept.last().unwrap().angle + HORIZON_EPSILON_DEG {
                kept.push(p);
            }
        }
        kept.push(last);
    }
    while kept.len() > HORIZON_MAX_BREAKPOINTS {
        let mut drop_idx = 1;
        let mut smallest = f32::INFINITY;
        for i in 1..kept.len() - 1 {
            let step = kept[i + 1].angle - kept[i].angle;
            if step < smallest {
                smallest = step;
                drop_idx = i;
            }
        }
        kept.remove(drop_idx);
    }
    kept
}

pub struct HorizonRow {
    pub frequency: u16,
    /// Degrees, bin start: 0.0, 0.5, ... 359.5
    pub bearing: f32,
    /// Thinned envelope, ascending in both distance and angle;
    /// breakpoints[0] is the lowest proven angle, the last one's distance the
    /// furthest accepted cell
    pub breakpoints: Vec<FrontierPoint>,
    pub count: u32,
}

pub struct HorizonFile {
    pub acc_type: AccumulatorType,
    pub file_id: String,
    pub rows: Vec<HorizonRow>,
}

/// Everything the arrow writer needs for the schema metadata
pub struct HorizonMeta {
    pub lat: f64,
    pub lng: f64,
    /// Station ground m MSL
    pub ground_m: f64,
    /// Beacon-derived antenna AGL; None = the configured default was assumed
    /// (persisted as stationAgl "NaN", matching the ground-horizon file)
    pub beacon_agl: Option<f64>,
    /// Whether the below-skyline filter ran for this cycle
    pub skyline_filtered: bool,
}

type BinKey = (AccumulatorType, String, FrequencyGroup);
type BinArray = Box<[Option<HorizonBin>]>;

/// Accumulates horizon bins for one station across a rollup cycle.
/// Fed once per (layer, destination accumulator) with the full merged cell
/// set; merging is idempotent frontier insertion so repeated walks of the
/// same destination (hanging-bucket healing) never skew the envelope.
pub struct HorizonCollector {
    lat: f64,
    lng: f64,
    /// Station ground m MSL
    ground_m: f64,
    /// Beacon-derived antenna AGL (see HorizonMeta)
    beacon_agl: Option<f64>,
    /// Antenna viewpoint m MSL: ground + beacon AGL or the configured default
    elevation_m: f64,
    /// Terrain skyline for the below-skyline cell filter; None = no filtering
    /// (no ground-horizon file yet, or it was generated elsewhere)
    skyline: Option<TerrainSkyline>,
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
        let beacon_agl = crate::ground_horizon::beacon_agl_m(meta.beacon_altitude, ground_m);
        let elevation_m = ground_m + beacon_agl.unwrap_or(*crate::config::GROUND_STATION_AGL_M);
        Some(HorizonCollector {
            lat,
            lng,
            ground_m,
            beacon_agl,
            elevation_m,
            skyline: None,
            bins: HashMap::new(),
            geom_cache: HashMap::new(),
        })
    }

    /// Attach the terrain skyline read from the station's ground-horizon file
    /// (ground_horizon::read_skyline with this collector's viewpoint)
    pub fn with_skyline(mut self, skyline: Option<TerrainSkyline>) -> Self {
        self.skyline = skyline;
        self
    }

    /// Antenna viewpoint m MSL - the reference read_skyline must be given
    pub fn viewpoint_m(&self) -> f64 {
        self.elevation_m
    }

    pub fn meta(&self) -> HorizonMeta {
        HorizonMeta {
            lat: self.lat,
            lng: self.lng,
            ground_m: self.ground_m,
            beacon_agl: self.beacon_agl,
            skyline_filtered: self.skyline.is_some(),
        }
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
        let skyline = self.skyline.as_ref();
        let geom_cache = &mut self.geom_cache;
        let bins = self
            .bins
            .entry((acc_type, file_id.to_string(), freq))
            .or_insert_with(|| vec![None; HORIZON_BINS].into_boxed_slice());

        for row in rows {
            if row.count < HORIZON_MIN_CELL_PACKETS {
                continue;
            }
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
            let point = FrontierPoint { distance_km: geom.distance_km, angle };

            for i in 0..geom.bin_count as usize {
                let bin_idx = (geom.first_bin as usize + i) % HORIZON_BINS;
                // Reception materially below this bin's skyline is a corrupt
                // position/altitude, not evidence (NaN skyline: no rejection)
                if let Some(sky) = skyline {
                    if angle < sky.angle_at(bin_idx, geom.distance_km as f64) - SKYLINE_TOLERANCE_DEG {
                        continue;
                    }
                }
                let bin = &mut bins[bin_idx];
                match bin {
                    None => *bin = Some(HorizonBin::new(point)),
                    Some(b) => {
                        b.insert(point);
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
                        breakpoints: thin_frontier(&b.frontier),
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
            // terms; without this the default AGL shifts them all
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
            // Two packets: single-packet cells are deliberately excluded
            count: 2,
            avg_gap: 0,
        }
    }

    /// A row roughly `distance_km` north of the test station
    fn row_north(distance_km: f64, min_alt: u16, min_agl: u16) -> ArrowStation {
        cell_row(STATION_LAT + distance_km * DEG_PER_KM, STATION_LNG, min_alt, min_agl)
    }

    fn bp(distance_km: f32, angle: f32) -> FrontierPoint {
        FrontierPoint { distance_km, angle }
    }

    /// The northern (bearing 0.0) row of the first built file
    fn north_row(c: &HorizonCollector) -> HorizonRow {
        let files = c.build_files();
        let row = files[0].rows.iter().find(|r| (r.bearing - 0.0).abs() < 0.01).unwrap();
        HorizonRow {
            frequency: row.frequency,
            bearing: row.bearing,
            breakpoints: row.breakpoints.clone(),
            count: row.count,
        }
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
    fn frontier_insert_keeps_pareto_staircase() {
        let mut b = HorizonBin::new(bp(40.0, 1.0));
        // Nearer and lower: both survive (staircase rises with distance)
        b.insert(bp(20.0, 0.5));
        assert_eq!(b.frontier, vec![bp(20.0, 0.5), bp(40.0, 1.0)]);
        // Farther and lower: dominates both
        b.insert(bp(60.0, 0.2));
        assert_eq!(b.frontier, vec![bp(60.0, 0.2)]);
        // Nearer and higher: dominated, discarded
        b.insert(bp(30.0, 0.4));
        assert_eq!(b.frontier, vec![bp(60.0, 0.2)]);
        // Farther and higher: survives as a new outer step
        b.insert(bp(90.0, 1.5));
        assert_eq!(b.frontier, vec![bp(60.0, 0.2), bp(90.0, 1.5)]);
        // Interior point between the steps survives
        b.insert(bp(75.0, 0.8));
        assert_eq!(b.frontier, vec![bp(60.0, 0.2), bp(75.0, 0.8), bp(90.0, 1.5)]);
        // Same distance, lower angle: replaces
        b.insert(bp(75.0, 0.5));
        assert_eq!(b.frontier, vec![bp(60.0, 0.2), bp(75.0, 0.5), bp(90.0, 1.5)]);
        // Same distance, higher angle: discarded
        b.insert(bp(75.0, 0.9));
        assert_eq!(b.frontier, vec![bp(60.0, 0.2), bp(75.0, 0.5), bp(90.0, 1.5)]);
        // Exact duplicate: no change (re-feed idempotence)
        b.insert(bp(75.0, 0.5));
        assert_eq!(b.frontier, vec![bp(60.0, 0.2), bp(75.0, 0.5), bp(90.0, 1.5)]);
    }

    #[test]
    fn frontier_insert_order_independent() {
        let points = [bp(40.0, 1.0), bp(20.0, 0.5), bp(60.0, 0.2), bp(90.0, 1.5), bp(75.0, 0.8), bp(30.0, 0.4)];
        let mut forward = HorizonBin::new(points[0]);
        for &p in &points[1..] {
            forward.insert(p);
        }
        let mut backward = HorizonBin::new(*points.last().unwrap());
        for &p in points[..points.len() - 1].iter().rev() {
            backward.insert(p);
        }
        assert_eq!(forward.frontier, backward.frontier);
    }

    #[test]
    fn thin_frontier_drops_small_steps_keeps_endpoints() {
        // Sub-epsilon chain (total rise 0.036 < epsilon): every interior step
        // thins away, the first (lowest) and last (furthest) always survive
        let dense: Vec<_> = (0..10).map(|i| bp(10.0 + i as f32 * 5.0, 0.1 + i as f32 * 0.004)).collect();
        let thinned = thin_frontier(&dense);
        assert_eq!(thinned, vec![dense[0], dense[9]]);

        // Large steps all survive up to the cap
        let coarse: Vec<_> = (0..5).map(|i| bp(10.0 + i as f32 * 20.0, 0.1 + i as f32 * 0.5)).collect();
        assert_eq!(thin_frontier(&coarse), coarse);

        // Over the cap: the smallest interior step goes first
        let six = vec![bp(10.0, 0.1), bp(20.0, 0.3), bp(30.0, 0.42), bp(40.0, 1.0), bp(50.0, 1.7), bp(60.0, 2.5)];
        let thinned = thin_frontier(&six);
        assert_eq!(thinned.len(), HORIZON_MAX_BREAKPOINTS);
        assert_eq!(thinned[0], six[0]);
        assert_eq!(*thinned.last().unwrap(), six[5]);
        // Removing 20km/0.3 raises its span only to 0.42 - the cheapest loss
        assert!(!thinned.contains(&six[1]));

        // Degenerate sizes
        assert!(thin_frontier(&[]).is_empty());
        assert_eq!(thin_frontier(&[bp(10.0, 0.1)]), vec![bp(10.0, 0.1)]);
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
        assert_eq!(c.viewpoint_m(), STATION_ELEVATION + 10.0);
        assert_eq!(c.meta().beacon_agl, Some(10.0));
        // Insane altitude (feet-as-metres etc) falls back to the default AGL
        let mut s = test_station();
        s.beacon_altitude = Some(STATION_ELEVATION + 5000.0);
        let c = HorizonCollector::from_station(&s).unwrap();
        assert_eq!(c.viewpoint_m(), STATION_ELEVATION + *crate::config::GROUND_STATION_AGL_M);
        assert_eq!(c.meta().beacon_agl, None);
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
        // reach the envelope
        c.feed(AccumulatorType::Month, "2026-07", Layer::Combined, &[row_north(150.0, 600, 4)]);
        assert!(c.build_files().is_empty());
        // Just inside the window still counts
        c.feed(AccumulatorType::Month, "2026-07", Layer::Combined, &[row_north(110.0, 3000, 500)]);
        let files = c.build_files();
        assert_eq!(files.len(), 1);
        assert!(files[0]
            .rows
            .iter()
            .all(|r| r.breakpoints.last().unwrap().distance_km <= 120.0));
    }

    #[test]
    fn single_packet_cells_excluded() {
        let mut c = HorizonCollector::from_station(&test_station()).unwrap();
        let mut row = row_north(20.0, 800, 100);
        row.count = 1;
        c.feed(AccumulatorType::Month, "2026-07", Layer::Combined, &[row]);
        assert!(c.build_files().is_empty());
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
        // Same cell, so one breakpoint per bin: the adsl row has the lower
        // altitude, hence the lower angle wins the 868 frontier
        assert!(f868.iter().all(|r| r.breakpoints.len() == 1));
        assert!(f868
            .iter()
            .zip(f1090.iter())
            .all(|(a, b)| a.breakpoints[0].angle < b.breakpoints[0].angle));

        // ~200m above station ground at ~20km, minus curvature dip
        let angle = f868[0].breakpoints[0].angle;
        assert!((0.3..0.9).contains(&angle), "angle {}", angle);
    }

    #[test]
    fn farther_and_lower_dominates() {
        let mut c = HorizonCollector::from_station(&test_station()).unwrap();
        // Same bearing: cell hugging terrain nearby (agl 50, steep ~4 degrees)
        // vs a barely-above-station cell further out (agl 300, ~0.15 degrees).
        // The far low cell proves everything the near steep one did - one
        // breakpoint survives
        c.feed(
            AccumulatorType::Year,
            "2026",
            Layer::Combined,
            &[row_north(20.0, 2000, 50), row_north(40.0, 700, 300)],
        );
        let row = north_row(&c);
        assert_eq!(row.breakpoints.len(), 1, "breakpoints {:?}", row.breakpoints);
        assert!((row.breakpoints[0].distance_km - 40.0).abs() < 1.0);
        assert!(row.breakpoints[0].angle < 0.3);
    }

    #[test]
    fn rising_envelope_keeps_both_steps() {
        let mut c = HorizonCollector::from_station(&test_station()).unwrap();
        // Low nearby, only high far out (the airliner shape): both survive as
        // an ascending staircase
        c.feed(
            AccumulatorType::Year,
            "2026",
            Layer::Combined,
            &[row_north(30.0, 700, 300), row_north(80.0, 3000, 2500)],
        );
        let row = north_row(&c);
        assert_eq!(row.breakpoints.len(), 2, "breakpoints {:?}", row.breakpoints);
        assert!(row.breakpoints[0].distance_km < row.breakpoints[1].distance_km);
        assert!(row.breakpoints[0].angle < row.breakpoints[1].angle);
        assert!((row.breakpoints[1].distance_km - 80.0).abs() < 1.5);
    }

    #[test]
    fn skyline_filter_rejects_below_skyline_cells() {
        use crate::ground_horizon::{TerrainSkyline, GROUND_BINS, GROUND_SAMPLES};
        // Flat 500m terrain with an 800m ridge at 20km on every bearing:
        // running-max skyline is ~0.79 deg at and beyond 20km
        let mut samples = vec![500i16; GROUND_BINS * GROUND_SAMPLES];
        for bin in 0..GROUND_BINS {
            samples[bin * GROUND_SAMPLES + 40] = 800;
        }
        let sky = TerrainSkyline::from_elevations(&samples, STATION_ELEVATION);

        // A cell at 30km claiming ~0.3 deg - more than the tolerance below
        // the 0.79 skyline - is rejected as corrupt
        let mut c = HorizonCollector::from_station(&test_station()).unwrap().with_skyline(Some(sky));
        c.feed(AccumulatorType::Year, "2026", Layer::Combined, &[row_north(30.0, 710, 200)]);
        assert!(c.build_files().is_empty());

        // ~0.7 deg at 30km is within the tolerance below the skyline: kept
        c.feed(AccumulatorType::Year, "2026", Layer::Combined, &[row_north(30.0, 920, 400)]);
        let row = north_row(&c);
        assert_eq!(row.breakpoints.len(), 1);
        assert!((row.breakpoints[0].angle - 0.7).abs() < 0.1, "angle {}", row.breakpoints[0].angle);

        // Without a skyline the same 0.3 deg cell is accepted (backstop only)
        let mut c = HorizonCollector::from_station(&test_station()).unwrap();
        c.feed(AccumulatorType::Year, "2026", Layer::Combined, &[row_north(30.0, 710, 200)]);
        assert!(!c.build_files().is_empty());
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
    fn refeed_is_idempotent() {
        let mut c = HorizonCollector::from_station(&test_station()).unwrap();
        let rows = [row_north(20.0, 800, 100), row_north(40.0, 700, 300), row_north(80.0, 3000, 2500)];
        let snapshot = |c: &HorizonCollector| -> Vec<(f32, Vec<FrontierPoint>)> {
            c.build_files()[0]
                .rows
                .iter()
                .map(|r| (r.bearing, r.breakpoints.clone()))
                .collect()
        };
        c.feed(AccumulatorType::Month, "2026-07", Layer::Combined, &rows);
        let first = snapshot(&c);
        c.feed(AccumulatorType::Month, "2026-07", Layer::Combined, &rows);
        assert_eq!(first, snapshot(&c));
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
