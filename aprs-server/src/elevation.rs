//! Terrarium DEM elevation tile lookup.
//!
//! Fetches Terrarium-encoded PNG DEM tiles (AWS Open Data `elevation-tiles-prod`
//! bucket by default, override via NEXT_PUBLIC_DEM_TILE_URL), caches decoded
//! tiles in an in-RAM LRU plus raw PNGs on disk under {DB_PATH}dem-tiles/, and
//! converts pixel values to elevation in meters. No API key required - this
//! replaced the previous Mapbox terrain-rgb source. Unlike Mapbox, Terrarium
//! includes ocean bathymetry, so every lookup clamps to a per-location floor:
//! sea level everywhere except a small table of genuine below-sea-level land
//! regions (see elevation_floor_m).

use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::config;

/// Tiles untouched (mtime not refreshed) for this long are pruned.
const DEM_CACHE_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(10 * 24 * 3600);

/// Cached decoded elevation tile: a grid of elevation values in meters
struct ElevationTile {
    data: Vec<u8>, // raw RGBA pixels
    width: u32,
    height: u32,
}

impl ElevationTile {
    fn get_elevation(&self, x: u32, y: u32) -> f64 {
        let idx = ((y * self.width + x) * 4) as usize;
        if idx + 2 >= self.data.len() {
            return 0.0;
        }
        let r = self.data[idx] as f64;
        let g = self.data[idx + 1] as f64;
        let b = self.data[idx + 2] as f64;
        // Terrarium encoding: height = (R*256 + G + B/256) - 32768.
        // Raw value - may be NEGATIVE seabed over oceans (Terrarium
        // composites ETOPO1 bathymetry). The geographic entry points clamp
        // to elevation_floor_m() - pixel space has no lat/lng, so the clamp
        // cannot live here.
        r * 256.0 + g + b / 256.0 - 32768.0
    }

    /// Elevation at an in-tile fraction (0..1 in each axis), nearest pixel
    fn sample_fraction(&self, fx: f64, fy: f64) -> f64 {
        let x = (fx * self.width as f64).floor() as u32;
        let y = (fy * self.height as f64).floor() as u32;
        self.get_elevation(x, y)
    }

    /// Corrupt-pixel preprocessing for FINE-zoom tiles (~40m pixels), run
    /// once at load before the tile enters the RAM cache (the disk cache
    /// keeps the pristine source PNG). Terrarium has garbage pixels at
    /// land/water seams (observed +795m and -1079m side by side over a
    /// Baltic bay, permanently in the source tiles). A pixel that towers
    /// over - or sinks below - its ENTIRE 3x3 neighbourhood by
    /// DESPIKE_PROMINENCE_M is a one-pixel-wide 250m tower/shaft, which no
    /// real terrain produces at this pitch (cliffs are steps: they rise and
    /// stay high), so it is clamped to the neighbourhood. All comparisons
    /// use the original values (fixes are applied after the scan) so one
    /// clamped pixel cannot cascade into its neighbour's window. NOT safe at
    /// coarse zooms - at ~600m/pixel real monoliths are single-pixel - so
    /// the caller only runs this on fine-zoom tiles. Returns the number of
    /// pixels clamped
    fn despike(&mut self) -> usize {
        let (w, h) = (self.width as usize, self.height as usize);
        if w < 2 || h < 2 {
            return 0;
        }
        let original: Vec<f64> = (0..w * h)
            .map(|i| self.get_elevation((i % w) as u32, (i / w) as u32))
            .collect();
        let mut fixes: Vec<(usize, f64)> = Vec::new();
        for y in 0..h {
            for x in 0..w {
                let v = original[y * w + x];
                let mut nmax = f64::NEG_INFINITY;
                let mut nmin = f64::INFINITY;
                for ny in y.saturating_sub(1)..=(y + 1).min(h - 1) {
                    for nx in x.saturating_sub(1)..=(x + 1).min(w - 1) {
                        if nx == x && ny == y {
                            continue;
                        }
                        let e = original[ny * w + nx];
                        nmax = nmax.max(e);
                        nmin = nmin.min(e);
                    }
                }
                if v - nmax > DESPIKE_PROMINENCE_M {
                    fixes.push((y * w + x, nmax));
                } else if nmin - v > DESPIKE_PROMINENCE_M {
                    fixes.push((y * w + x, nmin));
                }
            }
        }
        for &(i, m) in &fixes {
            // Inverse of the Terrarium decode; m came from a decoded
            // neighbour so it round-trips exactly
            let v = ((m + 32768.0) * 256.0).round() as u32;
            self.data[i * 4] = (v >> 16) as u8;
            self.data[i * 4 + 1] = (v >> 8) as u8;
            self.data[i * 4 + 2] = v as u8;
        }
        fixes.len()
    }

    /// Maximum elevation in a pixel neighborhood around (cx, cy) with given radius.
    fn max_elevation_around(&self, cx: u32, cy: u32, radius: u32) -> f64 {
        let x_min = cx.saturating_sub(radius);
        let y_min = cy.saturating_sub(radius);
        let x_max = (cx + radius).min(self.width.saturating_sub(1));
        let y_max = (cy + radius).min(self.height.saturating_sub(1));
        let mut max = 0.0f64;
        for y in y_min..=y_max {
            for x in x_min..=x_max {
                let e = self.get_elevation(x, y);
                if e > max {
                    max = e;
                }
            }
        }
        max
    }
}

/// Zoom level for coarse max-elevation lookups. At zoom 7, each pixel
/// covers ~1.2km at the equator (~0.9km at 45°N). A ±4 pixel scan
/// gives approximately 10km coverage. Also the far-field zoom for
/// ground-horizon rays - reusing it keeps the tile cache to two tile classes.
pub(crate) const COARSE_ZOOM: u32 = 7;
const COARSE_RADIUS: u32 = 4;

/// A fine-zoom pixel must exceed (or undercut) all 8 of its neighbours by
/// this much to be treated as a corrupt Terrarium pixel rather than terrain
const DESPIKE_PROMINENCE_M: f64 = 250.0;

pub struct ElevationService {
    cache: Arc<Mutex<LruCache<(u32, u32, u32), Arc<ElevationTile>>>>,
    client: reqwest::Client,
    resolution: u32,
    // Cumulative lookup counters (all tile consumers, including the
    // ground-horizon queue - they share this cache). Deltas are the
    // caller's job.
    ram_hits: AtomicU64,
    disk_hits: AtomicU64,
    net_fetches: AtomicU64,
    net_failures: AtomicU64,
    net_time_ms: AtomicU64,
}

/// Snapshot of the in-RAM tile cache plus cumulative lookup counters.
#[derive(Default)]
pub struct TileCacheStats {
    pub tiles: usize,
    pub max_tiles: usize,
    /// Decoded pixel bytes held in RAM (excludes per-tile overhead)
    pub bytes: usize,
    /// Tile count by zoom level, ascending
    pub per_zoom: Vec<(u32, usize)>,
    pub ram_hits: u64,
    pub disk_hits: u64,
    /// Successful network fetches
    pub net_fetches: u64,
    /// Failed fetches (HTTP/timeout) and undecodable responses
    pub net_failures: u64,
    /// Wall time spent on network fetches (successes and failures)
    pub net_time_ms: u64,
}

/// On-disk tile cache directory. DEM tiles are immutable raw PNG bytes keyed
/// by z/x/y, so a restarted daemon can read them back from disk instead of
/// re-fetching from S3.
fn dem_cache_dir() -> String {
    format!("{}dem-tiles/", *config::DB_PATH)
}

fn dem_tile_path(z: u32, x: u32, y: u32) -> String {
    format!("{}{}-{}-{}.png", dem_cache_dir(), z, x, y)
}

fn dem_tile_url(z: u32, x: u32, y: u32) -> String {
    config::DEM_TILE_URL
        .replace("{z}", &z.to_string())
        .replace("{x}", &x.to_string())
        .replace("{y}", &y.to_string())
}

impl ElevationService {
    pub fn new() -> Self {
        let max_tiles = *config::MAX_ELEVATION_TILES;
        let resolution = *config::ELEVATION_TILE_RESOLUTION;

        if let Err(e) = std::fs::create_dir_all(dem_cache_dir()) {
            warn!("unable to create DEM tile cache directory {}: {}", dem_cache_dir(), e);
        }

        ElevationService {
            cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(max_tiles).unwrap_or(NonZeroUsize::new(1000).unwrap()),
            ))),
            // Timeouts are load-bearing: lookups are awaited inline per packet,
            // so a hung fetch would stall the whole packet processor
            client: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(5))
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("failed to build reqwest client"),
            resolution,
            ram_hits: AtomicU64::new(0),
            disk_hits: AtomicU64::new(0),
            net_fetches: AtomicU64::new(0),
            net_failures: AtomicU64::new(0),
            net_time_ms: AtomicU64::new(0),
        }
    }

    /// Probe the DEM tile endpoint with a test tile. Log-only health check:
    /// per-lookup failures already return None and are retried, so a
    /// transient outage at startup must not disable lookups.
    pub async fn probe(&self) {
        match self.load_tile(1, 0, 0).await {
            Some(_) => info!("DEM tile endpoint probe succeeded"),
            None => warn!("DEM tile endpoint probe failed - elevation lookups will retry per-request"),
        }
    }

    /// Cache inventory (walks the LRU - a few ms at 32k tiles, call from
    /// periodic tasks not per-packet) plus the cumulative lookup counters.
    pub async fn stats(&self) -> TileCacheStats {
        let cache = self.cache.lock().await;
        let mut per_zoom = std::collections::BTreeMap::new();
        let mut bytes = 0usize;
        for ((z, _, _), tile) in cache.iter() {
            *per_zoom.entry(*z).or_insert(0usize) += 1;
            bytes += tile.data.len();
        }
        TileCacheStats {
            tiles: cache.len(),
            max_tiles: cache.cap().get(),
            bytes,
            per_zoom: per_zoom.into_iter().collect(),
            ram_hits: self.ram_hits.load(Ordering::Relaxed),
            disk_hits: self.disk_hits.load(Ordering::Relaxed),
            net_fetches: self.net_fetches.load(Ordering::Relaxed),
            net_failures: self.net_failures.load(Ordering::Relaxed),
            net_time_ms: self.net_time_ms.load(Ordering::Relaxed),
        }
    }

    /// Get terrain elevation at the given lat/lng in meters.
    /// Returns 0 if the elevation cannot be determined.
    pub async fn get_elevation(&self, lat: f64, lng: f64) -> f64 {
        self.try_get_elevation(lat, lng).await.unwrap_or(0.0)
    }

    /// Get terrain elevation at the given lat/lng in meters, or None when the
    /// tile fetch fails - callers that persist the result must be able to
    /// tell failure apart from a genuine 0m.
    pub async fn try_get_elevation(&self, lat: f64, lng: f64) -> Option<f64> {
        let ((tz, tile_x, tile_y), (fx, fy)) = tile_fraction(lng, lat, self.resolution);
        let tile = self.load_tile(tz, tile_x, tile_y).await?;
        Some(tile.sample_fraction(fx, fy).max(elevation_floor_m(lat, lng)).floor())
    }

    /// Get the maximum terrain elevation within ~10km of the given point.
    /// Uses a lower-resolution tile (zoom 7, ~1.2km/pixel) and scans a
    /// small pixel neighborhood rather than the precise per-point lookup.
    /// Never below 0 (max_elevation_around starts at 0m): bathymetry cannot
    /// leak in, and over-reading the ground in a depression only makes the
    /// bogus-altitude filter more lenient - the safe direction.
    pub async fn get_max_elevation_coarse(&self, lat: f64, lng: f64) -> f64 {
        let ((tz, tile_x, tile_y), (fx, fy)) = tile_fraction(lng, lat, COARSE_ZOOM);
        let tile = match self.load_tile(tz, tile_x, tile_y).await {
            Some(t) => t,
            None => return 0.0,
        };
        let px = (fx * tile.width as f64).floor() as u32;
        let py = (fy * tile.height as f64).floor() as u32;
        tile.max_elevation_around(px, py, COARSE_RADIUS).floor()
    }

    /// The zoom level point lookups use (ELEVATION_TILE_RESOLUTION)
    pub fn fine_zoom(&self) -> u32 {
        self.resolution
    }

    /// A ray-walking cursor over this service's tiles
    pub fn cursor(&self) -> ElevationCursor<'_> {
        ElevationCursor { service: self, held: None }
    }

    /// Shared tile loader: RAM LRU first, then on-disk cache, then network.
    /// A network fetch persists the raw PNG to disk (temp file + rename so a
    /// concurrent reader never sees a partial PNG); a disk hit refreshes the
    /// tile's mtime so the pruner treats it as recently used.
    async fn load_tile(&self, z: u32, x: u32, y: u32) -> Option<Arc<ElevationTile>> {
        let key = (z, x, y);
        {
            let mut cache = self.cache.lock().await;
            if let Some(tile) = cache.get(&key) {
                self.ram_hits.fetch_add(1, Ordering::Relaxed);
                return Some(tile.clone());
            }
        }

        let path = dem_tile_path(z, x, y);
        let tile = match tokio::fs::read(&path).await {
            Ok(bytes) => match decode_png(&bytes) {
                Ok(tile) => {
                    self.disk_hits.fetch_add(1, Ordering::Relaxed);
                    // Refresh mtime so the pruner treats this tile as recently
                    // used (atime is unreliable under noatime/relatime mounts).
                    let _ = std::fs::File::options()
                        .append(true)
                        .open(&path)
                        .and_then(|f| f.set_modified(std::time::SystemTime::now()));
                    tile
                }
                Err(e) => {
                    warn!("corrupt DEM tile cache {}, re-fetching: {}", path, e);
                    let _ = tokio::fs::remove_file(&path).await;
                    self.fetch_tile(z, x, y).await?
                }
            },
            Err(_) => self.fetch_tile(z, x, y).await?,
        };

        // Clean corrupt source pixels once, before the tile serves any reads;
        // only at the fine zoom where single-pixel spikes cannot be terrain.
        // In-RAM only: the disk cache above holds the pristine source PNG
        let mut tile = tile;
        if z == self.resolution {
            let fixed = tile.despike();
            if fixed > 0 {
                debug!("despiked {} corrupt pixel(s) in DEM tile {}/{}/{}", fixed, z, x, y);
            }
        }

        let tile = Arc::new(tile);
        let mut cache = self.cache.lock().await;
        cache.put(key, tile.clone());
        Some(tile)
    }

    /// Fetch a tile from the DEM endpoint and persist it to the disk cache.
    async fn fetch_tile(&self, z: u32, x: u32, y: u32) -> Option<ElevationTile> {
        let url = dem_tile_url(z, x, y);
        let started = std::time::Instant::now();
        let fetched = self.fetch_tile_bytes(&url).await;
        self.net_time_ms
            .fetch_add(started.elapsed().as_millis() as u64, Ordering::Relaxed);
        let bytes = match fetched {
            Ok(b) => b,
            Err(e) => {
                self.net_failures.fetch_add(1, Ordering::Relaxed);
                // {:?} keeps the source chain - reqwest's Display alone is
                // just "error sending request for url (...)"
                debug!("failed to fetch DEM tile {}: {:?}", url, e);
                return None;
            }
        };

        // Persist the raw PNG best-effort - a failed write only costs a re-fetch
        let path = dem_tile_path(z, x, y);
        let tmp = format!("{}.{}.tmp", path, std::process::id());
        if let Err(e) = tokio::fs::write(&tmp, &bytes).await {
            warn!("unable to persist DEM tile {}: {}", path, e);
        } else if let Err(e) = tokio::fs::rename(&tmp, &path).await {
            warn!("unable to persist DEM tile {}: {}", path, e);
            let _ = tokio::fs::remove_file(&tmp).await;
        }

        match decode_png(&bytes) {
            Ok(tile) => {
                self.net_fetches.fetch_add(1, Ordering::Relaxed);
                Some(tile)
            }
            Err(e) => {
                self.net_failures.fetch_add(1, Ordering::Relaxed);
                debug!("failed to decode DEM tile {}: {}", url, e);
                None
            }
        }
    }

    async fn fetch_tile_bytes(&self, url: &str) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let response = self.client.get(url).send().await?;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("DEM tile fetch returned {}", status).into());
        }
        Ok(response.bytes().await?.to_vec())
    }

    /// Prune the on-disk DEM tile cache: delete any tile untouched (mtime not
    /// refreshed) for DEM_CACHE_MAX_AGE. Every disk cache hit bumps the
    /// tile's mtime, so this removes only genuinely cold tiles. Blocking IO -
    /// call from spawn_blocking.
    pub fn prune_disk_cache(&self) {
        let dir = dem_cache_dir();
        let cutoff = std::time::SystemTime::now() - DEM_CACHE_MAX_AGE;
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) => {
                warn!("unable to prune DEM tile cache {}: {}", dir, e);
                return;
            }
        };
        let mut pruned = 0u32;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().map_or(true, |e| e != "png") {
                continue;
            }
            let stale = entry
                .metadata()
                .and_then(|m| m.modified())
                .map(|mtime| mtime < cutoff)
                .unwrap_or(false);
            if stale && std::fs::remove_file(&path).is_ok() {
                pruned += 1;
            }
        }
        if pruned > 0 {
            info!("pruned {} stale DEM tile(s) from {}", pruned, dir);
        }
    }
}

/// Tile cursor for walking rays: holds the most recently used tile so
/// consecutive samples along a ray read pixels directly instead of taking the
/// cache mutex per sample (a z11 tile is ~14km across vs 0.5km sample spacing,
/// so ~95% of consecutive samples share a tile). Held-key comparison rather
/// than neighbour stepping means diagonal tile crossings, zoom switches and
/// the antimeridian need no special cases.
pub struct ElevationCursor<'a> {
    service: &'a ElevationService,
    held: Option<((u32, u32, u32), Arc<ElevationTile>)>,
}

impl ElevationCursor<'_> {
    /// Elevation in metres at lat/lng read from the given zoom's tiles, or
    /// None when the tile fetch fails. Identical result to try_get_elevation
    /// when zoom == fine_zoom() - fine-zoom tiles are despiked at load
    pub async fn sample(&mut self, lat: f64, lng: f64, zoom: u32) -> Option<f64> {
        let (key, (fx, fy)) = tile_fraction(lng, lat, zoom);
        let floor_m = elevation_floor_m(lat, lng);
        if let Some((held_key, tile)) = &self.held {
            if *held_key == key {
                return Some(tile.sample_fraction(fx, fy).max(floor_m).floor());
            }
        }
        let tile = self.service.load_tile(key.0, key.1, key.2).await?;
        let value = tile.sample_fraction(fx, fy).max(floor_m).floor();
        self.held = Some((key, tile));
        Some(value)
    }
}

fn decode_png(bytes: &[u8]) -> Result<ElevationTile, Box<dyn std::error::Error + Send + Sync>> {
    let decoder = png::Decoder::new(std::io::Cursor::new(bytes));
    let mut reader = decoder.read_info()?;
    let mut img_data = vec![0u8; reader.output_buffer_size()];
    let info = reader.next_frame(&mut img_data)?;

    // Convert to RGBA if needed
    let (width, height) = (info.width, info.height);
    let data = match info.color_type {
        png::ColorType::Rgba => img_data[..info.buffer_size()].to_vec(),
        png::ColorType::Rgb => {
            // Expand RGB to RGBA
            let rgb = &img_data[..info.buffer_size()];
            let mut rgba = Vec::with_capacity(width as usize * height as usize * 4);
            for chunk in rgb.chunks(3) {
                rgba.extend_from_slice(chunk);
                rgba.push(255);
            }
            rgba
        }
        _ => {
            warn!("Unexpected PNG color type: {:?}", info.color_type);
            img_data[..info.buffer_size()].to_vec()
        }
    };

    Ok(ElevationTile { data, width, height })
}

/// Convert (lng, lat) to tile fraction at given zoom level
/// Returns (x_fraction, y_fraction, zoom)
fn point_to_tile_fraction(lng: f64, lat: f64, zoom: u32) -> (f64, f64, u32) {
    let n = 2f64.powi(zoom as i32);
    let x = (lng + 180.0) / 360.0 * n;
    let lat_rad = lat.to_radians();
    let y = (1.0 - lat_rad.tan().asinh() / std::f64::consts::PI) / 2.0 * n;
    (x, y, zoom)
}

/// Land regions genuinely below sea level: (lat_min, lat_max, lng_min,
/// lng_max, floor_m). Everywhere else the elevation floor is 0 - Terrarium
/// includes ocean bathymetry and the surface that matters for AGL and
/// horizon occlusion is the water, but these depressions really are dry (or
/// inland-lake) land below 0. The floor is each region's lowest surface
/// (slightly padded), NOT an exemption from clamping: a box that brushes
/// ocean can only expose seabed down to its floor, so the worst-case error
/// anywhere is bounded by the floor of the containing box. Boxes may be
/// generous - a lower floor never lifts real terrain.
#[rustfmt::skip]
const BELOW_SEA_LEVEL_REGIONS: &[(f64, f64, f64, f64, f64)] = &[
    (30.6, 33.0, 35.30, 35.80, -435.0),   // Jordan Rift: Dead Sea -430, Sea of Galilee -212
    (36.0, 47.5, 46.00, 55.50, -29.0),    // Caspian shore lowlands, lake surface ~-28
    (43.0, 43.7, 51.50, 52.20, -132.0),   // Karagiye depression (KZ) -132
    (42.2, 43.2, 88.00, 90.50, -155.0),   // Turpan depression (CN) -154
    (28.5, 30.5, 26.50, 29.00, -135.0),   // Qattara depression (EG) -133
    (11.5, 14.5, 39.80, 41.50, -130.0),   // Danakil/Afar (ET/ER) -125
    (11.5, 11.8, 42.30, 42.60, -157.0),   // Lake Assal (DJ) -155
    (35.8, 37.3, -117.50, -116.20, -86.0), // Death Valley (US) -86
    (32.4, 33.6, -116.30, -115.00, -86.0), // Salton trough / Imperial valley (US/MX) -85
    (50.7, 57.2, 3.30, 12.00, -8.0),      // NL/DE/DK polders and marsh -7 (box spans North Sea: bounded -8)
    (52.2, 53.0, -0.50, 0.50, -4.0),      // English Fens -3
    (-29.5, -27.5, 136.50, 138.50, -17.0), // Lake Eyre (AU) -16
    (-49.6, -49.2, -68.90, -68.20, -106.0), // Laguna del Carbon (AR) -105
    (-38.9, -38.3, -63.30, -62.40, -43.0), // Salinas Chicas (AR) -42
    (-42.5, -42.2, -64.80, -64.20, -43.0), // Salina Grande, Peninsula Valdes (AR) -42
    (33.2, 34.2, 5.50, 8.70, -41.0),      // Chott Melrhir / el Gharsa (DZ/TN) -40
    (18.3, 18.7, -71.80, -71.20, -48.0),  // Lake Enriquillo (DO) -46
];

/// The lowest elevation this location can legitimately report: 0 (sea level)
/// unless it falls inside a known below-sea-level land region
fn elevation_floor_m(lat: f64, lng: f64) -> f64 {
    for &(lat0, lat1, lng0, lng1, floor_m) in BELOW_SEA_LEVEL_REGIONS {
        if (lat0..=lat1).contains(&lat) && (lng0..=lng1).contains(&lng) {
            return floor_m;
        }
    }
    0.0
}

/// Tile key plus in-tile fraction for a point. Pure - shared by the point
/// lookups and the ray-walking cursor so all paths index tiles identically
fn tile_fraction(lng: f64, lat: f64, zoom: u32) -> ((u32, u32, u32), (f64, f64)) {
    let (tx, ty, tz) = point_to_tile_fraction(lng, lat, zoom);
    let tile_x = tx.floor() as u32;
    let tile_y = ty.floor() as u32;
    ((tz, tile_x, tile_y), (tx - tile_x as f64, ty - tile_y as f64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tile_fraction_origin() {
        // Greenwich/equator is the exact centre of the single zoom-0 tile
        let ((z, x, y), (fx, fy)) = tile_fraction(0.0, 0.0, 0);
        assert_eq!((z, x, y), (0, 0, 0));
        assert!((fx - 0.5).abs() < 1e-12 && (fy - 0.5).abs() < 1e-12);
        // ...and the corner shared by all four zoom-1 tiles
        let ((z, x, y), (fx, fy)) = tile_fraction(0.0, 0.0, 1);
        assert_eq!((z, x, y), (1, 1, 1));
        assert!(fx.abs() < 1e-12 && fy.abs() < 1e-12);
    }

    #[test]
    fn tile_fraction_quadrant() {
        // 45N 90E at zoom 1: x = 270/360*2 = 1.5; y from the Mercator
        // formula: (1 - asinh(tan 45°)/pi)/2 * 2 ≈ 0.7194
        let ((z, x, y), (fx, fy)) = tile_fraction(90.0, 45.0, 1);
        assert_eq!((z, x, y), (1, 1, 0));
        assert!((fx - 0.5).abs() < 1e-12, "fx {}", fx);
        assert!((fy - 0.7194).abs() < 1e-3, "fy {}", fy);
    }

    #[test]
    fn tile_fraction_adjacent_keys() {
        // Points either side of a tile boundary land in adjacent tiles; a z7
        // tile is wide enough that nearby points share one (the cursor's
        // held-tile assumption)
        let ((_, x_w, _), _) = tile_fraction(-0.01, 51.0, 11);
        let ((_, x_e, _), _) = tile_fraction(0.01, 51.0, 11);
        assert_eq!(x_e, x_w + 1);
        // Away from tile boundaries (Greenwich is one at every zoom), nearby
        // points share a z7 tile
        let (key_a, _) = tile_fraction(0.1, 51.0, 7);
        let (key_b, _) = tile_fraction(0.5, 51.2, 7);
        assert_eq!(key_a, key_b);
    }

    #[test]
    fn sample_fraction_nearest_pixel() {
        // 2x2 tile: pixel values decode via the Terrarium formula; fraction
        // 0.75 must select the second pixel, matching the floor() convention
        let mut data = vec![0u8; 2 * 2 * 4];
        // pixel (1,1): R=129 -> (129*256) - 32768 = 256m
        data[(1 * 2 + 1) * 4] = 129;
        let tile = ElevationTile { data, width: 2, height: 2 };
        assert_eq!(tile.sample_fraction(0.75, 0.75), 256.0);
        // pixel (0,0): R=128 -> (128*256) - 32768 = 0m (sea level)
        let mut data = vec![0u8; 2 * 2 * 4];
        data[0] = 128;
        let tile = ElevationTile { data, width: 2, height: 2 };
        assert_eq!(tile.sample_fraction(0.0, 0.0), 0.0);
    }

    /// Build a tile from elevation values (row-major), Terrarium-encoded
    fn tile_from_elevations(width: u32, height: u32, elevations: &[f64]) -> ElevationTile {
        let mut data = vec![0u8; (width * height * 4) as usize];
        for (i, &m) in elevations.iter().enumerate() {
            let v = ((m + 32768.0) * 256.0).round() as u32;
            data[i * 4] = (v >> 16) as u8;
            data[i * 4 + 1] = (v >> 8) as u8;
            data[i * 4 + 2] = v as u8;
        }
        ElevationTile { data, width, height }
    }

    #[test]
    fn despike_clamps_isolated_towers_and_shafts() {
        // Flat 100m with a corrupt 900m pixel in the middle (the LENNRTSNS
        // class: +795m one pixel wide over a flat Baltic shore)
        let mut elev = vec![100.0; 9];
        elev[4] = 900.0;
        let mut tile = tile_from_elevations(3, 3, &elev);
        assert_eq!(tile.sample_fraction(0.5, 0.5), 900.0);
        assert_eq!(tile.despike(), 1);
        assert_eq!(tile.sample_fraction(0.5, 0.5), 100.0);

        // A one-pixel hole clamps upward too
        elev[4] = -900.0;
        let mut tile = tile_from_elevations(3, 3, &elev);
        assert_eq!(tile.despike(), 1);
        assert_eq!(tile.sample_fraction(0.5, 0.5), 100.0);

        // Non-spiked pixels are untouched
        assert_eq!(tile.sample_fraction(0.1, 0.1), 100.0);
    }

    #[test]
    fn despike_keeps_real_cliffs_and_summits() {
        // A cliff step: right column 400m higher. Each high pixel has a
        // same-height neighbour, so none is an isolated tower
        let elev = vec![
            100.0, 100.0, 500.0, //
            100.0, 100.0, 500.0, //
            100.0, 100.0, 500.0,
        ];
        let mut tile = tile_from_elevations(3, 3, &elev);
        assert_eq!(tile.despike(), 0);
        assert_eq!(tile.sample_fraction(0.9, 0.5), 500.0);

        // A summit exactly at the prominence threshold survives
        let mut elev = vec![100.0; 9];
        elev[4] = 100.0 + DESPIKE_PROMINENCE_M;
        let mut tile = tile_from_elevations(3, 3, &elev);
        assert_eq!(tile.despike(), 0);
        assert_eq!(tile.sample_fraction(0.5, 0.5), 100.0 + DESPIKE_PROMINENCE_M);
    }

    #[test]
    fn despike_partial_window_at_tile_edge() {
        // Corner pixel spike: only 3 in-tile neighbours, still clamped
        let mut elev = vec![50.0; 9];
        elev[0] = 950.0;
        let mut tile = tile_from_elevations(3, 3, &elev);
        assert_eq!(tile.despike(), 1);
        assert_eq!(tile.sample_fraction(0.0, 0.0), 50.0);
    }

    #[test]
    fn despike_fixes_do_not_cascade() {
        // The +900 spike and the -900 hole are diagonal neighbours: each is
        // judged against ORIGINAL values, so the spike's window contains the
        // hole (and vice versa) yet both still clamp to the flat ground, and
        // the fixed values do not shift their neighbours' verdicts
        let mut elev = vec![100.0; 16];
        elev[5] = 900.0;
        elev[10] = -900.0;
        let mut tile = tile_from_elevations(4, 4, &elev);
        assert_eq!(tile.despike(), 2);
        assert_eq!(tile.sample_fraction(0.3, 0.3), 100.0);
        assert_eq!(tile.sample_fraction(0.6, 0.6), 100.0);
    }

    #[test]
    fn tile_decode_keeps_raw_bathymetry() {
        // R=126 G=200: (126*256 + 200) - 32768 = -312m of seabed. The tile
        // decodes raw; the geographic entry points apply elevation_floor_m
        let mut data = vec![0u8; 4];
        data[0] = 126;
        data[1] = 200;
        let tile = ElevationTile { data, width: 1, height: 1 };
        assert_eq!(tile.get_elevation(0, 0), -312.0);
    }

    #[test]
    fn elevation_floor_regions() {
        // Open ocean and ordinary land: sea level is the floor
        assert_eq!(elevation_floor_m(0.0, -30.0), 0.0);
        assert_eq!(elevation_floor_m(47.0, 8.0), 0.0);
        // Dead Sea shore, Death Valley, Dutch polders: genuine negatives pass
        assert_eq!(elevation_floor_m(31.5, 35.5), -435.0);
        assert_eq!(elevation_floor_m(36.4, -116.8), -86.0);
        assert_eq!(elevation_floor_m(52.3, 4.8), -8.0);
        // The Mediterranean just west of the Jordan rift box is NOT exempt
        assert_eq!(elevation_floor_m(32.0, 34.5), 0.0);
    }
}
