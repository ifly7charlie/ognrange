//! Terrarium DEM elevation tile lookup.
//!
//! Fetches Terrarium-encoded PNG DEM tiles (AWS Open Data `elevation-tiles-prod`
//! bucket by default, override via NEXT_PUBLIC_DEM_TILE_URL), caches decoded
//! tiles in an in-RAM LRU plus raw PNGs on disk under {DB_PATH}dem-tiles/, and
//! converts pixel values to elevation in meters. No API key required - this
//! replaced the previous Mapbox terrain-rgb source.

use lru::LruCache;
use std::num::NonZeroUsize;
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
        // Terrarium encoding: height = (R*256 + G + B/256) - 32768
        r * 256.0 + g + b / 256.0 - 32768.0
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
/// gives approximately 10km coverage.
const COARSE_ZOOM: u32 = 7;
const COARSE_RADIUS: u32 = 4;

pub struct ElevationService {
    cache: Arc<Mutex<LruCache<(u32, u32, u32), Arc<ElevationTile>>>>,
    client: reqwest::Client,
    resolution: u32,
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

    pub async fn cache_size_async(&self) -> usize {
        self.cache.lock().await.len()
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
        let (tx, ty, tz) = point_to_tile_fraction(lng, lat, self.resolution);
        let tile_x = tx.floor() as u32;
        let tile_y = ty.floor() as u32;

        let tile = self.load_tile(tz, tile_x, tile_y).await?;
        let xp = tx - tile_x as f64;
        let yp = ty - tile_y as f64;
        let x = (xp * tile.width as f64).floor() as u32;
        let y = (yp * tile.height as f64).floor() as u32;
        Some(tile.get_elevation(x, y).floor())
    }

    /// Get the maximum terrain elevation within ~10km of the given point.
    /// Uses a lower-resolution tile (zoom 7, ~1.2km/pixel) and scans a
    /// small pixel neighborhood rather than the precise per-point lookup.
    pub async fn get_max_elevation_coarse(&self, lat: f64, lng: f64) -> f64 {
        let (tx, ty, _) = point_to_tile_fraction(lng, lat, COARSE_ZOOM);
        let tile_x = tx.floor() as u32;
        let tile_y = ty.floor() as u32;

        let tile = match self.load_tile(COARSE_ZOOM, tile_x, tile_y).await {
            Some(t) => t,
            None => return 0.0,
        };
        let px = ((tx - tile_x as f64) * tile.width as f64).floor() as u32;
        let py = ((ty - tile_y as f64) * tile.height as f64).floor() as u32;
        tile.max_elevation_around(px, py, COARSE_RADIUS).floor()
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
                return Some(tile.clone());
            }
        }

        let path = dem_tile_path(z, x, y);
        let tile = match tokio::fs::read(&path).await {
            Ok(bytes) => match decode_png(&bytes) {
                Ok(tile) => {
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

        let tile = Arc::new(tile);
        let mut cache = self.cache.lock().await;
        cache.put(key, tile.clone());
        Some(tile)
    }

    /// Fetch a tile from the DEM endpoint and persist it to the disk cache.
    async fn fetch_tile(&self, z: u32, x: u32, y: u32) -> Option<ElevationTile> {
        let url = dem_tile_url(z, x, y);
        let bytes = match self.fetch_tile_bytes(&url).await {
            Ok(b) => b,
            Err(e) => {
                debug!("failed to fetch DEM tile {}: {}", url, e);
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
            Ok(tile) => Some(tile),
            Err(e) => {
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
