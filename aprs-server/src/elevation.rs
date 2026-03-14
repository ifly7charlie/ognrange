//! Mapbox terrain-RGB elevation tile lookup.
//!
//! Fetches terrain tiles from Mapbox API, caches them in an LRU cache,
//! and converts RGB pixel values to elevation in meters.

use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::config;

/// Cached decoded elevation tile: a grid of elevation values in meters
struct ElevationTile {
    data: Vec<u8>,   // raw RGBA pixels
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
        -10000.0 + (r * 256.0 * 256.0 + g * 256.0 + b) * 0.1
    }
}

pub struct ElevationService {
    cache: Arc<Mutex<LruCache<String, Arc<ElevationTile>>>>,
    client: reqwest::Client,
    access_token: Option<String>,
    referrer: String,
    resolution: u32,
    enabled: AtomicBool,
}

impl ElevationService {
    pub fn new() -> Self {
        let max_tiles = *config::MAX_ELEVATION_TILES;
        let access_token = {
            let t = config::NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN.clone();
            if t.is_empty() { None } else { Some(t) }
        };
        let referrer = format!("https://{}/", *config::NEXT_PUBLIC_SITEURL);
        let resolution = *config::ELEVATION_TILE_RESOLUTION;

        ElevationService {
            cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(max_tiles).unwrap_or(NonZeroUsize::new(1000).unwrap()),
            ))),
            client: reqwest::Client::new(),
            access_token,
            referrer,
            resolution,
            enabled: AtomicBool::new(true),
        }
    }

    /// Probe the Mapbox API with a test tile. If this fails, elevation
    /// lookups are permanently disabled (returns 0 for all queries).
    pub async fn probe(&self) {
        if self.access_token.is_none() {
            info!("No Mapbox access token, elevation lookups disabled");
            self.enabled.store(false, Ordering::Relaxed);
            return;
        }
        // Fetch a known tile (zoom 1, tile 0,0) as a health check
        let url = format!(
            "https://api.mapbox.com/v4/mapbox.terrain-rgb/1/0/0.pngraw?access_token={}",
            self.access_token.as_ref().unwrap()
        );
        match self.fetch_tile(&url).await {
            Ok(_) => {
                info!("Mapbox elevation API probe succeeded");
            }
            Err(e) => {
                warn!("Mapbox elevation API probe failed: {} — elevation lookups disabled", e);
                self.enabled.store(false, Ordering::Relaxed);
            }
        }
    }

    pub fn cache_size(&self) -> usize {
        // Can't easily get this without locking; return best estimate
        // In practice, callers will use the async version
        0
    }

    pub async fn cache_size_async(&self) -> usize {
        self.cache.lock().await.len()
    }

    /// Get terrain elevation at the given lat/lng in meters.
    /// Returns 0 if the elevation cannot be determined or lookups are disabled.
    pub async fn get_elevation(&self, lat: f64, lng: f64) -> f64 {
        if !self.enabled.load(Ordering::Relaxed) {
            return 0.0;
        }
        let access_token = match &self.access_token {
            Some(t) => t,
            None => return 0.0,
        };

        // Calculate tile coordinates
        let (tx, ty, _tz) = point_to_tile_fraction(lng, lat, self.resolution);
        let tile_x = tx.floor() as u32;
        let tile_y = ty.floor() as u32;
        let tile_z = self.resolution;

        let url = format!(
            "https://api.mapbox.com/v4/mapbox.terrain-rgb/{}/{}/{}.pngraw?access_token={}",
            tile_z, tile_x, tile_y, access_token
        );

        // Check cache
        {
            let mut cache = self.cache.lock().await;
            if let Some(tile) = cache.get(&url) {
                let xp = tx - tile_x as f64;
                let yp = ty - tile_y as f64;
                let x = (xp * tile.width as f64).floor() as u32;
                let y = (yp * tile.height as f64).floor() as u32;
                return tile.get_elevation(x, y).floor();
            }
        }

        // Fetch tile
        match self.fetch_tile(&url).await {
            Ok(tile) => {
                let xp = tx - tile_x as f64;
                let yp = ty - tile_y as f64;
                let x = (xp * tile.width as f64).floor() as u32;
                let y = (yp * tile.height as f64).floor() as u32;
                let elevation = tile.get_elevation(x, y).floor();

                let tile_arc = Arc::new(tile);
                let mut cache = self.cache.lock().await;
                cache.put(url, tile_arc);

                elevation
            }
            Err(e) => {
                debug!("Failed to fetch elevation tile: {}", e);
                0.0
            }
        }
    }

    async fn fetch_tile(&self, url: &str) -> Result<ElevationTile, Box<dyn std::error::Error + Send + Sync>> {
        let response = self
            .client
            .get(url)
            .header("Referer", &self.referrer)
            .send()
            .await?;

        let status = response.status();
        if status == reqwest::StatusCode::FORBIDDEN {
            error!("MapBox API returns 403 — check NEXT_PUBLIC_SITEURL ACL on Mapbox");
            return Err("MapBox 403 Forbidden".into());
        }
        if !status.is_success() {
            return Err(format!("MapBox API returns {}", status).into());
        }

        let bytes = response.bytes().await?;
        let decoder = png::Decoder::new(std::io::Cursor::new(&bytes));
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
