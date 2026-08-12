//! Generate a station's ground-horizon terrain file offline.
//!
//! Samples the DEM (real Terrarium tiles, cached under {DB_PATH}dem-tiles/)
//! along 720 bearing rays and writes {OUTPUT_PATH}{STATION}/{STATION}.ground-
//! horizon.arrow.gz exactly as the rollup generation pass would - use it to
//! produce a local fixture for frontend work or to spot-check a station after
//! deploy. Reads .env.local for OUTPUT_PATH/DB_PATH like the daemon.
//!
//!   groundhorizon <STATION> <LAT> <LNG> [BEACON_ALT_M]
//!
//! BEACON_ALT_M is the station's beaconed antenna altitude (m MSL); without
//! it the viewpoint falls back to ground + GROUND_STATION_AGL_M (default 10m),
//! exactly as the daemon does for a station that never beaconed an altitude.

// Shared modules are included via #[path]; each bin only uses part of them, so
// suppress the resulting dead-code/unused noise crate-wide for this tool.
#![allow(unused)]

#[path = "../types.rs"] mod types;
#[path = "../layers.rs"] mod layers;
#[path = "../coverage/mod.rs"] mod coverage;
#[path = "../config.rs"] mod config;
#[path = "../elevation.rs"] mod elevation;
#[path = "../ground_horizon.rs"] mod ground_horizon;

#[tokio::main]
async fn main() {
    let _ = dotenvy::from_filename(".env.local");
    // Surface elevation.rs fetch warnings/debug (RUST_LOG to override)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let args: Vec<String> = std::env::args().collect();
    if !(4..=5).contains(&args.len()) {
        eprintln!("usage: groundhorizon <STATION> <LAT> <LNG> [BEACON_ALT_M]");
        eprintln!("writes {{OUTPUT_PATH}}<STATION>/<STATION>.ground-horizon.arrow.gz");
        eprintln!("(+ uncompressed .arrow twin when UNCOMPRESSED_ARROW_FILES, default on)");
        std::process::exit(2);
    }
    let name = &args[1];
    let lat: f64 = args[2].parse().expect("invalid latitude");
    let lng: f64 = args[3].parse().expect("invalid longitude");
    let beacon_altitude: Option<f64> =
        args.get(4).map(|a| a.parse().expect("invalid beacon altitude"));

    let service = elevation::ElevationService::new();
    println!(
        "sampling {} rays x {} samples ({}km @ {}km) around {},{} ...",
        ground_horizon::GROUND_BINS,
        ground_horizon::GROUND_SAMPLES,
        ground_horizon::GROUND_MAX_KM,
        ground_horizon::GROUND_STEP_KM,
        lat,
        lng
    );
    let started = std::time::Instant::now();
    let Some(gh) = ground_horizon::compute(&service, lat, lng, beacon_altitude).await else {
        eprintln!("DEM sampling failed (tile fetch error) - rerun to retry");
        std::process::exit(1);
    };

    let out = config::output_dir(name);
    match ground_horizon::write_arrow(&out, name, &gh) {
        Ok(rows) => {
            let station_m = gh.elevations[0];
            let max_angle = gh.horizon_angle.iter().cloned().fold(f32::NEG_INFINITY, f32::max);
            println!(
                "wrote {} rows to {}/{}.ground-horizon.arrow.gz in {:.1}s (station {}m MSL, antenna +{:.1}m, max horizon {:.2} deg)",
                rows,
                out,
                name,
                started.elapsed().as_secs_f64(),
                station_m,
                gh.agl_m,
                max_angle
            );
        }
        Err(e) => {
            eprintln!("write failed: {}", e);
            std::process::exit(1);
        }
    }
}
