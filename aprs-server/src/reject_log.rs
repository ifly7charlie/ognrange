//! Optional reject log — writes unprocessed packets to a file for debugging.
//!
//! Enabled by setting REJECT_LOG env var to a file path.
//! Each line: `reason\traw_packet\n`

use once_cell::sync::Lazy;
use std::io::Write;
use std::sync::Mutex;

static REJECT_LOG: Lazy<Option<Mutex<std::fs::File>>> = Lazy::new(|| {
    let path = std::env::var("REJECT_LOG").ok()?;
    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        Ok(f) => {
            tracing::info!("Reject log enabled: {}", path);
            Some(Mutex::new(f))
        }
        Err(e) => {
            tracing::error!("Failed to open reject log {}: {}", path, e);
            None
        }
    }
});

/// Call at startup to force initialisation and log whether the reject log is active.
pub fn init() {
    let _ = &*REJECT_LOG;
}

pub fn log_reject(reason: &str, raw: &str) {
    if let Some(ref file) = *REJECT_LOG {
        if let Ok(mut f) = file.lock() {
            let _ = writeln!(f, "{}\t{}", reason, raw);
        }
    }
}
