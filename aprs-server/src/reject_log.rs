//! Optional reject log — writes unprocessed packets to a file for debugging.
//!
//! Enabled by setting REJECT_LOG env var to a file path.
//! Each line: `reason\traw_packet\n`
//!
//! Rotates when the file exceeds REJECT_LOG_MAX_MB (default 50MB),
//! renaming the current file to `{path}.1` and starting fresh.

use std::io::Write;
use std::sync::Mutex;

use tracing::{error, info};

use crate::config::REJECT_LOG_MAX_MB;

struct RejectLog {
    path: String,
    file: std::fs::File,
    written: u64,
    max_bytes: u64,
}

impl RejectLog {
    fn open(path: &str, max_bytes: u64) -> Option<Self> {
        match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            Ok(f) => {
                let written = f.metadata().map(|m| m.len()).unwrap_or(0);
                Some(Self {
                    path: path.to_string(),
                    file: f,
                    written,
                    max_bytes,
                })
            }
            Err(e) => {
                error!("Failed to open reject log {}: {}", path, e);
                None
            }
        }
    }

    fn write(&mut self, reason: &str, raw: &str) {
        let line = format!("{}\t{}\n", reason, raw);
        let len = line.len() as u64;
        if let Err(e) = self.file.write_all(line.as_bytes()) {
            error!("Failed to write reject log: {}", e);
            return;
        }
        self.written += len;

        if self.written >= self.max_bytes {
            self.rotate();
        }
    }

    fn rotate(&mut self) {
        let rotated = format!("{}.1", self.path);
        if let Err(e) = std::fs::rename(&self.path, &rotated) {
            error!("Failed to rotate reject log: {}", e);
            return;
        }
        match Self::open(&self.path, self.max_bytes) {
            Some(new) => {
                info!("Rotated reject log ({:.1} MB)", self.written as f64 / 1_048_576.0);
                *self = new;
            }
            None => {
                error!("Failed to reopen reject log after rotation");
            }
        }
    }
}

static REJECT_LOG: Mutex<Option<RejectLog>> = Mutex::new(None);

/// Call at startup to initialise and log whether the reject log is active.
pub fn init() {
    let path = match std::env::var("REJECT_LOG") {
        Ok(p) if !p.is_empty() => p,
        _ => return,
    };

    let max_bytes = *REJECT_LOG_MAX_MB * 1024 * 1024;

    if let Some(log) = RejectLog::open(&path, max_bytes) {
        info!(
            "Reject log enabled: {} (max {:.0} MB)",
            path,
            max_bytes as f64 / 1_048_576.0
        );
        *REJECT_LOG.lock().unwrap() = Some(log);
    }
}

pub fn log_reject(reason: &str, raw: &str) {
    if let Ok(mut guard) = REJECT_LOG.lock() {
        if let Some(ref mut log) = *guard {
            log.write(reason, raw);
        }
    }
}
