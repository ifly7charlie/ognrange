//! Time-based accumulator bucket calculation.
//!
//! Mirrors the TypeScript `accumulators.ts` — computes unique bucket IDs
//! for current, day, month, year, and year-nz (Southern Hemisphere season)
//! accumulator periods.

use chrono::{Datelike, Timelike, Utc};

use crate::config::ROLLUP_PERIOD_MINUTES;
use crate::coverage::header::AccumulatorBucket;
use crate::types::{prefix_with_zeros, Epoch};

/// Accumulator state for all time periods
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Accumulators {
    pub current: AccumulatorEntry,
    pub day: AccumulatorEntry,
    pub month: AccumulatorEntry,
    pub year: AccumulatorEntry,
    pub yearnz: AccumulatorEntry,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccumulatorEntry {
    pub bucket: AccumulatorBucket,
    pub file: String,
    pub effective_start: Epoch,
}

impl Accumulators {
    /// Describe accumulators for logging
    pub fn describe(&self) -> (String, String) {
        let current_start = self.current.effective_start.0 as i64;
        let current_text = if current_start > 0 {
            let dt = chrono::DateTime::from_timestamp(current_start, 0);
            dt.map(|d| format!("{:02}:{:02}", d.hour(), d.minute()))
                .unwrap_or_else(|| format!("{:04x}", self.current.bucket.0))
        } else {
            format!("{:04x}", self.current.bucket.0)
        };

        let dest_files: Vec<&str> = [&self.day, &self.month, &self.year, &self.yearnz]
            .iter()
            .map(|a| a.file.as_str())
            .filter(|f| !f.is_empty())
            .collect();

        (current_text, dest_files.join(","))
    }
}

/// Calculate the accumulators for the current time
pub fn what_accumulators(now: chrono::DateTime<Utc>) -> Accumulators {
    let rollup_minutes = *ROLLUP_PERIOD_MINUTES;
    let rollover_period =
        ((now.hour() * 60 + now.minute()) as f64 / rollup_minutes).floor() as u16;
    let new_bucket = ((now.day() as u16 & 0x1f) << 7) | (rollover_period & 0x7f);

    let y = now.year() as u32;
    let m = now.month();
    let d = now.day();

    let d_str = prefix_with_zeros(2, &d.to_string());
    let m_str = prefix_with_zeros(2, &m.to_string());

    let nz_start = if m >= 7 { y } else { y - 1 };

    // Calculate effective start timestamps
    let current_start = chrono::NaiveDate::from_ymd_opt(y as i32, m, d)
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|dt| dt.and_utc().timestamp() as u32 + rollover_period as u32 * rollup_minutes as u32 * 60)
        .unwrap_or(0);

    let day_start = chrono::NaiveDate::from_ymd_opt(y as i32, m, d)
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|dt| dt.and_utc().timestamp() as u32)
        .unwrap_or(0);

    let month_start = chrono::NaiveDate::from_ymd_opt(y as i32, m, 1)
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|dt| dt.and_utc().timestamp() as u32)
        .unwrap_or(0);

    let year_start = chrono::NaiveDate::from_ymd_opt(y as i32, 1, 1)
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|dt| dt.and_utc().timestamp() as u32)
        .unwrap_or(0);

    let yearnz_start = chrono::NaiveDate::from_ymd_opt(nz_start as i32, 7, 1)
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|dt| dt.and_utc().timestamp() as u32)
        .unwrap_or(0);

    Accumulators {
        current: AccumulatorEntry {
            bucket: AccumulatorBucket(new_bucket),
            file: String::new(),
            effective_start: Epoch(current_start),
        },
        day: AccumulatorEntry {
            bucket: AccumulatorBucket(
                (((y as u16) & 0x07) << 9) | (((m as u16) & 0x0f) << 5) | ((d as u16) & 0x1f),
            ),
            file: format!("{}-{}-{}", y, m_str, d_str),
            effective_start: Epoch(day_start),
        },
        month: AccumulatorEntry {
            bucket: AccumulatorBucket((((y as u16) & 0xff) << 4) | ((m as u16) & 0x0f)),
            file: format!("{}-{}", y, m_str),
            effective_start: Epoch(month_start),
        },
        year: AccumulatorEntry {
            bucket: AccumulatorBucket(y as u16),
            file: format!("{}", y),
            effective_start: Epoch(year_start),
        },
        yearnz: AccumulatorEntry {
            bucket: AccumulatorBucket(nz_start as u16),
            file: format!("{}nz", nz_start),
            effective_start: Epoch(yearnz_start),
        },
    }
}

/// Initialise accumulators from current time
pub fn initialise_accumulators() -> Accumulators {
    what_accumulators(Utc::now())
}

/// Calculate delay until the next rollup period boundary
pub fn next_rollup_delay() -> std::time::Duration {
    let now = Utc::now();
    let rollup_minutes = *ROLLUP_PERIOD_MINUTES;
    let current_minutes = (now.hour() * 60 + now.minute()) as f64;
    let next_rollup = rollup_minutes - (current_minutes % rollup_minutes);
    let delay_secs = next_rollup * 60.0 - now.second() as f64 + 0.5
        - now.nanosecond() as f64 / 1_000_000_000.0;
    let delay_secs = delay_secs.max(1.0);

    let next_time = now + chrono::Duration::seconds(delay_secs as i64);
    tracing::info!(
        "Rollup will be in {:.0} minutes at {}",
        next_rollup,
        next_time.format("%Y-%m-%dT%H:%M:%SZ")
    );

    std::time::Duration::from_secs_f64(delay_secs)
}
