//! Station outage notifications via ntfy.sh.
//!
//! Each station gets a persistent notification topic URL
//! `{NTFY_BASE_URL}/{NTFY_TOPIC_PREFIX}-{station}-{suffix}` where the suffix
//! is 16 random bytes base64url-encoded, generated once per station and kept
//! in the station DB. The URL is exported in stations.json and the
//! per-station JSON so the frontend can render a subscribe QR code
//! (encoding the https:// URL as-is - camera apps don't scan ntfy://).
//!
//! An outage is: no status beacons for more than OUTAGE_BEACON_SECS
//! (receiver/feed down), or no aircraft traffic for more than
//! OUTAGE_TRAFFIC_SECS (receiving but hearing nothing). Notifications are
//! only published between NOTIFY_WINDOW hours in the station's approximate
//! local time, derived from its longitude (15° per hour) - deliberately
//! fuzzy, no timezone database involved.

use tracing::warn;

use crate::config::{NTFY_BASE_URL, NTFY_TOPIC_PREFIX};
use crate::station::StationDetails;
use crate::types::error_chain;

/// Local-time window (hours) outside which no notifications are sent
pub const NOTIFY_WINDOW_START_HOUR: u32 = 10;
/// Exclusive end of the window - last send happens in the 16:xx local hour
pub const NOTIFY_WINDOW_END_HOUR: u32 = 17;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutageReason {
    /// No status beacons - the receiver or its feed is down
    Beacons,
    /// Beaconing but no aircraft heard - antenna/RF chain problem
    Traffic,
}

impl OutageReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            OutageReason::Beacons => "beacons",
            OutageReason::Traffic => "traffic",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Priority {
    Low,
    Default,
}

/// Generate a station's persistent notification URL:
/// `{base}/{prefix}-{station}-{16 random bytes as base64url}`
pub fn generate_url(station: &str) -> String {
    format!(
        "{}/{}-{}-{}",
        &*NTFY_BASE_URL,
        &*NTFY_TOPIC_PREFIX,
        sanitize_topic(station),
        random_suffix()
    )
}

/// ntfy topics only allow `[A-Za-z0-9_-]`; station callsigns are already
/// ASCII alphanumerics but anything else becomes '_' rather than producing
/// an unpublishable topic
fn sanitize_topic(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect()
}

fn random_suffix() -> String {
    let mut buf = [0u8; 16];
    // System RNG failure is effectively impossible on our targets; a panic
    // here would only kill the outage monitor task's current iteration
    getrandom::fill(&mut buf).expect("system RNG unavailable");
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
}

/// A detected outage: why, and when the threshold was crossed
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Outage {
    pub reason: OutageReason,
    /// Epoch at which the station tipped into outage (last activity plus
    /// the threshold) - lets the caller tell a live transition from one
    /// that predates the current process
    pub started: u32,
}

/// Decide whether a station is currently in outage.
/// Thresholds are passed in (from config) so tests don't depend on env.
/// Stations that have never beaconed are judged on traffic alone; a station
/// failing both checks reports Beacons (the receiver being down explains
/// the missing traffic too).
pub fn evaluate_outage(
    details: &StationDetails,
    now: u32,
    beacon_secs: u32,
    traffic_secs: u32,
) -> Option<Outage> {
    if let Some(last_beacon) = details.last_beacon {
        if now.saturating_sub(last_beacon.0) > beacon_secs {
            return Some(Outage { reason: OutageReason::Beacons, started: last_beacon.0 + beacon_secs });
        }
    }
    if let Some(last_packet) = details.last_packet {
        if now.saturating_sub(last_packet.0) > traffic_secs {
            return Some(Outage { reason: OutageReason::Traffic, started: last_packet.0 + traffic_secs });
        }
    }
    None
}

/// Approximate local hour for a station: UTC hour shifted by longitude
/// (15° per hour, rounded). No position means UTC is used as-is.
pub fn local_hour(utc_hour: u32, lng: Option<f64>) -> u32 {
    let offset = lng
        .filter(|l| l.is_finite())
        .map(|l| (l / 15.0).round() as i32)
        .unwrap_or(0);
    (utc_hour as i32 + offset).rem_euclid(24) as u32
}

/// True when the station's approximate local time is within the
/// notification window
pub fn in_notify_window(utc_hour: u32, lng: Option<f64>) -> bool {
    let h = local_hour(utc_hour, lng);
    (NOTIFY_WINDOW_START_HOUR..NOTIFY_WINDOW_END_HOUR).contains(&h)
}

/// Publish a notification to a station's topic. Returns true on 2xx so the
/// caller only records state transitions that actually reached the server.
pub async fn send(
    client: &reqwest::Client,
    url: &str,
    title: &str,
    message: &str,
    priority: Priority,
    tags: &str,
) -> bool {
    let priority_str = match priority {
        Priority::Low => "low",
        Priority::Default => "default",
    };
    match client
        .post(url)
        .header("Title", title)
        .header("Priority", priority_str)
        .header("Tags", tags)
        .body(message.to_string())
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => true,
        Ok(resp) => {
            let status = resp.status();
            // ntfy explains rejections in a JSON body (rate limit hit,
            // topic blocked, message too large) - the status alone doesn't
            let body = resp.text().await.unwrap_or_default();
            let body: String = body.trim().chars().take(300).collect();
            warn!("ntfy publish to {} failed: HTTP {} {}", url, status, body);
            false
        }
        Err(e) => {
            warn!("ntfy publish to {} failed: {}", url, error_chain(&e));
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Epoch;

    const DAY: u32 = 86400;
    const WEEK: u32 = 7 * DAY;

    fn details(last_beacon: Option<u32>, last_packet: Option<u32>) -> StationDetails {
        StationDetails {
            last_beacon: last_beacon.map(Epoch),
            last_packet: last_packet.map(Epoch),
            ..Default::default()
        }
    }

    #[test]
    fn test_generate_url_shape() {
        let url = generate_url("LFLE");
        let prefix = format!("{}/{}-LFLE-", &*NTFY_BASE_URL, &*NTFY_TOPIC_PREFIX);
        assert!(url.starts_with(&prefix), "{}", url);
        let suffix = &url[prefix.len()..];
        // 16 bytes -> 22 chars of unpadded base64url
        assert_eq!(suffix.len(), 22);
        assert!(suffix.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
        // Random per call
        assert_ne!(generate_url("LFLE"), url);
    }

    #[test]
    fn test_sanitize_topic() {
        assert_eq!(sanitize_topic("LFLE-1"), "LFLE-1");
        assert_eq!(sanitize_topic("Foo.Bar/Baz"), "Foo_Bar_Baz");
    }

    #[test]
    fn test_evaluate_outage() {
        let now = 100 * DAY;

        // Healthy: both fresh
        assert_eq!(evaluate_outage(&details(Some(now - 600), Some(now - 600)), now, DAY, WEEK), None);
        // Beacons stale, traffic fresh (traffic without beacons - feed relaying but process odd).
        // The outage started when the beacon gap crossed the threshold.
        assert_eq!(
            evaluate_outage(&details(Some(now - 2 * DAY), Some(now - 600)), now, DAY, WEEK),
            Some(Outage { reason: OutageReason::Beacons, started: now - DAY })
        );
        // Beaconing but silent for over a week
        assert_eq!(
            evaluate_outage(&details(Some(now - 600), Some(now - WEEK - 1)), now, DAY, WEEK),
            Some(Outage { reason: OutageReason::Traffic, started: now - 1 })
        );
        // Fully dead: beacons reason wins
        assert_eq!(
            evaluate_outage(&details(Some(now - 2 * WEEK), Some(now - 2 * WEEK)), now, DAY, WEEK),
            Some(Outage { reason: OutageReason::Beacons, started: now - 2 * WEEK + DAY })
        );
        // Never beaconed: judged on traffic only
        assert_eq!(evaluate_outage(&details(None, Some(now - 2 * DAY)), now, DAY, WEEK), None);
        assert_eq!(
            evaluate_outage(&details(None, Some(now - WEEK - 1)), now, DAY, WEEK),
            Some(Outage { reason: OutageReason::Traffic, started: now - 1 })
        );
        // Exactly at the threshold is not yet an outage (spec says "> 1 day")
        assert_eq!(evaluate_outage(&details(Some(now - DAY), Some(now - 600)), now, DAY, WEEK), None);
    }

    #[test]
    fn test_local_hour_and_window() {
        // Greenwich: local == UTC
        assert_eq!(local_hour(12, Some(0.0)), 12);
        // Sydney ~151°E -> +10
        assert_eq!(local_hour(2, Some(151.0)), 12);
        // California ~-122° -> -8, wraps below zero
        assert_eq!(local_hour(2, Some(-122.0)), 18);
        // No position: UTC
        assert_eq!(local_hour(23, None), 23);

        // Window is 10:00-16:59 local
        assert!(in_notify_window(12, Some(0.0)));
        assert!(in_notify_window(10, Some(0.0)));
        assert!(!in_notify_window(17, Some(0.0)));
        assert!(!in_notify_window(9, Some(0.0)));
        // 02:00 UTC is midday in Sydney
        assert!(in_notify_window(2, Some(151.0)));
        // ...and night-time at Greenwich
        assert!(!in_notify_window(2, Some(0.0)));
    }
}
