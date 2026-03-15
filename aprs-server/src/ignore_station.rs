use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashSet;

// when updating this don't forget to update docs/STATISTICS.md
static IGNORED_STATIONS: Lazy<HashSet<&'static str>> = Lazy::new(|| {
    [
        "AKASB", "CV32QG", "SPOT", "SPIDER", "INREACH", "FLYMASTER", "NAVITER", "CAPTURS",
        "LT24", "SKYLINES",
        "ANDROID", "IGCDROID", "APRSPUSH", "TEST", "DLY2APRS",
        "TTN2OGN", "TTN3OGN", "OBS2OGN", "HELIU2OGN", "MICROTRACK", "JETV-OGN",
        "GIGA01", "UNSET", "UNKNOWN", "STATIONS", "GLOBAL", "RELAY", "PWUNSET", "GLIDERNA",
        "X", "N1",
    ]
    .into_iter()
    .collect()
});

static RE_FULL: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[0-9]*$").unwrap());
static RE_START: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)^(XCG|XCC|RELAY|RND|bSky|N0TEST-).*$").unwrap());
static RE_ANY: Lazy<Regex> = Lazy::new(|| Regex::new(r"[^A-Za-z0-9_\-]").unwrap());

pub fn ignore_station(name: &str) -> bool {
    if name.is_empty() {
        return true;
    }

    // Check explicit blocklist (case-insensitive)
    let upper = name.to_uppercase();
    if IGNORED_STATIONS.contains(upper.as_str()) {
        return true;
    }

    // Pattern-based ignores
    if RE_FULL.is_match(name) || RE_START.is_match(name) || RE_ANY.is_match(name) {
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ignored_explicit() {
        assert!(ignore_station("RELAY"));
        assert!(ignore_station("relay")); // case-insensitive
        assert!(ignore_station("GLOBAL"));
    }

    #[test]
    fn test_ignored_patterns() {
        assert!(ignore_station("12345")); // all-digits
        assert!(ignore_station("XCGtest")); // XCG prefix
        assert!(ignore_station("RND42")); // RND prefix
        assert!(ignore_station("sta tion")); // space is not allowed
        assert!(ignore_station("bSkyFoo")); // bSky prefix
        assert!(ignore_station("N0TEST-1")); // N0TEST- prefix
    }

    #[test]
    fn test_not_ignored() {
        assert!(!ignore_station("SAFESKY")); // removed from ignore list
    }

    #[test]
    fn test_valid_stations() {
        assert!(!ignore_station("LFLE"));
        assert!(!ignore_station("Hedensted"));
        assert!(!ignore_station("My_Station-1"));
    }
}
