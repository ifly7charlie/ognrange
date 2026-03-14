//! APRS packet parser focused on OGN (Open Glider Network) packet formats.
//!
//! Handles the subset of APRS used by OGN:
//! - Position reports with timestamps (aircraft tracking)
//! - Station location/status beacons
//! - Comment parsing for signal strength, CRC, rotation, vertical speed

use once_cell::sync::Lazy;
use regex::Regex;

/// Compiled regexes for comment field extraction (matching the TypeScript originals)
static RE_EXTRACT_DB: Lazy<Regex> = Lazy::new(|| Regex::new(r" (-?[0-9.]+)dB( |$)").unwrap());
static RE_EXTRACT_CRC: Lazy<Regex> = Lazy::new(|| Regex::new(r" ([0-9])e ").unwrap());
static RE_EXTRACT_ROT: Lazy<Regex> = Lazy::new(|| Regex::new(r" [+-]([0-9.]+)rot ").unwrap());
static RE_EXTRACT_VC: Lazy<Regex> = Lazy::new(|| Regex::new(r" [+-]([0-9]+)fpm ").unwrap());
/// DAO extension for position precision: !Wab! where a,b are base-91 digits
static RE_DAO: Lazy<Regex> = Lazy::new(|| Regex::new(r"!W([0-9])([0-9])!").unwrap());

#[derive(Debug, Clone, PartialEq)]
pub enum PacketType {
    Location,
    Status,
    Other,
}

#[derive(Debug, Clone)]
pub struct Digipeater {
    pub callsign: String,
}

#[derive(Debug, Clone)]
pub struct AprsPacket {
    pub source_callsign: String,
    pub dest_callsign: String,
    pub digipeaters: Vec<Digipeater>,
    pub packet_type: PacketType,
    pub timestamp: Option<u32>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub altitude: Option<f64>, // meters
    pub speed: Option<f64>,    // knots
    pub comment: Option<String>,
    pub body: Option<String>,
    pub raw: String,
}

impl Default for AprsPacket {
    fn default() -> Self {
        Self {
            source_callsign: String::new(),
            dest_callsign: String::new(),
            digipeaters: Vec::new(),
            packet_type: PacketType::Other,
            timestamp: None,
            latitude: None,
            longitude: None,
            altitude: None,
            speed: None,
            comment: None,
            body: None,
            raw: String::new(),
        }
    }
}

/// Extract signal strength in dB from comment, returns raw float
pub fn extract_signal_db(comment: &str) -> Option<f32> {
    RE_EXTRACT_DB
        .captures(comment)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<f32>().ok())
}

/// Extract CRC error count from comment
pub fn extract_crc(comment: &str) -> u8 {
    RE_EXTRACT_CRC
        .captures(comment)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<u8>().ok())
        .unwrap_or(0)
}

/// Extract rotation rate from comment
pub fn extract_rotation(comment: &str) -> f32 {
    RE_EXTRACT_ROT
        .captures(comment)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<f32>().ok())
        .unwrap_or(0.0)
}

/// Extract vertical climb rate in fpm from comment
pub fn extract_vertical_speed(comment: &str) -> f32 {
    RE_EXTRACT_VC
        .captures(comment)
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse::<f32>().ok())
        .unwrap_or(0.0)
}

/// Parse a raw APRS packet line into structured data.
///
/// OGN APRS format examples:
/// ```text
/// FLR1234AB>OGFLR,qAS,MyStation:/123456h4800.00N/01200.00E'000/000/A=001234 !W42! id06... 7.0dB 0e
/// MyStation>OGNSDR,TCPIP*,qAC,GLIDERN2:/123456h4800.00N/01200.00E'/A=000123 v0.2.8...
/// MyStation>OGNSDR,TCPIP*,qAC,GLIDERN2:>123456h v0.2.8 status info
/// ```
pub fn parse_aprs(raw: &str) -> Option<AprsPacket> {
    let mut packet = AprsPacket {
        raw: raw.to_string(),
        ..Default::default()
    };

    // Split header from body at first ':'
    let colon_pos = raw.find(':')?;
    let header = &raw[..colon_pos];
    let body = &raw[colon_pos + 1..];

    // Parse header: SOURCE>DEST[,DIGI1[*],DIGI2[*],...]
    let gt_pos = header.find('>')?;
    packet.source_callsign = header[..gt_pos].to_string();
    let path = &header[gt_pos + 1..];

    let path_parts: Vec<&str> = path.split(',').collect();
    if path_parts.is_empty() {
        return None;
    }
    packet.dest_callsign = path_parts[0].trim_end_matches('*').to_string();

    for &part in &path_parts[1..] {
        packet.digipeaters.push(Digipeater {
            callsign: part.trim_end_matches('*').to_string(),
        });
    }

    if body.is_empty() {
        return Some(packet);
    }

    let first_char = body.as_bytes()[0];

    match first_char {
        // Timestamp + position (most common for OGN)
        b'/' | b'@' => {
            parse_position_with_timestamp(body, &mut packet);
        }
        // Position without timestamp
        b'!' | b'=' => {
            parse_position_without_timestamp(body, &mut packet);
        }
        // Status report
        b'>' => {
            packet.packet_type = PacketType::Status;
            let status_body = &body[1..];

            // Status may have a timestamp prefix: HHMMSSh or DDHHMMz
            if status_body.len() >= 7 {
                let seventh = status_body.as_bytes().get(6).copied().unwrap_or(0);
                if seventh == b'h' || seventh == b'z' {
                    packet.timestamp = parse_timestamp(&status_body[..7]);
                    packet.body = Some(status_body[7..].trim().to_string());
                } else {
                    packet.body = Some(status_body.to_string());
                }
            } else {
                packet.body = Some(status_body.to_string());
            }
        }
        _ => {
            packet.packet_type = PacketType::Other;
            packet.body = Some(body.to_string());
        }
    }

    Some(packet)
}

/// Parse a position report with timestamp prefix.
/// Format: /HHMMSSh DDmm.hhN/DDDmm.hhE symbol CSE/SPD /A=AAAAAA comment
fn parse_position_with_timestamp(body: &str, packet: &mut AprsPacket) {
    packet.packet_type = PacketType::Location;

    // Need at least: /HHMMSSh (8) + DDmm.hhN (8) + / (1) + DDDmm.hhE (9) + sym (1) = 27
    if body.len() < 27 {
        packet.packet_type = PacketType::Other;
        return;
    }

    // Parse timestamp: /HHMMSSh or /DDHHMMz
    let ts_str = &body[1..8];
    packet.timestamp = parse_timestamp(ts_str);

    // Parse position starting at offset 8
    parse_position(&body[8..], packet);
}

/// Parse a position report without timestamp.
/// Format: !DDmm.hhN/DDDmm.hhE symbol CSE/SPD /A=AAAAAA comment
fn parse_position_without_timestamp(body: &str, packet: &mut AprsPacket) {
    packet.packet_type = PacketType::Location;
    if body.len() < 20 {
        packet.packet_type = PacketType::Other;
        return;
    }
    parse_position(&body[1..], packet);
}

/// Parse position from: DDmm.hhN/DDDmm.hhE<sym> CSE/SPD /A=AAAAAA comment
fn parse_position(pos_str: &str, packet: &mut AprsPacket) {
    if pos_str.len() < 19 {
        packet.packet_type = PacketType::Other;
        return;
    }

    // Latitude: DDmm.hhN (8 chars)
    let lat_str = &pos_str[..8];
    // Separator
    let _sep = pos_str.as_bytes()[8]; // symbol table indicator
    // Longitude: DDDmm.hhE (9 chars)
    let lon_str = &pos_str[9..18];
    // Symbol code
    let _symbol = pos_str.as_bytes()[18];

    let mut lat = parse_latitude(lat_str);
    let mut lon = parse_longitude(lon_str);

    if lat.is_none() || lon.is_none() {
        packet.packet_type = PacketType::Other;
        return;
    }

    // The rest after position+symbol (offset 19)
    let rest = &pos_str[19..];

    // Parse course/speed if present: CCC/SSS
    if rest.len() >= 7 && rest.as_bytes()[3] == b'/' {
        if let Ok(speed) = rest[4..7].parse::<f64>() {
            packet.speed = Some(speed); // knots
        }
    }

    // Extract altitude: /A=NNNNNN (feet → meters)
    if let Some(alt_pos) = rest.find("/A=") {
        let alt_str = &rest[alt_pos + 3..];
        let alt_end = alt_str.find(|c: char| !c.is_ascii_digit() && c != '-').unwrap_or(alt_str.len());
        if let Ok(alt_feet) = alt_str[..alt_end].parse::<f64>() {
            packet.altitude = Some(alt_feet * 0.3048); // feet to meters
        }
    }

    // Extract comment (everything after /A=NNNNNN or after course/speed)
    // For OGN, the comment contains signal info and starts with !Wab! idXX...
    let comment_start = rest.find(" ").map(|p| p + 1).unwrap_or(0);
    let comment = &rest[comment_start..];

    // Apply DAO position enhancement (!Wab!)
    if let Some(caps) = RE_DAO.captures(comment) {
        if let (Some(a), Some(b)) = (caps.get(1), caps.get(2)) {
            if let (Ok(da), Ok(db)) = (a.as_str().parse::<f64>(), b.as_str().parse::<f64>()) {
                if let Some(ref mut lat_val) = lat {
                    let sign = if *lat_val >= 0.0 { 1.0 } else { -1.0 };
                    *lat_val += sign * da * 0.001 / 60.0;
                }
                if let Some(ref mut lon_val) = lon {
                    let sign = if *lon_val >= 0.0 { 1.0 } else { -1.0 };
                    *lon_val += sign * db * 0.001 / 60.0;
                }
            }
        }
    }

    packet.latitude = lat;
    packet.longitude = lon;
    packet.comment = Some(comment.to_string());
}

/// Parse APRS latitude: DDmm.hhN → decimal degrees
fn parse_latitude(s: &str) -> Option<f64> {
    if s.len() < 8 {
        return None;
    }
    let degrees: f64 = s[..2].parse().ok()?;
    let minutes: f64 = s[2..7].parse().ok()?;
    let hemisphere = s.as_bytes()[7];

    let mut lat = degrees + minutes / 60.0;
    if hemisphere == b'S' {
        lat = -lat;
    } else if hemisphere != b'N' {
        return None;
    }

    if lat < -90.0 || lat > 90.0 {
        return None;
    }
    Some(lat)
}

/// Parse APRS longitude: DDDmm.hhE → decimal degrees
fn parse_longitude(s: &str) -> Option<f64> {
    if s.len() < 9 {
        return None;
    }
    let degrees: f64 = s[..3].parse().ok()?;
    let minutes: f64 = s[3..8].parse().ok()?;
    let hemisphere = s.as_bytes()[8];

    let mut lon = degrees + minutes / 60.0;
    if hemisphere == b'W' {
        lon = -lon;
    } else if hemisphere != b'E' {
        return None;
    }

    if lon < -180.0 || lon > 180.0 {
        return None;
    }
    Some(lon)
}

/// Parse APRS timestamp: HHMMSSh (UTC HMS) or DDHHMMz (UTC DDHHMM)
fn parse_timestamp(s: &str) -> Option<u32> {
    if s.len() < 7 {
        return None;
    }
    let format_char = s.as_bytes()[6];

    let now = chrono::Utc::now();

    match format_char {
        b'h' => {
            // HHMMSSh format — UTC time today
            let hh: u32 = s[..2].parse().ok()?;
            let mm: u32 = s[2..4].parse().ok()?;
            let ss: u32 = s[4..6].parse().ok()?;

            if hh > 23 || mm > 59 || ss > 59 {
                return None;
            }

            // Build epoch for today at HH:MM:SS UTC
            use chrono::{Datelike, NaiveDate, NaiveTime};
            let date = NaiveDate::from_ymd_opt(now.year(), now.month(), now.day())?;
            let time = NaiveTime::from_hms_opt(hh, mm, ss)?;
            let dt = date.and_time(time);
            Some(dt.and_utc().timestamp() as u32)
        }
        b'z' => {
            // DDHHMMz format — day + time UTC
            let dd: u32 = s[..2].parse().ok()?;
            let hh: u32 = s[2..4].parse().ok()?;
            let mm: u32 = s[4..6].parse().ok()?;

            if dd > 31 || hh > 23 || mm > 59 {
                return None;
            }

            use chrono::{Datelike, NaiveDate, NaiveTime};
            let date = NaiveDate::from_ymd_opt(now.year(), now.month(), dd)?;
            let time = NaiveTime::from_hms_opt(hh, mm, 0)?;
            let dt = date.and_time(time);
            Some(dt.and_utc().timestamp() as u32)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_latitude() {
        assert!((parse_latitude("4539.16N").unwrap() - 45.65267).abs() < 0.001);
        assert!((parse_latitude("3412.50S").unwrap() - (-34.20833)).abs() < 0.001);
    }

    #[test]
    fn test_parse_longitude() {
        assert!((parse_longitude("00555.89E").unwrap() - 5.93150).abs() < 0.001);
        assert!((parse_longitude("12030.00W").unwrap() - (-120.5)).abs() < 0.001);
    }

    #[test]
    fn test_parse_ogn_packet() {
        let raw = "FLRDDA5BA>APRS,qAS,LFLE:/072319h4539.16N/00555.89E'267/131/A=004567 !W97! id06DDA5BA -019fpm +0.0rot 7.0dB 0e +51.2kHz gps4x6";
        let packet = parse_aprs(raw).unwrap();
        assert_eq!(packet.source_callsign, "FLRDDA5BA");
        assert_eq!(packet.dest_callsign, "APRS");
        assert_eq!(packet.digipeaters.len(), 2);
        assert_eq!(packet.digipeaters[0].callsign, "qAS");
        assert_eq!(packet.digipeaters[1].callsign, "LFLE");
        assert_eq!(packet.packet_type, PacketType::Location);
        assert!(packet.latitude.is_some());
        assert!(packet.longitude.is_some());
        assert!(packet.altitude.is_some());
        // Altitude: 4567 feet = ~1392 meters
        assert!((packet.altitude.unwrap() - 1392.0).abs() < 1.0);
        assert!(packet.comment.as_ref().unwrap().contains("id06DDA5BA"));
        assert!(packet.comment.as_ref().unwrap().contains("7.0dB"));
    }

    #[test]
    fn test_extract_signal() {
        let comment = "!W97! id06DDA5BA -019fpm +0.0rot 7.0dB 0e +51.2kHz gps4x6";
        assert!((extract_signal_db(comment).unwrap() - 7.0).abs() < 0.01);
        assert_eq!(extract_crc(comment), 0);
        assert!((extract_rotation(comment) - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_parse_station_status() {
        let raw = "MyStation>OGNSDR,TCPIP*,qAC,GLIDERN2:>072319h v0.2.8 CPU:0.9";
        let packet = parse_aprs(raw).unwrap();
        assert_eq!(packet.source_callsign, "MyStation");
        assert_eq!(packet.dest_callsign, "OGNSDR");
        assert_eq!(packet.packet_type, PacketType::Status);
        assert!(packet.body.is_some());
    }

    #[test]
    fn test_parse_station_location() {
        let raw = "Hedensted>OGNSDR,TCPIP*,qAC,GLIDERN4:/072319h5547.67N/00934.40E'/A=000075 v0.2.8 CPU:0.9";
        let packet = parse_aprs(raw).unwrap();
        assert_eq!(packet.packet_type, PacketType::Location);
        assert!(packet.latitude.is_some());
        assert!(packet.altitude.is_some());
    }
}
