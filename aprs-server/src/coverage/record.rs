//! Binary coverage record format — stored in the H3 cache and on disk in LevelDB.
//!
//! Layout matches the TypeScript CoverageRecord exactly for database compatibility.
//!
//! Station record (24 bytes):
//!   [0]      u8   version (0 = station)
//!   [1]      u8   unused
//!   [2]      u8   min_alt_max_sig (signal at the minimum altitude)
//!   [3]      u8   max_sig (peak signal)
//!   [4..8]   u32 LE  count
//!   [8..12]  u32 LE  sum_sig
//!   [12..16] u32 LE  sum_crc
//!   [16..20] u32 LE  sum_gap
//!   [20..22] u16 LE  min_alt_agl
//!   [22..24] u16 LE  min_alt
//!
//! Global header: same 24 bytes, version=1, byte[1]=head (linked list start index)
//! Global nested station (28 bytes):
//!   [0]      u8   version (2 = nested)
//!   [1]      u8   next (linked list pointer, 0=end)
//!   [2..24]  same observation fields
//!   [24..26] u16 LE  station_id
//!   [26..28] u16 LE  padding

use crate::types::{to_base36, StationId};

const HEADER_LEN: usize = 24;
const NESTED_LEN: usize = 28;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferType {
    Station = 0,
    Global = 1,
}


/// Observation data shared by all record types (22 bytes of payload at offsets 2..24).
#[derive(Debug, Clone, Copy, Default)]
struct ObservationData {
    min_alt_max_sig: u8,
    max_sig: u8,
    count: u32,
    sum_sig: u32,
    sum_crc: u32,
    sum_gap: u32,
    min_alt_agl: u16,
    min_alt: u16,
}

impl ObservationData {
    /// Update with a new observation.
    fn update(&mut self, altitude: u16, agl: u16, crc: u8, signal: u8, gap: u8) {
        if self.min_alt == 0 || self.min_alt > altitude {
            self.min_alt = altitude;
            self.min_alt_max_sig = signal;
        } else if self.min_alt == altitude {
            self.min_alt_max_sig = self.min_alt_max_sig.max(signal);
        }
        if self.min_alt_agl == 0 || self.min_alt_agl > agl {
            self.min_alt_agl = agl;
        }
        self.max_sig = self.max_sig.max(signal);
        self.sum_sig += (signal >> 2) as u32;
        self.sum_gap += gap as u32;
        self.sum_crc += crc as u32;
        self.count += 1;
    }

    /// Merge another record's data into this one (additive accumulation).
    fn merge_from(&mut self, src: &ObservationData) {
        if self.min_alt == 0 || self.min_alt > src.min_alt {
            self.min_alt = src.min_alt;
            self.min_alt_max_sig = src.min_alt_max_sig;
        } else if self.min_alt == src.min_alt {
            self.min_alt_max_sig = self.min_alt_max_sig.max(src.min_alt_max_sig);
        }
        if self.min_alt_agl == 0 || self.min_alt_agl > src.min_alt_agl {
            self.min_alt_agl = src.min_alt_agl;
        }
        self.max_sig = self.max_sig.max(src.max_sig);
        self.sum_sig += src.sum_sig;
        self.sum_crc += src.sum_crc;
        self.sum_gap += src.sum_gap;
        self.count += src.count;
    }

    /// Read from bytes at `base` offset. Fields at [base+2..base+24].
    fn read_from(buf: &[u8], base: usize) -> Self {
        ObservationData {
            min_alt_max_sig: buf[base + 2],
            max_sig: buf[base + 3],
            count: read_u32_le(buf, base + 4),
            sum_sig: read_u32_le(buf, base + 8),
            sum_crc: read_u32_le(buf, base + 12),
            sum_gap: read_u32_le(buf, base + 16),
            min_alt_agl: read_u16_le(buf, base + 20),
            min_alt: read_u16_le(buf, base + 22),
        }
    }

    /// Write to bytes at `base` offset. Fields at [base+2..base+24].
    fn write_to(&self, buf: &mut [u8], base: usize) {
        buf[base + 2] = self.min_alt_max_sig;
        buf[base + 3] = self.max_sig;
        write_u32_le(buf, base + 4, self.count);
        write_u32_le(buf, base + 8, self.sum_sig);
        write_u32_le(buf, base + 12, self.sum_crc);
        write_u32_le(buf, base + 16, self.sum_gap);
        write_u16_le(buf, base + 20, self.min_alt_agl);
        write_u16_le(buf, base + 22, self.min_alt);
    }
}

/// A per-station entry within a global record.
#[derive(Debug, Clone)]
struct NestedStation {
    station_id: u16,
    data: ObservationData,
}

/// Internal representation.
#[derive(Debug, Clone)]
enum RecordKind {
    Station(ObservationData),
    Global {
        summary: ObservationData,
        stations: Vec<NestedStation>,
    },
}

/// Coverage record — stored in H3 cache and LevelDB.
#[derive(Debug, Clone)]
pub struct CoverageRecord {
    inner: RecordKind,
}

impl CoverageRecord {
    /// Create a new empty record of the given type.
    pub fn new(buf_type: BufferType) -> Self {
        match buf_type {
            BufferType::Station => CoverageRecord {
                inner: RecordKind::Station(ObservationData::default()),
            },
            BufferType::Global => CoverageRecord {
                inner: RecordKind::Global {
                    summary: ObservationData::default(),
                    stations: Vec::new(),
                },
            },
        }
    }

    /// Parse from binary representation.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < HEADER_LEN {
            return None;
        }
        match data[0] {
            0 => Some(CoverageRecord {
                inner: RecordKind::Station(ObservationData::read_from(data, 0)),
            }),
            1 => {
                let summary = ObservationData::read_from(data, 0);
                let mut stations = Vec::new();
                let mut i = data[1] as usize;
                let mut safety = 0;
                while i != 0 && safety < 256 {
                    let base = HEADER_LEN + (i - 1) * NESTED_LEN;
                    if base + NESTED_LEN > data.len() {
                        break;
                    }
                    let station_id = read_u16_le(data, base + 24);
                    if station_id != 0 {
                        stations.push(NestedStation {
                            station_id,
                            data: ObservationData::read_from(data, base),
                        });
                    }
                    i = data[base + 1] as usize;
                    safety += 1;
                }
                Some(CoverageRecord {
                    inner: RecordKind::Global { summary, stations },
                })
            }
            _ => None,
        }
    }

    /// Serialize to the binary format.
    pub fn to_bytes(&self) -> Vec<u8> {
        match &self.inner {
            RecordKind::Station(data) => {
                let mut buf = vec![0u8; HEADER_LEN];
                data.write_to(&mut buf, 0);
                buf
            }
            RecordKind::Global { summary, stations } => {
                let mut buf = vec![0u8; HEADER_LEN + stations.len() * NESTED_LEN];
                buf[0] = 1;
                buf[1] = if stations.is_empty() { 0 } else { 1 };
                summary.write_to(&mut buf, 0);
                for (idx, station) in stations.iter().enumerate() {
                    let base = HEADER_LEN + idx * NESTED_LEN;
                    buf[base] = 2;
                    buf[base + 1] = if idx + 1 < stations.len() {
                        (idx + 2) as u8
                    } else {
                        0
                    };
                    station.data.write_to(&mut buf, base);
                    write_u16_le(&mut buf, base + 24, station.station_id);
                }
                buf
            }
        }
    }

    #[cfg(test)]
    pub fn buffer_type(&self) -> BufferType {
        match &self.inner {
            RecordKind::Station(_) => BufferType::Station,
            RecordKind::Global { .. } => BufferType::Global,
        }
    }

    #[cfg(test)]
    pub fn count(&self) -> u32 {
        match &self.inner {
            RecordKind::Station(data) => data.count,
            RecordKind::Global { summary, .. } => summary.count,
        }
    }

    /// Update with a new observation.
    pub fn update(&mut self, altitude: u16, agl: u16, crc: u8, signal: u8, gap: u8) {
        match &mut self.inner {
            RecordKind::Station(data) => data.update(altitude, agl, crc, signal, gap),
            RecordKind::Global { summary, .. } => summary.update(altitude, agl, crc, signal, gap),
        }
    }

    /// Update with a new observation and station tracking.
    pub fn update_with_station(
        &mut self,
        altitude: u16,
        agl: u16,
        crc: u8,
        signal: u8,
        gap: u8,
        station_id: StationId,
    ) {
        match &mut self.inner {
            RecordKind::Station(data) => data.update(altitude, agl, crc, signal, gap),
            RecordKind::Global { summary, stations } => {
                summary.update(altitude, agl, crc, signal, gap);
                let pos = stations.iter().position(|s| s.station_id == station_id.0);
                let idx = match pos {
                    Some(idx) => {
                        stations[idx].data.update(altitude, agl, crc, signal, gap);
                        idx
                    }
                    None => {
                        let mut data = ObservationData::default();
                        data.update(altitude, agl, crc, signal, gap);
                        stations.push(NestedStation {
                            station_id: station_id.0,
                            data,
                        });
                        stations.len() - 1
                    }
                };
                if idx > 0 && stations[idx].data.count > stations[idx - 1].data.count {
                    stations.swap(idx, idx - 1);
                }
            }
        }
    }

    /// Merge source record into self (rollup operation).
    pub fn rollup(
        &self,
        src: &CoverageRecord,
        valid_stations: Option<&std::collections::HashSet<StationId>>,
    ) -> Option<CoverageRecord> {
        match (&self.inner, &src.inner) {
            (RecordKind::Station(dest), RecordKind::Station(s)) => {
                let mut merged = *dest;
                merged.merge_from(s);
                Some(CoverageRecord {
                    inner: RecordKind::Station(merged),
                })
            }
            (
                RecordKind::Global {
                    stations: dest_stations,
                    ..
                },
                RecordKind::Global {
                    stations: src_stations,
                    ..
                },
            ) => {
                let mut station_map: std::collections::HashMap<u16, ObservationData> =
                    std::collections::HashMap::new();

                for ds in dest_stations {
                    if let Some(valid) = valid_stations {
                        if !valid.contains(&StationId(ds.station_id)) {
                            continue;
                        }
                    }
                    station_map
                        .entry(ds.station_id)
                        .and_modify(|d| d.merge_from(&ds.data))
                        .or_insert(ds.data);
                }

                for ss in src_stations {
                    station_map
                        .entry(ss.station_id)
                        .and_modify(|d| d.merge_from(&ss.data))
                        .or_insert(ss.data);
                }

                if station_map.is_empty() {
                    return None;
                }

                let mut stations: Vec<NestedStation> = station_map
                    .into_iter()
                    .map(|(station_id, data)| NestedStation { station_id, data })
                    .collect();
                stations.sort_by(|a, b| b.data.count.cmp(&a.data.count));

                let mut summary = ObservationData::default();
                for s in &stations {
                    summary.merge_from(&s.data);
                }

                Some(CoverageRecord {
                    inner: RecordKind::Global { summary, stations },
                })
            }
            _ => {
                let mut merged = self.clone();
                let src_data = match &src.inner {
                    RecordKind::Station(d) => d,
                    RecordKind::Global { summary, .. } => summary,
                };
                match &mut merged.inner {
                    RecordKind::Station(d) => d.merge_from(src_data),
                    RecordKind::Global { summary, .. } => summary.merge_from(src_data),
                }
                Some(merged)
            }
        }
    }
}

/// Arrow output row for a station-level record.
pub struct ArrowStation {
    pub h3lo: u32,
    pub h3hi: u32,
    pub min_agl: u16,
    pub min_alt: u16,
    pub min_alt_sig: u8,
    pub max_sig: u8,
    pub avg_sig: u8,
    pub avg_crc: u8,
    pub count: u32,
    pub avg_gap: u8,
}

/// Arrow output row for a global record (includes station info).
pub struct ArrowGlobal {
    pub h3lo: u32,
    pub h3hi: u32,
    pub min_agl: u16,
    pub min_alt: u16,
    pub min_alt_sig: u8,
    pub max_sig: u8,
    pub avg_sig: u8,
    pub avg_crc: u8,
    pub count: u32,
    pub avg_gap: u8,
    pub stations: String,
    pub expected_gap: u8,
    pub num_stations: u8,
}

impl ObservationData {
    fn arrow_averages(&self) -> (u8, u8, u8) {
        if self.count == 0 {
            return (0, 0, 0);
        }
        let c = self.count as f64;
        let avg_sig = ((self.sum_sig as f64 / c) * 4.0) as u8;
        let avg_crc = ((self.sum_crc as f64 / c) * 10.0) as u8;
        let avg_gap = ((self.sum_gap as f64 / c) * 4.0) as u8;
        (avg_sig, avg_crc, avg_gap)
    }
}

impl CoverageRecord {
    /// Convert to Arrow station row. Panics if called on a Global record.
    pub fn to_arrow_station(&self, h3lo: u32, h3hi: u32) -> ArrowStation {
        let data = match &self.inner {
            RecordKind::Station(d) => d,
            RecordKind::Global { summary, .. } => summary,
        };
        let (avg_sig, avg_crc, avg_gap) = data.arrow_averages();
        ArrowStation {
            h3lo, h3hi,
            min_agl: data.min_alt_agl,
            min_alt: data.min_alt,
            min_alt_sig: data.min_alt_max_sig,
            max_sig: data.max_sig,
            avg_sig, avg_crc, count: data.count, avg_gap,
        }
    }

    /// Convert to Arrow global row with station encoding.
    pub fn to_arrow_global(&self, h3lo: u32, h3hi: u32) -> ArrowGlobal {
        let (summary, stations_slice) = match &self.inner {
            RecordKind::Global { summary, stations } => (summary, stations.as_slice()),
            RecordKind::Station(d) => (d, &[] as &[NestedStation]),
        };
        let (avg_sig, avg_crc, avg_gap) = summary.arrow_averages();
        let num_stations = stations_slice.len().min(255) as u8;

        // Encode stations: base36(station_id << 4 | percentage), comma-separated, max 30
        let mut stations_str = String::new();
        let total_count = summary.count.max(1);
        for (i, ns) in stations_slice.iter().take(30).enumerate() {
            if i > 0 { stations_str.push(','); }
            // Match TS encoding: 0-10 scale (each unit = 10%), masked to 4 bits
            let pct = ((ns.data.count as u64 * 10) / total_count as u64) as u16 & 0x0f;
            let encoded = ((ns.station_id as u32) << 4) | pct as u32;
            stations_str.push_str(&to_base36(encoded));
        }

        let expected_gap = if num_stations > 0 {
            (avg_gap as u16 / num_stations as u16) as u8
        } else {
            avg_gap
        };

        ArrowGlobal {
            h3lo, h3hi,
            min_agl: summary.min_alt_agl,
            min_alt: summary.min_alt,
            min_alt_sig: summary.min_alt_max_sig,
            max_sig: summary.max_sig,
            avg_sig, avg_crc, count: summary.count, avg_gap,
            stations: stations_str,
            expected_gap,
            num_stations,
        }
    }

    /// Remove stations not in the valid set. Returns None if no stations remain.
    pub fn remove_invalid_stations(
        &self,
        valid_stations: &std::collections::HashSet<StationId>,
    ) -> Option<CoverageRecord> {
        match &self.inner {
            RecordKind::Station(_) => Some(self.clone()),
            RecordKind::Global { stations, .. } => {
                let filtered: Vec<NestedStation> = stations
                    .iter()
                    .filter(|s| valid_stations.contains(&StationId(s.station_id)))
                    .cloned()
                    .collect();
                if filtered.is_empty() {
                    return None;
                }
                let mut summary = ObservationData::default();
                for s in &filtered {
                    summary.merge_from(&s.data);
                }
                Some(CoverageRecord {
                    inner: RecordKind::Global { summary, stations: filtered },
                })
            }
        }
    }
}

fn read_u16_le(buf: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([buf[offset], buf[offset + 1]])
}

fn write_u16_le(buf: &mut [u8], offset: usize, val: u16) {
    buf[offset..offset + 2].copy_from_slice(&val.to_le_bytes());
}

fn read_u32_le(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3]])
}

fn write_u32_le(buf: &mut [u8], offset: usize, val: u32) {
    buf[offset..offset + 4].copy_from_slice(&val.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_station_record_basic() {
        let mut rec = CoverageRecord::new(BufferType::Station);
        assert_eq!(rec.count(), 0);

        rec.update(1000, 500, 2, 28, 5);
        assert_eq!(rec.count(), 1);

        rec.update(900, 400, 1, 32, 3);
        assert_eq!(rec.count(), 2);
    }

    #[test]
    fn test_global_record_with_stations() {
        let mut rec = CoverageRecord::new(BufferType::Global);
        rec.update_with_station(1000, 500, 2, 28, 5, StationId(1));
        rec.update_with_station(900, 400, 1, 32, 3, StationId(2));
        rec.update_with_station(800, 300, 0, 24, 4, StationId(1));

        assert_eq!(rec.count(), 3);
    }

    #[test]
    fn test_roundtrip_bytes() {
        let mut rec = CoverageRecord::new(BufferType::Station);
        rec.update(1000, 500, 2, 28, 5);

        let bytes = rec.to_bytes();
        let rec2 = CoverageRecord::from_bytes(&bytes).unwrap();
        assert_eq!(rec2.count(), 1);
        assert_eq!(rec2.buffer_type(), BufferType::Station);
    }

    #[test]
    fn test_binary_layout_station() {
        let mut buf = vec![0u8; 24];
        buf[0] = 0;
        buf[2] = 15;
        buf[3] = 20;
        buf[4..8].copy_from_slice(&42u32.to_le_bytes());
        buf[8..12].copy_from_slice(&100u32.to_le_bytes());
        buf[12..16].copy_from_slice(&50u32.to_le_bytes());
        buf[16..20].copy_from_slice(&30u32.to_le_bytes());
        buf[20..22].copy_from_slice(&200u16.to_le_bytes());
        buf[22..24].copy_from_slice(&500u16.to_le_bytes());

        let rec = CoverageRecord::from_bytes(&buf).unwrap();
        assert_eq!(rec.buffer_type(), BufferType::Station);
        assert_eq!(rec.count(), 42);
        assert_eq!(rec.to_bytes(), buf);
    }

    #[test]
    fn test_binary_layout_global() {
        let mut buf = vec![0u8; 24 + 2 * 28];
        buf[0] = 1;
        buf[1] = 1;
        buf[3] = 25;
        buf[4..8].copy_from_slice(&10u32.to_le_bytes());
        let base1 = 24;
        buf[base1] = 2;
        buf[base1 + 1] = 2;
        buf[base1 + 4..base1 + 8].copy_from_slice(&7u32.to_le_bytes());
        buf[base1 + 24..base1 + 26].copy_from_slice(&42u16.to_le_bytes());
        let base2 = 24 + 28;
        buf[base2] = 2;
        buf[base2 + 1] = 0;
        buf[base2 + 4..base2 + 8].copy_from_slice(&3u32.to_le_bytes());
        buf[base2 + 24..base2 + 26].copy_from_slice(&99u16.to_le_bytes());

        let rec = CoverageRecord::from_bytes(&buf).unwrap();
        assert_eq!(rec.buffer_type(), BufferType::Global);
        assert_eq!(rec.count(), 10);
        assert_eq!(rec.to_bytes(), buf);
    }

    #[test]
    fn test_global_non_sequential_linked_list() {
        // head -> slot 2 -> slot 1 -> end
        let mut buf = vec![0u8; 24 + 2 * 28];
        buf[0] = 1;
        buf[1] = 2; // head -> second slot
        buf[4..8].copy_from_slice(&15u32.to_le_bytes());
        let base1 = 24;
        buf[base1] = 2;
        buf[base1 + 1] = 0; // end
        buf[base1 + 4..base1 + 8].copy_from_slice(&5u32.to_le_bytes());
        buf[base1 + 24..base1 + 26].copy_from_slice(&10u16.to_le_bytes());
        let base2 = 24 + 28;
        buf[base2] = 2;
        buf[base2 + 1] = 1; // next -> slot 1
        buf[base2 + 4..base2 + 8].copy_from_slice(&10u32.to_le_bytes());
        buf[base2 + 24..base2 + 26].copy_from_slice(&20u16.to_le_bytes());

        let rec = CoverageRecord::from_bytes(&buf).unwrap();
        assert_eq!(rec.count(), 15);

        // After roundtrip, stations in linked-list order with sequential pointers
        let out = rec.to_bytes();
        assert_eq!(out[1], 1); // head -> first slot
        // First slot: station_id=20 (was slot 2 in input, first in linked list)
        assert_eq!(read_u16_le(&out, 24 + 24), 20);
        assert_eq!(out[24 + 1], 2); // next -> second
        // Second slot: station_id=10 (was slot 1 in input, second in linked list)
        assert_eq!(read_u16_le(&out, 52 + 24), 10);
        assert_eq!(out[52 + 1], 0); // end
    }

    #[test]
    fn test_station_rollup() {
        let mut r1 = CoverageRecord::new(BufferType::Station);
        r1.update(1000, 500, 2, 28, 5);

        let mut r2 = CoverageRecord::new(BufferType::Station);
        r2.update(900, 400, 1, 32, 3);

        let merged = r1.rollup(&r2, None).unwrap();
        assert_eq!(merged.count(), 2);
    }

    #[test]
    fn test_merge_sea_level_altitude() {
        let mut r1 = CoverageRecord::new(BufferType::Station);
        r1.update(500, 200, 1, 20, 5);

        let mut r2 = CoverageRecord::new(BufferType::Station);
        r2.update(0, 0, 1, 20, 5);

        // sea level (0) is lower than 500, should win
        let merged = r1.rollup(&r2, None).unwrap();
        let row = merged.to_arrow_station(0, 0);
        assert_eq!(row.min_alt, 0);
        assert_eq!(row.min_agl, 0);
    }

    #[test]
    fn test_global_rollup() {
        let mut r1 = CoverageRecord::new(BufferType::Global);
        r1.update_with_station(1000, 500, 2, 28, 5, StationId(1));
        r1.update_with_station(900, 400, 1, 32, 3, StationId(2));

        let mut r2 = CoverageRecord::new(BufferType::Global);
        r2.update_with_station(800, 300, 0, 24, 4, StationId(1));
        r2.update_with_station(700, 200, 3, 16, 2, StationId(3));

        let merged = r1.rollup(&r2, None).unwrap();
        assert_eq!(merged.count(), 4);
        assert_eq!(merged.buffer_type(), BufferType::Global);
    }

    #[test]
    fn test_arrow_station_format() {
        let mut rec = CoverageRecord::new(BufferType::Station);
        // signal=28 → sum_sig += 28>>2 = 7, crc=2, gap=5
        rec.update(1000, 500, 2, 28, 5);
        rec.update(900, 400, 1, 32, 3);

        let row = rec.to_arrow_station(0xAABBCCDD, 0x88);
        assert_eq!(row.h3lo, 0xAABBCCDD);
        assert_eq!(row.h3hi, 0x88);
        assert_eq!(row.count, 2);
        assert_eq!(row.min_alt, 900);
        assert_eq!(row.min_agl, 400);
        assert_eq!(row.max_sig, 32);
        // avgSig = (sum_sig / count) * 4 = ((7+8)/2)*4 = 30
        assert_eq!(row.avg_sig, 30);
    }

    #[test]
    fn test_arrow_global_format() {
        let mut rec = CoverageRecord::new(BufferType::Global);
        rec.update_with_station(1000, 500, 2, 28, 5, StationId(1));
        rec.update_with_station(900, 400, 1, 32, 3, StationId(2));
        rec.update_with_station(800, 300, 0, 24, 4, StationId(1));

        let row = rec.to_arrow_global(0x11, 0x22);
        assert_eq!(row.count, 3);
        assert_eq!(row.num_stations, 2);
        // stations string should contain two entries
        assert_eq!(row.stations.split(',').count(), 2);
    }

    #[test]
    fn test_remove_invalid_stations() {
        let mut rec = CoverageRecord::new(BufferType::Global);
        rec.update_with_station(1000, 500, 2, 28, 5, StationId(1));
        rec.update_with_station(900, 400, 1, 32, 3, StationId(2));
        rec.update_with_station(800, 300, 0, 24, 4, StationId(3));

        let mut valid = std::collections::HashSet::new();
        valid.insert(StationId(1));
        valid.insert(StationId(3));

        let filtered = rec.remove_invalid_stations(&valid).unwrap();
        assert_eq!(filtered.count(), 2); // station 2 removed
        assert_eq!(filtered.buffer_type(), BufferType::Global);
    }

    #[test]
    fn test_remove_invalid_stations_all_removed() {
        let mut rec = CoverageRecord::new(BufferType::Global);
        rec.update_with_station(1000, 500, 2, 28, 5, StationId(1));

        let valid = std::collections::HashSet::new(); // empty set
        assert!(rec.remove_invalid_stations(&valid).is_none());
    }

    // Tests for update_sum_gap removed — method was deleted
}
