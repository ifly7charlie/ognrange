//! CoverageHeader — key encoding for H3 cache and LevelDB databases.
//!
//! DB key format: "{layer_prefix}{accumulator_hex}/{h3_index}"
//!   e.g. "c/1042/8828308283fffff"
//!
//! Lock key format: "{dbid_base36}/{layer_prefix}{accumulator_hex}/{h3_index}"
//!   e.g. "0/c/1042/8828308283fffff"

use crate::layers::{layer_from_prefix, Layer};
use crate::types::{prefix_with_zeros, to_base36, H3Index, H3LockKey, StationId};

/// Accumulator bucket — 12-bit value encoding time period
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct AccumulatorBucket(pub u16);

/// Accumulator type — what level of aggregation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum AccumulatorType {
    Current = 0,
    Day = 1,
    // Week = 2 (unused)
    Month = 3,
    Year = 4,
    YearNz = 5,
}

impl AccumulatorType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Current),
            1 => Some(Self::Day),
            3 => Some(Self::Month),
            4 => Some(Self::Year),
            5 => Some(Self::YearNz),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Day => "day",
            Self::Month => "month",
            Self::Year => "year",
            Self::YearNz => "yearnz",
        }
    }
}

/// Combined type+bucket as a 16-bit value: (type << 12) | (bucket & 0xfff)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AccumulatorTypeAndBucket(pub u16);

impl AccumulatorTypeAndBucket {
    pub fn new(t: AccumulatorType, b: AccumulatorBucket) -> Self {
        Self((((t as u16) & 0x0f) << 12) | (b.0 & 0x0fff))
    }

    pub fn accumulator_type(&self) -> AccumulatorType {
        AccumulatorType::from_u8(((self.0 >> 12) & 0x0f) as u8).unwrap_or(AccumulatorType::Current)
    }

    pub fn bucket(&self) -> AccumulatorBucket {
        AccumulatorBucket(self.0 & 0x0fff)
    }

    pub fn to_hex(&self) -> String {
        prefix_with_zeros(4, &format!("{:x}", self.0))
    }
}

/// CoverageHeader — represents a key for H3 data in cache and database
#[derive(Debug, Clone)]
pub struct CoverageHeader {
    pub h3: H3Index,
    pub tb: AccumulatorTypeAndBucket,
    pub dbid: StationId,
    pub layer: Layer,
    lock_key: H3LockKey,
}

impl CoverageHeader {
    /// Create from explicit components
    pub fn new(
        dbid: StationId,
        acc_type: AccumulatorType,
        bucket: AccumulatorBucket,
        h3: H3Index,
        layer: Layer,
    ) -> Self {
        let tb = AccumulatorTypeAndBucket::new(acc_type, bucket);
        let lock_key = H3LockKey(format!(
            "{}/{}{}/{}",
            to_base36(dbid.0 as u32),
            layer.db_prefix(),
            tb.to_hex(),
            h3
        ));
        CoverageHeader {
            h3,
            tb,
            dbid,
            layer,
            lock_key,
        }
    }

    /// Parse from a lock key string: "dbid36/layer_prefix/accumulator/h3"
    pub fn from_lock_key(s: &str) -> Option<Self> {
        let first_slash = s.find('/')?;
        let dbid_str = &s[..first_slash];
        let rest = &s[first_slash + 1..];

        // Check for layer prefix
        let first_char = rest.chars().next()?;
        let (layer, after_layer) = if let Some(l) = layer_from_prefix(first_char) {
            if rest.len() > 1 && rest.as_bytes()[1] == b'/' {
                (l, &rest[2..])
            } else {
                (Layer::Combined, rest)
            }
        } else {
            (Layer::Combined, rest)
        };

        // Parse accumulator (4 hex chars) / h3
        if after_layer.len() < 6 {
            return None;
        }
        let tb_hex = &after_layer[..4];
        let h3_str = &after_layer[5..]; // skip the '/'

        let tb = u16::from_str_radix(tb_hex, 16).ok()?;
        let dbid = u16::from_str_radix(dbid_str, 36).ok()?;

        Some(CoverageHeader {
            h3: H3Index(h3_str.to_string()),
            tb: AccumulatorTypeAndBucket(tb),
            dbid: StationId(dbid),
            layer,
            lock_key: H3LockKey(s.to_string()),
        })
    }

    /// Parse from a database key: "layer_prefix/accumulator/h3" or "accumulator/h3" (legacy)
    pub fn from_db_key(s: &str) -> Option<Self> {
        let first_char = s.chars().next()?;
        let (layer, rest) = if let Some(l) = layer_from_prefix(first_char) {
            if s.len() > 1 && s.as_bytes()[1] == b'/' {
                (l, &s[2..])
            } else {
                (Layer::Combined, s)
            }
        } else {
            (Layer::Combined, s)
        };

        if rest.len() < 5 {
            return None;
        }
        let tb_hex = &rest[..4];
        let h3_str = &rest[5..];
        let tb = u16::from_str_radix(tb_hex, 16).ok()?;

        let lock_key = H3LockKey(format!("0/{}", s));

        Some(CoverageHeader {
            h3: H3Index(h3_str.to_string()),
            tb: AccumulatorTypeAndBucket(tb),
            dbid: StationId(0),
            layer,
            lock_key,
        })
    }

    /// Get the DB key: "layer_prefix/accumulator/h3"
    pub fn db_key(&self) -> String {
        format!(
            "{}{}/{}",
            self.layer.db_prefix(),
            self.tb.to_hex(),
            self.h3
        )
    }

    /// Get the lock key: "dbid36/layer_prefix/accumulator/h3"
    pub fn lock_key(&self) -> &H3LockKey {
        &self.lock_key
    }

    /// Get the accumulator type
    pub fn accumulator_type(&self) -> AccumulatorType {
        self.tb.accumulator_type()
    }

    /// Get the accumulator bucket
    pub fn bucket(&self) -> AccumulatorBucket {
        self.tb.bucket()
    }

    /// Get DB search range for an accumulator block (data records only)
    pub fn db_search_range(
        t: AccumulatorType,
        b: AccumulatorBucket,
        layer: Layer,
    ) -> (String, String) {
        let tb = AccumulatorTypeAndBucket::new(t, b);
        let prefix = layer.db_prefix();
        let hex = tb.to_hex();
        (
            format!("{}{}/8000000000000000", prefix, hex),
            format!("{}{}/9000000000000000", prefix, hex),
        )
    }

    /// Get DB search range including metadata key (for purging).
    /// Starts at "00_" to include the "00_meta" key.
    pub fn db_search_range_with_meta(
        t: AccumulatorType,
        b: AccumulatorBucket,
        layer: Layer,
    ) -> (String, String) {
        let tb = AccumulatorTypeAndBucket::new(t, b);
        let prefix = layer.db_prefix();
        let hex = tb.to_hex();
        (
            format!("{}{}/00_", prefix, hex),
            format!("{}{}/9000000000000000", prefix, hex),
        )
    }

    /// Whether this key is a metadata key (h3 starts with "00" or "80")
    pub fn is_meta(&self) -> bool {
        self.h3.as_str().starts_with("00") || self.h3.as_str().starts_with("80")
    }

    /// Create a meta key for an accumulator
    pub fn accumulator_meta(t: AccumulatorType, b: AccumulatorBucket, layer: Layer) -> Self {
        CoverageHeader::new(StationId(0), t, b, H3Index("00_meta".to_string()), layer)
    }

    /// Legacy meta key: "accumulator/00_meta" (no layer prefix)
    pub fn legacy_meta_key(t: AccumulatorType, b: AccumulatorBucket) -> String {
        let tb = AccumulatorTypeAndBucket::new(t, b);
        format!("{}/00_meta", tb.to_hex())
    }
}

impl std::fmt::Display for CoverageHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.lock_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_db_key_roundtrip() {
        let ch = CoverageHeader::new(
            StationId(42),
            AccumulatorType::Current,
            AccumulatorBucket(0x042),
            H3Index("8828308283fffff".to_string()),
            Layer::Combined,
        );
        let db_key = ch.db_key();
        assert!(db_key.starts_with("c/"));
        assert!(db_key.contains("8828308283fffff"));

        let parsed = CoverageHeader::from_db_key(&db_key).unwrap();
        assert_eq!(parsed.layer, Layer::Combined);
        assert_eq!(parsed.h3.as_str(), "8828308283fffff");
    }

    #[test]
    fn test_lock_key_roundtrip() {
        let ch = CoverageHeader::new(
            StationId(42),
            AccumulatorType::Day,
            AccumulatorBucket(0x123),
            H3Index("8828308283fffff".to_string()),
            Layer::Flarm,
        );
        let lk = ch.lock_key().0.clone();

        let parsed = CoverageHeader::from_lock_key(&lk).unwrap();
        assert_eq!(parsed.layer, Layer::Flarm);
        assert_eq!(parsed.dbid, StationId(42));
        assert_eq!(parsed.h3.as_str(), "8828308283fffff");
    }
}
