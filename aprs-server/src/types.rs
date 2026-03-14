/// Branded newtypes for type safety, mirroring the TypeScript branded types.

/// Unix timestamp in seconds
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, serde::Serialize, serde::Deserialize)]
pub struct Epoch(pub u32);

/// Unix timestamp in milliseconds
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, serde::Serialize, serde::Deserialize)]
pub struct EpochMs(pub u64);

/// Station numeric identifier (u16)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, serde::Serialize, serde::Deserialize)]
pub struct StationId(pub u16);

/// Station name string
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub struct StationName(pub String);

impl StationName {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for StationName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// H3 cell index as a hex string
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct H3Index(pub String);

impl H3Index {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Split into (lo, hi) u32 pair for Arrow output
    pub fn split_long(&self) -> (u32, u32) {
        let s = self.as_str();
        let hi_str = if s.len() > 8 { &s[..s.len() - 8] } else { "0" };
        let lo_str = if s.len() > 8 { &s[s.len() - 8..] } else { s };
        let hi = u32::from_str_radix(hi_str, 16).unwrap_or(0);
        let lo = u32::from_str_radix(lo_str, 16).unwrap_or(0);
        (lo, hi)
    }
}

impl std::fmt::Display for H3Index {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// H3 cache lock key: "dbid36/layer_prefix/accumulator/h3"
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct H3LockKey(pub String);

impl H3LockKey {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for H3LockKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Helper to format a number as zero-padded hex
pub fn prefix_with_zeros(width: usize, s: &str) -> String {
    format!("{:0>width$}", s, width = width)
}

/// Radix-36 encoding
pub fn to_base36(n: u32) -> String {
    if n == 0 {
        return "0".to_string();
    }
    let mut result = String::new();
    let mut val = n;
    while val > 0 {
        let digit = (val % 36) as u8;
        let c = if digit < 10 { b'0' + digit } else { b'a' + digit - 10 };
        result.push(c as char);
        val /= 36;
    }
    result.chars().rev().collect()
}
