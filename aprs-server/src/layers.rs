use std::collections::HashSet;

use crate::coverage::header::AccumulatorType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Layer {
    Combined,
    Flarm,
    Adsb,
    Adsl,
    Fanet,
    Ogntrk,
    Paw,
    Safesky,
}

impl Layer {
    pub fn name(&self) -> &'static str {
        match self {
            Layer::Combined => "combined",
            Layer::Flarm => "flarm",
            Layer::Adsb => "adsb",
            Layer::Adsl => "adsl",
            Layer::Fanet => "fanet",
            Layer::Ogntrk => "ogntrk",
            Layer::Paw => "paw",
            Layer::Safesky => "safesky",
        }
    }

    /// Suffix for output filenames: empty for combined, `.{name}` for others
    pub fn file_suffix(&self) -> &'static str {
        match self {
            Layer::Combined => "",
            Layer::Flarm => ".flarm",
            Layer::Adsb => ".adsb",
            Layer::Adsl => ".adsl",
            Layer::Fanet => ".fanet",
            Layer::Ogntrk => ".ogntrk",
            Layer::Paw => ".paw",
            Layer::Safesky => ".safesky",
        }
    }

    pub fn from_name(name: &str) -> Option<Layer> {
        match name {
            "combined" => Some(Layer::Combined),
            "flarm" => Some(Layer::Flarm),
            "adsb" => Some(Layer::Adsb),
            "adsl" => Some(Layer::Adsl),
            "fanet" => Some(Layer::Fanet),
            "ogntrk" => Some(Layer::Ogntrk),
            "paw" => Some(Layer::Paw),
            "safesky" => Some(Layer::Safesky),
            _ => None,
        }
    }

    /// DB key prefix per layer - sorted alphabetically for contiguous iteration
    pub fn db_prefix(&self) -> &'static str {
        match self {
            Layer::Adsb => "a/",
            Layer::Combined => "c/",
            Layer::Adsl => "d/",
            Layer::Flarm => "f/",
            Layer::Fanet => "n/",
            Layer::Paw => "p/",
            Layer::Safesky => "s/",
            Layer::Ogntrk => "t/",
        }
    }

    /// Bit position in the layer bitmask (u8, max 8 bits)
    pub fn bit_position(&self) -> u8 {
        match self {
            Layer::Combined => 0,
            Layer::Flarm => 1,
            Layer::Adsb => 2,
            Layer::Adsl => 3,
            Layer::Fanet => 4,
            Layer::Paw => 5,
            Layer::Ogntrk => 6,
            Layer::Safesky => 7,
        }
    }

    pub fn bit_mask(&self) -> u8 {
        1 << self.bit_position()
    }

    /// Frequency group for horizon aggregation: the receive horizon is an
    /// antenna/frequency property, not a protocol one. None = excluded
    /// (flarm/ogntrk are already inside combined - feeding combined instead
    /// halves the rows processed; safesky is network-sourced, not RF).
    pub fn frequency_group(&self) -> Option<FrequencyGroup> {
        match self {
            Layer::Combined | Layer::Fanet | Layer::Adsl | Layer::Paw => {
                Some(FrequencyGroup::Mhz868)
            }
            Layer::Adsb => Some(FrequencyGroup::Mhz1090),
            Layer::Flarm | Layer::Ogntrk | Layer::Safesky => None,
        }
    }
}

/// RF frequency groups for horizon output
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrequencyGroup {
    Mhz868,
    Mhz1090,
}

impl FrequencyGroup {
    pub const ALL: &'static [FrequencyGroup] = &[FrequencyGroup::Mhz868, FrequencyGroup::Mhz1090];

    pub fn mhz(&self) -> u16 {
        match self {
            FrequencyGroup::Mhz868 => 868,
            FrequencyGroup::Mhz1090 => 1090,
        }
    }
}

/// All layers in DB sort order
pub const ALL_LAYERS: &[Layer] = &[
    Layer::Adsb,
    Layer::Combined,
    Layer::Adsl,
    Layer::Flarm,
    Layer::Fanet,
    Layer::Paw,
    Layer::Safesky,
    Layer::Ogntrk,
];

/// Layer processing order for rollups: roughly lowest -> highest expected
/// traffic, so cheap layers complete (and free their current accumulators)
/// before the expensive ones. Must contain every layer in ALL_LAYERS.
pub const ROLLUP_LAYER_ORDER: &[Layer] = &[
    Layer::Ogntrk,
    Layer::Safesky,
    Layer::Paw,
    Layer::Fanet,
    Layer::Adsl,
    Layer::Flarm,
    Layer::Combined,
    Layer::Adsb,
];

/// Layers to roll up, in ROLLUP_LAYER_ORDER, filtered by the enabled set.
/// Gives a deterministic order even when ENABLED_LAYERS (a HashSet) is used.
pub fn rollup_layers(enabled: Option<&HashSet<Layer>>) -> Vec<Layer> {
    ROLLUP_LAYER_ORDER
        .iter()
        .copied()
        .filter(|l| enabled.is_none_or(|e| e.contains(l)))
        .collect()
}

/// Whether a specific accumulator type should be produced for this layer.
/// ADSB daily output is suppressed to reduce data volume (presence-only, high volume).
/// if this is changed don't forget to update the frontend shouldProduceOutput in lib/common/layers.ts
pub fn should_produce(layer: Layer, acc_type: AccumulatorType) -> bool {
    !matches!((layer, acc_type), (Layer::Adsb, AccumulatorType::Day))
}

/// Protocols that store synthetic signal (no real dB value available)
pub fn is_presence_only(layer: Layer) -> bool {
    matches!(layer, Layer::Adsb | Layer::Paw | Layer::Safesky)
}

/// Synthetic signal value for presence-only layers (4 ≈ 1.0dB equivalent)
pub const PRESENCE_SIGNAL: u8 = 4;

/// Protocols that also write to the combined layer
pub fn is_combined_layer(layer: Layer) -> bool {
    matches!(layer, Layer::Flarm | Layer::Ogntrk)
}

/// APRS TOCALL (destCallsign) → Layer mapping
pub fn layer_from_dest_callsign(dest: &str) -> Option<Layer> {
    match dest {
        "OGFLR" | "OGFLR6" | "OGFLR7" | "APRS" => Some(Layer::Flarm),
        "OGADSB" => Some(Layer::Adsb),
        "OGADSL" => Some(Layer::Adsl),
        "OGNFNT" => Some(Layer::Fanet),
        "OGNTRK" => Some(Layer::Ogntrk),
        "OGPAW" => Some(Layer::Paw),
        "OGNSKY" => Some(Layer::Safesky),
        _ => None,
    }
}

/// Returns the set of layers a packet should be written to
pub fn get_write_layers(layer: Layer) -> Vec<Layer> {
    if is_combined_layer(layer) {
        vec![Layer::Combined, layer]
    } else {
        vec![layer]
    }
}

/// Reverse lookup: prefix char → Layer
pub fn layer_from_prefix(prefix: char) -> Option<Layer> {
    match prefix {
        'a' => Some(Layer::Adsb),
        'c' => Some(Layer::Combined),
        'd' => Some(Layer::Adsl),
        'f' => Some(Layer::Flarm),
        'n' => Some(Layer::Fanet),
        'p' => Some(Layer::Paw),
        's' => Some(Layer::Safesky),
        't' => Some(Layer::Ogntrk),
        _ => None,
    }
}

/// Check if a DB key has a layer prefix (e.g. "c/0042/...")
pub fn is_layer_prefixed(key: &str) -> bool {
    key.len() > 1
        && key.as_bytes()[1] == b'/'
        && layer_from_prefix(key.as_bytes()[0] as char).is_some()
}

/// Computes a layer bitmask from an iterable of layers
pub fn layer_mask_from_set(layers: &[Layer]) -> u8 {
    layers.iter().fold(0u8, |mask, l| mask | l.bit_mask())
}

/// Decode a layer bitmask into a list of layer names
pub fn layer_names_from_mask(mask: u8) -> Vec<String> {
    ALL_LAYERS
        .iter()
        .filter(|l| mask & l.bit_mask() != 0)
        .map(|l| l.name().to_string())
        .collect()
}

/// Bitmask with all layer bits set
pub fn all_layers_mask() -> u8 {
    ALL_LAYERS.iter().fold(0u8, |m, l| m | l.bit_mask())
}

/// Parse ENABLED_LAYERS env var. Returns None to mean "all layers enabled"
pub fn parse_enabled_layers(env_value: Option<&str>) -> Option<HashSet<Layer>> {
    let val = env_value?;
    if val.is_empty() {
        return None;
    }
    let layers: HashSet<Layer> = val
        .split(',')
        .filter_map(|s| Layer::from_name(s.trim().to_lowercase().as_str()))
        .collect();
    if layers.is_empty() {
        None
    } else {
        Some(layers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollup_layer_order_is_permutation_of_all_layers() {
        assert_eq!(ROLLUP_LAYER_ORDER.len(), ALL_LAYERS.len());
        let all: HashSet<Layer> = ALL_LAYERS.iter().copied().collect();
        let order: HashSet<Layer> = ROLLUP_LAYER_ORDER.iter().copied().collect();
        assert_eq!(all, order, "ROLLUP_LAYER_ORDER must cover every layer in ALL_LAYERS");
    }

    #[test]
    fn rollup_layers_none_returns_full_order() {
        assert_eq!(rollup_layers(None), ROLLUP_LAYER_ORDER.to_vec());
    }

    #[test]
    fn rollup_layers_preserves_order_for_enabled_subset() {
        let enabled: HashSet<Layer> =
            [Layer::Combined, Layer::Flarm, Layer::Ogntrk].into_iter().collect();
        assert_eq!(
            rollup_layers(Some(&enabled)),
            vec![Layer::Ogntrk, Layer::Flarm, Layer::Combined]
        );
    }
}
