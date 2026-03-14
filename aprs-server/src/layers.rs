use std::collections::HashSet;

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

    /// DB key prefix per layer — sorted alphabetically for contiguous iteration
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
        "OGFLR" | "OGFLR7" | "APRS" => Some(Layer::Flarm),
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

/// Computes a layer bitmask from an iterable of layers
pub fn layer_mask_from_set(layers: &[Layer]) -> u8 {
    layers.iter().fold(0u8, |mask, l| mask | l.bit_mask())
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
