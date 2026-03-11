// Layer definitions for multi-protocol coverage tracking

export enum Layer {
    COMBINED = 'combined',
    FLARM = 'flarm',
    ADSB = 'adsb',
    ADSL = 'adsl',
    FANET = 'fanet',
    OGNTRK = 'ogntrk',
    PAW = 'paw'
}

// APRS TOCALL (destCallsign) → Layer mapping
const TOCALL_TO_LAYER: Record<string, Layer> = {
    OGFLR: Layer.FLARM,
    OGADSB: Layer.ADSB,
    OGADSL: Layer.ADSL,
    OGNFNT: Layer.FANET,
    OGNTRK: Layer.OGNTRK,
    OGPAW: Layer.PAW
};

// DB key prefix per layer — sorted alphabetically for contiguous iteration
const LAYER_DB_PREFIX: Record<Layer, string> = {
    [Layer.ADSB]: 'a/',
    [Layer.COMBINED]: 'c/',
    [Layer.ADSL]: 'd/',
    [Layer.FLARM]: 'f/',
    [Layer.FANET]: 'n/',
    [Layer.PAW]: 'p/',
    [Layer.OGNTRK]: 't/'
};

// Reverse lookup: prefix char → Layer
const PREFIX_TO_LAYER: Record<string, Layer> = {};
for (const [layer, prefix] of Object.entries(LAYER_DB_PREFIX)) {
    PREFIX_TO_LAYER[prefix[0]] = layer as Layer;
}

// Protocols that store synthetic signal (no real dB value available)
export const PRESENCE_ONLY: ReadonlySet<Layer> = new Set([Layer.ADSB, Layer.PAW]);

// Synthetic signal value for presence-only layers (4 = ~1.0dB equivalent)
export const PRESENCE_SIGNAL = 4;

// Protocols that also write to the combined layer
export const COMBINED_LAYERS: ReadonlySet<Layer> = new Set([Layer.FLARM, Layer.OGNTRK]);

// All layers in DB sort order
export const ALL_LAYERS: readonly Layer[] = [Layer.ADSB, Layer.COMBINED, Layer.ADSL, Layer.FLARM, Layer.FANET, Layer.PAW, Layer.OGNTRK];

export function layerFromDestCallsign(destCallsign: string): Layer | null {
    return TOCALL_TO_LAYER[destCallsign] ?? null;
}

export function dbKeyPrefix(layer: Layer): string {
    return LAYER_DB_PREFIX[layer];
}

export function layerFromPrefix(prefix: string): Layer | null {
    return PREFIX_TO_LAYER[prefix[0]] ?? null;
}

/** Returns the set of layers a packet should be written to */
export function getWriteLayers(layer: Layer): Layer[] {
    if (COMBINED_LAYERS.has(layer)) {
        return [Layer.COMBINED, layer];
    }
    return [layer];
}

// Bit position for each layer in the layerMask coverage bitmask (Uint8, max 7 bits)
export const LAYER_BIT: Record<Layer, number> = {
    [Layer.COMBINED]: 0,
    [Layer.FLARM]:    1,
    [Layer.ADSB]:     2,
    [Layer.ADSL]:     3,
    [Layer.FANET]:    4,
    [Layer.PAW]:      5,
    [Layer.OGNTRK]:   6
};

// Display color per layer for the layerCoverage visualisation [R, G, B]
export const LAYER_COLOR: Record<Layer, [number, number, number]> = {
    [Layer.COMBINED]: [155,  89, 182], // purple
    [Layer.FLARM]:    [230, 126,  34], // orange
    [Layer.ADSB]:     [ 26, 188, 156], // emerald
    [Layer.ADSL]:     [ 52, 152, 219], // blue
    [Layer.FANET]:    [ 46, 204, 113], // green
    [Layer.PAW]:      [241, 196,  15], // yellow
    [Layer.OGNTRK]:   [231,  76,  60]  // red
};

/** Returns the layerMask bit value for a given Arrow file URL */
export function layerBitFromUrl(url: string): number {
    for (const [layer, bit] of Object.entries(LAYER_BIT)) {
        if (url.endsWith(`.${layer}.arrow`)) return 1 << bit;
    }
    return 1 << LAYER_BIT[Layer.COMBINED];
}

/** Parse ENABLED_LAYERS env var. Returns null to mean "all layers enabled" */
export function parseEnabledLayers(envValue: string | undefined): Set<Layer> | null {
    if (!envValue) return null;
    const names = envValue.split(',').map((s) => s.trim().toLowerCase());
    const layers = new Set<Layer>();
    for (const name of names) {
        const layer = Object.values(Layer).find((l) => l === name);
        if (layer) {
            layers.add(layer);
        }
    }
    return layers.size > 0 ? layers : null;
}
