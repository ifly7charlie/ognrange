// Coverage-floor visualisation constants, shared by the floor worker, the map
// and the selector

// "No floor computable" sentinel in the Int16 result arrays. Int16 max acts as
// +infinity so a future multi-station merge is a plain Math.min with no null
// handling
export const FLOOR_UNKNOWN = 32767;

// Cells whose floor is more than this above the STATION's ground level (not
// sea level - an Alpine station would otherwise clip to nothing) are not
// drawn: coverage that only exists that high isn't useful, and the clip lets
// the colour ramp span 0-2400m above the station at ~10m per step
export const FLOOR_DISPLAY_MAX_M = 2400;

// Map visualisations computed from the floor disc rather than the coverage
// arrow files; only offered when a station is selected
export const FLOOR_VISUALISATIONS = ['coverageFloor', 'terrainFloor'] as const;
export const FLOOR_VISUALISATION_SET: Set<string> = new Set(FLOOR_VISUALISATIONS);

export function isFloorKnown(v: number): boolean {
    return v !== FLOOR_UNKNOWN;
}

// ADS-B is the only 1090MHz layer; everything else receives on 868. Shared so
// the map and the details panel can never disagree about which cap applies
export function floorFrequencyFor(layersParam: string | null | undefined): 868 | 1090 {
    return (layersParam || 'combined') === 'adsb' ? 1090 : 868;
}
