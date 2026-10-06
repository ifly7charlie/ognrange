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

// Why a coverage floor isn't proof at the cell's distance, as carried in the
// disc's receiveExtended array. The two cases need different wording: PROVEN
// means a real breakpoint at or beyond the cell governs, SKYLINE means the
// floor was lowered onto the terrain horizon by the measured margin, and
// BEYOND means the last breakpoint's angle is simply being carried outward
// past the furthest thing ever heard on that arc
export const RECEIVE_PROVEN = 0;
export const RECEIVE_SKYLINE_EXTENDED = 1;
export const RECEIVE_BEYOND_PROVEN = 2;

// ADS-B is the only 1090MHz layer; everything else receives on 868. Shared so
// the map and the details panel can never disagree about which cap applies
export function floorFrequencyFor(layersParam: string | null | undefined): 868 | 1090 {
    return (layersParam || 'combined') === 'adsb' ? 1090 : 868;
}

// The receive-horizon file backing the floor for the viewed period. Horizon
// files exist for month/year/yearnz, but the floor always uses a year-scale
// horizon (the terrain doesn't change and the yearly minimum is the best
// estimate): current periods use the year/yearnz symlink, a past year its
// dated (frozen) file
export function floorHorizonFileFor(dateStart: string | undefined): string {
    if (dateStart === 'yearnz') {
        return 'yearnz';
    }
    const nz = dateStart?.match(/^(\d{4})nz$/);
    if (nz) {
        return `yearnz.${nz[1]}nz`;
    }
    const dated = dateStart?.match(/^(\d{4})(-\d{2})?(-\d{2})?$/);
    if (dated) {
        return `year.${dated[1]}`;
    }
    return 'year';
}
