import type {Table} from 'apache-arrow';
import {gridDisk, latLngToCell, cellToLatLng, h3IndexToSplitLong, greatCircleDistance} from 'h3-js';

import {elevationAngleDeg, heightAtDistance, BAND_KEYS, BAND_RANGES_KM, BIN_COUNT, BIN_DEG} from './coveragedetails/horizondata';
import {GROUND_SAMPLES, GROUND_STEP_KM, GROUND_MAX_KM, groundMeta} from './coveragedetails/grounddata';
import {FLOOR_UNKNOWN} from '../common/floor';
import {H3_STATION_CELL_LEVEL} from '../common/config';

// The "coverage floor" map layer: for every res-8 cell in the station's 120km
// terrain disc, the lowest altitude (m MSL) at which an aircraft clears the
// terrain skyline between it and the station (terrainFloor), and additionally
// the station's measured receive horizon (coverageFloor). Computed from the
// same two arrow files the detail charts use - the per-hovered-bearing shadow
// envelope in grounddetails.tsx, swept over the whole disc. Pure functions
// only: this module is shared by floorworker.ts and the tests

// Half the across-flats width of an H3 res-8 cell (matches the receive-horizon
// writer, aprs-server/src/horizon.rs CELL_HALF_WIDTH_KM)
const CELL_HALF_WIDTH_KM = 0.4;

// Standard initial great-circle bearing from point 1 to point 2, degrees [0, 360)
export function initialBearingDeg(lat1: number, lng1: number, lat2: number, lng2: number): number {
    const p1 = (lat1 * Math.PI) / 180;
    const p2 = (lat2 * Math.PI) / 180;
    const dl = ((lng2 - lng1) * Math.PI) / 180;
    const y = Math.sin(dl) * Math.cos(p2);
    const x = Math.cos(p1) * Math.sin(p2) - Math.sin(p1) * Math.cos(p2) * Math.cos(dl);
    const deg = (Math.atan2(y, x) * 180) / Math.PI;
    return ((deg % 360) + 360) % 360;
}

// Bin span subtended by a cell's ~0.8km width at distanceKm, as
// [firstBin, binCount] with wraparound; always at least one bin. Port of the
// writer's arc spreading (horizon.rs bin_span) so a cell reads the same bins
// its packets were recorded into
export function binSpan(bearingDeg: number, distanceKm: number): [number, number] {
    const halfDeg = (Math.atan2(CELL_HALF_WIDTH_KM, distanceKm) * 180) / Math.PI;
    const first = Math.floor((bearingDeg - halfDeg) / BIN_DEG);
    const last = Math.floor((bearingDeg + halfDeg) / BIN_DEG);
    const count = Math.min(Math.max(last - first + 1, 1), BIN_COUNT);
    return [((first % BIN_COUNT) + BIN_COUNT) % BIN_COUNT, count];
}

// Terrain rays reshaped for whole-disc lookups: elevations flattened to one
// row-major array plus, per (bin, sample), the running-max elevation angle
// from the antenna over all samples up to that distance - the angle an
// aircraft must clear to be line-of-sight at that (bearing, distance)
export interface TerrainGrid {
    stationLat: number;
    stationLng: number;
    // Antenna m MSL: ray sample 0 + the file's stationAgl metadata
    viewpoint: number;
    // BIN_COUNT x GROUND_SAMPLES row-major, m MSL
    elev: Int16Array;
    // Running-max elevation angle (deg); NaN where the bin is missing
    prefixMax: Float32Array;
}

export function terrainGridFromTable(table: Table): TerrainGrid | null {
    const bearing = table.getChild('bearing');
    const elevations = table.getChild('elevations');
    const meta = groundMeta(table);
    if (!bearing || !elevations || meta.stationLat == null || meta.stationLng == null) {
        return null;
    }

    const elev = new Int16Array(BIN_COUNT * GROUND_SAMPLES);
    const prefixMax = new Float32Array(BIN_COUNT * GROUND_SAMPLES).fill(NaN);
    let viewpoint: number | null = null;
    for (let i = 0; i < table.numRows; i++) {
        const cell = elevations.get(i);
        if (!cell) {
            continue;
        }
        const bin = (Math.round((bearing.get(i) as number) / BIN_DEG) % BIN_COUNT + BIN_COUNT) % BIN_COUNT;
        const values = cell.toArray() as ArrayLike<number>;
        viewpoint = viewpoint ?? (values[0] ?? 0) + meta.stationAgl;
        const base = bin * GROUND_SAMPLES;
        elev[base] = values[0];
        let maxAngle = -Infinity;
        for (let s = 1; s < GROUND_SAMPLES; s++) {
            elev[base + s] = values[s];
            const angle = elevationAngleDeg(values[s] - viewpoint, s * GROUND_STEP_KM);
            if (angle > maxAngle) {
                maxAngle = angle;
            }
            prefixMax[base + s] = maxAngle;
        }
    }
    if (viewpoint == null) {
        return null;
    }
    return {stationLat: meta.stationLat, stationLng: meta.stationLng, viewpoint, elev, prefixMax};
}

// Receive-horizon band angles for one frequency reshaped to per-bin arrays;
// NaN = nothing received (no data, not "bad reception" - absence leaves the
// coverage floor terrain-only)
export interface ReceiveAngles {
    bands: Float32Array[]; // BAND_KEYS order
}

export function receiveAnglesFromTable(table: Table, frequency: number): ReceiveAngles | null {
    const freqCol = table.getChild('frequency');
    const bearingCol = table.getChild('bearing');
    if (!freqCol || !bearingCol) {
        return null;
    }
    const bandCols = BAND_KEYS.map((k) => table.getChild(k));
    const bands = BAND_KEYS.map(() => new Float32Array(BIN_COUNT).fill(NaN));
    let any = false;
    for (let i = 0; i < table.numRows; i++) {
        if (freqCol.get(i) !== frequency) {
            continue;
        }
        const bin = (Math.round((bearingCol.get(i) as number) / BIN_DEG) % BIN_COUNT + BIN_COUNT) % BIN_COUNT;
        for (let bi = 0; bi < bands.length; bi++) {
            const v = bandCols[bi]?.get(i);
            bands[bi][bin] = v == null ? NaN : v;
        }
        any = true;
    }
    return any ? {bands} : null;
}

// A band's constraint reaches inward: reception proven at its angle within
// [start, end] holds for every closer distance too (same angle, stronger
// signal). Beyond the outermost band with data the constraint continues at
// that band's angle - a horizon angle doesn't drop with distance, and letting
// the constraint vanish at the band edge made the floor collapse back to
// terrain mid-ray (receive-coloured, then a clipped gap, then lower floors
// further out). This keeps the floor monotone with distance along every
// bearing. lowestAngle stays deliberately unused: it mixes <5km near-field
// steepness into every distance
const BAND_REACH_KM = BAND_KEYS.map((k) => BAND_RANGES_KM[k][1]);

// Minimum angle anything was received at that constrains distanceKm: min
// across applicable bands within a bin, then min across the cell's bins with
// data - coverage anywhere in the cell counts, and one bin that only ever
// heard steep near-field traffic must not poison a cell that also straddles a
// well-proven bin. Min across the arc is also what keeps the floor monotone
// with distance: the arc narrows as cells get further out, and a min can only
// rise as bins drop out where a max could fall (constraint relaxing with
// distance = the clipped-gap-then-recovery artifact). -Infinity only when no
// subtended bin has any band data
export function receiveAngleAt(receive: ReceiveAngles, firstBin: number, binCount: number, distanceKm: number): number {
    let best = Infinity;
    for (let i = 0; i < binCount; i++) {
        const b = (firstBin + i) % BIN_COUNT;
        let binAngle = Infinity;
        let outermost = Infinity;
        for (let k = 0; k < BAND_REACH_KM.length; k++) {
            const v = receive.bands[k][b];
            if (Number.isNaN(v)) {
                continue;
            }
            outermost = v;
            if (distanceKm <= BAND_REACH_KM[k] && v < binAngle) {
                binAngle = v;
            }
        }
        // Past the outermost populated band's edge its observation governs
        if (binAngle === Infinity) {
            binAngle = outermost;
        }
        if (binAngle < best) {
            best = binAngle;
        }
    }
    return best === Infinity ? -Infinity : best;
}

export interface FloorDisc {
    h3lo: Uint32Array;
    h3hi: Uint32Array;
    // Cell ground m MSL from the nearest terrain-ray sample (~1km resolution
    // in the far field - fine for readouts, not a DEM)
    ground: Int16Array;
    terrainFloor: Int16Array;
    coverageFloor: Int16Array;
    // The angles behind the floors, for the hover details: the terrain
    // skyline angle governing the cell and the receive-horizon angle used
    // (NaN = unknown terrain / nothing received) - carried so the readout
    // always matches the floor exactly rather than re-deriving it
    terrainAngle: Float32Array;
    receiveAngle: Float32Array;
    length: number;
}

// gridDisk rings needed to reach GROUND_MAX_KM: res-8 centre spacing dips to
// ~0.66km where the icosahedron distorts, so 200 rings plus the per-cell
// distance filter always covers the 120km disc
const DISC_RINGS = 200;

function clampFloor(v: number): number {
    return Number.isFinite(v) ? Math.max(-32768, Math.min(FLOOR_UNKNOWN - 1, Math.round(v))) : FLOOR_UNKNOWN;
}

export function computeFloorDisc(groundTable: Table, horizonTable: Table | null, frequency: number, onProgress?: (fraction: number) => void): FloorDisc | null {
    const grid = terrainGridFromTable(groundTable);
    if (!grid) {
        return null;
    }
    const receive = horizonTable ? receiveAnglesFromTable(horizonTable, frequency) : null;

    const cells = gridDisk(latLngToCell(grid.stationLat, grid.stationLng, H3_STATION_CELL_LEVEL), DISC_RINGS);
    const h3lo = new Uint32Array(cells.length);
    const h3hi = new Uint32Array(cells.length);
    const ground = new Int16Array(cells.length);
    const terrainFloor = new Int16Array(cells.length);
    const coverageFloor = new Int16Array(cells.length);
    const terrainAngleOut = new Float32Array(cells.length);
    const receiveAngleOut = new Float32Array(cells.length);

    let n = 0;
    for (let ci = 0; ci < cells.length; ci++) {
        const cell = cells[ci];
        if (onProgress && (ci & 0x1fff) === 0) {
            onProgress(ci / cells.length);
        }
        const [clat, clng] = cellToLatLng(cell);
        const d = greatCircleDistance([grid.stationLat, grid.stationLng], [clat, clng], 'km');
        if (d > GROUND_MAX_KM) {
            continue;
        }
        // The station's own cell: bearing is meaningless, clamp to the first ray sample
        const bearing = d > 1e-6 ? initialBearingDeg(grid.stationLat, grid.stationLng, clat, clng) : 0;
        const nearestBin = Math.round(bearing / BIN_DEG) % BIN_COUNT;
        const [firstBin, binCount] = binSpan(bearing, d);
        const s = Math.min(Math.max(Math.round(d / GROUND_STEP_KM), 1), GROUND_SAMPLES - 1);

        // Terrain is cautious where receive is optimistic: max across the
        // subtended bins ("the whole cell clears the skyline") - terrain rays
        // are physically continuous so adjacent bins barely differ
        let terrainAngle = -Infinity;
        for (let i = 0; i < binCount; i++) {
            const v = grid.prefixMax[((firstBin + i) % BIN_COUNT) * GROUND_SAMPLES + s];
            if (!Number.isNaN(v) && v > terrainAngle) {
                terrainAngle = v;
            }
        }

        const [lo, hi] = h3IndexToSplitLong(cell);
        h3lo[n] = lo;
        h3hi[n] = hi;
        ground[n] = grid.elev[nearestBin * GROUND_SAMPLES + s];
        if (terrainAngle > -Infinity) {
            terrainFloor[n] = clampFloor(grid.viewpoint + heightAtDistance(terrainAngle, d));
            terrainAngleOut[n] = terrainAngle;
            const rxAngle = receive ? receiveAngleAt(receive, firstBin, binCount, d) : -Infinity;
            if (receive && rxAngle === -Infinity) {
                // The station has receive data, but not one packet was ever
                // heard over this cell's arc: that is evidence of NO likely
                // coverage, not licence to fall back to the terrain floor -
                // painting these cells terrain-coloured next to clipped
                // neighbours made coverage look better with distance
                coverageFloor[n] = FLOOR_UNKNOWN;
                receiveAngleOut[n] = NaN;
            } else {
                coverageFloor[n] = clampFloor(grid.viewpoint + heightAtDistance(Math.max(terrainAngle, rxAngle), d));
                receiveAngleOut[n] = rxAngle > -Infinity ? rxAngle : NaN;
            }
        } else {
            // Every subtended bin missing from the file - unknown, not zero
            terrainFloor[n] = FLOOR_UNKNOWN;
            coverageFloor[n] = FLOOR_UNKNOWN;
            terrainAngleOut[n] = NaN;
            receiveAngleOut[n] = NaN;
        }
        n++;
    }

    return {
        h3lo: h3lo.slice(0, n),
        h3hi: h3hi.slice(0, n),
        ground: ground.slice(0, n),
        terrainFloor: terrainFloor.slice(0, n),
        coverageFloor: coverageFloor.slice(0, n),
        terrainAngle: terrainAngleOut.slice(0, n),
        receiveAngle: receiveAngleOut.slice(0, n),
        length: n
    };
}
