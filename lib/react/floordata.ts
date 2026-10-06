import type {Table} from 'apache-arrow';
import {gridDisk, latLngToCell, cellToLatLng, h3IndexToSplitLong, greatCircleDistance} from 'h3-js';

import {elevationAngleDeg, heightAtDistance, breakpointColumns, BIN_COUNT, BIN_DEG, SKYLINE_TOLERANCE_DEG} from './coveragedetails/horizondata';
import {GROUND_SAMPLES, GROUND_STEP_KM, GROUND_MAX_KM, DEFAULT_STATION_AGL_M, groundMeta} from './coveragedetails/grounddata';
import {FLOOR_UNKNOWN, RECEIVE_PROVEN, RECEIVE_SKYLINE_EXTENDED, RECEIVE_BEYOND_PROVEN} from '../common/floor';
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

// Do the bin windows two bearings subtend at the same distance overlap? A
// floor curve drawn along one bearing reads binSpan(bearing, km), so a cell
// only feeds that curve where its own recorded span touches the same bins.
// Deliberately wider than comparing the bearings directly: binSpan floors to
// bin edges, so it reaches up to a further bin either side, and a cell just
// outside the bearing tolerance can still be the one setting the floor
export function binSpansOverlap(bearingA: number, bearingB: number, distanceKm: number): boolean {
    const [a, an] = binSpan(bearingA, distanceKm);
    const [b, bn] = binSpan(bearingB, distanceKm);
    // Two arcs on a circle overlap when either one's first bin lies in the other
    return (b - a + BIN_COUNT) % BIN_COUNT < an || (a - b + BIN_COUNT) % BIN_COUNT < bn;
}

// Terrain rays reshaped for whole-disc lookups: elevations flattened to one
// row-major array plus, per (bin, sample), the running-max elevation angle
// from the antenna over all samples up to that distance - the angle an
// aircraft must clear to be line-of-sight at that (bearing, distance)
export interface TerrainGrid {
    stationLat: number;
    stationLng: number;
    // Station ground m MSL - ray sample 0; the display ceiling is relative
    // to this, not to sea level
    stationGround: number;
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
    let stationGround: number | null = null;
    let viewpoint: number | null = null;
    for (let i = 0; i < table.numRows; i++) {
        const cell = elevations.get(i);
        if (!cell) {
            continue;
        }
        const bin = ((Math.round((bearing.get(i) as number) / BIN_DEG) % BIN_COUNT) + BIN_COUNT) % BIN_COUNT;
        const values = cell.toArray() as ArrayLike<number>;
        stationGround = stationGround ?? values[0] ?? 0;
        viewpoint = viewpoint ?? stationGround + (meta.stationAgl ?? DEFAULT_STATION_AGL_M);
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
    if (viewpoint == null || stationGround == null) {
        return null;
    }
    return {stationLat: meta.stationLat, stationLng: meta.stationLng, stationGround, viewpoint, elev, prefixMax};
}

// Nearest terrain-ray sample governing a distance, clamped into the sampled
// range - shared by every prefixMax lookup so all consumers agree
export function sampleIndex(distanceKm: number): number {
    return Math.min(Math.max(Math.round(distanceKm / GROUND_STEP_KM), 1), GROUND_SAMPLES - 1);
}

// Receive-horizon envelopes for one frequency reshaped to flat per-bin
// arrays: up to k breakpoints per bin, ascending in both distance and angle,
// NaN-padded. len = 0 means nothing received on that bearing (no data, not
// "bad reception" - absence leaves the coverage floor terrain-only)
export interface ReceiveEnvelope {
    km: Float32Array; // BIN_COUNT x k row-major
    angle: Float32Array;
    len: Uint8Array;
    k: number;
}

// Reshape one frequency's rows, mirroring the writer's below-skyline filter
// against the terrain grid: a breakpoint claiming reception more than
// SKYLINE_TOLERANCE_DEG below the skyline at its distance is a corrupt
// position/altitude, and under min-semantics one such point poisons the whole
// ray. The writer already filters when the station has a ground-horizon file;
// this mirror covers files written before it existed (first rollup of a new
// station, mobile stations)
export function receiveEnvelopeFromTable(table: Table, frequency: number, grid: TerrainGrid | null): ReceiveEnvelope | null {
    const freqCol = table.getChild('frequency');
    const bearingCol = table.getChild('bearing');
    const cols = breakpointColumns(table);
    if (!freqCol || !bearingCol || !cols.length) {
        return null;
    }
    const k = cols.length;
    const km = new Float32Array(BIN_COUNT * k).fill(NaN);
    const angle = new Float32Array(BIN_COUNT * k).fill(NaN);
    const len = new Uint8Array(BIN_COUNT);
    let any = false;
    for (let i = 0; i < table.numRows; i++) {
        if (freqCol.get(i) !== frequency) {
            continue;
        }
        const bin = ((Math.round((bearingCol.get(i) as number) / BIN_DEG) % BIN_COUNT) + BIN_COUNT) % BIN_COUNT;
        let n = 0;
        for (const c of cols) {
            const d = c.km.get(i);
            const a = c.angle.get(i);
            if (d == null || a == null) {
                break;
            }
            if (grid) {
                const limit = grid.prefixMax[bin * GROUND_SAMPLES + sampleIndex(d)];
                if (!Number.isNaN(limit) && a < limit - SKYLINE_TOLERANCE_DEG) {
                    continue;
                }
            }
            km[bin * k + n] = d;
            angle[bin * k + n] = a;
            n++;
        }
        len[bin] = n;
        if (n) {
            any = true;
        }
    }
    return any ? {km, angle, len, k} : null;
}

// Per-bin measured skyline margin: how far above its own terrain horizon the
// station's proven receptions sit, at best. A bin whose envelope hugs the
// skyline (margin ~0) is terrain-limited - the receiver demonstrably hears
// down to its physical horizon, so absence of low far receptions is traffic
// distribution, not radio - while a bin that only ever heard high traffic
// keeps a large margin and stays evidence-bound. NaN = no usable breakpoints
// or no terrain along the bin
export function envelopeMargins(receive: ReceiveEnvelope, grid: TerrainGrid): Float32Array {
    const margins = new Float32Array(BIN_COUNT).fill(NaN);
    for (let b = 0; b < BIN_COUNT; b++) {
        let m = NaN;
        for (let j = 0; j < receive.len[b]; j++) {
            const prefix = grid.prefixMax[b * GROUND_SAMPLES + sampleIndex(receive.km[b * receive.k + j])];
            if (Number.isNaN(prefix)) {
                continue;
            }
            const v = Math.max(receive.angle[b * receive.k + j] - prefix, 0);
            if (Number.isNaN(m) || v < m) {
                m = v;
            }
        }
        margins[b] = m;
    }
    return margins;
}

// How much lower the margin extension must sit before it counts as having
// replaced the staircase rather than tied with it. For a bin with a single
// breakpoint over terrain whose skyline has already plateaued by that
// breakpoint's distance, skyline + margin IS the breakpoint's own angle by
// construction, so the comparison lands on float32 rounding (~1e-8 deg) and
// the `extended` flag flips between neighbouring bins carrying identical
// evidence. A tenth of a milli-degree is under a metre of floor at 120km
const EXTENSION_TIE_DEG = 1e-4;

// Minimum angle likely receivable at distanceKm: per bin the envelope
// staircase f(d) - the first breakpoint at or beyond the distance, its angle
// continuing outward past the last one - capped by the skyline-plus-margin
// extension min(f(d), prefixMax + margin), then min across the cell's
// subtended bins (coverage anywhere in the cell counts, and the narrowing
// arc means a min can only rise with distance - floors stay monotone along
// every ray). Reception proven at an angle holds for every closer distance
// too (same angle, stronger signal), which is what the staircase encodes.
// extended = the winning bin's constraint came from the margin extension,
// not the staircase itself; provenKm = that bin's furthest breakpoint, so a
// caller can tell whether the distance is inside the measured range or on the
// outward continuation (both surfaced in the hover details). angle -Infinity
// only when no subtended bin has any breakpoints
export function envelopeAngleAt(receive: ReceiveEnvelope, margins: Float32Array | null, grid: TerrainGrid, firstBin: number, binCount: number, distanceKm: number): {angle: number; extended: boolean; provenKm: number} {
    let best = Infinity;
    let extended = false;
    let provenKm = 0;
    const s = sampleIndex(distanceKm);
    for (let i = 0; i < binCount; i++) {
        const b = (firstBin + i) % BIN_COUNT;
        const n = receive.len[b];
        if (!n) {
            continue;
        }
        let f = receive.angle[b * receive.k + n - 1];
        for (let j = 0; j < n; j++) {
            if (receive.km[b * receive.k + j] >= distanceKm) {
                f = receive.angle[b * receive.k + j];
                break;
            }
        }
        let binExtended = false;
        const margin = margins ? margins[b] : NaN;
        if (!Number.isNaN(margin)) {
            const capped = grid.prefixMax[b * GROUND_SAMPLES + s] + margin;
            if (capped < f - EXTENSION_TIE_DEG) {
                // NaN prefix fails the comparison, leaving the staircase
                f = capped;
                binExtended = true;
            }
        }
        if (f < best) {
            best = f;
            extended = binExtended;
            provenKm = receive.km[b * receive.k + n - 1];
        }
    }
    return best === Infinity ? {angle: -Infinity, extended: false, provenKm: 0} : {angle: best, extended, provenKm};
}

export interface FloorDisc {
    h3lo: Uint32Array;
    h3hi: Uint32Array;
    // Station ground m MSL: the FLOOR_DISPLAY_MAX_M ceiling is measured from
    // here so a mountain station isn't clipped to nothing
    stationGround: number;
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
    // Why the receive constraint isn't proof at this distance, one of the
    // RECEIVE_* codes: a proven breakpoint governs, the skyline+margin
    // extension does, or the last breakpoint's angle is being carried outward
    // past anything ever heard on the arc (the details explain which)
    receiveExtended: Uint8Array;
    length: number;
}

// gridDisk rings needed to reach maxKm: res-8 centre spacing dips to ~0.66km
// where the icosahedron distorts, so ceil(maxKm/0.66) rings plus the per-cell
// distance filter always covers the disc; +18 margin reproduces the previous
// fixed 200 rings at the full 120km
const DISC_RINGS_MAX = 200;
const discRings = (maxKm: number) => Math.min(DISC_RINGS_MAX, Math.ceil(maxKm / 0.66) + 18);

function clampFloor(v: number): number {
    return Number.isFinite(v) ? Math.max(-32768, Math.min(FLOOR_UNKNOWN - 1, Math.round(v))) : FLOOR_UNKNOWN;
}

export function computeFloorDisc(groundTable: Table, horizonTable: Table | null, frequency: number, maxKm: number = GROUND_MAX_KM, onProgress?: (fraction: number) => void): FloorDisc | null {
    const grid = terrainGridFromTable(groundTable);
    if (!grid) {
        return null;
    }
    // The terrain rays end at GROUND_MAX_KM, so a capability-derived range can
    // only shrink the disc, never extend it
    const rangeKm = maxKm > 0 ? Math.min(maxKm, GROUND_MAX_KM) : GROUND_MAX_KM;
    const receive = horizonTable ? receiveEnvelopeFromTable(horizonTable, frequency, grid) : null;
    const margins = receive ? envelopeMargins(receive, grid) : null;

    const cells = gridDisk(latLngToCell(grid.stationLat, grid.stationLng, H3_STATION_CELL_LEVEL), discRings(rangeKm));
    const h3lo = new Uint32Array(cells.length);
    const h3hi = new Uint32Array(cells.length);
    const ground = new Int16Array(cells.length);
    const terrainFloor = new Int16Array(cells.length);
    const coverageFloor = new Int16Array(cells.length);
    const terrainAngleOut = new Float32Array(cells.length);
    const receiveAngleOut = new Float32Array(cells.length);
    const receiveExtendedOut = new Uint8Array(cells.length);

    let n = 0;
    for (let ci = 0; ci < cells.length; ci++) {
        const cell = cells[ci];
        if (onProgress && (ci & 0x1fff) === 0) {
            onProgress(ci / cells.length);
        }
        const [clat, clng] = cellToLatLng(cell);
        const d = greatCircleDistance([grid.stationLat, grid.stationLng], [clat, clng], 'km');
        if (d > rangeKm) {
            continue;
        }
        // The station's own cell: bearing is meaningless, clamp to the first ray sample
        const bearing = d > 1e-6 ? initialBearingDeg(grid.stationLat, grid.stationLng, clat, clng) : 0;
        const nearestBin = Math.round(bearing / BIN_DEG) % BIN_COUNT;
        const [firstBin, binCount] = binSpan(bearing, d);
        const s = sampleIndex(d);

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
            const rx = receive ? envelopeAngleAt(receive, margins, grid, firstBin, binCount, d) : null;
            if (receive && rx && rx.angle === -Infinity) {
                // The station has receive data, but not one packet was ever
                // heard over this cell's arc: that is evidence of NO likely
                // coverage, not licence to fall back to the terrain floor -
                // painting these cells terrain-coloured next to clipped
                // neighbours made coverage look better with distance
                coverageFloor[n] = FLOOR_UNKNOWN;
                receiveAngleOut[n] = NaN;
            } else {
                const rxAngle = rx ? rx.angle : -Infinity;
                coverageFloor[n] = clampFloor(grid.viewpoint + heightAtDistance(Math.max(terrainAngle, rxAngle), d));
                receiveAngleOut[n] = rxAngle > -Infinity ? rxAngle : NaN;
                receiveExtendedOut[n] = rx?.extended ? RECEIVE_SKYLINE_EXTENDED : rx && d > rx.provenKm ? RECEIVE_BEYOND_PROVEN : RECEIVE_PROVEN;
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
        stationGround: grid.stationGround,
        ground: ground.slice(0, n),
        terrainFloor: terrainFloor.slice(0, n),
        coverageFloor: coverageFloor.slice(0, n),
        terrainAngle: terrainAngleOut.slice(0, n),
        receiveAngle: receiveAngleOut.slice(0, n),
        receiveExtended: receiveExtendedOut.slice(0, n),
        length: n
    };
}
