import {describe, it, expect} from 'vitest';
import {tableFromIPC, tableToIPC, vectorFromArray, Table, Schema, Field, Float32, Int16, Uint16, FixedSizeList} from 'apache-arrow';
import {splitLongToH3Index, cellToLatLng, greatCircleDistance} from 'h3-js';

import {initialBearingDeg, binSpan, binSpansOverlap, envelopeAngleAt, envelopeMargins, terrainGridFromTable, receiveEnvelopeFromTable, computeFloorDisc, FloorDisc} from '../../lib/react/floordata';
import {elevationAngleDeg, heightAtDistance, BIN_COUNT} from '../../lib/react/coveragedetails/horizondata';
import {GROUND_SAMPLES, GROUND_STEP_KM, GROUND_MAX_KM} from '../../lib/react/coveragedetails/grounddata';
import {FLOOR_UNKNOWN, FLOOR_DISPLAY_MAX_M, RECEIVE_PROVEN, RECEIVE_SKYLINE_EXTENDED, RECEIVE_BEYOND_PROVEN, floorHorizonFileFor} from '../../lib/common/floor';

// Same conventions as the Rust tests and grounddata.test.ts: station ground
// 500m, ridge reference 300m above ground at 20km -> 0.792 degrees. The disc
// tables carry all 720 bins (real ground-horizon files always do); sparseness
// is exercised separately
const STATION_LAT = 47;
const STATION_LNG = 8;
const GROUND = 500;

// Flat-ground running max is at the first sample: pure curvature dip
const FLAT_ANGLE = elevationAngleDeg(0, GROUND_STEP_KM);
const RIDGE_ANGLE = elevationAngleDeg(300, 40 * GROUND_STEP_KM);

function groundTable(rayFor: (bin: number) => number[], agl = 0): Table {
    const bearings = Array.from({length: BIN_COUNT}, (_, i) => i * 0.5);
    const elevType = new FixedSizeList(GROUND_SAMPLES, new Field('item', new Int16(), false));
    const table = new Table({
        bearing: vectorFromArray(bearings, new Float32()),
        elevations: vectorFromArray(bearings.map((_b, i) => rayFor(i)), elevType)
    });
    const metadata = new Map([
        ['stationLat', String(STATION_LAT)],
        ['stationLng', String(STATION_LNG)],
        ['stationAgl', String(agl)]
    ]);
    return tableFromIPC(tableToIPC(new Table(new Schema(table.schema.fields, metadata), table.batches)));
}

const flatRay = () => new Array(GROUND_SAMPLES).fill(GROUND);
// A ring ridge 300m above ground at 20km in every bearing
const ridgeRay = () => {
    const r = flatRay();
    r[40] = GROUND + 300;
    return r;
};

// A receive-horizon table in the envelope format: per row, up to 5
// (bp{j}Km, bp{j}Angle) breakpoint pairs, null-padded - ascending in both
// distance and angle like the writer's Pareto frontier
type Bp = {km: number; angle: number};
function envelopeRows(frequency: number, rows: {bin: number; bps: Bp[]}[]): Table {
    const n = rows.length;
    const cols: Record<string, ReturnType<typeof vectorFromArray>> = {
        frequency: vectorFromArray(new Array(n).fill(frequency), new Uint16()),
        bearing: vectorFromArray(
            rows.map((r) => r.bin * 0.5),
            new Float32()
        )
    };
    for (let j = 0; j < 5; j++) {
        cols[`bp${j}Km`] = vectorFromArray(
            rows.map((r) => r.bps[j]?.km ?? null),
            new Float32()
        );
        cols[`bp${j}Angle`] = vectorFromArray(
            rows.map((r) => r.bps[j]?.angle ?? null),
            new Float32()
        );
    }
    return tableFromIPC(tableToIPC(new Table(cols)));
}

// Rows only for the northern bins (0-20 and 700-719), all with the same
// breakpoints - the shape most tests want
function horizonTable(frequency: number, bps: Bp[]): Table {
    const bins = [...Array.from({length: 21}, (_, i) => i), ...Array.from({length: 20}, (_, i) => 700 + i)];
    return envelopeRows(
        frequency,
        bins.map((bin) => ({bin, bps}))
    );
}

// Distance/bearing of every disc cell from the station, computed back from the
// split H3 indexes so the whole h3 round-trip is covered
function locate(disc: FloorDisc) {
    return Array.from({length: disc.length}, (_, i) => {
        const [lat, lng] = cellToLatLng(splitLongToH3Index(disc.h3lo[i], disc.h3hi[i]));
        return {
            i,
            d: greatCircleDistance([STATION_LAT, STATION_LNG], [lat, lng], 'km'),
            bearing: initialBearingDeg(STATION_LAT, STATION_LNG, lat, lng)
        };
    });
}

// Nearest cell to (bearingDeg, dKm), only considering cells within a few
// degrees of the bearing so the ridge/receive sectors stay unambiguous
function cellAt(located: ReturnType<typeof locate>, bearingDeg: number, dKm: number) {
    let best: (typeof located)[number] | null = null;
    for (const c of located) {
        const db = Math.abs(((((c.bearing - bearingDeg) % 360) + 540) % 360) - 180);
        if (db > 3 && c.d > 1) {
            continue;
        }
        if (!best || Math.abs(c.d - dKm) < Math.abs(best.d - dKm)) {
            best = c;
        }
    }
    return best!;
}

describe('initialBearingDeg', () => {
    it('points the compass', () => {
        expect(initialBearingDeg(47, 8, 48, 8)).toBeCloseTo(0, 5);
        expect(initialBearingDeg(47, 8, 46, 8)).toBeCloseTo(180, 5);
        expect(initialBearingDeg(47, 8, 47, 8.1)).toBeCloseTo(90, 0);
        expect(initialBearingDeg(47, 8, 47, 7.9)).toBeCloseTo(270, 0);
    });
});

describe('binSpan', () => {
    it('spreads near cells over many bins and far cells over one or two', () => {
        const [, nearCount] = binSpan(45, 5);
        expect(nearCount).toBeGreaterThanOrEqual(18);
        expect(nearCount).toBeLessThanOrEqual(20);
        const [, farCount] = binSpan(45, 100);
        expect(farCount).toBeGreaterThanOrEqual(1);
        expect(farCount).toBeLessThanOrEqual(2);
    });

    it('wraps across north', () => {
        const [first, count] = binSpan(0, 5);
        expect(first).toBeGreaterThan(700);
        expect((first + count) % BIN_COUNT).toBeLessThan(20);
    });
});

describe('binSpansOverlap', () => {
    it('is true for a bearing whose own arc reaches the same bins', () => {
        // 0.4km half-width subtends 0.92 deg at 25km, so 47.0 spans bins
        // 92..95 (46.0-48.0 deg): a cell at 47.9 is 0.9 deg away - further
        // than the plain bearing tolerance allows in either direction - yet
        // its own arc lands in bin 95 and it can set the floor there
        expect(binSpansOverlap(47, 47.9, 25)).toBe(true);
        expect(binSpansOverlap(47, 46.1, 25)).toBe(true);
        // Two bins clear of the window, at a distance where the arc is narrow
        expect(binSpansOverlap(47, 49.5, 25)).toBe(false);
    });

    it('widens with proximity and is symmetric and wrap-safe', () => {
        // Same pair of bearings: touching close in, separate far out
        expect(binSpansOverlap(47, 50, 5)).toBe(true);
        expect(binSpansOverlap(47, 50, 100)).toBe(false);
        expect(binSpansOverlap(50, 47, 5)).toBe(true);
        // Across north, where the bin indexes run 719 -> 0
        expect(binSpansOverlap(0.1, 359.7, 20)).toBe(true);
        expect(binSpansOverlap(359.7, 0.1, 20)).toBe(true);
    });
});

describe('terrainGridFromTable', () => {
    it('builds running-max angles from the antenna viewpoint', () => {
        const grid = terrainGridFromTable(groundTable(ridgeRay, 0))!;
        expect(grid.viewpoint).toBe(GROUND);
        // Before the ridge the max is the first-sample curvature dip, after it the ridge
        expect(grid.prefixMax[39]).toBeCloseTo(FLAT_ANGLE, 5);
        expect(grid.prefixMax[40]).toBeCloseTo(RIDGE_ANGLE, 5);
        expect(grid.prefixMax[GROUND_SAMPLES - 1]).toBeCloseTo(RIDGE_ANGLE, 5);
    });

    it('raises the viewpoint by the stationAgl metadata', () => {
        const grid = terrainGridFromTable(groundTable(flatRay, 10))!;
        expect(grid.viewpoint).toBe(GROUND + 10);
        expect(grid.prefixMax[1]).toBeCloseTo(elevationAngleDeg(-10, GROUND_STEP_KM), 5);
    });
});

describe('computeFloorDisc: flat terrain', () => {
    const disc = computeFloorDisc(groundTable(flatRay), null, 868)!;
    const located = locate(disc);

    it('covers the whole 120km disc with no gaps at range', () => {
        expect(disc.length).toBeGreaterThan(50000);
        let maxD = 0;
        for (const c of located) {
            maxD = Math.max(maxD, c.d);
        }
        expect(maxD).toBeGreaterThan(119);
        expect(maxD).toBeLessThanOrEqual(120);
    });

    it('grows the floor with distance by pure earth curvature', () => {
        const near = cellAt(located, 0, 10);
        const far = cellAt(located, 0, 100);
        expect(disc.terrainFloor[near.i]).toBeCloseTo(GROUND + heightAtDistance(FLAT_ANGLE, near.d), 0);
        expect(disc.terrainFloor[far.i]).toBeCloseTo(GROUND + heightAtDistance(FLAT_ANGLE, far.d), 0);
        // ~586m of curvature at 100km
        expect(disc.terrainFloor[far.i]).toBeGreaterThan(GROUND + 500);
        expect(disc.terrainFloor[far.i]).toBeLessThan(GROUND + 700);
    });

    it('reports the flat ground under every cell', () => {
        const c = cellAt(located, 90, 50);
        expect(disc.ground[c.i]).toBe(GROUND);
    });

    it('equals the coverage floor with no receive horizon', () => {
        for (let i = 0; i < disc.length; i++) {
            expect(disc.coverageFloor[i]).toBe(disc.terrainFloor[i]);
        }
    });

    it('carries the governing angles for the hover readout', () => {
        const c = cellAt(located, 90, 50);
        expect(disc.terrainAngle[c.i]).toBeCloseTo(FLAT_ANGLE, 5);
        expect(disc.receiveAngle[c.i]).toBeNaN();
    });

    it('reports monotonic progress through the sweep', () => {
        const fractions: number[] = [];
        computeFloorDisc(groundTable(flatRay), null, 868, GROUND_MAX_KM, (f) => fractions.push(f));
        expect(fractions.length).toBeGreaterThan(2);
        expect(fractions[0]).toBe(0);
        for (let i = 1; i < fractions.length; i++) {
            expect(fractions[i]).toBeGreaterThan(fractions[i - 1]);
        }
        expect(fractions[fractions.length - 1]).toBeLessThanOrEqual(1);
    });
});

describe('computeFloorDisc: capability-reduced range', () => {
    const full = computeFloorDisc(groundTable(flatRay), null, 868)!;
    const reduced = computeFloorDisc(groundTable(flatRay), null, 868, 60)!;

    it('caps the disc at the reduced range', () => {
        let maxD = 0;
        for (const c of locate(reduced)) {
            maxD = Math.max(maxD, c.d);
        }
        expect(maxD).toBeGreaterThan(59);
        expect(maxD).toBeLessThanOrEqual(60);
    });

    it('produces a proportionally smaller disc', () => {
        // Area scales with radius squared: a 60km disc is ~a quarter of 120km
        expect(reduced.length).toBeLessThan(full.length / 3);
        expect(reduced.length).toBeGreaterThan(10000);
    });

    it('computes identical floors inside the reduced range', () => {
        const fullLocated = locate(full);
        const reducedLocated = locate(reduced);
        const f = cellAt(fullLocated, 0, 30);
        const r = cellAt(reducedLocated, 0, 30);
        expect(reduced.terrainFloor[r.i]).toBe(full.terrainFloor[f.i]);
    });

    it('never extends past the terrain rays regardless of the requested range', () => {
        const wild = computeFloorDisc(groundTable(flatRay), null, 868, 500)!;
        let maxD = 0;
        for (const c of locate(wild)) {
            maxD = Math.max(maxD, c.d);
        }
        expect(maxD).toBeLessThanOrEqual(GROUND_MAX_KM);
    });
});

describe('computeFloorDisc: ridge ring at 20km', () => {
    const disc = computeFloorDisc(groundTable(ridgeRay), null, 868)!;
    const located = locate(disc);

    it('floors cells beyond the ridge on its ray, leaves nearer cells alone', () => {
        const behind = cellAt(located, 180, 50);
        expect(disc.terrainFloor[behind.i]).toBeCloseTo(GROUND + heightAtDistance(RIDGE_ANGLE, behind.d), 0);
        const before = cellAt(located, 180, 10);
        expect(disc.terrainFloor[before.i]).toBeCloseTo(GROUND + heightAtDistance(FLAT_ANGLE, before.d), 0);
        expect(disc.terrainFloor[behind.i]).toBeGreaterThan(disc.terrainFloor[before.i]);
    });

    it('clips against the display ceiling far out', () => {
        // The 0.792 degree ridge ray reaches ~2400m above the station ground
        // around 110km out - the far field of this disc is exactly what the
        // (station-relative) FLOOR_DISPLAY_MAX_M ceiling clips
        const far = cellAt(located, 180, 119);
        expect(disc.terrainFloor[far.i]).toBeGreaterThan(GROUND + FLOOR_DISPLAY_MAX_M);
    });

    it('carries the station ground the display ceiling is measured from', () => {
        expect(disc.stationGround).toBe(GROUND);
    });
});

describe('computeFloorDisc: receive horizon', () => {
    const ground = groundTable(ridgeRay);

    it('applies the envelope, extends outward, and leaves silent arcs unknown', () => {
        // A single breakpoint proven at the ridge angle + 1.2 degrees
        const disc = computeFloorDisc(ground, horizonTable(868, [{km: 30, angle: 2.0}]), 868)!;
        const located = locate(disc);
        const north = cellAt(located, 0, 25);
        expect(disc.terrainFloor[north.i]).toBeCloseTo(GROUND + heightAtDistance(RIDGE_ANGLE, north.d), 0);
        expect(disc.coverageFloor[north.i]).toBeCloseTo(GROUND + heightAtDistance(2.0, north.d), 0);
        expect(disc.terrainAngle[north.i]).toBeCloseTo(RIDGE_ANGLE, 5);
        expect(disc.receiveAngle[north.i]).toBeCloseTo(2.0, 5);
        expect(disc.receiveExtended[north.i]).toBe(0);

        // Beyond the last breakpoint its observation keeps governing - the
        // constraint must not vanish, or the floor would collapse back to
        // terrain mid-ray
        const far = cellAt(located, 0, 100);
        expect(disc.receiveAngle[far.i]).toBeCloseTo(2.0, 4);
        expect(disc.coverageFloor[far.i]).toBeCloseTo(GROUND + heightAtDistance(2.0, far.d), 0);

        // BEFORE the ridge the skyline is lower, and the measured margin
        // (breakpoint angle minus the skyline at its distance) extends the
        // likely floor down with it: skyline + 1.2 instead of the full 2.0
        const near = cellAt(located, 0, 10);
        expect(disc.receiveAngle[near.i]).toBeGreaterThan(1.0);
        expect(disc.receiveAngle[near.i]).toBeLessThan(1.3);
        expect(disc.receiveExtended[near.i]).toBe(1);

        // No horizon rows to the south at all: the station demonstrably
        // receives (northern rows exist), so silence over this arc is
        // evidence of no likely coverage - unknown, not the terrain floor
        const south = cellAt(located, 180, 25);
        expect(disc.coverageFloor[south.i]).toBe(FLOOR_UNKNOWN);
        expect(disc.receiveAngle[south.i]).toBeNaN();
        expect(disc.terrainFloor[south.i]).not.toBe(FLOOR_UNKNOWN);
    });

    it('keeps the floor monotone along a ray - no clipped gap then recovery', () => {
        // The PWNOWZMA1 regression class: the receive constraint must never
        // relax with distance, so the map cannot show receive-floored cells,
        // a clipped gap, then LOWER terrain floors further out
        const disc = computeFloorDisc(ground, horizonTable(868, [{km: 30, angle: 4.2}]), 868)!;
        const located = locate(disc);
        let prev = -Infinity;
        for (const d of [5, 15, 25, 35, 60, 100, 119]) {
            const c = cellAt(located, 0, d);
            expect(disc.coverageFloor[c.i]).toBeGreaterThanOrEqual(prev);
            prev = disc.coverageFloor[c.i];
        }
        // and specifically: past the breakpoint it stays receive-governed
        const past = cellAt(located, 0, 45);
        expect(disc.receiveAngle[past.i]).toBeCloseTo(4.2, 5);
        expect(disc.coverageFloor[past.i]).toBeGreaterThan(GROUND + FLOOR_DISPLAY_MAX_M);
    });

    it('extends a steep near-field-only bin outward as clipped, never coloured', () => {
        // The EAGLES3 case: a bin that only ever heard traffic within 10km at
        // 15.9 degrees. Nothing was ever received below that angle, so the
        // constraint holds outward too - those cells clip out of the display
        // (consistently with their details) instead of extrapolating to an
        // absurd coloured value or dropping back to the terrain floor
        const disc = computeFloorDisc(ground, horizonTable(868, [{km: 8, angle: 15.9}]), 868)!;
        const located = locate(disc);
        const near = cellAt(located, 0, 8);
        expect(disc.receiveAngle[near.i]).toBeCloseTo(15.9, 5);
        expect(disc.coverageFloor[near.i]).toBeGreaterThan(disc.terrainFloor[near.i]);
        const far = cellAt(located, 0, 60);
        expect(disc.receiveAngle[far.i]).toBeCloseTo(15.9, 5);
        expect(disc.coverageFloor[far.i]).toBeGreaterThan(GROUND + FLOOR_DISPLAY_MAX_M);
        expect(disc.coverageFloor[far.i]).toBeGreaterThanOrEqual(disc.coverageFloor[near.i]);
        expect(disc.coverageFloor[far.i]).toBeLessThan(FLOOR_UNKNOWN);
    });

    it('ignores rows for the other frequency', () => {
        const disc = computeFloorDisc(ground, horizonTable(868, [{km: 30, angle: 2.0}]), 1090)!;
        const located = locate(disc);
        const north = cellAt(located, 0, 25);
        expect(disc.coverageFloor[north.i]).toBe(disc.terrainFloor[north.i]);
    });

    it('never lowers the floor below the terrain', () => {
        // Receive angle marginally below the ridge angle (within the skyline
        // tolerance, so the breakpoint survives): max() keeps the terrain floor
        const disc = computeFloorDisc(ground, horizonTable(868, [{km: 30, angle: 0.7}]), 868)!;
        const located = locate(disc);
        const north = cellAt(located, 0, 25);
        expect(disc.coverageFloor[north.i]).toBe(disc.terrainFloor[north.i]);
    });

    it('rejects breakpoints claiming reception well below the skyline', () => {
        // 0.2 degrees at 30km is more than the tolerance below the 0.79
        // degree ridge skyline - a corrupt cell. With every breakpoint
        // filtered the table carries no envelope at all, and the coverage
        // floor falls back to terrain-only (whole-table-absent behaviour)
        const disc = computeFloorDisc(ground, horizonTable(868, [{km: 30, angle: 0.2}]), 868)!;
        const located = locate(disc);
        const north = cellAt(located, 0, 25);
        expect(disc.coverageFloor[north.i]).toBe(disc.terrainFloor[north.i]);
        expect(disc.receiveAngle[north.i]).toBeNaN();
    });

    it('removes the Lennrtsns band-edge ring: no step where the evidence is skyline-limited', () => {
        // The motivating case: over flat terrain the envelope hugs the
        // skyline out to 40km (0.12/0.33 degrees), and the only thing heard
        // beyond was an airliner at 82km/1.64. The old fixed bands stepped
        // the floor from ~435m to ~1646m at exactly 50km on every such
        // bearing - a circle on the map. The measured margin (~0.12 degrees)
        // now carries the proven floor smoothly across, and the airliner
        // breakpoint no longer sets the floor anywhere
        const flat = groundTable(flatRay);
        const bps = [
            {km: 15, angle: 0.12},
            {km: 40, angle: 0.33},
            {km: 82, angle: 1.64}
        ];
        const disc = computeFloorDisc(flat, horizonTable(868, bps), 868)!;
        const located = locate(disc);
        const before = cellAt(located, 0, 49);
        const after = cellAt(located, 0, 51);
        // Smooth across the old 50km band edge, roughly skyline + margin
        expect(Math.abs(disc.coverageFloor[after.i] - disc.coverageFloor[before.i])).toBeLessThan(50);
        expect(disc.coverageFloor[before.i]).toBeGreaterThan(GROUND + 150);
        expect(disc.coverageFloor[before.i]).toBeLessThan(GROUND + 400);
        expect(disc.receiveExtended[after.i]).toBe(1);
        // The airliner's 1.64 degrees never reaches the floor
        expect(disc.receiveAngle[after.i]).toBeLessThan(0.2);
    });
});

describe('computeFloorDisc: degenerate inputs', () => {
    it('marks cells over missing bearings unknown, not zero', () => {
        // Only the due-north ray present - a sparse/damaged file
        const bearing = vectorFromArray([0], new Float32());
        const elevType = new FixedSizeList(GROUND_SAMPLES, new Field('item', new Int16(), false));
        const sparse = new Table({bearing, elevations: vectorFromArray([flatRay()], elevType)});
        const metadata = new Map([
            ['stationLat', String(STATION_LAT)],
            ['stationLng', String(STATION_LNG)],
            ['stationAgl', '0']
        ]);
        const table = tableFromIPC(tableToIPC(new Table(new Schema(sparse.schema.fields, metadata), sparse.batches)));
        const disc = computeFloorDisc(table, null, 868)!;
        const located = locate(disc);
        const north = cellAt(located, 0, 50);
        expect(disc.terrainFloor[north.i]).not.toBe(FLOOR_UNKNOWN);
        const south = cellAt(located, 180, 50);
        expect(disc.terrainFloor[south.i]).toBe(FLOOR_UNKNOWN);
        expect(disc.coverageFloor[south.i]).toBe(FLOOR_UNKNOWN);
    });

    it('keeps below-sea-level floors negative through the Int16 arrays', () => {
        const disc = computeFloorDisc(groundTable(() => new Array(GROUND_SAMPLES).fill(-100)), null, 868)!;
        const located = locate(disc);
        const near = cellAt(located, 0, 5);
        expect(disc.terrainFloor[near.i]).toBeLessThan(0);
        expect(disc.terrainFloor[near.i]).toBeCloseTo(-100 + heightAtDistance(elevationAngleDeg(0, GROUND_STEP_KM), near.d), 0);
    });

    it('returns null without station coordinates', () => {
        const elevType = new FixedSizeList(GROUND_SAMPLES, new Field('item', new Int16(), false));
        const table = tableFromIPC(
            tableToIPC(
                new Table({
                    bearing: vectorFromArray([0], new Float32()),
                    elevations: vectorFromArray([flatRay()], elevType)
                })
            )
        );
        expect(computeFloorDisc(table, null, 868)).toBeNull();
    });
});

describe('receiveEnvelopeFromTable / envelopeMargins / envelopeAngleAt', () => {
    const flatGrid = terrainGridFromTable(groundTable(flatRay))!;

    it('applies the staircase inward and extends the last breakpoint outward', () => {
        const receive = receiveEnvelopeFromTable(horizonTable(868, [{km: 30, angle: 2.0}]), 868, flatGrid)!;
        const margins = envelopeMargins(receive, flatGrid);
        // At and anywhere nearer than the breakpoint (same angle, stronger signal)
        expect(envelopeAngleAt(receive, margins, flatGrid, 0, 1, 25).angle).toBeCloseTo(2.0, 5);
        expect(envelopeAngleAt(receive, margins, flatGrid, 0, 1, 15).angle).toBeCloseTo(2.0, 5);
        expect(envelopeAngleAt(receive, margins, flatGrid, 0, 1, 4).angle).toBeCloseTo(2.0, 5);
        // Beyond the last breakpoint its angle keeps governing
        expect(envelopeAngleAt(receive, margins, flatGrid, 0, 1, 100).angle).toBeCloseTo(2.0, 4);
        // Over flat terrain the margin extension equals the staircase - the
        // whole ray is at skyline + margin, but nothing is lowered, so it must
        // not read as an extension however the float32 sum happens to round
        expect(envelopeAngleAt(receive, margins, flatGrid, 0, 1, 25).extended).toBe(false);
        expect(envelopeAngleAt(receive, margins, flatGrid, 0, 1, 100).extended).toBe(false);
        // The winning bin's furthest breakpoint, so a caller can tell the
        // measured range from the continuation beyond it
        expect(envelopeAngleAt(receive, margins, flatGrid, 0, 1, 25).provenKm).toBeCloseTo(30, 5);
        expect(envelopeAngleAt(receive, margins, flatGrid, 0, 1, 100).provenKm).toBeCloseTo(30, 5);
        // Bins with no rows at all
        expect(envelopeAngleAt(receive, margins, flatGrid, 360, 1, 25).angle).toBe(-Infinity);
        expect(envelopeAngleAt(receive, margins, flatGrid, 360, 1, 25).provenKm).toBe(0);
    });

    it('reports provenKm from the bin that won, not the widest one', () => {
        // Bin 0 proven far and shallow, bin 1 only near and steep: the shallow
        // bin sets the floor, so its 40km is the range that has been proven
        const receive = receiveEnvelopeFromTable(
            envelopeRows(868, [
                {bin: 0, bps: [{km: 40, angle: 1.4}]},
                {bin: 1, bps: [{km: 8, angle: 10.9}]}
            ]),
            868,
            flatGrid
        )!;
        const margins = envelopeMargins(receive, flatGrid);
        expect(envelopeAngleAt(receive, margins, flatGrid, 0, 2, 60).provenKm).toBeCloseTo(40, 5);
        expect(envelopeAngleAt(receive, margins, flatGrid, 1, 1, 60).provenKm).toBeCloseTo(8, 5);
    });

    it('takes the best-proven bin across the arc, not the worst', () => {
        // One bin proven shallow out to 40km next to a bin that only ever
        // heard steep near-field traffic. A cell straddling both must use the
        // shallow proof (coverage anywhere in the cell counts)
        const table = envelopeRows(868, [
            {bin: 0, bps: [{km: 40, angle: 1.4}]},
            {bin: 1, bps: [{km: 8, angle: 10.9}]}
        ]);
        const receive = receiveEnvelopeFromTable(table, 868, flatGrid)!;
        const margins = envelopeMargins(receive, flatGrid);
        expect(envelopeAngleAt(receive, margins, flatGrid, 0, 2, 40).angle).toBeCloseTo(1.4, 4);
        expect(envelopeAngleAt(receive, margins, flatGrid, 0, 2, 100).angle).toBeCloseTo(1.4, 4);
        // The junk bin alone still gives its own (clipped-at-display) answer
        expect(envelopeAngleAt(receive, margins, flatGrid, 1, 1, 40).angle).toBeCloseTo(10.9, 4);
    });

    it('carries a low proven margin across higher outer breakpoints', () => {
        // Proven 2.0 degrees at 8km over flat terrain: the margin says the
        // receiver hears down to skyline+2, so the steeper 4.0 breakpoint at
        // 30km never sets the floor - the staircase alone would (and without
        // margins does) step up to 4.0
        const receive = receiveEnvelopeFromTable(
            horizonTable(868, [
                {km: 8, angle: 2.0},
                {km: 30, angle: 4.0}
            ]),
            868,
            flatGrid
        )!;
        const margins = envelopeMargins(receive, flatGrid);
        expect(envelopeAngleAt(receive, margins, flatGrid, 0, 1, 8).angle).toBeCloseTo(2.0, 5);
        const at25 = envelopeAngleAt(receive, margins, flatGrid, 0, 1, 25);
        expect(at25.angle).toBeCloseTo(2.0, 4);
        expect(at25.extended).toBe(true);
        expect(envelopeAngleAt(receive, margins, flatGrid, 0, 1, 100).angle).toBeCloseTo(2.0, 4);
        // Staircase-only (no margins): the outer breakpoint governs its span
        expect(envelopeAngleAt(receive, null, flatGrid, 0, 1, 25).angle).toBeCloseTo(4.0, 5);
    });

    it('clamps margins at zero and mirrors the writer skyline filter', () => {
        const ridgeGrid = terrainGridFromTable(groundTable(ridgeRay))!;
        // Within tolerance below the 0.79 ridge skyline: kept, margin clamps to 0
        const receive = receiveEnvelopeFromTable(horizonTable(868, [{km: 30, angle: 0.7}]), 868, ridgeGrid)!;
        const margins = envelopeMargins(receive, ridgeGrid);
        expect(margins[0]).toBe(0);
        // Well below the skyline: every breakpoint rejected, no envelope at all
        expect(receiveEnvelopeFromTable(horizonTable(868, [{km: 30, angle: 0.2}]), 868, ridgeGrid)).toBeNull();
        // Without a terrain grid there is no mirror filter
        expect(receiveEnvelopeFromTable(horizonTable(868, [{km: 30, angle: 0.2}]), 868, null)).not.toBeNull();
    });

    it('returns null when the frequency has no rows', () => {
        expect(receiveEnvelopeFromTable(horizonTable(868, [{km: 30, angle: 2.0}]), 1090, flatGrid)).toBeNull();
    });
});

describe('getObjectFromIndex: floor layer', () => {
    it('extracts floor details from a picked disc cell', async () => {
        const {getObjectFromIndex} = await import('../../lib/react/pickabledetails');
        const disc = computeFloorDisc(groundTable(ridgeRay), horizonTable(868, [{km: 30, angle: 2.0}]), 868)!;
        const located = locate(disc);
        const north = cellAt(located, 0, 25);
        const details = getObjectFromIndex(north.i, {props: {data: disc}} as any);
        expect(details.type).toBe('floor');
        if (details.type === 'floor') {
            expect(details.ground).toBe(GROUND);
            expect(details.terrainFloor).toBe(disc.terrainFloor[north.i]);
            expect(details.coverageFloor).toBe(disc.coverageFloor[north.i]);
            expect(details.terrainAngle).toBeCloseTo(RIDGE_ANGLE, 5);
            expect(details.receiveAngle).toBeCloseTo(2.0, 5);
            expect(details.receiveExtended).toBe(RECEIVE_PROVEN);
            expect(details.h3).toMatch(/^[0-9a-f]{16}$/);
            // Before the ridge the margin extension governs - flagged for the readout
            const near = getObjectFromIndex(cellAt(located, 0, 10).i, {props: {data: disc}} as any);
            expect(near.type === 'floor' && near.receiveExtended).toBe(RECEIVE_SKYLINE_EXTENDED);
            // Past the only breakpoint nothing was ever heard: the floor is
            // that breakpoint's angle carried outward, flagged separately
            const far = getObjectFromIndex(cellAt(located, 0, 50).i, {props: {data: disc}} as any);
            expect(far.type === 'floor' && far.receiveExtended).toBe(RECEIVE_BEYOND_PROVEN);
            // NaN receive angle surfaces as null for the readout
            const noRx = getObjectFromIndex(cellAt(located, 180, 25).i, {props: {data: disc}} as any);
            expect(noRx.type === 'floor' && noRx.receiveAngle).toBeNull();
        }
    });
});

describe('floorHorizonFileFor', () => {
    it('maps the viewed period to a year-scale horizon file', () => {
        expect(floorHorizonFileFor(undefined)).toBe('year');
        expect(floorHorizonFileFor('year')).toBe('year');
        expect(floorHorizonFileFor('yearnz')).toBe('yearnz');
        expect(floorHorizonFileFor('day')).toBe('year');
        expect(floorHorizonFileFor('month')).toBe('year');
        expect(floorHorizonFileFor('2026-08-01')).toBe('year.2026');
        expect(floorHorizonFileFor('2025')).toBe('year.2025');
        expect(floorHorizonFileFor('2026nz')).toBe('yearnz.2026nz');
    });
});
