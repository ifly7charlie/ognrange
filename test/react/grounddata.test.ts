import {describe, it, expect} from 'vitest';
import {tableFromIPC, tableToIPC, vectorFromArray, Table, Schema, Field, Float32, Int16, FixedSizeList} from 'apache-arrow';

import {
    groundChartFromTable,
    profileForBearing,
    groundMeta,
    groundUrl,
    minGroundElevation,
    visibleCrests,
    DEFAULT_STATION_AGL_M,
    GROUND_BIN_COUNT,
    GROUND_SAMPLES,
    GROUND_STEP_KM
} from '../../lib/react/coveragedetails/grounddata';
import {elevationAngleDeg} from '../../lib/react/coveragedetails/horizondata';

// A sparse three-row table (real files always have all 720 bins - sparseness
// exercises the gap handling). Elevation rays are FixedSizeList<Int16>[241]
// like the Rust writer's; tableFromJSON can't build those, so the vectors are
// constructed explicitly and round-tripped through the IPC stream format.
// The skyline/crest values are computed from the rays - the legacy
// horizonAngle/horizonDistance columns are deliberately absent
const STATION = 291;
const BEARINGS = [0, 180, 240.5];

function ray(stationElevation: number, ...ridges: {sample: number; elevation: number}[]): number[] {
    const r = new Array(GROUND_SAMPLES).fill(stationElevation);
    for (const ridge of ridges) {
        r[ridge.sample] = ridge.elevation;
    }
    return r;
}

// North: a visible foothill at 4 km in front of the skyline ridge at 20 km;
// south: flat; 240.5: flat with a below-sea-level dip (Int16 sign check)
const RAYS = [
    ray(STATION, {sample: 8, elevation: 600}, {sample: 40, elevation: 2400}), //
    ray(STATION),
    ray(STATION, {sample: 240, elevation: -40})
];

// Default metadata pins the viewpoint at ground level so the expected angles
// below stay the writer-formula values; pass an explicit map (possibly empty)
// to exercise the beacon-height fallbacks
function makeTable(metadata: Map<string, string> = new Map([['stationAgl', '0.0']])): Table {
    const elevType = new FixedSizeList(GROUND_SAMPLES, new Field('item', new Int16(), false));
    const table = new Table({
        bearing: vectorFromArray(BEARINGS, new Float32()),
        elevations: vectorFromArray(RAYS, elevType)
    });
    const withMeta = metadata ? new Table(new Schema(table.schema.fields, metadata), table.batches) : table;
    return tableFromIPC(tableToIPC(withMeta));
}

// Angles the walk should find, straight from the writer's formula
const FOOTHILL_ANGLE = elevationAngleDeg(600 - STATION, 8 * GROUND_STEP_KM);
const SKYLINE_ANGLE = elevationAngleDeg(2400 - STATION, 40 * GROUND_STEP_KM);

describe('groundUrl', () => {
    it('builds the non-accumulator per-station url', () => {
        expect(groundUrl('https://x/data/', 'LFLE')).toBe('https://x/data/LFLE/LFLE.ground-horizon.arrow');
    });
});

describe('visibleCrests', () => {
    it('returns the skyline as the last crest with foreground crests before it', () => {
        const crests = visibleCrests(RAYS[0], 0);
        expect(crests.length).toBeGreaterThanOrEqual(2);
        const skyline = crests[crests.length - 1];
        expect(skyline.angle).toBeCloseTo(SKYLINE_ANGLE, 5);
        expect(skyline.km).toBe(40 * GROUND_STEP_KM);
        expect(crests.map((c) => c.km)).toContain(8 * GROUND_STEP_KM);
        // Crest angles strictly increase - each one set a new running max
        for (let i = 1; i < crests.length; i++) {
            expect(crests[i].angle).toBeGreaterThan(crests[i - 1].angle);
        }
    });

    it('flat terrain has a single near-field crest just below zero', () => {
        const crests = visibleCrests(RAYS[1], 0);
        expect(crests).toHaveLength(1);
        expect(crests[0].km).toBe(GROUND_STEP_KM);
        expect(crests[0].angle).toBeLessThan(0);
        expect(crests[0].angle).toBeGreaterThan(-0.1);
    });

    it('raising the viewpoint lowers every crest angle', () => {
        const ground = visibleCrests(RAYS[0], 0);
        const mast = visibleCrests(RAYS[0], 12);
        expect(mast[mast.length - 1].angle).toBeLessThan(ground[ground.length - 1].angle);
    });
});

describe('groundChartFromTable', () => {
    const chart = groundChartFromTable(makeTable())!;
    const {points} = chart;

    it('emits every bin with north in the middle, matching the receive-horizon convention', () => {
        expect(points).toHaveLength(GROUND_BIN_COUNT);
        expect(points[0].x).toBe(-180);
        expect(points[0].bearing).toBe(180);
        expect(points[GROUND_BIN_COUNT / 2].x).toBe(0);
        expect(points[GROUND_BIN_COUNT / 2].bearing).toBe(0);
        expect(points[GROUND_BIN_COUNT - 1].x).toBe(179.5);
        for (let i = 1; i < points.length; i++) {
            expect(points[i].x).toBeGreaterThan(points[i - 1].x);
        }
    });

    it('computes the skyline and foreground ridges from the elevations', () => {
        const north = points[GROUND_BIN_COUNT / 2];
        expect(north.horizonAngle).toBeCloseTo(SKYLINE_ANGLE, 5);
        expect(north.horizonDistance).toBe(20);
        // The 4 km foothill is a foreground ridge; the near-field flat-ground
        // contact at 0.5 km is filtered out by RIDGE_MIN_KM
        expect(north.ridge0).toBeCloseTo(FOOTHILL_ANGLE, 5);
        expect(chart.ridgeCount).toBe(1);

        const south = points[0];
        expect(south.horizonAngle).toBeLessThan(0);
        expect(south.horizonDistance).toBe(1);
        expect(south.ridge0).toBeUndefined();

        // bearing 240.5 -> x = -119.5 -> index (x + 180) / 0.5 = 121
        expect(points[121].bearing).toBeCloseTo(240.5);
        expect(points[121].horizonAngle).toBeLessThan(0);
    });

    it('exposes the station ground elevation from sample 0', () => {
        expect(chart.stationElevation).toBe(STATION);
    });

    it('uses the stationAgl metadata as the viewpoint height', () => {
        const raised = groundChartFromTable(makeTable(new Map([['stationAgl', '12.0']])))!;
        const north = raised.points[GROUND_BIN_COUNT / 2];
        expect(north.horizonAngle).toBeLessThan(SKYLINE_ANGLE);
    });

    it('assumes the default height when stationAgl is NaN or absent', () => {
        const expected = elevationAngleDeg(2400 - STATION - DEFAULT_STATION_AGL_M, 40 * GROUND_STEP_KM);
        // NaN = the writer fell back to its default; absent = pre-capture file
        for (const table of [makeTable(new Map([['stationAgl', 'NaN']])), makeTable(new Map())]) {
            const north = groundChartFromTable(table)!.points[GROUND_BIN_COUNT / 2];
            expect(north.horizonAngle).toBeCloseTo(expected, 5);
        }
    });

    it('leaves unpopulated bins as null gap points', () => {
        expect(points[1].horizonAngle).toBeNull();
        expect(points[1].horizonDistance).toBeNull();
    });

    it('returns null when the elevations column is missing', () => {
        const bogus = tableFromIPC(tableToIPC(new Table({bearing: vectorFromArray([1], new Float32())})));
        expect(groundChartFromTable(bogus)).toBeNull();
    });
});

describe('profileForBearing', () => {
    const table = makeTable();

    it('returns the full ray at the requested bearing with the computed skyline', () => {
        const p = profileForBearing(table, 0)!;
        expect(p.bearing).toBe(0);
        expect(p.points).toHaveLength(GROUND_SAMPLES);
        expect(p.points[0]).toEqual({km: 0, elevation: STATION});
        expect(p.stationElevation).toBe(STATION);
        // foothill at sample 8 = 4km
        expect(p.points[8]).toEqual({km: 8 * GROUND_STEP_KM, elevation: 600});
        expect(p.horizonAngle).toBeCloseTo(SKYLINE_ANGLE, 5);
        expect(p.horizonDistance).toBe(20);
    });

    it('resolves a fractional hover bearing to its bin row despite sparse row order', () => {
        // 240.5 is row 2 here, not at its natural index - forces the scan path
        const p = profileForBearing(table, 240.5)!;
        expect(p.bearing).toBe(240.5);
        // below-sea-level Int16 values survive
        expect(p.points[240].elevation).toBe(-40);
        expect(p.horizonAngle).toBeLessThan(0);
    });

    it('returns null for a bearing with no row', () => {
        expect(profileForBearing(table, 90)).toBeNull();
    });

    it('returns null when the elevations column is missing', () => {
        const noElev = tableFromIPC(tableToIPC(new Table({bearing: vectorFromArray([0], new Float32())})));
        expect(profileForBearing(noElev, 0)).toBeNull();
    });
});

describe('minGroundElevation', () => {
    it('finds the lowest sample across all bearings', () => {
        // The 240.5 ray dips to -40; every ray includes the station ground
        expect(minGroundElevation(makeTable())).toBe(-40);
    });

    it('returns null when the elevations column is missing', () => {
        const noElev = tableFromIPC(tableToIPC(new Table({bearing: vectorFromArray([0], new Float32())})));
        expect(minGroundElevation(noElev)).toBeNull();
    });
});

describe('groundMeta', () => {
    it('parses the writer metadata', () => {
        const meta = groundMeta(
            makeTable(
                new Map([
                    ['stationLat', '45.560333'],
                    ['stationLng', '5.975667'],
                    ['stationAgl', '12.0'],
                    ['stepKm', '0.5'],
                    ['maxKm', '120'],
                    ['samples', '241'],
                    ['generatedAt', '1786045426']
                ])
            )
        );
        expect(meta.stationLat).toBeCloseTo(45.560333);
        expect(meta.stationLng).toBeCloseTo(5.975667);
        expect(meta.stationAgl).toBe(12);
        expect(meta.stepKm).toBe(0.5);
        expect(meta.maxKm).toBe(120);
        expect(meta.generatedAt).toBe(1786045426);
    });

    it('falls back to defaults when metadata is absent', () => {
        const meta = groundMeta(makeTable(new Map()));
        expect(meta.stationLat).toBeNull();
        // Pre-viewpoint files carry no stationAgl: height unknown
        expect(meta.stationAgl).toBeNull();
        expect(meta.stepKm).toBe(0.5);
        expect(meta.maxKm).toBe(120);
        expect(meta.generatedAt).toBeNull();
    });

    it('reports a NaN stationAgl (writer assumed its default) as null', () => {
        expect(groundMeta(makeTable(new Map([['stationAgl', 'NaN']]))).stationAgl).toBeNull();
    });
});
