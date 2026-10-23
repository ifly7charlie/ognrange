import {describe, it, expect} from 'vitest';
import {tableFromJSON, tableFromIPC, tableToIPC} from 'apache-arrow';

import {chartsFromTable, horizonFileFor, heightAtDistance, destinationPoint, BIN_COUNT} from '../../lib/react/coveragedetails/horizondata';

describe('heightAtDistance', () => {
    // Inverts the writer's angle formula: reference values from the Rust
    // angle_curvature_dip test in aprs-server/src/horizon.rs
    it('recovers the height the writer computed the angle from', () => {
        // Flat terrain at 10km gives -0.0337 deg (pure curvature dip)
        expect(heightAtDistance(-0.0337, 10)).toBeCloseTo(0, 0);
        // 300m above station ground at 20km gives 0.792 deg
        expect(heightAtDistance(0.792, 20)).toBeCloseTo(300, -1);
    });
});

describe('destinationPoint', () => {
    it('moves due north and east by the expected degrees', () => {
        // 1 degree of latitude is ~111.2 km on a 6371 km sphere
        const [lngN, latN] = destinationPoint(47, 8, 0, 111.2);
        expect(latN).toBeCloseTo(48, 2);
        expect(lngN).toBeCloseTo(8, 3);

        // due east: longitude change scaled by cos(latitude), latitude nearly unchanged
        const [lngE, latE] = destinationPoint(47, 8, 90, 50);
        expect(lngE).toBeCloseTo(8 + 50 / (111.2 * Math.cos((47 * Math.PI) / 180)), 2);
        expect(latE).toBeCloseTo(47, 1);
    });

    it('wraps across the antimeridian', () => {
        const [lng] = destinationPoint(0, 179.9, 90, 50);
        expect(lng).toBeCloseTo(-179.65, 1);
    });
});

describe('horizonFileFor', () => {
    it('passes month/year/yearnz periods through', () => {
        expect(horizonFileFor('month.2026-07')).toBe('month.2026-07');
        expect(horizonFileFor('year.2026')).toBe('year.2026');
        expect(horizonFileFor('yearnz.2025nz')).toBe('yearnz.2025nz');
        expect(horizonFileFor('month')).toBe('month');
        expect(horizonFileFor('year')).toBe('year');
    });

    it('maps a day period to its containing month', () => {
        expect(horizonFileFor('day.2026-07-19')).toBe('month.2026-07');
        expect(horizonFileFor('day')).toBe('month');
    });

    it('defaults to the latest year file', () => {
        expect(horizonFileFor(undefined)).toBe('year');
        expect(horizonFileFor('')).toBe('year');
        expect(horizonFileFor('nonsense')).toBe('year');
    });
});

describe('chartsFromTable', () => {
    const rows = [
        {frequency: 868, bearing: 0, count: 12, bp0Km: 10, bp0Angle: 1.5, bp1Km: 55, bp1Angle: 2.5, bp2Km: null, bp2Angle: null, bp3Km: null, bp3Angle: null, bp4Km: null, bp4Angle: null},
        {frequency: 868, bearing: 240.5, count: 4, bp0Km: 30, bp0Angle: -0.5, bp1Km: null, bp1Angle: null, bp2Km: null, bp2Angle: null, bp3Km: null, bp3Angle: null, bp4Km: null, bp4Angle: null},
        {frequency: 1090, bearing: 180, count: 7, bp0Km: 20, bp0Angle: 0.25, bp1Km: 90, bp1Angle: 3.25, bp2Km: null, bp2Angle: null, bp3Km: null, bp3Angle: null, bp4Km: null, bp4Angle: null}
    ];

    // Round-trip through IPC stream format - the same wire format the Rust rollup writes
    const table = tableFromIPC(tableToIPC(tableFromJSON(rows)));

    it('splits rows into one chart per frequency, sorted ascending', () => {
        const charts = chartsFromTable(table);
        expect(charts.map((c) => c.frequency)).toEqual([868, 1090]);
    });

    it('emits every bin with north in the middle of the x range', () => {
        const [c868] = chartsFromTable(table);
        expect(c868.data).toHaveLength(BIN_COUNT);
        expect(c868.data[0].x).toBe(-180);
        expect(c868.data[0].bearing).toBe(180);
        expect(c868.data[BIN_COUNT - 1].x).toBe(179.5);
        expect(c868.data[BIN_COUNT / 2].x).toBe(0);
        expect(c868.data[BIN_COUNT / 2].bearing).toBe(0);
        // strictly ascending x
        for (let i = 1; i < c868.data.length; i++) {
            expect(c868.data[i].x).toBeGreaterThan(c868.data[i - 1].x);
        }
    });

    it('places rows in the correct bin with their breakpoints and chart keys', () => {
        const [c868, c1090] = chartsFromTable(table);

        const north = c868.data[BIN_COUNT / 2];
        expect(north.count).toBe(12);
        expect(north.breakpoints).toHaveLength(2);
        expect(north.breakpoints[0].km).toBeCloseTo(10);
        expect(north.breakpoints[0].angle).toBeCloseTo(1.5);
        expect(north.breakpoints[1].km).toBeCloseTo(55);
        expect(north.breakpoints[1].angle).toBeCloseTo(2.5);
        expect(north.bp0).toBeCloseTo(1.5);
        expect(north.bp1).toBeCloseTo(2.5);
        expect(north.bp2).toBeNull();

        // bearing 240.5 -> x = -119.5 -> index (x + 180) / 0.5 = 121
        const wsw = c868.data[121];
        expect(wsw.bearing).toBeCloseTo(240.5);
        expect(wsw.breakpoints).toHaveLength(1);
        expect(wsw.bp0).toBeCloseTo(-0.5);
        expect(wsw.bp1).toBeNull();

        // the 1090 chart only has its own row, at due south (start of the axis)
        const south = c1090.data[0];
        expect(south.bearing).toBe(180);
        expect(south.bp0).toBeCloseTo(0.25);
        expect(south.bp1).toBeCloseTo(3.25);
        expect(c1090.data[BIN_COUNT / 2].bp0).toBeNull();
    });

    it('leaves unpopulated bins as empty gap points', () => {
        const [c868] = chartsFromTable(table);
        const gap = c868.data[1];
        expect(gap.breakpoints).toHaveLength(0);
        expect(gap.count).toBeNull();
        expect(gap.bp0).toBeNull();
    });

    it('renders nothing for pre-envelope band-format files', () => {
        const legacy = tableFromIPC(tableToIPC(tableFromJSON([{frequency: 868, bearing: 0, lowestAngle: 1.5, angle10km: 2.5, count: 3}])));
        expect(chartsFromTable(legacy)).toHaveLength(0);
    });
});
