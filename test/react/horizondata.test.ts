import {describe, it, expect} from 'vitest';
import {tableFromJSON, tableFromIPC, tableToIPC} from 'apache-arrow';

import {chartsFromTable, horizonFileFor, BIN_COUNT} from '../../lib/react/coveragedetails/horizondata';

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
        {frequency: 868, bearing: 0, lowestAngle: 1.5, lowestAgl: 200, lowestDistance: 10, maxDistance: 55, angle5km: null, angle10km: 2.5, angle20km: null, angle30km: null, angle50km: 1.5, angle90km: null, count: 12},
        {frequency: 868, bearing: 240.5, lowestAngle: -0.5, lowestAgl: 150, lowestDistance: 30, maxDistance: 80, angle5km: 4, angle10km: null, angle20km: null, angle30km: -0.5, angle50km: null, angle90km: null, count: 4},
        {frequency: 1090, bearing: 180, lowestAngle: 0.25, lowestAgl: 500, lowestDistance: 20, maxDistance: 90, angle5km: null, angle10km: null, angle20km: 0.25, angle30km: null, angle50km: null, angle90km: 3.25, count: 7}
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

    it('places rows in the correct bin and keeps values and nulls', () => {
        const [c868, c1090] = chartsFromTable(table);

        const north = c868.data[BIN_COUNT / 2];
        expect(north.lowestAngle).toBeCloseTo(1.5);
        expect(north.lowestAgl).toBe(200);
        expect(north.lowestDistance).toBe(10);
        expect(north.maxDistance).toBe(55);
        expect(north.count).toBe(12);
        expect(north.angle5km).toBeNull();
        expect(north.angle10km).toBeCloseTo(2.5);
        expect(north.angle50km).toBeCloseTo(1.5);

        // bearing 240.5 -> x = -119.5 -> index (x + 180) / 0.5 = 121
        const wsw = c868.data[121];
        expect(wsw.bearing).toBeCloseTo(240.5);
        expect(wsw.lowestAngle).toBeCloseTo(-0.5);
        expect(wsw.angle5km).toBeCloseTo(4);
        expect(wsw.angle10km).toBeNull();

        // the 1090 chart only has its own row, at due south (start of the axis)
        const south = c1090.data[0];
        expect(south.bearing).toBe(180);
        expect(south.lowestAngle).toBeCloseTo(0.25);
        expect(south.angle90km).toBeCloseTo(3.25);
        expect(c1090.data[BIN_COUNT / 2].lowestAngle).toBeNull();
    });

    it('leaves unpopulated bins as all-null gap points', () => {
        const [c868] = chartsFromTable(table);
        const gap = c868.data[1];
        expect(gap.lowestAngle).toBeNull();
        expect(gap.count).toBeNull();
        expect(gap.angle5km).toBeNull();
    });
});
