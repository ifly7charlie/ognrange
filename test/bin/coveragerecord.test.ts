import {describe, it, expect} from 'vitest';
import {CoverageRecord, bufferTypes, bufferTypeNames} from '../../lib/bin/coveragerecord';
import type {CoverageRecordOut} from '../../lib/bin/coveragerecord';
import type {StationId} from '../../lib/bin/types';

// Helper types matching the old test patterns
type Sample = [altitude: number, agl: number, crc: number, signal: number, gap: number, stationid: StationId] | [altitude: number, agl: number, crc: number, signal: number, gap: number];

// Valid station sets for rollup/remove tests
const validStations1 = new Set<StationId>([1 as StationId]);
const validStations2 = new Set<StationId>([2 as StationId]);
const validStations12 = new Set<StationId>([1 as StationId, 2 as StationId]);

function applyUpdates(type: bufferTypes, inputs: Sample[]): CoverageRecord {
    const cr = new CoverageRecord(type);
    inputs.forEach((r) => cr.update(...r));
    return cr;
}

function checkOutput(o: CoverageRecordOut, expected: Record<string, any>) {
    for (const [key, value] of Object.entries(expected)) {
        const stationMatch = key.match(/^Station:(\d+)$/);
        if (stationMatch) {
            const pos = Number(stationMatch[1]);
            expect(o.stations).toBeDefined();
            if (Array.isArray(o.stations)) {
                const sub = o.stations[pos] as CoverageRecordOut;
                expect(sub).toBeDefined();
                for (const [sk, sv] of Object.entries(value[0])) {
                    expect(sub[sk]).toBe(sv);
                }
            }
        } else {
            expect(o[key]).toBe(value);
        }
    }
}

describe('CoverageRecord', () => {
    describe('constructor', () => {
        it('creates station type from bufferTypes.station', () => {
            const cr = new CoverageRecord(bufferTypes.station);
            expect(cr.type).toBe(bufferTypes.station);
            expect(cr.count).toBe(0);
        });

        it('creates global type from bufferTypes.global', () => {
            const cr = new CoverageRecord(bufferTypes.global);
            expect(cr.type).toBe(bufferTypes.global);
            expect(cr.count).toBe(0);
        });

        it('from Uint8Array round-trip (create -> buffer() -> reconstruct)', () => {
            const original = new CoverageRecord(bufferTypes.station);
            original.update(100, 100, 5, 20, 1);
            const buf = original.buffer();
            const reconstructed = new CoverageRecord(new Uint8Array(buf));
            expect(reconstructed.count).toBe(1);
            expect(reconstructed.type).toBe(bufferTypes.station);
            const origObj = original.toObject();
            const reconObj = reconstructed.toObject();
            expect(reconObj.MinAlt).toBe(origObj.MinAlt);
            expect(reconObj.Count).toBe(origObj.Count);
        });
    });

    describe('station update', () => {
        it('one entry: MinAlt/MinAltMaxSig/MinAltAgl/SumSig(>>2)/SumCrc/Count/SumGap', () => {
            const cr = applyUpdates(bufferTypes.station, [[10, 11, 1, 12, 0]]);
            const o = cr.toObject();
            checkOutput(o, {MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: 12 >> 2, SumCrc: 1, Count: 1, SumGap: 0, NumStations: undefined});
        });

        it('two identical updates: sums double, count=2', () => {
            const cr = applyUpdates(bufferTypes.station, [
                [10, 11, 1, 12, 0],
                [10, 11, 1, 12, 0]
            ]);
            const o = cr.toObject();
            checkOutput(o, {MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: (12 >> 2) * 2, SumCrc: 2, Count: 2, SumGap: 0});
        });

        it('two different: min/max tracking across pairs', () => {
            const cr = applyUpdates(bufferTypes.station, [
                [10, 10, 10, 10, 10],
                [10, 10, 10, 10, 10]
            ]);
            const o = cr.toObject();
            checkOutput(o, {MinAlt: 10, MinAltMaxSig: 10, MinAltAgl: 10, SumSig: (10 >> 2) * 2, SumCrc: 20, Count: 2, SumGap: 20});
        });

        it('lower altitude second: MinAlt updates, MinAltMaxSig follows', () => {
            const cr = applyUpdates(bufferTypes.station, [
                [10, 10, 10, 10, 10],
                [1, 12, 0, 20, 0]
            ]);
            const o = cr.toObject();
            checkOutput(o, {MinAlt: 1, MinAltMaxSig: 20, MinAltAgl: 10, SumSig: (10 >> 2) + (20 >> 2), SumCrc: 10, Count: 2, SumGap: 10});
        });

        it('same altitude, higher signal: MinAltMaxSig takes max', () => {
            const cr = applyUpdates(bufferTypes.station, [
                [10, 10, 10, 10, 10],
                [12, 1, 9, 20, 0]
            ]);
            const o = cr.toObject();
            checkOutput(o, {MinAlt: 10, MinAltMaxSig: 10, MinAltAgl: 1, SumSig: (10 >> 2) + (20 >> 2), SumCrc: 19, Count: 2, SumGap: 10});
        });

        it('signal >>2 shift applied to SumSig', () => {
            const cr = applyUpdates(bufferTypes.station, [[100, 100, 0, 255, 0]]);
            const o = cr.toObject();
            expect(o.SumSig).toBe(255 >> 2);
        });
    });

    describe('global update with stationid', () => {
        it('single station single entry', () => {
            const cr = applyUpdates(bufferTypes.global, [[10, 11, 1, 12, 0, 1 as StationId]]);
            const o = cr.toObject();
            checkOutput(o, {MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: 12 >> 2, SumCrc: 1, Count: 1, SumGap: 0, NumStations: 1});
        });

        it('two updates same station: linked list count=2', () => {
            const cr = applyUpdates(bufferTypes.global, [
                [10, 11, 1, 12, 0, 1 as StationId],
                [10, 11, 1, 12, 0, 1 as StationId]
            ]);
            const o = cr.toObject();
            checkOutput(o, {
                MinAlt: 10,
                MinAltMaxSig: 12,
                Count: 2,
                NumStations: 1,
                'Station:0': [{MinAlt: 10, Count: 2, MinAltMaxSig: 12, StationId: 1}]
            });
        });

        it('one from each of two stations: two linked list entries', () => {
            const cr = applyUpdates(bufferTypes.global, [
                [10, 11, 1, 12, 0, 1 as StationId],
                [10, 11, 1, 12, 0, 2 as StationId]
            ]);
            const o = cr.toObject();
            checkOutput(o, {
                Count: 2,
                NumStations: 2,
                'Station:0': [{Count: 1, StationId: 1}],
                'Station:1': [{Count: 1, StationId: 2}]
            });
        });

        it('sort order change when second station gets more count', () => {
            const cr = applyUpdates(bufferTypes.global, [
                [10, 11, 1, 12, 0, 1 as StationId],
                [10, 11, 1, 12, 0, 2 as StationId],
                [10, 11, 1, 12, 0, 2 as StationId]
            ]);
            const o = cr.toObject();
            checkOutput(o, {
                Count: 3,
                NumStations: 2,
                'Station:0': [{Count: 2, StationId: 2}],
                'Station:1': [{Count: 1, StationId: 1}]
            });
        });

        it('sort order stable after equalization', () => {
            const cr = applyUpdates(bufferTypes.global, [
                [10, 11, 1, 12, 0, 1 as StationId],
                [10, 11, 1, 12, 0, 2 as StationId],
                [10, 11, 1, 12, 0, 2 as StationId],
                [10, 11, 1, 12, 0, 1 as StationId]
            ]);
            const o = cr.toObject();
            checkOutput(o, {
                'Station:0': [{Count: 2, StationId: 2}],
                'Station:1': [{Count: 2, StationId: 1}],
                NumStations: 2
            });
        });

        it('third station added at tail', () => {
            const cr = applyUpdates(bufferTypes.global, [
                [10, 11, 1, 12, 0, 1 as StationId],
                [10, 11, 1, 12, 0, 2 as StationId],
                [10, 11, 1, 12, 0, 2 as StationId],
                [10, 11, 1, 12, 0, 1 as StationId],
                [10, 11, 1, 12, 0, 3 as StationId]
            ]);
            const o = cr.toObject();
            checkOutput(o, {
                'Station:0': [{Count: 2, StationId: 2}],
                'Station:1': [{Count: 2, StationId: 1}],
                'Station:2': [{Count: 1, StationId: 3}],
                NumStations: 3
            });
        });
    });

    describe('station rollup', () => {
        it('A: same values both sides accumulates', () => {
            const src = applyUpdates(bufferTypes.station, [[10, 11, 1, 12, 0]]);
            const dest = applyUpdates(bufferTypes.station, [[10, 11, 1, 12, 0]]);
            const out = dest.rollup(src);
            expect(out).not.toBeNull();
            checkOutput(out!.toObject(), {MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: (12 >> 2) * 2, SumCrc: 2, Count: 2, SumGap: 0});
        });

        it('B: different values min/max preserved', () => {
            const src = applyUpdates(bufferTypes.station, [[10, 11, 1, 12, 1]]);
            const dest = applyUpdates(bufferTypes.station, [[5, 10, 2, 8, 1]]);
            const out = dest.rollup(src);
            expect(out).not.toBeNull();
            checkOutput(out!.toObject(), {MinAlt: 5, MinAltMaxSig: 8, MinAltAgl: 10, SumSig: (12 >> 2) + (8 >> 2), SumCrc: 3, Count: 2, SumGap: 2});
        });

        it('C: same altitude, signal from higher takes over', () => {
            const src = applyUpdates(bufferTypes.station, [[10, 11, 1, 12, 1]]);
            const dest = applyUpdates(bufferTypes.station, [[10, 10, 2, 8, 1]]);
            const out = dest.rollup(src);
            expect(out).not.toBeNull();
            checkOutput(out!.toObject(), {MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 10, SumSig: (12 >> 2) + (8 >> 2), SumCrc: 3, Count: 2, SumGap: 2});
        });

        it('D: signal/crc/gap all sum correctly', () => {
            const src = applyUpdates(bufferTypes.station, [[10, 11, 1, 12, 1]]);
            const dest = applyUpdates(bufferTypes.station, [[10, 10, 2, 24, 1]]);
            const out = dest.rollup(src);
            expect(out).not.toBeNull();
            checkOutput(out!.toObject(), {MinAlt: 10, MinAltMaxSig: 24, MinAltAgl: 10, SumSig: (12 >> 2) + (24 >> 2), SumCrc: 3, Count: 2, SumGap: 2});
        });
    });

    describe('global rollup', () => {
        it('A: single station merge', () => {
            const src = applyUpdates(bufferTypes.global, [[10, 11, 1, 12, 0, 1 as StationId]]);
            const dest = applyUpdates(bufferTypes.global, [[10, 11, 1, 12, 0, 1 as StationId]]);
            const out = dest.rollup(src, validStations1);
            expect(out).not.toBeNull();
            checkOutput(out!.toObject(), {
                MinAlt: 10,
                MinAltMaxSig: 12,
                SumSig: (12 >> 2) * 2,
                Count: 2,
                NumStations: 1,
                'Station:0': [{Count: 2, StationId: 1}]
            });
        });

        it('B: multi-station merge with sort', () => {
            const src = applyUpdates(bufferTypes.global, [
                [10, 11, 1, 12, 0, 1 as StationId],
                [10, 11, 1, 12, 0, 2 as StationId]
            ]);
            const dest = applyUpdates(bufferTypes.global, [[10, 11, 1, 12, 0, 2 as StationId]]);
            const out = dest.rollup(src, validStations12);
            expect(out).not.toBeNull();
            const o = out!.toObject();
            checkOutput(o, {
                SumSig: (12 >> 2) * 3,
                Count: 3,
                NumStations: 2,
                'Station:0': [{Count: 2, StationId: 2}],
                'Station:1': [{Count: 1, StationId: 1}]
            });
        });

        it('C: src only (empty dest): data copied', () => {
            const src = applyUpdates(bufferTypes.global, [
                [10, 11, 1, 12, 0, 1 as StationId],
                [10, 11, 1, 12, 0, 2 as StationId],
                [10, 11, 1, 12, 0, 2 as StationId]
            ]);
            const dest = applyUpdates(bufferTypes.global, []);
            const out = dest.rollup(src, validStations12);
            expect(out).not.toBeNull();
            checkOutput(out!.toObject(), {
                Count: 3,
                NumStations: 2,
                'Station:0': [{Count: 2, StationId: 2}],
                'Station:1': [{Count: 1, StationId: 1}]
            });
        });

        it('D: dest only (empty src): data kept', () => {
            const src = applyUpdates(bufferTypes.global, []);
            const dest = applyUpdates(bufferTypes.global, [
                [10, 11, 1, 12, 0, 1 as StationId],
                [10, 11, 1, 12, 0, 2 as StationId],
                [10, 11, 1, 12, 0, 2 as StationId]
            ]);
            const out = dest.rollup(src, validStations12);
            expect(out).not.toBeNull();
            checkOutput(out!.toObject(), {
                Count: 3,
                NumStations: 2,
                'Station:0': [{Count: 2, StationId: 2}],
                'Station:1': [{Count: 1, StationId: 1}]
            });
        });

        it('E: station filtered by validStations: dropped from result, counts adjusted', () => {
            const src = applyUpdates(bufferTypes.global, [
                [10, 11, 1, 12, 0, 1 as StationId],
                [10, 11, 1, 12, 0, 2 as StationId],
                [10, 11, 1, 12, 0, 2 as StationId],
                [10, 11, 1, 12, 0, 1 as StationId]
            ]);
            const dest = applyUpdates(bufferTypes.global, [[10, 11, 1, 12, 0, 3 as StationId]]);
            const out = dest.rollup(src, validStations12);
            expect(out).not.toBeNull();
            const o = out!.toObject();
            checkOutput(o, {
                SumCrc: 4,
                Count: 4,
                NumStations: 2,
                'Station:0': [{Count: 2}],
                'Station:1': [{Count: 2}]
            });
        });

        it('F: returns null when all stations filtered out', () => {
            const src = applyUpdates(bufferTypes.global, []);
            const dest = applyUpdates(bufferTypes.global, [[10, 11, 1, 12, 0, 3 as StationId]]);
            const emptySet = new Set<StationId>();
            const out = dest.rollup(src, emptySet);
            expect(out).toBeNull();
        });
    });

    describe('removeInvalidStations', () => {
        it('all valid: returns self (identity)', () => {
            const cr = applyUpdates(bufferTypes.global, [
                [10, 11, 1, 12, 0, 1 as StationId],
                [10, 11, 1, 12, 0, 1 as StationId]
            ]);
            const out = cr.removeInvalidStations(validStations1);
            expect(out).toBe(cr); // identity check
        });

        it('one invalid removed: recalculated', () => {
            const cr = applyUpdates(bufferTypes.global, [
                [10, 11, 1, 12, 0, 1 as StationId],
                [9, 10, 2, 20, 1, 2 as StationId]
            ]);
            const out = cr.removeInvalidStations(validStations1);
            expect(out).not.toBeNull();
            expect(out).not.toBe(cr);
            checkOutput(out!.toObject(), {
                MinAlt: 10,
                MinAltMaxSig: 12,
                Count: 1,
                NumStations: 1,
                'Station:0': [{Count: 1, StationId: 1}]
            });
        });

        it('returns null when no stations remain', () => {
            const cr = applyUpdates(bufferTypes.global, [[10, 11, 1, 12, 0, 3 as StationId]]);
            const out = cr.removeInvalidStations(validStations1);
            expect(out).toBeNull();
        });

        it('no-op for station type (no nested)', () => {
            const cr = applyUpdates(bufferTypes.station, [[10, 11, 1, 12, 0]]);
            const out = cr.removeInvalidStations(validStations1);
            expect(out).toBe(cr);
        });
    });

    describe('toObject', () => {
        it('station: correct field names', () => {
            const cr = applyUpdates(bufferTypes.station, [[10, 11, 1, 12, 5]]);
            const o = cr.toObject();
            expect(o.MinAlt).toBe(10);
            expect(o.MinAltAgl).toBe(11);
            expect(o.MinAltMaxSig).toBe(12);
            expect(o.SumCrc).toBe(1);
            expect(o.SumSig).toBe(12 >> 2);
            expect(o.Count).toBe(1);
            expect(o.SumGap).toBe(5);
        });

        it('global: includes stations array, NumStations', () => {
            const cr = applyUpdates(bufferTypes.global, [
                [10, 11, 1, 12, 0, 1 as StationId],
                [10, 11, 1, 12, 0, 2 as StationId]
            ]);
            const o = cr.toObject();
            expect(o.NumStations).toBe(2);
            expect(Array.isArray(o.stations)).toBe(true);
        });
    });

    describe('arrowFormat', () => {
        it('station: averages computed correctly (avgSig = sumSig/count*4)', () => {
            const cr = applyUpdates(bufferTypes.station, [
                [100, 100, 5, 20, 4],
                [100, 100, 5, 20, 4]
            ]);
            const arrow = cr.arrowFormat([0x9affffff, 0x8708861]);
            expect(arrow.count).toBe(2);
            expect(arrow.minAlt).toBe(100);
            expect(arrow.minAgl).toBe(100);
            expect(arrow.avgSig).toBe(((((20 >> 2) * 2) / 2) * 4) >> 0);
            expect(arrow.avgCrc).toBe((((5 * 2) / 2) * 10) >> 0);
            expect(arrow.h3lo).toBe(0x9affffff);
            expect(arrow.h3hi).toBe(0x8708861);
        });

        it('global: includes stations string, expectedGap, numStations', () => {
            const cr = applyUpdates(bufferTypes.global, [
                [100, 100, 5, 20, 4, 1 as StationId],
                [100, 100, 5, 20, 4, 2 as StationId]
            ]);
            const arrow = cr.arrowFormat();
            expect(arrow.numStations).toBe(2);
            expect(typeof arrow.stations).toBe('string');
            expect(arrow.stations!.length).toBeGreaterThan(0);
            expect(typeof arrow.expectedGap).toBe('number');
        });

        it('h3splitlong passthrough', () => {
            const cr = applyUpdates(bufferTypes.station, [[100, 100, 5, 20, 4]]);
            const withH3 = cr.arrowFormat([12345, 67890]);
            expect(withH3.h3lo).toBe(12345);
            expect(withH3.h3hi).toBe(67890);
            const withoutH3 = cr.arrowFormat();
            expect(withoutH3.h3lo).toBeUndefined();
            expect(withoutH3.h3hi).toBeUndefined();
        });
    });

    describe('fromArrow', () => {
        it('round-trip preserves key fields (approximate due to integer division)', () => {
            const cr = applyUpdates(bufferTypes.station, [
                [100, 90, 5, 40, 8],
                [80, 70, 3, 60, 12]
            ]);
            const arrow = cr.arrowFormat();
            const recovered = CoverageRecord.fromArrow(arrow);
            expect(recovered.count).toBe(2);
            const rObj = recovered.toObject();
            expect(rObj.MinAlt).toBe(arrow.minAlt);
            expect(rObj.MinAltAgl).toBe(arrow.minAgl);
            expect(rObj.Count).toBe(arrow.count);
        });

        it('throws on global records (has stations)', () => {
            const cr = applyUpdates(bufferTypes.global, [[100, 100, 5, 20, 4, 1 as StationId]]);
            const arrow = cr.arrowFormat();
            expect(() => CoverageRecord.fromArrow(arrow)).toThrow('fromArrow can only be used to recover stations');
        });
    });

    describe('buffer', () => {
        it('returns underlying Uint8Array', () => {
            const cr = new CoverageRecord(bufferTypes.station);
            const buf = cr.buffer();
            expect(buf).toBeInstanceOf(Uint8Array);
        });

        it('modifications to returned buffer affect record', () => {
            const cr = new CoverageRecord(bufferTypes.station);
            cr.update(100, 100, 5, 20, 1);
            const buf = cr.buffer();
            const countBefore = cr.count;
            // the buffer IS the record's internal storage
            expect(buf).toBe(cr.buffer());
            expect(countBefore).toBe(1);
        });
    });

    describe('updateSumGap', () => {
        it('updates global SumGap total', () => {
            const cr = applyUpdates(bufferTypes.global, [[10, 11, 1, 12, 0, 1 as StationId]]);
            const before = cr.toObject().SumGap as number;
            cr.updateSumGap(10, 1 as StationId);
            expect(cr.toObject().SumGap).toBe(before + 10);
        });

        it('updates correct nested station SumGap', () => {
            const cr = applyUpdates(bufferTypes.global, [
                [10, 11, 1, 12, 0, 1 as StationId],
                [10, 11, 1, 12, 0, 2 as StationId]
            ]);
            cr.updateSumGap(5, 2 as StationId);
            const o = cr.toObject();
            // Global SumGap should increase
            expect(o.SumGap).toBe(5);
            // The station 2's nested SumGap should be 5
            const stations = o.stations as CoverageRecordOut[];
            const s2 = stations.find((s) => s.StationId === 2);
            expect(s2).toBeDefined();
            expect(s2!.SumGap).toBe(5);
        });
    });
});
