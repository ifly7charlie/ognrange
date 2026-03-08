import {describe, it, expect} from 'vitest';
import {CoverageHeader, AccumulatorType, formAccumulator, accumulatorTypes} from '../../lib/bin/coverageheader';
import type {AccumulatorBucket, AccumulatorTypeString} from '../../lib/bin/coverageheader';
import {h3IndexToSplitLong} from 'h3-js';
import type {StationId} from '../../lib/bin/types';
import {Layer} from '../../lib/common/layers';

describe('CoverageHeader', () => {
    describe('constructor - explicit params', () => {
        it('constructs with string type names', () => {
            const types: AccumulatorTypeString[] = ['day', 'month', 'year', 'yearnz', 'current'];
            for (const t of types) {
                const h = new CoverageHeader(0 as StationId, t, 0 as AccumulatorBucket, '87088619affffff');
                expect(h.typeName).toBe(t);
                expect(h.h3).toBe('87088619affffff');
            }
        });

        it('constructs with numeric AccumulatorType enum values', () => {
            const h = new CoverageHeader(0 as StationId, AccumulatorType.day, 0 as AccumulatorBucket, '87088619affffff');
            expect(h.typeName).toBe('day');
            expect(h.type).toBe(AccumulatorType.day);
        });

        it('packs type(4bit) and bucket(12bit) into _tb correctly', () => {
            const h = new CoverageHeader(0 as StationId, AccumulatorType.day, 5 as AccumulatorBucket, '87088619affffff');
            expect(h.type).toBe(AccumulatorType.day);
            expect(h.bucket).toBe(5);
            expect(h.accumulator).toBe('1005');
        });

        it('generates correct lockKey format with layer prefix', () => {
            const h = new CoverageHeader(0 as StationId, 'day', 0 as AccumulatorBucket, '87088619affffff');
            expect(h.lockKey).toBe('0/c/1000/87088619affffff');
            expect(h.layer).toBe(Layer.COMBINED);
        });

        it('generates correct lockKey with explicit layer', () => {
            const h = new CoverageHeader(0 as StationId, 'day', 0 as AccumulatorBucket, '87088619affffff', Layer.ADSB);
            expect(h.lockKey).toBe('0/a/1000/87088619affffff');
            expect(h.layer).toBe(Layer.ADSB);
        });

        it('different stationIds produce different lockKeys, same dbKey', () => {
            const l = new CoverageHeader(1 as StationId, 'day', 0 as AccumulatorBucket, '80dbfffffffffff');
            const r = new CoverageHeader(2 as StationId, 'day', 0 as AccumulatorBucket, '80dbfffffffffff');
            expect(l.dbKey()).toBe(r.dbKey());
            expect(l.lockKey).not.toBe(r.lockKey);
        });

        it('defaults to COMBINED layer when no layer specified', () => {
            const h = new CoverageHeader(0 as StationId, 'day', 0 as AccumulatorBucket, '87088619affffff');
            expect(h.layer).toBe(Layer.COMBINED);
        });
    });

    describe('constructor - from lockKey/Buffer (layer-prefixed)', () => {
        it('parses new-format dbKey with layer prefix', () => {
            const h = new CoverageHeader('c/1005/87088619affffff');
            expect(h.h3).toBe('87088619affffff');
            expect(h.accumulator).toBe('1005');
            expect(h.typeName).toBe('day');
            expect(h.bucket).toBe(0x05);
            expect(h.dbid).toBe(0);
            expect(h.layer).toBe(Layer.COMBINED);
        });

        it('parses ADSB layer-prefixed dbKey', () => {
            const h = new CoverageHeader('a/1005/87088619affffff');
            expect(h.layer).toBe(Layer.ADSB);
            expect(h.h3).toBe('87088619affffff');
            expect(h.accumulator).toBe('1005');
        });

        it('parses FLARM layer-prefixed dbKey', () => {
            const h = new CoverageHeader('f/1005/87088619affffff');
            expect(h.layer).toBe(Layer.FLARM);
        });

        it('parses new-format lockKey with layer after dbid', () => {
            const h = new CoverageHeader('0/c/1020/87088619affffff');
            expect(h.h3).toBe('87088619affffff');
            expect(h.accumulator).toBe('1020');
            expect(h.typeName).toBe('day');
            expect(h.bucket).toBe(0x20);
            expect(h.layer).toBe(Layer.COMBINED);
            expect(h.dbid).toBe(0);
        });

        it('parses lockKey with non-zero stationId and layer', () => {
            const h = new CoverageHeader('5/a/1020/87088619affffff');
            expect(h.dbid).toBe(5);
            expect(h.layer).toBe(Layer.ADSB);
            expect(h.h3).toBe('87088619affffff');
        });

        it('parses lockKey where stationId base36 is a layer letter', () => {
            // stationId 10 in base36 is 'a', which is also the ADSB layer prefix
            // lockKey: "a/c/1042/h3..." means stationId=10, layer=COMBINED
            const h = new CoverageHeader('a/c/1042/87088619affffff');
            expect(h.dbid).toBe(10);
            expect(h.layer).toBe(Layer.COMBINED);
        });

        it('round-trip: construct explicit -> lockKey -> reconstruct', () => {
            const original = new CoverageHeader(0 as StationId, 'day', 0 as AccumulatorBucket, '87088619affffff', Layer.ADSB);
            const reconstructed = new CoverageHeader(original.lockKey);
            expect(reconstructed.h3).toBe(original.h3);
            expect(reconstructed.accumulator).toBe(original.accumulator);
            expect(reconstructed.lockKey).toBe(original.lockKey);
            expect(reconstructed.dbid).toBe(original.dbid);
            expect(reconstructed.layer).toBe(Layer.ADSB);
        });

        it('round-trip with stationId: lockKey preserves dbid and layer', () => {
            const original = new CoverageHeader(1 as StationId, 'day', 0 as AccumulatorBucket, '80dbfffffffffff', Layer.FLARM);
            const reconstructed = new CoverageHeader(original.lockKey);
            expect(reconstructed.lockKey).toBe(original.lockKey);
            expect(reconstructed.h3).toBe(original.h3);
            expect(reconstructed.dbid).toBe(original.dbid);
            expect(reconstructed.layer).toBe(Layer.FLARM);
        });

        it('round-trip for all layers', () => {
            const layers = [Layer.COMBINED, Layer.FLARM, Layer.ADSB, Layer.ADSL, Layer.FANET, Layer.PAW, Layer.OGNTRK];
            for (const layer of layers) {
                const original = new CoverageHeader(3 as StationId, 'month', 7 as AccumulatorBucket, '87088619affffff', layer);
                const reconstructed = new CoverageHeader(original.lockKey);
                expect(reconstructed.layer).toBe(layer);
                expect(reconstructed.dbid).toBe(3);
                expect(reconstructed.h3).toBe('87088619affffff');
            }
        });
    });

    describe('constructor - legacy format (migration compat)', () => {
        it('parses legacy short-form dbKey as COMBINED', () => {
            const h = new CoverageHeader(Buffer.from('1005/87088619affffff'));
            expect(h.h3).toBe('87088619affffff');
            expect(h.accumulator).toBe('1005');
            expect(h.typeName).toBe('day');
            expect(h.bucket).toBe(0x05);
            expect(h.dbid).toBe(0);
            expect(h.layer).toBe(Layer.COMBINED);
        });

        it('parses legacy long-form lockKey as COMBINED', () => {
            // Legacy lockKey for stationId 5: "5/1042/87088619affffff"
            const h = new CoverageHeader('5/1042/87088619affffff');
            expect(h.dbid).toBe(5);
            expect(h.layer).toBe(Layer.COMBINED);
            expect(h.h3).toBe('87088619affffff');
        });

        it('parses from Buffer (same as string via latin1)', () => {
            const fromBuf = new CoverageHeader(Buffer.from('c/1005/87088619affffff'));
            const fromStr = new CoverageHeader('c/1005/87088619affffff');
            expect(fromBuf.h3).toBe(fromStr.h3);
            expect(fromBuf.accumulator).toBe(fromStr.accumulator);
            expect(fromBuf.layer).toBe(fromStr.layer);
        });
    });

    describe('dbKey', () => {
        it('includes layer prefix', () => {
            const h = new CoverageHeader(0 as StationId, 'day', 0 as AccumulatorBucket, '87088619affffff');
            expect(h.dbKey()).toBe('c/1000/87088619affffff');
        });

        it('includes correct layer prefix for non-combined layers', () => {
            const h = new CoverageHeader(0 as StationId, 'day', 0 as AccumulatorBucket, '87088619affffff', Layer.ADSB);
            expect(h.dbKey()).toBe('a/1000/87088619affffff');
        });

        it('dbKey round-trips through fromDbKey', () => {
            const original = new CoverageHeader(0 as StationId, 'month', 5 as AccumulatorBucket, '87088619affffff', Layer.FLARM);
            const dbKey = original.dbKey();
            const parsed = new CoverageHeader('0000/00_invalid');
            parsed.fromDbKey(dbKey);
            expect(parsed.h3).toBe(original.h3);
            expect(parsed.layer).toBe(Layer.FLARM);
            expect(parsed.accumulator).toBe(original.accumulator);
        });
    });

    describe('accessors', () => {
        it('h3, h3splitlong, h3bigint return correct values', () => {
            const h = new CoverageHeader(0 as StationId, 'day', 5 as AccumulatorBucket, '87088619affffff');
            expect(h.h3).toBe('87088619affffff');
            expect(h.h3splitlong).toStrictEqual([0x9affffff, 0x8708861]);
            expect(h.h3bigint).toBe(BigInt('0x087088619affffff'));
        });

        it('accumulator returns 4-char hex', () => {
            const h = new CoverageHeader(0 as StationId, 'day', 0 as AccumulatorBucket, '87088619affffff');
            expect(h.accumulator).toBe('1000');
            expect(h.accumulator.length).toBe(4);
        });

        it('type/typeName return numeric/string AccumulatorType', () => {
            const h = new CoverageHeader(1 as StationId, 'year', 2 as AccumulatorBucket, '80dbfffffffffff');
            expect(h.type).toBe(AccumulatorType.year);
            expect(h.typeName).toBe('year');
        });

        it('bucket returns lower 12 bits', () => {
            const h = new CoverageHeader(1 as StationId, 'year', 2 as AccumulatorBucket, '80dbfffffffffff');
            expect(h.bucket).toBe(2);
        });

        it('dbid and lockKey', () => {
            const h = new CoverageHeader(1 as StationId, 'year', 2 as AccumulatorBucket, '80dbfffffffffff');
            expect(h.dbid).toBe(1);
            expect(h.lockKey).toContain('80dbfffffffffff');
        });
    });

    describe('isMeta', () => {
        it('true for h3 starting with "00"', () => {
            const h = new CoverageHeader(0 as StationId, 'yearnz', 5 as AccumulatorBucket, '00_meta');
            expect(h.isMeta).toBe(true);
        });

        it('true for h3 starting with "80"', () => {
            const h = new CoverageHeader(0 as StationId, 'month', 0xa0 as AccumulatorBucket, '80aff00000000000');
            expect(h.isMeta).toBe(true);
        });

        it('false for normal h3 values', () => {
            const h = new CoverageHeader(0 as StationId, 'day', 5 as AccumulatorBucket, '87088619affffff');
            expect(h.isMeta).toBe(false);
        });
    });

    describe('fromDbKey', () => {
        it('updates from legacy dbKey Buffer, sets dbid=0', () => {
            const h = new CoverageHeader(0 as StationId, 'current', 0 as AccumulatorBucket, '00_invalid');
            h.fromDbKey(Buffer.from('1005/87088619affffff'));
            expect(h.h3).toBe('87088619affffff');
            expect(h.dbid).toBe(0);
            expect(h.typeName).toBe('day');
            expect(h.bucket).toBe(0x05);
            expect(h.layer).toBe(Layer.COMBINED);
        });

        it('updates from layer-prefixed dbKey', () => {
            const h = new CoverageHeader(0 as StationId, 'current', 0 as AccumulatorBucket, '00_invalid');
            h.fromDbKey(Buffer.from('a/1005/87088619affffff'));
            expect(h.h3).toBe('87088619affffff');
            expect(h.layer).toBe(Layer.ADSB);
            expect(h.typeName).toBe('day');
            expect(h.bucket).toBe(0x05);
        });

        it('handles hex-digit layer prefixes (c, d, f)', () => {
            const h = new CoverageHeader(0 as StationId, 'current', 0 as AccumulatorBucket, '00_invalid');
            h.fromDbKey('c/1005/87088619affffff');
            expect(h.layer).toBe(Layer.COMBINED);

            h.fromDbKey('d/1005/87088619affffff');
            expect(h.layer).toBe(Layer.ADSL);

            h.fromDbKey('f/1005/87088619affffff');
            expect(h.layer).toBe(Layer.FLARM);
        });

        it('h3splitlong matches h3IndexToSplitLong', () => {
            const h = new CoverageHeader(0 as StationId, 'current', 0 as AccumulatorBucket, '00_invalid');
            h.fromDbKey(Buffer.from('c/1005/87088619affffff'));
            expect(h.h3splitlong).toStrictEqual(h3IndexToSplitLong(h.h3));
            expect(h.h3splitlong).toStrictEqual([0x9affffff, 0x8708861]);
        });
    });

    describe('static methods', () => {
        it('getDbSearchRangeForAccumulator returns {gte, lt} with layer prefix', () => {
            const range = CoverageHeader.getDbSearchRangeForAccumulator('day', 0 as AccumulatorBucket);
            expect(range).toHaveProperty('gte');
            expect(range).toHaveProperty('lt');
            expect(range.gte < range.lt).toBe(true);
            expect(range.gte.startsWith('c/')).toBe(true);
            expect(range.lt.startsWith('c/')).toBe(true);
        });

        it('getDbSearchRangeForAccumulator with specific layer', () => {
            const range = CoverageHeader.getDbSearchRangeForAccumulator('day', 0 as AccumulatorBucket, false, Layer.ADSB);
            expect(range.gte.startsWith('a/')).toBe(true);
            expect(range.lt.startsWith('a/')).toBe(true);
        });

        it('getAccumulatorMeta returns header with h3="00_meta"', () => {
            const meta = CoverageHeader.getAccumulatorMeta('day', 0 as AccumulatorBucket);
            expect(meta.h3).toBe('00_meta');
            expect(meta.isMeta).toBe(true);
            expect(meta.layer).toBe(Layer.COMBINED);
        });

        it('getAccumulatorMeta with specific layer', () => {
            const meta = CoverageHeader.getAccumulatorMeta('day', 0 as AccumulatorBucket, Layer.ADSB);
            expect(meta.layer).toBe(Layer.ADSB);
            expect(meta.dbKey().startsWith('a/')).toBe(true);
        });

        it('compareH3: returns -1/0/1 for lt/eq/gt', () => {
            const a = new CoverageHeader(0 as StationId, 'day', 0 as AccumulatorBucket, '87088619affffff');
            const b = new CoverageHeader(0 as StationId, 'day', 0 as AccumulatorBucket, '87088619bffffff');
            expect(CoverageHeader.compareH3(a, b)).toBe(-1);
            expect(CoverageHeader.compareH3(b, a)).toBe(1);
            expect(CoverageHeader.compareH3(a, a)).toBe(0);
        });

        it('getAccumulatorForBucket: new header with swapped type+bucket, preserves layer', () => {
            const h = new CoverageHeader(0 as StationId, 'day', 5 as AccumulatorBucket, '87088619affffff', Layer.ADSB);
            const swapped = h.getAccumulatorForBucket(AccumulatorType.month, 10 as AccumulatorBucket);
            expect(swapped.typeName).toBe('month');
            expect(swapped.bucket).toBe(10);
            expect(swapped.h3).toBe('87088619affffff');
            expect(swapped.dbid).toBe(h.dbid);
            expect(swapped.layer).toBe(Layer.ADSB);
        });
    });

    describe('formAccumulator', () => {
        it('(AccumulatorType.day, 0) => "1000"', () => {
            expect(formAccumulator(AccumulatorType.day, 0 as AccumulatorBucket)).toBe('1000');
        });

        it('(AccumulatorType.month, 0) => "3000"', () => {
            expect(formAccumulator(AccumulatorType.month, 0 as AccumulatorBucket)).toBe('3000');
        });

        it('bucket bits preserved in lower 12', () => {
            expect(formAccumulator(AccumulatorType.day, 0x05 as AccumulatorBucket)).toBe('1005');
            expect(formAccumulator(AccumulatorType.current, 0xfff as AccumulatorBucket)).toBe('0fff');
        });

        it('day accumulator with different stationIds has same accumulator code', () => {
            const l = new CoverageHeader(0 as StationId, 'day', 0 as AccumulatorBucket, '87088619affffff');
            const r = new CoverageHeader(0 as StationId, 'month', 0 as AccumulatorBucket, '87088619affffff');
            expect(l.accumulator).toBe('1000');
            expect(r.accumulator).toBe('3000');
            expect(l.accumulator).not.toBe(r.accumulator);
        });
    });

    describe('key sort order', () => {
        it('layers sort in expected alphabetical order', () => {
            const layers = [Layer.ADSB, Layer.COMBINED, Layer.ADSL, Layer.FLARM, Layer.FANET, Layer.PAW, Layer.OGNTRK];
            const keys = layers.map((l) => new CoverageHeader(0 as StationId, 'day', 0 as AccumulatorBucket, '87088619affffff', l).dbKey());
            const sorted = [...keys].sort();
            expect(keys).toStrictEqual(sorted);
        });
    });
});
