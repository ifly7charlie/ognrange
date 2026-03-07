import {describe, it, expect} from 'vitest';
import {CoverageHeader, AccumulatorType, formAccumulator, accumulatorTypes} from '../../lib/bin/coverageheader';
import type {AccumulatorBucket, AccumulatorTypeString} from '../../lib/bin/coverageheader';
import {h3IndexToSplitLong} from 'h3-js';
import type {StationId} from '../../lib/bin/types';

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

        it('generates correct lockKey format: "dbid_base36/hex4_accumulator/h3"', () => {
            const h = new CoverageHeader(0 as StationId, 'day', 0 as AccumulatorBucket, '87088619affffff');
            expect(h.lockKey).toBe('0/1000/87088619affffff');
        });

        it('different stationIds produce different lockKeys, same dbKey', () => {
            const l = new CoverageHeader(1 as StationId, 'day', 0 as AccumulatorBucket, '80dbfffffffffff');
            const r = new CoverageHeader(2 as StationId, 'day', 0 as AccumulatorBucket, '80dbfffffffffff');
            expect(l.dbKey()).toBe(r.dbKey());
            expect(l.lockKey).not.toBe(r.lockKey);
        });
    });

    describe('constructor - from lockKey/Buffer', () => {
        it('parses short-form (no station prefix, <=20 chars)', () => {
            const h = new CoverageHeader(Buffer.from('1005/87088619affffff'));
            expect(h.h3).toBe('87088619affffff');
            expect(h.accumulator).toBe('1005');
            expect(h.typeName).toBe('day');
            expect(h.bucket).toBe(0x05);
            expect(h.dbid).toBe(0);
        });

        it('parses long-form with station id', () => {
            const h = new CoverageHeader(Buffer.from('0/1020/87088619affffff'));
            expect(h.h3).toBe('87088619affffff');
            expect(h.accumulator).toBe('1020');
            expect(h.typeName).toBe('day');
            expect(h.bucket).toBe(0x20);
        });

        it('parses from Buffer (same as string via latin1)', () => {
            const fromBuf = new CoverageHeader(Buffer.from('1005/87088619affffff'));
            const fromStr = new CoverageHeader('1005/87088619affffff');
            expect(fromBuf.h3).toBe(fromStr.h3);
            expect(fromBuf.accumulator).toBe(fromStr.accumulator);
        });

        it('round-trip: construct explicit -> lockKey -> construct from lockKey -> matches', () => {
            const original = new CoverageHeader(0 as StationId, 'day', 0 as AccumulatorBucket, '87088619affffff');
            const reconstructed = new CoverageHeader(original.lockKey);
            expect(reconstructed.h3).toBe(original.h3);
            expect(reconstructed.accumulator).toBe(original.accumulator);
            expect(reconstructed.lockKey).toBe(original.lockKey);
            expect(reconstructed.dbid).toBe(original.dbid);
        });

        it('round-trip with stationId: lockKey preserves dbid', () => {
            const original = new CoverageHeader(1 as StationId, 'day', 0 as AccumulatorBucket, '80dbfffffffffff');
            const reconstructed = new CoverageHeader(original.lockKey);
            expect(reconstructed.lockKey).toBe(original.lockKey);
            expect(reconstructed.h3).toBe(original.h3);
            expect(reconstructed.dbid).toBe(original.dbid);
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
            const h = new CoverageHeader(Buffer.from('2005/00_meta'));
            expect(h.isMeta).toBe(true);
        });

        it('true for h3 starting with "80"', () => {
            const h = new CoverageHeader(Buffer.from('30a0/80aff'));
            expect(h.isMeta).toBe(true);
        });

        it('false for normal h3 values', () => {
            const h = new CoverageHeader(Buffer.from('1005/87088619affffff'));
            expect(h.isMeta).toBe(false);
        });
    });

    describe('fromDbKey', () => {
        it('updates from dbKey Buffer, sets dbid=0', () => {
            const h = new CoverageHeader('0000/00_invalid');
            h.fromDbKey(Buffer.from('1005/87088619affffff'));
            expect(h.h3).toBe('87088619affffff');
            expect(h.dbid).toBe(0);
            expect(h.typeName).toBe('day');
            expect(h.bucket).toBe(0x05);
        });

        it('h3splitlong matches h3IndexToSplitLong', () => {
            const h = new CoverageHeader('0000/00_invalid');
            h.fromDbKey(Buffer.from('1005/87088619affffff'));
            expect(h.h3splitlong).toStrictEqual(h3IndexToSplitLong(h.h3));
            expect(h.h3splitlong).toStrictEqual([0x9affffff, 0x8708861]);
        });
    });

    describe('static methods', () => {
        it('getDbSearchRangeForAccumulator returns {gte, lt}', () => {
            const range = CoverageHeader.getDbSearchRangeForAccumulator('day', 0 as AccumulatorBucket);
            expect(range).toHaveProperty('gte');
            expect(range).toHaveProperty('lt');
            expect(range.gte < range.lt).toBe(true);
        });

        it('getAccumulatorMeta returns header with h3="00_meta"', () => {
            const meta = CoverageHeader.getAccumulatorMeta('day', 0 as AccumulatorBucket);
            expect(meta.h3).toBe('00_meta');
            expect(meta.isMeta).toBe(true);
        });

        it('compareH3: returns -1/0/1 for lt/eq/gt', () => {
            const a = new CoverageHeader(0 as StationId, 'day', 0 as AccumulatorBucket, '87088619affffff');
            const b = new CoverageHeader(0 as StationId, 'day', 0 as AccumulatorBucket, '87088619bffffff');
            expect(CoverageHeader.compareH3(a, b)).toBe(-1);
            expect(CoverageHeader.compareH3(b, a)).toBe(1);
            expect(CoverageHeader.compareH3(a, a)).toBe(0);
        });

        it('getAccumulatorForBucket: new header with swapped type+bucket', () => {
            const h = new CoverageHeader(0 as StationId, 'day', 5 as AccumulatorBucket, '87088619affffff');
            const swapped = h.getAccumulatorForBucket(AccumulatorType.month, 10 as AccumulatorBucket);
            expect(swapped.typeName).toBe('month');
            expect(swapped.bucket).toBe(10);
            expect(swapped.h3).toBe('87088619affffff');
            expect(swapped.dbid).toBe(h.dbid);
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
});
