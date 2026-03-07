import {describe, it, expect, vi, beforeEach} from 'vitest';
import {Uint8FromObject, saveAccumulatorMetadata} from '../../lib/worker/rollupmetadata';
import type {AccumulatorBucket} from '../../lib/bin/coverageheader';
import type {Accumulators} from '../../lib/bin/accumulators';
import type {Epoch} from '../../lib/bin/types';

describe('Uint8FromObject', () => {
    it('converts object to Uint8Array of JSON bytes', () => {
        const obj = {hello: 'world', num: 42};
        const result = Uint8FromObject(obj);
        expect(result).toBeInstanceOf(Uint8Array);
        const parsed = JSON.parse(Buffer.from(result).toString());
        expect(parsed).toEqual(obj);
    });

    it('round-trip: JSON.parse(Buffer.from(result).toString()) equals original', () => {
        const obj = {nested: {array: [1, 2, 3]}, flag: true};
        const result = Uint8FromObject(obj);
        expect(JSON.parse(Buffer.from(result).toString())).toEqual(obj);
    });
});

describe('saveAccumulatorMetadata', () => {
    function makeTestAccumulators(): Accumulators {
        return {
            current: {bucket: 100 as AccumulatorBucket, file: '', effectiveStart: 1000 as Epoch},
            day: {bucket: 200 as AccumulatorBucket, file: '2024-06-15', effectiveStart: 2000 as Epoch},
            month: {bucket: 300 as AccumulatorBucket, file: '2024-06', effectiveStart: 3000 as Epoch},
            year: {bucket: 400 as AccumulatorBucket, file: '2024', effectiveStart: 4000 as Epoch},
            yearnz: {bucket: 500 as AccumulatorBucket, file: '2023nz', effectiveStart: 5000 as Epoch}
        };
    }

    function makeMockDb() {
        const store: Record<string, Uint8Array> = {};
        return {
            get: vi.fn(async (key: string) => {
                if (key in store) return store[key];
                throw new Error('not found');
            }),
            put: vi.fn(async (key: string, value: Uint8Array) => {
                store[key] = value;
            }),
            _store: store,
            ognStationName: 'global'
        };
    }

    it('writes metadata for each accumulator type', async () => {
        const db = makeMockDb();
        const accumulators = makeTestAccumulators();
        await saveAccumulatorMetadata(db as any, accumulators);
        // 5 accumulator types = 5 get calls + 5 put calls
        expect(db.get).toHaveBeenCalledTimes(5);
        expect(db.put).toHaveBeenCalledTimes(5);
    });

    it('creates new when get() rejects (no existing)', async () => {
        const db = makeMockDb();
        const accumulators = makeTestAccumulators();
        await saveAccumulatorMetadata(db as any, accumulators);
        // All should go through catch path and still put
        expect(db.put).toHaveBeenCalledTimes(5);
        // Verify each put call wrote valid JSON
        for (const call of db.put.mock.calls) {
            const value = call[1] as Uint8Array;
            const parsed = JSON.parse(Buffer.from(value).toString());
            expect(parsed.accumulators).toBeDefined();
            expect(parsed.currentAccumulator).toBeDefined();
        }
    });

    it('merges with existing when get() returns data', async () => {
        const db = makeMockDb();
        const accumulators = makeTestAccumulators();
        // Pre-populate one entry
        const existingMeta = {
            start: 500,
            startUtc: '2024-01-01T00:00:00Z',
            allStarts: [{start: 500, startUtc: '2024-01-01T00:00:00Z'}],
            accumulators: makeTestAccumulators(),
            currentAccumulator: 99
        };
        // Override get for one key to return existing data
        // Must return Buffer (not Uint8Array) because String(Buffer) gives UTF-8 text
        let firstCall = true;
        db.get = vi.fn(async (key: string) => {
            if (firstCall) {
                firstCall = false;
                return Buffer.from(JSON.stringify(existingMeta));
            }
            throw new Error('not found');
        });

        await saveAccumulatorMetadata(db as any, accumulators);
        // The first put should have merged with existing
        const firstPut = db.put.mock.calls[0][1] as Uint8Array;
        const parsed = JSON.parse(Buffer.from(firstPut).toString());
        // ...existing spread overwrites allStarts, start, startUtc (comes after the initial values)
        // But accumulators and currentAccumulator come after ...existing so are updated
        expect(parsed.allStarts).toEqual(existingMeta.allStarts);
        expect(parsed.currentAccumulator).toBe(accumulators.current.bucket);
    });

    it('preserves allStarts history array from existing', async () => {
        const db = makeMockDb();
        const accumulators = makeTestAccumulators();
        const existingMeta = {
            start: 500,
            startUtc: '2024-01-01T00:00:00Z',
            allStarts: [
                {start: 400, startUtc: '2023-12-01T00:00:00Z'},
                {start: 500, startUtc: '2024-01-01T00:00:00Z'}
            ],
            accumulators: makeTestAccumulators(),
            currentAccumulator: 99
        };
        // Must return Buffer for String() to produce valid JSON
        db.get = vi.fn(async () => Buffer.from(JSON.stringify(existingMeta)));

        await saveAccumulatorMetadata(db as any, accumulators);
        const firstPut = db.put.mock.calls[0][1] as Uint8Array;
        const parsed = JSON.parse(Buffer.from(firstPut).toString());
        // ...existing spread overwrites the computed allStarts with existing.allStarts
        expect(parsed.allStarts.length).toBe(2);
        expect(parsed.allStarts).toEqual(existingMeta.allStarts);
    });

    it('sets currentAccumulator field', async () => {
        const db = makeMockDb();
        const accumulators = makeTestAccumulators();
        await saveAccumulatorMetadata(db as any, accumulators);
        const firstPut = db.put.mock.calls[0][1] as Uint8Array;
        const parsed = JSON.parse(Buffer.from(firstPut).toString());
        expect(parsed.currentAccumulator).toBe(accumulators.current.bucket);
    });
});
