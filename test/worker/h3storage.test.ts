import {describe, it, expect, vi, beforeEach, afterEach} from 'vitest';
import type {StationName, H3LockKey, StationId} from '../../lib/bin/types';
import type {AccumulatorBucket} from '../../lib/bin/coverageheader';
import {Layer} from '../../lib/common/layers';
import {CoverageHeader} from '../../lib/bin/coverageheader';
import {CoverageRecord, bufferTypes} from '../../lib/bin/coveragerecord';
import type {Accumulators} from '../../lib/bin/accumulators';

function makeMockDb(initialData: Record<string, Uint8Array> = {}) {
    const store: Record<string, Uint8Array> = {...initialData};
    return {
        get: vi.fn(async (key: string) => {
            if (key in store) return store[key];
            throw new Error('Key not found');
        }),
        getMany: vi.fn(async (keys: string[]) => keys.map((k) => store[k])),
        batch: vi.fn(async (ops: Array<{type: string; key: string; value?: Uint8Array}>) => {
            for (const op of ops) {
                if (op.type === 'put' && op.value) store[op.key] = op.value;
                else if (op.type === 'del') delete store[op.key];
            }
        }),
        close: vi.fn(async () => {}),
        _store: store
    };
}

let mockDb: ReturnType<typeof makeMockDb>;

vi.mock('../../lib/worker/stationcache', () => ({getDbThrow: vi.fn()}));
vi.mock('../../lib/worker/rollupmetadata', () => ({
    saveAccumulatorMetadata: vi.fn(async (db: any) => db)
}));

import {writeH3ToDB, flushH3DbOps} from '../../lib/worker/h3storage';
import {getDbThrow} from '../../lib/worker/stationcache';

function makeLockKey(h3: string, stationId: number, bucket: number): H3LockKey {
    return new CoverageHeader(stationId as StationId, 'current', bucket as AccumulatorBucket, h3 as any, Layer.COMBINED).lockKey as H3LockKey;
}

function makeBuffer(global = true): Uint8Array {
    const cr = new CoverageRecord(global ? bufferTypes.global : bufferTypes.station);
    cr.update(100, 50, 5, 20, 1, 1 as StationId);
    return cr.buffer();
}

function makeAccumulators(): Accumulators {
    return {
        current: {bucket: 100 as AccumulatorBucket, file: ''},
        day: {bucket: 200 as AccumulatorBucket, file: '2024-06-15'},
        month: {bucket: 300 as AccumulatorBucket, file: '2024-06'},
        year: {bucket: 400 as AccumulatorBucket, file: '2024'},
        yearnz: {bucket: 500 as AccumulatorBucket, file: '2023nz'}
    };
}

const STATION = 'station-1' as StationName;
const STATION2 = 'station-2' as StationName;

beforeEach(() => {
    mockDb = makeMockDb();
    vi.clearAllMocks();
    vi.mocked(getDbThrow).mockImplementation(async () => mockDb as any);
});

afterEach(async () => {
    // drain any pending state so tests don't bleed into each other
    await flushH3DbOps(makeAccumulators());
});

describe('writeH3ToDB + flushH3DbOps', () => {
    it('new key: writes incoming buffer to DB', async () => {
        const lockKey = makeLockKey('87088619affffff', 0, 100);
        const dbKey = new CoverageHeader(lockKey).dbKey();
        await writeH3ToDB(STATION, lockKey, makeBuffer());
        await flushH3DbOps(makeAccumulators());
        expect(mockDb._store[dbKey]).toBeDefined();
    });

    it('existing key: writes merged result, not raw incoming', async () => {
        const lockKey = makeLockKey('87088619bffffff', 0, 100);
        const dbKey = new CoverageHeader(lockKey).dbKey();
        // Pre-populate DB with one update
        mockDb._store[dbKey] = makeBuffer();

        await writeH3ToDB(STATION, lockKey, makeBuffer());
        await flushH3DbOps(makeAccumulators());

        const result = mockDb._store[dbKey];
        expect(result).toBeDefined();
        // Merged record has higher count than a single update
        const merged = new CoverageRecord(result);
        const single = new CoverageRecord(makeBuffer());
        expect(merged.count).toBeGreaterThan(single.count);
    });

    it('multiple keys same station: all written in one flush', async () => {
        const lock1 = makeLockKey('87088619affffff', 0, 100);
        const lock2 = makeLockKey('87088619cffffff', 0, 100);
        const key1 = new CoverageHeader(lock1).dbKey();
        const key2 = new CoverageHeader(lock2).dbKey();

        await writeH3ToDB(STATION, lock1, makeBuffer());
        await writeH3ToDB(STATION, lock2, makeBuffer());
        await flushH3DbOps(makeAccumulators());

        expect(mockDb._store[key1]).toBeDefined();
        expect(mockDb._store[key2]).toBeDefined();
    });

    it('multiple stations: returns correct databases count', async () => {
        const mockDb2 = makeMockDb();
        vi.mocked(getDbThrow)
            .mockResolvedValueOnce(mockDb as any)
            .mockResolvedValueOnce(mockDb2 as any);

        await writeH3ToDB(STATION, makeLockKey('87088619affffff', 0, 100), makeBuffer());
        await writeH3ToDB(STATION2, makeLockKey('87088619dffffff', 0, 100), makeBuffer());

        const result = await flushH3DbOps(makeAccumulators());
        expect(result.databases).toBe(2);
    });

    it('empty flush returns {databases: 0}', async () => {
        const result = await flushH3DbOps(makeAccumulators());
        expect(result.databases).toBe(0);
    });

    it('calls saveAccumulatorMetadata during flush', async () => {
        const {saveAccumulatorMetadata} = await import('../../lib/worker/rollupmetadata');
        await writeH3ToDB(STATION, makeLockKey('87088619affffff', 0, 100), makeBuffer());
        await flushH3DbOps(makeAccumulators());
        expect(saveAccumulatorMetadata).toHaveBeenCalled();
    });

    it('db error during flush: logs error, does not throw', async () => {
        vi.mocked(getDbThrow).mockRejectedValueOnce(new Error('db unavailable'));
        const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
        await writeH3ToDB(STATION, makeLockKey('87088619affffff', 0, 100), makeBuffer());
        await expect(flushH3DbOps(makeAccumulators())).resolves.toBeDefined();
        consoleSpy.mockRestore();
    });
});
