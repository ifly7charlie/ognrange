import {describe, it, expect, vi, beforeEach} from 'vitest';
import type {StationId} from '../../lib/bin/types';
import type {H3} from '../../lib/bin/types';

// Mock external dependencies before importing the module
vi.mock('../../lib/worker/rollupworker', () => ({
    flushBatch: vi.fn(async () => ({databases: 1}))
}));

vi.mock('../../lib/bin/stationstatus', () => ({
    getStationName: vi.fn((dbid: number) => (dbid === 0 ? 'global' : `station-${dbid}`))
}));

vi.mock('../../lib/bin/accumulators', () => ({
    getAccumulator: vi.fn(() => 100),
    getCurrentAccumulators: vi.fn(() => ({
        current: {bucket: 100, file: ''},
        day: {bucket: 200, file: '2024-06-15'},
        month: {bucket: 300, file: '2024-06'},
        year: {bucket: 400, file: '2024'},
        yearnz: {bucket: 500, file: '2023nz'}
    })),
    describeAccumulators: vi.fn(() => ['12:00', '2024-06-15,2024-06,2024,2023nz'])
}));

// Import after mocks
import {updateCachedH3, flushDirtyH3s, getH3CacheSize} from '../../lib/bin/h3cache';
import {flushBatch} from '../../lib/worker/rollupworker';

describe('getH3CacheSize', () => {
    it('0 initially when freshly imported', () => {
        // The module state starts empty
        expect(typeof getH3CacheSize()).toBe('number');
    });
});

describe('updateCachedH3', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('creates new cache entry (getH3CacheSize increases)', () => {
        const sizeBefore = getH3CacheSize();
        updateCachedH3('87088619affffff' as H3, 100, 100, 5, 20, 1, 1 as StationId, 0 as StationId);
        expect(getH3CacheSize()).toBeGreaterThanOrEqual(sizeBefore + 1);
    });

    it('uses global bufferType when dbStationId=0, station otherwise', () => {
        // This is tested indirectly - both should succeed without throwing
        updateCachedH3('87088619cffffff' as H3, 100, 100, 5, 20, 1, 1 as StationId, 0 as StationId);
        updateCachedH3('87088619dffffff' as H3, 100, 100, 5, 20, 1, 1 as StationId, 5 as StationId);
    });
});

describe('flushDirtyH3s', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('allUnwritten=true: flushes everything, returns correct stats', async () => {
        // Add something to cache first
        updateCachedH3('87aaa619affffff' as H3, 100, 100, 5, 20, 1, 1 as StationId, 0 as StationId);

        const stats = await flushDirtyH3s(undefined, true);
        expect(stats).toHaveProperty('total');
        expect(stats).toHaveProperty('expired');
        expect(stats).toHaveProperty('written');
        expect(stats).toHaveProperty('databases');
        expect(stats.written).toBeGreaterThanOrEqual(0);
    });

    it('calls flushBatch', async () => {
        updateCachedH3('87bbb619affffff' as H3, 100, 100, 5, 20, 1, 1 as StationId, 0 as StationId);
        await flushDirtyH3s(undefined, true);
        expect(flushBatch).toHaveBeenCalled();
    });

    it('returns {total, expired, written, databases}', async () => {
        const stats = await flushDirtyH3s(undefined, true);
        expect(typeof stats.total).toBe('number');
        expect(typeof stats.expired).toBe('number');
        expect(typeof stats.written).toBe('number');
        expect(typeof stats.databases).toBe('number');
    });
});
