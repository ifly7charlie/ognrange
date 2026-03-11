import {describe, it, expect, vi, beforeEach} from 'vitest';
import {CoverageHeader} from '../../lib/bin/coverageheader';
import {CoverageRecord, bufferTypes} from '../../lib/bin/coveragerecord';
import {Layer} from '../../lib/common/layers';
import type {AccumulatorBucket, AccumulatorTypeString} from '../../lib/bin/coverageheader';
import type {Accumulators} from '../../lib/bin/accumulators';
import type {Epoch, StationId} from '../../lib/bin/types';

// ── Mocks ──────────────────────────────────────────────────────────────────

vi.mock('fs', () => ({
    writeFileSync: vi.fn(),
    mkdirSync: vi.fn(),
    unlinkSync: vi.fn(),
    symlinkSync: vi.fn(),
    renameSync: vi.fn()
}));

vi.mock('../../lib/bin/coveragerecordwriter', () => ({
    CoverageRecordWriter: class {
        append() {}
        async finalize() {
            return 0;
        }
    }
}));

vi.mock('../../lib/worker/rollupmetadata', () => ({
    saveAccumulatorMetadata: vi.fn(async (db: any) => db)
}));

vi.mock('../../lib/worker/rollupworker', () => ({}));

vi.mock('../../lib/worker/stationcache', () => ({}));

vi.mock('../../lib/bin/stationstatus', () => ({
    StationDetails: undefined
}));

vi.mock('../../lib/bin/accumulators', () => ({
    describeAccumulators: vi.fn(() => ['12:00', '2024-06-15,2024-06,2024,2023nz'])
}));

let mockEnabledLayers: Set<Layer> | null = null;
vi.mock('../../lib/common/config', () => ({
    get ENABLED_LAYERS() {
        return mockEnabledLayers;
    },
    OUTPUT_PATH: '/tmp/test/',
    UNCOMPRESSED_ARROW_FILES: false,
    ROLLUP_PERIOD_MINUTES: 12
}));

import {migrateLegacyKeysToPrefix, rollupDatabaseStartup} from '../../lib/worker/rollupdatabase';

// ── Helpers ────────────────────────────────────────────────────────────────

function makeAccumulators(): Accumulators {
    return {
        current: {bucket: 0x64 as AccumulatorBucket, file: ''},
        day: {bucket: 0xc8 as AccumulatorBucket, file: '2024-06-15'},
        month: {bucket: 0x12c as AccumulatorBucket, file: '2024-06'},
        year: {bucket: 0x190 as AccumulatorBucket, file: '2024'},
        yearnz: {bucket: 0x1f4 as AccumulatorBucket, file: '2023nz'}
    };
}

function makeRecord(count = 1): CoverageRecord {
    const cr = new CoverageRecord(bufferTypes.global);
    for (let i = 0; i < count; i++) {
        cr.update(100, 50, 5, 20, 1, 1 as StationId);
    }
    return cr;
}

function legacyDataKey(type: AccumulatorTypeString, bucket: AccumulatorBucket, h3: string): string {
    return new CoverageHeader(0 as StationId, type, bucket, h3, Layer.COMBINED).legacyDbKey();
}

function prefixedDataKey(type: AccumulatorTypeString, bucket: AccumulatorBucket, h3: string): string {
    return new CoverageHeader(0 as StationId, type, bucket, h3, Layer.COMBINED).dbKey();
}

function legacyMetaKey(type: AccumulatorTypeString, bucket: AccumulatorBucket): string {
    return CoverageHeader.getLegacyAccumulatorMetaDbKey(type, bucket);
}

function prefixedMetaKey(type: AccumulatorTypeString, bucket: AccumulatorBucket): string {
    return CoverageHeader.getAccumulatorMeta(type, bucket, Layer.COMBINED).dbKey();
}

function metaValue(accumulators: Accumulators): Uint8Array {
    const meta = {
        start: 1000,
        startUtc: '2024-06-15T12:00:00Z',
        accumulators,
        currentAccumulator: accumulators.current.bucket,
        allStarts: [{start: 1000, startUtc: '2024-06-15T12:00:00Z'}]
    };
    return Buffer.from(JSON.stringify(meta));
}

// ── Mock DB ────────────────────────────────────────────────────────────────

class MockIterator {
    private entries: [string, Uint8Array][];
    private index: number;

    constructor(store: Map<string, Uint8Array>, range?: {gte?: string; lt?: string}) {
        this.entries = [...store.entries()]
            .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0))
            .filter(([key]) => {
                if (range?.gte && key < range.gte) return false;
                if (range?.lt && key >= range.lt) return false;
                return true;
            });
        this.index = 0;
    }

    async next(): Promise<[string, Uint8Array] | undefined> {
        if (this.index >= this.entries.length) return undefined;
        return this.entries[this.index++];
    }

    seek(target: string) {
        const idx = this.entries.findIndex(([key]) => key >= target);
        this.index = idx === -1 ? this.entries.length : idx;
    }

    close() {}

    [Symbol.asyncIterator]() {
        return {
            next: async () => {
                const val = await this.next();
                return val === undefined ? {done: true as const, value: undefined} : {done: false as const, value: val};
            }
        };
    }
}

function makeMockDb(name = 'TEST', initialData: Record<string, Uint8Array> = {}) {
    const store = new Map<string, Uint8Array>(Object.entries(initialData));
    return {
        ognStationName: name,
        global: true,
        get: vi.fn(async (key: string) => {
            if (store.has(key)) return store.get(key)!;
            throw new Error('Key not found');
        }),
        getMany: vi.fn(async (keys: string[]) => keys.map((k) => store.get(k))),
        put: vi.fn(async (key: string, value: Uint8Array) => {
            store.set(key, value);
        }),
        batch: vi.fn(async (ops: Array<{type: string; key: string; value?: Uint8Array}>) => {
            for (const op of ops) {
                if (op.type === 'put' && op.value) store.set(op.key, op.value);
                else if (op.type === 'del') store.delete(op.key);
            }
        }),
        clear: vi.fn(async (range: {gte: string; lt: string}) => {
            for (const key of store.keys()) {
                if (key >= range.gte && key < range.lt) store.delete(key);
            }
        }),
        iterator: vi.fn((range?: {gte?: string; lt?: string}) => new MockIterator(store, range)),
        compactRange: vi.fn(async () => {}),
        _store: store
    };
}

// ── Tests ──────────────────────────────────────────────────────────────────

const H3A = '8f283470d9a7fff';
const H3B = '8f283470d9b7fff';

describe('migrateLegacyKeysToPrefix', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('converts legacy-only keys to prefixed format', async () => {
        const acc = makeAccumulators();
        const rec = makeRecord();

        const db = makeMockDb('TEST', {
            [legacyDataKey('current', acc.current.bucket, H3A)]: rec.buffer(),
            [legacyDataKey('day', acc.day.bucket, H3A)]: rec.buffer()
        });

        const migrated = await migrateLegacyKeysToPrefix(db as any, acc);

        expect(migrated).toBe(2);
        // Prefixed keys exist
        expect(db._store.has(prefixedDataKey('current', acc.current.bucket, H3A))).toBe(true);
        expect(db._store.has(prefixedDataKey('day', acc.day.bucket, H3A))).toBe(true);
        // Legacy keys removed
        expect(db._store.has(legacyDataKey('current', acc.current.bucket, H3A))).toBe(false);
        expect(db._store.has(legacyDataKey('day', acc.day.bucket, H3A))).toBe(false);
    });

    it('preserves data values during migration', async () => {
        const acc = makeAccumulators();
        const rec = makeRecord(3);
        const originalCount = rec.count;

        const db = makeMockDb('TEST', {
            [legacyDataKey('year', acc.year.bucket, H3A)]: rec.buffer()
        });

        await migrateLegacyKeysToPrefix(db as any, acc);

        const prefixed = db._store.get(prefixedDataKey('year', acc.year.bucket, H3A))!;
        const result = new CoverageRecord(prefixed);
        expect(result.count).toBe(originalCount);
    });

    it('merges legacy with existing prefixed data', async () => {
        const acc = makeAccumulators();
        const legacyRec = makeRecord(2);
        const prefixedRec = makeRecord(3);

        const db = makeMockDb('TEST', {
            [legacyDataKey('day', acc.day.bucket, H3A)]: legacyRec.buffer(),
            [prefixedDataKey('day', acc.day.bucket, H3A)]: prefixedRec.buffer()
        });

        await migrateLegacyKeysToPrefix(db as any, acc);

        // Legacy key removed
        expect(db._store.has(legacyDataKey('day', acc.day.bucket, H3A))).toBe(false);
        // Prefixed key has merged data (count > either individual)
        const merged = new CoverageRecord(db._store.get(prefixedDataKey('day', acc.day.bucket, H3A))!);
        expect(merged.count).toBeGreaterThan(legacyRec.count);
        expect(merged.count).toBeGreaterThan(prefixedRec.count);
    });

    it('deletes legacy meta keys', async () => {
        const acc = makeAccumulators();
        const rec = makeRecord();

        const db = makeMockDb('TEST', {
            [legacyMetaKey('current', acc.current.bucket)]: metaValue(acc),
            [legacyDataKey('current', acc.current.bucket, H3A)]: rec.buffer()
        });

        await migrateLegacyKeysToPrefix(db as any, acc);

        expect(db._store.has(legacyMetaKey('current', acc.current.bucket))).toBe(false);
    });

    it('deletes legacy meta even when no data keys exist', async () => {
        const acc = makeAccumulators();

        const db = makeMockDb('TEST', {
            [legacyMetaKey('day', acc.day.bucket)]: metaValue(acc)
        });

        await migrateLegacyKeysToPrefix(db as any, acc);

        expect(db._store.has(legacyMetaKey('day', acc.day.bucket))).toBe(false);
    });

    it('skips meta keys in legacy data range (does not migrate them as data)', async () => {
        const acc = makeAccumulators();
        const rec = makeRecord();

        const db = makeMockDb('TEST', {
            [legacyMetaKey('current', acc.current.bucket)]: metaValue(acc),
            [legacyDataKey('current', acc.current.bucket, H3A)]: rec.buffer()
        });

        const migrated = await migrateLegacyKeysToPrefix(db as any, acc);

        // Only 1 data key migrated, not the meta
        expect(migrated).toBe(1);
    });

    it('returns 0 when no legacy keys exist', async () => {
        const acc = makeAccumulators();
        const db = makeMockDb('TEST');

        const migrated = await migrateLegacyKeysToPrefix(db as any, acc);
        expect(migrated).toBe(0);
    });

    it('handles multiple h3 cells across multiple accumulator types', async () => {
        const acc = makeAccumulators();
        const rec = makeRecord();

        const db = makeMockDb('TEST', {
            [legacyDataKey('current', acc.current.bucket, H3A)]: rec.buffer(),
            [legacyDataKey('current', acc.current.bucket, H3B)]: rec.buffer(),
            [legacyDataKey('day', acc.day.bucket, H3A)]: rec.buffer(),
            [legacyDataKey('month', acc.month.bucket, H3B)]: rec.buffer()
        });

        const migrated = await migrateLegacyKeysToPrefix(db as any, acc);

        expect(migrated).toBe(4);
        expect(db._store.has(prefixedDataKey('current', acc.current.bucket, H3A))).toBe(true);
        expect(db._store.has(prefixedDataKey('current', acc.current.bucket, H3B))).toBe(true);
        expect(db._store.has(prefixedDataKey('day', acc.day.bucket, H3A))).toBe(true);
        expect(db._store.has(prefixedDataKey('month', acc.month.bucket, H3B))).toBe(true);
    });
});

describe('rollupDatabaseStartup — legacy migration integration', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockEnabledLayers = null;
    });

    it('migrates legacy keys before rolling up', async () => {
        const acc = makeAccumulators();
        const rec = makeRecord();

        // Seed: prefixed current meta (discovery finds hanging rollup) +
        // legacy current data + prefixed destination metas
        const initial: Record<string, Uint8Array> = {
            [prefixedMetaKey('current', acc.current.bucket)]: metaValue(acc),
            [legacyDataKey('current', acc.current.bucket, H3A)]: rec.buffer(),
            [prefixedMetaKey('day', acc.day.bucket)]: metaValue(acc),
            [prefixedMetaKey('month', acc.month.bucket)]: metaValue(acc),
            [prefixedMetaKey('year', acc.year.bucket)]: metaValue(acc),
            [prefixedMetaKey('yearnz', acc.yearnz.bucket)]: metaValue(acc)
        };

        const db = makeMockDb('TEST', initial);
        const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

        await rollupDatabaseStartup(db as any, {now: 1000 as Epoch, accumulators: acc, stationMeta: {}});

        // Legacy current data key should be gone (migrated then rolled up)
        expect(db._store.has(legacyDataKey('current', acc.current.bucket, H3A))).toBe(false);
        // Prefixed destination keys should exist (rollup output)
        expect(db._store.has(prefixedDataKey('day', acc.day.bucket, H3A))).toBe(true);

        consoleSpy.mockRestore();
    });
});

describe('rollupDatabaseStartup — layer mask changes', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        mockEnabledLayers = null;
    });

    it('only rolls up enabled layers, preserves disabled layer data', async () => {
        const acc = makeAccumulators();
        const rec = makeRecord();

        // Seed current data for combined AND flarm layers
        const initial: Record<string, Uint8Array> = {
            // Combined layer metas + data
            [prefixedMetaKey('current', acc.current.bucket)]: metaValue(acc),
            [prefixedDataKey('current', acc.current.bucket, H3A)]: rec.buffer(),
            // Flarm layer metas + data
            [CoverageHeader.getAccumulatorMeta('current', acc.current.bucket, Layer.FLARM).dbKey()]: metaValue(acc),
            [new CoverageHeader(0 as StationId, 'current', acc.current.bucket, H3A, Layer.FLARM).dbKey()]: rec.buffer(),
            // Destination metas for both layers
            [prefixedMetaKey('day', acc.day.bucket)]: metaValue(acc),
            [prefixedMetaKey('month', acc.month.bucket)]: metaValue(acc),
            [prefixedMetaKey('year', acc.year.bucket)]: metaValue(acc),
            [prefixedMetaKey('yearnz', acc.yearnz.bucket)]: metaValue(acc),
            [CoverageHeader.getAccumulatorMeta('day', acc.day.bucket, Layer.FLARM).dbKey()]: metaValue(acc),
            [CoverageHeader.getAccumulatorMeta('month', acc.month.bucket, Layer.FLARM).dbKey()]: metaValue(acc),
            [CoverageHeader.getAccumulatorMeta('year', acc.year.bucket, Layer.FLARM).dbKey()]: metaValue(acc),
            [CoverageHeader.getAccumulatorMeta('yearnz', acc.yearnz.bucket, Layer.FLARM).dbKey()]: metaValue(acc)
        };

        const db = makeMockDb('TEST', initial);
        const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

        // Only enable combined — flarm should be skipped
        mockEnabledLayers = new Set([Layer.COMBINED]);
        await rollupDatabaseStartup(db as any, {now: 1000 as Epoch, accumulators: acc, stationMeta: {}});

        // Combined current data consumed
        expect(db._store.has(prefixedDataKey('current', acc.current.bucket, H3A))).toBe(false);

        // Flarm current data preserved (not rolled up)
        const flarmCurrentKey = new CoverageHeader(0 as StationId, 'current', acc.current.bucket, H3A, Layer.FLARM).dbKey();
        expect(db._store.has(flarmCurrentKey)).toBe(true);

        // Flarm current meta preserved (so next startup can pick it up)
        const flarmCurrentMeta = CoverageHeader.getAccumulatorMeta('current', acc.current.bucket, Layer.FLARM).dbKey();
        expect(db._store.has(flarmCurrentMeta)).toBe(true);

        consoleSpy.mockRestore();
    });

    it('rolls up previously-disabled layer when re-enabled', async () => {
        const acc = makeAccumulators();
        const rec = makeRecord();

        // Seed: only flarm current data (combined already rolled up in a prior run)
        const initial: Record<string, Uint8Array> = {
            [CoverageHeader.getAccumulatorMeta('current', acc.current.bucket, Layer.FLARM).dbKey()]: metaValue(acc),
            [new CoverageHeader(0 as StationId, 'current', acc.current.bucket, H3A, Layer.FLARM).dbKey()]: rec.buffer(),
            // Destination metas for flarm
            [CoverageHeader.getAccumulatorMeta('day', acc.day.bucket, Layer.FLARM).dbKey()]: metaValue(acc),
            [CoverageHeader.getAccumulatorMeta('month', acc.month.bucket, Layer.FLARM).dbKey()]: metaValue(acc),
            [CoverageHeader.getAccumulatorMeta('year', acc.year.bucket, Layer.FLARM).dbKey()]: metaValue(acc),
            [CoverageHeader.getAccumulatorMeta('yearnz', acc.yearnz.bucket, Layer.FLARM).dbKey()]: metaValue(acc)
        };

        const db = makeMockDb('TEST', initial);
        const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});

        // Now enable flarm
        mockEnabledLayers = new Set([Layer.COMBINED, Layer.FLARM]);
        await rollupDatabaseStartup(db as any, {now: 1000 as Epoch, accumulators: acc, stationMeta: {}});

        // Flarm current data consumed
        const flarmCurrentKey = new CoverageHeader(0 as StationId, 'current', acc.current.bucket, H3A, Layer.FLARM).dbKey();
        expect(db._store.has(flarmCurrentKey)).toBe(false);

        // Flarm day data created
        const flarmDayKey = new CoverageHeader(0 as StationId, 'day', acc.day.bucket, H3A, Layer.FLARM).dbKey();
        expect(db._store.has(flarmDayKey)).toBe(true);

        consoleSpy.mockRestore();
    });
});
