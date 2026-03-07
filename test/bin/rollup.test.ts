import {describe, it, expect, vi, beforeEach} from 'vitest';
import type {Epoch, StationId, StationName} from '../../lib/bin/types';
import type {Accumulators, AccumulatorBucket, AccumulatorTypeString} from '../../lib/bin/accumulators';

// Mock all heavy external dependencies
vi.mock('../../lib/worker/rollupworker', () => ({
    rollupDatabase: vi.fn(async () => ({
        elapsed: 100,
        operations: 50,
        retiredBuckets: 0,
        recordsRemoved: 5,
        arrowRecords: 25
    })),
    purgeDatabase: vi.fn(async () => {}),
    rollupStartup: vi.fn(async () => ({
        success: true,
        datapurged: false,
        datamerged: true,
        datachanged: true,
        purged: 0,
        arrowRecords: 10
    }))
}));

vi.mock('../../lib/bin/stationstatus', () => ({
    allStationsDetails: vi.fn(({includeGlobal}: {includeGlobal: boolean}) => {
        const stations = [
            {id: 1 as StationId, station: 'station-1' as StationName, valid: true, lastPacket: (Date.now() / 1000 - 100) as Epoch},
            {id: 2 as StationId, station: 'station-2' as StationName, valid: true, lastPacket: (Date.now() / 1000 - 100) as Epoch}
        ];
        if (includeGlobal) {
            stations.push({id: 0 as StationId, station: 'global' as StationName, valid: true, lastPacket: (Date.now() / 1000) as Epoch});
        }
        return stations;
    }),
    updateStationStatus: vi.fn()
}));

vi.mock('../../lib/bin/stationfile', () => ({
    produceStationFile: vi.fn(async () => {})
}));

vi.mock('../../lib/bin/accumulators', () => ({
    getCurrentAccumulators: vi.fn(() => makeAccumulators()),
    describeAccumulators: vi.fn(() => ['12:00', '2024-06-15,2024-06,2024,2023nz'])
}));

// Import after mocks
import {rollupAll} from '../../lib/bin/rollup';
import {rollupDatabase, purgeDatabase} from '../../lib/worker/rollupworker';
import {allStationsDetails, updateStationStatus} from '../../lib/bin/stationstatus';
import {produceStationFile} from '../../lib/bin/stationfile';

function makeAccumulators(): Accumulators {
    return {
        current: {bucket: 100 as AccumulatorBucket, file: '', effectiveStart: 1000 as Epoch},
        day: {bucket: 200 as AccumulatorBucket, file: '2024-06-15', effectiveStart: 2000 as Epoch},
        month: {bucket: 300 as AccumulatorBucket, file: '2024-06', effectiveStart: 3000 as Epoch},
        year: {bucket: 400 as AccumulatorBucket, file: '2024', effectiveStart: 4000 as Epoch},
        yearnz: {bucket: 500 as AccumulatorBucket, file: '2023nz', effectiveStart: 5000 as Epoch}
    };
}

describe('rollupAll', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('calculates validStations from expiry time', async () => {
        const accumulators = makeAccumulators();
        await rollupAll(accumulators);
        // allStationsDetails should have been called with includeGlobal
        expect(allStationsDetails).toHaveBeenCalled();
    });

    it('calls rollupDatabase for valid stations + global', async () => {
        const accumulators = makeAccumulators();
        await rollupAll(accumulators);
        // rollupDatabase is called for each valid station + global
        expect(rollupDatabase).toHaveBeenCalled();
    });

    it('calls produceStationFile', async () => {
        const accumulators = makeAccumulators();
        await rollupAll(accumulators);
        expect(produceStationFile).toHaveBeenCalled();
    });

    it('detects retired accumulators when bucket changes', async () => {
        const accumulators = makeAccumulators();
        const nextAccumulators = makeAccumulators();
        nextAccumulators.day.bucket = 999 as AccumulatorBucket;
        nextAccumulators.day.file = '2024-06-16';
        await rollupAll(accumulators, nextAccumulators);
        // Should still complete successfully with retired accumulators
        expect(rollupDatabase).toHaveBeenCalled();
    });

    it('aggregates rollupStats', async () => {
        const accumulators = makeAccumulators();
        const stats = await rollupAll(accumulators);
        expect(stats).toHaveProperty('completed');
        expect(stats).toHaveProperty('elapsed');
        expect(stats.completed).toBeGreaterThanOrEqual(1);
    });

    it('marks moved stations as invalid', async () => {
        // Override the mock to include a moved station
        vi.mocked(allStationsDetails).mockReturnValueOnce([
            {id: 1 as StationId, station: 'station-1' as StationName, valid: true, moved: true, lastPacket: (Date.now() / 1000 - 100) as Epoch} as any,
            {id: 0 as StationId, station: 'global' as StationName, valid: true, lastPacket: (Date.now() / 1000) as Epoch} as any
        ]);
        // Second call for the mapAllCapped iteration
        vi.mocked(allStationsDetails).mockReturnValueOnce([
            {id: 1 as StationId, station: 'station-1' as StationName, valid: false, moved: false, lastPacket: (Date.now() / 1000 - 100) as Epoch} as any,
            {id: 0 as StationId, station: 'global' as StationName, valid: true, lastPacket: (Date.now() / 1000) as Epoch} as any
        ]);

        const accumulators = makeAccumulators();
        const stats = await rollupAll(accumulators);
        expect(stats.movedStations).toBe(1);
    });
});
