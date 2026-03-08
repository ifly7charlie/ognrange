import {CoverageRecord} from '../bin/coveragerecord';
import {CoverageHeader} from '../bin/coverageheader';

import {getDbThrow, DB, BatchOperation} from './stationcache';

import {StationName, H3LockKey} from '../bin/types';

import {Accumulators} from '../bin/accumulators';

import {saveAccumulatorMetadata} from './rollupmetadata';

type PendingRecord = {header: CoverageHeader; record: CoverageRecord};
let h3Pending = new Map<StationName, Map<string, PendingRecord>>();

// Accumulate an incoming H3 buffer — no DB I/O here, that happens in flushH3DbOps.
// Records for the same key are pre-merged in memory so the DB only sees one write per key.
// This is how we get data from the APRS (main) thread to the DB thread
export function writeH3ToDB(station: StationName, h3lockkey: H3LockKey, buffer: Uint8Array): void {
    const h3k = new CoverageHeader(h3lockkey);
    const cr = new CoverageRecord(buffer);
    const key = h3k.dbKey();

    let stationMap = h3Pending.get(station);
    if (!stationMap) {
        stationMap = new Map();
        h3Pending.set(station, stationMap);
    }

    const existing = stationMap.get(key);
    if (existing) {
        const merged = existing.record.rollup(cr);
        if (merged) stationMap.set(key, {header: h3k, record: merged});
    } else {
        stationMap.set(key, {header: h3k, record: cr});
    }
}

// Flush all pending writes to the database. One getMany fetches all existing records for a
// station, merges with the incoming data, then writes everything back as a single batch.
export async function flushH3DbOps(accumulators: Accumulators): Promise<{databases: number}> {
    const pending = h3Pending;
    h3Pending = new Map();
    const promises: Promise<void>[] = [];

    // Now push these to the database
    for (const [station, keyMap] of pending) {
        promises.push(
            new Promise<void>((resolve) => {
                let cleanupDB: DB | undefined;
                getDbThrow(station)
                    .then((db) => {
                        cleanupDB = db;
                        const keys = [...keyMap.keys()];
                        // Fetch all existing records for this station in one round-trip
                        return db.getMany(keys).then((existingValues) => ({db, keys, existingValues}));
                    })
                    .then(({db, keys, existingValues}) => {
                        const batchOps: BatchOperation[] = [];
                        for (let i = 0; i < keys.length; i++) {
                            const key = keys[i];
                            const {record: incoming} = keyMap.get(key)!;
                            const existingData = existingValues[i];
                            // If we don't have a record then we can just use the raw value we received
                            const finalRecord = existingData
                                ? (incoming.rollup(new CoverageRecord(existingData)) ?? incoming)
                                : incoming;
                            batchOps.push({type: 'put', key, value: finalRecord.buffer()});
                        }
                        // Make sure we have updated the metadata before we write the batch
                        return saveAccumulatorMetadata(db, accumulators).then((db) => db.batch(batchOps));
                    })
                    .catch((e) => {
                        console.error(`${station}: error flushing ${keyMap.size} pending records: ${e}`);
                    })
                    .finally(() => {
                        cleanupDB?.close();
                        resolve();
                    });
            })
        );
    }

    await Promise.allSettled(promises);
    return {databases: pending.size};
}
