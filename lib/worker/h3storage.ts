import {CoverageRecord} from '../bin/coveragerecord';
import {CoverageHeader} from '../bin/coverageheader';

import {getDbThrow, DB, BatchOperation} from './stationcache';

import {StationName, H3LockKey} from '../bin/types';

import {Accumulators} from '../bin/accumulators';

import {saveAccumulatorMetadata} from './rollupmetadata';

let h3dbOps = new Map<StationName, BatchOperation[]>();

// This reads the DB for the record and then adds data to it - it's how we get data from the APRS
// (main) thread to the DB thread
export async function writeH3ToDB(station: StationName, h3lockkey: H3LockKey, buffer: Uint8Array): Promise<void> {
    const h3k = new CoverageHeader(h3lockkey);
    const cr = new CoverageRecord(buffer);

    const existingOperation = h3dbOps.get(station) ?? [];

    // Save it back for flushing - we can still update it as by reference
    // and this ensures that everybody in the async code following updates the
    // same array
    if (!existingOperation.length) {
        h3dbOps.set(station, existingOperation);
    }

    const getOperation = (db: DB): Promise<BatchOperation | null> =>
        db
            .get(h3k.dbKey())
            .then((dbData: Uint8Array): BatchOperation | null => {
                const newCr = cr.rollup(new CoverageRecord(dbData));
                if (newCr) {
                    return {type: 'put', key: h3k.dbKey(), value: newCr.buffer()};
                } else {
                    return null;
                }
            })
            .catch((): BatchOperation => {
                // If we don't have a record then we can just use the raw value we received
                return {type: 'put', key: h3k.dbKey(), value: buffer};
            });

    let cleanupDB: DB | undefined;

    await getDbThrow(station)
        .then((db: DB) => {
            return (cleanupDB = db);
        })
        .then((db: DB) => getOperation(db))
        .then((operation) => {
            if (operation) {
                existingOperation.push(operation);
            }
        })
        .catch((e) => {
            console.error(`unable to find db for id ${h3k.dbid}/${station}, ${e}`);
        })
        .finally(() => {
            cleanupDB?.close();
        });
}

// Flush all writes pending in the dbOps table
export async function flushH3DbOps(accumulators: Accumulators): Promise<{databases: number}> {
    const promises: Promise<void>[] = [];

    const outputOps = h3dbOps;
    h3dbOps = new Map<StationName, BatchOperation[]>();

    // Now push these to the database
    for (const [station, v] of outputOps) {
        promises.push(
            new Promise<void>((resolve) => {
                //
                let cleanupDB: DB | undefined;
                getDbThrow(station)
                    .then((db: DB | undefined) => {
                        if (!db) {
                            throw new Error(`unable to find db for ${station}`);
                        }
                        return (cleanupDB = db);
                    })
                    // Make sure we have updated the meta data before we write the batch
                    .then((db: DB) => saveAccumulatorMetadata(db, accumulators))
                    // Execute all changes as a batch
                    .then((db: DB) => {
                        db.batch(v);
                        return db;
                    })
                    .catch((e) => {
                        console.error(`${station}: error flushing ${v.length} db operations: ${e}`);
                    })
                    .finally(() => {
                        cleanupDB?.close();
                        resolve();
                    });
            })
        );
    }

    await Promise.allSettled(promises);
    return {databases: outputOps.size};
}
