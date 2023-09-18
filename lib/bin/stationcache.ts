// DB
import {ClassicLevel, BatchOperation as ClassicBatchOperation} from 'classic-level';

// Least Recently Used cache for Station Database connectiosn
import LRUCache from 'lru-cache';

import AsyncLock from 'async-lock';
let lock = new AsyncLock();

// Map id to name
import {StationName, EpochMS} from './types';

import {H3_CACHE_FLUSH_PERIOD_MS, MAX_STATION_DBS, STATION_DB_EXPIRY_MS, DB_PATH} from '../common/config';

import {isMainThread} from 'node:worker_threads';

export type BatchOperation = ClassicBatchOperation<DB, string, Uint8Array>;

const options = {
    max: MAX_STATION_DBS + 1, // global is stored in the cache
    dispose: function (db: Promise<DB>, key: StationName, r: string) {
        db.then((db) => {
            if (db.status == 'closed') {
                return; // if it's already closed then that's cool let it go
            }
            //        console.log('~', db.ognStationName, key, r);
            if (db.global && r == 'evict') {
                //            console.warn('closing global db on cache evict');
            }
            if (stationDbCache.getRemainingTTL(key) < H3_CACHE_FLUSH_PERIOD_MS / 1000) {
                //                console.log(`Closing database ${key} while it's still needed. You should increase MAX_STATION_DBS in .env.local [db status ${db.status}]`);
            }
            db.close((e) => {
                console.log('closed', db.ognStationName, e);
            });
        }).catch((_e) => {
            /**/
        });
    },
    updateAgeOnGet: true,
    allowStale: true,
    ttl: STATION_DB_EXPIRY_MS
};

//
// Instantiate - we drop the type because we
const stationDbCache = new LRUCache<StationName, Promise<DB>>(options);
export interface DB extends ClassicLevel<string, Uint8Array> {
    ognInitialTS: EpochMS;
    ognStationName: StationName;
    global: boolean;
    cached: boolean;
}

export async function getDb(
    stationName: StationName, //
    options: {cache?: boolean; open?: boolean; existingOnly?: boolean; throw?: boolean; noMeta?: boolean} = {cache: true, open: true}
): Promise<DB | undefined> {
    return getDbThrow(stationName, options).catch((e) => {
        if (options.throw) {
            throw e;
        }
        return undefined;
    });
}

//
// Get a db from the cache, by name or number
export async function getDbThrow(
    stationName: StationName, //
    options: {cache?: boolean; open?: boolean; existingOnly?: boolean; noMeta?: boolean} = {cache: true, open: true}
): Promise<DB> {
    //
    let stationDbPromise: Promise<DB> | undefined = stationDbCache.get(stationName);

    if (!stationDbPromise) {
        stationDbPromise = new Promise<DB>((resolve, reject) => {
            if (options.existingOnly) {
                reject(new Error(`Unable to create ${stationName} as only existing db requested`));
                return;
            }

            // You can get either global or specific station, they are stored in slightly different places
            const path = stationName == 'global' ? DB_PATH + stationName : DB_PATH + '/stations/' + stationName;

            const stationDb = new ClassicLevel<string, Uint8Array>(path, {valueEncoding: 'view'}) as DB;

            stationDb.ognInitialTS = Date.now() as EpochMS;
            stationDb.ognStationName = stationName;
            stationDb.global = stationName == 'global';

            // We are supposed to open the database
            stationDb
                .open()
                .then(() => {
                    if (stationDb !== undefined && !(stationDb.status == 'open' || stationDb.status == 'opening')) {
                        console.log(stationName, stationDb.status, new Error('db status invalid'));
                        reject(new Error(`Db ${stationName} status invalid, ${stationDb.status}`));
                    }
                    resolve(stationDb);
                })
                .catch((e: any) => {
                    console.log(`${stationName}: Failed to open: ${stationDb.status}: ${e.cause?.code || e.code}`);
                    stationDb.close().catch((e) => {
                        /* ignore */
                    });
                    reject(e);
                });
        });
    }

    // Return the promise, we now cache promises ;)
    stationDbCache.set(stationName, stationDbPromise);
    return stationDbPromise;
}

// Size excluding global
export function getStationDbCacheSize(): number {
    return stationDbCache.size;
}

//
// Purge all entries - will call the dispose function thereby closing the database entry
export async function closeAllStationDbs(): Promise<void> {
    // Temp copy and clear as non-interruptable operation
    const numberOfDbs = stationDbCache.size;
    const dbs = stationDbCache.rvalues();

    // Close all the databases
    const promises: Promise<void>[] = [];
    for (const v of dbs) {
        promises.push(
            v
                .then((db: DB) => db.close())
                .catch((_e) => {
                    /**/
                })
        );
    }
    await Promise.allSettled(promises);
    stationDbCache.clear();
    console.log(`closed ${numberOfDbs} station databases`);
}
