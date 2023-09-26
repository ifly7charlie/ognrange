// DB
import {ClassicLevel, BatchOperation as ClassicBatchOperation} from 'classic-level';

// Least Recently Used cache for Station Database connectiosn
import LRUCache from 'lru-cache';

// Map id to name
import {StationName, EpochMS} from '../bin/types';

import {H3_CACHE_FLUSH_PERIOD_MS, MAX_STATION_DBS, STATION_DB_EXPIRY_MS, DB_PATH} from '../common/config';

export type BatchOperation = ClassicBatchOperation<DB, string, Uint8Array>;

const options = {
    max: MAX_STATION_DBS + 1, // global is stored in the cache
    dispose: function (db: Promise<DB>, key: StationName, r: string) {
        db.then((db) => {
            if (db.status == 'closed') {
                return; // if it's already closed then that's cool let it go
            }
            return db.dispose();
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
export class DB extends ClassicLevel<string, Uint8Array> {
    ognInitialTS: EpochMS;
    ognStationName: StationName;
    global: boolean;

    constructor(stationName: StationName) {
        const path = stationName === 'global' ? DB_PATH + stationName : DB_PATH + '/stations/' + stationName;
        super(path, {valueEncoding: 'view'});

        // You can get either global or specific station, they are stored in slightly different places
        this.ognStationName = stationName;
        this.ognInitialTS = Date.now() as EpochMS;
        this.global = stationName === 'global';
        this.manuallyClosed = false;
    }

    async close() {
        return this.dispose(true);
    }

    async dispose(manualClose: boolean = false) {
        if (!manualClose || stationDbCache.getRemainingTTL(this.ognStationName) < H3_CACHE_FLUSH_PERIOD_MS / 1000) {
            console.log(`Closing database ${this.ognStationName} while it's still needed. You should increase MAX_STATION_DBS in .env.local [db status ${this.status}]`);
        }

        try {
            await super.close();
        } catch (e) {
            console.log(`error closing ${this.ognStationName}: ${e} closeReason: ${manualClose ? 'manual' : 'cache dispose'}`);
        }

        stationDbCache.delete(this.ognStationName);
        //        return super.close();
    }

    private manuallyClosed: boolean;
}

export async function getDb(
    stationName: StationName, //
    options: {open?: boolean; existingOnly?: boolean; throw?: boolean} = {open: true}
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
    options: {open?: boolean; existingOnly?: boolean} = {open: true}
): Promise<DB> {
    //
    let stationDbPromise: Promise<DB> | undefined = stationDbCache.get(stationName);

    if (!stationDbPromise) {
        stationDbPromise = new Promise<DB>((resolve, reject) => {
            if (options.existingOnly) {
                reject(new Error(`Unable to create ${stationName} as only existing db requested`));
                return;
            }

            const stationDb = new DB(stationName) as DB;

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
