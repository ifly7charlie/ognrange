// DB
import {ClassicLevel, BatchOperation as ClassicBatchOperation} from 'classic-level';

// Least Recently Used cache for Station Database connectiosn
import LRUCache from 'lru-cache';

// Map id to name
import {StationName, EpochMS} from '../bin/types';

import {MAX_STATION_DBS, STATION_DB_EXPIRY_MS, DB_PATH} from '../common/config';

export type BatchOperation = ClassicBatchOperation<DB, string, Uint8Array>;

const options = {
    max: MAX_STATION_DBS + 1, // global is stored in the cache
    dispose: function (db: DB, key: StationName, r: string) {
        if (r === 'evict' && db.status != 'closed') {
            return db.dispose();
        }
    },
    updateAgeOnGet: true,
    allowStale: true,
    ttl: STATION_DB_EXPIRY_MS
};

const openStations = new Map<StationName, DB>();
const stationDbCache = new LRUCache<StationName, DB>(options);

//
// Instantiate - we drop the type because we
export class DB extends ClassicLevel<string, Uint8Array> {
    ognInitialTS: EpochMS;
    ognStationName: StationName;
    global: boolean;
    referenceCount: number;

    constructor(stationName: StationName) {
        const path = stationName === 'global' ? DB_PATH + stationName : DB_PATH + '/stations/' + stationName;
        super(path, {valueEncoding: 'view'});

        // You can get either global or specific station, they are stored in slightly different places
        this.ognStationName = stationName;
        this.ognInitialTS = Date.now() as EpochMS;
        this.global = stationName === 'global';
        this.referenceCount = 0;

        //        console.log(`opening ${stationName} from ${openStations.has(stationName) ? 'open list' : stationDbCache.has(this.ognStationName) ? 'cached list' : 'disk'} ${openStations.size}o ${stationDbCache.size}c`);

        this.use();
    }

    //
    async close() {
        //        console.log(`closing ${this.ognStationName} ${this.referenceCount}`);
        this.referenceCount -= 1;
        if (!this.referenceCount && !this.global) {
            openStations.delete(this.ognStationName);
            stationDbCache.set(this.ognStationName, this);
        }
    }

    private use() {
        //        console.log(`opening ${this.ognStationName} ${this.referenceCount}`);
        if (!this.referenceCount) {
            openStations.set(this.ognStationName, this);
            stationDbCache.delete(this.ognStationName);
        }
        this.referenceCount += 1;
        return this;
    }

    // Called when it drops out of the LRU cache
    async dispose() {
        if (this.referenceCount > 0) {
            console.error(new Error(`Closing database ${this.ognStationName} while it's still needed ${this.referenceCount}`));
        }

        try {
            await super.close();
        } catch (e) {
            console.log(`error closing ${this.ognStationName}: ${e}`);
        }

        stationDbCache.delete(this.ognStationName);
    }

    // Helper for opening the database
    static async getDbThrow(
        stationName: StationName, //
        options: {open?: boolean; existingOnly?: boolean} = {open: true}
    ): Promise<DB> {
        //
        let stationDb: DB | undefined = openStations.get(stationName) ?? stationDbCache.get(stationName);

        if (stationDb) {
            return stationDb.use();
        }

        if (options.existingOnly) {
            throw new Error(`Unable to create ${stationName} as only existing db requested`);
        }

        const db = new DB(stationName);

        // We are supposed to open the database
        return db
            .open()
            .then(() => {
                if (!(db.status == 'open' || db.status == 'opening')) {
                    console.log(stationName, db.status, new Error('db status invalid'));
                    throw new Error(`Db ${stationName} status invalid, ${db.status}`);
                }
                return db;
            })
            .catch((e: any) => {
                console.log(`${stationName}: Failed to open: ${db.status}: ${e.cause?.code || e.code}`);
                db.close().catch((e) => {
                    /* ignore */
                });
                throw e;
            });
    }
}

//
// Get a db from the cache, by name or number
export async function getDbThrow(
    stationName: StationName, //
    options: {open?: boolean; existingOnly?: boolean} = {open: true}
): Promise<DB> {
    return DB.getDbThrow(stationName, options);
}

//
// Purge all entries - will call the dispose function thereby closing the database entry
export async function closeAllStationDbs(): Promise<void> {
    // Temp copy and clear as non-interruptable but async operation
    const numberOfDbs = stationDbCache.size;
    const dbs = [...stationDbCache.rvalues()];
    const open = [...openStations.entries()];
    stationDbCache.clear();
    openStations.clear();

    // Force close all the databases, will cause errors as DBs are probably still in use
    const promises: Promise<void>[] = [];
    for (const db of dbs) {
        promises.push(
            db.dispose().catch((_e) => {
                /**/
            })
        );
    }

    for (const [_name, db] of open) {
        promises.push(
            db.dispose().catch((_e) => {
                /**/
            })
        );
    }

    await Promise.allSettled(promises);
    console.log(`closed ${numberOfDbs} station databases`);
}
