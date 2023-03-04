// DB
import {ClassicLevel} from 'classic-level';

// Least Recently Used cache for Station Database connectiosn
import LRUCache from 'lru-cache';

import AsyncLock from 'async-lock';
let lock = new AsyncLock();

// Map id to name
import {getStationName} from './stationstatus';

import {StationName, StationId, EpochMS} from './types';

import {saveAccumulatorMetadata} from './rollup';

import {H3_CACHE_FLUSH_PERIOD_MS, MAX_STATION_DBS, STATION_DB_EXPIRY_MS, DB_PATH} from './config';

const options = {
    max: MAX_STATION_DBS + 1, // global is stored in the cache
    dispose: function (db, key, r) {
        if (db.status == 'closed') {
            return; // if it's already closed then that's cool let it go
        }
        console.log('~', db.ognStationName, key, r);
        if (db.global && r == 'evict') {
            console.warn('closing global db on cache evict');
        }
        if (stationDbCache.getRemainingTTL(key) < H3_CACHE_FLUSH_PERIOD_MS / 1000) {
            console.log(`Closing database ${key} while it's still needed. You should increase MAX_STATION_DBS in .env.local [db status ${db.status}]`);
        }
        db.close((e) => {
            console.log('closed', db.ognStationName, e);
        });
    },
    updateAgeOnGet: true,
    allowStale: true,
    ttl: STATION_DB_EXPIRY_MS
};

//
// Instantiate - we drop the type because we
const stationDbCache = new LRUCache(options);
export interface DB extends ClassicLevel<string, Uint8Array> {
    ognInitialTS: EpochMS;
    ognStationName: StationName;
    global: boolean;
    cached: boolean;
}

const CurrentlyOpen: Set<StationName> = new Set<StationName>();

// Open the global database
export function initialiseStationDbCache() {
    //    getDb('global' as StationName, true);
}

//
// Get a db from the cache, by name or number
export async function getDb(station: StationName | StationId, options: {cache?: boolean; open?: boolean; existingOnly?: boolean; throw?: boolean; noMeta?: boolean} = {cache: true, open: true}): Promise<DB | undefined> {
    // If it's a number we need a name
    let stationName: StationName | undefined = typeof station === 'number' ? getStationName(station) : station;
    if (!stationName) {
        console.error(`Unable to getDb, ${station} unknown`);
        if (options.throw) {
            throw new Error(`Unable to getDb, ${station} unknown`);
        }
        return undefined;
    }
    const stack = new Error('stack');

    // Prevent re-entrancy
    const openDb = async () => {
        let stationDb = stationDbCache.get(stationName) as DB;
        if (!stationDb && !options.existingOnly) {
            // You can get either global or specific station, they are stored in slightly different places
            const path = stationName == 'global' ? DB_PATH + stationName : DB_PATH + '/stations/' + stationName;

            stationDb = new ClassicLevel<string, Uint8Array>(path, {valueEncoding: 'view'}) as DB;
            CurrentlyOpen.add(stationName);

            stationDb.ognInitialTS = Date.now() as EpochMS;
            stationDb.ognStationName = stationName;
            stationDb.global = stationName == 'global';

            // We are supposed to open the database
            if (options.open) {
                try {
                    await stationDb.open();
                } catch (e) {
                    CurrentlyOpen.delete(stationName);
                    console.log(`${stationName}: Failed to open: ${stationDb.status}: ${e.cause?.code || e.code}`, stack);
                    stationDb.close();
                    stationDb = undefined;
                    if (options.throw) {
                        throw e;
                    }
                }
            }

            // If we opened successfully and are supposed to cache it
            if (options.cache && stationDb) {
                stationDbCache.set(stationName, stationDb);
                stationDb.cached = true;
            }

            // If we are opening and haven't been told to skip the meta data then we will write the current meta
            // data into the file. Only reason to open without skipping is because we have a record to save in the
            // file
            if (!options.noMeta) {
                await saveAccumulatorMetadata(stationDb);
            }
        }

        if (stationDb && !(stationDb.status == 'open' || stationDb.status == 'opening')) {
            console.log(stationName, stationDb.status, new Error('db status invalid'));
        }

        return stationDb;
    };

    return lock.acquire(stationName, (done) => {
        openDb()
            .then((r) => done(null, r))
            .catch((e) => done(e, null));
    });
}

// Size excluding global
export function getStationDbCacheSize(): number {
    return stationDbCache.size;
}

//
// Close an open database and remove from the cache
// removing from cache closes the database
export async function closeDb(db: DB): Promise<void> {
    if (db.cached) throw new Error(`closing Cached DB ${db.ognStationName} not supported`);
    CurrentlyOpen.delete(db.ognStationName);
    return db.close();
}

//
// Purge all entries - will call the dispose function thereby closing the database entry
export async function closeAllStationDbs(): Promise<void> {
    // Temp copy and clear as non-interruptable operation
    const dbs = stationDbCache.rvalues();
    CurrentlyOpen.clear();

    // Close all the databases
    const promises: Promise<void>[] = [];
    for (const v of dbs) {
        promises.push(
            lock.acquire(v.ognStationName, (done) => {
                v.close((r) => done(null, r));
            })
        );
    }
    await Promise.allSettled(promises);
    stationDbCache.clear();
}

export function allOpenDbs(header) {
    console.log(header, [...CurrentlyOpen.keys()].sort().join(','));
}
