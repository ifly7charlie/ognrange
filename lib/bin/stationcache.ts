// DB
import {ClassicLevel} from 'classic-level';

// Least Recently Used cache for Station Database connectiosn
import LRUCache from 'lru-cache';

// Map id to name
import {getStationName} from './stationstatus';

import {StationName, StationId, EpochMS} from './types';

import {H3_CACHE_FLUSH_PERIOD_MS, MAX_STATION_DBS, STATION_DB_EXPIRY_MS, DB_PATH} from './config';

const options = {
    max: MAX_STATION_DBS + 1, // global is stored in the cache
    dispose: function (db, key, r) {
        try {
            db.close();
        } catch (e) {
            console.log('ummm', e);
        }
        if (stationDbCache.getRemainingTTL(key) < H3_CACHE_FLUSH_PERIOD_MS / 1000) {
            console.log(`Closing database ${key} while it's still needed. You should increase MAX_STATION_DBS in .env.local`);
        }
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
}

// Open the global database
export function initialiseStationDbCache() {
    //    getDb('global' as StationName, true);
}

//
// Get a db from the cache, by name or number
export function getDb(station: StationName | StationId, open = true): DB | undefined {
    if (typeof station === 'number') {
        station = getStationName(station);
        if (!station) {
            console.error('unable to getDb', station);
            return undefined;
        }
    }
    let stationDb = stationDbCache.get(station) as DB;
    if (!stationDb && open) {
        // You can get either global or specific station, they are stored in slightly different places
        const path = station == 'global' ? DB_PATH + station : DB_PATH + '/stations/' + station;

        stationDbCache.set(station, (stationDb = new ClassicLevel<string, Uint8Array>(path, {valueEncoding: 'view'}) as DB));
        stationDb.ognInitialTS = Date.now() as EpochMS;
        stationDb.ognStationName = station;
        stationDb.global = station == 'global';

        stationDb.open();
    }
    return stationDb;
}

// Size excluding global
export function getStationDbCacheSize(): number {
    return stationDbCache.size - 1;
}

//
// Close an open database and remove from the cache
// removing from cache closes the database
export function closeDb(station: StationName | DB): void {
    const db = typeof station === 'string' ? getDb(station, false) : station;
    if (db && !db.global) {
        stationDbCache.delete(station);
    }
}

//
// Purge all entries - will call the dispose function thereby closing the database entry
export function closeAllStationDbs(): void {
    stationDbCache.clear();
}
