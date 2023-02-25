// DB
import LevelUP from 'levelup';
import LevelDOWN from 'leveldown';

// Least Recently Used cache for Station Database connectiosn
const LRUCache = require('lru-cache');

import {H3_CACHE_FLUSH_PERIOD_MS, MAX_STATION_DBS, STATION_DB_EXPIRY_MS, DB_PATH} from './config.js';

const options = {
    max: MAX_STATION_DBS,
    dispose: function (db, key, r) {
        try {
            db.close();
        } catch (e) {
            console.log('ummm', e);
        }
        if (getCacheTtl(key) < H3_CACHE_FLUSH_PERIOD_MS / 1000) {
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

function getCacheTtl(k) {
    return (typeof performance === 'object' && performance && typeof performance.now === 'function' ? performance : Date).now() - stationDbCache.starts[stationDbCache.keyMap.get(k)];
}

export interface db extends LevelUP {
    ognInitialTS: number;
    ognStationName: string;
    global: boolean;
}

stationDbCache.set('global', LevelUP(LevelDOWN(DB_PATH + 'global')));

//
// Get a db from the cache
export function getDb(station: string, open = true): db {
    let stationDb = stationDbCache.get(station) as db;
    if (!stationDb && open) {
        // You can get either global or specific station, they are stored in slightly different places
        const path = station == 'global' ? DB_PATH + station : DB_PATH + '/stations/' + station;

        stationDbCache.set(station, (stationDb = LevelUP(LevelDOWN(path))));
        stationDb.ognInitialTS = Date.now();
        stationDb.ognStationName = station;
        stationDb.global = station == 'global';
    }
    return stationDb;
}

//
// Close an open database and remove from the cache
// removing from cache closes the database
export function closeDb(station: string | db): void {
    const db = typeof station === 'string' ? getDb(station, false) : station;
    if (db && !db.global) {
        stationDbCache.delete(station);
    }
}

//
// Purge all entries - will call the dispose function thereby closing the database entry
export function closeAllDbs(): void {
    stationDbCache.clear();
}
