import LevelUP from 'levelup';
import LevelDOWN from 'leveldown';

import dotenv from 'dotenv';

import {ignoreStation} from '../lib/bin/ignorestation.js';

import {CoverageRecord, bufferTypes} from '../lib/bin/coveragerecord.js';
import {CoverageHeader, accumulatorTypes} from '../lib/bin/coverageheader.js';

import {whatAccumulators, purgeOldAccumulators} from '../lib/bin/rollup.js';

import {DB_PATH, OUTPUT_PATH} from '../lib/common/config.js';

import yargs from 'yargs';

import {setTimeout} from 'timers/promises';

main().then('exiting');

//
// Primary configuration loading and start the aprs receiver
async function main() {
    const args = yargs(process.argv.slice(2)) //
        .option('db', {alias: 'd', type: 'string', default: null, description: 'Choose Database, empty for all'})
        .help()
        .alias('help', 'h').argv;

    // What file/s
    const stations = {};
    const stationDbCache = new Map();

    const now = new Date();

    // If none specified then process all of them
    if (!args.db) {
        async function loadStationStatus(statusdb) {
            const nowEpoch = Math.floor(now / 1000);
            const expiryEpoch = nowEpoch - STATION_EXPIRY_TIME_SECS;
            try {
                for await (const [key, value] of statusdb.iterator()) {
                    stations[key] = JSON.parse(String(value));
                }
            } catch (e) {
                console.log('Unable to loadStationStatus', e);
            }
            const nextid =
                (_reduce(
                    stations,
                    (highest, i) => {
                        return highest < (i.id || 0) ? i.id : highest;
                    },
                    0
                ) || 0) + 1;
        }
        await loadStationStatus(statusDb);
    } else if (args.db == 'global') {
        // One entry as a dummy for the purge function
        stations[0] = {station: args.db, id: 0};
        stationDbCache.set(0, LevelUP(LevelDOWN(DB_PATH + 'global')));
    } else {
        // One entry as a dummy for the purge function
        stations[0] = {station: args.db, id: 0};
    }

    const {current, accumulators} = whatAccumulators(now);

    for (const a in accumulators) {
        console.log('  ' + JSON.stringify(accumulators[a]));
    }

    console.log(stations);

    // Run the purge
    await purgeOldAccumulators(stations, stationDbCache, current, accumulators);
    /*  
    const ld = new LevelDOWN(DB_PATH + '/stations/' + 'global');
    console.log(await Promise.allSettled([new Promise((resolve) => ld.open({createIfMissing: false}, resolve))]));
    console.log(`global: compacting...`);
    await Promise.allSettled([new Promise((resolve) => ld.compactRange('0', 'ffff/ffffffffffffffff', resolve))]);
    console.log(ld.db.getProperty('leveldb.stats'));
    ld.approximateSize('0000/0', 'ffff/ffff', (e, r) => {
        console.log(`global: remaining size: ${r.toFixed(0)}`);
    });
    console.log('done');
    */

    //    for (const db in stationDbCache) {
    //        await db.open();
    //    stationDbCache.get(0).db.close((a) => {
    //        console.log('db closed', a);
    //    });
    //    await stationDbCache.get(0).close();
    //    stationDbCache.set(0, null);
    //    //    }
    await setTimeout(20000);

    console.log('reopen');
    const db1 = LevelUP(LevelDOWN(DB_PATH + 'global'));
    await setTimeout(10000);
    try {
        console.log('get');
        await db1.get('1213');
    } catch (e) {
        console.log(e);
    }

    //        stations[0] = {station: args.db, id: 0};
    /*
//    const db = LevelDOWN(DB_PATH + 'global');
//    await Promise.allSettled([new Promise((resolve) => db.open({createIfMissing: false}, resolve))]);

    //    await Promise.allSettled([new Promise((resolve) => leveldown.repair(DB_PATH + '/stations/' + 'global', resolve))]);
*/
    // Let the compact run for a bit...
    for (let a = 0; a < 10; a++) {
        await setTimeout(10000);
        //        console.log(stationDbCache.get(0).db.getProperty('leveldb.stats'));
    }
}
