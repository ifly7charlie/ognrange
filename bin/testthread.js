import {ClassicLevel} from 'classic-level';

import AsyncLock from 'async-lock';
import dotenv from 'dotenv';

import {ignoreStation} from '../lib/bin/ignorestation.js';

import {CoverageRecord, bufferTypes} from '../lib/bin/coveragerecord.js';
import {CoverageHeader, accumulatorTypes} from '../lib/bin/coverageheader.js';

import {whatAccumulators, purgeOldAccumulators} from '../lib/bin/rollup.js';

import {DB_PATH, OUTPUT_PATH} from '../lib/bin/config.js';

import yargs from 'yargs';

import {setTimeout as _setTimeout} from 'timers/promises';

let lock = new AsyncLock();

if (isMainThread) {
    main().then('exiting');
}

import {BroadcastChannel, Worker, parentPort, isMainThread, workerData, SHARE_ENV} from 'node:worker_threads';

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

    //        stations[0] = {station: args.db, id: 0};
    /*
//    const db = LevelDOWN(DB_PATH + 'global');
//    await Promise.allSettled([new Promise((resolve) => db.open({createIfMissing: false}, resolve))]);

    //    await Promise.allSettled([new Promise((resolve) => leveldown.repair(DB_PATH + '/stations/' + 'global', resolve))]);
*/
    // Let the compact run for a bit...
    //    for (let a = 0; a < 10; a++) {
    //    await setTimeout(10000);
    //        console.log(stationDbCache.get(0).db.getProperty('leveldb.stats'));
    //    }
    //    spawn();

    setTimeout(() => {
        lock.acquire('a', (done) => {
            console.log('a', 1);
            setTimeout(done, 5000);
        });
    }, 500);

    setTimeout(() => {
        lock.acquire('a', (done) => {
            console.log('a', 2);
            setTimeout(done, 1000);
        });
    }, 700);

    //    await _setTimeout(900);

    await lock.acquire('a', (done) => {
        console.log('a', 0);
        setTimeout(done, 10000);
    });

    //    const db1 = new ClassicLevel(DB_PATH + 'global', {valueEncoding: 'buffer'});
    console.log(
        //        await db1.get('123').catch((e) => {
        //            console.log('no way', e);
        //        })
        'done'
    );
    await _setTimeout(20000);
}

import {fileURLToPath} from 'url';
export function spawn(db) {
    if (!isMainThread) {
        throw new Error('umm, this is only available in main thread');
    }
    console.log('Starting APRS worker thread', fileURLToPath(import.meta.url));

    return new Worker(fileURLToPath(import.meta.url), {env: SHARE_ENV, workerData: db});
}

if (!isMainThread) {
    console.log('thread');
    await setTimeout(10000);
    //    const db1 = new ClassicLevel(DB_PATH + 'global', {valueEncoding: 'buffer'});
    //    await db1.open();
    //    const db = workerData;

    for (let a = 0; a < 10; a++) {
        try {
            console.log('get');
            //            await db.get('1213').catch((e) => {
            //              console.log('nope', e);
            //        });
            console.log('~get');
        } catch (e) {
            console.log(e);
        }
        await setTimeout(2000);
    }
    await setTimeout(10000);
}
