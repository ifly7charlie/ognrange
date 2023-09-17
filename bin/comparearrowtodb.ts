import dotenv from 'dotenv';
dotenv.config({path: '.env.local', override: true});

import {readFileSync} from 'fs';
import {tableFromIPC, RecordBatchStreamReader} from 'apache-arrow/Arrow.node';

import {ClassicLevel} from 'classic-level';

import {CoverageRecord} from '../lib/bin/coveragerecord';
import {CoverageHeader} from '../lib/bin/coverageheader';

import {StationId} from '../lib/bin/types';

import {DB_PATH, OUTPUT_PATH} from '../lib/common/config';

import {pickBy} from 'lodash';

import yargs from 'yargs';
import {open} from 'node:fs/promises';

import {prefixWithZeros} from '../lib/common/prefixwithzeros';
import {Readable, pipeline} from 'node:stream';

import {createGunzip} from 'node:zlib';

type DB = ClassicLevel<string, Uint8Array>;

async function getArgs() {
    const args = await yargs(process.argv.slice(2)) //
        .option('stations', {type: 'boolean'})
        .option('station', {alias: 's', type: 'string', default: 'global', description: 'Station'})
        .option('period', {alias: 'p', type: 'string', default: 'year', description: 'Type of file (accumulator)'})
        .help()
        .alias('help', 'h').argv;

    return args;
}

async function openDb(args: Awaited<ReturnType<typeof getArgs>>): Promise<DB> {
    let dbPath = DB_PATH;
    if (args.station && args.station != 'global') {
        dbPath += '/stations/' + args.station;
    } else {
        dbPath += 'global';
    }

    let db = null;

    try {
        db = new ClassicLevel<string, Uint8Array>(dbPath, {valueEncoding: 'view', createIfMissing: false});
        await db.open();
    } catch (e) {
        console.error(e);
        throw e;
    }

    return db;
}
async function getAccumulatorsFromDb(db: DB) {
    let n = db.iterator();
    let accumulators: Record<string, any> = {};
    let x = n.next();
    let y = null;
    while ((y = await x)) {
        const [key, value] = y;
        let hr = new CoverageHeader(key);

        if (hr.isMeta) {
            accumulators[hr.typeName] = {hr: hr, meta: JSON.parse(String(value)), count: 0, size: 0};
            n.seek(CoverageHeader.getAccumulatorEnd(hr.type, hr.bucket));
        }
        x = n.next();
    }
    return accumulators;
}

// Check all arrow data is in the station
async function reconcile() {
    const args = await getArgs();
    const db = await openDb(args);
    const accumulators = await getAccumulatorsFromDb(db);

    const a = accumulators[args.period];
    if (!a) {
        console.log(`accumulator ${args.period} not found in database, available accumulators:`, accumulators);
        return;
    }

    const file = OUTPUT_PATH + args.station + '/' + args.station + '.' + args.period + '.arrow.gz';
    let differences = 0;
    let rows = 0;
    const fd = await open(file);

    const reader = await RecordBatchStreamReader.from(fd.createReadStream().pipe(createGunzip()));

    for await (const batch of reader) {
        let c = 0;
        let d = 0;

        let lo = 0;
        let hi = 0;
        for (const columns of batch) {
            let out = '';
            const json = columns.toJSON();

            const h3 = prefixWithZeros(7, json.h3hi?.toString(16) || 'null') + prefixWithZeros(8, json.h3lo?.toString(16) || 'null');
            delete json.h3lo;
            delete json.h3hi;

            const ch = new CoverageHeader(0 as StationId, a.hr.type, a.hr.bucket, h3);

            // Look it up in the db
            try {
                rows++;
                const row = await db.get(ch.dbKey());
                if (row) {
                    const cr = new CoverageRecord(row);
                    const db = cr.arrowFormat(ch);

                    const difference = pickBy(db as any, (v, k) => json[k] !== v);
                    if (difference.length) {
                        console.log(h3, difference);
                        console.log(cr.arrowFormat(ch), json);
                        differences++;
                    }
                }
            } catch (e) {
                console.log(ch.dbKey(), e);
            }
        }
    }
    console.log(`${differences} differences, ${rows} rows from ${args.station} ${args.period}`);
}

reconcile();
