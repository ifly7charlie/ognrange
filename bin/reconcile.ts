import dotenv from 'dotenv';
dotenv.config({path: '.env.local', override: true});

import {readFileSync, readdirSync} from 'fs';
import {tableFromIPC, RecordBatchStreamReader} from 'apache-arrow/Arrow.node';

import {ClassicLevel} from 'classic-level';

import {CoverageRecord} from '../lib/bin/coveragerecord';
import {CoverageHeader, AccumulatorTypeString} from '../lib/bin/coverageheader';
import {whatAccumulators} from '../lib/bin/accumulators';
import {saveAccumulatorMetadata} from '../lib/worker/rollupmetadata';

import {StationId} from '../lib/bin/types';

import {ROLLUP_PERIOD_MINUTES, DB_PATH, OUTPUT_PATH} from '../lib/common/config';

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
        .option('fix', {type: 'boolean', default: false, description: 'attempt to recover missing or low records'})
        .option('fixmeta', {type: 'boolean', default: false, description: 'just fix missing metadata - uses arrow files'})
        .option('station', {alias: 's', type: 'string', default: 'global', description: 'Station'})
        .option('period', {alias: 'p', type: 'string', default: 'day,month,year', description: 'Type of file (accumulator)'})
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
        db = new ClassicLevel<string, Uint8Array>(dbPath, {valueEncoding: 'view', createIfMissing: args.fix});
        await db.open();
    } catch (e) {
        console.error(e);
        throw e;
    }

    return db;
}
/*async function getAccumulatorsFromDb(db: DB) {
    let n = db.iterator();
    let accumulators: Record<string, any> = {};
    let x = n.next();
    let y: any = null;
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
}*/

function getAccumulatorsFromDisk(file: string) {
    const d = JSON.parse(readFileSync(file, {encoding: 'utf-8'}));
    //    console.log(d);
    try {
        d.accumulators['day'].hr = CoverageHeader.getAccumulatorMeta('day' as AccumulatorTypeString, d.accumulators['day'].bucket);
        d.accumulators['month'].hr = CoverageHeader.getAccumulatorMeta('month' as AccumulatorTypeString, d.accumulators['month'].bucket);
        d.accumulators['year'].hr = CoverageHeader.getAccumulatorMeta('year' as AccumulatorTypeString, d.accumulators['year'].bucket);

        // Ensure we have a current
        if (!d.accumulators.current) {
            const now = new Date().toISOString().startsWith(d.accumulators.day.file) ? new Date() : new Date(d.accumulators.day.file);
            const a = whatAccumulators(now) as any;
            d.accumulators.current = a.current;
        }
    } catch (e) {
        console.log(`incomplete metagdata for ${file}`);
        return null;
    }

    return d.accumulators;
}

function getFilesForStation(station: string) {
    const files = readdirSync(OUTPUT_PATH + station)
        .map((fn) => {
            const [_all, type, date] = fn.match(/(day|month|year)\.([0-9-]+)\.arrow\.gz/) ?? ['', null, null];
            return {type: type ?? '', date: date ?? '', fileName: fn};
        })
        .filter((a) => !!a.type)
        .sort((b, a) => a.fileName.localeCompare(b.fileName));

    const dayFile = files.find((x) => x.type === 'day');
    const monthFile = files.find((x) => x.type === 'month');
    const yearFile = files.find((x) => x.type === 'year');

    if (!dayFile?.date || !monthFile?.date || !yearFile || !dayFile?.date?.startsWith(yearFile?.date ?? '-no') || !dayFile?.date?.startsWith(monthFile?.date ?? '-no')) {
        throw new Error(`Arrow files are not consistent in dates ${dayFile}, ${monthFile} ${yearFile}`);
    }

    const now = new Date().toISOString().startsWith(dayFile.date) ? new Date() : new Date(dayFile.date);

    const a = whatAccumulators(now) as any;
    a['day'].hr = CoverageHeader.getAccumulatorMeta('day' as AccumulatorTypeString, a['day'].bucket);
    a['month'].hr = CoverageHeader.getAccumulatorMeta('month' as AccumulatorTypeString, a['month'].bucket);
    a['year'].hr = CoverageHeader.getAccumulatorMeta('year' as AccumulatorTypeString, a['year'].bucket);

    return a;
}

// Check all arrow data is in the station
async function reconcilePeriod(sa: any, period: string, station: string, db: DB, fix: boolean, fixmeta: boolean) {
    const file = OUTPUT_PATH + station + '/' + station + '.' + period + '.arrow.gz';
    let differences = 0;
    let rows = 0;
    const fd = await open(file);

    const a = sa[period];

    const reader = await RecordBatchStreamReader.from(fd.createReadStream().pipe(createGunzip()));

    for await (const batch of reader) {
        for (const columns of batch) {
            const json = columns.toJSON();

            const h3 = prefixWithZeros(7, json.h3hi?.toString(16) || 'null') + prefixWithZeros(8, json.h3lo?.toString(16) || 'null');
            delete json.h3lo;
            delete json.h3hi;

            const ch = new CoverageHeader(0 as StationId, a.hr.type, a.hr.bucket, h3);

            // Look it up in the db
            let row: Uint8Array | undefined = undefined;
            try {
                rows++;
                row = await db.get(ch.dbKey());
            } catch (e) {}

            let dofix = fix;
            if (row) {
                const cr = new CoverageRecord(row);
                const dbAsArrow = cr.arrowFormat();

                const difference = pickBy(dbAsArrow as any, (v, k) => json[k] !== v);
                if (difference.length) {
                    console.log(h3, difference);
                    differences++;
                    if (cr.count >= json.count) {
                        dofix = false;
                    }
                } else {
                    dofix = false;
                }
            } else {
                differences++;
                console.log(h3, 'absent from db');
            }

            if (dofix) {
                // If the database is behind the json then we will rebuild
                console.log(`-> fixing ${ch.dbKey()}`);

                try {
                    const newCr = CoverageRecord.fromArrow(json);
                    console.log('=>', newCr.arrowFormat(), json);
                    await db.put(ch.dbKey(), newCr.buffer());
                } catch (e) {
                    console.log('attempting to fix:', ch.dbKey(), e);
                }
            }
        }
    }

    if ((differences && fix) || fixmeta) {
        console.log('saving metadata');
        await saveAccumulatorMetadata(db as any, sa);
    }

    console.log(`${a.file}: ${differences} differences, ${rows} rows from ${station} ${period}`);
}

async function reconcile() {
    const args = await getArgs();
    const db = await openDb(args);
    ////await getAccumulatorsFromDb(db);

    // Figure out what accumulators are relevant
    let saccumulators: any = getFilesForStation(args.station) ?? getAccumulatorsFromDisk(OUTPUT_PATH + args.station + '/' + args.station + '.year.json');
    let gaccumulators = getAccumulatorsFromDisk(OUTPUT_PATH + 'global/global.year.json');

    for (const period of args.period.split(',')) {
        const sa = saccumulators[period];
        const ga = gaccumulators[period];

        if (!sa || !ga) {
            console.log(`accumulator ${period} not found in station or global, available accumulators:`, saccumulators, gaccumulators);
            return;
        }

        // If it isn't relevant any longer then we can ignore it
        if (sa.file < ga.file) {
            console.log(`station accumulator for ${period} no longer relevant ${sa.file} < ${ga.file}`);
        } else if (sa.file > ga.file) {
            console.log(`data missing from global records for ${sa.file}`);
        } else {
            if (!args.fix && args.fixmeta) {
                await saveAccumulatorMetadata(db as any, saccumulators);
            } else {
                reconcilePeriod(saccumulators, period, args.station, db, args.fix, args.fixmeta);
            }
        }
    }
}

reconcile();
