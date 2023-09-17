import dotenv from 'dotenv';
dotenv.config({path: '.env.local', override: true});

import {ClassicLevel} from 'classic-level';

import {CoverageRecord} from '../lib/bin/coveragerecord';
import {CoverageHeader} from '../lib/bin/coverageheader';

import {DB_PATH} from '../lib/common/config';

import yargs from 'yargs';

main().then(() => 'exiting');

//
// Primary configuration loading and start the aprs receiver
async function main() {
    const args = await yargs(process.argv.slice(2)) //
        .option('db', {alias: 'd', type: 'string', default: 'global', description: 'Choose Database'})
        .option('all', {alias: 'a', type: 'boolean', description: 'dump all records'})
        .option('match', {type: 'string', description: 'regex match of dbkey'})
        .option('size', {alias: 's', type: 'boolean', description: 'determine approximate size of each block of records'})
        .option('count', {alias: 'c', type: 'boolean', description: 'count number of records in each block'})
        .help()
        .alias('help', 'h').argv;

    // What file
    let dbPath = DB_PATH;
    if (args.db && args.db != 'global') {
        dbPath += '/stations/' + args.db;
    } else {
        dbPath += 'global';
    }

    let db = null;

    try {
        db = new ClassicLevel<string, Uint8Array>(dbPath, {valueEncoding: 'view', createIfMissing: false});
        await db.open();
    } catch (e) {
        console.error(e);
    }

    if (!db) {
        return;
    }

    console.log('---', dbPath, '---');

    let n = db.iterator();
    let accumulators: Record<string, any> = {};
    let x = n.next();
    let y = null;
    while ((y = await x)) {
        const [key, value] = y;
        let hr = new CoverageHeader(key);

        if (!args.match || hr.dbKey().match(args.match)) {
            if (hr.isMeta) {
                accumulators[hr.accumulator] = {hr: hr, meta: JSON.parse(String(value)), count: 0, size: 0};
                console.log(hr.dbKey(), String(value));

                if (args.size) {
                    db.approximateSize(CoverageHeader.getAccumulatorBegin(hr.type, hr.bucket), CoverageHeader.getAccumulatorEnd(hr.type, hr.bucket), (e, r) => {
                        accumulators[hr.accumulator].size = r;
                    });
                }
            } else {
                if (accumulators[hr.accumulator]) {
                    accumulators[hr.accumulator].count++;
                } else {
                    accumulators[hr.accumulator] = {hr: hr, count: 1, size: 0};
                }

                if (accumulators[hr.accumulator].count == 1 && args.size) {
                    db.approximateSize(CoverageHeader.getAccumulatorBegin(hr.type, hr.bucket), CoverageHeader.getAccumulatorEnd(hr.type, hr.bucket), (e, r) => {
                        accumulators[hr.accumulator].size = r;
                    });
                }
                if (args.all) {
                    console.log(hr.dbKey(), JSON.stringify(new CoverageRecord(value).toObject()));
                } else if (args.count) {
                } else {
                    n.seek(CoverageHeader.getAccumulatorEnd(hr.type, hr.bucket));
                }
            }
        }
        x = n.next();

        if ((y = await x)) {
            const [key, value] = y;
            let hr = new CoverageHeader(key);
        }
    }
    for (const a in accumulators) {
        console.log(`${accumulators[a].hr.typeName} [${a}]: ${accumulators[a].count} records, ~ ${accumulators[a].size} bytes`);
        console.log('  ' + JSON.stringify(accumulators[a].meta));
    }
}
