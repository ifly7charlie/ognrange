import dotenv from 'dotenv';
dotenv.config({path: '.env.local', override: true});

import {ClassicLevel} from 'classic-level';

import {CoverageRecord, CoverageRecordOut} from '../lib/bin/coveragerecord';
import {CoverageHeader} from '../lib/bin/coverageheader';

import {DB_PATH} from '../lib/common/config';

import yargs from 'yargs';

main().then(() => 'exiting');

//
// Primary configuration loading and start the aprs receiver
async function main() {
    const args = await yargs(process.argv.slice(2)) //
        .option('db', {alias: 'd', type: 'string', default: 'global', description: 'Choose Database'})
        .option('station', {alias: 's', type: 'string', description: 'Choose station database (same as -d <name>)'})
        .option('all', {alias: 'a', type: 'boolean', description: 'dump all records'})
        .option('match', {type: 'string', description: 'regex match of dbkey'})
        .option('size', {type: 'boolean', description: 'determine approximate size of each block of records'})
        .option('count', {alias: 'c', type: 'boolean', description: 'count number of records in each block'})
        .option('keys', {alias: 'k', type: 'boolean', description: 'dump only keys'})
        .option('summary', {type: 'boolean', description: 'show decoded keys, summarising h3 data keys per accumulator'})
        .option('scan', {type: 'boolean', description: 'scan global db and list stations per layer & accumulator'})
        .help()
        .alias('help', 'h').argv;

    // --station / -s takes precedence over --db / -d
    const dbName = args.station ?? args.db;

    // Scan mode: read global db and extract station lists per layer/accumulator
    if (args.scan) {
        await scanStations();
        return;
    }

    // What file
    let dbPath = DB_PATH;
    if (dbName && dbName != 'global') {
        dbPath += '/stations/' + dbName;
    } else {
        dbPath += 'global';
    }

    let db: ClassicLevel<string, Uint8Array> | null = null;

    try {
        db = new ClassicLevel<string, Uint8Array>(dbPath, {valueEncoding: 'view', createIfMissing: false});
        await db.open();
    } catch (e) {
        console.log(args.db, 'error', e);
        //        console.error(e);
        return;
    }

    if (!db) {
        return;
    }

    console.log('---', dbPath, '---');

    if (args.keys) {
        for await (const key of db.keys()) {
            console.log(key);
        }
        await db.close();
        return;
    }

    if (args.summary) {
        let currentCount = 0;
        let currentAcc = '';

        const flush = () => {
            if (currentCount > 0) {
                console.log(`${currentAcc}    ${currentCount} h3 keys`);
            }
        };

        for await (const [key, value] of db.iterator()) {
            const hr = new CoverageHeader(key);
            const isLegacy = hr.layer === 'combined' && !key.startsWith('c/');
            const layerLabel = isLegacy ? 'LEGACY' : hr.layer;
            const acc = `${layerLabel} ${hr.typeName} [${hr.accumulator}]`;
            if (hr.isMeta) {
                flush();
                currentCount = 0;
                currentAcc = '';
                const meta = JSON.parse(String(value));
                const file = meta?.accumulators?.[hr.typeName]?.file ?? '';
                const start = meta?.startUtc ?? '';
                console.log(`${acc}  META  ${file}  started=${start}  key=${key}`);
            } else {
                if (acc !== currentAcc) {
                    flush();
                    currentCount = 0;
                    currentAcc = acc;
                }
                currentCount++;
            }
        }
        flush();
        await db.close();
        return;
    }

    let n = db.iterator();
    let accumulators: Record<string, any> = {};
    let x = n.next();
    let y: Awaited<typeof x> = undefined;
    while ((y = await x)) {
        const [key, value] = y;
        let hr = new CoverageHeader(key);

        if (!args.match || hr.dbKey().match(args.match)) {
            if (hr.isMeta) {
                accumulators[hr.accumulator] = {hr: hr, meta: JSON.parse(String(value)), count: 0, size: 0};
                console.log(hr.dbKey(), String(value));
                if (args.size) {
                    const r = await db.approximateSize(CoverageHeader.getAccumulatorBegin(hr.type, hr.bucket, false, hr.layer), CoverageHeader.getAccumulatorEnd(hr.type, hr.bucket, hr.layer));
                    accumulators[hr.accumulator].size = r;
                }
            } else {
                if (accumulators[hr.accumulator]) {
                    accumulators[hr.accumulator].count++;
                } else {
                    accumulators[hr.accumulator] = {hr: hr, count: 1, size: 0};
                }

                if (accumulators[hr.accumulator].count == 1 && args.size) {
                    const r = await db.approximateSize(CoverageHeader.getAccumulatorBegin(hr.type, hr.bucket, false, hr.layer), CoverageHeader.getAccumulatorEnd(hr.type, hr.bucket, hr.layer));
                    accumulators[hr.accumulator].size = r;
                }
                if (args.all) {
                    console.log(hr.dbKey(), JSON.stringify(new CoverageRecord(value).toObject()));
                } else if (args.count) {
                } else {
                    n.seek(CoverageHeader.getAccumulatorEnd(hr.type, hr.bucket, hr.layer));
                }
            }
        }
        x = n.next();

        //        if ((y = await x)) {
        //            const [key] = y;
        //          let hr = new CoverageHeader(key);
        //    }
    }
    for (const a in accumulators) {
        console.log(`${accumulators[a].hr.layer}/${accumulators[a].hr.typeName} [${a}]: ${accumulators[a].count} records, ~ ${accumulators[a].size} bytes`);
        console.log('  ' + JSON.stringify(accumulators[a].meta));
    }

    if (Object.keys(accumulators).length < 3) {
        //    console.log(args.db)
    }
}

async function scanStations() {
    // Load station ID → name mapping from status database
    const idToName = new Map<number, string>();
    try {
        const statusDb = new ClassicLevel<string, any>(DB_PATH + 'status', {valueEncoding: 'json', createIfMissing: false});
        await statusDb.open();
        for await (const [name, details] of statusDb.iterator()) {
            if (details?.id !== undefined) {
                idToName.set(details.id, name);
            }
        }
        await statusDb.close();
        console.log(`Loaded ${idToName.size} station name mappings from status db`);
    } catch (e) {
        console.log('Warning: could not open status db for station name resolution', e);
    }

    // Open global database
    const globalPath = DB_PATH + 'global';
    let db: ClassicLevel<string, Uint8Array>;
    try {
        db = new ClassicLevel<string, Uint8Array>(globalPath, {valueEncoding: 'view', createIfMissing: false});
        await db.open();
    } catch (e) {
        console.log('Could not open global database at', globalPath, e);
        return;
    }

    // layer -> accumulator label -> set of station names/ids
    const index: Record<string, Record<string, Set<string>>> = {};
    let recordCount = 0;

    for await (const [key, value] of db.iterator()) {
        const hr = new CoverageHeader(key);
        if (hr.isMeta) continue;

        const isLegacy = hr.layer === 'combined' && !key.startsWith('c/');
        const layerLabel = isLegacy ? 'LEGACY' : hr.layer;
        const accLabel = `${hr.typeName} [${hr.accumulator}]`;

        // Extract station IDs from the global CoverageRecord's nested station list
        const cr = new CoverageRecord(value);
        const obj = cr.toObject();
        const stationList = obj['stations'] as CoverageRecordOut[] | undefined;

        if (stationList) {
            if (!index[layerLabel]) index[layerLabel] = {};
            if (!index[layerLabel][accLabel]) index[layerLabel][accLabel] = new Set();

            for (const s of stationList) {
                const sid = s['StationId'] as number;
                if (sid) {
                    const name = idToName.get(sid);
                    index[layerLabel][accLabel].add(name ?? `#${sid}`);
                }
            }
        }
        recordCount++;
    }

    await db.close();
    console.log(`Scanned ${recordCount} h3 records from global database\n`);

    // Print results grouped by layer
    const layers = Object.keys(index).sort();
    for (const layer of layers) {
        console.log(`=== ${layer} ===`);
        const accs = Object.keys(index[layer]).sort();
        for (const acc of accs) {
            const stations = [...index[layer][acc]].sort();
            console.log(`  ${acc}: ${stations.length} stations`);
            console.log(`    ${stations.join(', ')}`);
        }
        console.log();
    }
}
