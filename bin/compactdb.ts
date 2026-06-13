import dotenv from 'dotenv';
dotenv.config({path: '.env.local', override: true});

import {ClassicLevel} from 'classic-level';

import {DB_PATH} from '../lib/common/config';

import yargs from 'yargs';

main().then(() => 'exiting');

//
// Force a full LevelDB compaction over a station DB (default: global).
//
// LevelDB only drops superseded key versions when a compaction merges them, and
// the per-cycle compact_range in the rollup is bounded, so over time the global
// DB accumulates ~one obsolete version per key per cycle. This collapses that:
// a compactRange across the entire keyspace rewrites every level, drops every
// superseded version, and unlinks the now-obsolete SST files. Expect a large DB
// (e.g. ~100GB of mostly-garbage) to shrink to roughly its live size.
//
// IMPORTANT: opens the DB read-write and takes the LevelDB LOCK. The aprs-server
// must NOT be holding this DB (stop it, or run against a copied snapshot dir).
async function main() {
    const args = await yargs(process.argv.slice(2)) //
        .option('db', {alias: 'd', type: 'string', default: 'global', description: 'Database to compact'})
        .option('station', {alias: 's', type: 'string', description: 'Station database to compact (same as -d <name>)'})
        .help()
        .alias('help', 'h').argv;

    const dbName = args.station ?? args.db;

    let dbPath = DB_PATH;
    if (dbName && dbName != 'global') {
        dbPath += '/stations/' + dbName;
    } else {
        dbPath += 'global';
    }

    // maxFileSize matches the Rust side's DB_MAX_FILE_SIZE_MB default (16MB) so the
    // rebuilt DB lands in a few hundred large files rather than thousands of 2MB
    // ones; the larger write buffer / open-file budget just speed the rebuild.
    const MB = 1024 * 1024;
    let db: ClassicLevel<string, Uint8Array>;
    try {
        db = new ClassicLevel<string, Uint8Array>(dbPath, {
            valueEncoding: 'view',
            keyEncoding: 'utf8',
            createIfMissing: false,
            maxFileSize: 16 * MB,
            writeBufferSize: 64 * MB,
            maxOpenFiles: 4000,
        });
        await db.open();
    } catch (e) {
        console.error(`Failed to open ${dbPath}:`, e);
        process.exitCode = 1;
        return;
    }

    console.log('---', dbPath, '---');
    console.log('\n=== BEFORE ===');
    console.log(db.getProperty('leveldb.stats'));

    // Find the actual key bounds so compactRange covers the whole keyspace.
    let first: string | undefined;
    let last: string | undefined;
    for await (const k of db.keys({limit: 1})) first = k;
    for await (const k of db.keys({limit: 1, reverse: true})) last = k;

    if (first === undefined || last === undefined) {
        console.log('Database is empty - nothing to compact.');
        await db.close();
        return;
    }

    console.log(`\nCompacting full range [${first} .. ${last}] - this rewrites every level and may take a while...`);
    const t0 = Date.now();
    // compactRange's end bound is exclusive, so append a high byte to include the last key.
    await db.compactRange(first, last + '\xff');
    const secs = ((Date.now() - t0) / 1000).toFixed(1);

    console.log(`\nCompaction finished in ${secs}s`);
    console.log('\n=== AFTER ===');
    console.log(db.getProperty('leveldb.stats'));

    await db.close();
}
