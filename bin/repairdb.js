import {ClassicLevel} from 'classic-level';

import dotenv from 'dotenv';

import {DB_PATH, OUTPUT_PATH} from '../lib/common/config.js';

import yargs from 'yargs';

main().then('exiting');

//
// Primary configuration loading and start the aprs receiver
async function main() {
    const args = yargs(process.argv.slice(2)) //
        .option('db', {alias: 'd', type: 'string', default: undefined, description: 'Choose Database'})
        .demandOption(['d'])
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

    console.log('---', dbPath, '---');
    try {
        console.log('repair completed', await ClassicLevel.repair(dbPath));
    } catch (e) {
        console.error(e);
    }
}
