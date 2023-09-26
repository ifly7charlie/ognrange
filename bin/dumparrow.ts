import dotenv from 'dotenv';
dotenv.config({path: '.env.local', override: true});

import {readFileSync} from 'fs';
import {tableFromIPC, RecordBatchStreamReader} from 'apache-arrow/Arrow.node';

import {DB_PATH, OUTPUT_PATH} from '../lib/common/config';
import yargs from 'yargs';
import {open} from 'node:fs/promises';

import {prefixWithZeros} from '../lib/common/prefixwithzeros';
import {Readable, pipeline} from 'node:stream';

import {createGunzip} from 'node:zlib';

async function dump() {
    const args = await yargs(process.argv.slice(2)) //
        .option('stations', {type: 'boolean'})
        .option('station', {alias: 's', type: 'string', default: 'global', description: 'Arrow file'})
        .option('file', {alias: 'f', type: 'string', default: 'year.arrow', description: 'Arrow file'})
        .help()
        .alias('help', 'h').argv;

    console.log(OUTPUT_PATH, {...args});
    const fd = await open(args.stations ? OUTPUT_PATH + 'stations.arrow' : OUTPUT_PATH + args.station + '/' + args.station + '.' + args.file + '.gz');

    const reader = await RecordBatchStreamReader.from(fd.createReadStream().pipe(createGunzip()));

    for await (const batch of reader) {
        let c = 0;
        let d = 0;

        let lo = 0;
        let hi = 0;
        for (const columns of batch) {
            let out = '';
            const json = columns.toJSON();

            json.h3 = prefixWithZeros(7, json.h3hi?.toString(16) || 'null') + prefixWithZeros(8, json.h3lo?.toString(16) || 'null');
            //            delete json.h3lo;
            //            delete json.h3hi;
            console.log(JSON.stringify(json));
        }
    }
}

dump();
