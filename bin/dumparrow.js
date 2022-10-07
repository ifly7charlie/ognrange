import {readFileSync} from 'fs';
import {tableFromIPC, RecordBatchReader} from 'apache-arrow/Arrow.node';

import {DB_PATH, OUTPUT_PATH} from '../lib/bin/config.js';
import yargs from 'yargs';

const args = yargs(process.argv.slice(2)) //
    .option('station', {alias: 's', type: 'string', default: 'global', description: 'Arrow file'})
    .option('file', {alias: 'f', type: 'string', default: 'year.arrow', description: 'Arrow file'})
    .help()
    .alias('help', 'h').argv;

console.log(OUTPUT_PATH, {...args});
import {open} from 'node:fs/promises';
const fd = await open(OUTPUT_PATH + args.station + '/' + args.station + '.' + args.file);

const reader = await RecordBatchReader.from(fd.createReadStream());

for await (const batch of reader) {
    console.log(batch.data.type);
    let c = 0;
    let d = 0;

    let lo = 0;
    let hi = 0;
    for (const columns of batch) {
        //	console.log(
        let out = '';
        const json = columns.toJSON();

        json.h3 = json.h3hi.toString(16) + ',' + json.h3lo.toString(16);
        delete json.h3lo;
        delete json.h3hi;
        console.log(JSON.stringify(json));
    }
}
