import {readFileSync} from 'fs';
import {tableFromIPC, RecordBatchReader} from 'apache-arrow/Arrow.node';

import {Utf8, Uint8, Uint16, Uint32, Uint64, makeBuilder, makeTable, RecordBatchWriter} from 'apache-arrow/Arrow.node';

import {createWriteStream} from 'fs';
import {PassThrough} from 'stream';

import zlib from 'zlib';

import {DB_PATH, OUTPUT_PATH} from '../lib/bin/config.js';
import yargs from 'yargs';

const NEW_PATH = OUTPUT_PATH + '../converted/';

const args = yargs(process.argv.slice(2)) //
    .option('station', {alias: 's', type: 'string', default: 'global', description: 'Arrow file'})
    .option('file', {alias: 'f', type: 'string', default: 'year.arrow', description: 'Arrow file'})
    .help()
    .alias('help', 'h').argv;

const arrowFile = readFileSync(OUTPUT_PATH + args.station + '/' + args.station + '.' + args.file);
const table = tableFromIPC([arrowFile]);

console.log(table.schema.fields);

const h3lo = makeBuilder({type: new Uint32()});
const h3hi = makeBuilder({type: new Uint32()});
const numStations = makeBuilder({type: new Uint8()});

// Accumulate the h3s into two structures
console.log('H3...');
let rowCount = 0;
for (const h3part of table.getChild('h3')) {
    if (rowCount % 2 == 0) {
        h3lo.append(h3part);
    } else {
        h3hi.append(h3part);
    }
    rowCount++;
}

let globalType = !!table.getChild('stations');

if (globalType) {
    console.log('Station Count...');
    for (const station of table.getChild('stations')) {
        numStations.append(Math.min(station?.split(',')?.length, 255));
    }
}

console.log('Generating updates');
let arrow = {
    h3lo: h3lo.finish().toVector(),
    h3hi: h3hi.finish().toVector()
};
if (globalType) {
    arrow.numStations = numStations.finish().toVector();
}
const tableUpdates = makeTable(arrow);

console.log('Merging');
{
    const resultant = table //
        .select(['minAgl', 'minAlt', 'minAltSig', 'maxSig', 'avgSig', 'avgCrc', 'count', 'avgGap', 'stations', 'expectedGap'])
        .assign(tableUpdates);

    const fileName = NEW_PATH + args.station + '/' + args.station + '.' + args.file;

    {
        const pt = new PassThrough({objectMode: true});
        const result = pt //
            .pipe(RecordBatchWriter.throughNode())
            .pipe(createWriteStream(fileName));
        pt.write(resultant);
        pt.end();
    }
    {
        const pt = new PassThrough({objectMode: true});
        const result = pt
            .pipe(RecordBatchWriter.throughNode())
            .pipe(zlib.createGzip())
            .pipe(createWriteStream(fileName + '.gz'));
        pt.write(resultant);
        pt.end();
    }
}
