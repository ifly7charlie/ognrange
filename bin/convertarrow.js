import {existsSync, readFileSync, mkdirSync, readdirSync} from 'fs';
import {tableFromIPC, RecordBatchReader} from 'apache-arrow/Arrow.node';

import {Utf8, Uint8, Uint16, Uint32, Uint64, makeBuilder, makeTable, RecordBatchWriter} from 'apache-arrow/Arrow.node';

import {createWriteStream} from 'fs';
import {PassThrough} from 'stream';

import zlib from 'zlib';

import {DB_PATH, OUTPUT_PATH, UNCOMPRESSED_ARROW_FILES} from '../lib/bin/config.js';
import yargs from 'yargs';

const NEW_PATH = OUTPUT_PATH + '../converted/';

// Top level
readdirSync(OUTPUT_PATH).forEach(function (subdir) {
    // One dir for each station
    console.log(subdir + ':');
    try {
        readdirSync(OUTPUT_PATH + subdir).forEach(function (file) {
            let fileName = subdir + '/' + file;
            if (fileName.match(/[0-9].arrow$/)) {
                try {
                    processFile(subdir, fileName);
                } catch (e) {
                    console.log('\t*** failed, ', String(e).split('\n')?.[0]);
                }
            }
        });
    } catch (e) {}
});

function processFile(station, fileName) {
    const outputFileName = NEW_PATH + fileName;
    const arrowFile = readFileSync(OUTPUT_PATH + fileName);

    mkdirSync(NEW_PATH + station, {recursive: true});

    if (existsSync(outputFileName)) {
        console.log('\t' + fileName + '->' + outputFileName + ' * skipping as it exists');
        return;
    }

    const table = tableFromIPC([arrowFile]);

    if (!table.getChild('h3')) {
        return;
    }

    const h3lo = makeBuilder({type: new Uint32()});
    const h3hi = makeBuilder({type: new Uint32()});
    const numStations = makeBuilder({type: new Uint8()});

    // Accumulate the h3s into two structures
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
        for (const station of table.getChild('stations')) {
            numStations.append(Math.min(station?.split(',')?.length, 255));
        }
    }

    let arrow = {
        h3lo: h3lo.finish().toVector(),
        h3hi: h3hi.finish().toVector()
    };
    if (globalType) {
        arrow.numStations = numStations.finish().toVector();
    }
    const tableUpdates = makeTable(arrow);
    {
        const resultant = table //
            .select(['minAgl', 'minAlt', 'minAltSig', 'maxSig', 'avgSig', 'avgCrc', 'count', 'avgGap', 'stations', 'expectedGap'])
            .assign(tableUpdates);

        if (UNCOMPRESSED_ARROW_FILES) {
            const pt = new PassThrough({objectMode: true});
            const result = pt //
                .pipe(RecordBatchWriter.throughNode())
                .pipe(createWriteStream(outputFileName));
            pt.write(resultant);
            pt.end();
        }
        {
            const pt = new PassThrough({objectMode: true});
            const result = pt
                .pipe(RecordBatchWriter.throughNode())
                .pipe(zlib.createGzip())
                .pipe(createWriteStream(outputFileName + '.gz'));
            pt.write(resultant);
            pt.end();
        }
    }
    console.log('\t' + fileName + '->' + outputFileName + ',bytes:' + arrowFile.length);
}
