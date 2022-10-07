import {existsSync, readFile, mkdirSync, readdirSync} from 'fs';
import {tableFromIPC, RecordBatchReader} from 'apache-arrow/Arrow.node';

import {Utf8, Uint8, Uint16, Uint32, Uint64, makeBuilder, makeTable, RecordBatchWriter} from 'apache-arrow/Arrow.node';

import {createWriteStream} from 'fs';
import {PassThrough} from 'stream';

import zlib from 'zlib';

import {DB_PATH, OUTPUT_PATH, UNCOMPRESSED_ARROW_FILES} from '../lib/bin/config.js';
import yargs from 'yargs';

const NEW_PATH = OUTPUT_PATH + '../converted/';
const OVERWRITE = false;

async function processAllFiles() {
    // Top level
    const pending = new Map();
    const subdirs = readdirSync(OUTPUT_PATH);
    for (const subdir of subdirs) {
        // One dir for each station
        try {
            const files = readdirSync(OUTPUT_PATH + subdir);

            for (const file of files) {
                let fileName = subdir + '/' + file;
                if (fileName.match(/[0-9].arrow$/)) {
                    pending.set(
                        fileName,
                        new Promise((resolve) => {
                            processFile(subdir, fileName, () => {
                                resolve(fileName);
                            });
                        })
                    );
                }
                // Keep the queue size down so we don't run out of files
                // or memory
                if (pending.size > 100) {
                    const done = await Promise.race(pending.values());
                    pending.delete(done);
                }
            }
        } catch (e) {}
    }
    //
    console.log('All files in process, waiting for completion..');
    await Promise.allSettled(pending.values());
    console.log('done');
}

processAllFiles().then('done');

function processFile(station, fileName, resolve) {
    const outputFileName = NEW_PATH + fileName;

    // Read file, decompress if needed
    readFile(OUTPUT_PATH + fileName, null, (err, arrowFileContents) => {
        if (err) {
            console.log(err);
            resolve();
            return;
        }

        if (fileName.match(/.gz$/)) {
            arrowFileContents = zlib.gunzipSync(arrowFileContents);
        }

        mkdirSync(NEW_PATH + station, {recursive: true});

        if (existsSync(outputFileName) && !OVERWRITE) {
            //        console.log(fileName + '->' + outputFileName + ' * skipping as it exists');
            resolve();
            return;
        }

        const table = tableFromIPC([arrowFileContents]);

        if (!table.getChild('h3')) {
            resolve();
            return;
        }

        let tableUpdates = null;
        let numRows = 0;

        try {
            const h3lo = makeBuilder({type: new Uint32()});
            const h3hi = makeBuilder({type: new Uint32()});
            const numStations = makeBuilder({type: new Uint8()});

            const h3column = table.getChild('h3');

            if (typeof h3column.get(0) == 'bigint') {
                // initially we had a bigint but it came through on the browser as two 32bits
                // so changed it to be what it appeared to be
                for (const h3part of h3column) {
                    h3lo.append(Number(h3part & BigInt(0xffffffff)));
                    h3hi.append(Number((h3part >> 32n) & BigInt(0xffffffff)));
                    numRows++;
                }
            } else {
                // Accumulate the h3s into two structures - this is tidier
                let currentRow = 0;
                for (const h3part of h3column) {
                    if (currentRow % 2 == 0) {
                        h3lo.append(h3part);
                    } else {
                        h3hi.append(h3part);
                    }
                    currentRow++;
                }
                numRows = currentRow / 2;
            }

            let globalType = !!table.getChild('stations');

            if (globalType) {
                for (const station of table.getChild('stations')) {
                    numStations.append(Math.min(station?.split(',')?.length, 255));
                }
            }

            let arrow = {
                h3lo: h3lo.finish().toVector(),
                h3hi: h3hi.finish().toVector(),
                minAgl: table.getChild('minAgl').slice(0, numRows),
                minAlt: table.getChild('minAlt').slice(0, numRows),
                minAltSig: table.getChild('minAltSig').slice(0, numRows),
                maxSig: table.getChild('maxSig').slice(0, numRows),
                avgSig: table.getChild('avgSig').slice(0, numRows),
                avgCrc: table.getChild('avgCrc').slice(0, numRows),
                count: table.getChild('count').slice(0, numRows),
                avgGap: table.getChild('avgGap').slice(0, numRows)
            };
            if (globalType) {
                arrow.numStations = numStations.finish().toVector();
                arrow.stations = table.getChild('stations').slice(0, numRows);
                arrow.expectedGap = table.getChild('expectedGap').slice(0, numRows);
            }

            // Convert into output file
            tableUpdates = makeTable(arrow);
            //
        } catch (e) {
            console.log('❌', fileName + '->' + outputFileName + ' * error with data', e);
            resolve();
            return;
        }
        {
            //            console.log('minAgl:', tableUpdates.getChild('minAgl').length, table.getChild('minAgl').length, 'h3:', rowCount / 2);

            if (UNCOMPRESSED_ARROW_FILES) {
                const pt = new PassThrough({objectMode: true});
                const result = pt //
                    .pipe(RecordBatchWriter.throughNode())
                    .pipe(createWriteStream(outputFileName));
                pt.write(tableUpdates);
                pt.end();
            }
            {
                const pt = new PassThrough({objectMode: true, emitClose: true});
                pt.on('close', () => {
                    console.log('✅', fileName + '->' + outputFileName + ',bytes:' + arrowFileContents.length, ', rows: ', numRows);
                    resolve();
                });
                const result = pt
                    .pipe(RecordBatchWriter.throughNode())
                    .pipe(zlib.createGzip())
                    .pipe(createWriteStream(outputFileName + '.gz'));
                pt.write(tableUpdates);

                pt.end();
            }
        }
    });
}
