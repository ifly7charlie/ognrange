import {existsSync, readFile, mkdirSync, readdirSync} from 'fs';
import {tableFromIPC, RecordBatchReader} from 'apache-arrow/Arrow.node';

import {Utf8, Uint8, Uint16, Uint32, Uint64, makeBuilder, makeTable, RecordBatchWriter} from 'apache-arrow/Arrow.node';

import {createWriteStream} from 'fs';
import {PassThrough} from 'stream';

import zlib from 'zlib';

import {splitLongToh3Index, h3IndexToSplitLong} from 'h3-js';

import lodash from 'lodash';
//import {sortedLastIndex} from 'lodash';

import {DB_PATH, OUTPUT_PATH, UNCOMPRESSED_ARROW_FILES} from '../lib/bin/config.js';
import yargs from 'yargs';

const T_OUTPUT_PATH = OUTPUT_PATH + '../converted/';
const OVERWRITE = true;

const args = yargs(process.argv.slice(2)) //
    .option('station', {alias: 's', type: 'string', default: 'global', description: 'Arrow file'})
    .option('h3', {type: 'string', description: 'H3 coordinate to find'})
    .help()
    .alias('help', 'h').argv;

if (!args.h3) {
    console.log('no h3 specified --h3 flag');
    process.exit(1);
}

async function processAllFiles() {
    // Top level
    const pending = new Map();
    const subdir = args.station;
    const h3SplitLong = h3IndexToSplitLong(args.h3);
    const result = {};

    // One dir for each station
    try {
        const files = readdirSync(T_OUTPUT_PATH + subdir);

        for (const fileName of files) {
            const matched = fileName.match(/day\.([0-9-]+)\.arrow$/);
            if (matched) {
                pending.set(
                    fileName,
                    new Promise((resolve) => {
                        processFile(subdir, fileName, matched[1], h3SplitLong, (row) => {
                            if (row) {
                                result[matched[1]] = row;
                            }
                            resolve(fileName);
                        });
                    })
                );
            }
            // Keep the queue size down so we don't run out of files
            // or memory
            if (pending.size > 0) {
                const done = await Promise.race(pending.values());
                pending.delete(done);
            }
        }
    } catch (e) {
        console.log(e);
    }

    //
    await Promise.allSettled(pending.values());
    console.log(result);
}

processAllFiles();

function processFile(station, fileName, date, [h3lo, h3hi], resolve) {
    // Read file, decompress if needed
    readFile(T_OUTPUT_PATH + station + '/' + fileName, null, (err, arrowFileContents) => {
        if (err) {
            console.log(err);
            resolve();
            return;
        }

        if (fileName.match(/.gz$/)) {
            arrowFileContents = zlib.gunzipSync(arrowFileContents);
        }

        try {
            const table = tableFromIPC([arrowFileContents]);

            //
            const h3hiArray = table.getChild('h3hi').toArray();

            // Find the first h3hi in the file
            const index = lodash.sortedIndexOf(h3hiArray, h3hi);

            // none found then it's not in the file
            if (index == -1) {
                resolve();
                return;
            }

            // We now know the range it could be in
            const lastIndex = lodash.sortedLastIndex(h3hiArray, h3hi);

            //            console.log(fileName, 'index from ', index, lastIndex);

            const h3loArray = table.getChild('h3lo').toArray();

            // All the rows with h3hi
            const subset = h3loArray.subarray(index, lastIndex);
            //            console.table(subset);

            // If one matches
            const subIndex = lodash.sortedIndexOf(subset, h3lo);
            if (subIndex == -1) {
                resolve();
                return;
            }

            const json = table.get(subIndex + index).toJSON();
            //            const h3found = json.h3hi.toString(16) + ',' + json.h3lo.toString(16);
            delete json.h3lo;
            delete json.h3hi;
            resolve(json);
            return;
        } catch (e) {
            console.log(OUTPUT_PATH + station + '/' + fileName, e);
        }
        resolve();
        return;
    });
}
