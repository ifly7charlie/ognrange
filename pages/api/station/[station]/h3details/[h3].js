import {readFile, readdirSync} from 'fs';
import {tableFromIPC, RecordBatchReader} from 'apache-arrow/Arrow.node';

import zlib from 'zlib';

import {splitLongToh3Index, h3IndexToSplitLong} from 'h3-js';

import lodash from 'lodash';

import {DB_PATH, OUTPUT_PATH, UNCOMPRESSED_ARROW_FILES} from '../../../../../lib/bin/config.js';

import {prefixWithZeros} from '../../../../../lib/bin/prefixwithzeros.js';

export default async function getH3Details(req, res) {
    // Top level
    const pending = new Map();
    const subdir = req.query.station;
    const selectedFile = req.query.file;
    const h3SplitLong = h3IndexToSplitLong(req.query.h3);
    const result = [];
    const now = new Date();
    const dateFormat = new Intl.DateTimeFormat(['en-US'], {month: 'short', day: 'numeric', timeZone: 'UTC'});

    if (!h3SplitLong) {
        res.status(200).json([]);
        return;
    }

    // Get a Year/Month component from the file
    let fileDateMatch = selectedFile?.match(/([0-9]{4}-[0-9]{2})(|-[0-9]{2})$/)?.[1];
    if (!fileDateMatch) {
        if (!selectedFile || selectedFile == 'undefined' || selectedFile == 'year') {
            fileDateMatch = now.getUTCFullYear();
        } else {
            fileDateMatch = `${now.getUTCFullYear()}-${prefixWithZeros(2, String(now.getUTCMonth()))}`;
        }
    }
    console.log(selectedFile, fileDateMatch, req.query.h3, h3SplitLong);

    // One dir for each station
    try {
        const files = readdirSync(OUTPUT_PATH + subdir)
            .filter((x) => x.match(fileDateMatch) && x.match(/day\.([0-9-]+)\.arrow$/))
            .sort();

        for (const fileName of files) {
            const matched = fileName.match(/day\.([0-9-]+)\.arrow$/);
            if (matched) {
                const fileDate = new Date(matched[1]);

                pending.set(
                    fileName,
                    new Promise((resolve) => {
                        processFile(`${OUTPUT_PATH}${subdir}/${fileName}`, h3SplitLong, (row) => {
                            if (row) {
                                row.date = dateFormat.format(fileDate).replace(' ', '-');
                                result.push(row);
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
    res.status(200).json(result);
}

function processFile(fileName, [h3lo, h3hi], resolve) {
    // Read file, decompress if needed
    readFile(fileName, null, (err, arrowFileContents) => {
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
            const h3hiArray = table.getChild('h3hi')?.toArray();
            if (!h3hiArray) {
                console.log(`file ${fileName} is not in the correct format`);
                resolve();
                return;
            }

            // Find the first h3hi in the file
            const index = lodash.sortedIndexOf(h3hiArray, h3hi);

            // none found then it's not in the file
            if (index == -1) {
                resolve({});
                return;
            }

            // We now know the range it could be in
            const lastIndex = lodash.sortedLastIndex(h3hiArray, h3hi);

            const h3loArray = table.getChild('h3lo').toArray();

            // All the rows with h3hi
            const subset = h3loArray.subarray(index, lastIndex);

            // If one matches
            const subIndex = lodash.sortedIndexOf(subset, h3lo);
            if (subIndex == -1) {
                resolve({});
                return;
            }

            const json = table.get(subIndex + index).toJSON();
            const output = {
                avgGap: json.avgGap >> 2,
                maxSig: json.maxSig / 4,
                avgSig: json.avgSig / 4,
                minAltSig: json.minAltSig / 4,
                minAgl: json.minAgl,
                count: json.count
            };

            if (json.expectedGap) {
                output.expectedGap = json.expectedGap >> 2;
            }
            resolve(output);
            return;
        } catch (e) {
            console.log(fileName, e);
        }
        resolve();
        return;
    });
}
