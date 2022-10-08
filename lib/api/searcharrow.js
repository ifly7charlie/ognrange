import lodash from 'lodash';

import {readFile, readdirSync, readFileSync, statSync} from 'fs';
import {tableFromIPC} from 'apache-arrow/Arrow.node';

import zlib from 'zlib';

import {MAX_ARROW_FILES} from '../bin/config.js';

import LRU from 'lru-cache';
const options = {max: MAX_ARROW_FILES, updateAgeOnGet: true, allowStale: true, ttl: 3 * 3600 * 1000},
    cache = new LRU(options);

const dateFormat = new Intl.DateTimeFormat(['en-US'], {month: 'short', day: 'numeric', timeZone: 'UTC'});

//
// Search a single file
export async function searchArrowFileInline(fileName, h3SplitLong) {
    return await new Promise((resolve) => {
        searchArrowFile(fileName, h3SplitLong, resolve);
    });
}

export function searchArrowFile(fileName, h3SplitLong, resolve) {
    let table = cache.get(fileName);

    // If the table is loaded then it's super quick to just search and return
    if (table) {
        searchTableForH3(fileName, table, h3SplitLong, resolve);
        return;
    }

    // Otherwise we need to read it - cache the table itself
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
            table = tableFromIPC([arrowFileContents]);
        } catch (e) {
            console.log(fileName, 'invalid arrow table', e);
        }
        cache.set(fileName, table);

        searchTableForH3(fileName, table, h3SplitLong, resolve);
    });
}

// Scan directory for files
export async function searchMatchingArrowFiles(OUTPUT_PATH, station, fileDateMatch, h3SplitLong, oldest, combine) {
    const pending = new Map();
    try {
        const files = readdirSync(OUTPUT_PATH + station)
            .filter((x) => x.match(fileDateMatch) && x.match(/day\.([0-9-]+)\.arrow$/))
            .sort();

        for (const fileName of files) {
            const matched = fileName.match(/day\.([0-9-]+)\.arrow$/);
            if (matched) {
                const fileDate = new Date(matched[1]);
                if (oldest && fileDate < oldest) {
                    continue;
                }

                pending.set(
                    fileName,
                    new Promise((resolve) => {
                        searchArrowFile(`${OUTPUT_PATH}${station}/${fileName}`, h3SplitLong, (row) => {
                            if (row) {
                                combine(row, dateFormat.format(fileDate).replace(' ', '-'));
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
    await Promise.allSettled(pending.values());
}

function searchTableForH3(fileName, table, [h3lo, h3hi], resolve) {
    //
    try {
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

        resolve(table.get(subIndex + index).toJSON());
        return;
    } catch (e) {
        console.log(fileName, e);
    }
    resolve();
    return;
}

// Used to keep temporary copy
let arrowStationTable = null;
let arrowStationFileMTime = null;

export function searchStationArrowFile(fileName, id) {
    // Read file, decompress if needed
    try {
        // Check if file changed
        const fileStats = statSync(fileName);
        if (arrowStationFileMTime != fileStats.mtime) {
            arrowStationTable = null;
            arrowStationFileMTime = fileStats.mtime;
        }

        // Do we need to load it
        if (!arrowStationTable) {
            let arrowFileContents = readFileSync(fileName);

            if (fileName.match(/.gz$/)) {
                arrowFileContents = zlib.gunzipSync(arrowFileContents);
            }

            arrowStationTable = tableFromIPC([arrowFileContents]);
        }

        //
        const idArray = arrowStationTable.getChild('id')?.toArray();
        if (!idArray) {
            console.log(`file ${fileName} is not in the correct format`);
            return null;
        }

        // Find the first h3hi in the file
        const index = lodash.sortedIndexOf(idArray, id);

        // none found then it's not in the file
        if (index == -1) {
            return null;
        }

        return arrowStationTable.get(index).toJSON();
    } catch (e) {
        console.log(fileName, e);
    }
    return null;
}
