import {sortedIndexOf, sortedLastIndex} from 'lodash';

import {readFile, readdirSync, readFileSync, statSync} from 'fs';
import {tableFromIPC, Table} from 'apache-arrow/Arrow.node';

import {gunzipSync} from 'zlib';

import {MAX_ARROW_FILES, ARROW_PATH, UNCOMPRESSED_ARROW_FILES} from '../common/config';

type ArrowTableType = Table<any>;

import LRU from 'lru-cache';
const options = {max: MAX_ARROW_FILES, updateAgeOnGet: true, allowStale: true, ttl: 3 * 3600 * 1000},
    cache = new LRU<string, ArrowTableType>(options);

const dateFormat = new Intl.DateTimeFormat(['en-US'], {month: 'short', day: 'numeric', timeZone: 'UTC'});

interface RowResult {
    h3lo: number;
    h3hi: number;
    minAgl: number;
    minAlt: number;
    minAltSig: number;
    maxSig: number;
    avgSig: number;
    avgCrc: number;
    count: number;
    avgGap: number;
    stations?: string;
    expectedGap?: number;
    numStations?: number;
}

type RowResultFunction = (data: RowResult | void) => void;

//
// Search a single file
export async function searchArrowFileInline(fileName: string, h3SplitLong: [number, number]): Promise<RowResult | void> {
    return new Promise<RowResult | void>((resolve) => {
        searchArrowFile(fileName, h3SplitLong, resolve);
    });
}

export function searchArrowFile(fileName: string, h3SplitLong: [number, number], resolve: RowResultFunction) {
    //
    let table = cache.get(fileName);

    // If the table is loaded then it's super quick to just search and return
    if (table) {
        searchTableForH3(fileName, table, h3SplitLong, resolve);
        return;
    }

    // Otherwise we need to read it - cache the table itself
    // Read file, decompress if needed
    readFile(ARROW_PATH + fileName, null, (err, arrowFileContents) => {
        if (err) {
            console.log(err);
            resolve();
            return;
        }

        try {
            if (fileName.match(/.gz$/)) {
                arrowFileContents = gunzipSync(arrowFileContents);
            }

            table = tableFromIPC([arrowFileContents]);
            cache.set(fileName, table);
            searchTableForH3(fileName, table, h3SplitLong, resolve);
        } catch (e) {
            console.log(fileName, 'invalid arrow table', e);
            resolve();
        }
    });
}

// Scan directory for files
export async function searchMatchingArrowFiles(station: string, fileDateMatch: string, h3SplitLong: [number, number], oldest: Date | undefined, combine: Function) {
    const pending = new Map<string, Promise<string>>();
    try {
        const files = readdirSync(ARROW_PATH + station)
            .map((fn) => {
                return {date: fn.match(/day\.([0-9-]+)\.arrow\.gz$/)?.[1] || '', fileName: fn};
            })
            .filter((x) => x.date?.substring(0, fileDateMatch.length) == fileDateMatch);
        //            .sort((a, b) => a.fileName.localeCompare(b.fileName));

        for (const file of files) {
            const fileDate = new Date(file.date);
            if (oldest && fileDate < oldest) {
                continue;
            }

            pending.set(
                file.fileName,
                new Promise<string>((resolve) => {
                    searchArrowFile(`${station}/${file.fileName}`, h3SplitLong, (row) => {
                        if (row) {
                            combine(row, dateFormat.format(fileDate).replace(' ', '-'));
                        }
                        resolve(file.fileName);
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
    } catch (e) {
        console.log(e);
    }
    await Promise.allSettled(pending.values());
}

function searchTableForH3(fileName: string, table: ArrowTableType, [h3lo, h3hi]: [number, number], resolve: RowResultFunction) {
    //
    try {
        const h3hiArray = table.getChild('h3hi')?.toArray();
        if (!h3hiArray) {
            console.log(`file ${fileName} is not in the correct format`);
            resolve();
            return;
        }

        // Find the first h3hi in the file
        const index = sortedIndexOf(h3hiArray, h3hi);

        // none found then it's not in the file
        if (index == -1) {
            resolve();
            return;
        }

        // We now know the range it could be in
        const lastIndex = sortedLastIndex(h3hiArray, h3hi);

        const h3loArray = table.getChild('h3lo')?.toArray();

        if (!h3loArray) {
            resolve();
            return;
        }

        // All the rows with h3hi
        const subset = h3loArray.subarray(index, lastIndex);

        // If one matches
        const subIndex = sortedIndexOf(subset, h3lo);
        if (subIndex == -1) {
            resolve();
            return;
        }

        resolve(table.get(subIndex + index)?.toJSON() as RowResult);
        return;
    } catch (e) {
        console.log(fileName, e);
    }
    resolve();
    return;
}

// Used to keep temporary copy - it's a global file so we can cache it like this
// easily enough
let arrowStationTable: Table<any> | null = null;
let arrowStationFileMTime: Date | null = null;

//
// This is an arrow version of stations.json we use it to speed up the
// name resolution from station ID when producing output
export function searchStationArrowFile(id: number): Record<string, any> | null {
    const fileName = ARROW_PATH + 'stations.arrow.gz';

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
                arrowFileContents = gunzipSync(arrowFileContents);
            }

            arrowStationTable = tableFromIPC<any>([arrowFileContents]);
        }

        //
        const idArray = arrowStationTable.getChild('id')?.toArray();
        if (!idArray) {
            console.log(`file ${fileName} is not in the correct format`);
            return null;
        }

        // Find the first h3hi in the file
        const index = sortedIndexOf(idArray, id);

        // none found then it's not in the file
        if (index == -1) {
            return null;
        }

        return arrowStationTable.get(index)?.toJSON() || null;
    } catch (e) {
        console.log(fileName, e);
    }
    return null;
}
