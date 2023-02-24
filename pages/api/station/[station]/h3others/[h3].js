//
// Use the global DB to find neighbouring stations so we can
// extract information from them!
//

import {readdirSync} from 'fs';

import {splitLongToh3Index, h3IndexToSplitLong, h3ToParent} from 'h3-js';

import {searchArrowFile, searchArrowFileInline, searchStationArrowFile, searchMatchingArrowFiles} from '../../../../../lib/api/searcharrow.js';

import {DB_PATH, OUTPUT_PATH, UNCOMPRESSED_ARROW_FILES, H3_GLOBAL_CELL_LEVEL} from '../../../../../lib/bin/config.js';

import {prefixWithZeros} from '../../../../../lib/bin/prefixwithzeros.js';

import _map from 'lodash.map';
import _reduce from 'lodash.reduce';
import _sortBy from 'lodash.sortby';

export default async function getH3Details(req, res) {
    // Top level
    const subdir = req.query.station;
    const selectedFile = req.query.file;
    const lockedH3 = parseInt(req.query.lockedH3 || '0');
    const h3SplitLong = h3IndexToSplitLong(req.query.h3);
    const now = new Date();
    const dateFormat = new Intl.DateTimeFormat(['en-US'], {month: 'short', day: 'numeric', timeZone: 'UTC'});

    if (!h3SplitLong) {
        res.status(200).json([]);
        return;
    }

    // We only work on a specific station
    if (subdir == 'global') {
        res.status(200).json([]);
        return;
    }

    // Find in the global DB - this is so we can get a c
    const parentH3 = h3ToParent(req.query.h3, H3_GLOBAL_CELL_LEVEL);
    const parentH3SplitLong = h3IndexToSplitLong(parentH3);

    // Get a Year/Month component from the file
    let fileDateMatches = selectedFile?.match(/([0-9]{4})(-[0-9]{2})*(-[0-9]{2})*$/);
    let fileDateMatch = fileDateMatches?.[1] + fileDateMatches?.[2] || '';
    let globalFileName = selectedFile;
    let oldest = 0;
    if (!fileDateMatch) {
        if (!selectedFile || selectedFile == 'undefined' || selectedFile == 'year') {
            fileDateMatch = now.getUTCFullYear();
            globalFileName = `year.${fileDateMatch}`;
            oldest = !lockedH3 ? new Date(now - 2 * 30 * 24 * 3600 * 1000) : null;
        } else {
            fileDateMatch = `${now.getUTCFullYear()}-${prefixWithZeros(2, String(now.getUTCMonth() + 1))}`;
        }
    }

    console.log(now.toISOString(), ' h3others', selectedFile, fileDateMatch, fileDateMatches, req.query.h3, h3SplitLong);

    // Find the enclosing global record
    const globalRecord = await searchArrowFileInline(OUTPUT_PATH + 'global/global.' + globalFileName + '.arrow', parentH3SplitLong);

    if (!globalRecord || !globalRecord.stations) {
        console.log('no record in global file');
        res.String(200).json([]);
        return;
    }

    const result = {};

    // Now we will go through the list of stations and get the stations that could match
    for (const station of globalRecord.stations.split(',')) {
        const sid = parseInt(station, 36) >> 4;
        // get station name
        const stationName = searchStationArrowFile(OUTPUT_PATH + 'stations.arrow', sid)?.name;

        if (!stationName) {
            console.log('station', sid, ' not found!');
        } else {
            await searchMatchingArrowFiles(OUTPUT_PATH, stationName, fileDateMatch, h3SplitLong, oldest, (row, date) => {
                result[date] = Object.assign(result[date] || {});
                result[date][stationName] = row.count;
            });
        }
    }

    // Sum up how many points
    let total = 0;
    const summed = _reduce(
        result,
        (r, v, k) => {
            return _reduce(
                v,
                (r, v, k) => {
                    r[k] = (r[k] || 0) + (v || 0);
                    total = total + (v || 0);
                    return r;
                },
                r
            );
        },
        {}
    );

    // Sort and find top 5
    const top5 = _sortBy(
        _reduce(
            summed,
            (r, v, k) => {
                r.push({s: k, c: v});
                return r;
            },
            []
        ),
        (v) => -v.c
    ).slice(0, 5);

    const data = _map(result, (v, k) => {
        return _reduce(
            top5,
            (r, top5key) => {
                r.Other -= v[top5key.s];
                r[top5key.s] = v[top5key.s];
                return r;
            },
            {date: k, Other: _reduce(v, (r, count) => r + (count || 0), 0)}
        );
    });

    //    console.log(total);

    // Return the selected top 5 along with the number left over so we can
    // do a proper graph
    res.status(200).json({
        series: [...top5, {s: 'Other', c: _reduce(top5, (r, v) => (r = r - v.c), total)}],
        data
    });
}
