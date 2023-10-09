//
// Use the global DB to find neighbouring stations so we can
// extract information from them!
//

import {h3IndexToSplitLong} from 'h3-js';

import {searchStationArrowFile, searchMatchingArrowFiles} from '../../../../../lib/api/searcharrow';

import {MAXIMUM_GRAPH_AGE_MSEC} from '../../../../../lib/common/config';

import {ignoreStation} from '../../../../../lib/common/ignorestation';

import {prefixWithZeros} from '../../../../../lib/common/prefixwithzeros';

import {map as _map, reduce as _reduce, sortBy as _sortBy} from 'lodash';

export default async function getH3Details(req, res) {
    // Top level
    const stationName: string = req.query.station;
    const selectedFile: string = req.query.file;
    const lockedH3 = parseInt(req.query.lockedH3 || '0');
    const h3SplitLong = h3IndexToSplitLong(req.query.h3);
    const now = new Date();

    if (!h3SplitLong) {
        res.status(200).json([]);
        return;
    }

    // We only work on a global
    if (stationName !== 'global') {
        res.status(200).json([]);
        return;
    }

    // Get a Year/Month component from the file
    let fileDateMatches = selectedFile?.match(/([0-9]{4})(-[0-9]{2})*(-[0-9]{2})*$/);
    let fileDateMatch: string = (fileDateMatches?.[1] || '') + (fileDateMatches?.[2] || '');
    let oldest: Date | undefined = undefined;
    if (!fileDateMatch) {
        if (!selectedFile || selectedFile == 'undefined' || selectedFile === 'null' || selectedFile == 'year') {
            fileDateMatch = '' + now.getUTCFullYear();
            oldest = !lockedH3 ? new Date(Number(now) - MAXIMUM_GRAPH_AGE_MSEC) : undefined;
        } else {
            fileDateMatch = `${now.getUTCFullYear()}-${prefixWithZeros(2, String(now.getUTCMonth() + 1))}`;
        }
    }

    console.log(now.toISOString(), ' h3summary', stationName, selectedFile, fileDateMatch, req.query.h3, h3SplitLong);

    const result: Record<string, Record<string, number>> = {};
    const sids = {};

    await searchMatchingArrowFiles(stationName, fileDateMatch, h3SplitLong, oldest, (row, date) => {
        result[date] ??= {}; //Object.assign(result[date] || {});
        result[date] = _reduce(
            row?.stations?.split(',') || [],
            (acc, x) => {
                const decoded = parseInt(x, 36);
                const percentage = (decoded & 0x0f) * 10;
                if (percentage) {
                    const sid = decoded >> 4;
                    sids[sid] ??= searchStationArrowFile(sid)?.name || 'unknown';
                    const sname = sids[sid];
                    acc[sname] = percentage;
                }
                return acc;
            },
            {}
        );
    });

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
        if (Object.keys(v).length == 0) {
            return {date: k};
        }
        return _reduce(
            top5,
            (r, top5key) => {
                r.Other = r.Other - (v[top5key.s] || 0);
                r[top5key.s] = v[top5key.s] || 0;
                return r;
            },
            {
                date: k,
                Other: 100
            }
        );
    }).sort((a, b) => b.date.localeCompare(a.date));

    // Return the selected top 5 along with the number left over so we can
    // do a proper graph
    res.status(200).json({
        series: [...top5, {s: 'Other', c: _reduce(top5, (r, v) => (r = r - v.c), total)}],
        data
    });

    console.log('<-', Date.now() - now.getTime(), 'msec', data.length);
}
