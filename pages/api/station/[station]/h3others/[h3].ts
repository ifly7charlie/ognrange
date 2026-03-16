//
// Use the global DB to find neighbouring stations so we can
// extract information from them!
//

import {h3IndexToSplitLong, cellToParent} from 'h3-js';
import {searchArrowFileInline, searchStationArrowFile, searchMatchingArrowFiles} from '../../../../../lib/api/searcharrow';

import {H3_GLOBAL_CELL_LEVEL, MAXIMUM_GRAPH_AGE_MSEC} from '../../../../../lib/common/config';

import {dateBounds} from '../../../../../lib/common/datebounds';

import {map as _map, reduce as _reduce, sortBy as _sortBy} from 'lodash';

function buildSeriesData(result: Record<string, Record<string, number>>) {
    let total = 0;
    const summed = _reduce(
        result,
        (r, v) => {
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
        {} as Record<string, number>
    );

    const top5 = _sortBy(
        _reduce(summed, (r, v, k) => { r.push({s: k, c: v}); return r; }, [] as {s: string; c: number}[]),
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
            {date: k, Other: _reduce(v, (r, count) => r + (count || 0), 0)} as Record<string, any>
        );
    }).sort((a, b) => a.date.localeCompare(b.date));

    return {
        series: [...top5, {s: 'Other', c: _reduce(top5, (r, v) => r - v.c, total)}],
        data
    };
}

export default async function getH3Details(req, res) {
    // Top level
    const selectedFile: string = req.query.file;
    const lockedH3 = parseInt(req.query.lockedH3 || '0');
    const h3SplitLong = h3IndexToSplitLong(req.query.h3);
    const layers = (req.query.layers as string || 'combined').split(',').map((s) => s.trim());
    const now = new Date();

    if (!h3SplitLong) {
        res.status(200).json({layers: {}});
        return;
    }

    // Find in the global DB - this is so we can get a c
    const parentH3 = cellToParent(req.query.h3, H3_GLOBAL_CELL_LEVEL);
    const parentH3SplitLong = h3IndexToSplitLong(parentH3);

    // Resolve date range from dateStart/dateEnd params, falling back to file param
    const dateStartParam = (req.query.dateStart as string) || selectedFile || 'year';
    const dateEndParam = (req.query.dateEnd as string) || selectedFile || 'year';
    const startBounds = dateBounds(dateStartParam);
    const endBounds = dateBounds(dateEndParam);
    const rangeStart = startBounds?.start || `${now.getUTCFullYear()}-01-01`;
    const rangeEnd = endBounds?.end || `${now.getUTCFullYear()}-12-31`;
    let oldest: Date | undefined = undefined;
    if (!req.query.dateStart && !req.query.dateEnd) {
        if (!selectedFile || selectedFile == 'undefined' || selectedFile === 'null' || selectedFile == 'year') {
            oldest = !lockedH3 ? new Date(Number(now) - MAXIMUM_GRAPH_AGE_MSEC) : undefined;
        }
    }

    // Build global file name from rangeStart for the station lookup
    const rangeStartParts = rangeStart.split('-');
    const globalFileName = rangeStartParts.length >= 2 ? `month.${rangeStartParts[0]}-${rangeStartParts[1]}` : `year.${rangeStartParts[0]}`;

    console.log(now.toISOString(), ' h3others', selectedFile, rangeStart, rangeEnd, req.query.h3, h3SplitLong);

    // Find the enclosing global record
    const globalRecord = await searchArrowFileInline('global/global.' + globalFileName + '.arrow.gz', parentH3SplitLong);

    if (!globalRecord || !globalRecord.stations) {
        console.log('no record in global file');
        res.status(200).json({layers: {}});
        return;
    }

    const countsByLayer: Record<string, Record<string, Record<string, number>>> = {};

    // Now we will go through the list of stations and get the stations that could match
    for (const station of globalRecord?.stations?.split(',') || []) {
        const sid = parseInt(station, 36) >> 4;
        // get station name
        const stationName = searchStationArrowFile(sid)?.name;

        if (!stationName) {
            console.log('station', sid, ' not found!');
        } else {
            await searchMatchingArrowFiles(
                stationName,
                rangeStart,
                rangeEnd,
                h3SplitLong,
                oldest,
                (row, date, layer) => {
                    countsByLayer[layer] ??= {};
                    countsByLayer[layer][date] ??= {};
                    countsByLayer[layer][date][stationName] = (countsByLayer[layer][date][stationName] || 0) + row.count;
                },
                layers
            );
        }
    }

    const resultByLayer: Record<string, any> = {};
    for (const [layer, result] of Object.entries(countsByLayer)) {
        resultByLayer[layer] = buildSeriesData(result);
    }

    // Build 'all' whenever multiple layers were requested (even if only one has data)
    const layerKeys = Object.keys(resultByLayer);
    if (layers.length > 1) {
        const allResult: Record<string, Record<string, number>> = {};
        for (const result of Object.values(countsByLayer)) {
            for (const [date, stations] of Object.entries(result)) {
                allResult[date] ??= {};
                for (const [sname, count] of Object.entries(stations)) {
                    allResult[date][sname] = (allResult[date][sname] || 0) + count;
                }
            }
        }
        resultByLayer['all'] = buildSeriesData(allResult);
    }

    res.status(200).json({layers: resultByLayer});
    console.log('<-', Date.now() - now.getTime(), 'msec', layerKeys.join(','), 'rows');
}
