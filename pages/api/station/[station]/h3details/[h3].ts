import {h3IndexToSplitLong} from 'h3-js';

import {searchMatchingArrowFiles, mergeRows, RowResult} from '../../../../../lib/api/searcharrow';

import {MAXIMUM_GRAPH_AGE_MSEC} from '../../../../../lib/common/config';

import {prefixWithZeros} from '../../../../../lib/common/prefixwithzeros';

import {H3DetailsOutputStructure} from '../../../../../lib/api/types';

import {ignoreStation} from '../../../../../lib/common/ignorestation';

function buildEntry(json: RowResult, date: string): H3DetailsOutputStructure {
    if (json.avgGap) {
        const output: H3DetailsOutputStructure = {
            date,
            avgGap: json.avgGap >> 2,
            maxSig: json.maxSig / 4,
            avgSig: json.avgSig / 4,
            minAltSig: json.minAltSig / 4,
            minAgl: json.minAgl,
            count: json.count
        };
        if (json.expectedGap) output.expectedGap = json.expectedGap >> 2;
        return output;
    }
    return {date} as H3DetailsOutputStructure;
}

export default async function getH3Details(req, res) {
    // Top level
    const subdir: string = req.query.station;
    const selectedFile: string = req.query.file;
    const lockedH3 = parseInt(req.query.lockedH3 || '0');
    const h3SplitLong = h3IndexToSplitLong(req.query.h3);
    const layers = (req.query.layers as string || 'combined').split(',').map((s) => s.trim());
    const now = new Date();

    if (!h3SplitLong) {
        res.status(200).json({layers: {}});
        return;
    }

    if (subdir !== 'global' && ignoreStation(subdir)) {
        res.status(404).json({error: 'invalid station name'});
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
    console.log(now.toISOString(), 'h3details', subdir, selectedFile, fileDateMatch, oldest?.toISOString(), req.query.h3, h3SplitLong, lockedH3);

    const rowsByLayer: Record<string, Record<string, RowResult[]>> = {};

    await searchMatchingArrowFiles(
        subdir,
        fileDateMatch,
        h3SplitLong,
        oldest,
        (row, date, layer) => {
            rowsByLayer[layer] ??= {};
            (rowsByLayer[layer][date] ??= []).push(row);
        },
        layers
    );

    // Build per-layer result arrays
    const resultByLayer: Record<string, H3DetailsOutputStructure[]> = {};
    for (const [layer, dateMap] of Object.entries(rowsByLayer)) {
        resultByLayer[layer] = Object.entries(dateMap)
            .map(([date, rows]) => buildEntry((mergeRows(rows) ?? rows[0]) as RowResult, date))
            .sort((a, b) => a.date.localeCompare(b.date));
    }

    // Build 'all' whenever multiple layers were requested (even if only one has data)
    const layerKeys = Object.keys(resultByLayer);
    if (layers.length > 1) {
        const allDateMap: Record<string, RowResult[]> = {};
        for (const dateMap of Object.values(rowsByLayer)) {
            for (const [date, rows] of Object.entries(dateMap)) {
                (allDateMap[date] ??= []).push(...rows);
            }
        }
        resultByLayer['all'] = Object.entries(allDateMap)
            .map(([date, rows]) => buildEntry((mergeRows(rows) ?? rows[0]) as RowResult, date))
            .sort((a, b) => a.date.localeCompare(b.date));
    }

    res.status(200).json({layers: resultByLayer});
    console.log('<-', Date.now() - now.getTime(), 'msec', layerKeys.join(','));
}
