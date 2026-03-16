import {h3IndexToSplitLong} from 'h3-js';

import {searchMatchingArrowFiles, mergeRows, RowResult} from '../../../../../lib/api/searcharrow';

import {MAXIMUM_GRAPH_AGE_MSEC, ROLLUP_PERIOD_MINUTES} from '../../../../../lib/common/config';

import {dateBounds} from '../../../../../lib/common/datebounds';

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
        res.setHeader('Cache-Control', `public, max-age=${ROLLUP_PERIOD_MINUTES * 60}, s-maxage=${ROLLUP_PERIOD_MINUTES * 60}, stale-while-revalidate=300`);
        res.status(200).json({layers: {}});
        return;
    }

    if (subdir !== 'global' && ignoreStation(subdir)) {
        res.setHeader('Cache-Control', `public, max-age=${ROLLUP_PERIOD_MINUTES * 60}, s-maxage=${ROLLUP_PERIOD_MINUTES * 60}, stale-while-revalidate=300`);
        res.status(404).json({error: 'invalid station name'});
        return;
    }

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
    console.log(now.toISOString(), 'h3details', subdir, selectedFile, rangeStart, rangeEnd, oldest?.toISOString(), req.query.h3, h3SplitLong, lockedH3);

    const rowsByLayer: Record<string, Record<string, RowResult[]>> = {};

    await searchMatchingArrowFiles(
        subdir,
        rangeStart,
        rangeEnd,
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

    res.setHeader('Cache-Control', `public, max-age=${ROLLUP_PERIOD_MINUTES * 60}, s-maxage=${ROLLUP_PERIOD_MINUTES * 60}, stale-while-revalidate=300`);
    res.status(200).json({layers: resultByLayer});
    console.log('<-', Date.now() - now.getTime(), 'msec', layerKeys.join(','));
}
