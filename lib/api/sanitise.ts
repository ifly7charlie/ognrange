//
// Query parameter validation shared by every handler in pages/api
//
// Station names and period tokens are interpolated into filesystem paths, so
// anything that could escape the output directory is rejected here rather
// than being checked (or not) in each route. Which stations exist is decided
// by aprs-server (aprs-server/src/ignore_list.rs) - an ignored station simply
// has no output directory, so the API doesn't need its own copy of that list
//

import type {NextApiRequest, NextApiResponse} from 'next';
import {isValidCell} from 'h3-js';

import {ALL_LAYER_NAMES} from '../common/layers';

// Same character set aprs-server accepts for a station name
const stationPattern = /^[A-Za-z0-9_-]+$/;

// Period/file tokens: dot-separated segments of [A-Za-z0-9-], e.g. "year", "2026-03-12",
// "day.2026-03-12.flarm", "month.2026-03". Structurally excludes path separators and ".."
const periodPattern = /^[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*$/;

export interface ApiQuery {
    station: string; // '' when not supplied
    h3: string; // '' when not supplied, otherwise a valid H3 cell
    file: string;
    dateStart: string;
    dateEnd: string;
    layers: string[]; // known layer names only, ['combined'] when not supplied
    locked: boolean;
}

export interface RequiredParams {
    station?: boolean;
    h3?: boolean;
}

type RawQuery = Record<string, string | string[] | undefined>;

export function parseQuery(raw: RawQuery, required: RequiredParams = {}): {query: ApiQuery; error?: never} | {query?: never; error: string} {
    // Repeated parameters arrive as arrays - only a single string is acceptable
    const single = (name: string): string | null => {
        const value = raw?.[name];
        if (value === undefined) return '';
        return typeof value === 'string' ? value : null;
    };

    const station = single('station');
    if (station === null || (station && !stationPattern.test(station)) || (required.station && !station)) {
        return {error: 'invalid station name'};
    }

    const h3 = single('h3');
    if (h3 === null || (h3 && !isValidCell(h3)) || (required.h3 && !h3)) {
        return {error: 'invalid h3 cell'};
    }

    const periods: Record<'file' | 'dateStart' | 'dateEnd', string> = {file: '', dateStart: '', dateEnd: ''};
    for (const name of Object.keys(periods) as (keyof typeof periods)[]) {
        const value = single(name);
        if (value === null || (value && !periodPattern.test(value))) {
            return {error: `invalid ${name} parameter`};
        }
        periods[name] = value;
    }

    const layersParam = single('layers');
    if (layersParam === null) {
        return {error: 'invalid layers parameter'};
    }
    const layers = (layersParam || 'combined')
        .split(',')
        .map((s) => s.trim())
        .filter((l) => ALL_LAYER_NAMES.has(l));

    const lockedParam = single('lockedH3');
    const locked = !!parseInt(lockedParam || '0');

    return {query: {station, h3, ...periods, layers, locked}};
}

/** Parse req.query for a handler, sending a 400 and returning null if it is invalid */
export function sanitiseQuery(req: NextApiRequest, res: NextApiResponse, required: RequiredParams = {}): ApiQuery | null {
    const result = parseQuery(req.query, required);
    if (!result.query) {
        res.status(400).json({error: result.error});
        return null;
    }
    return result.query;
}
