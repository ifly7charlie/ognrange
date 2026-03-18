//
// Find list of available arrow files for a station
//

import {ARROW_PATH, ROLLUP_PERIOD_MINUTES} from '../../../lib/common/config';
import {ignoreStation} from '../../../lib/common/ignorestation';
import {Layer, shouldProduceOutput} from '../../../lib/common/layers';

import {readdirSync} from 'fs';

const layerPattern = Object.values(Layer).join('|');
const fileMatcher = new RegExp(`\\.(day|month|year|yearnz)\\.([0-9-]+[nz]*)(?:\\.(${layerPattern}))?\\.arrow\\.gz$`);

export default async function getH3Details(req, res) {
    const stationName: string = req.query.station;

    if (stationName !== 'global' && ignoreStation(stationName)) {
        res.status(404).text('invalid station name');
        return;
    }

    // Produces: { day: { combined: ["2024-03-18", ...], flarm: [...] }, month: {...}, ... }
    const result = readdirSync(ARROW_PATH + stationName)
        .map((fileName: string) => {
            const parts = fileName.match(fileMatcher);
            if (!parts || parts.length < 2) {
                return null;
            }
            return {fileName, type: parts[1], date: parts[2], layerName: parts[3] || 'combined'};
        })
        .filter((parts) => parts && parts.type && parts.date && shouldProduceOutput((parts.layerName ?? 'combined') as Layer, parts.type))
        .sort((a, b) => a.date.localeCompare(b.date))
        .reduce((output, parts) => {
            const {type, date, layerName} = parts;
            output[type] ??= {};
            output[type][layerName] ??= [];
            output[type][layerName].push(date);
            return output;
        }, {} as any);

    // How long should it be cached - rollup period is good enough
    res.setHeader('Cache-Control', `public, max-age=${ROLLUP_PERIOD_MINUTES * 60}, s-maxage=${ROLLUP_PERIOD_MINUTES * 60}, stale-while-revalidate=300`);

    res.status(200).json(result);
}
