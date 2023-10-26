//
// Find list of available arrow files for a station
//

import {ARROW_PATH, ROLLUP_PERIOD_MINUTES} from '../../../lib/common/config';
import {ignoreStation} from '../../../lib/common/ignorestation';

import {readdirSync} from 'fs';

const fileMatcher = /\.(day|month|year)\.([0-9-]+)\.arrow\.gz$/;

export default async function getH3Details(req, res) {
    const stationName: string = req.query.station;

    if (stationName !== 'global' && ignoreStation(stationName)) {
        res.status(404).text('invalid station name');
        return;
    }

    // This will produce a list of arrays of available arrow files for the
    // station, we need to convert that into
    /*
  "files": {
    "day": {
      "current": "UKDUN2/UKDUN2.day.2023-09-23",
      "all": [
        "UKDUN2/UKDUN2.day.2023-09-23",
      ]
    },
    "month": ...
    "year": ...
  }

*/

    const files = readdirSync(ARROW_PATH + stationName)
        .map((fileName: string) => {
            const parts = fileName.match(fileMatcher);
            if (!parts || parts.length < 2) {
                return null;
            }
            return {fileName, type: parts[1], date: parts[2]};
        })
        .filter((parts) => parts && parts.type && parts.date)
        .sort((a, b) => a.date.localeCompare(b.date))
        .reduce((output, parts) => {
            const pathToUse = `${stationName}/${stationName}.${parts.type}.${parts.date}`;
            output[parts.type] ??= {all: []};
            output[parts.type].all.push(pathToUse);
            output[parts.type].current = pathToUse;
            return output;
        }, {} as any);

    // How long should it be cached - rollup period is good enough
    res.setHeader('Cache-Control', `public, s-maxage=${ROLLUP_PERIOD_MINUTES * 60}, stale-while-revalidate=300`);

    // Return the selected top 5 along with the number left over so we can
    // do a proper graph
    res.status(200).json({files: {day: files.day, month: files.month, year: files.year}});
}
