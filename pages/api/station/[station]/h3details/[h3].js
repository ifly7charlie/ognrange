import {splitLongToh3Index, h3IndexToSplitLong} from 'h3-js';

import {searchMatchingArrowFiles} from '../../../../../lib/api/searcharrow.js';

import {OUTPUT_PATH} from '../../../../../lib/bin/config.js';

import {prefixWithZeros} from '../../../../../lib/bin/prefixwithzeros.js';

export default async function getH3Details(req, res) {
    // Top level
    const pending = new Map();
    const subdir = req.query.station;
    const selectedFile = req.query.file;
    const lockedH3 = parseInt(req.query.lockedH3 || '0');
    const h3SplitLong = h3IndexToSplitLong(req.query.h3);
    const result = [];
    const now = new Date();
    const dateFormat = new Intl.DateTimeFormat(['en-US'], {month: 'short', day: 'numeric', timeZone: 'UTC'});

    if (!h3SplitLong) {
        res.status(200).json([]);
        return;
    }

    // Get a Year/Month component from the file
    let fileDateMatches = selectedFile?.match(/([0-9]{4})(-[0-9]{2})*(-[0-9]{2})*$/);
    let fileDateMatch = fileDateMatches?.[1] + (fileDateMatches?.[2] || '');
    let oldest = 0;
    if (!fileDateMatch) {
        if (!selectedFile || selectedFile == 'undefined' || selectedFile == 'year') {
            fileDateMatch = now.getUTCFullYear();
            oldest = !lockedH3 ? new Date(now - 2 * 30 * 24 * 3600 * 1000) : 0;
        } else {
            fileDateMatch = `${now.getUTCFullYear()}-${prefixWithZeros(2, String(now.getUTCMonth() + 1))}`;
        }
    }
    console.log(now.toISOString(), ' h3details', selectedFile, fileDateMatch, req.query.h3, h3SplitLong, lockedH3, oldest);

    // One dir for each station
    await searchMatchingArrowFiles(OUTPUT_PATH, subdir, fileDateMatch, h3SplitLong, oldest, (json, date) => {
        if (json.avgGap) {
            const output = {
                date,
                avgGap: json.avgGap >> 2,
                maxSig: json.maxSig / 4,
                avgSig: json.avgSig / 4,
                minAltSig: json.minAltSig / 4,
                minAgl: json.minAgl,
                count: json.count
            };

            if (json.expectedGap) {
                output.expectedGap = json.expectedGap >> 2;
            }
            result.push(output);
        } else {
            result.push({date});
        }
    });

    //
    res.status(200).json(result);
}
