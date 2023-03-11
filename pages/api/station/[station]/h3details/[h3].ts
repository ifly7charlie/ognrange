import {h3IndexToSplitLong} from 'h3-js';

import {searchMatchingArrowFiles} from '/lib/api/searcharrow';

import {MAXIMUM_GRAPH_AGE_MSEC} from '/lib/common/config';

import {prefixWithZeros} from '/lib/common/prefixwithzeros';

import {H3DetailsOutputStructure, H3DetailsOutput} from 'lib/api/types';

export default async function getH3Details(req, res) {
    // Top level
    const subdir = req.query.station;
    const selectedFile = req.query.file;
    const lockedH3 = parseInt(req.query.lockedH3 || '0');
    const h3SplitLong = h3IndexToSplitLong(req.query.h3);
    const result: H3DetailsOutput = [];
    const now = new Date();

    if (!h3SplitLong) {
        res.status(200).json([]);
        return;
    }

    // Get a Year/Month component from the file
    let fileDateMatches = selectedFile?.match(/([0-9]{4})(-[0-9]{2})*(-[0-9]{2})*$/);
    let fileDateMatch = (fileDateMatches?.[1] || '') + (fileDateMatches?.[2] || '');
    let oldest: Date | undefined = undefined;
    if (!fileDateMatch) {
        if (!selectedFile || selectedFile == 'undefined' || selectedFile == 'year') {
            fileDateMatch = now.getUTCFullYear();
            oldest = !lockedH3 ? new Date(Number(now) - MAXIMUM_GRAPH_AGE_MSEC) : undefined;
        } else {
            fileDateMatch = `${now.getUTCFullYear()}-${prefixWithZeros(2, String(now.getUTCMonth() + 1))}`;
        }
    }
    console.log(now.toISOString(), 'h3details', subdir, selectedFile, fileDateMatch, req.query.h3, h3SplitLong, lockedH3);

    // One dir for each station
    await searchMatchingArrowFiles(subdir, fileDateMatch, h3SplitLong, oldest, (json, date) => {
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
    console.log('<-', Date.now() - now.getTime(), 'msec', result.length, 'rows');
}
