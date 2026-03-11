/// <reference lib="webworker" />

import {ArrowLoader} from '@loaders.gl/arrow';
import {load} from '@loaders.gl/core';
import type {LoaderOptions} from '@loaders.gl/core';
import {progressFetch, cancelCurrent} from './progressFetch';
import {PRESENCE_SIGNAL, layerBitFromUrl} from '../common/layers';

interface WorkerRequest {
    urls: string[];
    presenceOnly: boolean[];
    requestId: number;
}

// Working typed arrays for the merged result
let h3lo: Uint32Array;
let h3hi: Uint32Array;
let minAgl: Uint16Array;
let minAlt: Uint16Array;
let minAltSig: Uint8Array;
let maxSig: Uint8Array;
let avgSig: Uint8Array;
let avgCrc: Uint8Array;
let count: Uint32Array;
let avgGap: Uint8Array;
let layerMask: Uint8Array;
let stationsArr: string[] | undefined;
let numStationsArr: Uint8Array | undefined;
let expectedGapArr: Uint8Array | undefined;
let length = 0;

let currentRequestId = -1;
let hasPartialData = false;
let trackLayers = false;

function mergeStationStrings(
    s1: string, count1: number,
    s2: string, count2: number,
    mergedCount: number
): {encoded: string; numStations: number} {
    const counts = new Map<number, number>();
    for (const token of s1.split(',')) {
        if (!token) continue;
        const v = parseInt(token, 36);
        counts.set(v >> 4, Math.round((v & 0xf) * count1 / 10));
    }
    for (const token of s2.split(',')) {
        if (!token) continue;
        const v = parseInt(token, 36);
        const sid = v >> 4;
        counts.set(sid, (counts.get(sid) ?? 0) + Math.round((v & 0xf) * count2 / 10));
    }
    const totalUnique = counts.size;
    const entries = [...counts.entries()].sort((a, b) => b[1] - a[1]).slice(0, 30);
    const encoded = entries.map(([sid, cnt]) => {
        const pct = Math.trunc(10 * cnt / mergedCount) & 0xf;
        return ((sid << 4) | pct).toString(36);
    }).join(',');
    return {encoded, numStations: Math.min(totalUnique, 255)};
}

function weightedAvg(oldCount: number, oldVal: number, newCount: number, newVal: number): number {
    const total = oldCount + newCount;
    if (total === 0) return 0;
    return Math.round((oldVal * oldCount + newVal * newCount) / total);
}

async function loadFile(url: string, fileIndex: number, totalFiles: number, requestId: number): Promise<any | null> {
    try {
        let lastProgressMs = 0;
        const setProgress = (p: number) => {
            const now = Date.now();
            if (p >= 1 || now - lastProgressMs >= 100) {
                lastProgressMs = now;
                self.postMessage({type: 'progress', requestId, fileIndex, progress: p ?? 1, url, totalFiles});
            }
        };
        const result = await load(url, ArrowLoader, {
            fetch: async (input: any, init?: any) => {
                const response = await fetch(input, init);
                if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
                return progressFetch(setProgress)(response);
            }
        } as LoaderOptions);
        return (result as any).data;
    } catch (e) {
        console.log(`arrowworker: skipping ${url}: ${e}`);
        return null;
    }
}

function postResult(requestId: number) {
    const layerMaskExtras = trackLayers ? {layerMask} : {};
    const layerMaskBuffers = trackLayers ? [layerMask.buffer] : [];
    const stationExtras = stationsArr
        ? {stations: stationsArr, numStations: numStationsArr, expectedGap: expectedGapArr}
        : {};
    const stationBuffers = stationsArr
        ? [numStationsArr!.buffer, expectedGapArr!.buffer]
        : [];
    self.postMessage(
        {type: 'result', requestId, h3lo, h3hi, minAgl, minAlt, minAltSig, maxSig, avgSig, avgCrc, count, avgGap, length, ...layerMaskExtras, ...stationExtras},
        [h3lo.buffer, h3hi.buffer, minAgl.buffer, minAlt.buffer,
         minAltSig.buffer, maxSig.buffer, avgSig.buffer, avgCrc.buffer,
         count.buffer, avgGap.buffer, ...layerMaskBuffers, ...stationBuffers] as ArrayBuffer[]
    );
}

function mergeData(newData: any, presenceOnly: boolean, layerBit: number): void {
    const newH3lo = new Uint32Array(newData.h3lo);
    const newH3hi = new Uint32Array(newData.h3hi);
    const newCount: Uint32Array = newData.count;
    const newMinAgl: Uint16Array = newData.minAgl;
    const newMinAlt: Uint16Array = newData.minAlt;
    const newMinAltSig: Uint8Array = newData.minAltSig;
    const newMaxSig: Uint8Array = newData.maxSig;
    const newAvgSig: Uint8Array = newData.avgSig;
    const newAvgCrc: Uint8Array = newData.avgCrc;
    const newAvgGap: Uint8Array = newData.avgGap;
    const newStations: string[] | undefined = newData.stations;
    const newNumStations: Uint8Array | undefined = newData.numStations;
    const newLen = newH3lo.length;

    const maxLen = length + newLen;
    const outH3lo = new Uint32Array(maxLen);
    const outH3hi = new Uint32Array(maxLen);
    const outMinAgl = new Uint16Array(maxLen);
    const outMinAlt = new Uint16Array(maxLen);
    const outMinAltSig = new Uint8Array(maxLen);
    const outMaxSig = new Uint8Array(maxLen);
    const outAvgSig = new Uint8Array(maxLen);
    const outAvgCrc = new Uint8Array(maxLen);
    const outCount = new Uint32Array(maxLen);
    const outAvgGap = new Uint8Array(maxLen);
    const outLayerMaskArr = trackLayers ? new Uint8Array(maxLen) : null;
    const outStationsArr = stationsArr ? new Array<string>(maxLen) : null;
    const outNumStationsArr = stationsArr ? new Uint8Array(maxLen) : null;
    const outExpectedGapArr = stationsArr ? new Uint8Array(maxLen) : null;
    const newExpectedGap: Uint8Array | undefined = newData.expectedGap;

    const bulkCopyExisting = (iStart: number, iEnd: number, kDest: number) => {
        outH3lo.set(h3lo.subarray(iStart, iEnd), kDest);
        outH3hi.set(h3hi.subarray(iStart, iEnd), kDest);
        outMinAgl.set(minAgl.subarray(iStart, iEnd), kDest);
        outMinAlt.set(minAlt.subarray(iStart, iEnd), kDest);
        outMinAltSig.set(minAltSig.subarray(iStart, iEnd), kDest);
        outMaxSig.set(maxSig.subarray(iStart, iEnd), kDest);
        outAvgSig.set(avgSig.subarray(iStart, iEnd), kDest);
        outAvgCrc.set(avgCrc.subarray(iStart, iEnd), kDest);
        outCount.set(count.subarray(iStart, iEnd), kDest);
        outAvgGap.set(avgGap.subarray(iStart, iEnd), kDest);
        if (outLayerMaskArr) outLayerMaskArr.set(layerMask.subarray(iStart, iEnd), kDest);
        if (outStationsArr) {
            const run = iEnd - iStart;
            for (let x = 0; x < run; x++) outStationsArr[kDest + x] = stationsArr![iStart + x];
            outNumStationsArr!.set(numStationsArr!.subarray(iStart, iEnd), kDest);
            outExpectedGapArr!.set(expectedGapArr!.subarray(iStart, iEnd), kDest);
        }
    };

    const bulkCopyNew = (jStart: number, jEnd: number, kDest: number) => {
        const run = jEnd - jStart;
        outH3lo.set(newH3lo.subarray(jStart, jEnd), kDest);
        outH3hi.set(newH3hi.subarray(jStart, jEnd), kDest);
        outMinAgl.set(newMinAgl.subarray(jStart, jEnd), kDest);
        outMinAlt.set(newMinAlt.subarray(jStart, jEnd), kDest);
        outMinAltSig.set(newMinAltSig.subarray(jStart, jEnd), kDest);
        outMaxSig.set(newMaxSig.subarray(jStart, jEnd), kDest);
        outAvgSig.set(newAvgSig.subarray(jStart, jEnd), kDest);
        outAvgCrc.set(newAvgCrc.subarray(jStart, jEnd), kDest);
        outCount.set(newCount.subarray(jStart, jEnd), kDest);
        outAvgGap.set(newAvgGap.subarray(jStart, jEnd), kDest);
        if (outLayerMaskArr) outLayerMaskArr.fill(layerBit, kDest, kDest + run);
        if (outStationsArr && newStations) {
            for (let x = 0; x < run; x++) outStationsArr[kDest + x] = newStations[jStart + x] ?? '';
            if (newNumStations) outNumStationsArr!.set(newNumStations.subarray(jStart, jEnd), kDest);
            if (newExpectedGap) outExpectedGapArr!.set(newExpectedGap.subarray(jStart, jEnd), kDest);
        }
    };

    let i = 0, j = 0, k = 0;
    while (i < length && j < newLen) {
        let cmp: number;
        if (h3hi[i] !== newH3hi[j])      cmp = h3hi[i] < newH3hi[j] ? -1 : 1;
        else if (h3lo[i] !== newH3lo[j]) cmp = h3lo[i] < newH3lo[j] ? -1 : 1;
        else                              cmp = 0;

        if (cmp < 0) {
            // existing hex not in new file — bulk-copy the contiguous run
            let iEnd = i + 1;
            const tHi = newH3hi[j], tLo = newH3lo[j];
            while (iEnd < length && (h3hi[iEnd] < tHi || (h3hi[iEnd] === tHi && h3lo[iEnd] < tLo))) iEnd++;
            bulkCopyExisting(i, iEnd, k);
            k += iEnd - i; i = iEnd;
        } else if (cmp > 0) {
            // new hex not in existing — bulk-copy the contiguous run
            let jEnd = j + 1;
            const tHi = h3hi[i], tLo = h3lo[i];
            while (jEnd < newLen && (newH3hi[jEnd] < tHi || (newH3hi[jEnd] === tHi && newH3lo[jEnd] < tLo))) jEnd++;
            bulkCopyNew(j, jEnd, k);
            k += jEnd - j; j = jEnd;
        } else {
            // same hex — merge
            const oldCount = count[i];
            const nc = newCount[j];
            const mergedCount = oldCount + nc;

            outH3lo[k] = h3lo[i];
            outH3hi[k] = h3hi[i];
            outAvgGap[k] = weightedAvg(oldCount, avgGap[i], nc, newAvgGap[j]);
            outCount[k] = mergedCount;

            if (newMinAgl[j] < minAgl[i]) {
                outMinAgl[k] = newMinAgl[j];
            } else {
                outMinAgl[k] = minAgl[i];
            }
            if (newMinAlt[j] < minAlt[i]) {
                outMinAlt[k] = newMinAlt[j];
                outMinAltSig[k] = presenceOnly ? PRESENCE_SIGNAL : newMinAltSig[j];
            } else {
                outMinAlt[k] = minAlt[i];
                outMinAltSig[k] = minAltSig[i];
            }
            if (!presenceOnly) {
                outMaxSig[k] = newMaxSig[j] > maxSig[i] ? newMaxSig[j] : maxSig[i];
                outAvgSig[k] = weightedAvg(oldCount, avgSig[i], nc, newAvgSig[j]);
                outAvgCrc[k] = weightedAvg(oldCount, avgCrc[i], nc, newAvgCrc[j]);
            } else {
                outMaxSig[k] = maxSig[i];
                outAvgSig[k] = avgSig[i];
                outAvgCrc[k] = avgCrc[i];
            }

            if (outLayerMaskArr) outLayerMaskArr[k] = layerMask[i] | layerBit;

            if (outStationsArr) {
                if (newStations) {
                    const merged = mergeStationStrings(stationsArr![i], oldCount, newStations[j], nc, mergedCount);
                    outStationsArr[k] = merged.encoded;
                    outNumStationsArr![k] = merged.numStations;
                    outExpectedGapArr![k] = merged.numStations > 0 ? (outAvgGap[k] / merged.numStations) >> 0 : 0;
                } else {
                    outStationsArr[k] = stationsArr![i];
                    outNumStationsArr![k] = numStationsArr![i];
                    outExpectedGapArr![k] = expectedGapArr![i];
                }
            }

            k++; i++; j++;
        }
    }

    // drain remaining entries from whichever side wasn't exhausted
    if (i < length) { bulkCopyExisting(i, length, k); k += length - i; }
    if (j < newLen)  { bulkCopyNew(j, newLen, k);     k += newLen - j; }

    // trim to exact size
    h3lo = outH3lo.slice(0, k);
    h3hi = outH3hi.slice(0, k);
    minAgl = outMinAgl.slice(0, k);
    minAlt = outMinAlt.slice(0, k);
    minAltSig = outMinAltSig.slice(0, k);
    maxSig = outMaxSig.slice(0, k);
    avgSig = outAvgSig.slice(0, k);
    avgCrc = outAvgCrc.slice(0, k);
    count = outCount.slice(0, k);
    avgGap = outAvgGap.slice(0, k);
    if (outLayerMaskArr) layerMask = outLayerMaskArr.slice(0, k);
    if (outStationsArr) {
        stationsArr = outStationsArr.slice(0, k);
        numStationsArr = outNumStationsArr!.slice(0, k);
        expectedGapArr = outExpectedGapArr!.slice(0, k);
    }
    length = k;
}

self.addEventListener('message', async (event: MessageEvent<WorkerRequest>) => {
    if ((event.data as any).type === 'abort') {
        const abortReqId = (event.data as any).requestId;
        if (currentRequestId === abortReqId) {
            cancelCurrent();
            currentRequestId = -1;
            if (hasPartialData) {
                postResult(abortReqId);
            } else {
                self.postMessage({type: 'result', requestId: abortReqId, length: 0});
            }
        }
        return;
    }

    const {urls, presenceOnly, requestId} = event.data;
    currentRequestId = requestId;
    hasPartialData = false;
    const urlLayerBits = urls.map(layerBitFromUrl);
    trackLayers = new Set(urlLayerBits).size > 1;

    if (!urls.length) {
        self.postMessage({type: 'result', requestId, length: 0});
        return;
    }

    try {
        let firstLoaded = false;
        stationsArr = undefined;
        numStationsArr = undefined;
        expectedGapArr = undefined;

        for (let i = 0; i < urls.length; i++) {
            if (currentRequestId !== requestId) return;

            const data = await loadFile(urls[i], i, urls.length, requestId);
            if (currentRequestId !== requestId) return;
            if (!data) continue;

            if (!firstLoaded) {
                h3lo = new Uint32Array(data.h3lo);
                h3hi = new Uint32Array(data.h3hi);
                minAgl = new Uint16Array(data.minAgl);
                minAlt = new Uint16Array(data.minAlt);
                minAltSig = new Uint8Array(data.minAltSig);
                maxSig = new Uint8Array(data.maxSig);
                avgSig = new Uint8Array(data.avgSig);
                avgCrc = new Uint8Array(data.avgCrc);
                count = new Uint32Array(data.count);
                avgGap = new Uint8Array(data.avgGap);
                length = h3lo.length;
                if (trackLayers) layerMask = new Uint8Array(length).fill(urlLayerBits[i]);
                if (data.stations) {
                    stationsArr = Array.from(data.stations as string[]);
                    numStationsArr = new Uint8Array(data.numStations ?? new Uint8Array(length));
                    expectedGapArr = new Uint8Array(data.expectedGap ?? new Uint8Array(length));
                }
                firstLoaded = true;
                hasPartialData = true;
            } else {
                mergeData(data, presenceOnly[i], urlLayerBits[i]);
            }
        }

        if (!firstLoaded) {
            self.postMessage({type: 'result', requestId, length: 0});
            return;
        }

        postResult(requestId);
        hasPartialData = false; // buffers are now detached; a stale abort must not re-transfer them
    } catch (e) {
        console.error('arrowworker error:', e);
        self.postMessage({type: 'result', requestId, length: 0});
    }
});
