/// <reference lib="webworker" />

import {ArrowLoader} from '@loaders.gl/arrow';
import {load} from '@loaders.gl/core';
import type {LoaderOptions} from '@loaders.gl/core';
import {progressFetch, cancelCurrent} from './progressFetch';
import {PRESENCE_SIGNAL} from '../common/layers';

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
let length = 0;

let currentRequestId = -1;
let hasPartialData = false;

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

function postResult(requestId: number, extra?: object) {
    self.postMessage(
        {type: 'result', requestId, h3lo, h3hi, minAgl, minAlt, minAltSig, maxSig, avgSig, avgCrc, count, avgGap, length, ...extra},
        [h3lo.buffer, h3hi.buffer, minAgl.buffer, minAlt.buffer,
         minAltSig.buffer, maxSig.buffer, avgSig.buffer, avgCrc.buffer,
         count.buffer, avgGap.buffer] as ArrayBuffer[]
    );
}

function buildIndex(): Map<bigint, number> {
    const index = new Map<bigint, number>();
    for (let i = 0; i < length; i++) {
        const key = (BigInt(h3hi[i]) << 32n) | BigInt(h3lo[i]);
        index.set(key, i);
    }
    return index;
}

function mergeData(newData: any, presenceOnly: boolean): void {
    const newH3lo: Uint32Array = newData.h3lo;
    const newH3hi: Uint32Array = newData.h3hi;
    const newCount: Uint32Array = newData.count;
    const newMinAgl: Uint16Array = newData.minAgl;
    const newMinAlt: Uint16Array = newData.minAlt;
    const newMinAltSig: Uint8Array = newData.minAltSig;
    const newMaxSig: Uint8Array = newData.maxSig;
    const newAvgSig: Uint8Array = newData.avgSig;
    const newAvgCrc: Uint8Array = newData.avgCrc;
    const newAvgGap: Uint8Array = newData.avgGap;
    const newLen = newH3lo.length;

    const index = buildIndex();

    const appendIndices: number[] = [];

    for (let j = 0; j < newLen; j++) {
        const key = (BigInt(newH3hi[j]) << 32n) | BigInt(newH3lo[j]);
        const i = index.get(key);

        if (i !== undefined) {
            const oldCount = count[i];
            const nc = newCount[j];

            avgGap[i] = weightedAvg(oldCount, avgGap[i], nc, newAvgGap[j]);

            if (newMinAgl[j] < minAgl[i]) {
                minAgl[i] = newMinAgl[j];
            }
            if (newMinAlt[j] < minAlt[i]) {
                minAlt[i] = newMinAlt[j];
                minAltSig[i] = presenceOnly ? PRESENCE_SIGNAL : newMinAltSig[j];
            }
            if (!presenceOnly) {
                if (newMaxSig[j] > maxSig[i]) {
                    maxSig[i] = newMaxSig[j];
                }
                avgSig[i] = weightedAvg(oldCount, avgSig[i], nc, newAvgSig[j]);
                avgCrc[i] = weightedAvg(oldCount, avgCrc[i], nc, newAvgCrc[j]);
            }

            count[i] += nc;
        } else {
            appendIndices.push(j);
        }
    }

    if (appendIndices.length > 0) {
        const newLength = length + appendIndices.length;
        const newH3loArr = new Uint32Array(newLength);
        const newH3hiArr = new Uint32Array(newLength);
        const newMinAglArr = new Uint16Array(newLength);
        const newMinAltArr = new Uint16Array(newLength);
        const newMinAltSigArr = new Uint8Array(newLength);
        const newMaxSigArr = new Uint8Array(newLength);
        const newAvgSigArr = new Uint8Array(newLength);
        const newAvgCrcArr = new Uint8Array(newLength);
        const newCountArr = new Uint32Array(newLength);
        const newAvgGapArr = new Uint8Array(newLength);

        newH3loArr.set(h3lo.subarray(0, length));
        newH3hiArr.set(h3hi.subarray(0, length));
        newMinAglArr.set(minAgl.subarray(0, length));
        newMinAltArr.set(minAlt.subarray(0, length));
        newMinAltSigArr.set(minAltSig.subarray(0, length));
        newMaxSigArr.set(maxSig.subarray(0, length));
        newAvgSigArr.set(avgSig.subarray(0, length));
        newAvgCrcArr.set(avgCrc.subarray(0, length));
        newCountArr.set(count.subarray(0, length));
        newAvgGapArr.set(avgGap.subarray(0, length));

        for (let k = 0; k < appendIndices.length; k++) {
            const j = appendIndices[k];
            const idx = length + k;
            newH3loArr[idx] = newH3lo[j];
            newH3hiArr[idx] = newH3hi[j];
            newMinAglArr[idx] = newMinAgl[j];
            newMinAltArr[idx] = newMinAlt[j];
            newMinAltSigArr[idx] = newMinAltSig[j];
            newMaxSigArr[idx] = newMaxSig[j];
            newAvgSigArr[idx] = newAvgSig[j];
            newAvgCrcArr[idx] = newAvgCrc[j];
            newCountArr[idx] = newCount[j];
            newAvgGapArr[idx] = newAvgGap[j];
        }

        h3lo = newH3loArr;
        h3hi = newH3hiArr;
        minAgl = newMinAglArr;
        minAlt = newMinAltArr;
        minAltSig = newMinAltSigArr;
        maxSig = newMaxSigArr;
        avgSig = newAvgSigArr;
        avgCrc = newAvgCrcArr;
        count = newCountArr;
        avgGap = newAvgGapArr;
        length = newLength;
    }
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

    if (!urls.length) {
        self.postMessage({type: 'result', requestId, length: 0});
        return;
    }

    try {
        let firstLoaded = false;
        let firstStations: string[] | undefined;
        let firstExpectedGap: any;
        let firstNumStations: any;

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
                firstStations = data.stations;
                firstExpectedGap = data.expectedGap;
                firstNumStations = data.numStations;
                firstLoaded = true;
                hasPartialData = true;
            } else {
                mergeData(data, presenceOnly[i]);
            }
        }

        if (!firstLoaded) {
            self.postMessage({type: 'result', requestId, length: 0});
            return;
        }

        postResult(requestId, {stations: firstStations, expectedGap: firstExpectedGap, numStations: firstNumStations});
        hasPartialData = false; // buffers are now detached; a stale abort must not re-transfer them
    } catch (e) {
        console.error('arrowworker error:', e);
        self.postMessage({type: 'result', requestId, length: 0});
    }
});
