/// <reference lib="webworker" />

import {tableFromIPC, Table} from 'apache-arrow';
import {progressFetch} from './progressFetch';
import {computeFloorDisc} from './floordata';

// Off-main-thread coverage-floor computation: fetch the station's
// ground-horizon and receive-horizon arrow files and sweep the shadow-envelope
// math over the whole 120km disc (~61k cells). Progress is posted as a single
// 0-1 fraction spanning both downloads and the computation; results carry
// transferred buffers. A stale requestId means the map moved on and the
// message is dropped by the hook

export interface FloorWorkerRequest {
    groundUrl: string;
    horizonUrl: string;
    frequency: number;
    requestId: number;
}

// Share of the overall progress bar given to each phase: the ground terrain
// file dominates the download, the horizon file is small, then the disc sweep
const GROUND_FETCH_END = 0.45;
const HORIZON_FETCH_END = 0.55;

async function fetchTable(url: string, onProgress: (fraction: number) => void): Promise<Table | null> {
    try {
        const res = await fetch(url);
        if (!res.ok) {
            return null;
        }
        // progressFetch's total estimate can overshoot with content-encoding - clamp
        let wrapped: Response = res;
        try {
            wrapped = progressFetch((p: number) => onProgress(Math.min(p ?? 1, 1)))(res);
        } catch (_e) {
            // No usable size headers - just skip download progress for this file
        }
        const buffer = await wrapped.arrayBuffer();
        return tableFromIPC(new Uint8Array(buffer));
    } catch (e) {
        console.log(`floorworker: unable to load ${url}: ${e}`);
        return null;
    }
}

self.onmessage = async (e: MessageEvent<FloorWorkerRequest>) => {
    const {groundUrl, horizonUrl, frequency, requestId} = e.data;
    const post = (phase: 'fetch' | 'compute', progress: number) => self.postMessage({type: 'progress', requestId, phase, progress});

    try {
        // Serial: progressFetch only tracks one stream at a time
        post('fetch', 0);
        const groundTable = await fetchTable(groundUrl, (p) => post('fetch', p * GROUND_FETCH_END));
        post('fetch', GROUND_FETCH_END);
        const horizonTable = await fetchTable(horizonUrl, (p) => post('fetch', GROUND_FETCH_END + p * (HORIZON_FETCH_END - GROUND_FETCH_END)));
        post('compute', HORIZON_FETCH_END);

        // No receive horizon just means terrain-only floors; no ground horizon
        // (mobile/new station) means no disc at all
        const disc = groundTable //
            ? computeFloorDisc(groundTable, horizonTable, frequency, (f) => post('compute', HORIZON_FETCH_END + f * (1 - HORIZON_FETCH_END)))
            : null;
        if (!disc) {
            self.postMessage({type: 'result', requestId, length: 0});
            return;
        }
        self.postMessage(
            {type: 'result', requestId, ...disc}, //
            [disc.h3lo.buffer, disc.h3hi.buffer, disc.ground.buffer, disc.terrainFloor.buffer, disc.coverageFloor.buffer, disc.terrainAngle.buffer, disc.receiveAngle.buffer] as unknown as Transferable[]
        );
    } catch (err) {
        console.log(`floorworker: ${err}`);
        self.postMessage({type: 'result', requestId, length: 0});
    }
};
