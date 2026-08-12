import {useCallback, useEffect, useState} from 'react';
import {useTranslation} from 'next-i18next';

import {groundUrl} from './coveragedetails/grounddata';
import type {FloorDisc} from './floordata';

// Loads the coverage-floor disc for the selected station via floorworker.ts.
// Only fetches while a floor visualisation is enabled; the result is kept
// around so toggling between floor and coverage visualisations doesn't
// recompute. Data for a different station/period/frequency is never returned.
// While loading, `loadingLayer` carries the same progress-bar overlay the
// coverage file loader uses (downloads then the disc computation).
//
// Each request gets its own worker, terminated in the effect cleanup: the
// request lifecycle is fully owned by one effect run, so StrictMode's dev
// double-mount (or an HMR remount) mid-load just cancels and retries instead
// of deadlocking on a ref that survived the remount

// The receive-horizon file backing the floor for the viewed period. Horizon
// files exist for month/year/yearnz, but the floor always uses a year-scale
// horizon (the terrain doesn't change and the yearly minimum is the best
// estimate): current periods use the year/yearnz symlink, a past year its
// dated (frozen) file
export function floorHorizonFileFor(dateStart: string | undefined): string {
    if (dateStart === 'yearnz') {
        return 'yearnz';
    }
    const nz = dateStart?.match(/^(\d{4})nz$/);
    if (nz) {
        return `yearnz.${nz[1]}nz`;
    }
    const dated = dateStart?.match(/^(\d{4})(-\d{2})?(-\d{2})?$/);
    if (dated) {
        return `year.${dated[1]}`;
    }
    return 'year';
}

interface FloorProgress {
    phase: 'fetch' | 'compute';
    fraction: number;
}

export function useFloorData(
    enabled: boolean,
    station: string | null | undefined,
    dateStart: string | undefined,
    frequency: number,
    maxKm: number,
    dataUrl: string
): {disc: FloorDisc | null; loadingLayer: React.ReactNode | null} {
    const {t} = useTranslation();
    const [data, setData] = useState<{key: string; disc: FloorDisc | null} | null>(null);
    const [progress, setProgress] = useState<FloorProgress | null>(null);

    const horizonFile = floorHorizonFileFor(dateStart);
    const key = station ? `${dataUrl}${station}|${horizonFile}|${frequency}|${maxKm}` : null;
    const loaded = data?.key === key;

    useEffect(() => {
        if (!enabled || !station || !key || loaded) {
            return;
        }

        const worker = new Worker(new URL('./floorworker.ts', import.meta.url));
        setProgress({phase: 'fetch', fraction: 0});
        worker.onmessage = (e) => {
            if (e.data.type === 'progress') {
                setProgress({phase: e.data.phase, fraction: e.data.progress});
            } else if (e.data.type === 'result') {
                setProgress(null);
                setData({key, disc: e.data.length ? (e.data as FloorDisc) : null});
            }
        };
        worker.postMessage({
            groundUrl: groundUrl(dataUrl, station),
            horizonUrl: `${dataUrl}${station}/${station}.${horizonFile}.horizon.arrow`,
            frequency,
            maxKm,
            requestId: 0
        });

        return () => {
            worker.terminate();
            setProgress(null);
        };
    }, [enabled, key, loaded]);

    // Cancel: record an empty result for this key so the load effect stops
    // retrying it; its cleanup terminates the in-flight worker. Changing
    // station/period/frequency clears the block naturally
    const handleAbort = useCallback(() => {
        if (key) {
            setData({key, disc: null});
        }
    }, [key]);

    const loadingLayer =
        enabled && progress ? (
            <div className="progress-bar">
                <button className="progress-cancel" onClick={handleAbort} title="Cancel">
                    ✕
                </button>
                <div className="progress-layer">
                    <div className="progress-layer-header">
                        <span className="progress-layer-name">{t(progress.phase === 'fetch' ? 'details.floor.loadingFetch' : 'details.floor.loadingCompute')}</span>
                        <span className="progress-layer-count">{Math.round(progress.fraction * 100)}%</span>
                    </div>
                    <div className="progress" style={{transform: `scaleX(${progress.fraction})`}} />
                </div>
            </div>
        ) : null;

    return {disc: enabled && loaded ? data!.disc : null, loadingLayer};
}
