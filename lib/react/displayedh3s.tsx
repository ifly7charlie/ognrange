import {NEXT_PUBLIC_DATA_URL} from '../common/config';
import {PRESENCE_ONLY, Layer} from '../common/layers';

import {createContext, useCallback, useContext, useEffect, useState, useRef, useMemo} from 'react';
import {useSearchParams} from 'next/navigation';

import useSWR from 'swr';

const DisplayedH3sContext = createContext<DisplayedH3sType>({length: 0});

const fetcher = (url: string) => fetch(url).then((r) => r.json());

export interface DisplayedH3sType {
    length: number;
    d?: ArrowFileType;
    logMaxCount?: number;
    loadingLayer?: any;
    isPresenceOnly?: boolean;
}

export interface ArrowFileType {
    h3lo: Uint32Array;
    h3hi: Uint32Array;
    minAgl: number[];
    minAlt: number[];
    minAltSig: number[];
    maxSig: number[];
    avgSig: number[];
    avgCrc: number[];
    count: number[];
    avgGap: number[];
    stations?: string[];
    expectedGap?: number[];
    numStations?: number[];
    layerMask?: Uint8Array;
}

interface LayerProgress {
    completedFiles: number;
    totalFiles: number;
    fileProgress: number;
    isActive: boolean;
}

interface LoadProgress {
    byLayer: Record<string, LayerProgress>;
}

export function useDisplayedH3s() {
    return useContext(DisplayedH3sContext);
}

let requestCounter = 0;

export function DisplayedH3s(props: React.PropsWithChildren<{env?: {NEXT_PUBLIC_DATA_URL?: string}}>) {
    const params = useSearchParams();
    const station = params.get('station');
    const file = params.get('file')?.toString();
    const layersParam = params.get('layers') || 'combined';
    const dateStart = params.get('dateStart') || file || 'year';
    const dateEnd = params.get('dateEnd') || file || 'year';

    const [displayedH3s, setDisplayedH3s] = useState<DisplayedH3sType>({length: 0});
    const [loadProgress, setLoadProgress] = useState<LoadProgress | null>(null);
    const workerRef = useRef<Worker | null>(null);
    const requestIdRef = useRef(0);

    const {data: stationData} = useSWR(`/api/station/${station || 'global'}`, fetcher);

    const DATA_URL = props.env?.NEXT_PUBLIC_DATA_URL || NEXT_PUBLIC_DATA_URL;
    const stationName = station || 'global';

    const layerList = useMemo(
        () =>
            layersParam
                .split(',')
                .map((l) => l.trim())
                .filter(Boolean),
        [layersParam]
    );

    const isTypeOnly = /^(day|month|year|yearnz)$/.test(dateStart) && dateStart === dateEnd;

    // Type-only URLs (current symlink) — stable, no stationData dep to avoid double-fetch
    const typeOnlyBuildUrls = useMemo(() => {
        if (!isTypeOnly) return null;
        const urls = layerList.map((layer) => {
            const suffix = layer === 'combined' ? '' : `.${layer}`;
            return `${DATA_URL}${stationName}/${stationName}.${dateStart}${suffix}.arrow`;
        });
        const presenceOnly = layerList.map((layer) => PRESENCE_ONLY.has(layer as Layer));
        const urlLayers = layerList.slice();
        return {urls, presenceOnly, layerList, urlLayers};
    }, [isTypeOnly, stationName, layerList, dateStart, DATA_URL]);

    // Date-range URLs — only computed when stationData is available and not type-only
    const rangeBuildUrls = useMemo(() => {
        if (isTypeOnly || !stationData) return null;
        const rangeType = dateStart.split('.')[0];
        const typeFiles = (stationData?.files || {})[rangeType] as Record<string, {all?: string[]}> | undefined;

        const urls: string[] = [];
        const presenceOnly: boolean[] = [];
        const urlLayers: string[] = [];

        for (const layer of layerList) {
            const layerFiles = typeFiles?.[layer];
            for (const path of (layerFiles?.all || []) as string[]) {
                const match = path.match(/\.(day|month|year|yearnz)\.([0-9-]+[nz]*)(?:\.[a-z]+)?$/);
                if (!match) continue;
                const key = `${match[1]}.${match[2]}`;
                if (key >= dateStart && key <= dateEnd) {
                    const suffix = layer === 'combined' ? '' : `.${layer}`;
                    urls.push(`${DATA_URL}${stationName}/${stationName}.${match[1]}.${match[2]}${suffix}.arrow`);
                    presenceOnly.push(PRESENCE_ONLY.has(layer as Layer));
                    urlLayers.push(layer);
                }
            }
        }
        return {urls, presenceOnly, layerList, urlLayers};
    }, [isTypeOnly, stationName, layerList, dateStart, dateEnd, stationData, DATA_URL]);

    const buildUrls = useMemo(
        () => typeOnlyBuildUrls ?? rangeBuildUrls ?? {urls: [], presenceOnly: [], layerList: [], urlLayers: []},
        [typeOnlyBuildUrls, rangeBuildUrls]
    );

    function clearProgress() {
        setLoadProgress(null);
    }

    const handleAbort = useCallback(() => {
        workerRef.current?.postMessage({type: 'abort', requestId: requestIdRef.current});
    }, []);

    // Create the worker once
    useEffect(() => {
        const w = new Worker(new URL('./arrowworker.ts', import.meta.url));
        workerRef.current = w;
        return () => {
            w.terminate();
            workerRef.current = null;
        };
    }, []);

    // Dispatch to worker when URL list changes, debounced to avoid spurious loads
    // while the user is still adjusting date range controls
    useEffect(() => {
        const worker = workerRef.current;
        if (!worker || !buildUrls.urls.length) return;

        const {urls, presenceOnly, layerList, urlLayers} = buildUrls;
        const timer = setTimeout(() => {
            // Abort any previous in-flight request before starting the new one,
            // so the old (potentially large) download stops immediately
            const prevId = requestIdRef.current;
            if (prevId > 0) worker.postMessage({type: 'abort', requestId: prevId});

            const requestId = ++requestCounter;
            requestIdRef.current = requestId;

            // Compute per-layer file totals
            const layerTotals: Record<string, number> = {};
            for (const l of layerList) layerTotals[l] = 0;
            for (const l of urlLayers) layerTotals[l]++;

            const initialByLayer: Record<string, LayerProgress> = {};
            for (const l of layerList) {
                initialByLayer[l] = {completedFiles: 0, totalFiles: layerTotals[l], fileProgress: 0, isActive: false};
            }
            setLoadProgress({byLayer: initialByLayer});

            worker.onmessage = (e) => {
                const msg = e.data;
                if (msg.requestId !== requestId) return; // stale

                if (msg.type === 'progress') {
                    const fi: number = msg.fileIndex;
                    const prog: number = msg.progress;
                    const byLayer: Record<string, LayerProgress> = {};
                    for (const l of layerList) {
                        const completed = urlLayers.slice(0, fi).filter((u: string) => u === l).length;
                        const isActive = urlLayers[fi] === l;
                        byLayer[l] = {completedFiles: completed, totalFiles: layerTotals[l], fileProgress: isActive ? prog : 0, isActive};
                    }
                    setLoadProgress({byLayer});
                } else if (msg.type === 'result') {
                    if (msg.length === 0) {
                        clearProgress();
                        setDisplayedH3s({length: 0});
                        return;
                    }
                    const data = msg as any;
                    let maxCount = 0;
                    const c = data.count as Uint32Array;
                    for (let i = 0; i < data.length; i++) {
                        if (c[i] > maxCount) maxCount = c[i];
                    }
                    const isPresenceOnly = layerList.length > 0 && layerList.every((l) => PRESENCE_ONLY.has(l as Layer));
                    clearProgress();
                    setDisplayedH3s({length: data.length, d: data as ArrowFileType, logMaxCount: Math.log2(maxCount), isPresenceOnly});
                }
            };

            worker.postMessage({urls, presenceOnly, urlLayers, requestId});
        }, 300);

        return () => clearTimeout(timer);
    }, [buildUrls]);

    displayedH3s.loadingLayer = loadProgress ? (
        <div className="progress-bar">
            <button className="progress-cancel" onClick={handleAbort} title="Cancel">✕</button>
            {Object.entries(loadProgress.byLayer).map(([layer, lp]) => (
                <div key={layer} className="progress-layer">
                    <div className="progress-layer-header">
                        <span className="progress-layer-name">{layer}</span>
                        <span className="progress-layer-count">
                            {lp.completedFiles + (lp.isActive ? 1 : 0)} / {lp.totalFiles}
                        </span>
                    </div>
                    {lp.isActive && (
                        <>
                            <div className="progress" style={{transform: `scaleX(${lp.fileProgress})`}} />
                            <div className="progress-pct">{(lp.fileProgress * 100).toFixed(0)}%</div>
                        </>
                    )}
                </div>
            ))}
        </div>
    ) : null;

    return (
        //
        <DisplayedH3sContext.Provider value={displayedH3s}>{props.children}</DisplayedH3sContext.Provider>
    );
}
