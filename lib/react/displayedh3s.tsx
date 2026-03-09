import {NEXT_PUBLIC_DATA_URL} from '../common/config';
import {PRESENCE_ONLY, Layer} from '../common/layers';

import {createContext, useContext, useEffect, useState, useRef, useMemo} from 'react';
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
    const [isLoaded, setLoaded] = useState<number | null>(null);
    const workerRef = useRef<Worker | null>(null);

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
        return {urls, presenceOnly, layerList};
    }, [isTypeOnly, stationName, layerList, dateStart, DATA_URL]);

    // Date-range URLs — only computed when stationData is available and not type-only
    const rangeBuildUrls = useMemo(() => {
        if (isTypeOnly || !stationData) return null;
        // Extract period type from dateStart (e.g. 'year' from 'year.2023')
        const rangeType = dateStart.split('.')[0];
        const typeFiles = (stationData?.files || {})[rangeType];
        const combined = (typeFiles as any)?.combined ?? typeFiles;
        const filesInRange: {type: string; date: string}[] = [];
        for (const path of (combined?.all || []) as string[]) {
            const match = path.match(/\.(day|month|year|yearnz)\.([0-9-]+[nz]*)$/);
            if (!match) continue;
            const key = `${match[1]}.${match[2]}`;
            if (key >= dateStart && key <= dateEnd) {
                filesInRange.push({type: match[1], date: match[2]});
            }
        }
        const urls: string[] = [];
        const presenceOnly: boolean[] = [];
        for (const {type, date} of filesInRange) {
            for (const layer of layerList) {
                const suffix = layer === 'combined' ? '' : `.${layer}`;
                urls.push(`${DATA_URL}${stationName}/${stationName}.${type}.${date}${suffix}.arrow`);
                presenceOnly.push(PRESENCE_ONLY.has(layer as Layer));
            }
        }
        return {urls, presenceOnly, layerList};
    }, [isTypeOnly, stationName, layerList, dateStart, dateEnd, stationData, DATA_URL]);

    const buildUrls = useMemo(
        () => typeOnlyBuildUrls ?? rangeBuildUrls ?? {urls: [], presenceOnly: [], layerList: []},
        [typeOnlyBuildUrls, rangeBuildUrls]
    );

    // Create the worker once
    useEffect(() => {
        const w = new Worker(new URL('./arrowworker.ts', import.meta.url));
        workerRef.current = w;
        return () => {
            w.terminate();
            workerRef.current = null;
        };
    }, []);

    // Dispatch to worker when URL list changes
    useEffect(() => {
        const worker = workerRef.current;
        if (!worker || !buildUrls.urls.length) return;

        const {urls, presenceOnly, layerList} = buildUrls;
        const requestId = ++requestCounter;
        setLoaded(0.01);

        worker.onmessage = (e) => {
            const msg = e.data;
            if (msg.requestId !== requestId) return; // stale

            if (msg.type === 'progress') {
                setLoaded((msg.fileIndex + msg.progress) / (msg.totalFiles || urls.length));
            } else if (msg.type === 'result') {
                if (msg.length === 0) {
                    setLoaded(null);
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
                setLoaded(null);
                setDisplayedH3s({length: data.length, d: data as ArrowFileType, logMaxCount: Math.log2(maxCount), isPresenceOnly});
            }
        };

        worker.postMessage({urls, presenceOnly, requestId});
    }, [buildUrls]);

    displayedH3s.loadingLayer = isLoaded ? (
        <div className="progress-bar">
            {(isLoaded * 100).toFixed(0)}%
            <div className="progress" style={{transform: `scaleX(${isLoaded})`}} />
        </div>
    ) : null;

    return (
        //
        <DisplayedH3sContext.Provider value={displayedH3s}>{props.children}</DisplayedH3sContext.Provider>
    );
}
