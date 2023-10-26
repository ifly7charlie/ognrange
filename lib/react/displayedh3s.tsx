import {NEXT_PUBLIC_DATA_URL} from '../common/config';

import {createContext, useContext, useEffect, useState} from 'react';
import {useSearchParams} from 'next/navigation';

import {ArrowLoader} from '@loaders.gl/arrow';
import {load} from '@loaders.gl/core';
import {progressFetch} from './progressFetch';

const DisplayedH3sContext = createContext<DisplayedH3sType>({length: 0});

export interface DisplayedH3sType {
    length: number;
    d?: ArrowFileType;
    logMaxCount?: number;
    loadingLayer?: any;
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

export function DisplayedH3s(props: React.PropsWithChildren<{env: {NEXT_PUBLIC_DATA_URL: string}}>) {
    const params = useSearchParams();
    const station = params.get('station');
    const file = params.get('file')?.toString();

    const [displayedH3s, setDisplayedH3s] = useState<DisplayedH3sType>({length: 0});
    const [isLoaded, setLoaded] = useState<number | null>(null);

    useEffect(() => {
        load(
            `${props.env.NEXT_PUBLIC_DATA_URL || NEXT_PUBLIC_DATA_URL}${station || 'global'}/${station || 'global'}.${file || 'year'}.arrow`, //
            ArrowLoader,
            {
                fetch: (input, init) => {
                    return fetch(input, init).then(progressFetch(setLoaded));
                }
            }
        )
            .then((data: ArrowFileType) => {
                console.log('setting arrow file', station || 'global', file || 'year', data.h3lo.length, 'h3s');
                let maxCount = 0;
                for (const v of data.count) {
                    maxCount = Math.max(maxCount, v);
                }
                setLoaded(null);
                setDisplayedH3s({length: data.h3lo.length, d: data, logMaxCount: Math.log2(maxCount)});
            })
            .catch((e) => {
                console.log(station, file, e);
            });
    }, [station, file]);

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
