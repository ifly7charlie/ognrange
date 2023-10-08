import {NEXT_PUBLIC_DATA_URL} from '../common/config';

export interface StationMeta {
    name: string[];
    lng: number[];
    lat: number[];
    id: number[];
    length: number;
}

import {createContext, useContext, useEffect, useState} from 'react';

import {ArrowLoader} from '@loaders.gl/arrow';
import {load} from '@loaders.gl/core';

const StationMetaContext = createContext<StationMeta | null>(null);

export function useStationMeta() {
    return useContext(StationMetaContext);
}

export function StationMeta(props: React.PropsWithChildren<{env: {NEXT_PUBLIC_DATA_URL: string}}>) {
    const [stationMeta, setStationMeta] = useState<StationMeta>({name: [], lng: [], lat: [], id: [], length: 0});

    useEffect(() => {
        load(`${props.env.NEXT_PUBLIC_DATA_URL ?? NEXT_PUBLIC_DATA_URL}stations.arrow`, ArrowLoader)
            .then((data: StationMeta) => {
                console.log('setting station meta', data.id.length, 'stations');
                setStationMeta({...data, length: data.id.length});
            })
            .catch((e) => {
                console.log(e);
            });
    }, [true]);

    return <StationMetaContext.Provider value={stationMeta}>{props.children}</StationMetaContext.Provider>;
}
