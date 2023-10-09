import {NEXT_PUBLIC_DATA_URL} from '../common/config';

import {createContext, useContext, useEffect, useState} from 'react';
import {useSearchParams} from 'next/navigation';

import {ArrowLoader} from '@loaders.gl/arrow';
import {load} from '@loaders.gl/core';

const StationMetaContext = createContext<StationMeta | null>(null);

export interface StationMeta {
    name: string[];
    lng: Float32Array;
    lat: Float32Array;
    id: Uint32Array;
    lastBeacon?: Uint32Array;
    lastPacket?: Uint32Array;

    length: number;
}

export function useStationMeta() {
    return useContext(StationMetaContext);
}

export function StationMeta(props: React.PropsWithChildren<{env: {NEXT_PUBLIC_DATA_URL: string}}>) {
    // So we can load a suitable file for this
    const params = useSearchParams();
    const file = params.get('file')?.toString() ?? 'year'; // default if no file is current year

    // What has been loaded
    const [stationMeta, setStationMeta] = useState<StationMeta>(() => ({
        name: [], //
        lng: new Float32Array(),
        lat: new Float32Array(),
        id: new Uint32Array(),
        length: 0
    }));

    useEffect(() => {
        load(`${props.env.NEXT_PUBLIC_DATA_URL ?? NEXT_PUBLIC_DATA_URL}stations.${file}.arrow`, ArrowLoader)
            .then((data: StationMeta) => {
                console.log('setting station meta for', file, 'with', data.id.length, 'stations');
                setStationMeta({...data, length: data.id.length});
            })
            .catch((e) => {
                if (e.message.match(/arrow \(404\)/)) {
                    return load(`${props.env.NEXT_PUBLIC_DATA_URL ?? NEXT_PUBLIC_DATA_URL}stations.arrow`, ArrowLoader)
                        .then((data: StationMeta) => {
                            console.log('setting station meta', data.id.length, 'stations');
                            setStationMeta({...data, length: data.id.length});
                        })
                        .catch((e) => {
                            console.log('Error loading stations.arrow (fallback)', e);
                        });
                }
            });
    }, [file]);

    return <StationMetaContext.Provider value={stationMeta}>{props.children}</StationMetaContext.Provider>;
}
