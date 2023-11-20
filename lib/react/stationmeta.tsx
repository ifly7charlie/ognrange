import {NEXT_PUBLIC_DATA_URL} from '../common/config';

import {createContext, useContext, useEffect, useCallback, useState} from 'react';
import {useSearchParams} from 'next/navigation';

import {ArrowLoader} from '@loaders.gl/arrow';
import {load} from '@loaders.gl/core';

const StationMetaContext = createContext<StationMeta | null>(null);

export interface StationMeta {
    name: string[];
    lng: Float32Array;
    lat: Float32Array;
    id: Uint32Array;
    valid?: boolean[];
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
    const allStations = parseInt(params.get('allStations')?.toString() ?? '0');

    // What has been loaded
    const [stationMeta, setStationMetaInternal] = useState<StationMeta>(() => ({
        name: [], //
        lng: new Float32Array(),
        lat: new Float32Array(),
        id: new Uint32Array(),
        valid: [],
        length: 0
    }));

    const setStationMeta = useCallback(
        (data) => {
            const validFilter = (_value: any, index: number) => data.valid[index];
            const filteredData =
                allStations || !('valid' in data)
                    ? data
                    : {
                          name: data.name.filter(validFilter), //
                          lng: data.lng.filter(validFilter),
                          lat: data.lat.filter(validFilter),
                          id: data.id.filter(validFilter),
                          valid: data.valid.filter(validFilter)
                      };
            setStationMetaInternal({...filteredData, length: filteredData.id.length});
        },
        [allStations]
    );

    useEffect(() => {
        load(`${props.env.NEXT_PUBLIC_DATA_URL ?? NEXT_PUBLIC_DATA_URL}stations/stations.${file}.arrow`, ArrowLoader)
            .then((data: StationMeta) => {
                console.log('setting station meta for', file, 'with', data.id.length, 'stations');
                setStationMeta(data);
            })
            .catch((e) => {
                if (e.message.match(/arrow \(404\)/)) {
                    // Fallback to the old style if it's not found
                    return load(`${props.env.NEXT_PUBLIC_DATA_URL ?? NEXT_PUBLIC_DATA_URL}stations.arrow`, ArrowLoader)
                        .then((data: StationMeta) => {
                            console.log('setting station meta', data.id.length, 'stations');
                            setStationMeta(data);
                        })
                        .catch((e) => {
                            console.log('Error loading stations.arrow (fallback)', e);
                        });
                }
            });
    }, [file, allStations]);

    return <StationMetaContext.Provider value={stationMeta}>{props.children}</StationMetaContext.Provider>;
}
