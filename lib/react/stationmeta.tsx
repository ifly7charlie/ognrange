import {NEXT_PUBLIC_DATA_URL} from '../common/config';

import {createContext, useContext, useEffect, useMemo, useCallback, useState, useRef} from 'react';
import {useSearchParams} from 'next/navigation';

import {ArrowLoader} from '@loaders.gl/arrow';
import {load} from '@loaders.gl/core';

import {Layer, LAYER_BIT, layerMaskFromSet, ALL_LAYER_NAMES} from '../common/layers';

interface StationMetaContextValue {
    filteredList: StationMeta;
    unfilteredList: StationMeta | null;
}

const StationMetaContext = createContext<StationMetaContextValue | null>(null);

export interface StationMeta {
    name: string[];
    lng: Float32Array;
    lat: Float32Array;
    id: Uint32Array;
    valid?: boolean[];
    lastPacket?: Uint32Array;
    layerMask?: Uint8Array;

    length: number;
}

export interface SingleStationMeta {
    name: string;
    lat: number;
    lng: number;
    id: number;
    layerMask: number;
}

/** Returns the filtered station list (filtered by selected layers and online/offline) */
export function useStationListMeta() {
    return useContext(StationMetaContext)?.filteredList;
}

/** Returns the unfiltered station list — use for ID lookups where any station may be referenced */
export function useStationListMetaUnfiltered() {
    return useContext(StationMetaContext)?.unfilteredList;
}

/** Look up a single station by name from the unfiltered data */
export function useStationMeta(stationName: string): SingleStationMeta | null {
    const ctx = useContext(StationMetaContext);
    return useMemo(() => {
        if (!stationName || !ctx?.unfilteredList) return null;
        const raw = ctx.unfilteredList;
        const idx = raw.name.indexOf(stationName);
        if (idx === -1) return null;
        const mask = raw.layerMask ? raw.layerMask[idx] : 0;
        return {
            name: raw.name[idx],
            lat: raw.lat[idx],
            lng: raw.lng[idx],
            id: raw.id[idx],
            layerMask: mask || (1 << LAYER_BIT[Layer.COMBINED])
        };
    }, [stationName, ctx?.unfilteredList]);
}

export function StationMeta(props: React.PropsWithChildren<{env: {NEXT_PUBLIC_DATA_URL: string}}>) {
    // So we can load a suitable file for this
    const params = useSearchParams();
    const file = params.get('file')?.toString() ?? 'year'; // default if no file is current year
    const allStations = parseInt(params.get('allStations')?.toString() ?? '0');
    const layersParam = params.get('layers')?.toString();

    // Compute selected layer bitmask from URL params
    const selectedLayerMask = useMemo(() => {
        if (!layersParam) return null; // no filter — show all
        const layerValues = layersParam
            .split(',')
            .map((s) => s.trim())
            .filter((s) => ALL_LAYER_NAMES.has(s)) as Layer[];
        if (layerValues.length === 0) return null;
        return layerMaskFromSet(layerValues);
    }, [layersParam]);

    // Raw data from the arrow file (unfiltered)
    const [unfilteredList, setUnfilteredList] = useState<StationMeta | null>(null);
    const unfilteredListRef = useRef<StationMeta | null>(null);

    // What has been loaded (filtered)
    const [filteredList, setFilteredList] = useState<StationMeta>(() => ({
        name: [], //
        lng: new Float32Array(),
        lat: new Float32Array(),
        id: new Uint32Array(),
        valid: [],
        length: 0
    }));

    // Apply both valid and layer filters to raw data
    const applyFilters = useCallback(
        (data: StationMeta) => {
            const combinedBit = 1 << LAYER_BIT[Layer.COMBINED];

            // Build a combined filter predicate
            const passesFilter = (_value: any, index: number): boolean => {
                // Valid filter
                if (!allStations && data.valid && !data.valid[index]) return false;
                // Layer filter
                if (selectedLayerMask !== null) {
                    const mask = data.layerMask ? data.layerMask[index] : 0;
                    const effectiveMask = mask === 0 ? combinedBit : mask;
                    if ((effectiveMask & selectedLayerMask) === 0) return false;
                }
                return true;
            };

            const filteredData = {
                name: data.name.filter(passesFilter),
                lng: data.lng.filter(passesFilter),
                lat: data.lat.filter(passesFilter),
                id: data.id.filter(passesFilter),
                valid: data.valid?.filter(passesFilter),
                layerMask: data.layerMask?.filter(passesFilter)
            };
            setFilteredList({...filteredData, length: filteredData.id.length});
        },
        [allStations, selectedLayerMask]
    );

    // Re-apply filters when allStations or selectedLayerMask changes
    useEffect(() => {
        if (unfilteredListRef.current) {
            applyFilters(unfilteredListRef.current);
        }
    }, [applyFilters]);

    useEffect(() => {
        load(`${props.env.NEXT_PUBLIC_DATA_URL ?? NEXT_PUBLIC_DATA_URL}stations/stations.${file}.arrow`, ArrowLoader)
            .then((result) => {
                const data = (result as any).data as StationMeta;
                console.log('setting station meta for', file, 'with', data.id.length, 'stations');
                unfilteredListRef.current = data;
                setUnfilteredList(data);
                applyFilters(data);
            })
            .catch((e) => {
                if (e.message.match(/arrow \(404\)/)) {
                    // Fallback to the old style if it's not found
                    return load(`${props.env.NEXT_PUBLIC_DATA_URL ?? NEXT_PUBLIC_DATA_URL}stations.arrow`, ArrowLoader)
                        .then((result) => {
                            const data = (result as any).data as StationMeta;
                            console.log('setting station meta', data.id.length, 'stations');
                            unfilteredListRef.current = data;
                            setUnfilteredList(data);
                            applyFilters(data);
                        })
                        .catch((e) => {
                            console.log('Error loading stations.arrow (fallback)', e);
                        });
                }
            });
    }, [file]);

    const contextValue = useMemo(() => ({filteredList, unfilteredList}), [filteredList, unfilteredList]);

    return <StationMetaContext.Provider value={contextValue}>{props.children}</StationMetaContext.Provider>;
}
