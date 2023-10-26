export interface PickableH3Details {
    type: 'hexagon';
    i: number;
    h: [number, number]; //layer.props.data.h3s[i],
    h3: string;
    a: number;
    b: number;
    c: number;
    d: number;
    e: number;
    f: number;
    g: number;
    p: number;
    q: number;
    s: string;
    t: number;
    length: number;
}

export interface PickableStationDetails {
    type: 'station';
    name: string;
    pos: [number, number];
    id: number;
}

export type PickableDetails = PickableStationDetails | PickableH3Details | {type: 'none'};

import {prefixWithZeros} from '../common/prefixwithzeros';

import type {DisplayedH3sType} from './displayedh3s';
import {h3IndexToSplitLong} from 'h3-js';
import {sortedIndexOf as _sortedIndexOf, sortedLastIndex as _sortedLastIndex} from 'lodash';

export function getObjectFromH3s(displayedH3s: DisplayedH3sType, h3: string): PickableDetails {
    //    const displayedH3s = useDisplayedH3s();
    if (!displayedH3s.length) {
        return {type: 'none'};
    }

    const [h3lo, h3hi] = h3IndexToSplitLong(h3);

    // Find the first h3hi in the file
    const index = _sortedIndexOf(displayedH3s.d.h3hi, h3hi);
    // none found then it's not in the file
    if (index == -1) {
        return {type: 'none'};
    }

    // We now know the range it could be in
    const lastIndex = _sortedLastIndex(displayedH3s.d.h3hi, h3hi);

    // All the rows with h3hi
    const subset = displayedH3s.d.h3lo.subarray(index, lastIndex);

    // If one matches
    const subIndex = _sortedIndexOf(subset, h3lo);
    if (subIndex == -1) {
        return {type: 'none'};
    }

    // Actual index
    const matchIndex = subIndex + index;
    return getObjectFromIndex(matchIndex, {props: {data: {...displayedH3s}}});
}

export function getObjectFromIndex(i: number, layer: {props: {data: {d: any} | any}}): PickableDetails {
    const d = layer?.props?.data.d;
    if (d) {
        if ('h3lo' in d) {
            return {
                type: 'hexagon',
                i,
                h: [d.h3lo[i], d.h3hi[i]] as [number, number], //layer.props.data.h3s[i],
                h3: prefixWithZeros(8, d.h3hi[i].toString(16)) + prefixWithZeros(8, d.h3lo[i].toString(16)),
                a: d.avgSig[i],
                b: d.minAlt[i],
                c: d.count[i],
                d: d.minAltSig[i],
                e: d.maxSig[i],
                f: d.avgCrc[i],
                g: d.minAgl[i],
                p: d.avgGap[i],
                q: d.expectedGap?.[i],
                s: d.stations?.[i] || '',
                t: d.numStations?.[i] ?? d.stations?.[i]?.split(',')?.length ?? 0,
                length: d.avgSig.length
            };
        }
        console.log('unexpected layer data', d);
        return {type: 'none'};
    } else if (layer?.props?.data && i < layer.props.data.length) {
        const dS = layer?.props?.data;
        return {
            type: 'station',
            name: dS.name[i],
            pos: [dS.lng[i], dS.lat[i]],
            id: dS.id[i]
        };
    }

    return {type: 'none'};
}
