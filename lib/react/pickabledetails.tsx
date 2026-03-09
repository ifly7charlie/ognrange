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

    // h3IndexToSplitLong returns signed int32 values; Uint32Array reads unsigned.
    // >>> 0 converts to unsigned so both sides of the comparison are consistent.
    const [h3lo_raw, h3hi_raw] = h3IndexToSplitLong(h3);
    const h3lo = h3lo_raw >>> 0;
    const h3hi = h3hi_raw >>> 0;

    // H3 cell indices always have h3hi < 2^31 (mode bits = 0x08), so signed/unsigned
    // are identical — binary search on Uint32Array is safe for h3hi.
    const index = _sortedIndexOf(displayedH3s.d.h3hi, h3hi);
    if (index == -1) {
        return {type: 'none'};
    }

    const lastIndex = _sortedLastIndex(displayedH3s.d.h3hi, h3hi);

    // h3lo values within a bucket are sorted by signed int32 order (as written by the
    // backend), but Uint32Array presents them as unsigned. For values with bit 31 set
    // the signed sort position differs from unsigned sort position, so binary search
    // would fail. Linear scan within the (typically small) bucket is always correct.
    const subset = displayedH3s.d.h3lo.subarray(index, lastIndex);
    let subIndex = -1;
    for (let k = 0; k < subset.length; k++) {
        if (subset[k] === h3lo) {
            subIndex = k;
            break;
        }
    }
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
