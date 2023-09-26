export interface PickableH3Details {
    i: number;
    h: [number, number]; //layer.props.data.h3s[i],
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

export interface PickableStationDetails {}

export function getObjectFromIndex(i: number, layer: {props: {data: {d: any} | any}}): PickableH3Details & PickableStationDetails {
    const d = layer?.props?.data.d;
    if (d) {
        return {
            i,
            h: [d.h3lo[i], d.h3hi[i]] as [number, number], //layer.props.data.h3s[i],
            a: d.avgSig[i],
            b: d.minAlt[i],
            c: d.count[i],
            d: d.minAltSig[i],
            e: d.maxSig[i],
            f: d.avgCrc[i],
            g: d.minAgl[i],
            p: d.avgGap[i],
            q: d.expectedGap?.[i],
            s: d.stations?.[i] || 0,
            t: d.numStations?.[i] ?? d.stations?.[i]?.split(',')?.length ?? 0,
            length: d.avgSig.length
        };
    }
    if (layer?.props?.data && i < layer.props.data.length) {
        return layer.props.data[i];
    }
    return null;
}
