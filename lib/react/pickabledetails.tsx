export interface PickableH3Details {
    type: 'hexagon';
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

export interface PickableStationDetails {
    type: 'station';
    name: string;
    pos: [number, number];
    id: number;
}

export function getObjectFromIndex(i: number, layer: {props: {data: {d: any} | any}}): PickableH3Details | PickableStationDetails {
    const d = layer?.props?.data.d;
    if (d) {
        if ('h3lo' in d) {
            return {
                type: 'hexagon',
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
                s: d.stations?.[i] || '',
                t: d.numStations?.[i] ?? d.stations?.[i]?.split(',')?.length ?? 0,
                length: d.avgSig.length
            };
        }
        console.log('unexpected layer data', d);
        return null;
    } else if (layer?.props?.data && i < layer.props.data.length) {
        const dS = layer?.props?.data;
        return {
            type: 'station',
            name: dS.name[i],
            pos: [dS.lng[i], dS.lat[i]],
            id: dS.id[i]
        };
    }

    return null;
}
