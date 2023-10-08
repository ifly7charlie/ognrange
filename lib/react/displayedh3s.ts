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

export let displayedH3s: ArrowFileType | undefined = undefined;

export function setDisplayedH3s(h3s: ArrowFileType) {
    displayedH3s = h3s;
}
