//
// API Data Output structures
//

export type H3DetailsOutput = H3DetailsOutputStructure | {date: string}[];

export interface H3DetailsOutputStructure {
    date: string;
    avgGap: number;
    maxSig: number;
    avgSig: number;
    minAltSig: number;
    minAgl: number;
    count: number;
    expectedGap?: number;
}
