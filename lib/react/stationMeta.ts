export interface StationMeta {
    name: string[];
    lng: number[];
    lat: number[];
    id: number[];
}

export let stationMeta: StationMeta = undefined;

export function setStationMeta(sm: StationMeta) {
    stationMeta = sm;
}
