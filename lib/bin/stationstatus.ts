import {ClassicLevel} from 'classic-level';

import {Epoch, StationName, StationId, Latitude, Longitude} from './types';

// H3 hexagon cell library
import * as h3 from 'h3-js';

// Configuration
import {STATION_MOVE_THRESHOLD_KM, DB_PATH} from '../common/config';

import {reduce} from 'lodash';

export interface StationDetails {
    id: StationId;
    station: StationName;
    lat?: number;
    lng?: number;
    lastPacket?: Epoch; //epoch
    lastLocation?: Epoch; // epoch
    lastOutputFile?: Epoch;
    lastBeacon?: Epoch; // epoch
    status?: string;
    notice?: string;
    moved?: boolean;
    valid?: boolean;
    outputEpoch?: Epoch;
    outputDate?: string;
}

let stations: Record<StationName, StationDetails> = {}; // map from string to details
let stationIds: Record<StationId, StationName> = {}; // map from id to string
let statusDb: ClassicLevel<StationName, StationDetails> | undefined;

// We need to use a protected data structure to generate ids
// for the station ID. This allows us to use atomics, will also
// support clustering if we need it
const sabbuffer = new SharedArrayBuffer(2);
const nextStation = new Uint16Array(sabbuffer);

export function getNextStationId() {
    return Number(nextStation);
}

// Load the status of the current stations
export async function loadStationStatus() {
    console.log('loading station status');
    // Open the status database

    try {
        statusDb = new ClassicLevel<StationName, StationDetails>(DB_PATH + 'status', {valueEncoding: 'json'});
        await statusDb.open();

        for await (const [key, value] of statusDb.iterator()) {
            stations[key] = value;
            stationIds[stations[key].id] = key;
        }
    } catch (e) {
        console.log('Unable to loadStationStatus', e);
        process.exit(1);
    }

    // Figure out the next id and save it
    const nextid =
        (reduce(
            stations,
            (highest: number, i: StationDetails) => {
                return highest < (i.id || 0) ? i.id : highest;
            },
            0
        ) || 0) + 1;
    console.log('next station id', nextid);
    Atomics.store(nextStation, 0, nextid);
}

// Find the ID number for a station
export function getStationDetails(stationName: StationName, serialise = true): StationDetails {
    // Figure out which station we are
    let stationid = undefined;
    if (!stationName) {
        throw new Error('no station name provided');
    }

    if (!stations[stationName]) {
        stations[stationName] = {station: stationName, id: (stationid = Atomics.add(nextStation, 0, 1) as StationId)};
        stationIds[stationid] = stationName;
        console.log(`allocated id ${stationid} to ${stationName}, ${Object.keys(stations).length} in hash`);

        if (serialise && statusDb !== undefined) {
            statusDb.put(stationName, stations[stationName]);
        }
    }

    return stations[stationName];
}
export function allStationsDetails({includeGlobal}: {includeGlobal: boolean} = {includeGlobal: false}): StationDetails[] {
    const values = Object.values(stations);
    if (!includeGlobal) {
        values.shift();
    }
    return values;
}
export function allStationsNames({includeGlobal}: {includeGlobal: boolean} = {includeGlobal: false}): string[] {
    const values = Object.keys(stations);
    if (!includeGlobal) {
        values.shift();
    }
    return values;
}

export function getStationName(stationId: StationId): StationName | undefined {
    return stationId === 0 ? ('global' as StationName) : stationIds[stationId] || undefined;
}

// Check if we have moved too far ( a little wander is considered ok )
export function checkStationMoved(stationName: StationName, latitude: Latitude, longitude: Longitude, timestamp: Epoch): void {
    let details = getStationDetails(stationName);
    const distance = details.lat && details.lng ? h3.greatCircleDistance([details.lat, details.lng], [latitude, longitude], 'km') : 0;
    details.lat = latitude;
    details.lng = longitude;
    details.lastLocation = timestamp;

    // Did it move?
    if (distance > STATION_MOVE_THRESHOLD_KM) {
        details.notice = `${Math.round(distance)}km move detected ${new Date(timestamp * 1000).toISOString()} resetting history`;
        details.moved = true; // we need to persist this
        console.log(`station ${stationName} has moved location from ${details.lat},${details.lng} to ${latitude},${longitude} which is ${distance.toFixed(1)}km`);
        updateStationStatus(details);
    }
}

// Capture the beacon for status purposes
export function updateStationBeacon(stationName: StationName, body: string, timestamp: Epoch): void {
    let details = getStationDetails(stationName);
    details.lastBeacon = timestamp;
    details.status = body;
    updateStationStatus(details);
}

export function updateStationStatus(details: StationDetails): Promise<void> {
    return statusDb?.put(details.station, details) || new Promise<void>(() => {});
}

export async function closeStatusDb(): Promise<void> {
    return statusDb?.close() || new Promise<void>(() => {});
}
