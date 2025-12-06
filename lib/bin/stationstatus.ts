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
    previous_location?: [number, number];
    primary_location?: [number, number];
    lastPacket?: Epoch; //epoch
    lastLocation?: Epoch; // epoch
    lastOutputFile?: Epoch;
    lastBeacon?: Epoch; // epoch
    status?: string;
    notice?: string;
    moved?: boolean;
    bouncing?: boolean;
    valid?: boolean;
    outputEpoch?: Epoch;
    outputDate?: string;

    stats: {
        ignoredPAW: number; // we ignore from PAW devices
        ignoredTracker: number; //  OGNTRK or not sent to qA first (ie repeated)
        invalidTracker: number; // invalid flarmid
        invalidTimestamp: number; // no timestamp in message
        ignoredStationary: number; // device not moving at all
        ignoredSignal0: number; // no signal strength
        ignoredH3stationary: number; // device jumping between only a few locations
        ignoredElevation: number; // unable to determine elevation of coordinates
        count: number; // total packets
    };
}

function emptyStats() {
    return {
        ignoredTracker: 0, //
        invalidTracker: 0,
        invalidTimestamp: 0,
        ignoredStationary: 0,
        ignoredSignal0: 0,
        ignoredPAW: 0,
        ignoredH3stationary: 0,
        ignoredElevation: 0,
        count: 0
    };
}

const stations: Map<StationName, StationDetails> = new Map<StationName, StationDetails>(); // map from string to details
const stationIds: Map<StationId, StationName> = new Map<StationId, StationName>(); // map from id to string
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

        for await (const [name, details] of statusDb.iterator()) {
            details.stats ??= emptyStats();
            stations.set(name, details);
            stationIds.set(details.id, name);
        }
        1;
    } catch (e) {
        console.log('Unable to loadStationStatus', e);
        process.exit(1);
    }

    // Figure out the next id and save it
    const nextid =
        (reduce(
            [...stations.values()],
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

    let details = stations.get(stationName);

    if (!details) {
        details = {
            station: stationName, //
            id: (stationid = Atomics.add(nextStation, 0, 1) as StationId),
            stats: emptyStats()
        };

        stations.set(stationName, details);
        stationIds.set(stationid, stationName);
        console.log(`allocated id ${stationid} to ${stationName}, ${stations.size} have metadata`);

        if (serialise && statusDb !== undefined) {
            statusDb.put(stationName, details);
        }
    }

    return details;
}
export function allStationsDetails({includeGlobal}: {includeGlobal: boolean} = {includeGlobal: false}): StationDetails[] {
    const values = [...stations.values()];
    if (includeGlobal) {
        values.unshift({station: 'global' as StationName, id: 0 as StationId, stats: emptyStats()});
    }
    return values;
}
export function allStationsNames({includeGlobal}: {includeGlobal: boolean} = {includeGlobal: false}): string[] {
    const values = [...stations.keys()];
    if (includeGlobal) {
        values.unshift('global' as StationName);
    }
    return values;
}

export function getStationName(stationId: StationId): StationName | undefined {
    return stationId === 0 ? ('global' as StationName) : stationIds.get(stationId) || undefined;
}

// Check if we have moved too far ( a little wander is considered ok )
export function checkStationMoved(stationName: StationName, latitude: Latitude, longitude: Longitude, timestamp: Epoch, packet: string): void {
    let details = getStationDetails(stationName);

    if (!details.primary_location) {
        details.primary_location = [latitude, longitude];
    }
    if (!details.previous_location) {
        details.previous_location = details.lat !== undefined && details.lng !== undefined ? [details.lat, details.lng] : details.primary_location;
    }

    const distance = h3.greatCircleDistance(details.primary_location, [latitude, longitude], 'km');

    // Did it move?
    if (distance > STATION_MOVE_THRESHOLD_KM) {
        // How far from previous position? some stations get duplicate names so this might help catch it
        const previous_distance = h3.greatCircleDistance(details.previous_location, [latitude, longitude], 'km');
        if (previous_distance > STATION_MOVE_THRESHOLD_KM) {
            details.notice = `${Math.round(distance)}km move detected ${new Date(timestamp * 1000).toISOString()} resetting history`;
            console.log(packet);
            console.log(`station ${stationName} has moved location to ${latitude},${longitude} which is ${distance.toFixed(1)}km ${JSON.stringify(details, null, 4)}`);
            details.moved = true; // we need to persist this, and relock on new location
            details.previous_location = details.primary_location;
            delete details.bouncing;
            details.primary_location = [latitude, longitude];
        } else if (previous_distance > 0.1) {
            details.notice = 'station appears to be in motion, resetting history';
            console.log(stationName, details.notice);
            details.moved = true; // we need to persist this, and relock on new location
            delete details.bouncing;
            details.primary_location = [latitude, longitude];
        } else {
            console.log(packet);
            console.log(`station ${stationName} bouncing between two locations ${h3.greatCircleDistance(details.previous_location, details.primary_location, 'km').toFixed(1)}km (merging)`);
            details.notice = 'station appears to be bouncing between two locations, merging data';
            delete details.moved;
            details.bouncing = true;
        }
    } else {
        if (distance > 0.1) {
            details.notice = `small ${distance.toFixed(1)}km move detected ${new Date(timestamp * 1000).toISOString()} keeping history`;
        } else {
            details.notice = '';
        }
        delete details.bouncing;
    }
    details.lat = latitude;
    details.lng = longitude;
    details.lastLocation = timestamp;
    updateStationStatus(details);
}

// Capture the beacon for status purposes
export function updateStationBeacon(stationName: StationName, body: string, timestamp: Epoch): void {
    let details = getStationDetails(stationName);
    details.lastBeacon = timestamp;
    details.status = body;
    updateStationStatus(details);
}

export function updateStationStatus(details: StationDetails): Promise<void> {
    return statusDb?.put(details.station, details) ?? Promise.resolve();
}

export async function closeStatusDb(): Promise<void> {
    // Make sure we save all of the lastPackets for the stations
    for (const [k, v] of stations) {
        if (v.lastBeacon && v.lastPacket && v.lastBeacon < v.lastPacket) {
            await statusDb?.put(k, v);
        }
    }

    // And now close it
    return statusDb?.close() ?? Promise.resolve();
}
