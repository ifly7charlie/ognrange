#!/usr/bin/env node

// Import the APRS server
import {ISSocket} from 'js-aprs-is';
import {aprsParser} from 'js-aprs-fap';

// Height above ground calculations, uses mapbox to get height for point
//import geo from './lib/getelevationoffset.js';
import {getCacheSize, getElevationOffset} from '../lib/bin/getelevationoffset.js';

import {mkdirSync} from 'fs';

import {ignoreStation} from '../lib/bin/ignorestation.js';

// DB Structure
import {CoverageRecord, bufferTypes} from '../lib/bin/coveragerecord.js';
import {CoverageHeader} from '../lib/bin/coverageheader.js';

import {gitVersion} from '../lib/bin/gitversion.js';

import {loadStationStatus, getStationId, checkStationMoved, updateStationBeacon, closeStatusDb, getNextStationId, stationDetails} from '../lib/bin/stationstatus';
import {closeAllStationDbs, initialiseStationDbCache, getStationDbCacheSize} from '../lib/bin/stationcache';

import {Epoch, StationName, Longitude, Latitude} from '../lib/bin/types';

// H3 hexagon cell library
import h3 from 'h3-js';

import _reduce from 'lodash.reduce';

let globalDb = undefined;

// track any setInterval/setTimeout calls so we can stop them when asked to exit
// also the async rollups we do during startup
let intervals = [];
let timeouts: Record<string, any> = {};
let startupPromise = null;

// APRS connection
let connection: any = {};

// Tracking aircraft so we can calculate gaps and double check for
// planes that have GPS on but are stationary and have poor
// coverage so jump a lot
let aircraftStation = new Map();
let allAircraft = new Map();

// PM2 Metrics
let metrics = undefined;

// shortcuts so regexp compiled once
const reExtractDb = / ([0-9.]+)dB /;
const reExtractCrc = / ([0-9])e /;
const reExtractRot = / [+-]([0-9.]+)rot /;
const reExtractVC = / [+-]([0-9]+)fpm /;

import {
    ROLLUP_PERIOD_MINUTES, //
    NEXT_PUBLIC_SITEURL,
    APRS_SERVER,
    APRS_TRAFFIC_FILTER,
    APRS_KEEPALIVE_PERIOD_MS,
    H3_CACHE_FLUSH_PERIOD_MS,
    FORGET_AIRCRAFT_AFTER_SEC,
    STATION_MOVE_THRESHOLD_KM,
    H3_STATION_CELL_LEVEL,
    H3_GLOBAL_CELL_LEVEL,
    DB_PATH,
    OUTPUT_PATH,
    STATION_EXPIRY_TIME_SECS,
    MAX_STATION_DBS,
    STATION_DB_EXPIRY_MS
} from '../lib/bin/config.js';

// h3 cache functions
import {flushDirtyH3s, updateCachedH3, getH3CacheSize} from '../lib/bin/h3cache';

// Rollup functions
import {rollupAll, rollupStartupAll, rollupStats} from '../lib/bin/rollup';
import {getAccumulator, getCurrentAccumulators, updateAndProcessAccumulators, initialiseAccumulators} from '../lib/bin/accumulators';

// Get our git version
const gv = gitVersion().trim();

let packetStats = {ignoredStation: 0, ignoredTracker: 0, ignoredStationary: 0, ignoredSignal0: 0, ignoredPAW: 0, ignoredH3stationary: 0, count: 0, rawCount: 0, pps: '', rawPps: ''};

// Run stuff magically
main().then(() => console.log('exiting'));

//
// Primary configuration loading and start the aprs receiver
async function main() {
    if (ROLLUP_PERIOD_MINUTES < 12) {
        console.log(`ROLLUP_PERIOD_MINUTES is too short, it must be more than 12 minutes`);
        process.exit();
    }

    console.log(`Configuration loaded DB@${DB_PATH} Output@${OUTPUT_PATH}, Version ${gv}`);

    // Make sure our paths exist
    try {
        mkdirSync(DB_PATH + 'stations', {recursive: true});
        mkdirSync(OUTPUT_PATH, {recursive: true});
    } catch (e) {}

    // Open our databases
    initialiseAccumulators();
    await loadStationStatus();
    initialiseStationDbCache();

    // Check and process unflushed accumulators at the start
    // then we can increment the current number for each accumulator merge
    await (startupPromise = rollupStartupAll());
    await (startupPromise = updateAndProcessAccumulators());
    startupPromise = null;

    // Start listening to APRS and setup the regular housekeeping functions
    startAprsListener();
    setupPeriodicFunctions();
}

//
// Tidily exit if the user requests it
// we need to stop receiving,
// output the current data, close any databases,
// and then kill of any timers
async function handleExit(signal) {
    console.log(`${signal}: flushing data`);
    if (connection) {
        connection.exiting = true;
        connection.disconnect && connection.disconnect();
    }

    if (startupPromise) {
        console.log('waiting for startup to finish');
        await startupPromise;
    }

    for (const i of intervals) {
        clearInterval(i);
    }
    for (const i of Object.values(timeouts)) {
        clearTimeout(i);
    }
    if (connection && connection.interval) {
        clearInterval(connection.interval);
    }

    // Flush everything to disk
    console.log(await flushDirtyH3s({allUnwritten: true, lockForRead: true}));
    if (getCurrentAccumulators()) {
        const current = getCurrentAccumulators();
        await rollupAll({current: current.currentAccumulator, processAccumulators: current.accumulators, newAccumulatorFiles: false});
    } else {
        console.log(`unable to output a rollup as service still starting`);
    }

    // Close all the databases and cleanly exit
    closeAllStationDbs();
    closeStatusDb();

    connection = null;
    console.log(`${signal}: done`);
}
process.on('SIGINT', handleExit);
process.on('SIGQUIT', handleExit);
process.on('SIGTERM', handleExit);

process.on('SIGINFO', displayStatus);

// dump out? not good idea really better to exit and restart
process.on('SIGUSR1', async function () {
    console.log('-- data dump requested --');
    await flushDirtyH3s({allUnwritten: true, lockForRead: true});
    if (getCurrentAccumulators()) {
        const current = getCurrentAccumulators();
        await rollupAll({current: current.currentAccumulator, processAccumulators: current.accumulators, newAccumulatorFiles: false});
    }
});

//
// Connect to the APRS Server
async function startAprsListener() {
    // Settings for connecting to the APRS server
    const CALLSIGN = NEXT_PUBLIC_SITEURL;
    const PASSCODE = -1;
    const [APRSSERVER, PORTNUMBER] = APRS_SERVER.split(':') || ['aprs.glidernet.org', '14580'];

    // If we were connected then cleanup the old stuff
    if (connection) {
        console.log(`reconnecting to ${APRSSERVER}:${PORTNUMBER}`);
        try {
            connection.disconnect();
            clearInterval(connection.interval);
        } catch (e) {}
        connection = null;
    }

    // Connect to the APRS server
    connection = new ISSocket(APRSSERVER, parseInt(PORTNUMBER) || 14580, 'OGNRANGE', 0, APRS_TRAFFIC_FILTER, `ognrange v${gv}`);
    let parser = new aprsParser();

    // Handle a connect
    connection.on('connect', () => {
        connection.sendLine(connection.userLogin);
        connection.sendLine(`# ognrange ${CALLSIGN} ${gv}`);
    });

    // Handle a data packet
    connection.on('packet', async function (data) {
        if (!connection || connection.exiting) {
            return;
        }
        connection.valid = true;
        if (data.charAt(0) != '#' && !data.startsWith('user')) {
            packetStats.rawCount++;
            const packet = parser.parseaprs(data);
            if ('latitude' in packet && 'longitude' in packet && 'comment' in packet && packet.comment?.substr(0, 2) == 'id') {
                processPacket(packet);
            } else {
                if ((packet.destCallsign == 'OGNSDR' || data.match(/qAC/)) && !ignoreStation(packet.sourceCallsign)) {
                    if (packet.type == 'location') {
                        checkStationMoved(packet.sourceCallsign as StationName, packet.latitude as Latitude, packet.longitude as Longitude, packet.timestamp as Epoch);
                    } else if (packet.type == 'status') {
                        updateStationBeacon(packet.sourceCallsign as StationName, packet.body, packet.timestamp as Epoch);
                    } else {
                        console.log(data, packet);
                    }
                }
            }
        } else {
            // Server keepalive
            console.log(data, '#', packetStats.rawCount);
            if (data.match(/aprsc/)) {
                connection.aprsc = data;
            }
        }
    });

    // Failed to connect, will create a new connection at the next periodic interval
    connection.on('error', (err) => {
        if (!connection.exiting) {
            console.log('Error: ' + err);
            connection.disconnect();
            connection.valid = false;
        }
    });

    if (!connection || connection.exiting) {
        return;
    }

    // Start the APRS connection
    connection.connect();

    // And every (APRS_KEEPALIVE_PERIOD) minutes we need to confirm the APRS
    // connection has had some traffic, and reconnect if not
    connection.interval = setInterval(function () {
        try {
            // Send APRS keep alive or we will get dumped
            connection.sendLine(`# ${CALLSIGN} ${gv}`);
        } catch (e) {
            console.log(`exception ${e} in sendLine status`);
        }

        // Re-establish the APRS connection if we haven't had anything in
        if (!connection || ((!connection.isConnected() || !connection.valid) && !connection.exiting)) {
            console.log('failed APRS connection, retrying');
            try {
                connection.disconnect();
            } catch (e) {}
            // We want to restart the APRS listener if this happens
            startAprsListener();
        }
        if (connection) {
            connection.valid = false; // reset by receiving a packet
        }
    }, APRS_KEEPALIVE_PERIOD_MS);
}

//
// We have a series of different tasks that need to be done on a
// regular basis, they can all persist through a reconnection of
// the APRS server
async function setupPeriodicFunctions() {
    let lastPacketCount = 0;
    let lastRawPacketCount = 0;
    let lastH3length = 0;

    // We also need to flush our h3 cache to disk on a regular basis
    // this is used as an opportunity to display some statistics
    intervals.push(
        setInterval(async function () {
            // Flush the cache
            const flushStats = await flushDirtyH3s({allUnwritten: false});

            // Report some status on that
            const packets = packetStats.count - lastPacketCount;
            const rawPackets = packetStats.rawCount - lastRawPacketCount;
            const pps = (packetStats.pps = (packets / (H3_CACHE_FLUSH_PERIOD_MS / 1000)).toFixed(1));
            const rawPps = (packetStats.rawPps = (rawPackets / (H3_CACHE_FLUSH_PERIOD_MS / 1000)).toFixed(1));
            const h3length = flushStats.total;
            const h3delta = h3length - lastH3length;
            const h3expired = flushStats.expired;
            const h3written = flushStats.written;
            console.log(`elevation cache: ${getCacheSize()}, valid packets: ${packets} ${pps}/s, all packets ${rawPackets} ${rawPps}/s`);
            console.log(`total stations: ${getNextStationId() - 1}, openDbs: ${getStationDbCacheSize()}/${MAX_STATION_DBS}`);
            console.log(JSON.stringify(packetStats));
            console.log(JSON.stringify(rollupStats));
            console.log(`h3s: ${h3length} delta ${h3delta} (${((h3delta * 100) / h3length).toFixed(0)}%): `, ` expired ${h3expired} (${((h3expired * 100) / h3length).toFixed(0)}%), written ${h3written} (${((h3written * 100) / h3length).toFixed(0)}%)[${flushStats.databases} stations]`, ` ${((h3written * 100) / packets).toFixed(1)}% ${(h3written / (H3_CACHE_FLUSH_PERIOD_MS / 1000)).toFixed(1)}/s ${(packets / h3written).toFixed(0)}:1`);

            // Although this isn't an error it does mean that there will be churn in the DB cache and
            // that will increase load - which is not ideal because we are obviously busy otherwise we wouldn't have
            // so many stations sending us traffic...
            if (flushStats.databases > MAX_STATION_DBS * 0.9) {
                console.log(`** please increase the database cache (MAX_STATION_DBS) it should be larger than the number of stations receiving traffic in H3_CACHE_FLUSH_PERIOD_MINUTES`);
            }

            // purge and flush H3s to disk
            // carry forward state for stats next time round
            lastPacketCount = packetStats.count;
            lastRawPacketCount = packetStats.rawCount;
            lastH3length = h3length;
        }, H3_CACHE_FLUSH_PERIOD_MS)
    );

    timeouts['forget'] = setTimeout(() => {
        delete timeouts['forget'];
        intervals.push(
            setInterval(async function () {
                const purgeBefore = Date.now() / 1000 - FORGET_AIRCRAFT_AFTER_SEC;
                const purgeH3sBefore = Date.now() / 1000 - 3600; // 1 hour
                let total = allAircraft.size;

                allAircraft.forEach((aircraft, key) => {
                    if (aircraft.seen < purgeBefore) {
                        allAircraft.delete(key);
                    }
                });
                aircraftStation.forEach((timestamp, key) => {
                    if (timestamp < purgeBefore) {
                        aircraftStation.delete(key);
                    }
                });

                let purged = total - allAircraft.size;
                console.log(`purged ${purged} aircraft from gap map, ${allAircraft.size} remaining`);
            }, 3600 * 1000)
        ); // every hour we do this
    }, (FORGET_AIRCRAFT_AFTER_SEC + Math.random() * 300) * 1000);

    // Make sure our accumulator is correct, this will also
    // ensure we rollover and produce output files correctly
    const now = new Date();
    const nextRollup = ROLLUP_PERIOD_MINUTES - ((now.getUTCHours() * 60 + now.getUTCMinutes()) % ROLLUP_PERIOD_MINUTES);
    console.log(`first rollup will be in ${nextRollup} minutes at ${new Date(Date.now() + nextRollup * 60000 + 500).toISOString()}`);
    timeouts['rollup'] = setTimeout(async function () {
        delete timeouts['rollup'];
        intervals.push(
            setInterval(async function () {
                updateAndProcessAccumulators();
                console.log(`next rollup will be in ${ROLLUP_PERIOD_MINUTES} minutes at ` + `${new Date(Date.now() + ROLLUP_PERIOD_MINUTES * 60000 + 500).toISOString()}`);
            }, ROLLUP_PERIOD_MINUTES * 60 * 1000)
        );
        // this shouldn't drift because it's an interval...
        updateAndProcessAccumulators(); // do the first one, then let the interval do them afterwards
    }, nextRollup * 60 * 1000 + 500);
    // how long till they roll over, delayed 1/2 a second + whatever remainder was left in getUTCSeconds()...
    // better a little late than too early as it won't rollover then and we will wait a whole period to pick it up
}

function displayStatus() {
    console.log(`elevation cache: ${getCacheSize()}, h3cache: ${getH3CacheSize()},  valid packets: ${packetStats.count} ${packetStats.pps}/s, all packets ${packetStats.rawCount} ${packetStats.rawPps}/s`);
    console.log(`total stations: ${getNextStationId() - 1}, openDbs: ${getStationDbCacheSize() + 2}/${MAX_STATION_DBS}`);
    console.log(JSON.stringify(packetStats));
}

//
// collect points, emit to competition db every 30 seconds
async function processPacket(packet) {
    // Count this packet into pm2
    metrics?.ognPerSecond?.mark();

    // Flarm ID we use is last 6 characters, check if OGN tracker or regular flarm
    const flarmId = packet.sourceCallsign.slice(packet.sourceCallsign.length - 6);
    const pawTracker = packet.sourceCallsign.slice(0, 3) == 'PAW';

    // Lookup the altitude adjustment for the
    const station = packet.digipeaters?.pop()?.callsign || 'unknown';

    // Obvious reasons to ignore stations
    if (ignoreStation(station)) {
        packetStats.ignoredStation++;
        return;
    }
    if (packet.destCallsign == 'OGNTRK' && packet.digipeaters?.[0]?.callsign?.slice(0, 2) != 'qA') {
        packetStats.ignoredTracker++;
        return;
    }
    if (pawTracker) {
        packetStats.ignoredPAW++;
        return;
    }

    let altitude = Math.floor(packet.altitude);

    // Proxy for the plane
    let aircraft = allAircraft.get(flarmId);
    if (!aircraft) {
        allAircraft.set(flarmId, (aircraft = {h3s: new Set(), first: packet.timestamp, packets: 0, seen: 0}));
    }

    // Make sure they are moving... we can get this from the packet without any
    // vertical speed of 30 is ~0.5feet per second or ~15cm/sec and I'm guessing
    // helicopters can't hover that precisely. NOTE this threshold is not 0 because
    // the roc jumps a lot in the packet stream.
    if (packet.speed < 1) {
        const rawRot = (packet.comment.match(reExtractRot) || [0, 0])[1];
        const rawVC = (packet.comment.match(reExtractVC) || [0, 0])[1];
        if (rawRot == 0.0 && rawVC < 30) {
            packetStats.ignoredStationary++;
            aircraft.seen = packet.timestamp;
            return;
        }
    }

    // If we have a gap then we will capture this (it was from a previous record but only time
    // that is an issue is when rolling aggregators - at which point we have reset aircraftStation
    // anyway (IS IT??)
    //
    // The goal is to have some kind of shading that indicates how reliable packet reception is
    // which is to  a little to do with how many packets are received.
    let gap;
    let first = false;
    {
        const gs = station + '/' + flarmId;
        const seen = aircraft.seen;
        const when = aircraftStation.get(gs);
        gap = when ? Math.min(60, packet.timestamp - when) : Math.min(60, Math.max(1, packet.timestamp - (seen || packet.timestamp)));
        aircraftStation.set(gs, packet.timestamp);
        if (aircraft.seen < packet.timestamp) {
            aircraft.seen = packet.timestamp;
            first = true;
        }
    }

    // A GPS device indoors in poor coverage may report movement even though it is
    // not. Over a period of a year of sending 1pps it this alone is enough traffic
    // to overflow the counters. As we are generally excluding stationary devices
    // this is a bit of extra logic to identify devices that remain in a small
    // set of cells for a lot of points. Most stationary devices will be picked up
    // by the normal logic, this is basically a long term backup plan to stop
    // overflow caused by planes in 'hangers'
    //
    // store in a set until we have 4 and then  we can zap that (less private info stored)
    // 9 is 174m/side or an area of 0.1sqkm - h3 could be about
    // 2xedgeLength in width so that's about 350m, so this is max 10kph at 120s/ per h3.
    // 10 is 65m/side so 130m/120s => 3.9kph
    // (4 => allows to jump over side as hexagon no point is near more than
    //       3 other hexagons)
    //
    // we could check to see if they are adjacent but I don't think that is necessary
    // as any aircraft that is actually moving will eventually end up with more h3s
    //
    // also, helicopters?! may miss hovers for start but should not skip anything
    // afterwards.
    //
    if (first) {
        aircraft.h3s.add(h3.geoToH3(packet.latitude, packet.longitude, 10));
        if (aircraft.h3s.size > 4) {
            // remove oldest (first in set is always the earliest added)
            // also reset count as there has been a change to h3s so may
            // be moving - this is less likely to false positive
            aircraft.h3s.delete(aircraft.h3s.values().next().value);
            aircraft.packets = 0;
        } else {
            aircraft.packets++;
        }

        // 90 first points per h3 is enough to trigger
        // stationary, that basically means they haven't moved in
        // at least 90 seconds, but more like 90*5 seconds
        const s = aircraft.h3s.size;
        if (aircraft.packets / s > 90) {
            packetStats.ignoredH3stationary++;
            return;
        }
    }

    // Look for signal strength and checksum - we will ignore any packet without a signal strength
    // sometimes this happens to be missing and other times it happens because it is reported as 0.0
    const rawSignalStrength = (packet.comment.match(reExtractDb) || [0, 0])[1];
    const signal = Math.min(Math.round(parseFloat(rawSignalStrength) * 4), 255);

    // crc may be absent, if it is then it's a 0
    const crc = parseInt((packet.comment.match(reExtractCrc) || [0, 0])[1]);

    // If we have no signal strength then we'll ignore the packet... don't know where these
    // come from or why they exist...
    if (signal <= 0) {
        packetStats.ignoredSignal0++;
        return;
    }

    packetStats.count++;

    // Enrich with elevation and send to everybody, this is async
    // and we don't need it's results to say we logged the packet
    getElevationOffset(packet.latitude, packet.longitude, async (gl) => {
        const agl = Math.round(Math.max(altitude - gl, 0));

        // Find the id for the station or allocate
        const stationid = await getStationId(station);

        // Packet for station marks it for dumping next time round
        stationDetails(station).lastPacket = packet.timestamp;

        // What hexagon are we working with
        const h3id = h3.geoToH3(packet.latitude, packet.longitude, H3_STATION_CELL_LEVEL);

        //
        // We store the database records as binary bytes - in the format described in the mapping() above
        // this reduces the amount of storage we need and means we aren't constantly parsing text
        // and printing text.
        async function mergeDataIntoDatabase(db: StationName, lockKeyStationId, h3) {
            // Header details for our update
            const h3k = new CoverageHeader(lockKeyStationId, ...getAccumulator(), h3);

            // And tell the cache to fetch/update - this may block if it needs to read
            // and there is nothing yet available
            updateCachedH3(db, h3k, altitude, agl, crc, signal, gap, stationid);
        }

        // Merge into both the station db (0,0) and the global db with the stationid we allocated
        // we don't pass stationid into the station specific db because there only ever is one
        // it gets used to build the list of stations that can see the cell
        mergeDataIntoDatabase(station, stationid, h3id);

        mergeDataIntoDatabase('global' as StationName, 0, h3.h3ToParent(h3id, H3_GLOBAL_CELL_LEVEL));
    });
}
