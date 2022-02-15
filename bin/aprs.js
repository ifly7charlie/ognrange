#!/usr/bin/env node

// Import the APRS server
import { ISSocket } from 'js-aprs-is';
import { aprsParser } from  'js-aprs-fap';

// Correction factors
//import { altitudeOffsetAdjust } from '../offsets.js';
//import { getOffset } from '../egm96.mjs';

// Height above ground calculations, uses mapbox to get height for point
//import geo from './lib/getelevationoffset.js';
import { getCacheSize, getElevationOffset } from '../lib/bin/getelevationoffset.js'

import LevelUP from 'levelup';
import LevelDOWN from 'leveldown';

import dotenv from 'dotenv';

import { ignoreStation } from '../lib/bin/ignorestation.js'

import { CoverageRecord, bufferTypes } from '../lib/bin/coveragerecord.js';
import { CoverageHeader, accumulatorTypes } from '../lib/bin/coverageheader.js';
import { prefixWithZeros } from '../lib/bin/prefixwithzeros.js';

import { mapAllCapped } from '../lib/bin/mapallcapped.js';

import h3 from 'h3-js';

import _map from 'lodash.map';
import _reduce from 'lodash.reduce';
import _sortby from 'lodash.sortby';
import _isequal from 'lodash.isequal';
import _clonedeep from 'lodash.clonedeep';
import _filter from 'lodash.filter';

// DB locking
import { Lock } from 'lock'
let lock = Lock();

let stations = {};
let globalDb = undefined;
let statusDb = undefined;

// Least Recently Used cache for Station Database connectiosn 
import LRU from 'lru-cache'
const options = { max: parseInt(process.env.MAX_STATION_DBS)||3200,
				  dispose: function (db, key, r) {
					  try { db.close(); } catch (e) { console.log('ummm',e); }
					  if( stationDbCache.getTtl(key) < h3CacheFlushPeriod ) {
						  console.log( `Closing database ${key} while it's still needed. You should increase MAX_STATION_DBS in .env.local` );
					  }
				  },
				  updateAgeOnGet: true, allowStale: true,
				  ttl: (process.env.STATION_DB_EXPIRY_HOURS||12) * 3600 * 1000 }
	, stationDbCache = new LRU(options)

stationDbCache.getTtl = (k) => {
	return (typeof performance === 'object' && performance &&
	 typeof performance.now === 'function' ? performance : Date).now() - stationDbCache.starts[stationDbCache.keyMap.get(k)]
}

// track any setInterval/setTimeout calls so we can stop them when asked to exit
let intervals = [];
let timeouts = [];

// APRS connection
let connection = {};
let aircraftStation = new Map();
let aircraftSeen = new Map();
let validStations = new Set();

// PM2 Metrics
let metrics = undefined;

// We serialise to disk using protobuf
//import protobuf from 'protobufjs/light.js';
//import { OnglideRangeMessage } from '../lib/range-protobuf.mjs';
import { writeFile, mkdirSync, unlinkSync, symlinkSync } from 'fs';


// Default paths, can be overloaded using .env.local
let dbPath = './db/';
let outputPath = './public/data/';

// shortcuts so regexp compiled once
const reExtractDb = / ([0-9.]+)dB /;
const reExtractCrc = / ([0-9])c /;
const reExtractRot = / [+-]([0-9.]+)rot /;
const reExtractVC = / [+-]([0-9]+)fpm /;

// Cache so we aren't constantly reading/writing from the db
let cachedH3s = new Map();

// APRS Server Keep Alive
const aprsKeepAlivePeriod = (process.env.APRS_KEEPALIVE_PERIOD_MINUTES||2) * 60 * 1000;

/*
# Cache control - we cache the datablocks by station and h3 to save us needing
# to read/write them from/to the DB constantly. Note that this can use quite a lot
# of memory, but is a lot easier on the computer (in MINUTES)
# - flush period is how long they need to have been unused to be written
#   it is also the period of time between checks for flushing. Increasing this
#   will reduce the number of DB writes when there are lots of points being
#   tracked
# - MAXIMUM_DIRTY_PERIOD ensures that they will be written at least this often
# - expirytime is how long they can remain in memory without being purged. If it
#   is in memory then it will be used rather than reading from the db.
#   purges happen normally at flush period intervals (so 5 and 16 really it will
#   be flushed at the flush run at 20min)
*/
const h3CacheFlushPeriod = (process.env.H3_CACHE_FLUSH_PERIOD_MINUTES||1)*60*1000;
const h3CacheExpiryTime = (process.env.H3_CACHE_EXPIRY_TIME_MINUTES||4)*60*1000;
const h3CacheMaximumDirtyPeriod = (process.env.H3_CACHE_MAXIMUM_DIRTY_PERIOD_MINUTES||30)*60*1000;

/*
# ROLLUP is when the current accumulators are merged with the daily/monthly/annual
# accumulators. All are done at the same time and the accumulators are 'rolled'
# over to prevent double counting. This is a fairly costly activity so if the
# disk or cpu load goes too high during this process (it potentially reads and 
# writes EVERYTHING in every database) you should increase this number */
const h3RollupPeriod = (process.env.ROLLUP_PERIOD_HOURS||3)*3600*1000;
const ACCUMULATOR_CHANGEOVER_CHECK_PERIOD_MS = (process.env.ACCUMULATOR_CHANGEOVER_CHECK_PERIOD_SECONDS||60)*1000;

// how much detail to collect, bigger numbers = more cells! goes up fast see
// https://h3geo.org/docs/core-library/restable for what the sizes mean
const H3_STATION_CELL_LEVEL = (process.env.H3_STATION_CELL_LEVEL||8);
const H3_GLOBAL_CELL_LEVEL = (process.env.H3_GLOBAL_CELL_LEVEL||7);

// # We keep maps of when we last saw aircraft and where so we can determine the
// # timegap prior to the packet, this is sort of a proxy for the 'edge' of coverage
// # however we don't need to know this forever so we should forget them after
// # a while. The signfigence of forgetting is we will assume no gap before the
// # first packet for the first aircraft/station pair. Doesn't start running
// # until approximately this many hours have passed
const FORGET_AIRCRAFT_AFTER_SEC = (process.env.FORGET_AIRCRAFT_AFTER_HOURS||12)*3600;

// How far a station is allowed to move without resetting the history for it
const STATION_MOVE_THRESHOLD_KM = (process.env.STATION_MOVE_THRESHOLD_KM||2);

// If we haven't had traffic in this long then we expire the station
const STATION_EXPIRY_TIME_SECS = (process.env.STATION_EXPIRY_TIME_DAYS||31)*3600*24;

// We need to use a protected data structure to generate ids
// for the station ID. This allows us to use atomics, will also
// support clustering if we need it
const sabbuffer = new SharedArrayBuffer(2);
const nextStation = new Uint16Array(sabbuffer);
let packetStats = { ignoredStation: 0, ignoredTracker:0, ignoredStationary:0, ignoredSignal0:0, ignoredPAW:0, count: 0, rawCount:0 };
let rollupStats = { completed: 0, elapsed: 0 };

let currentAccumulator = undefined; 
let accumulators = {};

function getAccumulator() { return currentAccumulator; }

// Run stuff magically
main()
    .then("exiting");

//
// Primary configuration loading and start the aprs receiver
async function main() {

	// Load the configuration from a file
	dotenv.config({ path: '.env.local' })

	dbPath = process.env.DB_PATH||dbPath;
	outputPath = process.env.OUTPUT_PATH||outputPath;	

	console.log( `Configuration loaded DB@${dbPath} Output@${outputPath}` );

	if( !outputPath.match(/\/$/)) {
		outputPath += '/';
	}
	if( !dbPath.match(/\/$/)) {
		dbPath += '/';
	}

	// Make sure our paths exist
	try { 
		mkdirSync(dbPath+'stations', {recursive:true});
		mkdirSync(outputPath, {recursive:true});
	} catch(e) {};

	// Open our databases
	globalDb = LevelUP(LevelDOWN(dbPath+'global'))
	statusDb = LevelUP(LevelDOWN(dbPath+'status'))

	// And start listening for messages
	await startAprsListener();
}

//
// Tidily exit if the user requests it
// we need to stop receiving,
// output the current data, close any databases,
// and then kill of any timers
async function handleExit(signal) {
	console.log(`${signal}: flushing data`)
	connection.exiting = true;
	connection.disconnect();
	for( const i of intervals ) {
		clearInterval(i);
	}
	for( const i of timeouts ) {
		clearTimeout(i);
	}

	// Flush everything to disk
	await flushDirtyH3s(true);
	await rollupAll( currentAccumulator, accumulators );

	// Close all the databases and cleanly exit
	stationDbCache.forEach( async function (db,key) {
		db.close();
	});
	globalDb.close();
	statusDb.close();
	connection = null;
	console.log(`${signal}: done`)
}
process.on('SIGINT', handleExit);
process.on('SIGQUIT', handleExit);
process.on('SIGTERM', handleExit);

process.on('SIGINFO', displayStatus);
process.on('SIGUSR1', async function () {
	console.log( '-- data dump requested --' );
	await flushDirtyH3s(true); rollupAll( currentAccumulator, accumulators ); })
//
// Connect to the APRS Server
async function startAprsListener( m = undefined ) {

	// In case we are using pm2 metrics
	metrics = m;
	let lastPacketCount = 0;
	let lastRawPacketCount = 0;
	let lastH3length = 0;

    // Settings for connecting to the APRS server
    const CALLSIGN = process.env.NEXT_PUBLIC_SITEURL;
    const PASSCODE = -1;
    const APRSSERVER = 'aprs.glidernet.org';
    const PORTNUMBER = 14580;
	
    // Connect to the APRS server
    connection = new ISSocket(APRSSERVER, PORTNUMBER, 'OGNRBETA', '', 't/spuoimnwt' );
    let parser = new aprsParser();

    // Handle a connect
    connection.on('connect', () => {
        connection.sendLine( connection.userLogin );
        connection.sendLine(`# onglide ${CALLSIGN} ${process.env.NEXT_PUBLIC_WEBSOCKET_HOST}`);
    });

    // Handle a data packet
    connection.on('packet', async function (data) {
		if( !connection || connection.exiting ) {
			return;
		}
        connection.valid = true;
        if( data.charAt(0) != '#' && !data.startsWith('user')) {
			packetStats.rawCount++;
            const packet = parser.parseaprs(data);
            if( "latitude" in packet && "longitude" in packet &&
                "comment" in packet && packet.comment?.substr(0,2) == 'id' ) {
				processPacket( packet );
            }
			else {
				if( (packet.destCallsign == 'OGNSDR' || data.match(/qAC/)) && ! ignoreStation(packet.sourceCallsign)) {

					if( packet.type == 'location' ) {	
						const stationid = getStationId( packet.sourceCallsign, true );

						// Check if we have moved too far ( a little wander is considered ok )
						const details = stations[ packet.sourceCallsign ];
						if( details.lat && details.lng ) {
							const distance = h3.pointDist( [details.lat,details.lng], [packet.latitude,packet.longitude], 'km' );
							if( distance > STATION_MOVE_THRESHOLD_KM ) {
								details.notice = `${Math.round(distance)}km move detected ${Date(packet.timestamp*1000).toISOString()} resetting history`;
								details.moved = true; // we need to persist this
								statusDb.put( packet.sourceCallsign, JSON.stringify(details) );
							}
						}
						details.lat = packet.latitude;
						details.lng = packet.longitude;
					}
					else if( packet.type == 'status' ) {
						const stationid = getStationId( packet.sourceCallsign, false ); // don't write as we do it in next line
						statusDb.put( packet.sourceCallsign, JSON.stringify({ ...stations[packet.sourceCallsign], status: packet.body }) );
					}
					else {
						console.log( data, packet );
					}
				}
			}
        } else {
            // Server keepalive
            console.log(data, '#', packetStats.rawCount);
            if( data.match(/aprsc/) ) {
                connection.aprsc = data;
            }
        }
    });

    // Failed to connect
    connection.on('error', (err) => {
		if( ! connection.exiting ) {
			console.log('Error: ' + err);
			connection.disconnect();
			connection.connect();
		}
    });

	// Load the status
	async function loadStationStatus(statusdb) {
		const nowEpoch = Math.floor(Date.now()/1000);
		const expiryEpoch = nowEpoch - STATION_EXPIRY_TIME_SECS;
		try { 
			for await ( const [key,value] of statusdb.iterator()) {
				stations[key] = JSON.parse(''+value)
				if( (stations[key].lastPacket||nowEpoch) > expiryEpoch ) {
					validStations.add(Number(stations[key].id));
				}
			}
		} catch(e) {
			console.log('oops',e)
		}
		const nextid = (_reduce( stations, (highest,i) => { return highest < (i.id||0) ? i.id : highest }, 0 )||0)+1;
		console.log( 'next id', nextid );
		return nextid;
	}
	console.log( 'loading station status' );
	Atomics.store(nextStation, 0, (await loadStationStatus(statusDb))||1)

	// Check and process unflushed accumulators at the start
	// then we can increment the current number for each accumulator merge
	await rollupStartup(globalDb);
	await updateAndProcessAccumulators();
	
    // Start the APRS connection
    connection.connect();

	// And every (2) minutes we need to confirm the APRS
	// connection has had some traffic
	intervals.push(setInterval( function() {
		try {
			// Send APRS keep alive or we will get dumped
			connection.sendLine(`# ${CALLSIGN} ${process.env.NEXT_PUBLIC_WEBSOCKET_HOST}`);
		} catch(e) {
			console.log( `exception ${e} in sendLine status` );
			connection.valid = false;
		}
		
        // Re-establish the APRS connection if we haven't had anything in
        if( ! connection.valid && ! connection.exiting ) {
            console.log( "failed APRS connection, retrying" );
            connection.disconnect( () => { connection.connect() } );
        }
        connection.valid = false;
	}, aprsKeepAlivePeriod ));

	// We also need to flush our h3 cache to disk on a regular basis
	// this is used as an opportunity to display some statistics
	intervals.push(setInterval( async function() {

		// Flush the cache
		const flushStats = await flushDirtyH3s();

		// Report some status on that
		const packets = (packetStats.count - lastPacketCount);
		const rawPackets = (packetStats.rawCount - lastRawPacketCount);
		const pps = packetStats.pps = (packets/(h3CacheFlushPeriod/1000)).toFixed(1);
		const rawPps = packetStats.rawPps = (rawPackets/(h3CacheFlushPeriod/1000)).toFixed(1);
		const h3length = flushStats.total;
		const h3delta = h3length - lastH3length;
		const h3expired = flushStats.expired;
		const h3written = flushStats.written;
		console.log( `elevation cache: ${getCacheSize()}, valid packets: ${packets} ${pps}/s, all packets ${rawPackets} ${rawPps}/s` );
		console.log( `total stations: ${nextStation-1}, valid stations ${validStations.size}, openDbs: ${stationDbCache.size+2}` );
		console.log( JSON.stringify(packetStats))
		console.log( JSON.stringify(rollupStats))
		console.log( `h3s: ${h3length} delta ${h3delta} (${(h3delta*100/h3length).toFixed(0)}%): `,
					 ` expired ${h3expired} (${(h3expired*100/h3length).toFixed(0)}%), written ${h3written} (${(h3written*100/h3length).toFixed(0)}%)`,
					 ` ${((h3written*100)/packets).toFixed(1)}% ${(h3written/(h3CacheFlushPeriod/1000)).toFixed(1)}/s ${(packets/h3written).toFixed(0)}:1`,
	); 

		// purge and flush H3s to disk
		// carry forward state for stats next time round
		lastPacketCount = packetStats.count;
		lastRawPacketCount = packetStats.rawCount;
		lastH3length = h3length;

	}, h3CacheFlushPeriod ));

	timeouts.push( setTimeout( () => {
		intervals.push( setInterval( async function() {
			
			const purgeBefore = (Date.now()/1000) - FORGET_AIRCRAFT_AFTER_SEC;
			let total = aircraftSeen.size;
			
			aircraftSeen.forEach( (timestamp,key) => {
				if( timestamp < purgeBefore ) {
					aircraftSeen.delete(key);
				}
			});
			aircraftStation.forEach( (timestamp,key) => {
				if( timestamp < purgeBefore ) {
					aircraftStation.delete(key);
				}
			});
			
			let purged = total - aircraftSeen.size;
			console.log( `purged ${purged} aircraft from gap map, ${aircraftSeen.size} remaining` );
			
		}, (3600*1000) )); // every hour we do this
	}, (FORGET_AIRCRAFT_AFTER_SEC+Math.random(300))*1000));
	
	// Make sure our accumulator is correct, this will also
	// ensure we rollover and produce output files correctly
	intervals.push(setInterval( async function() {		
		updateAndProcessAccumulators();
	}, ACCUMULATOR_CHANGEOVER_CHECK_PERIOD_MS ));

	// Make sure our accumulator is correct, this will also
	// ensure we rollover and produce output files correctly
	intervals.push(setInterval( async function() {		
		rollupAll( current, accumulators );
	}, h3RollupPeriod ));
}

function displayStatus() {
	console.log( `elevation cache: ${getCacheSize()}, h3cache: ${cachedH3s.size},  valid packets: ${packetStats.count} ${packetStats.pps}/s, all packets ${packetStats.rawCount} ${packetStats.rawPps}/s` );
	console.log( `total stations: ${nextStation-1}, valid stations ${validStations.size}, openDbs: ${stationDbCache.size+2}` );
	console.log( JSON.stringify(packetStats))
}

function getStationId( station, serialise = true ) {
	// Figure out which station we are - this is synchronous though don't really
	// understand why the put can't happen in the background
	let stationid = undefined;
	if( station ) {
		if( ! stations[ station ] ) {
			stations[station]={ station:station }
		}
		
		if( 'id' in stations[station] ) {
			stationid = stations[station].id;
		}
		else {
			stationid = stations[station].id = Atomics.add(nextStation, 0, 1);
			console.log( `allocated id ${stationid} to ${station}, ${Object.keys(stations).length} in hash` )

			if( serialise ) {
				statusDb.put( station, JSON.stringify(stations[station]) );
			}
		}
	}
	return stationid;
}

//
// collect points, emit to competition db every 30 seconds
async function processPacket( packet ) {

    // Count this packet into pm2
    metrics?.ognPerSecond?.mark();

    // Flarm ID we use is last 6 characters, check if OGN tracker or regular flarm
    const flarmId = packet.sourceCallsign.slice( packet.sourceCallsign.length - 6 );
	const pawTracker = (packet.sourceCallsign.slice( 0, 3 ) == 'PAW');

	// Lookup the altitude adjustment for the 
	const sender = packet.digipeaters?.pop()?.callsign||'unknown';

	// Obvious reasons to ignore stations
	if( ignoreStation( sender ) ) {
		packetStats.ignoredStation++;
		return;
	}
	if( packet.destCallsign == 'OGNTRK' && packet.digipeaters?.[0]?.callsign?.slice(0,2) != 'qA' ) {
		packetStats.ignoredTracker++;
		return;
	}
	if( pawTracker ) {
		packetStats.ignoredPAW++;
		return;
	}

    let altitude = Math.floor(packet.altitude);

	// Make sure they are moving... we can get this from the packet without any
	// vertical speed of 30 is ~0.5feet per second or ~15cm/sec and I'm guessing
	// helicopters can't hover that precisely. NOTE this threshold is not 0 because
	// the roc jumps a lot in the packet stream.
	if( packet.speed < 1 ) {
		const rawRot = (packet.comment.match(reExtractRot)||[0,0])[1];
		const rawVC = (packet.comment.match(reExtractVC)||[0,0])[1];
		if( rawRot == 0.0 && rawVC < 30 ) {
			packetStats.ignoredStationary++;
			aircraftSeen.set(flarmId, packet.timestamp );
			return;
		}
	}

	// If we have a gap then we will capture this (it was from a previous record but only time
	// that is an issue is when rolling aggregators - at which point we have reset aircraftStation
	// anyway (IS IT??)
	//
	// THIS LOGIC IS EXPERIMENTAL!
	//
	// The goal is to have some kind of shading that indicates how reliable packet reception is
	// which is to  a little to do with how many packets are received.
	let gap;
	{
		const gs = sender + '/' + flarmId;
		const seen = aircraftSeen.get(flarmId);
		const when = aircraftStation.get( gs );
		gap = when ? Math.min(60,(packet.timestamp - when)) : (Math.min(60,Math.max(1,packet.timestamp - (seen||packet.timestamp))));
		aircraftStation.set( gs, packet.timestamp );
		aircraftSeen.set(flarmId, packet.timestamp );
	}

	// Look for signal strength and checksum - we will ignore any packet without a signal strength
	// sometimes this happens to be missing and other times it happens because it is reported as 0.0
	const rawStrength = (packet.comment.match(reExtractDb)||[0,0])[1];
	const strength = Math.min(Math.round(parseFloat(rawStrength)*4),255);

	// crc may be absent, if it is then it's a 0
	const crc = parseInt((packet.comment.match(reExtractCrc)||[0,0])[1]);

	// If we have no signal strength then we'll ignore the packet... don't know where these
	// come from or why they exist...
	if( strength <= 0 ) {
		packetStats.ignoredSignal0++;
		return;
	}
	
	// Enrich with elevation and send to everybody, this is async
	// and we don't need it's results to say we logged the packet
	getElevationOffset( packet.latitude, packet.longitude,
						async (gl) => {
							const agl = Math.round(Math.max(altitude-gl,0));
							packetCallback( sender, h3.geoToH3(packet.latitude, packet.longitude,  H3_STATION_CELL_LEVEL),
											altitude, agl, crc, strength, gap, packet.timestamp );
	});
	
	packetStats.count++;
}

//
// Actually serialise the packet into the database after processing the data
async function packetCallback( station, h3id, altitude, agl, crc, signal, gap, timestamp ) {

	// Find the id for the station or allocate
	const stationid = await getStationId( station );
	
	// Packet for station marks it for dumping next time round
	stations[station].lastPacket = timestamp;
	
	// Open the database, do this first as takes a bit of time
	let stationDb = stationDbCache.get(stationid);
	if( ! stationDb ) {
		stationDbCache.set(stationid, stationDb = LevelUP(LevelDOWN(dbPath+'/stations/'+station)))
		stationDb.ognInitialTS = Date.now();
		stationDb.ognStationName = station;
	}

	// Merge into both the station db (0,0) and the global db with the stationid we allocated
	// we don't pass stationid into the station specific db because there only ever is one
	// it gets used to build the list of stations that can see the cell
	mergeDataIntoDatabase( 0,          stationid, stationDb, h3id, altitude, agl, crc, signal, gap );
	mergeDataIntoDatabase( stationid,  0,         globalDb, h3.h3ToParent(h3id, H3_GLOBAL_CELL_LEVEL), altitude, agl, crc, signal, gap );
}

//
// We store the database records as binary bytes - in the format described in the mapping() above
// this reduces the amount of storage we need and means we aren't constantly parsing text
// and printing text.
async function mergeDataIntoDatabase( stationid, dbStationId, db, h3, altitude, agl, crc, signal, gap ) {

	// Because the DB is asynchronous we need to ensure that only
	// one transaction is active for a given h3 at a time, this will
	// block all the other ones until the first completes, it's per db
	// no issues updating h3s in different dbs at the same time
	const h3k = new CoverageHeader( dbStationId, ...getAccumulator(h3), h3 );
	lock( h3k.lockKey, function (release) {

		// If we have some cached changes for this h3 then we will simply
		// use the entry in the 'dirty' table. This table gets flushed
		// on a periodic basis and saves us hitting the disk for very
		// busy h3s
		const cachedH3 = cachedH3s.get(h3k.lockKey);
		if( cachedH3 ) {
			cachedH3.dirty = true;
			cachedH3.lastAccess = Date.now();
			cachedH3.br.update( altitude, agl, crc, signal, gap, stationid );
			release()();
		}
		else {
			db.get( h3k.dbKey() )
			  .then( (value) => {
				  const buffer = new CoverageRecord( value );
				  cachedH3s.set(h3k.lockKey,{br:buffer,dirty:true,lastAccess:Date.now(),lastWrite:Date.now()});
				  buffer.update( altitude, agl, crc, signal, gap, stationid );
				  release()();
			  })
			  .catch( (err) => {
				  const buffer = new CoverageRecord( stationid ? bufferTypes.global : bufferTypes.station );
				  cachedH3s.set(h3k.lockKey,{br:buffer,dirty:true,lastAccess:Date.now(),lastWrite:Date.now()});
				  buffer.update( altitude, agl, crc, signal, gap, stationid );
				  release()();
			  });
		}
	});
	
}

//
// This function writes the H3 buffers to the disk if they are dirty, and 
// clears the records if it has expired. It is actually a synchronous function
// as it will not return before everything has been written
async function flushDirtyH3s(allUnwritten) {

	// When do we write and when do we expire
	const now = Date.now();
	const flushTime = Math.max( 0, now - h3CacheFlushPeriod);
	const maxDirtyTime = Math.max( 0, now - h3CacheMaximumDirtyPeriod);
	const expiryTime = Math.max( 0, now - h3CacheExpiryTime);
	

	let stats = {
		total: cachedH3s.size,
		expired: 0,
		written: 0,
		databases: 0,
	};
	
	// We will keep track of all the async actions to make sure we
	// don't get out of order during the lock() or return before everything
	// has been serialised
	let promises = [];

	const dbOps = new Map(); //[station]=>list of db ops

	// Go through all H3s in memory and write them out if they were updated
	// since last time we flushed
	for (const [h3klockkey,v] of cachedH3s) {

		promises.push( new Promise( (resolve) => {
		
			// Because the DB is asynchronous we need to ensure that only
			// one transaction is active for a given h3 at a time, this will
			// block all the other ones until the first completes, it's per db
			// no issues updating h3s in different dbs at the same time
			lock( h3klockkey, function (release) {

				// If we are dirty all we can do is write it out
				if( v.dirty ) {

					// either periodic flush (eg before rollup) or flushTime elapsed
					// or it's been in the cache so long we need to flush it
					if( allUnwritten || (v.lastAccess < flushTime) || (v.lastWrite < maxDirtyTime) ) {
						const h3k = new CoverageHeader(h3klockkey);
					
						// Add to the write out structures
						let ops = dbOps.get(h3k.dbid);
						if( ! ops ) {
							dbOps.set(h3k.dbid, ops = new Array());
						}
						ops.push( { type: 'put', key: h3k.dbKey(), value: Buffer.from(v.br.buffer()) });
						stats.written++;
						v.lastWrite = now;
						v.dirty = false;
					}
				}
				// If we are clean then we can be deleted
				else if( v.lastAccess < expiryTime ) {
					cachedH3s.delete(h3klockkey);
					stats.expired++;
				}
				
				// we are done, no race condition on write as it's either written to the
				// disk above, or it was written earlier and has simply expired, it's not possible
				// to expire and write (expiry is period after last write)... ie it's still
				// in cache after write till expiry so only cache lock required for integrity
				release()();
				resolve();
			});
		}));
	}

	// We need to wait for all promises to complete before we can do the next part
	await Promise.all( promises );
	promises = [];
	
	// So we know where to start writing
	stats.databases = dbOps.size;

	// Now push these to the database
	for ( const [dbid,v] of dbOps ) {
		promises.push( new Promise( (resolve) => {
			//
			let db = (dbid != 0) ? stationDbCache.get(dbid) : globalDb;

			// If we are writing for it then it's a valid station
			if( dbid ) {
				validStations.add(dbid);
			}

			// Open DB if needed 
			if( ! db ) {
				console.log( `weirdly opening db to write for cache ${dbid}, your stationDbCache is too small for active set` );
				const stationName = _find(stations, { id: dbid })?.station;
				if( ! stationName ) {
					throw 'Unable to find station name for id ${dbid}... this is obviously not ideal, probably data corruption ;)';
				}
				stationDbCache.set(dbid, db = LevelUP(LevelDOWN(dbPath+'/stations/'+stationName)))
				db.ognInitialTS = Date.now();
				db.ognStationName = stationName;
			}
			
			// Execute all changes as a batch
			db.batch(v, (e) => {
				// log errors
				if(e) console.error('error flushing db operations for station id',dbid,e);
				resolve();
			});
		}));
	}
	await Promise.all( promises );
	return stats;
}


//
// Dump the meta data for all the stations, we take from our in memory copy
// it will have been primed on start from the db and then we update and flush
// back to the disk
async function produceStationFile( statusdb ) {

	// Form a list of hashes
	let statusOutput = _filter( stations, (v) => {return v.valid});

	// Write this to the stations.json file
	writeFile( outputPath + 'stations.json', JSON.stringify(statusOutput,null,2), (err) => err ? console.log("station write error",err) : null);
}

//
// We will add meta data to the database for each of the accumulators
// this makes it easier to check what needs to be done
function updateGlobalAccumulatorMetadata() {

	const dbkey = CoverageHeader.getAccumulatorMeta( ...getAccumulator() ).dbKey();
	const now = new Date;
	
	globalDb.get( dbkey )
		.then( (value) => {
			const meta = JSON.parse( String(value) );
			meta.oldStarts = [ ...meta?.oldStarts, { start: meta.start, startUtc: meta.startUtc } ];
			meta.accumulators = accumulators;
			meta.start = Math.floor(now/1000);
			metat.startUtc = now.toISOString();
			globalDb.put( dbkey, JSON.stringify( meta ));
		})
		.catch((e) => {
			globalDb.put( dbkey, JSON.stringify( {
				accumulators: accumulators,
				oldStarts: [],
				start: Math.floor(now/1000),
				startUtc: now.toISOString()
			}));
		});

	// make sure we have an up to date header for each accumulator
	for( const type in accumulators ) {
		const currentHeader = CoverageHeader.getAccumulatorMeta( type, accumulators[type].bucket );
		const dbkey = currentHeader.dbKey();
		globalDb.get( dbkey )
			.then( (value) => {
				const meta = JSON.parse( String(value) );
				globalDb.put( dbkey, JSON.stringify( { ...meta,
													   accumulators: accumulators,
													   currentAccumulator: currentAccumulator[1] }));
			})
			.catch( (e) => {
				globalDb.put( dbkey, JSON.stringify( { start: Math.floor(now/1000),
													   startUtc: now.toISOString(),
													   accumulators: accumulators,
													   currentAccumulator: currentAccumulator[1] }));
			});
	}
}

function updateAndProcessAccumulators() {
	
	const now = new Date();

	// Calculate the bucket and short circuit if it's not changed - on startup current will
	// be empty so we will carry on
	const newAccumulatorBucket = (now.getUTCFullYear()%8) * 385 + now.getUTCMonth() * 32 + now.getUTCDate()
	if( currentAccumulator?.[1] == newAccumulatorBucket ) {
		return;
	}

	// Make a copy
	const oldAccumulators = _clonedeep( accumulators );
	const oldAccumulator = _clonedeep( currentAccumulator );

	// We need a current that is basically unique so we don't rollup the wrong thing at the wrong time
	// our goal is to make sure we survive restart without getting same code if it's not the same day...
	// if you run this after an 8 year gap then welcome back ;) and I'm sorry ;)  [it has to fit in 12bits]
	// this takes effect immediately so all new packets will move to the new accumulator
	currentAccumulator = [ 'current', newAccumulatorBucket ];

	const n = {
		d: prefixWithZeros(2,now.getUTCDate()),
		m: prefixWithZeros(2,String(now.getUTCMonth())),
		y: now.getUTCFullYear()
	};
	
	// Our accumulators
	accumulators = { day: { bucket: now.getUTCDate(), file: `${n.y}-${n.m}-${n.d}`, },
					 month: { bucket: now.getUTCMonth(), file: `${n.y}-${n.m}` },
					 year:  { bucket: now.getUTCFullYear(), file: `${n.y}` }};
	

	// If we have a new accumulator then we need to do a rollup
	if( oldAccumulator && currentAccumulator[1] != oldAccumulator[1] ) {

		console.log( `accumulator rotation scheduling:` );
		console.log( JSON.stringify(oldAccumulators) );
		console.log( '----' );
		console.log( JSON.stringify(accumulators) );

		// Now we need to make sure we have flushed our H3 cache and everything
		// inflight has finished before doing this. we could purge cache
		// but that doesn't ensure that all the inflight has happened
		flushDirtyH3s(true).then( () => {
			console.log( `accumulator rotation happening` );
			rollupAll( oldAccumulator, oldAccumulators, true );
		})

	}

	// If any of the accumulators have changed then we need to update all the
	// meta data
	if( ! _isequal( accumulators, oldAccumulators ) || ! _isequal( currentAccumulator, oldAccumulator )) {
		updateGlobalAccumulatorMetadata();
	}
}		

	
//
// We need to make sure we know what rollups the DB has, and process pending rollup data
// when the process starts. If we don't do this all sorts of weird may happen
// (only used for global but could theoretically be used everywhere)
async function rollupStartup( db ) {

	let accumulatorsToPurge = {};
	let hangingCurrents = [];

	const now = new Date();
	
	// Our accumulators 
	const expectedAccumulators = { day: { bucket: now.getUTCDate() },
						   month: { bucket: now.getUTCMonth() },
						   year:  { bucket: now.getUTCFullYear() }};

	// We need a current that is basically unique so we don't rollup the wrong thing at the wrong time
	// our goal is to make sure we survive restart without getting same code if it's not the same day...
	// if you run this after an 8 year gap then welcome back ;) and I'm sorry ;)  [it has to fit in 12bits]
	const expectedCurrentAccumulator = [ 'current', (now.getUTCFullYear()%8) * 385 + now.getUTCMonth() * 32 + now.getUTCDate()];
		
	// First thing we need to do is find all the accumulators in the database
	let iterator = db.iterator();
	let iteratorPromise = iterator.next(), row = null;
	while( row = await iteratorPromise ) {
		const [key,value] = row;
		let hr = new CoverageHeader(key);

		// 80000000 is the h3 cell code we use to
		// store the metadata for our iterator
		if( ! hr.isMeta ) {
			console.log( 'ignoring weird database format', hr.h3 )
			iterator.seek( CoverageHeader.getAccumulatorEnd( hr.type, hr.bucket ));
			iteratorPromise = iterator.next();
			continue;
		}
		const meta = JSON.parse( String(value) ) || {};

		// If it's a current and not OUR current then we need to
		// figure out what to do with it... We may merge it into rollup
		// accumulators if it was current when they last updated their meta
		if( hr.typeName == 'current' ) {
			if( hr.bucket != expectedCurrentAccumulator[1] ) {
				hangingCurrents[ hr.dbKey() ] = meta;
				console.log( `current: hanging accumulator ${hr.accumulator} (${hr.bucket})`);
			}
			else {
				console.log( `current: resuming accumulator ${hr.accumulator} (${hr.bucket}) as still valid`);
			}
		}

		// accumulator not configured on this machine - dump and purge
		else if( ! expectedAccumulators[ hr.typeName ] ) {
			accumulatorsToPurge[ hr.accumulator ] = { accumulator: hr.accumulator, meta: meta, typeName: hr.typeName, t: hr.type, b: hr.bucket };
		}
		// new bucket for the accumulators - we should dump this
		// and purge as adding new data to it will cause grief
		// note the META will indicate the last active accumulator
		// and we should merge that if we find it
		else if( expectedAccumulators[ hr.typeName ].bucket != hr.bucket ) {
			accumulatorsToPurge[ hr.accumulator ] = { accumulator: hr.accumulator, meta: meta, typeName: hr.typeName, t: hr.type, b: hr.bucket };
		}
		else {
			console.log( `${hr.typeName}: resuming accumulator ${hr.accumulator} (${hr.bucket}) as still valid` );
		}

		// Done with this one lets skip forward
		iterator.seek( CoverageHeader.getAccumulatorEnd( hr.type, hr.bucket ));
		iteratorPromise = iterator.next();
	}


	//
	// We will add meta data to the database for each of the current accumulators
	// this makes it easier to check what needs to be done?
	{
			const dbkey = CoverageHeader.getAccumulatorMeta( ...expectedCurrentAccumulator ).dbKey();
			db.get( dbkey )
			  .then( (value) => {
				  const meta = JSON.parse( String(value) );
				  meta.oldStarts = [ ...meta?.oldStarts, { start: meta.start, startUtc: meta.startUtc } ];
				  meta.start = Math.floor(now/1000);
				  meta.startUtc = now.toISOString();
				  globalDb.put( dbkey, JSON.stringify( meta ));
			  })
			  .catch((e) => {
				  globalDb.put( dbkey, JSON.stringify( {
					  start: Math.floor(now/1000),
					  startUtc: now.toISOString()
				  }));
			  }
		);

		// make sure we have an up to date header for each accumulator
		const currentAccumulatorHeader = CoverageHeader.getAccumulatorMeta( ...expectedCurrentAccumulator );
		for( const type in expectedAccumulators ) {
			const dbkey = CoverageHeader.getAccumulatorMeta( type, expectedAccumulators[type].bucket ).dbKey();
			db.get( dbkey )
			  .then( (value) => {
				  const meta = JSON.parse( String(value) );
				  globalDb.put( dbkey, JSON.stringify( { ...meta,
														 currentAccumulator: currentAccumulatorHeader.bucket }));
			  })
			  .catch( (e) => {
				  globalDb.put( dbkey, JSON.stringify( { start: Math.floor(now/1000),
														 startUtc: now.toISOString(),
														 currentAccumulator: currentAccumulatorHeader.bucket }));
			  });
		}
	}		

	// This is more interesting, this is a current that could be rolled into one of the other
	// existing accumulators... 
	if( hangingCurrents ) {
		// we need to purge these	
		for( const key in hangingCurrents ) {
			const hangingHeader = new CoverageHeader( key );

			const meta = hangingCurrents[key];
			
			// So we need to figure out what combination of expected and existing accumulators should
			// be updated with the hanging accumulator, if we don't have the bucket anywhere any more
			// then we zap it. Note the buckets can't change we will only ever roll up into the buckets
			// the accumulator was started with
			let rollupAccumulators = {};
			for( const type of Object.keys(expectedAccumulators)) {
				const ch = CoverageHeader.getAccumulatorMeta( type, meta.accumulators?.[type]?.bucket||-1 );
				if( accumulatorsToPurge[ ch.accumulator ] || expectedAccumulators[type].bucket == ch.bucket ) {
					rollupAccumulators[ type ] = { bucket: ch.bucket };
				}
			}

			if( Object.keys(rollupAccumulators).length ) { 
				console.log( ` rolling up hanging current accumulator ${hangingHeader.accumulator} into ${JSON.stringify(rollupAccumulators)}` );
				await rollupAll( [hangingHeader.type,hangingHeader.bucket], rollupAccumulators, true );
			} else {
				console.log( `purging hanging current accumulator ${hangingHeader.accumulator} and associated sub accumulators` );
			}

			// now we clear it
			globalDb.clear( CoverageHeader.getDbSearchRangeForAccumulator( hangingHeader.type, hangingHeader.bucket, true ),
							(e) => { console.log( `${hangingHeader.type}/${hangingHeader.accumulator} clear completed ${e||'successfully'}`) } );
		}
	}
	
	// These are old accumulators we purge them because we aren't sure what else can be done
	if( accumulatorsToPurge.length ) {
		console.log( 'purging:' );
		console.log( accumulatorsToPurge );
		accumulatorsToPurge.forEach( (a) =>  {
			globalDb.clear( CoverageHeader.getDbSearchRangeForAccumulator( a.t, a.b, true ),
							(e) => { console.log( `${a.typeName}: ${a.accumulator} purged completed ${e||'successfully'}`) } );
		});
	}

}

//
// This iterates through all open databases and rolls them up.
// ***HMMMM OPEN DATABASE - so if DBs are not staying open we have a problem
async function rollupAll( current, processAccumulators, newAccumulator = false ) {

	// Make sure we have updated validStations
	const nowEpoch = Math.floor(Date.now()/1000);
	const expiryEpoch = nowEpoch - STATION_EXPIRY_TIME_SECS;
	validStations.clear()
	for ( const station of Object.values(stations)) {
		if( (station.lastPacket||nowEpoch) > expiryEpoch && ! station.moved) {
			validStations.add(Number(station.id));
		}
		if( station.moved ) {
			station.moved = false;
			console.log( `purging moved station ${station.station}` );
			statusDb.put( station.station, JSON.stringify(station) );
		}
	}
	
	console.log( `performing rollup and output of ${validStations.size} stations + global ` );
	
	rollupStats = { ...rollupStats, 
					lastStart: Date.now(),
					last: {		sumElapsed: 0,
								operations: 0,
								databases: 0, accumulators: processAccumulators }
	};

	// Global is biggest and takes longest
	let promises = [];
	promises.push( new Promise( async function (resolve) {
		const r = await rollupDatabase( globalDb, 'global', current, processAccumulators );
		rollupStats.last.sumElapsed += r.elapsed;
		rollupStats.last.operations += r.operations;
		rollupStats.last.databases ++;
		resolve();
	}));
	
	// each of the stations, capped at 20% of db cache (or 30 tasks) to reduce risk of purging the whole cache
	// mapAllCapped will not return till all have completed, but this doesn't block the processing
	// of the global db or other actions.
	// it is worth running them in parallel as there is a lot of IO which would block
	promises.push( mapAllCapped( Object.keys(stations), async function (station) {

		// Open DB if needed 
		let db = stationDbCache.get(stations[station].id)
		if( ! db ) {
			stationDbCache.set(stations[station].id, db = LevelUP(LevelDOWN(dbPath+'/stations/'+station)));
			db.ognInitialTS = Date.now();
			db.ognStationName = station;
		}

		// If a station is not valid we are clearing the data from it from the registers
		if( ! validStations.has( stations[station].id ) ) {
			// empty the database... we could delete it but this is very simple and should be good enough
			console.log( `clearing database for ${station} as it is not valid` );
			await db.clear();
			return;
		}
		
		const r = await rollupDatabase( db, station, current, processAccumulators, newAccumulator );
		rollupStats.last.sumElapsed += r.elapsed;
		rollupStats.last.operations += r.operations;
		rollupStats.last.databases ++;
		
	}, Math.max(Math.floor(process.env.MAX_STATION_DBS/5),30) ));
	
	// And the global json
	promises.push( produceStationFile( statusDb ) );

	// Wait for all to be done
	await Promise.allSettled(promises);
	
	// Flush old database from the cache
	stationDbCache.purgeStale();

	// Report stats on the rollup
	rollupStats.lastElapsed = Date.now() - rollupStats.lastStart;
	rollupStats.elapsed += rollupStats.lastElapsed;
	rollupStats.completed++;
	rollupStats.lastStart = (new Date(rollupStats.lastStart)).toISOString();
	console.log( 'rollup completed', JSON.stringify(rollupStats) );
}

//
// Rotate and Rollup all the data we have
// we do this by iterating through each database looking for things in default
// aggregator (which is always just the raw h3id)
async function rollupDatabase( db, station, current, processAccumulators, newAccumulator ) {

	const now = new Date();
	const name = station;
	let currentMeta = {};

	// Details about when we wrote, also contains information about the station if
	// it's not global
	let stationMeta = station != 'global' ? stations[station] : {};
	stationMeta.outputDate = now.toISOString();
	stationMeta.outputEpoch = Math.floor(now/1000);
	
	let dbOps = [];
	//
	// Basically we finish our current accumulator into the active buckets for each of the others
	// and then we need to check if we should be moving them to new buckets or not
	
	// We step through all of the items together and update as one
	const rollupIterators = _map( Object.keys(processAccumulators), (r) => {
		return { type:r, bucket: processAccumulators[r].bucket,
				 meta: { rollups: [] },
				 stats: {
					 h3missing:0,
					 h3noChange: 0,
					 h3updated:0,
					 h3emptied:0,
				 },
				 iterator: db.iterator( CoverageHeader.getDbSearchRangeForAccumulator( r, processAccumulators[r].bucket )),
				 arrow: CoverageRecord.initArrow( (station == 'global') ? bufferTypes.global : bufferTypes.station ),
		}
	});

	// Enrich with the meta data for each accumulator type
	await Promise.all( _map( [...rollupIterators, { type: current[0], bucket: current[1] }],
							 (r) => new Promise( (resolve) => {
								 const ch = CoverageHeader.getAccumulatorMeta( r.type, r.bucket );
								 db.get( ch.dbKey()) 
									 .then( (value) => {
										 if( r.type == current[0] && r.bucket == current[1] ) { currentMeta = JSON.parse(value);  }
										 else { r.meta = JSON.parse(String(value)||{}); }
										 resolve()
									 })
									 .catch( (e) => { resolve(); } )
							 })));

	// Initalise the rollup array with [k,v]
	const rollupData = _map( rollupIterators, (r) => {return { n:r.iterator.next(), ...r}} );

	// Create the 'outer' iterator - this walks through the primary accumulator 
	for await ( const [key,value] of db.iterator( CoverageHeader.getDbSearchRangeForAccumulator(...current)) ) {

		// The 'current' value - ie the data we are merging in.
		const h3k = new CoverageHeader(key);

		if( h3k.isMeta ) {
			continue;
		}
		
		const currentBr = new CoverageRecord(value);
		let advance = true;

		do {
			advance = true;
			
			// now we go through each of the rollups in lockstep
			// we advance and action all of the rollups till their
			// key matches the outer key. This is async so makes sense
			// to do them interleaved even though we await
			for( const r of rollupData ) {

				// iterator async so wait for it to complete
				let n = r.n ? (await r.n) : undefined;
				let [prefixedh3r,rollupValue] = n || [null, null]; 

				// We have hit the end of the data for the accumulator but we still have items
				// then we need to copy the next data across - 
				if( ! prefixedh3r ) {
					
					if( r.lastCopiedH3r != h3k.lockKey ) {
						const h3kr = h3k.getAccumulatorForBucket(r.type,r.bucket);
						dbOps.push( { type: 'put',
									  key: h3kr.dbKey(),
									  value: Buffer.from(currentBr.buffer()) } );
						currentBr.appendToArrow( h3kr, r.arrow );
						r.lastCopiedH3r = h3k.lockKey;
						r.stats.h3missing++;
					}
					
					// We need to cleanup when we are done
					if( r.n ) {
						r.iterator.end();
						r.n = null;
					}
					continue;
				}
				
				const h3kr = new CoverageHeader(prefixedh3r);
				if( h3kr.isMeta ) { // skip meta
					continue;
				}

				// One check for ordering so we know if we need to
				// advance or are done
				const ordering = CoverageHeader.compare(h3k,h3kr);
								
				// Need to wait for others to catch up and to advance current
				// (primary is less than rollup) depends on await working twice on
				// a promise (await r.n above) because we haven't done .next()
				// this is fine but will yield which is also fine
				if( ordering < 0 ) {
					continue;
				}

				// We know we are editing the record so load it up, our update
				// methods will return a new CoverageRecord if they change anything
				// hence the updatedBr
				let br = new CoverageRecord(rollupValue);
				let updatedBr = null;
				
				// Primary is greater than rollup
				if( ordering > 0 ) {
					updatedBr = br.removeInvalidStations(validStations);
				}

				// Otherwise we are the same so we need to rollup into it
				else  {
					updatedBr = br.rollup(currentBr, validStations);
				}
				
				// Check to see what we need to do with the database
				// this is a pointer check as pointer will ALWAYS change on
				// adjustment 
				if( updatedBr != br ) {
					if( ! updatedBr ) {
						dbOps.push( { type: 'del', key: h3kr.dbKey() } );
						r.stats.h3emptied++;
					}
					else {
						r.stats.h3updated++;
						dbOps.push( { type: 'put', key: h3kr.dbKey(), value: Buffer.from(updatedBr.buffer()) });
					}
				}
				else {
					r.stats.h3noChange++;
				}
				
				// If we had data then write it out
				if( updatedBr ) {
					updatedBr.appendToArrow( h3kr, r.arrow );
				}
				
				// Move to the next one, we don't advance till nobody has moved forward (async so promise)
				r.n = r.iterator.next();
				advance = false;
			}
			
		} while( ! advance );

		// Once we have accumulated we delete the accumulator key
		dbOps.push( { type: 'del', key: h3k.dbKey() })
	}

	
	// Finally if we have rollups with data after us then we need to update their invalidstations
	// now we go through them in step form
	for( const r of rollupData ) {
		if( r.n ) {
			let n = await r.n;
			let [prefixedh3r,rollupValue] =  n||[null,null];
			
			while( prefixedh3r ) { 
				const h3kr = new CoverageHeader(prefixedh3r);
				let br = new CoverageRecord(rollupValue);
				
				let updatedBr = br.removeInvalidStations(validStations);
				
				// Check to see what we need to do with the database
				if( updatedBr != br ) {
					if( ! updatedBr ) {
						dbOps.push( { type: 'del', key: h3kr.dbKey() } );
						r.stats.h3emptied++;
					}
					else {	
						dbOps.push( { type: 'put', key: h3kr.dbKey(), value: Buffer.from(updatedBr.buffer()) });
						r.stats.h3updated++;
					}
				}
				else {
					r.stats.h3noChange++;
				}
				
				if( updatedBr ) {
					updatedBr.appendToArrow( h3kr, r.arrow );
				}
				// Move to the next one, we don't advance till nobody has moved forward
				r.n = r.iterator.next();
				n = r.n ? (await r.n) : undefined;
				[prefixedh3r,rollupValue] = n || [null, null]; // iterator async so wait for it to complete
			}

			r.n = null;
			r.iterator.end();
		}
	}

	stationMeta.accumulators = [];

	// Write everything out
	for( const r of rollupData ) {

		// We are going to write out our accumulators this saves us writing it
		// in a different process and ensures that we always write the correct thing
		const accumulatorName = `${name}/${name}.${r.type}.${processAccumulators[r.type].file}`;
		
		// Keep a record of all the rollups in the meta
		// each record
		if( ! r.meta.rollups ) {
			r.meta.rollups = [];
		}
		r.meta.rollups.push( { source: currentMeta, stats: r.stats, file: accumulatorName } );
		stationMeta.accumulators.push( r.meta );

		// May not have a directory if new station
		mkdirSync(outputPath + name, {recursive:true});

		// Finalise the arrow table and serialise it to the disk
		CoverageRecord.finalizeArrow(r.arrow, outputPath+accumulatorName+'.arrow' );

		writeFile( outputPath + accumulatorName + '.json',
				   JSON.stringify(r.meta,null,2), (err) => err ? console.log("rollup json metadata write failed",err) : null)
		
		// link it all up for latest
		symlink( outputPath+accumulatorName, outputPath+`${name}/${name}.${r.type}.arrow` );
	}

	stationMeta.lastOutputFile = now;
	
	// record when we wrote for the whole station
	writeFile( outputPath+name+'.json', JSON.stringify(stationMeta,null,2), (err) => err ? console.log("stationmeta write error",err) : null);

	if( station != 'global' ) {
		delete stationMeta.accumulators; // don't persist this it's not important
		await statusDb.put( station, JSON.stringify(stations[station]) );
	}

	// If we have a new accumulator then we need to purge the old meta data records - we
	// have already purged the data above
	if( newAccumulator ) {
		dbOps.push( { type: 'del', key: CoverageHeader.getAccumulatorMeta( ...current ).dbKey() });
	}
	
	// Is this actually beneficial? - feed operations to the database in key type sorted order
	// so it can just process them. Keys should be stored clustered so theoretically this will
	// help with writing but perhaps benchmarking is a good idea
	dbOps = _sortby( dbOps, [ 'key', 'type' ])

	//
	// Finally execute all the operations on the database
	const p = new Promise( (resolve) => {
		db.batch(dbOps, (e) => {
			// log errors
			if(e) console.error('error flushing db operations for station id',name,e);
			resolve();
		});
	});
	await p;

	return { elapsed: Date.now() - now, operations: dbOps.length };
}								

function symlink(src,dest) {
	try {
		unlinkSync( dest );
	} catch(e) {
	}
	try {
		symlinkSync( src, dest, 'file' );
	} catch(e) {
		console.log( `error symlinking ${src}.arrow to ${dest}: ${e}` );
	}
}
