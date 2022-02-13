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

import h3 from 'h3-js';

import _map from 'lodash.map';
import _reduce from 'lodash.reduce';
import _sortby from 'lodash.sortby';

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

// track any setInterval calls so we can stop them when asked to exit
let intervals = [];

// APRS connection
let connection = {};
let aircraftStation = new Map();
let aircraftSeen = new Map();

// PM2 Metrics
let metrics = undefined;

// We serialise to disk using protobuf
//import protobuf from 'protobufjs/light.js';
//import { OnglideRangeMessage } from '../lib/range-protobuf.mjs';
import { writeFile, mkdirSync, createWriteStream, unlinkSync, symlinkSync } from 'fs';
import { PassThrough } from 'stream';

import { makeTable, tableFromArrays, RecordBatchWriter, makeVector, FixedSizeBinary,
		 Utf8, Uint8, makeBuilder } from 'apache-arrow';


// Default paths, can be overloaded using .env.local
let dbPath = './db/';
let outputPath = './public/data/';

// shortcuts so regexp compiled once
const reExtractDb = / ([0-9.]+)dB /;
const reExtractCrc = / ([0-9])c /;
const reExtractRot = / [+-]([0-9.]+)rot /;
const reExtractVC = / [+-]([0-9]+)fpm /;

// Cache so we aren't constantly reading/writing from the db
let dirtyH3s = new Map();
let lastH3update = new Map();

// APRS Server Keep Alive
const aprsKeepAlivePeriod = process.env.APRS_KEEPALIVE_PERIOD_MINUTES||2 * 60 * 1000;

// Cache control - we cache the datablocks by station and h3 to save us needing
// to read/write them from/to the DB constantly. Note that this can use quite a lot
// of memory, but is a lot easier on the computer
//
// - flush period is how long they can remain in memory without being written
// - expirytime is how long they can remain in memory without being purged. If it
//   is in memory then it will be used rather than reading from the db.
// 
const h3CacheFlushPeriod = (process.env.H3_CACHE_FLUSH_PERIOD_MINUTES||5)*60*1000;
const h3CacheExpiryTime = (process.env.H3_CACHE_EXPIRY_TIME_MINUTES||16)*60*1000;

const h3RollupPeriod = (process.env.ROLLUP_PERIOD_HOURS||60)*60*1000;

// how much detail to collect, bigger numbers = more cells! goes up fast see
// https://h3geo.org/docs/core-library/restable for what the sizes mean
const H3_STATION_CELL_LEVEL = (process.env.H3_STATION_CELL_LEVEL||8);
const H3_GLOBAL_CELL_LEVEL = (process.env.H3_GLOBAL_CELL_LEVEL||7);

// We need to use a protected data structure to generate ids
// for the station ID. This allows us to use atomics, will also
// support clustering if we need it
const sabbuffer = new SharedArrayBuffer(2);
const nextStation = new Uint16Array(sabbuffer);
let packetStats = { ignoredStation: 0, ignoredTracker:0, ignoredStationary:0, ignoredSignal0:0, ignoredPAW:0, count: 0 };

let currentAccumulator = [ 'current', 0 ];
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

	const now = new Date();
//	currentAccumulator = [ 'y', now.getUTCMinutes() ]; // get it in UTC

	// Track all the accumulators
	accumulators = { day: { bucket: now.getUTCMinutes(), previous: new Date(Date.now()-(24*3600+1)).getUTCMinutes() },
					 month: { bucket: now.getUTCMonth(), previous: (now.getUTCMonth()+11)%12 },
					 year:  { bucket: now.getUTCFullYear(), previous: now.getUTCFullYear()-1 }};
	
	console.log( currentAccumulator );
	console.log( accumulators );

	// Check and process unflushed accumulators at the start
	// then we can increment the current number for each accumulator merge
	

	console.log( `Configuration loaded DB@${dbPath} Output@${outputPath}` );

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
	
	await flushDirtyH3s(true);
	await produceOutputFiles();
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


//
// Connect to the APRS Server
async function startAprsListener( m = undefined ) {

	// In case we are using pm2 metrics
	metrics = m;
	let rawPacketCount = 0;
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
            const packet = parser.parseaprs(data);
            if( "latitude" in packet && "longitude" in packet &&
                "comment" in packet && packet.comment?.substr(0,2) == 'id' ) {
				processPacket( packet );
				rawPacketCount++;
            }
			else {
				if( (packet.destCallsign == 'OGNSDR' || data.match(/qAC/)) && ! ignoreStation(packet.sourceCallsign)) {

					if( packet.type == 'location' ) {	
						const stationid = getStationId( packet.sourceCallsign, true );
						stations[ packet.sourceCallsign ] = { ...stations[packet.sourceCallsign], lat: packet.latitude, lng: packet.longitude};
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
            console.log(data, '#', rawPacketCount);
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
		try { 
			for await ( const [key,value] of statusdb.iterator()) {
				stations[key] = JSON.parse(''+value)
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
		const rawPackets = (rawPacketCount - lastRawPacketCount);
		const pps = (packets/(h3CacheFlushPeriod/1000)).toFixed(1);
		const rawPps = (rawPackets/(h3CacheFlushPeriod/1000)).toFixed(1);
		const h3length = flushStats.total;
		const h3delta = h3length - lastH3length;
		const h3expired = flushStats.expired;
		const h3written = flushStats.written;
		console.log( `elevation cache: ${getCacheSize()}, openDbs: ${stationDbCache.size+2},  valid packets: ${packets} ${pps}/s, all packets ${rawPackets} ${rawPps}/s` );
		console.log( `total stations: ${nextStation-1}, seen stations ${Object.keys(stations).length}` );
		console.log( JSON.stringify(packetStats))
		console.log( `h3s: ${h3length} delta ${h3delta} (${(h3delta*100/h3length).toFixed(0)}%): `,
					 ` expired ${h3expired} (${(h3expired*100/h3length).toFixed(0)}%), written ${h3written} (${(h3written*100/h3length).toFixed(0)}%)`,
					 ` ${((h3written*100)/packets).toFixed(1)}% ${(h3written/(h3CacheFlushPeriod/1000)).toFixed(1)}/s ${(packets/h3written).toFixed(0)}:1`,
	); 

		// purge and flush H3s to disk
		// carry forward state for stats next time round
		lastPacketCount = packetStats.count;
		lastRawPacketCount = rawPacketCount;
		lastH3length = h3length;

	}, h3CacheFlushPeriod ));

	intervals.push(setInterval( async function() {

		const purgeBefore = (Date.now()/1000) - (process.env.FORGET_AIRCRAFT_AFTER_HOURS||12)*60*3600;
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
		
	}, h3CacheFlushPeriod*2.5 ));
	
	let doneOnce = false;
	intervals.push(setInterval( async function() {
//		const now = new Date();
		const oldAccumulator = [...currentAccumulator];

		// If the accumulator period has changed then we need to update
		// where we are writing
//		if( now.getUTCMinutes() != currentAccumulator[1] && ! doneOnce ) {
//			currentAccumulator = [ 'current', now.getUTCMinutes() ]; // get it in UTC

			// Make sure everything has been flushed - new ones should be into new accumulator
			// which we are ignoring anyway
			flushDirtyH3s(true).then( () => {
				// rollup old accumulator
				rollup( oldAccumulator );
			});
//		}
	}, h3RollupPeriod ));

	// Make sure we have these from existing DB as soon as possible
	produceOutputFiles();

	// On an interval we will dump out the coverage tables
/*	intervals.push(setInterval( function() {
		flushDirtyH3s(true).then( () => {
			produceOutputFiles()
		});
	}, (process.env.OUTPUT_INTERVAL_MIN||60)*60*1000));
*/	
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

function updateStationBuffer(stationid, h3k, br, altitude, agl, crc, signal, gap, release) {

	// Update the binary record with these values
	br.update( altitude, agl, crc, signal, gap, stationid );
	
	// 
	lastH3update.set(h3k.lockKey, Date.now());
	release();
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

		// If we have some unwritten changes for this h3 then we will simply
		// use the entry in the 'dirty' table. This table gets flushed
		// on a periodic basis
		const br = dirtyH3s.get(h3k.lockKey);
		if( br ) {
			updateStationBuffer( stationid, h3k, br, altitude, agl, crc, signal, gap, release() )
		}
		else {
			db.get( h3k.dbKey() )
			  .then( (value) => {
				  const buffer = new CoverageRecord( value );
				  dirtyH3s.set(h3k.lockKey,buffer);
				  updateStationBuffer( stationid, h3k, buffer, altitude, agl, crc, signal, gap, release() );
			  })
			  .catch( (err) => {
				  const buffer = new CoverageRecord( stationid ? bufferTypes.global : bufferTypes.station );
				  dirtyH3s.set(h3k.lockKey,buffer);
				  updateStationBuffer( stationid, h3k, buffer, altitude, agl, crc, signal, gap, release() );
			  });
		}
	});
	
}

// When did we last flush?
let lastDirtyWrite = 0;

//
// This function writes the H3 buffers to the disk if they are dirty, and 
// clears the records if it has expired. It is actually a synchronous function
// as it will not return before everything has been written
async function flushDirtyH3s(allUnwritten) {

	const flushPeriod = 3*60*1000;
	const nextDirtyWrite = Date.now();
	const expirypoint = Math.max( lastDirtyWrite-flushPeriod, 0 );

	let stats = {
		total: dirtyH3s.size,
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
	for (const [h3klockkey,v] of dirtyH3s) {

		promises.push( new Promise( (resolve) => {
		
			// Because the DB is asynchronous we need to ensure that only
			// one transaction is active for a given h3 at a time, this will
			// block all the other ones until the first completes, it's per db
			// no issues updating h3s in different dbs at the same time
			lock( h3klockkey, function (release) {
				
				const updateTime = lastH3update.get(h3klockkey);
				
				// If it's expired then we will purge it... 
				if( updateTime < expirypoint ) {
					dirtyH3s.delete(h3klockkey);
					lastH3update.delete(h3klockkey);
					stats.expired++;
				}
				// Only write if changes and not active
				else if( allUnwritten || updateTime < lastDirtyWrite ) {
					
					const h3k = new CoverageHeader(h3klockkey);
					
					// Add to the write out structures
					if( ! dbOps.has(h3k.dbid) ) {
						dbOps.set(h3k.dbid, new Array());
					}
					dbOps.get(h3k.dbid).push( { type: 'put', key: h3k.dbKey(), value: Buffer.from(v.buffer()) });
					stats.written++;
				}
				// we are done, no race condition on write as it's either written to the
				// disk above, or it was before and his simply expired, it's not possible
				// to expire and write (expiry is period after last write)
				release()();
				resolve();
			});
		}));
	}

	// We need to wait for all promises to complete before we can do the next part
	Promise.all( promises );
	promises = [];
	
	// So we know where to start writing
	lastDirtyWrite = nextDirtyWrite
	stats.databases = dbOps.size;

	// Now push these to the database
	for ( const [dbid,v] of dbOps ) {
		promises.push( new Promise( (resolve) => {
			//
			let db = (dbid != 0) ? stationDbCache.get(dbid) : globalDb;
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
			db.batch(v, (e) => {
				// log errors
				if(e) console.error('error flushing db operations for station id',dbid,e);
				resolve();
			});
		}));
	}
	Promise.all( promises );
	return stats;
}

//
// Rotate and Rollup all the data we have
// we do this by iterating through each database looking for things in default
// aggregator (which is always just the raw h3id)

async function rollup( accumulator ) {

	console.log( "rollup!!");
	const now = new Date();
	const nowEpoch = Math.floor(Date.now()/1000);
	const expiryEpoch = nowEpoch - ((process.env.STATION_EXPIRY_TIME_DAYS||31)*24*3600);
	
	const rollups = [ 'day', 'month', 'year' ];

	// Determine if any stations have had traffic before expiry
	const validStations = new Set();
	Object.keys(stations).forEach( (k) => {
		if( (stations[k].lastPacket||nowEpoch) > expiryEpoch ) {
			validStations.add(Number(stations[k].id));
		}
		else {
			console.log( `expiring ${k} no traffic for a while` );
		}
	});
	
	let db = globalDb;
	let station = 0;
	const name = (station||'global')
	{
		let dbOps = [];

		accumulators = { day: { bucket: now.getUTCDay(), previous: new Date(Date.now()-(24*3600+1)).getUTCDay() },
						 month: { bucket: now.getUTCMonth(), previous: (now.getUTCMonth()+11)%12 },
						 year:  { bucket: now.getUTCFullYear(), previous: now.getUTCFullYear()-1 }};
		//
		// Basically we finish our current accumulator into the active buckets for each of the others
		// and then we need to check if we should be moving them to new buckets or not
	// Start the writing process by initalising a set of serialisers for arrow data
//	let arrow = CoverageRecord.initArrow( (! station) ? bufferTypes.global : bufferTypes.station );
		
		// We step through all of the items together and update as one
		const rollupIterators = _map( rollups, (r) => {
			return { type:r,
					 iterator:db.iterator( CoverageHeader.getDbSearchRangeForAccumulator( r, accumulators[r].bucket )),
					 arrow: CoverageRecord.initArrow( (! station) ? bufferTypes.global : bufferTypes.station ),
			}
		});

		console.log('my accu',CoverageHeader.getDbSearchRangeForAccumulator(...accumulator));
		console.log( 'current h3k',(new CoverageHeader( 0, ...accumulator, '801212341234ffff' )).toString());

		// Initalise the rollup array with [k,v]
		const rollupData = _map( rollupIterators, (r) => {return { n:r.iterator.next(), ...r}} );

		/*
		for await ( const [key,value] of db.iterator()) {
			const h3kr = new CoverageHeader(key);
			if( h3kr.bucket == accumulator[1] ) {
				console.log( h3kr.toString() );
			}
		} */
		
		for await ( const [key,value] of db.iterator( CoverageHeader.getDbSearchRangeForAccumulator(...accumulator)) ) {

			// The 'current' value - ie the data we are merging in.
			const h3k = new CoverageHeader(key);
			const currentBr = new CoverageRecord(value);
			let advance = true;

			do {
				advance = true;
				
				// now we go through them in step form
				for( const r of rollupData ) {
					let n = r.n ? (await r.n) : undefined;
					let [prefixedh3r,rollupValue] = n || [null, null]; // iterator async so wait for it to complete

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
						}
						
						// We need to cleanup when we are done
						if( r.n ) {
							r.iterator.end();
							r.n = null;
						}
						continue;
					}
					
					const h3kr = new CoverageHeader(prefixedh3r);
					const ordering = CoverageHeader.compare(h3k,h3kr);
					
					// Need to wait for others to catch up and to advance current
					// (primary is less than rollup)
					if( ordering < 0 ) {
						continue;
					}
					
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
						}
						else {
							dbOps.push( { type: 'put', key: h3kr.dbKey(), value: Buffer.from(updatedBr.buffer()) });
						}
					}
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
						}
						else {
							dbOps.push( { type: 'put', key: h3kr.dbKey(), value: Buffer.from(updatedBr.buffer()) });
						}
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

			// We are going to write out our accumulators
			const accumulatorName = `${name}.${r.type}.${accumulators[r.type].bucket}.arrow`;

			// Finalise the arrow table so we can serialise it to the disk
			{
			const outputTable = CoverageRecord.finalizeArrow(r.arrow);
			const pt = new PassThrough( { objectMode: true } )
			const result = pt
				.pipe( RecordBatchWriter.throughNode())
				.pipe( createWriteStream( outputPath+accumulatorName ));
			
			pt.write(outputTable);
			pt.end();
			console.log( `written ${accumulatorName}` );
			}
			
			try {
				unlinkSync( outputPath+`${name}.${r.type}.arrow` );
			} catch(e) {
			}
			try {
				symlinkSync( outputPath+`${name}.${r.type}.arrow`, outputPath+accumulatorName, 'file' );
			} catch(e) {
				console.log( `error symlinking ${outputPath}${name}.${r.type}.arrow for ${outputPath+accumulatorName}` );
			}
		}

		console.table(dbOps.slice(0,10))
		dbOps = _sortby( dbOps, [ 'key' ])

		//
		// Finally execute all the operations on the database
		console.log( `Writing ${dbOps.length} rollup operations on the database ${db.ognStationName||'global'}` );
		const p = new Promise( (resolve) => {
			db.batch(dbOps, (e) => {
				// log errors
				if(e) console.error('error flushing db operations for station id',name,e);
				resolve();
			});
		});
		await p;
	
		console.log( `Done ${dbOps.length} rollup operations on the database ${db.ognStationName||'global'}` );
	}

	
}

//
// Dump all of the output files that need to be dumped, this goes through
// everything that may need to be written and writes it to the disk
async function produceOutputFiles() {
	console.log( `producing output files for ${stationDbCache.size} stations + global ` )

	let promises = [];
	
	// each of the stations
	stationDbCache.forEach( async function (db,key) {
		promises.push( new Promise( async function (resolve) {
			await produceOutputFile( db );
			resolve();
		}));
	});

	Promise.all(promises);
	
	// And the global output
	await produceStationFile( statusDb );
	await produceOutputFile( globalDb );

	// Flush old database from the cache
	stationDbCache.purgeStale();
}


//
// Produce the output based on the accumulations in the database
//
// This outputs using Apache Arrow columnar table format which can be loaded
// in a webworking by deck.gl speeding up the whole process.
//
// We generate a static file on a timed basis making it trivial for
// distributing the load amongst either many servers or a CDN cache
// it also has the advantage that you can keep a snapshot in time simply
// by keeping the file.
async function produceOutputFile( inputdb ) {

	const station = inputdb.stationName;
	
	// Form up meta data useful for the display, this needs to be written regardless
	//
	let now = Math.floor(Date.now()/1000);
	let outputDate = new Date().toISOString();
	let stationmeta = {	outputDate: outputDate, outputEpoch: now };
	if( station && station != 'global') {
		try {
			stationmeta = { ...stationmeta, meta: JSON.parse(await statusDb.get( station )) };
		} catch(e) {
			console.log( 'missing metadata for ', station );
		}
	}
	const name = (station||'global')
	writeFile( outputPath+name+'.json', JSON.stringify(stationmeta,null,2), (err) => err ? console.log("stationmeta write error",err) : null);
	
	// Check to see if we need to produce an output file, set to 1 when clean
	if( station && stations[station].lastPacket < stations[station].lastOutputFile ) {
		return 0;
	}

	// Start the writing process by initalising a set of serialisers for arrow data
	let arrow = CoverageRecord.initArrow( (! station) ? bufferTypes.global : bufferTypes.station );

	// Go throuh the whole database and load each record, parse it and add to the arrow
	// structure. The structure is data aware so will generate appropriate output for
	// each type
	for await ( const [key,value] of inputdb.iterator()) {
		let br = new CoverageRecord(value);
		br.appendToArrow( new CoverageHeader(''+key), arrow );
	}

	// Finalise the arrow table so we can serialise it to the disk
	const outputTable = CoverageRecord.finalizeArrow(arrow);

//	console.table( [...outputTable].slice(0,10))
	
	const pt = new PassThrough( { objectMode: true } )
	const result = pt
		.pipe( RecordBatchWriter.throughNode())
		.pipe( createWriteStream( outputPath+name+'.arrow' ));
	
	pt.write(outputTable);
	pt.end();

	// We are clean so we won't dump till new packets
	if( station ) {
		stations[station].outputDate = outputDate;
		stations[station].outputEpoch = now;
		stations[station].lastOutputFile = now;
	}
}

//
// Dump the meta data for all the stations, we take from our in memory copy
// it will have been primed on start from the db and then we update and flush
// back to the disk
async function produceStationFile( statusdb ) {

	// Form a list of hashes
	let statusOutput = _map( stations, (v,k) => {return { station: k, ...v };});

	// Write this to the stations.json file
	writeFile( outputPath + 'stations.json', JSON.stringify(statusOutput,null,2), (err) => err ? console.log("station write error",err) : null);
}
	

