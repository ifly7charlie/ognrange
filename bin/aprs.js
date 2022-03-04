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

import { ignoreStation } from '../lib/bin/ignorestation.js'

import { CoverageRecord, bufferTypes } from '../lib/bin/coveragerecord.js';
import { CoverageHeader } from '../lib/bin/coverageheader.js';

import { gitVersion } from '../lib/bin/gitversion.js';

import h3 from 'h3-js';

import _reduce from 'lodash.reduce';

let stations = {};
let globalDb = undefined;
let statusDb = undefined;

// track any setInterval/setTimeout calls so we can stop them when asked to exit
// also the async rollups we do during startup
let intervals = [];
let timeouts = {};
let startupPromise = null;

// APRS connection
let connection = {};
let aircraftStation = new Map();
let aircraftSeen = new Map();

// PM2 Metrics
let metrics = undefined;


import { mkdirSync } from 'fs';


// shortcuts so regexp compiled once
const reExtractDb = / ([0-9.]+)dB /;
const reExtractCrc = / ([0-9])e /;
const reExtractRot = / [+-]([0-9.]+)rot /;
const reExtractVC = / [+-]([0-9]+)fpm /;


import { ROLLUP_PERIOD_MINUTES,
		 NEXT_PUBLIC_SITEURL,
		 APRS_SERVER, APRS_TRAFFIC_FILTER,
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
		 STATION_DB_EXPIRY_MS,
	   } from '../lib/bin/config.js'

// h3 cache functions
import { cachedH3s, flushDirtyH3s, H3lock } from '../lib/bin/h3cache.js';

// Rollup functions
import { getAccumulator, rollupAll, updateAndProcessAccumulators, rollupStartup, rollupStats } from '../lib/bin/rollup.js';

// Get our git version
const gv = gitVersion().trim();

// We need to use a protected data structure to generate ids
// for the station ID. This allows us to use atomics, will also
// support clustering if we need it
const sabbuffer = new SharedArrayBuffer(2);
const nextStation = new Uint16Array(sabbuffer);
let packetStats = { ignoredStation: 0, ignoredTracker:0, ignoredStationary:0, ignoredSignal0:0, ignoredPAW:0, count: 0, rawCount:0 };

// Least Recently Used cache for Station Database connectiosn 
import LRU from 'lru-cache'
const options = { max: MAX_STATION_DBS,
				  dispose: function (db, key, r) {
					  try { db.close(); } catch (e) { console.log('ummm',e); }
					  if( stationDbCache.getTtl(key) < (H3_CACHE_FLUSH_PERIOD_MS/1000) ) {
						  console.log( `Closing database ${key} while it's still needed. You should increase MAX_STATION_DBS in .env.local` );
					  }
				  },
				  updateAgeOnGet: true, allowStale: true,
				  ttl: STATION_DB_EXPIRY_MS }
	, stationDbCache = new LRU(options)

stationDbCache.getTtl = (k) => {
	return (typeof performance === 'object' && performance &&
	 typeof performance.now === 'function' ? performance : Date).now() - stationDbCache.starts[stationDbCache.keyMap.get(k)]
}



// Run stuff magically
main()
    .then("exiting");

//
// Primary configuration loading and start the aprs receiver
async function main() {

	if( ROLLUP_PERIOD_MINUTES < 12 ) {
		console.log( `ROLLUP_PERIOD_MINUTES is too short, it must be more than 12 minutes` );
		process.exit();
	}

	console.log( `Configuration loaded DB@${DB_PATH} Output@${OUTPUT_PATH}, Version ${gv}` );

	// Make sure our paths exist
	try { 
		mkdirSync(DB_PATH+'stations', {recursive:true});
		mkdirSync(OUTPUT_PATH, {recursive:true});
	} catch(e) {};

	// Open our databases
	globalDb = LevelUP(LevelDOWN(DB_PATH+'global'))
	statusDb = LevelUP(LevelDOWN(DB_PATH+'status'))

	// Load the status of the current stations
	async function loadStationStatus(statusdb) {
		const nowEpoch = Math.floor(Date.now()/1000);
		const expiryEpoch = nowEpoch - STATION_EXPIRY_TIME_SECS;
		try { 
			for await ( const [key,value] of statusdb.iterator()) {
				stations[key] = JSON.parse(String(value))
			}
		} catch(e) {
			console.log('Unable to loadStationStatus',e)
		}
		const nextid = (_reduce( stations, (highest,i) => { return highest < (i.id||0) ? i.id : highest }, 0 )||0)+1;
		console.log( 'next station id', nextid );
		return nextid;
	}
	console.log( 'loading station status' );
	Atomics.store(nextStation, 0, (await loadStationStatus(statusDb))||1)

	// Check and process unflushed accumulators at the start
	// then we can increment the current number for each accumulator merge
	await (startupPromise = rollupStartup({globalDb, statusDb, stationDbCache, stations}));
	await (startupPromise = updateAndProcessAccumulators({globalDb, statusDb, stationDbCache, stations}));
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
	console.log(`${signal}: flushing data`)
	if( connection ) {
		connection.exiting = true;
		connection.disconnect && connection.disconnect()
	}

	if( startupPromise ) {
		console.log( 'waiting for startup to finish' );
		await startupPromise;
	}
	
	for( const i of intervals ) {
		clearInterval(i);
	}
	for( const i of Object.values(timeouts) ) {
		clearTimeout(i);
	}
	if( connection && connection.interval ) {
		clearInterval( connection.interval );
	}
		
	// Flush everything to disk
	console.log( await flushDirtyH3s( {globalDb, stationDbCache, stations, allUnwritten:true } ));
	if( getAccumulator() ) {
		await rollupAll( {globalDb, statusDb, stationDbCache, stations} );
	}
	else {
		console.log( `unable to output a rollup as service still starting` );
	}

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

// dump out? not good idea really better to exit and restart
process.on('SIGUSR1', async function () {
	console.log( '-- data dump requested --' );
	await flushDirtyH3s({globalDb, stationDbCache, stations, allUnwritten:true });
	rollupAll( {globalDb, statusDb, stationDbCache, stations} );
});

//
// Connect to the APRS Server
async function startAprsListener() {

    // Settings for connecting to the APRS server
    const CALLSIGN = NEXT_PUBLIC_SITEURL;
    const PASSCODE = -1;
    const [ APRSSERVER, PORTNUMBER ] = APRS_SERVER.split(':')||['aprs.glidernet.org','14580'];

	// If we were connected then cleanup the old stuff
	if( connection ) {
		console.log( `reconnecting to ${APRSSERVER}:${PORTNUMBER}` );
		try {
			connection.disconnect();
			clearInterval( connection.interval );
		} catch(e) {
		}
		connection = null;
	}
	
    // Connect to the APRS server
    connection = new ISSocket(APRSSERVER, parseInt(PORTNUMBER)||14580, 'OGNRANGE', '', APRS_TRAFFIC_FILTER, `ognrange v${gv}` );
    let parser = new aprsParser();

    // Handle a connect
    connection.on('connect', () => {
        connection.sendLine( connection.userLogin );
        connection.sendLine(`# ognrange ${CALLSIGN} ${gv}`);
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
								details.notice = `${Math.round(distance)}km move detected ${(new Date(packet.timestamp*1000)).toISOString()} resetting history`;
								details.moved = true; // we need to persist this
								console.log( `station ${packet.sourceCallsign} has moved location from ${details.lat},${details.lng} to ${packet.latitude},${packet.longitude} which is ${distance.toFixed(1)}km`);
								statusDb.put( packet.sourceCallsign, JSON.stringify(details) );
							}
						}
						details.lat = packet.latitude;
						details.lng = packet.longitude;
						details.lastLocation = packet.timestamp;
					}
					else if( packet.type == 'status' ) {
						const stationid = getStationId( packet.sourceCallsign, false ); // don't write as we do it in next line		
						const details = stations[ packet.sourceCallsign ];	
						details.lastBeacon = packet.timestamp;
						statusDb.put( packet.sourceCallsign, JSON.stringify({ ...details, status: packet.body }) );
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

    // Failed to connect, will create a new connection at the next periodic interval
    connection.on('error', (err) => {
		if( ! connection.exiting ) {
			console.log('Error: ' + err);
			connection.disconnect();
			connection.valid = false;
		}
    });


	if( ! connection || connection.exiting ) {
		return;
	}
	
    // Start the APRS connection
    connection.connect();
	
	// And every (APRS_KEEPALIVE_PERIOD) minutes we need to confirm the APRS
	// connection has had some traffic, and reconnect if not
	connection.interval = setInterval( function() {
		try {
			// Send APRS keep alive or we will get dumped
			connection.sendLine(`# ${CALLSIGN} ${gv}`);
		} catch(e) {
			console.log( `exception ${e} in sendLine status` );
		}
		
        // Re-establish the APRS connection if we haven't had anything in
        if( !connection || ((! connection.isConnected() || ! connection.valid) && ! connection.exiting)) {
            console.log( "failed APRS connection, retrying" );
			try {
				connection.disconnect();
			} catch(e) {
			}
			// We want to restart the APRS listener if this happens
			startAprsListener();
        }
		if( connection ) {
			connection.valid = false; // reset by receiving a packet
		}
	}, APRS_KEEPALIVE_PERIOD_MS );
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
	intervals.push(setInterval( async function() {

		// Flush the cache
		const flushStats = await flushDirtyH3s({globalDb, stationDbCache, stations, allUnwritten:false });

		// Report some status on that
		const packets = (packetStats.count - lastPacketCount);
		const rawPackets = (packetStats.rawCount - lastRawPacketCount);
		const pps = packetStats.pps = (packets/(H3_CACHE_FLUSH_PERIOD_MS/1000)).toFixed(1);
		const rawPps = packetStats.rawPps = (rawPackets/(H3_CACHE_FLUSH_PERIOD_MS/1000)).toFixed(1);
		const h3length = flushStats.total;
		const h3delta = h3length - lastH3length;
		const h3expired = flushStats.expired;
		const h3written = flushStats.written;
		console.log( `elevation cache: ${getCacheSize()}, valid packets: ${packets} ${pps}/s, all packets ${rawPackets} ${rawPps}/s` );
		console.log( `total stations: ${nextStation-1}, openDbs: ${stationDbCache.size+2}/${MAX_STATION_DBS}` );
		console.log( JSON.stringify(packetStats))
		console.log( JSON.stringify(rollupStats))
		console.log( `h3s: ${h3length} delta ${h3delta} (${(h3delta*100/h3length).toFixed(0)}%): `,
					 ` expired ${h3expired} (${(h3expired*100/h3length).toFixed(0)}%), written ${h3written} (${(h3written*100/h3length).toFixed(0)}%)[${flushStats.databases} stations]`,
					 ` ${((h3written*100)/packets).toFixed(1)}% ${(h3written/(H3_CACHE_FLUSH_PERIOD_MS/1000)).toFixed(1)}/s ${(packets/h3written).toFixed(0)}:1`,
				   );

		// Although this isn't an error it does mean that there will be churn in the DB cache and
		// that will increase load - which is not ideal because we are obviously busy otherwise we wouldn't have
		// so many stations sending us traffic...
		if( flushStats.databases > MAX_STATION_DBS*0.9 ) {
			console.log( `** please increase the database cache (MAX_STATION_DBS) it should be larger than the number of stations receiving traffic in H3_CACHE_FLUSH_PERIOD_MINUTES (${H3_CACHE_FLUSH_PERIOD_MINUTES})` );
		}

		// purge and flush H3s to disk
		// carry forward state for stats next time round
		lastPacketCount = packetStats.count;
		lastRawPacketCount = packetStats.rawCount;
		lastH3length = h3length;

	}, H3_CACHE_FLUSH_PERIOD_MS ));

	timeouts['forget'] = (setTimeout( () => {
		delete timeouts['forget'];
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
	const now = new Date();
	const nextRollup = ROLLUP_PERIOD_MINUTES - (((now.getUTCHours()*60)+now.getUTCMinutes())%ROLLUP_PERIOD_MINUTES);
	console.log( `first rollup will be in ${nextRollup} minutes at ${new Date(Date.now()+nextRollup*60000+500).toISOString()}` );
	timeouts['rollup'] = (setTimeout( async function() {
		delete timeouts['rollup'];
		intervals.push(setInterval( async function() {
			updateAndProcessAccumulators( {globalDb, statusDb, stationDbCache, stations} );
			console.log( `next rollup will be in ${ROLLUP_PERIOD_MINUTES} minutes at ` + 
						 `${new Date(Date.now()+ROLLUP_PERIOD_MINUTES*60000+500).toISOString()}` );
		}, ROLLUP_PERIOD_MINUTES * 60*1000 ));
		// this shouldn't drift because it's an interval...
		updateAndProcessAccumulators( {globalDb, statusDb, stationDbCache, stations} ); // do the first one, then let the interval do them afterwards
	}, (nextRollup*60*1000)+500));
	// how long till they roll over, delayed 1/2 a second + whatever remainder was left in getUTCSeconds()...
	// better a little late than too early as it won't rollover then and we will wait a whole period to pick it up

}

function displayStatus() {
	console.log( `elevation cache: ${getCacheSize()}, h3cache: ${cachedH3s.size},  valid packets: ${packetStats.count} ${packetStats.pps}/s, all packets ${packetStats.rawCount} ${packetStats.rawPps}/s` );
	console.log( `total stations: ${nextStation-1}, openDbs: ${stationDbCache.size+2}/${MAX_STATION_DBS}` );
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
		stationDbCache.set(stationid, stationDb = LevelUP(LevelDOWN(DB_PATH+'/stations/'+station)))
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
	H3lock( h3k.lockKey, function (release) {

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

