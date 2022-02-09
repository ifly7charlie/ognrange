// Import the APRS server
import { ISSocket } from 'js-aprs-is';
import { aprsParser } from  'js-aprs-fap';

// Correction factors
//import { altitudeOffsetAdjust } from '../offsets.js';
//import { getOffset } from '../egm96.mjs';

// Height above ground calculations, uses mapbox to get height for point
//import geo from './lib/getelevationoffset.js';
import { getCacheSize, getElevationOffset } from '../lib/bin/getelevationoffset.js'

// Helper function for geometry
import distance from '@turf/distance';
import { point } from '@turf/helpers';

import LevelUP from 'levelup';
import LevelDOWN from 'rocksdb';

import dotenv from 'dotenv';

import { ignoreStation } from '../lib/bin/ignorestation.js'

import h3 from 'h3-js';

import _findindex from 'lodash.findindex';
import _zip from 'lodash.zip';
import _map from 'lodash.map';
import _reduce from 'lodash.reduce';
import _sortby from 'lodash.sortby';
import _reject from 'lodash.reject';

// DB locking
import { Lock } from 'lock'
let lock = Lock();

let stations = {};
let globalDb = undefined;
let statusDb = undefined;

// Least Recently Used cache for Station Database connectiosn 
import LRU from 'lru-cache'
const options = { max: parseInt(process.env.MAX_STATION_DBS)||3200,
				  dispose: function (key, n,r) { n.close(); console.log( `closed database for ${key}, ${r} ${(Date.now()-n.ognInitialTS)/1000/3600}h` ); },
				  updateAgeOnGet: true, allowStale: true,
				  ttl: (process.env.STATION_DB_EXPIRY_HOURS||12) * 3600 * 1000 }
, stationDbCache = new LRU(options)


// track any setInterval calls so we can stop them when asked to exit
let intervals = [];

// APRS connection
let connection = {};
let gliders = {};

// PM2 Metrics
let metrics = undefined;

// We serialise to disk using protobuf
//import protobuf from 'protobufjs/light.js';
//import { OnglideRangeMessage } from '../lib/range-protobuf.mjs';
import { writeFile, mkdirSync, createWriteStream } from 'fs';
import { PassThrough } from 'stream';

import { makeTable, tableFromArrays, RecordBatchWriter, makeVector, FixedSizeBinary,
		 Utf8, Uint8, makeBuilder } from 'apache-arrow';

// How many stations we will report to the front end, list is sorted so this means
// 20 busiest stations
const numberOfStationsToTrack = 20;

// Default paths, can be overloaded using .env.local
let dbPath = './db/';
let outputPath = './public/data/';

// shortcuts so regexp compiled once
const reExtractDb = / ([0-9.]+)dB /
const reExtractCrc = / ([0-9])c /

// Cache so we aren't constantly reading/writing from the db
let dirtyH3s = new Map();
let lastH3update = new Map();
let lastPacketCount = 0;
let lastPacketsInInterval = 0;
let lastH3length = 0;

// APRS Server Keep Alive
const aprsKeepAlivePeriod = process.env.APRS_KEEPALIVE_PERIOD||2 * 60 * 1000;

// Cache control - we cache the datablocks by station and h3 to save us needing
// to read/write them from/to the DB constantly. Note that this can use quite a lot
// of memory, but is a lot easier on the computer
//
// - flush period is how long they can remain in memory without being written
// - expirytime is how long they can remain in memory without being purged. If it
//   is in memory then it will be used rather than reading from the db.
// 
const h3CacheFlushPeriod = (process.env.H3_CACHE_FLUSH_PERIOD||5)*60*1000;
const h3CacheExpiryTime = (process.env.H3_CACHE_EXPIRY_TIME||14)*60*1000;


// We need to use a protected data structure to generate ids
// for the station ID. This allows us to use atomics, will also
// support clustering if we need it
const sabbuffer = new SharedArrayBuffer(2);
const nextStation = new Uint16Array(sabbuffer);

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
	flushDirtyH3s();
	await produceOutputFiles();
	stationDbCache.forEach( async function (db,key) {
		db.close();
	});
	globalDb.close();
	statusDb.close();
	for( const i of intervals ) {
		clearInterval(i);
	}
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
	let packetCount = 0;

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
        connection.valid = true;
        if(data.charAt(0) != '#' && !data.startsWith('user')) {
            const packet = parser.parseaprs(data);
            if( "latitude" in packet && "longitude" in packet &&
                "comment" in packet && packet.comment?.substr(0,2) == 'id' ) {
				processPacket( packet );
				packetCount++;
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
            console.log(data, '#', packetCount);
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
        if( ! connection.valid ) {
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
		const packets = (packetCount - lastPacketCount);
		const pps = (packets/(h3CacheFlushPeriod/1000)).toFixed(1);
		const h3length = flushStats.total;
		const h3delta = h3length - lastH3length;
		const h3expired = flushStats.expired;
		const h3written = flushStats.written;
		console.log( `elevation cache: ${getCacheSize()}, openDbs: ${stationDbCache.length+2},  packets: ${packetCount} ${pps}/s` );
		console.log( `total stations: ${nextStation-1}, seen stations ${Object.keys(stations).length}` );
		console.log( `h3s: ${h3length} delta ${h3delta} (${(h3delta/h3length).toFixed(0)}%: `,
					 ` expired ${h3expired} (${(h3expired*100/h3length).toFixed(0)}%), written ${h3written} (${(h3written*100/h3length).toFixed(0)}%)`,
					 ` ${((h3written*100)/packets).toFixed(1)}% ${(h3written/(h3CacheFlushPeriod/1000)).toFixed(1)}/s ${(packets/h3written).toFixed(0)}:1`,
	); 

		// purge and flush H3s to disk
		// carry forward state for stats next time round
		lastPacketCount = packetCount;
		lastH3length = h3length;
		lastPacketsInInterval = packets;

	}, h3CacheFlushPeriod ));

	// Make sure we have these from existing DB as soon as possible
	produceOutputFiles();

	// On an interval we will dump out the coverage tables
	intervals.push(setInterval( function() {
		produceOutputFiles();
	}, (process.env.OUTPUT_INTERVAL_MIN||15)*60*1000));
	
}


function getStationId( station, serialise = true ) {
	// Figure out which station we are - this is synchronous though don't really
	// understand why the put can't happen in the background
	let stationid = undefined;
	if( station ) {
		if( ! stations[ station ] ) {
			stations[station]={}
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

	if( ignoreStation( sender ) ) {
		return;
	}
	if( packet.destCallsign == 'OGNTRK' && packet.digipeaters?.[0]?.callsign?.slice(0,2) != 'qA' ) {
		console.log( 'TRKREPEAT:', packet.origpacket );
		return;
	}

	// Protect from file system injection - we only accept normal characters
	if( ! sender.match(/^([a-zA-Z0-9]+)$/i)) {
		console.log( 'invalid sender', sender )
		return;
	}
	
	// Apply the correction
    let altitude = Math.floor(packet.altitude);

	// geojson for helper function slater
	const jPoint = point( [packet.latitude, packet.longitude] );

    // Check if the packet is late, based on previous packets for the glider
    const now = (new Date()).getTime()/1000;
    const td = Math.floor(now - packet.timestamp);

    // Look it up, have we had packets for this before?
	// we use this simply to make sure we aren't constantly recording points
	// for stationary gliders
    let glider = gliders[flarmId];
	if( ! glider ) {
		glider = gliders[flarmId] = { lastTime: packet.timestamp };
	}

    // Check to make sure they have moved more than 50 meters, thermal circle normally 100+ m
    // this reduces load from stationary gliders on the ground and allows us to track stationary gliders
    // better. the 1 ensures that first packet gets picked up after restart
    const distanceFromLast = glider.lastPoint ? distance( jPoint, glider.lastPoint ) : 1;
	const elapsedTime = packet.timestamp - glider.lastTime;
	
    if( distanceFromLast < 0.05  ) {
		glider.stationary++;
        return;
    }

	// If it has moved then save it away to make sure we aren't stationary
    if( packet.timestamp > glider.lastTime ) {        
		glider.lastPoint = jPoint;
        glider.lastTime = packet.timestamp;
    }

	// Look for signal strength and checksum - we will ignore any packet without a signal strength
	// sometimes this happens to be missing and other times it happens because it is reported as 0.0
	const rawStrength = (packet.comment.match(reExtractDb)||[0,0])[1];
	const strength = Math.round(parseFloat(rawStrength));

	// crc may be absent, if it is then it's a 0
	const crc = parseInt((packet.comment.match(reExtractCrc)||[0,0])[1]);

	// If we have no signal strength then we'll ignore the packet... don't know where these
	// come from or why they exist...
	if( strength > 0 ) {


		// Can't use these as we don't actually know what they are so ignore them for now
		if( rawStrength == 20.0 && pawTracker ) {
//			console.log( pawTracker?'ignoring paw tracker':'??', packet.origpacket );
			return;
		}

		// Enrich with elevation and send to everybody, this is async
		getElevationOffset( packet.latitude, packet.longitude,
							async (gl) => {
								const agl = Math.round(Math.max(altitude-gl,0));
								packetCallback( sender, h3.geoToH3(packet.latitude, packet.longitude, 8), altitude, agl, glider, crc, strength );
		});
	}
}

//
// Actually serialise the packet into the database after processing the data
async function packetCallback( station, h3id, altitude, agl, glider, crc, signal ) {

	// Open the database, do this first as takes a bit of time
	let stationDb = stationDbCache.get(station);
	if( ! stationDb ) {
		stationDbCache.set(station, stationDb = LevelUP(LevelDOWN(dbPath+'/stations/'+station)))
		stationDb.ognInitialTS = Date.now();
	}

	// Find the id for the station or allocate
	const stationid = await getStationId( station );

	// Packet for station marks it for dumping next time round
	stations[station].clean = false;

	// Merge into both the station db (0,0) and the global db with the stationid we allocated
	// we don't pass stationid into the station specific db because there only ever is one
	// it gets used to build the list of stations that can see the cell
	mergeDataIntoDatabase( 0,          station, stationDb, h3id, altitude, agl, crc, signal );
	mergeDataIntoDatabase( stationid, 'global', globalDb, h3.h3ToParent(h3id,7), altitude, agl, crc, signal);
}

//
// Generate a fake structure that maps directly into the byte array we are working with
const normalLength = 3*4+3*2+2;
function mapping(buffer) {
		const extraStationSlots = (buffer.byteLength - normalLength)/6;
												  
		return {
			count: new Uint32Array(buffer, 0*4,1),
			sumSig: new Uint32Array(buffer,1*4,1),
			sumCrc: new Uint32Array(buffer,2*4,1),
			minAltAgl: new Uint16Array(buffer,3*4,1),
			minAlt: new Uint16Array(buffer,3*4+1*2,1),
			minAltMaxSig: new Uint8Array(buffer,3*4+2*2,1),
			maxSig: new Uint8Array(buffer,3*4+3*2+1,1),
			extra: ! extraStationSlots ? undefined : {
				number: extraStationSlots,
				stations: new Uint16Array( buffer, normalLength, extraStationSlots ),
				count: new Uint32Array( buffer, normalLength + extraStationSlots*2, extraStationSlots )
			}
		}
}

function updateStationBuffer(stationid, dbname, h3, buffer, altitude, agl, crc, signal, release) {
	
	// We may change the output if we extend it
	let outputBuffer = buffer;
	
	// Lets get the structured data (this is a bit of a hack but TypedArray on a buffer
	// will just expose a window into existing buffer so we don't have to do any fancy math.
	// ...[0] is only option available as we only map one entry to each item
	let existing = mapping(buffer);
	if( ! existing.minAlt[0] || existing.minAlt[0] > altitude ) {
		existing.minAlt[0] = altitude;
		
		if( existing.minAltMaxSig[0] < signal ) {
			existing.minAltMaxSig[0] = signal;
		}
	}
	if( ! existing.minAltAgl[0] || existing.minAltAgl[0] > agl ) {
		existing.minAltAgl[0] = agl;
	}
	
	if( existing.maxSig[0] < signal ) {
		existing.maxSig[0] = signal;
	}
	
	existing.sumSig[0] += signal;
	existing.sumCrc[0] += crc;
	existing.count[0] ++;

	// For global squares we also maintain a station list
	// we do this by storing a the stationid and count for each one that hits the
	// square, 
	// - note this will occasionally invalidate the existing structure
	// as it allocates a new buffer if more space is needed.
	// It should probably sort the list as well
	// but...
	if( stationid && existing.extra ) {
		
		let updateIdx = _findindex(existing.extra.stations,(f)=>(f == stationid));
		if( updateIdx >= 0 ) {
			existing.extra.count[updateIdx]++;
		}
		else {
			// Look for empty slot, if we find it then we update,
			updateIdx = _findindex(existing.extra.stations,(f)=>(f == 0));
			if( updateIdx >= 0 ) {
				existing.extra.stations[updateIdx] = stationid;
				existing.extra.count[updateIdx] = 1;
			}
			else {
				// New buffer and copy into it, we will expand by two stations at a time
				// it's not generous but should be ok
				let nb = new Uint8Array( buffer.byteLength + 12 );
				nb.set( buffer, 0 );

				// We know where it goes so just put the data directly there rather
				// than remapping the whole data structure
				let s = new Uint16Array( nb, buffer.byteLength, 1 );
				let c = new Uint32Array( nb, buffer.byteLength+4, 1 );
				s[0] = stationid;
				c[0] = 1;

				//
				outputBuffer = nb.buffer;
			}
		}
	}

	// Save back to db, either original or updated buffer
	//		db.put( h3, Buffer.from(outputBuffer), {}, release );
	const cacheKey = dbname+','+h3;
	lastH3update.set(cacheKey, Date.now());
	dirtyH3s.set(cacheKey,Buffer.from(outputBuffer));
	release();
}

//
// We store the database records as binary bytes - in the format described in the mapping() above
// this reduces the amount of storage we need and means we aren't constantly parsing text
// and printing text.
async function mergeDataIntoDatabase( stationid, dbname, db, h3, altitude, agl, crc, signal ) {

	// Because the DB is asynchronous we need to ensure that only
	// one transaction is active for a given h3 at a time, this will
	// block all the other ones until the first completes, it's per db
	// no issues updating h3s in different dbs at the same time
	lock( dbname+h3, function (release) {

		// If we have some unwritten changes for this h3 then we will simply
		// use the entry in the 'dirty' table. This table gets flushed
		// on a periodic basis
		const cacheKey = dbname+','+h3;
		const cacheValue = dirtyH3s.get(cacheKey);
		if( cacheValue ) {
			updateStationBuffer( stationid, dbname, h3, cacheValue, altitude, agl, crc, signal, release() )
		}
		else {
			db.get( h3 )
			  .then( (value) => {
				  updateStationBuffer( stationid, dbname, h3, value.buffer, altitude, agl, crc, signal, release() );
			  })
			  .catch( (err) => {
				  // Allocate a new buffer to update and then set it in the database -
				  // if we have a station id then twe will make sure we reserve some space
				  // because we keep a list of all stations seen in the global cells
				  let buffer = new Uint8Array( normalLength + (stationid ? 12 : 0) );
				  updateStationBuffer( stationid, dbname, h3, buffer.buffer, altitude, agl, crc, signal, release() );
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
async function flushDirtyH3s() {

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
	for (const [k,v] of dirtyH3s) {
		const [ dbname, h3 ] =  (''+k).split(',');

		promises.push( new Promise( (resolve) => {
		
			// Because the DB is asynchronous we need to ensure that only
			// one transaction is active for a given h3 at a time, this will
			// block all the other ones until the first completes, it's per db
			// no issues updating h3s in different dbs at the same time
			lock( dbname+h3, function (release) {
				
				const updateTime = lastH3update.get(k);
				
				// Only write if changes
				if( updateTime >= lastDirtyWrite ) {
					
					// Add to the write out structures
					if( ! dbOps.has(dbname) ) {
						dbOps.set(dbname, new Array());
					}
					dbOps.get(dbname).push( { type: 'put', key: h3, value: Buffer.from(v) });
					stats.written++;
				}
				
				// If it's expired then we will purge it... 
				else if( updateTime < expirypoint ) {
					dirtyH3s.delete(k);
					lastH3update.delete(k);
					stats.expired++;
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
	for ( const [station,v] of dbOps ) {
		promises.push( new Promise( (resolve) => {
			//
			let db = (station != 'global') ? stationDbCache.get(station) : globalDb;
			if( ! db ) {
				console.log( `weirdly opening db to write for cache ${dbname}` );
				stationDbCache.set(station, db = LevelUP(LevelDOWN(dbPath+'/stations/'+station)))	
				db.ognInitialTS = Date.now();
			}
			db.batch(v, (e) => {
				// log errors
				if(e) console.err('error flushing db operations for station',station,e);
				resolve();
			});
		}));
	}
	Promise.all( promises );
	return stats;
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
			await produceOutputFile( key, db );
			resolve();
		}));
	});

	Promise.all(promises);
	
	// And the global output
	await produceStationFile( statusDb );
	await produceOutputFile( 'global', globalDb );

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
async function produceOutputFile( station, inputdb ) {
	let length = 500;
	let position = 0;

	// Form up meta data useful for the display, this needs to be written regardless
	// 
	let stationmeta = {	outputDate: new Date().toISOString() };
	if( station && station != 'global') {
		try {
			stationmeta = { ...stationmeta, meta: JSON.parse(await statusDb.get( station )) };
		} catch(e) {
			console.log( 'missing metadata for ', station );
		}
	}
	writeFile( outputPath+(station?station:'global')+'.json', JSON.stringify(stationmeta,null,2), (err) => err ? console.log("stationmeta write error",err) : null);
	
	// Check to see if we need to produce an output file, set to 1 when clean
	if( station != 'global' && stations[station].clean ) {
		return 0;
	}
	
	// Helper for resizing TypedArrays so we don't end up with them being huge
	function resize( a, b ) {
		let c = new a.constructor( b );	c.set( a );	return c;
	}

	// for global we need a list of stations
	let stationsBuilder = station == 'global' ?
						  makeBuilder({type: new Utf8()}):undefined;
	let stationsCount = station == 'global' ?
						makeBuilder({type: new Uint8()}):undefined;

	// we have two of these for each record
	const lengthMultiplier = { h3out: 2 };

	let data = { 
		h3out: new Uint32Array( length*2 ),
		minAgl: new Uint16Array( length ),
		minAlt: new Uint16Array( length ),
		minAltSig: new Uint8Array( length ),
		avgSig: new Uint8Array( length ),
		maxSig: new Uint8Array( length ),
		avgCrc: new Uint8Array( length ),
		count: new Uint32Array( length ),
	};


	// Go through all the keys
	for await ( const [key,value] of inputdb.iterator()) {
		let c = mapping(value.buffer);

		// Save them in the output arrays
		const lh = h3.h3IndexToSplitLong(''+key);
		data.h3out[position*2] = lh[0];
		data.h3out[position*2+1] = lh[1];
		data.minAgl[position] = c.minAltAgl[0];
		data.minAlt[position] = c.minAlt[0];
		data.minAltSig[position] = c.minAltMaxSig[0];
		data.maxSig[position] = c.maxSig[0];
		data.count[position] = c.count[0];
		
		// Calculated ones
		data.avgSig[position] = Math.round(c.sumSig[0] / c.count[0]);
		data.avgCrc[position] = Math.round(c.sumCrc[0] / c.count[0]);

		if( c.extra ) {
			const zip = _sortby( _reject( _zip( c.extra.stations, c.extra.count ),
										(r)=>!r[1] ),
							   (s) => s[1] );
			const o =  _map(zip, (f) => f[0].toString(16)).join(',');
			stationsBuilder.append(o)
			stationsCount.append(zip.length)
		}

		// And make sure we don't run out of space
		position++;
		if( position == length ) {
			length += 500;
			for ( const k of Object.keys(data)) {
				data[k] = resize( data[k], length*(lengthMultiplier[k]||1) );
			}
		}
	}

	// Now we have all the data we need we put each one into protobuf and serialise it to disk
	function output( name, data ) {
		const stationsInject = station == 'global' ? { stations: stationsBuilder.finish().toVector(), scount: stationsCount.finish().toVector() } : {};
		const outputTable = makeTable({
			h3: new Uint32Array(data.h3out.buffer,0,position*2),
			minAgl: new Uint16Array(data.minAgl,0,position),
			minAlt: new Uint16Array(data.minAlt,0,position),
			minAltSig: new Uint8Array(data.minAltSig,0,position),
			maxSig: new Uint8Array(data.maxSig,0,position),
			count: new Uint32Array(data.count,0,position),
			avgSig: new Uint8Array(data.avgSig,0,position),
			avgCrc: new Uint8Array(data.avgCrc,0,position),
			...stationsInject
		});

//		console.table( [...outputTable].slice(0,100))

		const pt = new PassThrough( { objectMode: true } )
		const result = pt
			  .pipe( RecordBatchWriter.throughNode())
			  .pipe( createWriteStream( './public/data/'+name+'.arrow' ));
						
		pt.write(outputTable);
		pt.end();
	}

	// Output the station information
	output( station, data );

	// We are clean so we won't dump till new packets
	if( station != 'global' ) {
		stations[station].clean = 1;
	}
	return position;
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
	

