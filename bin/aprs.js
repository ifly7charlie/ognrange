// Import the APRS server
import { ISSocket } from 'js-aprs-is';
import { aprsParser } from  'js-aprs-fap';

// Correction factors
//import { altitudeOffsetAdjust } from '../offsets.js';
//import { getOffset } from '../egm96.mjs';

// Height above ground calculations, uses mapbox to get height for point
//import geo from './lib/getelevationoffset.js';
import { getElevationOffset } from '../lib/bin/getelevationoffset.js'

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
let stationDbs = {};
let globalDb = undefined;
let statusDb = undefined;

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
		 Utf8, makeBuilder } from 'apache-arrow';

const numberOfStationsToTrack = 20;

let dbPath = './db/';
let outputPath = './public/data/';

//
const sabbuffer = new SharedArrayBuffer(2);
const nextStation = new Uint16Array(sabbuffer);

// Set up background fetching of the competition
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
	
	await startAprsListener();
}

main()
    .then("exiting");

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
        console.log('Error: ' + err);
        connection.disconnect();
        connection.connect();
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

	// And every 2 minutes we need to confirm the APRS
	// connection has had some traffic
	setInterval( function() {

		try {
			// Send APRS keep alive or we will get dumped
			connection.sendLine(`# ${CALLSIGN} ${process.env.NEXT_PUBLIC_WEBSOCKET_HOST}`);
		} catch(e) {
			console.log( `exception ${e} in sendLine status` );
			connection.valud = false;
		}
		
        // Re-establish the APRS connection if we haven't had anything in
        if( ! connection.valid ) {
            console.log( "failed APRS connection, retrying" );
            connection.disconnect( () => { connection.connect() } );
        }
        connection.valid = false;
	}, 2*60*1000);

	// Make sure we have these from existing DB as soon as possible
	produceStationFile( statusDb );	
	produceOutputFiles( 'global', globalDb );

	// On an interval we will dump out the coverage tables
	setInterval( function() {
		console.log( `producing output files for ${Object.keys(statusDb).length} stations + global ` )

		// each of the stations
		for( const station of Object.keys(stationDbs) ) {
			produceOutputFiles( station, stationDbs[station] );
		}
		// And the global output
		produceStationFile( statusDb );
		produceOutputFiles( 'global', globalDb );
	}, (process.env.OUTPUT_INTERVAL_MIN||15)*60*1000);
	
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
	const ognTracker = (packet.sourceCallsign.slice( 0, 3 ) == 'OGN');

	// Lookup the altitude adjustment for the 
    const sender = packet.digipeaters?.pop()?.callsign||'unknown';

	if( ignoreStation( sender ) ) {
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
    let glider = gliders[flarmId];

	// If it is undefined then we drop everything from here on
	if( ! glider ) {
		glider = gliders[flarmId] = { lastTime: packet.timestamp };
	}

    // Check to make sure they have moved or that it's been about 10 seconds since the last update
    // this reduces load from stationary gliders on the ground and allows us to track stationary gliders
    // better. the 1 ensures that first packet gets picked up after restart
    const distanceFromLast = glider.lastPoint ? distance( jPoint, glider.lastPoint ) : 1;
	const elapsedTime = packet.timestamp - glider.lastTime;
	
    if( distanceFromLast < 0.04 && elapsedTime < 30 ) {
		glider.stationary++;
        return;
    }
    if( distanceFromLast > 100 && distanceFromLast/(elapsedTime/3600) > 400 ) {
		glider.lastMoved = packet.timestamp;
		glider.jumps = (glider.jumps||0)+1;
    }
	
    if( packet.timestamp > glider.lastTime ) {        
		glider.lastPoint = jPoint;
		glider.lastAlt = altitude;
        glider.lastTime = packet.timestamp;
    }

	const values = packet.comment.match( /([0-9.]+)dB ([0-9])e/ );

	if( values ) {
		const strength = Math.round(values[1]);
		const crc = parseInt(values[2]);
		packetCallback( sender, h3.geoToH3(packet.latitude, packet.longitude, 8), altitude, altitude, glider, crc, strength );
	}
	
    // Enrich with elevation and send to everybody, this is async
//    getElevationOffset( packet.latitude, packet.longitude,
  //                 async (gl) => {
	//				   const agl = Math.round(Math.max(altitude-gl,0));
	//				   packetCallback( sender, h3.geoToH3(packet.latitude, packet.longitude, 7), altitude, agl, glider, 0, 10 );
//	});
}

//
// Actually serialise the packet into the database after processing the data
async function packetCallback( station, h3id, altitude, agl, glider, crc, signal ) {

	if( ! signal ) {
		return;
	}
	
	if( ! stationDbs[station] ) {
		stationDbs[station] = LevelUP(LevelDOWN(dbPath+'/stations/'+station))
	}

	const stationid = await getStationId( station );
	
	mergeDataIntoDatabase( 0, 0, stationDbs[station], h3id, altitude, agl, crc, signal );
	mergeDataIntoDatabase( station, stationid, globalDb, h3.h3ToParent(h3id,7), altitude, agl, crc, signal);
}

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

async function mergeDataIntoDatabase( station, stationid, db, h3, altitude, agl, crc, signal ) {

	function update(buffer, release) {
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
		db.put( h3, new Buffer(outputBuffer), {}, release );
	}

	lock( h3, function (release) {

			  db.get( h3 )
				.then( (value) => {
					update( value.buffer, release() );
				})
				.catch( (err) => {
					
					// Allocate a new buffer to update and then set it in the database -
					// if we have a station id then twe will make sure we reserve some space for it
					let buffer = new Uint8Array( normalLength + (stationid ? 12 : 0) );
					update( buffer.buffer, release() );
				})
		  })
	
}

// Helper fro resizing TypedArrays so we don't end up with them being huge
function resize( a, b ) {
	let c = new a.constructor( b );
	c.set( a );
	return c;
}

async function produceOutputFiles( station, inputdb, metadata ) {
	let length = 500;
	let position = 0;

	const types = [ 'minAgl', 'minAlt', 'minAltSig', 'avgSig', 'maxSig', 'avgCrc', 'count', 'stations' ];
	const byteMultiplier = { count: 2 };
	const lengthMultiplier = { h3out: 2 };

	let stationsBuilder = station == 'global' ?
						  makeBuilder({type: new Utf8()}):undefined;

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
			let zip = _sortby( _reject( _zip( c.extra.stations, c.extra.count ),
										(r)=>!r[1] ),
							   (s) => s[1] );
			const o =  _map(zip, (f) => f[0].toString(16)).join(',');
			stationsBuilder.append(o)
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

				

	let bytes = 0;
	// Now we have all the data we need we put each one into protobuf and serialise it to disk
	function output( name, data ) {
		const stationsInject = station == 'global' ? { stations: stationsBuilder.finish().toVector() } : {};
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

	// Form up meta data useful for the display
	let stationmeta = {	date: new Date().toISOString() };
	if( station ) {
		try {
			stationmeta = { ...stationmeta, meta: JSON.parse(await statusDb.get( station )) };
		} catch(e) {
			console.log( 'missing metadata for ', station );
		}
	}

	// Output the station information
	output( station, data );

	//
	return position;
}

		
	
async function produceStationFile( statusdb ) {

	let statusOutput = [];
	
	// Go through all the keys
	for await ( const [key,value] of statusdb.iterator()) {
		if( ! ignoreStation(key) ) {
			statusOutput.push( {station:''+key, ...JSON.parse(''+value)})
		}
	}

	writeFile( './public/data/stations.json', JSON.stringify(statusOutput,null,2), (err) => err ? console.log("station write error",err) : null);
}
	

