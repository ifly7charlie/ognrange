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

let stations = {};
let stationDbs = {};
let globalDb = LevelUP(LevelDOWN('./db/global'))
let statusDb = LevelUP(LevelDOWN('./db/status'))

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

import { makeTable, tableFromArrays, RecordBatchWriter } from 'apache-arrow';

// For the database we use a 'C structure', it's already compressed so
// not so useful using protobuf
import Struct from 'struct';
let Record = Struct()
	.word16Ule('minAltAgl')
	.word16Ule('minAlt')
	.word8Ule('minAltMaxSig')
	.word32Ule('sumSig')
	.word32Ule('count')
	.word8Ule('maxSig')
	.word32Ule('sumCrc')

// Set up background fetching of the competition
async function main() {
	dotenv.config({ path: '.env.local' })
	try { 
		mkdirSync('./db/stations', {recursive:true});
	} catch(e) {};
	startAprsListener();
}

main()
    .then("exiting");

//
// Connect to the APRS Server
function startAprsListener( m = undefined ) {

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
    connection.on('packet', (data) => {
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
						stations[ packet.sourceCallsign ] = { ...stations[packet.sourceCallsign], lat: packet.latitude, lng: packet.longitude};
					}
					else if( packet.type == 'status' ) {
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

    // Start the APRS connection
    connection.connect();

	// And every 2 minutes we need to confirm the APRS
	// connection has had some traffic
	setInterval( function() {
		
		// Send APRS keep alive or we will get dumped
        connection.sendLine(`# ${CALLSIGN} ${process.env.NEXT_PUBLIC_WEBSOCKET_HOST}`);

        // Re-establish the APRS connection if we haven't had anything in
        if( ! connection.valid ) {
            console.log( "failed APRS connection, retrying" );
            connection.disconnect( () => { connection.connect() } );
        }
        connection.valid = false;
	}, 2*60*1000);

	produceOutputFiles( 'global', globalDb );
	produceStationFile( statusDb );
	setInterval( function() {
		for( const station of Object.keys(stationDbs) ) {
			produceOutputFiles( station, stationDbs[station] );
		}
		produceOutputFiles( 'global', globalDb );
		produceStationFile( statusDb );
	}, 15*60*1000);
	
}

//
// collect points, emit to competition db every 30 seconds
function processPacket( packet ) {

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
function packetCallback( sender, h3id, altitude, agl, glider, crc, signal ) {
	if( ! stationDbs[sender] ) {
		stationDbs[sender] = LevelUP(LevelDOWN('./db/stations/'+sender))
	}
	
	mergeDataIntoDatabase( sender, stationDbs[sender], h3id, altitude, agl, crc, signal );
	mergeDataIntoDatabase( 'global', globalDb, h3.h3ToParent(h3id,7), altitude, agl, crc, signal );
}

function mergeDataIntoDatabase( station, db, h3, altitude, agl, crc, signal ) {

	let record = Record.clone();

	db.get( h3 )
		.then( (value) => {
			record._setBuff( value );
			let existing = record.fields;
			if( existing.minAlt > altitude ) {
				existing.minAlt = altitude;
				
				if( existing.minAltMaxSig < signal ) {
					existing.minAltMaxSig = signal;
				}
			}
			if( existing.minAltAgl > agl ) {
				existing.minAltAgl = agl;
			}
			
			if( existing.maxSig < signal ) {
				existing.maxSig = signal;
			}
			
			existing.sumSig += signal;
			existing.sumCrc += crc;
			existing.count ++;
			db.put( h3, record.buffer() );
		})
		.catch( (err) => {
			record.allocate();
			let existing = record.fields;
			existing.minAlt = altitude;
			existing.minAltMaxSig = signal;
			existing.minAltAgl = agl;
			existing.maxSig = signal;
			existing.sumSig = signal;
			existing.sumCrc = crc;
			existing.count  = 1;
			db.put( h3, record.buffer() );

			// count distinct rows for the station
			if( stations[ station ] ) {
				stations[ station ].cells++;
			}
			else {
				stations[station] = { cells:1 }
			}
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

	const types = [ 'minAgl', 'minAlt', 'minAltSig', 'avgSig', 'maxSig', 'avgCrc', 'count' ];
	const byteMultiplier = { count: 2 };
	const lengthMultiplier = { h3out: 2 };

	let data = { 
		h3out: new Uint32Array( length*2 ),
		minAgl: new Uint16Array( length ),
		minAlt: new Uint16Array( length ),
		minAltSig: new Uint8Array( length ),
		avgSig: new Uint8Array( length ),
		maxSig: new Uint8Array( length ),
		avgCrc: new Uint8Array( length ),
		count: new Uint32Array( length )
	};


//	console.log( inputdb.approximateSize
	let record = Record.clone();

	// Go through all the keys
	for await ( const [key,value] of inputdb.iterator()) {
		record._setBuff( value );
		let c = record.fields;

		// Save them in the output arrays
		const lh = h3.h3IndexToSplitLong(''+key);
		data.h3out[position*2] = lh[0];
		data.h3out[position*2+1] = lh[1];
		data.minAgl[position] = c.minAltAgl;
		data.minAlt[position] = c.minAlt;
		data.minAltSig[position] = c.minAltMaxSig;
		data.maxSig[position] = c.maxSig;
		data.count[position] = c.count;
		
		// Calculated ones
		data.avgSig[position] = Math.round(c.sumSig / c.count);
		data.avgCrc[position] = Math.round(c.sumCrc / c.count);

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
		const outputTable = makeTable({
			h3: new Uint32Array(data.h3out.buffer,0,position*2),
			minAgl: data.minAgl,
			minAlt: data.minAlt,
			minAltSig: data.minAltSig,
			maxSig: data.maxSig,
			c: data.count,
			avgSig: data.avgSig,
			avgCrc: data.avgCrc
		});

		console.log( name, 'bl:', outputTable.byteLength )

		const pt = new PassThrough( { objectMode: true } )
		const result = pt
			  .pipe( RecordBatchWriter.throughNode())
			  .pipe( createWriteStream( './public/data/'+name+'.arrow' ));
						
		pt.write(outputTable);
		pt.end();
		
//		writeFile( './webdata/'+name+'.pbuf', data, (err) => { if( err ) { console.log( name, 'file failed:', err ); }} );
	}

//	let pbRoot = protobuf.Root.fromJSON(OnglideRangeMessage);
//	let pbOnglideRangeMessage = pbRoot.lookupType( "OnglideRangeMessage" );
//	function encodePb( msg ) {
//		let message = pbOnglideRangeMessage.create( msg );
//		return pbOnglideRangeMessage.encode(message).finish();
//	}

	// Make sure we have a directory for it
	try { 
		mkdirSync('./public/data/'+station, {recursive:true});
	} catch(e) {};

	let stationmeta = {};
	if( station ) {
		try {
			stationmeta = { stationmeta: JSON.parse(await statusDb.get( station )) };
		} catch(e) {
			console.log( 'missing metadata for ', station );
		}
	}

	const date = new Date().toISOString();
	output( station, data );

//					{ ...stationmeta,
//				  station: station, type: outputType,
//				  end: date,
//				  count: position,
//				  h3s: new Uint8Array(data.h3out.buffer,0,position*8),
//				  values: new Uint8Array(data[outputType].buffer,0,position*(byteMultiplier[outputType]||1))});
//	}

	console.log( station, position, 'cells', bytes, 'bytes' );
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
	

