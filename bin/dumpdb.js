import LevelUP from 'levelup';
import LevelDOWN from 'leveldown';

import dotenv from 'dotenv';

import { ignoreStation } from '../lib/bin/ignorestation.js'

import { CoverageRecord, bufferTypes } from '../lib/bin/coveragerecord.js';
import { CoverageHeader, accumulatorTypes } from '../lib/bin/coverageheader.js';

// Default paths, can be overloaded using .env.local
let dbPath = './db/';
let outputPath = './public/data/';

main()
    .then("exiting");

//
// Primary configuration loading and start the aprs receiver
async function main() {

	// Load the configuration from a file
	dotenv.config({ path: '.env.local' })

	dbPath = process.env.DB_PATH||dbPath;
	outputPath = process.env.OUTPUT_PATH||outputPath;

	let db = LevelUP(LevelDOWN(dbPath+'global'))
/*
	for await ( const [key,value] of db.iterator() ) {
		console.log( ''+key );
	}
	console.log('---');

	for await ( const [key,value] of db.iterator({ gte: '0/y/87283318affffff'}) ) {
		console.log( ''+key, value );
	}

	console.log('++++');
	
	for await ( const [key,value] of db.iterator() ) {
		let br = new CoverageRecord(value);
		let hr = new CoverageHeader(key);
		console.log( hr.lockKey )
	}
		
	console.log('---');
	for await ( const [key,value] of db.iterator({gte: new CoverageHeader('0/y/87283318affffff').lockKey}) ) {
		let br = new CoverageRecord(value);
		let hr = new CoverageHeader(key);
		console.log( hr.lockKey )
	}
	console.log('>>> tada>>>');
	*/

	console.log( CoverageHeader.getDbSearchRangeForAccumulator('day',0, true) );
	for await ( const [key,value] of db.iterator( CoverageHeader.getDbSearchRangeForAccumulator('day',1,true) )) {
		console.log(String(key))
		db.del( key );
	}

	
	let n = db.iterator();
	let accumulators = {}, count = {};
	let x = n.next();
	let y = null;
	while( y = await x) {
		const [key,value] = y;
		let hr = new CoverageHeader(key);

		if( hr.isMeta ) {
			accumulators[ hr.accumulator ] = { hr: hr, meta: JSON.parse(String(value)), count:0, size: 0};

			db.db.approximateSize( CoverageHeader.getAccumulatorBegin(hr.type,hr.bucket),
								   CoverageHeader.getAccumulatorEnd(hr.type,hr.bucket),
								   (e,r) => { accumulators[ hr.accumulator ].size = r } );
		}
		else {
			if( accumulators[ hr.accumulator ] ) {
				accumulators[ hr.accumulator ].count++;
			}
		
			// Skip to next bucket
//			n.seek( CoverageHeader.getAccumulatorEnd( hr.type, hr.bucket ));
		}
		x = n.next();
		
		if( y = await x) {
			const [key,value] = y;
			let hr = new CoverageHeader(key);
		}
		
	}
	for (const a in accumulators ) {
		console.log( `${accumulators[a].hr.typeName} [${a}]: ${accumulators[a].count} records, ~ ${accumulators[a].size} bytes` );
		console.log( '  '+ JSON.stringify(accumulators[a].meta) );
	}
}

