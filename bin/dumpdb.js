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
	
	for await ( const [key,value] of db.iterator() ) {
		console.log( ''+key, value );
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

	
	let n = db.iterator();
	let p = await n.seek( '0/y/9' );

	let x = n.next();
	let y = null;
	 while(y = await x) {

		if( y ) {
			const [key,value] = y;
			let br = new CoverageRecord(value);
			let hr = new CoverageHeader(key);

			console.log( hr.lockKey )
//		br.print();
		}
		x = n.next();
	}
}

