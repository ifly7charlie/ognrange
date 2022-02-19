import LevelUP from 'levelup';
import LevelDOWN from 'leveldown';

import dotenv from 'dotenv';

import { ignoreStation } from '../lib/bin/ignorestation.js'

import { CoverageRecord, bufferTypes } from '../lib/bin/coveragerecord.js';
import { CoverageHeader, accumulatorTypes } from '../lib/bin/coverageheader.js';

import { DB_PATH, OUTPUT_PATH } from '../lib/bin/config.js';

import yargs from 'yargs';

main()
    .then("exiting");

//
// Primary configuration loading and start the aprs receiver
async function main() {

	const args = yargs(process.argv.slice(2))
		.option( 'db', 
				  { alias: 'd',
					type: 'string',
					default: 'global',
					description: 'Choose Database'
		})
		.option( 'all',
				  { alias: 'a',
					type: 'boolean',
					description: 'dump all records'})
		.option( 'match',
				 { type: 'string',
				   description: 'regex match of dbkey' })
		.option( 'size',
				  { alias: 's',
					type: 'boolean',
					description: 'determine approximate size of each block of records' })
		.help()
		.alias( 'help', 'h' ).argv;
			

	// What file
	let dbPath = DB_PATH;
	if( args.db && args.db != 'global' ) {
		dbPath += '/stations/' + args.db;
	}
	else {
		dbPath += 'global';
	}

	let db = null

	try {
		db = LevelUP(LevelDOWN(dbPath, {createIfMissing:false}));
	} catch(e) {
		console.error(e)
	}

	console.log( '---', dbPath, '---' );
	
	
	let n = db.iterator();
	let accumulators = {}, count = {};
	let x = n.next();
	let y = null;
	while( y = await x) {
		const [key,value] = y;
		let hr = new CoverageHeader(key);

		if( ! args.match || hr.dbKey().match( args.match )) {
			
			if( hr.isMeta ) {
				accumulators[ hr.accumulator ] = { hr: hr, meta: JSON.parse(String(value)), count:0, size: 0};
				console.log( hr.toString(), String(value))
				
				if( args.size ) {
					db.db.approximateSize( CoverageHeader.getAccumulatorBegin(hr.type,hr.bucket),
										   CoverageHeader.getAccumulatorEnd(hr.type,hr.bucket),
										   (e,r) => { accumulators[ hr.accumulator ].size = r } );
				}
			}
			else {
				if( accumulators[ hr.accumulator ] ) {
					accumulators[ hr.accumulator ].count++;
				}
				
				if( args.all ) {
					console.log( hr.dbKey(), JSON.stringify(new CoverageRecord(value).toObject()));
				}
				else {
					n.seek( CoverageHeader.getAccumulatorEnd( hr.type, hr.bucket ));
				}
			}
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

