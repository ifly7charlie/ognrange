
import LevelUP from 'levelup';
import LevelDOWN from 'leveldown';

import { CoverageRecord, bufferTypes } from './coveragerecord.js';
import { CoverageHeader, accumulatorTypes } from './coverageheader.js';
import { prefixWithZeros } from './prefixwithzeros.js';

import { mapAllCapped } from './mapallcapped.js';

import { flushDirtyH3s } from './h3cache.js';

import _clonedeep from 'lodash.clonedeep';
import _isequal from 'lodash.isequal';
import _map from 'lodash.map';
import _sortby from 'lodash.sortby';
import _filter from 'lodash.filter';
import _uniq from 'lodash.uniq';

import { writeFileSync, readFileSync, mkdirSync, unlinkSync, symlinkSync } from 'fs';
import zlib from 'zlib';

//
// What accumulators we are operating on these are internal
let currentAccumulator = undefined; 
let accumulators = {};

import { ROLLUP_PERIOD_MINUTES, STATION_EXPIRY_TIME_SECS, MAX_STATION_DBS,
		 DB_PATH, OUTPUT_PATH } from './config.js'

//
// Information about last rollup
export let rollupStats = { completed: 0, elapsed: 0 };

//
// Helper for getting current accumulator used as  ...getAccumulator() in
// calls to CoverageHeader
export function getAccumulator() { return currentAccumulator; }
export function getAccumulatorForType(t) {
	if( t == 'current' ) {
		return currentAccumulator;
	}
	else {
		return [ t, accumulators[t].bucket]
	}
}

// Calculate the bucket and short circuit if it's not changed - we need to change
// accumulator every time we dump but we need a unique name for it...
//
// We need accumulator buckets that are basically unique so we don't rollup the wrong thing at the wrong time
// our goal is to make sure we survive restart without getting same code if it's not the same day...
//
// Same applies to the buckets we roll into, if it's unique then we can probably resume into it and still
// output a reasonable file. If it was simply 'day of month' then a one mount outage would break everything
//
// if you run this after a month gap then welcome back ;) and I'm sorry ;)  [it has to fit in 12bits]
//
// in this situation if it happens to be identical bucket it will resume into current month
// otherwise it will try and rollup into the buckets that existed at the time (which are valid
/// for several years) or discard the data. 
//
// this takes effect immediately so all new packets will move to the new accumulator
// rolling over is maximum of 12 times an hour...
export function whatAccumulators(now) {
	const rolloverperiod = Math.floor(((now.getUTCHours()*60)+now.getUTCMinutes())/ROLLUP_PERIOD_MINUTES);
	const newAccumulatorBucket =  ((now.getUTCDate()&0x1f)<<7)|(rolloverperiod&0x7f)
	const n = {
		d: prefixWithZeros(2,String(now.getUTCDate())),
		m: prefixWithZeros(2,String(now.getUTCMonth()+1)),
		y: now.getUTCFullYear()
	};
	accumulators = { day: { bucket: ((now.getUTCFullYear()&0x07)<<9)|((now.getUTCMonth()&0x0f)<<5)|((now.getUTCDate()&0x1f)),
							file: `${n.y}-${n.m}-${n.d}`, },
					 month: { bucket: ((now.getUTCFullYear()&0xff)<<4)|(now.getUTCMonth()&0x0f), file: `${n.y}-${n.m}` },
					 year:  { bucket: now.getUTCFullYear(), file: `${n.y}` }
				   };	
	return { current: newAccumulatorBucket,
			 accumulators: accumulators };
}

export async function updateAndProcessAccumulators( { globalDb, statusDb, stationDbCache, stations } ) {
	
	const now = new Date();

	// Calculate the bucket and short circuit if it's not changed - we need to change
	const { current: newAccumulatorBucket, accumulators: newAccumulators } = whatAccumulators(now);

	if( currentAccumulator?.[1] == newAccumulatorBucket ) {
		return;
	}

	// Make a copy
	const oldAccumulators = _clonedeep( accumulators );
	const oldAccumulator = _clonedeep( currentAccumulator );

	// Update the live ones
	currentAccumulator = [ 'current', newAccumulatorBucket ];
	accumulators = newAccumulators;
	
	// If we have a new accumulator (ignore startup when old is null)
	if( oldAccumulator ) {

		console.log( `accumulator rotation:` );
		console.log( JSON.stringify(oldAccumulators) );
		console.log( '----' );
		console.log( JSON.stringify(accumulators) );

		// Now we need to make sure we have flushed our H3 cache and everything
		// inflight has finished before doing this. we could purge cache
		// but that doesn't ensure that all the inflight has happened
		flushDirtyH3s({globalDb, stationDbCache, stations, allUnwritten:true }).then( (s) => {
			console.log( `accumulator rotation happening` );
			console.log( s );
			rollupAll( { current: oldAccumulator, processAccumulators: oldAccumulators,
						 globalDb, statusDb, stationDbCache, stations,
						 newAccumulatorFiles: !_isequal( accumulators, oldAccumulators ) } );
		})

	}

	// If any of the accumulators have changed then we need to update all the
	// meta data
	if( ! _isequal( accumulators, oldAccumulators ) || ! _isequal( currentAccumulator, oldAccumulator )) {
		updateGlobalAccumulatorMetadata( {globalDb} );
	}
}		

	
//
// We need to make sure we know what rollups the DB has, and process pending rollup data
// when the process starts. If we don't do this all sorts of weird may happen
// (only used for global but could theoretically be used everywhere)
export async function rollupStartup( { globalDb, statusDb, stationDbCache, stations } ) {

	let accumulatorsToPurge = {};
	let hangingCurrents = [];

	const now = new Date();
	
	// Our accumulators 
	const { current: expectedCurrentAccumulatorBucket, accumulators: expectedAccumulators } = whatAccumulators(now);

	// We need a current that is basically unique so we don't rollup the wrong thing at the wrong time
	// our goal is to make sure we survive restart without getting same code if it's not the same day...
	// if you run this after an 8 year gap then welcome back ;) and I'm sorry ;)  [it has to fit in 12bits]
	const expectedCurrentAccumulator = [ 'current', expectedCurrentAccumulatorBucket ];
		
	// First thing we need to do is find all the accumulators in the database
	let iterator = globalDb.iterator();
	let iteratorPromise = iterator.next(), row = null;
	while( row = await iteratorPromise ) {
		const [key,value] = row;
		let hr = new CoverageHeader(key);

		// 80000000 is the h3 cell code we use to
		// store the metadata for our iterator
		if( ! hr.isMeta ) {
			console.log( 'ignoring weird database format, try bin/dumpdb.js for info', hr.h3 )
			console.log( hr, new CoverageRecord(value).toObject() );
			iterator.seek( CoverageHeader.getAccumulatorEnd( hr.type, hr.bucket ));
			iteratorPromise = iterator.next();
			continue;
		}
		const meta = JSON.parse( String(value) ) || {};

		// If it's a current and not OUR current then we need to
		// figure out what to do with it... We may merge it into rollup
		// accumulators if it was current when they last updated their meta
		if( hr.typeName == 'current' ) {
			if( hr.bucket != expectedCurrentAccumulatorBucket ) {
				hangingCurrents[ hr.dbKey() ] = meta;
				console.log( `current: hanging accumulator ${hr.accumulator} (${hr.bucket}) [started at: ${meta.startUtc}]`);
			}
			else {
				console.log( `current: resuming accumulator ${hr.accumulator} (${hr.bucket}) as still valid [stated at: ${meta.startUtc}]`);
			}
		}

		// accumulator not configured on this machine - dump and purge
		else if( ! expectedAccumulators[ hr.typeName ] ) {
			accumulatorsToPurge[ hr.accumulator ] = { accumulator: hr.accumulator, meta: meta, typeName: hr.typeName, t: hr.type, b: hr.bucket, file: hr.file };
		}
		// new bucket for the accumulators - we should dump this
		// and purge as adding new data to it will cause grief
		// note the META will indicate the last active accumulator
		// and we should merge that if we find it
		else if( expectedAccumulators[ hr.typeName ].bucket != hr.bucket ) {
			accumulatorsToPurge[ hr.accumulator ] = { accumulator: hr.accumulator, meta: meta, typeName: hr.typeName, t: hr.type, b: hr.bucket, file: hr.file };
		}
		else {
			console.log( `${hr.typeName}: resuming accumulator ${hr.accumulator} (${hr.bucket}) as still valid  [started at: ${meta.startUtc}]` );
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
			globalDb.get( dbkey )
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
			globalDb.get( dbkey )
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
				if( accumulatorsToPurge[ ch.accumulator ] ) {
					rollupAccumulators[ type ] = { bucket: ch.bucket, file: accumulatorsToPurge[ ch.accumulator ].file };
				}
				else if( expectedAccumulators[type].bucket == ch.bucket ) {
					rollupAccumulators[ type ] = { bucket: ch.bucket, file: expectedAccumulators[type].file };
				}
			}

			if( Object.keys(rollupAccumulators).length ) { 
				console.log( ` rolling up hanging current accumulator ${hangingHeader.accumulator} into ${JSON.stringify(rollupAccumulators)}` );
				await rollupAll( { current: [hangingHeader.type,hangingHeader.bucket], processAccumulators: rollupAccumulators,
								   globalDb, statusDb, stationDbCache, stations,
								   newAccumulatorFiles: true } );
			} else {
				console.log( `purging hanging current accumulator ${hangingHeader.accumulator} and associated sub accumulators` );
			}

			// now we clear it
			await globalDb.clear( CoverageHeader.getDbSearchRangeForAccumulator( hangingHeader.type, hangingHeader.bucket, true ),
								  (e) => { console.log( `${hangingHeader.type}/${hangingHeader.accumulator} purge completed ${e||'successfully'}`) } );
		}
	}
	
	// These are old accumulators we purge them because we aren't sure what else can be done
	for( const key in accumulatorsToPurge ) {
		const { t, b, accumulator, typeName } = accumulatorsToPurge[key];
		await globalDb.clear( CoverageHeader.getDbSearchRangeForAccumulator( t, b, true ),
							  (e) => { console.log( `${typeName}: ${accumulator} purge completed ${e||'successfully'}`) } );
	}

}

//
// This iterates through all open databases and rolls them up.
// ***HMMMM OPEN DATABASE - so if DBs are not staying open we have a problem
export async function rollupAll( { current = currentAccumulator,
								   processAccumulators = accumulators,
								   globalDb, statusDb, stationDbCache, stations,
								   newAccumulatorFiles = false } ) {

	// Make sure we have updated validStations
	const nowEpoch = Math.floor(Date.now()/1000);
	const expiryEpoch = nowEpoch - STATION_EXPIRY_TIME_SECS;
	const validStations = new Set();
	rollupStats.movedStations = 0;
	for ( const station of Object.values(stations)) {
		station.valid = false;
		if( (station.lastPacket||station.lastBeacon||nowEpoch) > expiryEpoch && ! station.moved) {
			validStations.add(Number(station.id));
			station.valid = true;
		}
		if( station.moved ) {
			station.moved = false;
			rollupStats.movedStations ++;
			console.log( `purging moved station ${station.station}` );
			if( statusDb ) {
				statusDb.put( station.station, JSON.stringify(station) ); // perhaps always write?
			}
		}

		if( ! station.valid ) {
			console.log( `purging invalid station ${station.station}` );
		}
			
	}

	rollupStats.validStations = validStations.size;

	let commonArgs = {
		globalDb, validStations,
		current, processAccumulators,
		newAccumulatorFiles };
	
	console.log( `performing rollup and output of ${validStations.size} stations + global, removing ${Object.keys(stations).length-validStations.size} stations ` );
	
	rollupStats = { ...rollupStats, 
					lastStart: Date.now(),
					last: {sumElapsed: 0,
						   operations: 0,
						   databases: 0,
						   skippedStations: 0,
						   accumulators: processAccumulators,
						   current: CoverageHeader.getAccumulatorMeta(...current).accumulator }
	};

	// Global is biggest and takes longest
	let promises = [];
	promises.push( new Promise( async function (resolve) {
		const r = await rollupDatabase( { ...commonArgs, db:globalDb, stationName: 'global' } );
		rollupStats.last.sumElapsed += r.elapsed;
		rollupStats.last.operations += r.operations;
		rollupStats.last.databases ++;
		resolve();
	}));

	// each of the stations, capped at 20% of db cache (or 30 tasks) to reduce risk of purging the whole cache
	// mapAllCapped will not return till all have completed, but this doesn't block the processing
	// of the global db or other actions.
	// it is worth running them in parallel as there is a lot of IO which would block
	promises.push( mapAllCapped( Object.values(stations), async function (stationMeta) {

		const station = stationMeta.station;
		
		// If there has been no packets since the last output then we don't gain anything by scanning the whole db and processing it
		if(	!newAccumulatorFiles && stationMeta.outputEpoch && (!stationMeta.moved) &&
			((stationMeta.lastPacket||0) < stationMeta.outputEpoch )) {
			rollupStats.last.skippedStations++;
			return;
		}
		
		// Open DB if needed 
		let db = stationDbCache.get(stationMeta.id)
		if( ! db ) {
			stationDbCache.set(stationMeta.id, db = LevelUP(LevelDOWN(DB_PATH+'/stations/'+station)));
			db.ognInitialTS = Date.now();
			db.ognStationName = station;
		}

		// If a station is not valid we are clearing the data from it from the registers
		if( ! validStations.has( stationMeta.id ) ) {
			// empty the database... we could delete it but this is very simple and should be good enough
			console.log( `clearing database for ${station} as it is not valid` );
			await db.clear();
			rollupStats.last.databases ++;
			return;
		}
		
		const r = await rollupDatabase( { ...commonArgs, db, stationName:station, stationMeta } );
		rollupStats.last.sumElapsed += r.elapsed;
		rollupStats.last.operations += r.operations;
		rollupStats.last.databases ++;
		
	}, Math.max(Math.floor(MAX_STATION_DBS/5),30) ));
	
	// And the global json
	produceStationFile( stations );

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
//
// exported for testing
export async function rollupDatabase( { db, stationName, stationMeta = {}, validStations, current=currentAccumulator, processAccumulators=accumulators } ) {
	const now = new Date(), nowEpoch = Math.floor(now/1000);
	const name = stationName;
	let currentMeta = {};

	// Details about when we wrote, also contains information about the station if
	// it's not global
	stationMeta.outputDate = now.toISOString();
	stationMeta.outputEpoch = nowEpoch;

	//	const log = stationName == 'tatry1' ? console.log : ()=>false;
	const log = () => 0;
	
	let dbOps = [];
	let h3source = 0;

	//
	// Basically we finish our current accumulator into the active buckets for each of the others
	// and then we need to check if we should be moving them to new buckets or not
	
	// We step through all of the items together and update as one
	const rollupIterators = _map( Object.keys(processAccumulators), (r) => {
		return { type:r, bucket: processAccumulators[r].bucket, file: processAccumulators[r].file,
				 meta: { rollups: [] },
				 stats: {
					 h3missing:0,
					 h3noChange: 0,
					 h3updated:0,
					 h3emptied:0,
					 h3stationsRemoved:0,
					 h3extra:0,
				 },
				 iterator: db.iterator( CoverageHeader.getDbSearchRangeForAccumulator( r, processAccumulators[r].bucket )),
				 arrow: CoverageRecord.initArrow( (stationName == 'global') ? bufferTypes.global : bufferTypes.station ),
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
		const h3p = new CoverageHeader(key);

		if( h3p.isMeta ) {
			continue;
		}

		log( h3p.dbKey() , '------------------------' );
		
		const currentBr = new CoverageRecord(value);
		let advancePrimary;

		do {
			advancePrimary = true;
			
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
					
					if( r.lastCopiedH3p != h3p.lockKey ) {
						const h3kr = h3p.getAccumulatorForBucket(r.type,r.bucket);
						dbOps.push( { type: 'put',
									  key: h3kr.dbKey(),
									  value: Buffer.from(currentBr.buffer()) } );
						currentBr.appendToArrow( h3kr, r.arrow );
						r.lastCopiedH3p = h3p.lockKey;
						r.stats.h3missing++;
						log('       h3missing (end)', h3kr.dbKey(), h3p.dbKey(), r.type ); 
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
					console.log( `unexpected meta information processing ${stationName}, ${r.type} at ${h3kr.dbKey()}, ignoring` );
					advancePrimary = false; // we need more
					continue;
				}

				// One check for ordering so we know if we need to
				// advance or are done
				const ordering = CoverageHeader.compareH3(h3p,h3kr);

				log( ' ', ordering, h3kr.dbKey(), h3p.dbKey());

				// Need to wait for others to catch up and to advance current
				// (primary is less than rollup) depends on await working twice on
				// a promise (await r.n above) because we haven't done .next()
				// this is fine but will yield which is also fine. note we
				// never remove stations from source
				if( ordering < 0 ) {
					if( r.lastCopiedH3p != h3p.lockKey ) {
						const h3kr = h3p.getAccumulatorForBucket(r.type,r.bucket);
						dbOps.push( { type: 'put',
									  key: h3kr.dbKey(),
									  value: Buffer.from(currentBr.buffer()) } );
						currentBr.appendToArrow( h3kr, r.arrow );
						r.lastCopiedH3p = h3p.lockKey;
						r.stats.h3missing++;
						log('       h3missing', h3kr.dbKey(), h3p.dbKey(), r.type); 
					}
					continue;
				}

				// We know we are editing the record so load it up, our update
				// methods will return a new CoverageRecord if they change anything
				// hence the updatedBr
				let br = new CoverageRecord(rollupValue);
				let updatedBr = null;
				let changed = false;
				
				// Primary is greater than rollup
				if( ordering > 0 ) {
					updatedBr = br.removeInvalidStations(validStations);
					advancePrimary = false; // we need more to catch up to primary
					r.stats.h3stationsRemoved += updatedBr == br ? 0 : 1;
				}

				// Otherwise we are the same so we need to rollup into it, but only once!
				else  {
					if( r.lastCopiedH3p == h3p.lockKey )  {
						continue;
					}
						
					updatedBr = br.rollup(currentBr, validStations);
					changed = true; // updatedBr may not always change so
					r.lastCopiedH3p = h3p.lockKey;
					// we are caught up to primary so allow advance if everybody else is fine
				}
				
				// Check to see what we need to do with the database
				// this is a pointer check as pointer will ALWAYS change on
				// adjustment 
				if( changed || updatedBr != br ) {
					if( ! updatedBr ) {
						dbOps.push( { type: 'del', key: h3kr.dbKey() } );
						r.stats.h3emptied++;
						log('       h3emptied', h3kr.dbKey(), h3p.dbKey(), r.type ); 
					}
					else {
						r.stats.h3updated++;
						dbOps.push( { type: 'put', key: h3kr.dbKey(), value: Buffer.from(updatedBr.buffer()) });
						log('       h3updated', h3kr.dbKey(), h3p.dbKey(), r.type); 
					}
				}
				else {
					r.stats.h3noChange++;
					log('       h3nochange', h3kr.dbKey(), h3p.dbKey(), r.type); 
				}
				
				// If we had data then write it out
				if( updatedBr ) {
					updatedBr.appendToArrow( h3kr, r.arrow );
				}
				
				// Move us to the next one, allow 
				r.n = r.iterator.next();
			}
			
		} while( ! advancePrimary );

		// Once we have accumulated we delete the accumulator key
		h3source++;
		dbOps.push( { type: 'del', key: h3p.dbKey() })
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
				log( ' x ', h3kr.dbKey() );
				
				let updatedBr = br.removeInvalidStations(validStations);
				
				// Check to see what we need to do with the database
				if( updatedBr != br ) {
					r.stats.h3stationsRemoved++;
					if( ! updatedBr ) {
						dbOps.push( { type: 'del', key: h3kr.dbKey() } );
						r.stats.h3emptied++;
						log('       h3emptied', h3kr.dbKey(), '[end]'); 
					}
					else {	
						dbOps.push( { type: 'put', key: h3kr.dbKey(), value: Buffer.from(updatedBr.buffer()) });
						r.stats.h3updated++;
						log('       h3update', h3kr.dbKey(), '[end]'); 
					}
				}
				else {
					r.stats.h3noChange++;
						log('       h3nochange', h3kr.dbKey(), '[end]'); 
				}
				
				if( updatedBr ) {
					updatedBr.appendToArrow( h3kr, r.arrow );
				}

				r.stats.h3extra++;

				
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
		r.stats.dbOps = dbOps.length;
		r.stats.h3source = h3source;
		r.meta.rollups.push( { source: currentMeta, stats: r.stats, file: accumulatorName } );

		stationMeta.accumulators[r.type] = r.stats;

		// May not have a directory if new station
		mkdirSync(OUTPUT_PATH + name, {recursive:true});

		// Finalise the arrow table and serialise it to the disk
		CoverageRecord.finalizeArrow(r.arrow, OUTPUT_PATH+accumulatorName+'.arrow' );

		try {
			writeFileSync( OUTPUT_PATH + accumulatorName + '.json',
						   JSON.stringify(r.meta,null,2) );
		}
		catch(err) {
			console.log("rollup json metadata write failed",err)
		};

		// Fix directory index
		let index = {};
		try {
			const data = readFileSync( OUTPUT_PATH + `${name}/${name}.index.json`, 'utf8');
			index = JSON.parse(data);
		} catch(e) {
			if( e.code != 'ENOENT' ) {
				console.log( `unable to read file index ${name} ${e}` );
			}
		}

		if( ! index.files ) {
			index.files = {};
		}

		index.files[r.type] = { current: accumulatorName, all: _uniq([ ...(index.files[r.type]?.all||[]), accumulatorName ]) };

		try {
			writeFileSync( OUTPUT_PATH + `${name}/${name}.index.json`, JSON.stringify(index,null,2) );
		}
		catch (err) {
			console.log(`station ${name} index write error`,err);
		}
		
		// link it all up for latest
		symlink( `${name}.${r.type}.${processAccumulators[r.type].file}.arrow.gz`, OUTPUT_PATH+`${name}/${name}.${r.type}.arrow.gz` );
		symlink( `${name}.${r.type}.${processAccumulators[r.type].file}.arrow`, OUTPUT_PATH+`${name}/${name}.${r.type}.arrow` );
		symlink( `${name}.${r.type}.${processAccumulators[r.type].file}.json`, OUTPUT_PATH+`${name}/${name}.${r.type}.json` );

		if( (r.stats.h3source) != (r.stats.h3missing + r.stats.h3updated) ) {
			console.error( "********* stats don't add up ", r.type, r.bucket.toString(16), JSON.stringify({m:r.meta,s:r.stats}))
		}
	}

	stationMeta.lastOutputFile = nowEpoch;
	
	// record when we wrote for the whole station
	try {
		writeFileSync( OUTPUT_PATH+`${name}/${name}.json`, JSON.stringify(stationMeta,null,2));
	} catch (err) {
		console.log("stationmeta write error",err);
	}

	if( stationName != 'global' && stationName != '!test') {
		delete stationMeta.accumulators; // don't persist this it's not important
	}

	// If we have a new accumulator then we need to purge the old meta data records - we
	// have already purged the data above
	dbOps.push( { type: 'del', key: CoverageHeader.getAccumulatorMeta( ...current ).dbKey() });
	if( stationName == 'global' ) {
		console.log( `rollup: current bucket ${[...current]} completed, removing` );
	}
	
	// Write everything out
	for( const r of rollupData ) {
		if( accumulators[ r.type ].bucket != r.bucket ) {
			if( stationName == 'global' ) {
				console.log( `rollup: ${r.type} bucket has changed to ${accumulators[r.type].bucket}, deleting old bucket ${r.bucket} ${r.file}` );
			}
			dbOps.push( { type: 'del', key: CoverageHeader.getAccumulatorMeta( r.type, r.bucket ).dbKey() });
		}
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

	return { elapsed: Date.now() - now, operations: dbOps.length, accumulators: stationMeta.accumulators };
}								


//
// We will add meta data to the database for each of the accumulators
// this makes it easier to check what needs to be done
export function updateGlobalAccumulatorMetadata( {globalDb, currentAccumulator = getAccumulator(), allAccumulators = accumulators }) {

	const dbkey = CoverageHeader.getAccumulatorMeta( ...currentAccumulator ).dbKey();
	const now = new Date;
	
	globalDb.get( dbkey )
		.then( (value) => {
			const meta = JSON.parse( String(value) );
			meta.oldStarts = [ ...meta?.oldStarts, { start: meta.start, startUtc: meta.startUtc } ];
			meta.accumulators = allAccumulators;
			meta.start = Math.floor(now/1000);
			metat.startUtc = now.toISOString();
			globalDb.put( dbkey, JSON.stringify( meta ));
		})
		.catch((e) => {
			globalDb.put( dbkey, JSON.stringify( {
				accumulators: allAccumulators,
				oldStarts: [],
				start: Math.floor(now/1000),
				startUtc: now.toISOString()
			}));
		});

	// make sure we have an up to date header for each accumulator
	for( const type in allAccumulators ) {
		const currentHeader = CoverageHeader.getAccumulatorMeta( type, allAccumulators[type].bucket );
		const dbkey = currentHeader.dbKey();
		globalDb.get( dbkey )
			.then( (value) => {
				const meta = JSON.parse( String(value) );
				globalDb.put( dbkey, JSON.stringify( { ...meta,
													   accumulators: allAccumulators,
													   currentAccumulator: currentAccumulator[1] }));
			})
			.catch( (e) => {
				globalDb.put( dbkey, JSON.stringify( { start: Math.floor(now/1000),
													   startUtc: now.toISOString(),
													   accumulators: allAccumulators,
													   currentAccumulator: currentAccumulator[1] }));
			});
	}
}

//
// Dump the meta data for all the stations, we take from our in memory copy
// it will have been primed on start from the db and then we update and flush
// back to the disk
function produceStationFile( stations ) {

	// Form a list of hashes
	let statusOutput = _filter( stations, (v) => {return v.valid});

	// Write this to the stations.json file
	try {
		writeFileSync( OUTPUT_PATH + 'stations.json', JSON.stringify(statusOutput) );
		writeFileSync( OUTPUT_PATH + 'stations.json.gz', zlib.gzipSync(JSON.stringify(statusOutput)) );
	}
	catch(err) {
		console.log("stations.json write error",err)
	}
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

