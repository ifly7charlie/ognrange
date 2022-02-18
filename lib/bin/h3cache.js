
import { CoverageHeader } from './coverageheader.js';

import { H3_CACHE_FLUSH_PERIOD_MS,
		 H3_CACHE_EXPIRY_TIME_MS,
		 H3_CACHE_MAXIMUM_DIRTY_PERIOD_MS } from './config.js'

import LevelUP from 'levelup';
import LevelDOWN from 'leveldown';

// h3cache locking
import { Lock } from 'lock'
export let H3lock = Lock();

import _find from 'lodash.find';

// Cache so we aren't constantly reading/writing from the db
export let cachedH3s = new Map();

//
// This function writes the H3 buffers to the disk if they are dirty, and 
// clears the records if it has expired. It is actually a synchronous function
// as it will not return before everything has been written
export async function flushDirtyH3s( { globalDb, stationDbCache, stations, allUnwritten = false}) {

	// When do we write and when do we expire
	const now = Date.now();
	const flushTime = Math.max( 0, now - H3_CACHE_FLUSH_PERIOD_MS);
	const maxDirtyTime = Math.max( 0, now - H3_CACHE_MAXIMUM_DIRTY_PERIOD_MS);
	const expiryTime = Math.max( 0, now - H3_CACHE_EXPIRY_TIME_MS);
	

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
			H3lock( h3klockkey, function (release) {

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

