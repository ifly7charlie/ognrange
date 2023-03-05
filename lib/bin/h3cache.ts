import {CoverageHeader} from './coverageheader';
import {CoverageRecord, bufferTypes} from './coveragerecord';

import {H3_CACHE_FLUSH_PERIOD_MS, H3_CACHE_EXPIRY_TIME_MS, DB_PATH, H3_CACHE_MAXIMUM_DIRTY_PERIOD_MS} from '../common/config';

import {getDb} from './stationcache';
import {getStationName} from './stationstatus';

import {StationName, StationId} from './types';

// h3cache locking
import AsyncLock from 'async-lock';
let lock = new AsyncLock();

import _find from 'lodash.find';

import {setTimeout} from 'timers/promises';

// Cache so we aren't constantly reading/writing from the db
let cachedH3s = new Map();
let blockWrites = false;

export function unlockH3sForReads() {
    blockWrites = false;
}

export function getH3CacheSize() {
    return cachedH3s.size;
}

//
// This function writes the H3 buffers to the disk if they are dirty, and
// clears the records if it has expired. It is actually a synchronous function
// as it will not return before everything has been written
export async function flushDirtyH3s({allUnwritten = false, lockForRead = false}): Promise<any> {
    return lock.acquire('flushDirtyH3s', function (done) {
        internalFlushDirtyH3s({allUnwritten, lockForRead})
            .then((r) => done(null, r))
            .catch((e) => done(e, null));
    });
}

async function internalFlushDirtyH3s({allUnwritten = false, lockForRead = false}) {
    // When do we write and when do we expire
    const now = Date.now();
    const flushTime = Math.max(0, now - H3_CACHE_FLUSH_PERIOD_MS);
    const maxDirtyTime = Math.max(0, now - H3_CACHE_MAXIMUM_DIRTY_PERIOD_MS);
    const expiryTime = Math.max(0, now - H3_CACHE_EXPIRY_TIME_MS);

    let stats = {
        total: cachedH3s.size,
        expired: 0,
        written: 0,
        databases: 0
    };

    // If we are currently doing other work then don't write out here
    if (blockWrites) {
        console.log(`Not flushing H3s as currently blocked for writes`);
        return stats;
    }

    const h3sToFlush = cachedH3s;

    // And if we want to lock then we MUST flush everything
    // NOTE, locking should only be used when changing current accumulator
    if (lockForRead) {
        if (allUnwritten) {
            blockWrites = lockForRead;
            cachedH3s = new Map(); // reset the cache to empty
        } else {
            console.error('Lock requested but not all changes requested to be flushed');
            allUnwritten = true;
        }
    }

    if (blockWrites) {
        let pendingLocks: string[] = Object.keys(lock.queues);
        while (pendingLocks.length != 1) {
            console.log('pendinglocks', pendingLocks.join(','));
            await setTimeout(100);
            pendingLocks = Object.keys(lock.queues);
        }
    }

    // We will keep track of all the async actions to make sure we
    // don't get out of order during the lock() or return before everything
    // has been serialised
    let promises = [];

    const dbOps = new Map<StationId, any>(); //[station]=>list of db ops

    // Go through all H3s in memory and write them out if they were updated
    // since last time we flushed
    for (const [h3klockkey, v] of h3sToFlush) {
        promises.push(
            new Promise<void>((resolvePromise) => {
                // Because the DB is asynchronous we need to ensure that only
                // one transaction is active for a given h3 at a time, this will
                // block all the other ones until the first completes, it's per db
                // no issues updating h3s in different dbs at the same time
                lock.acquire(h3klockkey, function (releaseLock) {
                    // If we are dirty all we can do is write it out
                    if (v.dirty) {
                        // either periodic flush (eg before rollup) or flushTime elapsed
                        // or it's been in the cache so long we need to flush it
                        if (allUnwritten || v.lastAccess < flushTime || v.lastWrite < maxDirtyTime) {
                            const h3k = new CoverageHeader(h3klockkey);

                            // Add to the write out structures
                            let ops = dbOps.get(h3k.dbid);
                            if (!ops) {
                                dbOps.set(h3k.dbid, (ops = new Array()));
                            }
                            ops.push({type: 'put', key: h3k.dbKey(), value: Buffer.from(v.br.buffer())});
                            stats.written++;
                            v.lastWrite = now;
                            v.dirty = false;
                        }
                    }
                    // If we are clean then we can be deleted, don't purge if we are locked
                    // for read (v.dirty will not be set to false)
                    else if (v.lastAccess < expiryTime) {
                        cachedH3s.delete(h3klockkey);
                        stats.expired++;
                    }

                    // we are done, no race condition on write as it's either written to the
                    // disk above, or it was written earlier and has simply expired, it's not possible
                    // to expire and write (expiry is period after last write)... ie it's still
                    // in cache after write till expiry so only cache lock required for integrity
                    releaseLock();
                    resolvePromise();
                });
            })
        );
    }

    // We need to wait for all promises to complete before we can do the next part
    await Promise.allSettled(promises);
    promises = [];

    // So we know where to start writing
    stats.databases = dbOps.size;

    // Now push these to the database
    for (const [dbid, v] of dbOps) {
        promises.push(
            new Promise<void>((resolve) => {
                //
                getDb(dbid, {cache: true, open: true})
                    .then((db) => {
                        if (!db) {
                            console.error(`unable to find db for id ${dbid}/${getStationName(dbid)}`);
                            resolve();
                        } else {
                            // Execute all changes as a batch
                            db.batch(v, (e) => {
                                // log errors
                                if (e) console.error(`error flushing ${v.length} db operations for station ${db.ognStationName}`, e);
                                resolve();
                            });
                        }
                    })
                    .catch(resolve);
            })
        );
    }
    await Promise.allSettled(promises);
    return stats;
}

export async function updateCachedH3(db: StationName, h3k, altitude, agl, crc, signal, gap, stationid) {
    // Because the DB is asynchronous we need to ensure that only
    // one transaction is active for a given h3 at a time, this will
    // block all the other ones until the first completes, it's per db
    // no issues updating h3s in different dbs at the same time
    await lock.acquire(h3k.lockKey, function (release) {
        // If we have some cached changes for this h3 then we will simply
        // use the entry in the 'dirty' table. This table gets flushed
        // on a periodic basis and saves us hitting the disk for very
        // busy h3s
        const cachedH3 = cachedH3s.get(h3k.lockKey);
        if (cachedH3) {
            cachedH3.dirty = true;
            cachedH3.lastAccess = Date.now();
            cachedH3.br.update(altitude, agl, crc, signal, gap, stationid);
            release()();
        } else {
            const updateH3Entry = (value?: Uint8Array) => {
                const buffer = new CoverageRecord(value ? value : h3k.dbid ? bufferTypes.station : bufferTypes.global);
                buffer.update(altitude, agl, crc, signal, gap, stationid);
                cachedH3s.set(h3k.lockKey, {br: buffer, dirty: true, lastAccess: Date.now(), lastWrite: Date.now()});
                release();
            };

            // If we haven't allowed any writes yet then we can just make an empty one
            if (blockWrites) {
                updateH3Entry();
            } else {
                getDb(db, {throw: true, open: true, cache: true})
                    .then((db) => {
                        db.get(h3k.dbKey())
                            .then(updateH3Entry) // gets called with buffer
                            .catch((_) => updateH3Entry()); // not found, make new entry
                    })
                    .catch((_) => updateH3Entry()); // if db can't be opened
            }
        }
    });
}
