import {CoverageHeader} from './coverageheader';
import {CoverageRecord, bufferTypes} from './coveragerecord';

import {H3_CACHE_FLUSH_PERIOD_MS, H3_CACHE_MAXIMUM_DIRTY_PERIOD_MS} from '../common/config';

import {getStationName} from './stationstatus';

import {H3, H3LockKey, StationId, EpochMS} from './types';

import {flushH3, flushPendingH3s} from './rollupworker';

import {getAccumulator, getCurrentAccumulators} from './accumulators';
import type {Accumulators} from './accumulators';

// Cache so we aren't constantly reading/writing from the db
let cachedH3s = new Map<H3LockKey, CacheDetails>();

interface CacheDetails {
    br: CoverageRecord;
    lastAccess: EpochMS;
    firstAccess: EpochMS;
}

export function getH3CacheSize() {
    return cachedH3s.size;
}

interface FlushStats {
    total: number;
    expired: number;
    written: number;
    databases: number;
    elapsed?: EpochMS;
}

let pendingFlush: Promise<FlushStats> | null = null;

//
// This function writes the H3 buffers to the disk if they are dirty, and
// clears the records if it has expired. It will not return before everything has been written
//
// It will block on the pending flush until the previous flush has completed, this ensures
// that a normal write doesn't get obliterated by a rollup flush and is simpler than
// using a lock
export async function flushDirtyH3s(accumulators?: Accumulators, allUnwritten: boolean = false): Promise<FlushStats> {
    if (pendingFlush) {
        console.log(`flushDirtyH3s(${allUnwritten}) blocked waiting for previous flush to complete`);
        await pendingFlush;
    }
    accumulators ??= getCurrentAccumulators() ?? superThrow('no accumulators when they MUST have been set');

    const start = Date.now();
    return (pendingFlush = flushDirtyH3sInternal(accumulators, allUnwritten))
        .then((stats) => {
            stats.elapsed = (Date.now() - start) as EpochMS;
            return stats;
        })
        .finally(() => {
            pendingFlush = null;
        });
}

// Internal actually do the work - used to ensure we only have one running at a time
async function flushDirtyH3sInternal(accumulators: Accumulators, allUnwritten: boolean = false): Promise<FlushStats> {
    //
    // When do we write and when do we expire
    const now = Date.now();
    const flushTime = Math.max(0, now - H3_CACHE_FLUSH_PERIOD_MS);
    const maxDirtyTime = Math.max(0, now - H3_CACHE_MAXIMUM_DIRTY_PERIOD_MS);

    let stats: FlushStats = {
        total: cachedH3s.size,
        expired: 0,
        written: 0,
        databases: 0
    };

    // Sort them in key order as the Map is sorted in action order which will mean bouncing around databases.
    // Before the new map is up to date don't use any functions that could yield (eg async call)
    const originalMap = cachedH3s;
    let sortedh3sToFlush: typeof cachedH3s;

    if (allUnwritten) {
        cachedH3s = new Map<H3LockKey, CacheDetails>(); // new map to be async safe
        sortedh3sToFlush = new Map([...originalMap].sort((a, b) => String(a[0]).localeCompare(b[0])));
    } else {
        const toWrite = new Map<H3LockKey, CacheDetails>();
        for (const [k, v] of originalMap) {
            if (v.lastAccess < flushTime || v.firstAccess < maxDirtyTime) {
                cachedH3s.delete(k);
                toWrite.set(k, v);
                if (v.lastAccess < flushTime) {
                    stats.expired++;
                }
            }
        }
        sortedh3sToFlush = new Map([...toWrite].sort((a, b) => String(a[0]).localeCompare(b[0])));
    }

    // We will keep track of all the async actions to make sure we
    // don't get out of order during the lock() or return before everything
    // has been serialised
    let promises = [];

    // Go through all H3s in memory and write them out if they were updated
    // since last time we flushed - we do this by sending them to the webworker
    // the worker accumulates all the transactions and then we call flush
    // to actually write them to the database
    for (const [h3klockkey, v] of sortedh3sToFlush) {
        const station = getStationName(new CoverageHeader(h3klockkey).dbid);
        if (!station) {
            console.error(`unknown station ${h3klockkey}`);
        } else {
            // Flush but don't wait
            promises.push(flushH3(station, h3klockkey, v.br.buffer()));
            stats.written++;
        }
    }
    await Promise.allSettled(promises);

    // Finally flush them all to disk and tag with metadata - use the metadata from when we started
    // as it may have rotated while we were writing
    stats.databases = (await flushPendingH3s(accumulators)).databases;
    return stats;
}

export function updateCachedH3(h3: H3, altitude: number, agl: number, crc: number, signal: number, gap: number, packetStationId: StationId, dbStationId: StationId): void {
    // Header details for our update - we look up the bucket and accumulator
    // here because it may have changed during the flush operation
    const h3k = new CoverageHeader(dbStationId, 'current', getAccumulator(), h3);

    // If we have some cached changes for this h3 then we will simply
    // use the entry in the 'dirty' table. This table gets flushed
    // on a periodic basis and saves us hitting the disk for very
    // busy h3s. We don't bother reading what is there we just start a new one
    // when it is sent to the DB it will be merged
    const cachedH3 = cachedH3s.get(h3k.lockKey);
    if (cachedH3) {
        cachedH3.lastAccess = Date.now() as EpochMS;
        cachedH3.br.update(altitude, agl, crc, signal, gap, packetStationId);
    } else {
        const buffer = new CoverageRecord(h3k.dbid ? bufferTypes.station : bufferTypes.global);
        buffer.update(altitude, agl, crc, signal, gap, packetStationId);
        cachedH3s.set(h3k.lockKey, {br: buffer, lastAccess: Date.now() as EpochMS, firstAccess: Date.now() as EpochMS});
    }
}

function superThrow(t: string): never {
    throw new Error(t);
}
