import {CoverageHeader} from './coverageheader';
import {CoverageRecord, bufferTypes} from './coveragerecord';

import {H3_CACHE_FLUSH_PERIOD_MS, H3_CACHE_MAXIMUM_DIRTY_PERIOD_MS} from '../common/config';

import {getStationName} from './stationstatus';

import {H3, H3LockKey, StationId, StationName, EpochMS, superThrow} from './types';

import {flushBatch} from '../worker/rollupworker';

import {getAccumulator, getCurrentAccumulators, describeAccumulators} from './accumulators';
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

// Singleton for running so we don't have two at once
import AsyncLock from 'async-lock';
let lock = new AsyncLock();

//
// This function writes the H3 buffers to the disk if they are dirty, and
// removes expired entries from the cache (not the database). It will not return before everything has been written
//
// It will block on the pending flush until the previous flush has completed, this ensures
// that a normal write doesn't get obliterated by a rollup flush and is simpler than
// using a lock
export async function flushDirtyH3s(_accumulators?: Accumulators, allUnwritten: boolean = false): Promise<FlushStats> {
    //
    // Make sure we have a current accumulator
    const accumulators = _accumulators ?? getCurrentAccumulators() ?? superThrow('no accumulators when they MUST have been set');

    const start = Date.now();
    return lock
        .acquire(
            'h3',
            // Internal actually do the work - used to ensure we only have one running at a time
            async (): Promise<FlushStats> => {
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

                const accus: Record<string, number> = {};
                sortedh3sToFlush.forEach((_v, k) => {
                    const ca = k.slice(-20, -16);
                    accus[ca] = (accus[ca] ?? 0) + 1;
                });

                console.log('flushing h3cache accumulators:', accus);

                // Go through all H3s in memory and collect them for a single postMessage to the
                // worker thread. The worker fetches existing records and writes everything as a
                // batch, so we don't return before everything has been serialised
                const records: {station: StationName; h3lockkey: H3LockKey; buffer: Uint8Array}[] = [];

                for (const [h3klockkey, v] of sortedh3sToFlush) {
                    const station = getStationName(new CoverageHeader(h3klockkey).dbid);
                    if (!station) {
                        console.error(`unknown station ${h3klockkey}`);
                    } else {
                        records.push({station, h3lockkey: h3klockkey, buffer: v.br.buffer()});
                        stats.written++;
                    }
                }

                // Finally flush them all to disk and tag with metadata - use the metadata from when we started
                // as it may have rotated while we were writing
                stats.databases = (await flushBatch(records, accumulators)).databases;
                return stats;
            }
        )
        .then((stats) => {
            stats.elapsed = (Date.now() - start) as EpochMS;
            return stats;
        });
}

import {Layer} from '../common/layers';

export function updateCachedH3(h3: H3, altitude: number, agl: number, crc: number, signal: number, gap: number, packetStationId: StationId, dbStationId: StationId, layer: Layer = Layer.COMBINED): void {
    // Header details for our update - we look up the bucket and accumulator
    // here because it may have changed during the flush operation
    const h3k = new CoverageHeader(dbStationId, 'current', getAccumulator(), h3, layer);

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
