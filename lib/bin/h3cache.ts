import {CoverageHeader} from './coverageheader';
import {CoverageRecord, bufferTypes} from './coveragerecord';

import {H3_CACHE_FLUSH_PERIOD_MS, H3_CACHE_MAXIMUM_DIRTY_PERIOD_MS} from '../common/config';

import {getStationName} from './stationstatus';

import {H3LockKey, StationId, EpochMS} from './types';

import {find as _find} from 'lodash';

import {updateH3, flushPendingH3s} from './rollupworker';

// Cache so we aren't constantly reading/writing from the db
let cachedH3s = new Map<H3LockKey, CacheDetails>();
let blockWrites = false;

interface CacheDetails {
    br: CoverageRecord;
    lastAccess: EpochMS;
    firstAccess: EpochMS;
}

export function unlockH3sForReads() {
    blockWrites = false;
}

export function getH3CacheSize() {
    return cachedH3s.size;
}

export const exportedForTest = {
    cachedH3s,
    blockWrites
};

interface FlushStats {
    total: number;
    expired: number;
    written: number;
    databases: number;
}

//
// This function writes the H3 buffers to the disk if they are dirty, and
// clears the records if it has expired. It is actually a synchronous function
// as it will not return before everything has been written
export async function flushDirtyH3s({allUnwritten = false}: {allUnwritten: boolean}): Promise<FlushStats> {
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

    // And if we want to lock then we MUST flush everything
    const h3sToFlush = cachedH3s;
    if (allUnwritten) {
        cachedH3s = new Map<H3LockKey, CacheDetails>(); // reset the cache to empty
    }

    // We will keep track of all the async actions to make sure we
    // don't get out of order during the lock() or return before everything
    // has been serialised
    let promises = [];

    // Go through all H3s in memory and write them out if they were updated
    // since last time we flushed
    for (const [h3klockkey, v] of h3sToFlush) {
        if (allUnwritten || v.lastAccess < flushTime || v.firstAccess < maxDirtyTime) {
            const station = getStationName(new CoverageHeader(h3klockkey).dbid);
            if (!station) {
                console.error(`unknown station ${h3klockkey}`);
            } else {
                promises.push(updateH3(station, h3klockkey, v.br.buffer()));
                stats.written++;
                if (!allUnwritten) {
                    cachedH3s.delete(h3klockkey);
                    stats.expired++;
                }
            }
        }
    }
    await Promise.allSettled(promises);

    stats.databases = (await flushPendingH3s()).databases;
    return stats;
}

export function updateCachedH3(h3k: CoverageHeader, altitude: number, agl: number, crc: number, signal: number, gap: number, packetStationId: StationId): void {
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
