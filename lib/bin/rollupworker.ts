import {CoverageRecord, bufferTypes} from './coveragerecord';
import {CoverageHeader} from './coverageheader';

import {cloneDeep as _clonedeep, isEqual as _isequal, map as _map, reduce as _reduce, sortBy as _sortBy, filter as _filter, uniq as _uniq} from 'lodash';

import {writeFileSync, readFileSync, mkdirSync, unlinkSync, symlinkSync} from 'fs';

import {getDb, getDbThrow, DB, BatchOperation, closeAllStationDbs} from './stationcache';

import {Epoch, EpochMS, StationName, StationId, H3LockKey} from './types';

import {OUTPUT_PATH, UNCOMPRESSED_ARROW_FILES} from '../common/config';

import {Worker, parentPort, isMainThread, SHARE_ENV} from 'node:worker_threads';

import {CurrentAccumulator, Accumulators, AccumulatorTypeString} from './accumulators';
import {StationDetails} from './stationstatus';

import {backupDatabase as backupDatabaseInternal} from './backupdatabase';

interface RollupResult {
    elapsed: number;
    operations: number;
}

export interface RollupDatabaseArgs {
    validStations?: Set<StationId>;
    now: number;
    current: CurrentAccumulator;
    processAccumulators: Accumulators;
    needValidPurge: boolean;
    stationMeta: StationDetails | undefined;
}

interface RollupStartupAccumulators {
    current: CurrentAccumulator;
    processAccumulators: Accumulators;
}

//
// Record of all the outstanding transactions
const promises: Record<string, {resolve: Function}> = {};

// So we can wait for all of them
const h3promises: Promise<void>[] = [];
//
// Start the worker thread
const worker = isMainThread ? new Worker(__filename, {env: SHARE_ENV}) : null;

// Update the disk version of the H3 by transferring the buffer record to
// the worker thread, the buffer is NO LONGER VALID
export async function updateH3(station: StationName, h3lockkey: H3LockKey, buffer: Uint8Array) {
    if (!worker) {
        return;
    }
    const donePromise = new Promise<void>((resolve) => {
        promises[h3lockkey + '_updateH3'] = {resolve};
        worker.postMessage({action: 'updateH3', now: Date.now(), station, h3lockkey, buffer}, [buffer.buffer]);
    });

    // Keep copy so we can wait for it and also return it so the caller can wait
    h3promises.push(donePromise);
    return donePromise;
}

// Send all the recently flushed operations to the disk, called after h3cache flush is done and
// before a rollup can start
export async function flushPendingH3s(): Promise<{databases: number}> {
    if (!worker) {
        return {databases: 0};
    }
    // Make sure all the h3promises are settled, then reset that - we don't
    // care what happens just want to make sure we don't flush too early
    await Promise.allSettled(h3promises);
    h3promises.length = 0;

    return new Promise<{databases: number}>((resolve) => {
        promises['all_flushPending'] = {resolve};
        worker.postMessage({station: 'all', action: 'flushPending', now: Date.now()});
    });
}

export async function shutdownRollupWorker() {
    if (!worker) {
        return;
    }

    // Do the sync in the worker thread
    return new Promise<void>((resolve) => {
        promises['all_shutdown'] = {resolve};
        worker.postMessage({station: 'all', action: 'shutdown', now: Date.now()});
    });
}

export async function rollupStartup(station: StationName, whatAccumulators: RollupStartupAccumulators, stationMeta?: StationDetails): Promise<any> {
    if (!worker) {
        return;
    }

    // Do the sync in the worker thread
    return new Promise<any>((resolve) => {
        promises[station + '_startup'] = {resolve};
        worker.postMessage({station, action: 'startup', now: Date.now(), whatAccumulators, stationMeta});
    });
}

export async function rollupAbortStartup() {
    if (!worker) {
        return;
    }
    return new Promise<void>((resolve) => {
        promises['all_abortstartup'] = {resolve};
        worker.postMessage({station: 'all', action: 'abortstartup', now: Date.now()});
    });
}
export async function rollupDatabase(station: StationName, commonArgs: RollupDatabaseArgs): Promise<RollupResult | void> {
    if (!worker) {
        return;
    }

    // Safety check
    if (h3promises.length) {
        console.error(`rollupDatabase ${station} requested but h3s pending to disk`);
        await flushPendingH3s();
    }

    return new Promise<RollupResult>((resolve) => {
        promises[station + '_rollup'] = {resolve};
        worker.postMessage({station, action: 'rollup', ...commonArgs});
    });
}

export async function purgeDatabase(station: StationName): Promise<any> {
    if (!worker) {
        return;
    }
    return new Promise<any>((resolve) => {
        promises[station + '_purge'] = {resolve};
        worker.postMessage({station, action: 'purge'});
    });
}

export async function backupDatabase(station: StationName, whatAccumulators: Accumulators): Promise<{rows: number; elapsed: EpochMS}> {
    if (!worker) {
        return {rows: 0, elapsed: 0 as EpochMS};
    }
    // Do the sync in the worker thread
    return new Promise<any>((resolve) => {
        promises[station + '_backup'] = {resolve};
        worker.postMessage({station, action: 'backup', now: Date.now(), whatAccumulators});
    });
}

// block startup from continuing - variable in worker thread only
let abortStartup = false;

let h3dbOps = new Map<StationName, BatchOperation[]>();

//
// Inbound in the thread Dispatch to the correct place
if (!isMainThread) {
    parentPort!.on('message', async (task) => {
        let out: any = {success: false};
        try {
            switch (task.action) {
                case 'updateH3':
                    await writeH3ToDB(task.station, task.h3lockkey, task.buffer);
                    parentPort!.postMessage({action: task.action, h3lockkey: task.h3lockkey, success: true});
                    return;
                case 'flushPending':
                    out = await flushH3DbOps();
                    parentPort!.postMessage({action: task.action, ...out, station: task.station, success: true});
                    return;
                case 'shutdown':
                    await closeAllStationDbs();
                    parentPort!.postMessage({action: task.action, ...out, station: task.station, success: true});
                    return;
            }

            let db: DB | undefined = undefined;
            try {
                db = await getDb(task.station, {cache: false, open: true, noMeta: true});

                if (db) {
                    switch (task.action) {
                        case 'rollup':
                            out = await rollupDatabaseInternal(db, task);
                            break;
                        case 'abortstartup':
                            out = {success: true};
                            abortStartup = true;
                            break;
                        case 'startup':
                            out = !abortStartup ? await rollupDatabaseStartup(db, task) : {success: false};
                            if (out.success && !abortStartup) {
                                const db = await getDb(task.station, {cache: true, open: true, throw: false});
                                await db?.compactRange('0', 'Z');
                                out.datacompacted = true;
                            }
                            break;
                        case 'purge':
                            await purgeDatabaseInternal(db, 'purge');
                            break;
                        case 'backup':
                            out = await backupDatabaseInternal(db, task);
                            break;
                    }
                }
            } catch (e) {
                console.error(task, e);
            }
            parentPort!.postMessage({action: task.action, station: task.station, ...out});
        } catch (e) {
            console.error(task, e);
        }
    });
}

//
// Response from the thread will finish the promise created when the message is called
// and pass the response values to the
else {
    worker!.on('message', (data) => {
        const promiseKey = (data.h3lockkey ?? data.station) + '_' + data.action;
        const resolver = promises[promiseKey]?.resolve;
        delete promises[promiseKey];
        if (resolver) {
            resolver(data);
        } else {
            console.error(`missing resolve function for ${promiseKey}/`);
        }
    });
}

// Clear data from the db
async function purge(db: DB, hr: CoverageHeader) {
    // See if there are actually data entries
    const first100KeyCount = (await db.keys({...CoverageHeader.getDbSearchRangeForAccumulator(hr.type, hr.bucket, false), limit: 50}).all()).length;

    // Now clear and compact
    await db.clear(CoverageHeader.getDbSearchRangeForAccumulator(hr.type, hr.bucket, true));
    //    await db.compactRange(
    //      CoverageHeader.getAccumulatorBegin(hr.type, hr.bucket, true), //
    //        CoverageHeader.getAccumulatorEnd(hr.type, hr.bucket)
    //    );

    // And provide a status update
    if (first100KeyCount) {
        console.log(`${db.ognStationName}: ${hr.typeName} - ${hr.dbKey()} purged [${first100KeyCount > 49 ? '>49' : first100KeyCount}] entries successfully`);
    }
}

//
// We need to make sure we know what rollups the DB has, and process pending rollup data
// when the process starts. If we don't do this all sorts of weird may happen
// (only used for global but could theoretically be used everywhere)
export async function rollupDatabaseStartup(
    db: DB, //
    {now, whatAccumulators, stationMeta}: {now: number; whatAccumulators: {current: CurrentAccumulator; processAccumulators: Accumulators}; stationMeta: any}
) {
    let accumulatorsToPurge: Record<string, {accumulator: string; meta: null | any; typeName: string; t: number; b: number; file?: string}> = {};
    let hangingCurrents: Record<string, any> = {};
    let datapurged = false;
    let datamerged = false;

    // Our accumulators
    const {current: expectedCurrentAccumulator, processAccumulators: expectedAccumulators} = whatAccumulators;

    // We need a current that is basically unique so we don't rollup the wrong thing at the wrong time
    // our goal is to make sure we survive restart without getting same code if it's not the same day...
    // if you run this after an 8 year gap then welcome back ;) and I'm sorry ;)  [it has to fit in 12bits]
    const expectedCurrentAccumulatorBucket = expectedCurrentAccumulator[1];

    // First thing we need to do is find all the accumulators in the database
    let iterator = db.iterator();
    let iteratorPromise = iterator.next(),
        row = null;

    while ((row = await iteratorPromise)) {
        const [key, value] = row;
        let hr = new CoverageHeader(key);

        // 80000000 is the h3 cell code we use to
        // store the metadata for our iterator
        if (!hr.isMeta) {
            accumulatorsToPurge[hr.accumulator] = {accumulator: hr.accumulator, meta: null, typeName: hr.typeName, t: hr.type, b: hr.bucket};
            //console.log(`${db.ognStationName}: purging entry without metadata ${hr.lockKey}`);
            iterator.seek(CoverageHeader.getAccumulatorEnd(hr.type, hr.bucket));
            iteratorPromise = iterator.next();
            continue;
        }
        const meta = JSON.parse(String(value)) || {};

        // If it's a current and not OUR current then we need to
        // figure out what to do with it... We may merge it into rollup
        // accumulators if it was current when they last updated their meta
        if (hr.typeName == 'current') {
            if (hr.bucket != expectedCurrentAccumulatorBucket) {
                hangingCurrents[hr.dbKey()] = meta;
            } else if (db.global) {
                console.log(`${db.ognStationName}: current: resuming accumulator ${hr.accumulator} (${hr.bucket}) as still valid [stated at: ${meta.startUtc}]`);
            }
        }

        // accumulator not configured on this machine - dump and purge
        else if (!expectedAccumulators[hr.typeName]) {
            accumulatorsToPurge[hr.accumulator] = {accumulator: hr.accumulator, meta: meta, typeName: hr.typeName, t: hr.type, b: hr.bucket};
        }
        // new bucket for the accumulators - we should dump this
        // and purge as adding new data to it will cause grief
        // note the META will indicate the last active accumulator
        // and we should merge that if we find it
        else if (expectedAccumulators[hr.typeName]!.bucket != hr.bucket) {
            accumulatorsToPurge[hr.accumulator] = {accumulator: hr.accumulator, meta: meta, typeName: hr.typeName, t: hr.type, b: hr.bucket};
        } else if (db.global) {
            console.log(`${db.ognStationName}: ${hr.typeName}: resuming accumulator ${hr.accumulator} (${hr.bucket}) as still valid  [started at: ${meta.startUtc}]`);
        }

        // Done with this one lets skip forward
        iterator.seek(CoverageHeader.getAccumulatorEnd(hr.type, hr.bucket));
        if (db.status != 'open') {
            console.log(db);
        }
        iteratorPromise = iterator.next();
    }

    //
    // We will add meta data to the database for each of the current accumulators
    // this makes it easier to check what needs to be done?
    {
        const dbkey = CoverageHeader.getAccumulatorMeta(...expectedCurrentAccumulator).dbKey();

        // Get existing and add it to the history
        try {
            const meta = await db
                .get(dbkey)
                .then((value) => {
                    const meta = JSON.parse(String(value));
                    meta.oldStarts = [...(meta?.oldStarts || []), {start: meta.start, startUtc: meta.startUtc}];
                    return meta;
                })
                .catch((e) => {
                    if (e.code == 'LEVEL_NOT_FOUND') {
                        return {
                            start: Math.floor(now / 1000),
                            startUtc: new Date(now).toISOString()
                        };
                    }
                    throw e;
                });
            meta.start = Math.floor(now / 1000);
            meta.startUtc = new Date(now).toISOString();
            await db.put(dbkey, Uint8FromObject(meta));
        } catch (e: any) {
            console.error(`unable to save metadata for ${db.ognStationName}/current - error ${e.code}, db status ${db.status}`, e);
        }

        // make sure we have an up to date header for each accumulator
        const currentAccumulatorHeader = CoverageHeader.getAccumulatorMeta(...expectedCurrentAccumulator);
        for (const typeString in expectedAccumulators) {
            const type = typeString as AccumulatorTypeString;
            const dbkey = CoverageHeader.getAccumulatorMeta(type, expectedAccumulators[type]!.bucket).dbKey();
            try {
                const meta = await db
                    .get(dbkey)
                    .then((value) => JSON.parse(String(value)))
                    .catch((e) => {
                        if (e.code == 'LEVEL_NOT_FOUND') {
                            return {start: Math.floor(now / 1000), startUtc: new Date(now).toISOString(), currentAccumulator: currentAccumulatorHeader.bucket};
                        }
                        throw e;
                    });

                await db.put(dbkey, Uint8FromObject(meta));
            } catch (e: any) {
                console.error(`unable to save metadata for ${db.ognStationName}/${type} - error ${e.code}, db status ${db.status}`, e);
            }
        }
    }

    // This is more interesting, this is a current that could be rolled into one of the other
    // existing accumulators...
    if (hangingCurrents) {
        // we need to purge these
        for (const key in hangingCurrents) {
            const hangingHeader = new CoverageHeader(key);

            const meta = hangingCurrents[key];

            // So we need to figure out what combination of expected and existing accumulators should
            // be updated with the hanging accumulator, if we don't have the bucket anywhere any more
            // then we zap it. Note the buckets can't change we will only ever roll up into the buckets
            // the accumulator was started with
            let rollupAccumulators: Accumulators = {};
            for (const typeString of Object.keys(expectedAccumulators)) {
                const type = typeString as AccumulatorTypeString;
                const ch = CoverageHeader.getAccumulatorMeta(type, meta.accumulators?.[type]?.bucket || -1);
                if (accumulatorsToPurge[ch.accumulator]) {
                    rollupAccumulators[type] = {bucket: ch.bucket, file: accumulatorsToPurge[ch.accumulator].file || ''};
                } else if (expectedAccumulators[type]!.bucket == ch.bucket) {
                    rollupAccumulators[type] = {bucket: ch.bucket, file: expectedAccumulators[type]!.file};
                }
            }

            if (Object.keys(rollupAccumulators).length) {
                const rollupResult = await rollupDatabaseInternal(db, {current: [hangingHeader.typeName, hangingHeader.bucket], processAccumulators: rollupAccumulators, now, needValidPurge: false, stationMeta});
                console.log(`${db.ognStationName}: rolled up hanging current accumulator ${hangingHeader.accumulator} into ${JSON.stringify(rollupAccumulators)}: ${JSON.stringify(rollupResult)}`);
                datamerged = true;
            } else {
                //                console.log(`${db.ognStationName}: purging hanging current accumulator ${JSON.stringify(hangingHeader)} and associated sub accumulators`);
                // now we clear it
                await purge(db, hangingHeader);
                datapurged = true;
            }
        }
    }

    // These are old accumulators we purge them because we aren't sure what else can be done
    for (const key in accumulatorsToPurge) {
        await purge(db, new CoverageHeader(key));
        datapurged = true;
    }

    return {success: true, datapurged, datamerged, datachanged: datamerged || datapurged};
}

async function purgeDatabaseInternal(db: DB, reason: string) {
    // empty the database... we could delete it but this is very simple and should be good enough
    console.log(`clearing database for ${db.ognStationName} because ${reason}`);
    await db.clear();
    return;
}

//
// Rotate and Rollup all the data we have
// we do this by iterating through each database looking for things in default
// aggregator (which is always just the raw h3id)
//
async function rollupDatabaseInternal(db: DB, {validStations, now, current, processAccumulators, needValidPurge, stationMeta}: RollupDatabaseArgs): Promise<RollupResult> {
    //
    //
    const nowEpoch = Math.floor(now / 1000) as Epoch;
    const startTime = Date.now();
    const name = db.ognStationName;
    let currentMeta = {};

    if (needValidPurge && !validStations) {
        throw new Error('invalid arguments to rollupDatabaseInternal( needValidPurge && ! validStations)');
    }

    //
    // Use the correct accumulators as passed in - not the static/global ones
    const allAccumulators = _clonedeep(processAccumulators);
    allAccumulators['current'] = {bucket: current[1], file: ''};

    //	const log = stationName == 'tatry1' ? console.log : ()=>false;
    const log = () => 0;

    let dbOps: BatchOperation[] = [];
    let h3source = 0;

    //
    // Basically we finish our current accumulator into the active buckets for each of the others
    // and then we need to check if we should be moving them to new buckets or not

    // We step through all of the items together and update as one
    const rollupIterators = Object.keys(processAccumulators).map((key: string): any => {
        const r = key as AccumulatorTypeString;
        const par = processAccumulators[r];
        return {
            type: r,
            bucket: par!.bucket,
            file: par!.file,
            meta: {rollups: []},
            stats: {
                h3missing: 0,
                h3noChange: 0,
                h3updated: 0,
                h3emptied: 0,
                h3stationsRemoved: 0,
                h3extra: 0
            },
            iterator: db.iterator(CoverageHeader.getDbSearchRangeForAccumulator(r, par!.bucket)),
            arrow: CoverageRecord.initArrow(db.global ? bufferTypes.global : bufferTypes.station)
        };
    });

    // Enrich with the meta data for each accumulator type
    await Promise.allSettled(
        [...rollupIterators, {type: current[0], bucket: current[1]}].map(
            (r) =>
                new Promise<void>((resolve) => {
                    const ch = CoverageHeader.getAccumulatorMeta(r.type, r.bucket);
                    db.get(ch.dbKey())
                        .then((value) => {
                            if (r.type == current[0] && r.bucket == current[1]) {
                                currentMeta = JSON.parse(value.toString());
                            } else {
                                r.meta = JSON.parse(value?.toString() || '{}');
                            }
                            resolve();
                        })
                        .catch((e) => {
                            resolve();
                        });
                })
        )
    );

    // Initalise the rollup array with [k,v]
    const rollupData = rollupIterators.map((r: any) => {
        return {n: r.iterator.next(), current: null, h3kr: new CoverageHeader('0000/00_fake'), ...r};
    });

    // Create the 'outer' iterator - this walks through the primary accumulator
    for await (const [key, value] of db.iterator(CoverageHeader.getDbSearchRangeForAccumulator(...current))) {
        // The 'current' value - ie the data we are merging in.
        const h3p = new CoverageHeader(key);

        if (h3p.isMeta) {
            continue;
        }

        const currentBr = new CoverageRecord(value);
        let advancePrimary: boolean;
        const seth3k = (r: any, t: any) => {
            t && t[0] && r.h3kr.fromDbKey(t[0]);
            return t;
        };

        do {
            advancePrimary = true;

            // now we go through each of the rollups in lockstep
            // we advance and action all of the rollups till their
            // key matches the outer key. This is async so makes sense
            // to do them interleaved even though we await
            for (const r of rollupData) {
                // iterator async so wait for it to complete
                let [prefixedh3r, rollupValue] = r.current ? r.current : (r.current = r.n ? seth3k(r, await r.n) || [null, null] : [null, null]);

                // We have hit the end of the data for the accumulator but we still have items
                // then we need to copy the next data across -
                if (!prefixedh3r) {
                    if (r.lastCopiedH3p != h3p.h3) {
                        const h3kr = h3p.getAccumulatorForBucket(r.type, r.bucket);
                        dbOps.push({type: 'put', key: h3kr.dbKey(), value: currentBr.buffer()});
                        currentBr.appendToArrow(h3kr, r.arrow);
                        r.lastCopiedH3p = h3p.h3;
                        r.stats.h3missing++;
                    }

                    // We need to cleanup when we are done
                    if (r.n) {
                        r.current = null;
                        r.iterator.close();
                        r.n = null;
                    }
                    continue;
                }

                const h3kr = r.h3kr; //.fromDbKey(prefixedh3r);
                if (h3kr.isMeta) {
                    // skip meta
                    console.log(`unexpected meta information processing ${db.ognStationName}, ${r.type} at ${h3kr.dbKey()}, ignoring`);
                    advancePrimary = false; // we need more
                    continue;
                }

                // One check for ordering so we know if we need to
                // advance or are done
                const ordering = CoverageHeader.compareH3(h3p, h3kr);

                // Need to wait for others to catch up and to advance current
                // (primary is less than rollup) depends on await working twice on
                // a promise (await r.n above) because we haven't done .next()
                // this is fine but will yield which is also fine. note we
                // never remove stations from source
                if (ordering < 0) {
                    if (r.lastCopiedH3p != h3p.h3) {
                        const h3kr = h3p.getAccumulatorForBucket(r.type, r.bucket);
                        dbOps.push({type: 'put', key: h3kr.dbKey(), value: currentBr.buffer()});
                        currentBr.appendToArrow(h3kr, r.arrow);
                        r.lastCopiedH3p = h3p.h3;
                        r.stats.h3missing++;
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
                if (ordering > 0) {
                    updatedBr = validStations ? br.removeInvalidStations(validStations) : br;
                    advancePrimary = false; // we need more to catch up to primary
                    r.stats.h3stationsRemoved += updatedBr == br ? 0 : 1;
                }

                // Otherwise we are the same so we need to rollup into it, but only once!
                else {
                    if (r.lastCopiedH3p == h3p.h3) {
                        continue;
                    }

                    updatedBr = br.rollup(currentBr, validStations);
                    changed = true; // updatedBr may not always change so
                    r.lastCopiedH3p = h3p.h3;
                    // we are caught up to primary so allow advance if everybody else is fine
                }

                // Check to see what we need to do with the database
                // this is a pointer check as pointer will ALWAYS change on
                // adjustment
                if (changed || updatedBr != br) {
                    if (!updatedBr) {
                        dbOps.push({type: 'del', key: prefixedh3r});
                        r.stats.h3emptied++;
                    } else {
                        r.stats.h3updated++;
                        dbOps.push({type: 'put', key: prefixedh3r, value: updatedBr.buffer()});
                    }
                } else {
                    r.stats.h3noChange++;
                }

                // If we had data then write it out
                if (updatedBr) {
                    updatedBr.appendToArrow(h3kr, r.arrow);
                }

                // Move us to the next one, allow
                r.n = r.iterator.next();
                r.current = null;
            }
        } while (!advancePrimary);

        // Once we have accumulated we delete the accumulator key
        h3source++;
        dbOps.push({type: 'del', key: h3p.dbKey()});
    }

    // Finally if we have rollups with data after us then we need to update their invalidstations
    // now we go through them in step form
    for (const r of rollupData) {
        if (r.n) {
            let n = await r.n;
            let [prefixedh3r, rollupValue] = n || [null, null];

            while (prefixedh3r) {
                const h3kr = new CoverageHeader(prefixedh3r);
                let br = new CoverageRecord(rollupValue);

                let updatedBr = validStations ? br.removeInvalidStations(validStations) : br;

                // Check to see what we need to do with the database
                if (updatedBr != br) {
                    r.stats.h3stationsRemoved++;
                    if (!updatedBr) {
                        dbOps.push({type: 'del', key: prefixedh3r});
                        r.stats.h3emptied++;
                    } else {
                        dbOps.push({type: 'put', key: prefixedh3r, value: updatedBr.buffer()});
                        r.stats.h3updated++;
                    }
                } else {
                    r.stats.h3noChange++;
                }

                if (updatedBr) {
                    updatedBr.appendToArrow(h3kr, r.arrow);
                }

                r.stats.h3extra++;

                // Move to the next one, we don't advance till nobody has moved forward
                r.n = r.iterator.next();
                n = r.n ? await r.n : undefined;
                [prefixedh3r, rollupValue] = n || [null, null]; // iterator async so wait for it to complete
            }

            r.n = null;
            r.iterator.close();
        }
    }

    // Write everything out
    for (const r of rollupData) {
        const rType = r.type as AccumulatorTypeString;

        // We are going to write out our accumulators this saves us writing it
        // in a different process and ensures that we always write the correct thing
        const accumulatorName = `${name}/${name}.${rType}.${processAccumulators[rType]!.file}`;

        // Keep a record of all the rollups in the meta
        // each record
        if (!r.meta.rollups) {
            r.meta.rollups = [];
        }
        r.stats.dbOps = dbOps.length;
        r.stats.h3source = h3source;
        r.meta.rollups.push({source: currentMeta, stats: r.stats, file: accumulatorName});

        if (r.stats.h3source != r.stats.h3missing + r.stats.h3updated) {
            console.error("********* stats don't add up ", rType, r.bucket.toString(16), JSON.stringify({m: r.meta, s: r.stats}));
        }

        // May not have a directory if new station
        mkdirSync(OUTPUT_PATH + name, {recursive: true});

        // Finalise the arrow table and serialise it to the disk
        CoverageRecord.finalizeArrow(r.arrow, OUTPUT_PATH + accumulatorName + '.arrow');

        try {
            writeFileSync(OUTPUT_PATH + accumulatorName + '.json', JSON.stringify(r.meta, null, 2));
        } catch (err) {
            console.log('rollup json metadata write failed', err);
        }

        // Fix directory index
        let index: any = {};
        try {
            const data = readFileSync(OUTPUT_PATH + `${name}/${name}.index.json`, 'utf8');
            index = JSON.parse(data);
        } catch (e: any) {
            if (e.code != 'ENOENT') {
                console.log(`unable to read file index ${name} ${e}`);
            }
        }

        if (!index.files) {
            index.files = {};
        }

        index.files[r.type] = {current: accumulatorName, all: _uniq([...(index.files[r.type]?.all || []), accumulatorName])};

        try {
            writeFileSync(OUTPUT_PATH + `${name}/${name}.index.json`, JSON.stringify(index, null, 2));
        } catch (err) {
            console.log(`station ${name} index write error`, err);
        }

        // link it all up for latest
        symlink(`${name}.${r.type}.${processAccumulators[rType]!.file}.arrow.gz`, OUTPUT_PATH + `${name}/${name}.${r.type}.arrow.gz`);
        if (UNCOMPRESSED_ARROW_FILES) {
            symlink(`${name}.${r.type}.${processAccumulators[rType]!.file}.arrow`, OUTPUT_PATH + `${name}/${name}.${r.type}.arrow`);
        }
        symlink(`${name}.${r.type}.${processAccumulators[rType]!.file}.json`, OUTPUT_PATH + `${name}/${name}.${r.type}.json`);
    }

    // Only output if we have some meta
    if (stationMeta) {
        stationMeta.lastOutputFile = nowEpoch;

        // record when we wrote for the whole station
        try {
            writeFileSync(OUTPUT_PATH + `${name}/${name}.json`, JSON.stringify(stationMeta, null, 2));
        } catch (err) {
            console.log(`${OUTPUT_PATH}${name}/${name}.json stationmeta write error`, err);
        }
    }

    // Make sure we have updated the meta data
    await saveAccumulatorMetadata(db, current, allAccumulators);

    // If we have a new accumulator then we need to purge the old meta data records - we
    // have already purged the data above
    dbOps.push({type: 'del', key: CoverageHeader.getAccumulatorMeta(...current).dbKey()});
    if (db.global) {
        console.log(`rollup: current bucket ${[...current]} completed, removing ${CoverageHeader.getAccumulatorMeta(...current).dbKey()}`);
    }

    // Is this actually beneficial? - feed operations to the database in key type sorted order
    // so it can just process them. Keys should be stored clustered so theoretically this will
    // help with writing but perhaps benchmarking is a good idea
    dbOps = _sortBy(dbOps, ['key', 'type']);

    //
    // Finally execute all the operations on the database
    await db.batch(dbOps);

    // Purge everything from the current accumulator, this should just do a compact as we
    // have already deleted in the batch above
    await purge(db, CoverageHeader.getAccumulatorMeta(...current));

    return {elapsed: Date.now() - startTime, operations: dbOps.length};
}

function symlink(src: string, dest: string) {
    try {
        unlinkSync(dest);
    } catch (e) {}
    try {
        symlinkSync(src, dest, 'file');
    } catch (e) {
        console.log(`error symlinking ${src}.arrow to ${dest}: ${e}`);
    }
}

function Uint8FromObject(o: Record<any, any>): Uint8Array {
    return Uint8Array.from(Buffer.from(JSON.stringify(o)));
}

// This reads the DB for the record and then adds data to it - it's how we get data from the APRS
// (main) thread to the DB thread
async function writeH3ToDB(station: StationName, h3lockkey: H3LockKey, buffer: Uint8Array): Promise<void> {
    const h3k = new CoverageHeader(h3lockkey);
    const cr = new CoverageRecord(buffer);

    const existingOperation = h3dbOps.get(station) ?? [];

    // Save it back for flushing - we can still update it as by reference
    // and this ensures that everybody in the async code following updates the
    // same array
    if (!existingOperation.length) {
        h3dbOps.set(station, existingOperation);
    }

    const getOperation = (db: DB): Promise<BatchOperation | null> =>
        db
            .get(h3k.dbKey())
            .then((dbData: Uint8Array): BatchOperation | null => {
                const newCr = cr.rollup(new CoverageRecord(dbData));
                if (newCr) {
                    return {type: 'put', key: h3k.dbKey(), value: newCr.buffer()};
                } else {
                    return null;
                }
            })
            .catch((): BatchOperation => {
                // If we don't have a record then we can just use the raw value we received
                return {type: 'put', key: h3k.dbKey(), value: buffer};
            });

    await getDbThrow(station, {open: true, cache: true})
        .then((db: DB) => getOperation(db))
        .then((operation) => {
            if (operation) {
                existingOperation.push(operation);
            }
        })

        .catch((e) => {
            console.error(`unable to find db for id ${h3k.dbid}/${station}, ${e}`);
        });
}

// Flush all writes pending in the dbOps table
async function flushH3DbOps(): Promise<{databases: number}> {
    const promises: Promise<void>[] = [];

    const outputOps = h3dbOps;
    h3dbOps = new Map<StationName, BatchOperation[]>();

    console.log(`flushH3DbOps :${h3dbOps.size} dbs`);

    // Now push these to the database
    for (const [station, v] of outputOps) {
        promises.push(
            new Promise<void>((resolve) => {
                //
                getDb(station, {cache: true, open: true})
                    .then((db) => {
                        if (!db) {
                            console.error(`unable to find db for ${station}, discarding ${v.length} operations`);
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
    return {databases: outputOps.size};
}

// Helpers for testing
export const exportedForTest = {
    rollupDatabaseInternal,
    rollupDatabaseStartup,
    purgeDatabaseInternal,
    flushH3DbOps,
    writeH3ToDB
};

export async function saveAccumulatorMetadata(db: DB, currentAccumulator: CurrentAccumulator, allAccumulators: Accumulators): Promise<void> {
    const dbkey = CoverageHeader.getAccumulatorMeta(...currentAccumulator).dbKey();
    const now = new Date();
    const nowEpoch = Math.trunc(now.valueOf() / 1000);
    await db
        .get(dbkey)
        .then((value) => {
            const meta = JSON.parse(String(value));
            meta.oldStarts = [...meta?.oldStarts, {start: meta.start, startUtc: meta.startUtc}];
            meta.accumulators = allAccumulators;
            meta.start = nowEpoch;
            meta.startUtc = now.toISOString();
            db.put(dbkey, Uint8FromObject(meta));
        })
        .catch((e) => {
            db.put(
                dbkey,
                Uint8FromObject({
                    accumulators: allAccumulators,
                    oldStarts: [],
                    start: nowEpoch,
                    startUtc: now.toISOString()
                })
            );
        });
    // make sure we have an up to date header for each accumulator
    for (const typeString in allAccumulators) {
        const type = typeString as AccumulatorTypeString;
        const currentHeader = CoverageHeader.getAccumulatorMeta(type, allAccumulators[type]!.bucket);
        const dbkey = currentHeader.dbKey();
        await db
            .get(dbkey)
            .then((value) => {
                const meta = JSON.parse(String(value));
                db.put(dbkey, Uint8FromObject({...meta, accumulators: allAccumulators, currentAccumulator: currentAccumulator[1]}));
            })
            .catch((e) => {
                db.put(dbkey, Uint8FromObject({start: nowEpoch, startUtc: now.toISOString(), accumulators: allAccumulators, currentAccumulator: currentAccumulator[1]}));
            });
    }
}
