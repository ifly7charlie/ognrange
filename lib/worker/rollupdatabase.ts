import {CoverageRecord, bufferTypes} from '../bin/coveragerecord';
import {CoverageHeader} from '../bin/coverageheader';
import {CoverageRecordWriter} from '../bin/coveragerecordwriter';

//import {cloneDeep as _clonedeep, isEqual as _isequal, map as _map, reduce as _reduce, sortBy as _sortBy, filter as _filter, uniq as _uniq} from 'lodash';

import {sortBy as _sortBy, uniq as _uniq} from 'lodash';

import {writeFileSync, readFileSync, mkdirSync, unlinkSync, symlinkSync, renameSync} from 'fs';

import {DB, BatchOperation} from './stationcache';

import {Epoch, EpochMS, StationName, StationId} from '../bin/types';

import {OUTPUT_PATH, UNCOMPRESSED_ARROW_FILES} from '../common/config';

import {Accumulators, AccumulatorTypeString, describeAccumulators} from '../bin/accumulators';
import {StationDetails} from '../bin/stationstatus';

import {saveAccumulatorMetadata} from './rollupmetadata';

import {RollupWorkerResult} from './rollupworker';

export type RollupDatabaseCommand = {
    action: 'rollup';
    station: StationName;
    now: EpochMS;
    commonArgs: RollupDatabaseArgs;
};

export interface RollupResult extends RollupWorkerResult {
    elapsed: number;
    operations: number;
    recordsRemoved: number;
    retiredBuckets: number;
    arrowRecords: number;
}

export interface RollupDatabaseArgs {
    validStations?: Set<StationId>;
    now: Epoch;
    accumulators: Accumulators;
    needValidPurge: boolean;
    stationMeta: StationDetails | undefined;
    historical?: boolean;
    wasMissing?: AccumulatorTypeString[];
    retiredAccumulators?: AccumulatorTypeString[]; // if we are rotating then this should be set
}

import type {DBMetaRecord} from './rollupmetadata';

export type AccumulatorsExtended = {
    [key in AccumulatorTypeString]: {
        found?: boolean;
    };
};

//
//
// Rotate and Rollup all the data we have
// we do this by iterating through each database looking for things in default
// aggregator (which is always just the raw h3id)
//
export async function rollupDatabaseInternal(
    db: DB, //
    {validStations, now, accumulators, needValidPurge, stationMeta, historical, wasMissing, retiredAccumulators}: RollupDatabaseArgs
): Promise<RollupResult> {
    //
    //
    const startTime = Date.now();
    const name = db.ognStationName;
    let currentMeta = {};

    if (needValidPurge && !validStations) {
        throw new Error(`${db.ognStationName}: invalid arguments to rollupDatabaseInternal( needValidPurge && ! validStations)`);
    }

    let dbOps: BatchOperation[] = [];
    let h3source = 0;

    // May not have a directory if new station
    try {
        mkdirSync(OUTPUT_PATH + name, {recursive: true});
    } catch (e) {
        console.log(`unable to make output path ${OUTPUT_PATH + name}: ${e}`);
    }

    //
    // Basically we finish our current accumulator into the active buckets for each of the others
    // and then we need to check if we should be moving them to new buckets or not

    // We step through all of the items together and update as one
    const rollupIterators = Object.keys(accumulators)
        .filter((t) => t != 'current') // all but current accumulator
        .map((key: string): any => {
            const r = key as AccumulatorTypeString;
            const par = accumulators[r];
            const accumulatorOutputName = `${OUTPUT_PATH}${name}/${name}.${r}.${accumulators[r]!.file}.arrow`;
            return {
                type: r,
                bucket: par!.bucket,
                file: par!.file,
                meta: {rollups: []},
                stats: {
                    h3added: 0, // missing from dest (ie was added unchanged)
                    h3noChange: 0, // unchanged in dest (no disk op required)
                    h3updated: 0, // updated (changed accumulators or station list)
                    h3emptied: 0, // emptied (no valid stations any longer)
                    h3stationsRemoved: 0, // some station removed
                    h3disk: 0, // read from disk
                    h3extra: 0, // on disk but after the end of the accumulator (included in emptied/updated if station invalid, and noChange otherwise)
                    arrowRecords: 0 // how many records writte to arrow file
                },
                iterator: db.iterator(CoverageHeader.getDbSearchRangeForAccumulator(r, par!.bucket)),
                arrow: new CoverageRecordWriter(db.global ? bufferTypes.global : bufferTypes.station, accumulatorOutputName),
                arrowName: accumulatorOutputName
            };
        });

    // Enrich with the meta data for each accumulator type
    await Promise.allSettled(
        [...rollupIterators, {type: 'current', bucket: accumulators.current.bucket}].map(
            (r) =>
                new Promise<void>((resolve) => {
                    const ch = CoverageHeader.getAccumulatorMeta(r.type, r.bucket);
                    db.get(ch.dbKey())
                        .then((value) => {
                            r.meta = JSON.parse(value?.toString() || '{}');
                            resolve();
                        })
                        .catch(() => {
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
    for await (const [key, value] of db.iterator(CoverageHeader.getDbSearchRangeForAccumulator('current', accumulators.current.bucket))) {
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
                        r.arrow.append(h3kr, currentBr);
                        r.lastCopiedH3p = h3p.h3;
                        r.stats.h3added++;
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
                        r.arrow.append(h3kr, currentBr);
                        r.lastCopiedH3p = h3p.h3;
                        r.stats.h3added++;
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
                        if (changed) {
                            // only count if it isn't a station removal otherwise stats don't add up
                            r.stats.h3updated++;
                        }
                        dbOps.push({type: 'put', key: prefixedh3r, value: updatedBr.buffer()});
                    }
                } else {
                    r.stats.h3noChange++;
                }

                // If we had data then write it out
                if (updatedBr) {
                    r.arrow.append(h3kr, updatedBr);
                }

                // Move us to the next one, allow
                r.n = r.iterator.next();
                r.current = null;
                r.stats.h3disk++;
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

                r.stats.h3disk++;

                let updatedBr = validStations ? br.removeInvalidStations(validStations) : br;

                // Check to see what we need to do with the database
                if (updatedBr != br) {
                    r.stats.h3stationsRemoved++;
                    if (!updatedBr) {
                        dbOps.push({type: 'del', key: prefixedh3r});
                        r.stats.h3emptied++;
                    } else {
                        dbOps.push({type: 'put', key: prefixedh3r, value: updatedBr.buffer()});
                        //                        r.stats.h3updated++;
                    }
                } else {
                    r.stats.h3noChange++;
                }

                if (updatedBr) {
                    r.arrow.append(h3kr, updatedBr);
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
    let arrowRecords = 0;
    for (const r of rollupData) {
        const rType = r.type as AccumulatorTypeString;

        // We are going to write out our accumulators this saves us writing it
        // in a different process and ensures that we always write the correct thing
        const accumulatorName = `${name}/${name}.${rType}.${accumulators[rType]!.file}`;

        // Keep a record of all the rollups in the meta
        // each record
        if (!r.meta.rollups) {
            r.meta.rollups = [];
        }
        r.stats.dbOps = dbOps.length;
        r.stats.h3source = h3source;
        r.meta.rollups.push({source: currentMeta, stats: r.stats, file: accumulatorName});

        if (r.stats.h3source != r.stats.h3added + r.stats.h3updated) {
            console.error("********* stats don't add up ", rType, r.bucket.toString(16), JSON.stringify({m: r.meta, s: r.stats}));
        }

        // If we need to keep previous version then do the renaming before
        // finalizing the new arrow so that we are looking at the old version
        if (historical) {
            try {
                unlinkSync(r.arrowName + '.gz.1');
            } catch (e) {}
            try {
                renameSync(r.arrowName + '.gz', r.arrowName + '.gz.1');
                if (wasMissing?.some((s) => s == rType)) {
                    console.log(`${name}: output file ${r.arrowName} already exists and we are putting incomplete historical data in it, this is potentially a loss of data situation`);
                }
            } catch (e) {}
        }

        // Finalise the arrow table and serialise it to the disk
        arrowRecords += r.stats.arrowRecords = await r.arrow.finalize();

        try {
            const output = (stationMeta ?? {}) as any;
            output.lastOutputFile = now;
            output.rollups = r.meta;
            writeFileSync(OUTPUT_PATH + `${name}/${name}.${rType}.${accumulators[rType]!.file}.json`, JSON.stringify(output, null, 2));
            symlink(`${name}.${rType}.${accumulators[rType]!.file}.json`, OUTPUT_PATH + `${name}/${name}.${rType}.json`);
        } catch (err) {
            console.log('rollup json metadata write failed', err);
        }

        // link it all up for latest
        symlink(`${name}.${r.type}.${accumulators[rType]!.file}.arrow.gz`, OUTPUT_PATH + `${name}/${name}.${r.type}.arrow.gz`);
        if (UNCOMPRESSED_ARROW_FILES) {
            symlink(`${name}.${r.type}.${accumulators[rType]!.file}.arrow`, OUTPUT_PATH + `${name}/${name}.${r.type}.arrow`);
        }
        symlink(`${name}.${r.type}.${accumulators[rType]!.file}.json`, OUTPUT_PATH + `${name}/${name}.${r.type}.json`);
    }

    // Only output if we have some meta
    if (stationMeta) {
        stationMeta.lastOutputFile = now;

        // record when we wrote for the whole station
        try {
            writeFileSync(OUTPUT_PATH + `${name}/${name}.json`, JSON.stringify(stationMeta, null, 2));
        } catch (err) {
            console.log(`${OUTPUT_PATH}${name}/${name}.json stationmeta write error`, err);
        }
    }

    // Make sure we have updated the meta data
    await saveAccumulatorMetadata(db, accumulators);

    // If we have a new accumulator then we need to purge the old meta data records - we
    // have already purged the data above
    dbOps.push({type: 'del', key: CoverageHeader.getAccumulatorMeta('current', accumulators.current.bucket).dbKey()});
    const recordsRemoved = dbOps.filter((o) => o.type === 'del').length - 1;
    //if (db.global) {
    //    if (countToDelete > 0) {
    //        console.log(`rollup: ${db.ognStationName}: current bucket ${[accumulators.current.bucket]} completed, removing ${countToDelete} records`);
    //    }

    // Is this actually beneficial? - feed operations to the database in key type sorted order
    // so it can just process them. Keys should be stored clustered so theoretically this will
    // help with writing but perhaps benchmarking is a good idea
    dbOps = _sortBy(dbOps, ['key', 'type']);

    //
    // Finally execute all the operations on the database
    await db.batch(dbOps);

    const retired = {retiredBuckets: 1};
    if (retiredAccumulators) {
        // And we then need to purge anything that is no longer current
        await Promise.allSettled(
            retiredAccumulators //
                .map((type) => purge(db, new CoverageHeader(0 as StationId, type as AccumulatorTypeString, accumulators[type as AccumulatorTypeString].bucket, '')))
        );
        retired.retiredBuckets += retiredAccumulators.length;
    }

    // Purge everything from the current accumulator, this should just do a compact as we
    // have already deleted in the batch above
    await purge(db, CoverageHeader.getAccumulatorMeta('current', accumulators.current.bucket));

    return {elapsed: Date.now() - startTime, operations: dbOps.length, recordsRemoved, arrowRecords, ...retired};
}

//
// We need to make sure we know what rollups the DB has, and process pending rollup data
// when the process starts. If we don't do this all sorts of weird may happen
// (only used for global but could theoretically be used everywhere)
//
// This relies on the fact that the 'current' accumulator is deleted when it is rolled
// up, so any 'current' accumulators left in the DB are data that has been flushed to
// disk but not rolled up.
//
// There shouldn't be a regular situation where this is more than one accumulator but it is
// possible for example due to startup failures, or perhaps disk space issues
//
// LevelDB is transactional and the rollup should be treated as one transaction
//
export async function rollupDatabaseStartup(
    db: DB, //
    {now, accumulators: expectedAccumulators, stationMeta}: {now: Epoch; accumulators: Accumulators; stationMeta: any}
) {
    const accumulatorsToPurge: Record<string, string> = {}; // purge no other action
    const allAccumulators: Record<string, CoverageHeader> = {};
    const hangingRollups: Record<string, Accumulators & AccumulatorsExtended> = {};
    let datapurged = false;
    let datamerged = false;

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
            accumulatorsToPurge[hr.dbKey()] = hr.accumulator;
            console.log(`${db.ognStationName}: purging entry without metadata ${hr.lockKey}`);
            iterator.seek(CoverageHeader.getAccumulatorEnd(hr.type, hr.bucket));
            iteratorPromise = iterator.next();
            continue;
        }

        const unrefinedMeta = JSON.parse(String(value));
        const meta: DBMetaRecord = unrefinedMeta as DBMetaRecord;

        // accumulator TYPE not configured on this machine - purge with no other action
        if (
            !unrefinedMeta || //
            !(hr.typeName in expectedAccumulators) ||
            !('accumulators' in meta) ||
            (hr.typeName !== 'current' && !meta?.accumulators?.[hr.typeName]?.file)
        ) {
            console.log(`${db.ognStationName}: invalid accumulator or missing metadata for ${hr.dbKey()}, ${String(value)}, file:${meta?.accumulators?.[hr.typeName]?.file}`);
            accumulatorsToPurge[hr.dbKey()] = meta?.accumulators?.[hr.typeName]?.file ?? hr.accumulator;
        } else {
            // currents are the only ones that can start a rollup, and regardless of the metadata
            // we know exactly what bucket they are
            if (hr.typeName === 'current') {
                // We need a current accumulator for the rollup to work
                // this used to be in a different location or totally absent
                // so lets reconstruct it - this code can be removed once
                // all dbs are updated
                if (!meta.accumulators.current) {
                    meta.accumulators.current = {
                        bucket: hr.bucket,
                        file: ''
                    };
                }
                hangingRollups[hr.bucket] = meta.accumulators;
            }
            // Otherwise it's a target and we need to capture them so we know if they are valid
            else {
                // for this we don't need anything more except the filename
                allAccumulators[meta?.accumulators?.[hr.typeName]?.file] = hr;
            }
        }

        // Done with this one lets skip forward
        iterator.seek(CoverageHeader.getAccumulatorEnd(hr.type, hr.bucket));
        iteratorPromise = iterator.next();
    }

    let arrowRecords = 0;

    // This is more interesting, this is a current that could be rolled into one of the other
    // existing accumulators... Current's are a "buffering" of the points until they get
    // merged hence why we can roll them up. this will also cause output files to be generated
    for (const key in hangingRollups) {
        const hangingAccumulators = hangingRollups[key];

        // If we have a current then we roll it up regardless of the rest
        const missingBuckets = Object.keys(hangingAccumulators) //
            .filter((a) => a !== 'current')
            .filter((a) => !(hangingAccumulators[a as AccumulatorTypeString].file in allAccumulators)) as AccumulatorTypeString[];

        const [currentStart, destinationFiles] = describeAccumulators(hangingAccumulators);

        // As long as we have a single bucket then we can proceed
        if (missingBuckets.length < 3) {
            Object.values(hangingAccumulators).forEach((v) => delete v.found);
            const rollupResult = await rollupDatabaseInternal(db, {
                //
                accumulators: hangingAccumulators,
                now,
                needValidPurge: false,
                stationMeta,
                historical: true,
                wasMissing: missingBuckets
            });
            arrowRecords += rollupResult.arrowRecords;
            if (hangingAccumulators.current.bucket !== expectedAccumulators.current.bucket) {
                console.log(
                    `${db.ognStationName}: rolled up hanging current accumulator ${key}(${currentStart}) into ${destinationFiles}: ${JSON.stringify(rollupResult)}` + //
                        (missingBuckets.length ? `${missingBuckets.join(',')} were missing` : '')
                );
            }
            datamerged = true;
        } else {
            console.log(`${db.ognStationName}: DROPPING hanging current accumulator ${key}(${currentStart}) for ${destinationFiles}: ${missingBuckets.join(',')} were missing`);
        }
    }

    const purgePromises: Promise<void>[] = [
        // And we then need to purge anything that is no longer current (ie isn't
        // in expected)
        ...Object.values(allAccumulators)
            .filter((ch) => expectedAccumulators[ch.typeName].bucket != ch.bucket)
            .map((hr) => purge(db, hr)),

        // These are old accumulators we purge them because we aren't sure what else can be done
        ...Object.keys(accumulatorsToPurge).map((key) => {
            datapurged = true;
            return purge(db, new CoverageHeader(key));
        })
    ];
    const purgedAccumulators =
        Object.values(accumulatorsToPurge).join(',') +
        '&' +
        Object.values(allAccumulators)
            .filter((ch) => expectedAccumulators[ch.typeName].bucket != ch.bucket)
            .map((ch: CoverageHeader) => ch.accumulator)
            .join(',');

    // Wait for it to finish
    await Promise.allSettled(purgePromises);

    if (datapurged) {
        console.log(`${db.ognStationName} purged ${purgePromises.length}: ${purgedAccumulators}`);
    }

    return {success: true, datapurged, datamerged, datachanged: datamerged || datapurged, purged: purgePromises.length, arrowRecords};
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

// Clear data from the db
async function purge(db: DB, hr: CoverageHeader) {
    // Now clear and compact
    await db.clear(CoverageHeader.getDbSearchRangeForAccumulator(hr.type, hr.bucket, true));
}
