import LevelUP from 'levelup';
import LevelDOWN from 'leveldown';

import {CoverageRecord, bufferTypes} from './coveragerecord.js';
import {CoverageHeader, accumulatorTypes} from './coverageheader.js';
import {prefixWithZeros} from './prefixwithzeros.js';

import {mapAllCapped} from './mapallcapped.js';

import {flushDirtyH3s} from './h3cache.js';

import _clonedeep from 'lodash.clonedeep';
import _isequal from 'lodash.isequal';
import _map from 'lodash.map';
import _reduce from 'lodash.reduce';
import _sortby from 'lodash.sortby';
import _filter from 'lodash.filter';
import _uniq from 'lodash.uniq';

import {writeFileSync, readFileSync, mkdirSync, unlinkSync, symlinkSync} from 'fs';
import {createWriteStream} from 'fs';
import {PassThrough} from 'stream';
import {Utf8, Uint32, Float32, makeBuilder, makeTable, RecordBatchWriter} from 'apache-arrow/Arrow.node';

import {getDb, closeDb, closeAllDbs } from './stationcache.js'

import zlib from 'zlib';

//
// What accumulators we are operating on these are internal
let currentAccumulator = undefined;
let accumulators = {};

import {
    ROLLUP_PERIOD_MINUTES, //
    MAX_SIMULTANEOUS_ROLLUPS,
    STATION_EXPIRY_TIME_SECS,
    MAX_STATION_DBS,
    DB_PATH,
    OUTPUT_PATH,
    UNCOMPRESSED_ARROW_FILES
} from './config.js';

//
// Information about last rollup
export let rollupStats = {completed: 0, elapsed: 0};

if( ! isMainThread ) {

    parentPort.on('message', (task) => {

        if( task.action == 'rollup' ) {
            const out = await rollupDatabase( getDb( task.db), task );
            parentPort.postMessage( { action: 'rollup', out, task } );
        }

    }
}
        
    

//
// Rotate and Rollup all the data we have
// we do this by iterating through each database looking for things in default
// aggregator (which is always just the raw h3id)
//
// exported for testing
export async function rollupDatabase(db, {stationName, stationMeta = {}, validStations, needValidPurge, current = currentAccumulator, processAccumulators = accumulators}) {
    const now = new Date(),
          nowEpoch = Math.floor(now / 1000);
    const name = stationName;
    let currentMeta = {};

    //
    // Use the correct accumulators as passed in - not the static/global ones
    const allAccumulators = _clonedeep( processAccumulators );
    allAccumulators['current'] = {bucket: currentAccumulator};
    
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
    const rollupIterators = _map(Object.keys(processAccumulators), (r) => {
        return {
            type: r,
            bucket: processAccumulators[r].bucket,
            file: processAccumulators[r].file,
            meta: {rollups: []},
            stats: {
                h3missing: 0,
                h3noChange: 0,
                h3updated: 0,
                h3emptied: 0,
                h3stationsRemoved: 0,
                h3extra: 0
            },
            iterator: db.iterator(CoverageHeader.getDbSearchRangeForAccumulator(r, processAccumulators[r].bucket)),
            arrow: CoverageRecord.initArrow(stationName == 'global' ? bufferTypes.global : bufferTypes.station)
        };
    });

    // Enrich with the meta data for each accumulator type
    await Promise.allSettled(
        _map(
            [...rollupIterators, {type: current[0], bucket: current[1]}],
            (r) =>
                new Promise((resolve) => {
                    const ch = CoverageHeader.getAccumulatorMeta(r.type, r.bucket);
                    db.get(ch.dbKey())
                        .then((value) => {
                            if (r.type == current[0] && r.bucket == current[1]) {
                                currentMeta = JSON.parse(value);
                            } else {
                                r.meta = JSON.parse(String(value) || {});
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
    const rollupData = _map(rollupIterators, (r) => {
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
        let advancePrimary;
        function seth3k(r, t) {
            t && t[0] && r.h3kr.fromDbKey(t[0]);
            return t;
        }

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
                        dbOps.push({type: 'put', key: h3kr.dbKey(), value: Buffer.from(currentBr.buffer())});
                        currentBr.appendToArrow(h3kr, r.arrow);
                        r.lastCopiedH3p = h3p.h3;
                        r.stats.h3missing++;
                    }

                    // We need to cleanup when we are done
                    if (r.n) {
                        r.current = null;
                        r.iterator.end();
                        r.n = null;
                    }
                    continue;
                }

                const h3kr = r.h3kr; //.fromDbKey(prefixedh3r);
                if (h3kr.isMeta) {
                    // skip meta
                    console.log(`unexpected meta information processing ${stationName}, ${r.type} at ${h3kr.dbKey()}, ignoring`);
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
                        dbOps.push({type: 'put', key: h3kr.dbKey(), value: Buffer.from(currentBr.buffer())});
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
                    updatedBr = needValidPurge ? br.removeInvalidStations(validStations) : br;
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
                        dbOps.push({type: 'put', key: prefixedh3r, value: Buffer.from(updatedBr.buffer())});
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

                let updatedBr = needValidPurge ? br.removeInvalidStations(validStations) : br;

                // Check to see what we need to do with the database
                if (updatedBr != br) {
                    r.stats.h3stationsRemoved++;
                    if (!updatedBr) {
                        dbOps.push({type: 'del', key: prefixedh3r});
                        r.stats.h3emptied++;
                    } else {
                        dbOps.push({type: 'put', key: prefixedh3r, value: Buffer.from(updatedBr.buffer())});
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
            r.iterator.end();
        }
    }

    stationMeta.accumulators = [];

    // Write everything out
    for (const r of rollupData) {
        // We are going to write out our accumulators this saves us writing it
        // in a different process and ensures that we always write the correct thing
        const accumulatorName = `${name}/${name}.${r.type}.${processAccumulators[r.type].file}`;

        // Keep a record of all the rollups in the meta
        // each record
        if (!r.meta.rollups) {
            r.meta.rollups = [];
        }
        r.stats.dbOps = dbOps.length;
        r.stats.h3source = h3source;
        r.meta.rollups.push({source: currentMeta, stats: r.stats, file: accumulatorName});

        stationMeta.accumulators[r.type] = r.stats;

        if (r.stats.h3source != r.stats.h3missing + r.stats.h3updated) {
            console.error("********* stats don't add up ", r.type, r.bucket.toString(16), JSON.stringify({m: r.meta, s: r.stats}));
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
        let index = {};
        try {
            const data = readFileSync(OUTPUT_PATH + `${name}/${name}.index.json`, 'utf8');
            index = JSON.parse(data);
        } catch (e) {
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
        symlink(`${name}.${r.type}.${processAccumulators[r.type].file}.arrow.gz`, OUTPUT_PATH + `${name}/${name}.${r.type}.arrow.gz`);
        symlink(`${name}.${r.type}.${processAccumulators[r.type].file}.arrow`, OUTPUT_PATH + `${name}/${name}.${r.type}.arrow`);
        symlink(`${name}.${r.type}.${processAccumulators[r.type].file}.json`, OUTPUT_PATH + `${name}/${name}.${r.type}.json`);
    }

    stationMeta.lastOutputFile = nowEpoch;

    // record when we wrote for the whole station
    try {
        writeFileSync(OUTPUT_PATH + `${name}/${name}.json`, JSON.stringify(stationMeta, null, 2));
    } catch (err) {
        console.log('stationmeta write error', err);
    }

    if (stationName != 'global' && stationName != '!test') {
        delete stationMeta.accumulators; // don't persist this it's not important
    }

    // If we have a new accumulator then we need to purge the old meta data records - we
    // have already purged the data above
    dbOps.push({type: 'del', key: CoverageHeader.getAccumulatorMeta(...current).dbKey()});
    if (stationName == 'global') {
        console.log(`rollup: current bucket ${[...current]} completed, removing`);
    }

    // Write everything out
    for (const r of rollupData) {
        if (accumulators[r.type].bucket != r.bucket) {
            if (stationName == 'global') {
                console.log(`rollup: ${r.type} bucket has changed to ${accumulators[r.type].bucket}, deleting old bucket ${r.bucket} ${r.file}`);
            }
            dbOps.push({type: 'del', key: CoverageHeader.getAccumulatorMeta(r.type, r.bucket).dbKey()});
        }
    }

    // Is this actually beneficial? - feed operations to the database in key type sorted order
    // so it can just process them. Keys should be stored clustered so theoretically this will
    // help with writing but perhaps benchmarking is a good idea
    dbOps = _sortby(dbOps, ['key', 'type']);

    //
    // Finally execute all the operations on the database
    await new Promise((resolve) => {
        db.batch(dbOps, (e) => {
            // log errors
            if (e) console.error('error flushing db operations for station id', name, e);
            resolve();
        });
    });

    // Save the meta data - no big deal having this for stations as well so we won't differentiate
    await saveAccumulatorMetadata(db,allAccumulators);

    // And remove any unwanted accumulators
    await purgeOldAccumulators(db,allAccumulators);

    return {elapsed: Date.now() - now, operations: dbOps.length, accumulators: stationMeta.accumulators};
}

//
// We will add meta data to the database for each of the accumulators
// this makes it easier to check what needs to be done
export async function saveAccumulatorMetadata(db, allAccumulators) {
    const dbkey = CoverageHeader.getAccumulatorMeta(...currentAccumulator).dbKey();
    const now = new Date();

    // make sure we have an up to date header for each accumulator
    for (const type in allAccumulators) {
        const currentHeader = CoverageHeader.getAccumulatorMeta(type, allAccumulators[type].bucket);
        const dbkey = currentHeader.dbKey();
        db
            .get(dbkey)
            .then((value) => {
                const meta = JSON.parse(String(value));
                meta.oldStarts = [...meta?.oldStarts, {start: meta.start, startUtc: meta.startUtc}];
                db.put(dbkey, JSON.stringify({...meta, accumulators: allAccumulators, currentAccumulator: allAccumulators['current']}));
            })
            .catch((e) => {
                db.put(dbkey, JSON.stringify({start: Math.floor(now / 1000), startUtc: now.toISOString(), oldStarts:[], accumulators: allAccumulators, currentAccumulator: allAccumulators['current']}));
            });
    }
}


export async function purgeOldAccumulators(db, allAccumulators) {

    try {
        
        let dbAccumulators = [];
        let n = db.iterator({values: false});
        let y = null;
        while ((y = await n.next())) {
            console.log('.');
            // Identify the record and store the accumulator, we don't care if it's meta data
            // or not (stations currently don't have metadata anyway)
            let hr = new CoverageHeader(y[0]);
            dbAccumulators.push(hr);
            // Seek to the next possible bucket
            n.seek(CoverageHeader.getAccumulatorEnd(hr.type, hr.bucket));
        }
        
        for (const hr of dbAccumulators) {
            if (accumulators[hr.typeName].bucket != hr.bucket) {
                try {
                    await db.clear(CoverageHeader.getDbSearchRangeForAccumulator(hr.type, hr.bucket, true));
                    console.log(`${stationMeta.station}: ${hr.typeName} ${hr.accumulator} purge completed successfully`);
                    
                    await new Promise((resolve) =>
                            db.db.compactRange(
                                CoverageHeader.getAccumulatorBegin(hr.type, hr.bucket, true), //
                                CoverageHeader.getAccumulatorEnd(hr.type, hr.bucket),
                                resolve
                            )
                        )
                    console.log(`${stationMeta.station}: ${hr.typeName} ${hr.accumulator} compact completed successfully`);
                } catch (e) {
                    console.log(`${stationMeta.station}: ${hr.typeName} ${hr.accumulator} purge failed ${e}`);
                }
            } else {
                console.log(`${stationMeta.station}: ${hr.typeName} ${hr.accumulator} retained`);
            }
        }
        closeDb(db);
    }
    catch(e) {
        console.log( `exception ${e} purging accumulators for ${db.station}`);
    }
}
            

        //
// We need to make sure we know what rollups the DB has, and process pending rollup data
// when the process starts. If we don't do this all sorts of weird may happen
// (only used for global but could theoretically be used everywhere)
export async function rollupStartup(db, {globalDb, statusDb, stationDbCache, stations}) {
    let accumulatorsToPurge = {};
    let hangingCurrents = [];

    const now = new Date();

    // Our accumulators
    const {current: expectedCurrentAccumulatorBucket, accumulators: expectedAccumulators} = whatAccumulators(now);

    // We need a current that is basically unique so we don't rollup the wrong thing at the wrong time
    // our goal is to make sure we survive restart without getting same code if it's not the same day...
    // if you run this after an 8 year gap then welcome back ;) and I'm sorry ;)  [it has to fit in 12bits]
    const expectedCurrentAccumulator = ['current', expectedCurrentAccumulatorBucket];

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
            console.log('ignoring weird database format, try bin/dumpdb.js for info', hr.h3);
            console.log(hr, new CoverageRecord(value).toObject());
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
                console.log(`current: hanging accumulator ${hr.accumulator} (${hr.bucket}) [started at: ${meta.startUtc}]`);
            } else {
                console.log(`current: resuming accumulator ${hr.accumulator} (${hr.bucket}) as still valid [stated at: ${meta.startUtc}]`);
            }
        }

        // accumulator not configured on this machine - dump and purge
        else if (!expectedAccumulators[hr.typeName]) {
            accumulatorsToPurge[hr.accumulator] = {accumulator: hr.accumulator, meta: meta, typeName: hr.typeName, t: hr.type, b: hr.bucket, file: hr.file};
        }
        // new bucket for the accumulators - we should dump this
        // and purge as adding new data to it will cause grief
        // note the META will indicate the last active accumulator
        // and we should merge that if we find it
        else if (expectedAccumulators[hr.typeName].bucket != hr.bucket) {
            accumulatorsToPurge[hr.accumulator] = {accumulator: hr.accumulator, meta: meta, typeName: hr.typeName, t: hr.type, b: hr.bucket, file: hr.file};
        } else {
            console.log(`${hr.typeName}: resuming accumulator ${hr.accumulator} (${hr.bucket}) as still valid  [started at: ${meta.startUtc}]`);
        }

        // Done with this one lets skip forward
        iterator.seek(CoverageHeader.getAccumulatorEnd(hr.type, hr.bucket));
        iteratorPromise = iterator.next();
    }

    //
    // We will add meta data to the database for each of the current accumulators
    // this makes it easier to check what needs to be done?
    {
        const dbkey = CoverageHeader.getAccumulatorMeta(...expectedCurrentAccumulator).dbKey();
        db
            .get(dbkey)
            .then((value) => {
                const meta = JSON.parse(String(value));
                meta.oldStarts = [...meta?.oldStarts, {start: meta.start, startUtc: meta.startUtc}];
                meta.start = Math.floor(now / 1000);
                meta.startUtc = now.toISOString();
                db.put(dbkey, JSON.stringify(meta));
            })
            .catch((e) => {
                db.put(
                    dbkey,
                    JSON.stringify({
                        start: Math.floor(now / 1000),
                        startUtc: now.toISOString()
                    })
                );
            });

        // make sure we have an up to date header for each accumulator
        const currentAccumulatorHeader = CoverageHeader.getAccumulatorMeta(...expectedCurrentAccumulator);
        for (const type in expectedAccumulators) {
            const dbkey = CoverageHeader.getAccumulatorMeta(type, expectedAccumulators[type].bucket).dbKey();
            db
                .get(dbkey)
                .then((value) => {
                    const meta = JSON.parse(String(value));
                    db.put(dbkey, JSON.stringify({...meta, currentAccumulator: currentAccumulatorHeader.bucket}));
                })
                .catch((e) => {
                    db.put(dbkey, JSON.stringify({start: Math.floor(now / 1000), startUtc: now.toISOString(), currentAccumulator: currentAccumulatorHeader.bucket}));
                });
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
            let rollupAccumulators = {};
            for (const type of Object.keys(expectedAccumulators)) {
                const ch = CoverageHeader.getAccumulatorMeta(type, meta.accumulators?.[type]?.bucket || -1);
                if (accumulatorsToPurge[ch.accumulator]) {
                    rollupAccumulators[type] = {bucket: ch.bucket, file: accumulatorsToPurge[ch.accumulator].file};
                } else if (expectedAccumulators[type].bucket == ch.bucket) {
                    rollupAccumulators[type] = {bucket: ch.bucket, file: expectedAccumulators[type].file};
                }
            }

            if (Object.keys(rollupAccumulators).length) {
                console.log(` rolling up hanging current accumulator ${hangingHeader.accumulator} into ${JSON.stringify(rollupAccumulators)}`);
                await rollupAll({current: [hangingHeader.type, hangingHeader.bucket], processAccumulators: rollupAccumulators, statusDb, stations, newAccumulatorFiles: true});
            } else {
                console.log(`purging hanging current accumulator ${hangingHeader.accumulator} and associated sub accumulators`);
            }

            // now we clear it
            await db.clear(CoverageHeader.getDbSearchRangeForAccumulator(hangingHeader.type, hangingHeader.bucket, true), (e) => {
                console.log(`${hangingHeader.type}/${hangingHeader.accumulator} purge completed ${e || 'successfully'}`);
            });
        }
    }

    // These are old accumulators we purge them because we aren't sure what else can be done
    for (const key in accumulatorsToPurge) {
        const {t, b, accumulator, typeName} = accumulatorsToPurge[key];
        await db.clear(CoverageHeader.getDbSearchRangeForAccumulator(t, b, true), (e) => {
            console.log(`${typeName}: ${accumulator} purge completed ${e || 'successfully'}`);
        });
    }
}


//
// Dump the meta data for all the stations, we take from our in memory copy
// it will have been primed on start from the db and then we update and flush
// back to the disk
function produceStationFile(stations) {
    // Form a list of hashes
    let statusOutput = _filter(stations, (v) => {
        return v.valid && v.lastPacket;
    });

    // Write this to the stations.json file
    try {
        const output = JSON.stringify(statusOutput);
        writeFileSync(OUTPUT_PATH + 'stations.json', output);
        writeFileSync(OUTPUT_PATH + 'stations.json.gz', zlib.gzipSync(output));
    } catch (err) {
        console.log('stations.json write error', err);
    }

    // Create an arrow version of the stations list - it will be smaller and quicker to
    // load
    try {
        const id = makeBuilder({type: new Uint32()}),
            name = makeBuilder({type: new Utf8()}),
            lat = makeBuilder({type: new Float32()}),
            lng = makeBuilder({type: new Float32()});

        // Output an id sorted list of stations
        for (const station of statusOutput.sort((a, b) => a.id - b.id)) {
            id.append(station.id);
            name.append(station.station);
            lat.append(station.lat);
            lng.append(station.lng);
        }

        // Convert into output file
        const arrow = {
            id: id.finish().toVector(),
            name: name.finish().toVector(),
            lat: lat.finish().toVector(),
            lng: lng.finish().toVector()
        };
        const tableUpdates = makeTable(arrow);

        // And write them out
        if (UNCOMPRESSED_ARROW_FILES) {
            const pt = new PassThrough({objectMode: true});
            const result = pt //
                .pipe(RecordBatchWriter.throughNode())
                .pipe(createWriteStream(OUTPUT_PATH + 'stations.arrow'));
            pt.write(tableUpdates);
            pt.end();
        }
        {
            const pt = new PassThrough({objectMode: true, emitClose: true});
            const result = pt
                .pipe(RecordBatchWriter.throughNode())
                .pipe(zlib.createGzip())
                .pipe(createWriteStream(OUTPUT_PATH + 'stations.arrow.gz'));
            pt.write(tableUpdates);
            pt.end();
        }
    } catch (error) {
        console.log('stations.arrow write error', error);
    }

    // Write this to the stations.json file
    try {
        const output = JSON.stringify(stations);
        writeFileSync(OUTPUT_PATH + 'stations-complete.json', output);
        writeFileSync(OUTPUT_PATH + 'stations-complete.json.gz', zlib.gzipSync(output));
    } catch (err) {
        console.log('stations-complete.json write error', err);
    }
}

function symlink(src, dest) {
    try {
        unlinkSync(dest);
    } catch (e) {}
    try {
        symlinkSync(src, dest, 'file');
    } catch (e) {
        console.log(`error symlinking ${src}.arrow to ${dest}: ${e}`);
    }
}
