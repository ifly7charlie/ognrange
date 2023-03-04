import {mapAllCapped} from './mapallcapped.js';

import _clonedeep from 'lodash.clonedeep';
import _isequal from 'lodash.isequal';
import _map from 'lodash.map';
import _reduce from 'lodash.reduce';
import _sortby from 'lodash.sortby';
import _filter from 'lodash.filter';
import _uniq from 'lodash.uniq';

import {writeFileSync, unlinkSync, symlinkSync} from 'fs';
import {createWriteStream} from 'fs';
import {PassThrough} from 'stream';
import {Utf8, Uint32, Float32, makeBuilder, Table, RecordBatchWriter} from 'apache-arrow/Arrow.node';

import {rollupDatabase, purgeDatabase, rollupStartup, RollupCommonArguments} from './rollupworker';

import {allStationsDetails, updateStationStatus, stationDetails} from './stationstatus';

import {DB} from './stationcache';

import {CoverageHeader, accumulatorTypes} from './coverageheader.js';
import {getCurrentAccumulators} from './accumulators';

import {gzipSync, createGzip} from 'zlib';

import {StationName, StationId} from './types';

import {
    MAX_SIMULTANEOUS_ROLLUPS, //
    STATION_EXPIRY_TIME_SECS,
    OUTPUT_PATH,
    UNCOMPRESSED_ARROW_FILES
} from './config.js';

export interface RollupStats {
    completed: number;
    elapsed: number;
    movedStations?: number;
    validStations?: number;
    invalidStations?: number;
    lastElapsed?: number;
    lastStart?: string;
    last?: {
        sumElapsed: number;
        operations: number;
        databases: number;
        skippedStations: number;
    };
}
//
// Information about last rollup
export let rollupStats: RollupStats = {completed: 0, elapsed: 0};

//
// This iterates through all open databases and rolls them up.
export async function rollupAll({current, processAccumulators}): Promise<RollupStats> {
    //
    // Make sure we have updated validStations
    const nowDate = new Date();
    const nowEpoch = Math.floor(Date.now() / 1000);
    const expiryEpoch = nowEpoch - STATION_EXPIRY_TIME_SECS;
    const validStations = new Set<StationId>();
    let needValidPurge = false;
    let invalidStations = 0;
    rollupStats.movedStations = 0;
    for (const station of allStationsDetails({includeGlobal: false})) {
        const wasStationValid = station.valid;

        if (station.moved) {
            station.moved = false;
            station.valid = false;
            rollupStats.movedStations++;
            console.log(`purging moved station ${station.station}`);
            updateStationStatus(station);
        } else if ((station.lastPacket || station.lastBeacon || nowEpoch) > expiryEpoch) {
            validStations.add(Number(station.id) as StationId);
            station.valid = true;
        } else {
            station.valid = false;
        }

        if (!station.valid && wasStationValid) {
            updateStationStatus(station);
            needValidPurge = true;
            invalidStations++;
            console.log(`purging invalid station ${station.station}`);
        }
    }

    rollupStats.validStations = validStations.size;
    rollupStats.invalidStations = invalidStations;

    let commonArgs: RollupCommonArguments = {
        current,
        processAccumulators,
        needValidPurge,
        stationMeta: undefined
    };

    console.log(`performing rollup and output of ${validStations.size} stations + global, removing ${invalidStations} stations`);

    rollupStats = {
        ...rollupStats, //
        last: {
            sumElapsed: 0, //
            operations: 0,
            databases: 0,
            skippedStations: 0
            //            accumulators: processAccumulators,
            //            current: CoverageHeader.getAccumulatorMeta(...current).accumulator
        }
    };

    // Global is biggest and takes longest
    let promises = [];
    promises.push(
        new Promise<void>(async function (resolve) {
            const r = await rollupDatabase('global' as StationName, {...commonArgs, validStations, stationMeta: {}});
            rollupStats.last.sumElapsed += r.elapsed;
            rollupStats.last.operations += r.operations;
            rollupStats.last.databases++;
            resolve();
        })
    );

    // each of the stations, capped at 20% of db cache (or 30 tasks) to reduce risk of purging the whole cache
    // mapAllCapped will not return till all have completed, but this doesn't block the processing
    // of the global db or other actions.
    // it is worth running them in parallel as there is a lot of IO which would block
    promises.push(
        mapAllCapped(
            allStationsDetails(),
            async function (stationMeta) {
                const station = stationMeta.station;

                // If there has been no packets since the last output then we don't gain anything by scanning the whole db and processing it
                if (stationMeta.outputEpoch && !stationMeta.moved && (stationMeta.lastPacket || 0) < stationMeta.outputEpoch) {
                    rollupStats.last.skippedStations++;
                    return;
                }

                // If a station is not valid we are clearing the data from it from the registers
                if (needValidPurge && !validStations.has(stationMeta.id)) {
                    // empty the database... we could delete it but this is very simple and should be good enough
                    console.log(`clearing database for ${station} as it is not valid`);
                    await purgeDatabase(station);
                    rollupStats.last.databases++;
                    return;
                }

                const r = await rollupDatabase(station, {...commonArgs, stationMeta});
                rollupStats.last.sumElapsed += r.elapsed;
                rollupStats.last.operations += r.operations;
                rollupStats.last.databases++;

                // Details about when we wrote, also contains information about the station if
                // it's not global
                stationMeta.outputDate = nowDate.toISOString();
                stationMeta.outputEpoch = nowEpoch;
            },
            MAX_SIMULTANEOUS_ROLLUPS
        )
    );

    // And the global json
    produceStationFile();

    // Wait for all to be done
    await Promise.allSettled(promises);

    // Report stats on the rollup
    rollupStats.lastElapsed = Date.now() - nowDate.valueOf();
    rollupStats.elapsed += rollupStats.lastElapsed;
    rollupStats.completed++;
    rollupStats.lastStart = nowDate.toISOString();
    console.log(`rollup of ${current.join('/')} completed`, JSON.stringify(rollupStats));

    return rollupStats;
}

export async function rollupStartupAll() {
    const current = getCurrentAccumulators();
    const common = {current: current.currentAccumulator, processAccumulators: current.accumulators};

    // Global is biggest and takes longest
    let promises = [];
    promises.push(
        new Promise<void>(async function (resolve) {
            await rollupStartup('global' as StationName, common, {});
            resolve();
        })
    );

    // each of the stations, capped at 20% of db cache (or 30 tasks) to reduce risk of purging the whole cache
    // mapAllCapped will not return till all have completed, but this doesn't block the processing
    // of the global db or other actions.
    // it is worth running them in parallel as there is a lot of IO which would block
    promises.push(
        mapAllCapped(
            allStationsDetails(),
            async (stationMeta) => {
                const station = stationMeta.station;
                await rollupStartup(station, common, stationMeta);
            },
            MAX_SIMULTANEOUS_ROLLUPS
        )
    );
    await Promise.allSettled(promises);
}

function Uint8FromObject(o: Record<any, any>): Uint8Array {
    return Buffer.from(JSON.stringify(o));
}

export async function saveAccumulatorMetadata(db: DB): Promise<void> {
    const {currentAccumulator, accumulators: allAccumulators} = getCurrentAccumulators();

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
    for (const type in allAccumulators) {
        const currentHeader = CoverageHeader.getAccumulatorMeta(type, allAccumulators[type].bucket);
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

//
// Dump the meta data for all the stations, we take from our in memory copy
// it will have been primed on start from the db and then we update and flush
// back to the disk
function produceStationFile() {
    const stationDetailsArray = allStationsDetails();
    // Form a list of hashes
    let statusOutput = _filter(stationDetailsArray, (v) => {
        return v.valid && v.lastPacket;
    });

    // Write this to the stations.json file
    try {
        const output = JSON.stringify(statusOutput);
        writeFileSync(OUTPUT_PATH + 'stations.json', output);
        writeFileSync(OUTPUT_PATH + 'stations.json.gz', gzipSync(output));
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
        const tableUpdates = new Table(arrow);

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
                .pipe(createGzip())
                .pipe(createWriteStream(OUTPUT_PATH + 'stations.arrow.gz'));
            pt.write(tableUpdates);
            pt.end();
        }
    } catch (error) {
        console.log('stations.arrow write error', error);
    }

    // Write this to the stations.json file
    try {
        const output = JSON.stringify(stationDetailsArray);
        writeFileSync(OUTPUT_PATH + 'stations-complete.json', output);
        writeFileSync(OUTPUT_PATH + 'stations-complete.json.gz', gzipSync(output));
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
