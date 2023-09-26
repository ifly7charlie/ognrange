import {mapAllCapped} from './mapallcapped';

import {cloneDeep as _clonedeep, isEqual as _isequal, map as _map, reduce as _reduce, sortBy as _sortBy, filter as _filter, uniq as _uniq} from 'lodash';

import {writeFileSync, unlinkSync, symlinkSync} from 'fs';
import {createWriteStream} from 'fs';
import {PassThrough} from 'stream';
import {Utf8, Uint32, Float32, makeBuilder, Table, RecordBatchWriter} from 'apache-arrow/Arrow.node';

import {rollupDatabase, purgeDatabase, rollupStartup, RollupDatabaseArgs} from '../worker/rollupworker';

import {allStationsDetails, updateStationStatus} from './stationstatus';

import {Accumulators, getCurrentAccumulators, AccumulatorTypeString, describeAccumulators} from './accumulators';

import {gzipSync, createGzip} from 'zlib';

import {Epoch, StationId} from './types';

import {
    MAX_SIMULTANEOUS_ROLLUPS, //
    STATION_EXPIRY_TIME_SECS,
    OUTPUT_PATH,
    UNCOMPRESSED_ARROW_FILES
} from '../common/config';

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
        retiredBuckets: number;
        recordsRemoved: number;
        databases: number;
        skippedStations: number;
        arrowRecords: number;
    };
}

// When did we load this module (ie start the process)
// Used to ensure we don't purge immediately after restart
const startupTime = Math.trunc(Date.now() / 1000) as Epoch;

//
// Information about last rollup
export let rollupStats: RollupStats = {completed: 0, elapsed: 0};

//
// This iterates through all open databases and rolls them up.
export async function rollupAll(accumulators: Accumulators, nextAccumulators?: Accumulators): Promise<RollupStats> {
    //
    // Make sure we have updated validStations
    const nowDate = new Date();
    const nowEpoch = Math.floor(Date.now() / 1000) as Epoch;
    const expiryEpoch = Math.max(nowEpoch - STATION_EXPIRY_TIME_SECS, startupTime - STATION_EXPIRY_TIME_SECS);
    console.log(`Rollup starting, station expiry time ${new Date(expiryEpoch * 1000).toISOString()}`);

    const validStations = new Set<StationId>();
    let needValidPurge = false;
    let invalidStations = 0;
    rollupStats.movedStations = 0;
    for (const station of allStationsDetails({includeGlobal: false})) {
        const wasStationValid = station.valid;

        // Any of these, if we don't have a timestamp yet assume it is valid
        const stationValidityTimestamp = station.lastPacket || station.lastBeacon || nowEpoch;

        if (station.moved) {
            station.moved = false;
            station.valid = false;
            rollupStats.movedStations++;
        } else if (stationValidityTimestamp > expiryEpoch) {
            validStations.add(Number(station.id) as StationId);
            station.valid = true;
        } else {
            station.valid = false;
        }

        if (!station.valid && wasStationValid) {
            updateStationStatus(station);
            needValidPurge = true;
            invalidStations++;
            console.log(`purging ${station.moved ? 'moved' : 'expired'} station ${station.station} last timestamp ${new Date(stationValidityTimestamp * 1000).toISOString()}`);
        }
    }

    rollupStats.validStations = validStations.size;
    rollupStats.invalidStations = invalidStations;

    // See if any are no longer valid (current is never valid twice so it'll go)
    const retiredAccumulators = (
        nextAccumulators
            ? Object.keys(accumulators) //
                  .filter((a) => a !== 'current')
                  .filter((a) => accumulators[a as AccumulatorTypeString].bucket != nextAccumulators[a as AccumulatorTypeString].bucket)
            : []
    ) as AccumulatorTypeString[];

    // Figure out what the cutoff for changes is for reviewing the datases
    // it's the oldest time that the accumulator isn't valid for (including current)
    const updateCutOff = Math.min(
        ...(nextAccumulators
            ? Object.keys(accumulators) //
                  .filter((a) => accumulators[a as AccumulatorTypeString].bucket != nextAccumulators[a as AccumulatorTypeString].bucket)
                  .map((a) => accumulators[a as AccumulatorTypeString].effectiveStart ?? 0)
            : [0])
    ) as Epoch;

    let commonArgs: RollupDatabaseArgs = {
        now: nowEpoch,
        accumulators,
        validStations,
        needValidPurge,
        stationMeta: undefined,
        retiredAccumulators
    };

    console.log(`performing rollup and output of ${validStations.size} stations + global, removing ${invalidStations} stations`);
    if (invalidStations / validStations.size > 0.02 /*%*/) {
        console.error(`there are too many invalid stations, not purging any`);
        commonArgs.needValidPurge = false;
    }

    if (retiredAccumulators.length) {
        console.log(`${retiredAccumulators.join(',')} have changed accumulator, removing from all databases`);
    }

    rollupStats = {
        ...rollupStats, //
        last: {
            sumElapsed: 0, //
            operations: 0,
            retiredBuckets: 0,
            recordsRemoved: 0,
            databases: 0,
            skippedStations: 0,
            arrowRecords: 0
            //            accumulators: processAccumulators,
            //            current: CoverageHeader.getAccumulatorMeta(...current).accumulator
        }
    };

    // each of the stations, capped at 20% of db cache (or 30 tasks) to reduce risk of purging the whole cache
    // mapAllCapped will not return till all have completed, but this doesn't block the processing
    // of the global db or other actions.
    // it is worth running them in parallel as there is a lot of IO which would block
    let promise = mapAllCapped(
        'rollup',
        allStationsDetails({includeGlobal: true}),
        async function (stationMeta) {
            const station = stationMeta.station;

            // If there has been no packets since the last output then we don't gain anything by scanning the whole db and processing it
            if (station !== 'global' && !retiredAccumulators.length && stationMeta.outputEpoch && !stationMeta.moved && (stationMeta.lastPacket || 0) < (updateCutOff || stationMeta.outputEpoch)) {
                rollupStats.last!.skippedStations++;
                return;
            }

            // If a station is not valid we are clearing the data from it from the registers
            if (stationMeta.station != 'global' && needValidPurge && !validStations.has(stationMeta.id)) {
                // empty the database... we could delete it but this is very simple and should be good enough
                console.log(`clearing database for ${station} as it is not valid`);
                await purgeDatabase(station);
                rollupStats.last!.databases++;
            } else {
                // Or if it is valid we roll it up
                const r = await rollupDatabase(station, {...commonArgs, stationMeta});
                if (r) {
                    rollupStats.last!.sumElapsed += r.elapsed;
                    rollupStats.last!.operations += r.operations;
                    rollupStats.last!.retiredBuckets += r.retiredBuckets;
                    rollupStats.last!.recordsRemoved += r.recordsRemoved;
                    rollupStats.last!.arrowRecords += r.arrowRecords;
                    rollupStats.last!.databases++;
                }
            }

            // Details about when we wrote, also contains information about the station if
            // it's not global
            stationMeta.outputDate = nowDate.toISOString();
            stationMeta.outputEpoch = nowEpoch;
        },
        MAX_SIMULTANEOUS_ROLLUPS
    );

    // And the global json
    produceStationFile();

    // Wait for all to be done
    await promise;

    // Report stats on the rollup
    rollupStats.lastElapsed = Date.now() - nowDate.valueOf();
    rollupStats.elapsed += rollupStats.lastElapsed;
    rollupStats.completed++;
    rollupStats.lastStart = nowDate.toISOString();
    console.log(`rollup of ${describeAccumulators(accumulators).join(' ')} completed: ${JSON.stringify(rollupStats)}`);

    return rollupStats;
}

export async function rollupStartupAll() {
    const now = Date.now();
    const current = getCurrentAccumulators() || superThrow('no accumulators on startup');

    const allStations = allStationsDetails({includeGlobal: true});

    const [currentStart, destinationFiles] = describeAccumulators(current);

    const startupStats = {
        datapurged: 0,
        datamerged: 0,
        datachanged: 0,
        databases: 0,
        arrowRecords: 0,
        elapsed: 0
    };

    console.log(`performing startup rollup and output of ${allStations.length} stations (including global) resuming ${currentStart},${destinationFiles}`);

    // each of the stations, capped at 20% of db cache (or 30 tasks) to reduce risk of purging the whole cache
    // mapAllCapped will not return till all have completed, but this doesn't block the processing
    // of the global db or other actions.
    // it is worth running them in parallel as there is a lot of IO which would block
    await mapAllCapped(
        'startup',
        allStations,
        async (stationMeta) => {
            const station = stationMeta.station;
            const r = await rollupStartup(station, current, stationMeta);
            if (r) {
                startupStats.datachanged += r.datachanged ? 1 : 0;
                startupStats.datamerged += r.datamerged ? 1 : 0;
                startupStats.datapurged += r.datapurged ? 1 : 0;
                startupStats.arrowRecords += r.arrowRecords;
                startupStats.databases++;
            }
        },
        MAX_SIMULTANEOUS_ROLLUPS
    );

    startupStats.elapsed = Date.now() - now;
    console.log(`done initial rollup ${JSON.stringify(startupStats)}`);
}

//
// Dump the meta data for all the stations, we take from our in memory copy
// it will have been primed on start from the db and then we update and flush
// back to the disk
function produceStationFile() {
    const stationDetailsArray = allStationsDetails();
    // Form a list of hashes
    let statusOutput = stationDetailsArray.filter((v) => {
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
            pt.pipe(RecordBatchWriter.throughNode()) //
                .pipe(createWriteStream(OUTPUT_PATH + 'stations.arrow'));
            pt.write(tableUpdates);
            pt.end();
        }
        {
            const pt = new PassThrough({objectMode: true, emitClose: true});
            pt.pipe(RecordBatchWriter.throughNode())
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

export function symlink(src: string, dest: string) {
    try {
        unlinkSync(dest);
    } catch (e) {}
    try {
        symlinkSync(src, dest, 'file');
    } catch (e) {
        console.log(`error symlinking ${src}.arrow to ${dest}: ${e}`);
    }
}

function superThrow(t: string): never {
    throw new Error(t);
}
