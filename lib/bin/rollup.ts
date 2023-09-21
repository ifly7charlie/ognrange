import {mapAllCapped} from './mapallcapped';

import {cloneDeep as _clonedeep, isEqual as _isequal, map as _map, reduce as _reduce, sortBy as _sortBy, filter as _filter, uniq as _uniq} from 'lodash';

import {writeFileSync, unlinkSync, symlinkSync} from 'fs';
import {createWriteStream} from 'fs';
import {PassThrough} from 'stream';
import {Utf8, Uint32, Float32, makeBuilder, Table, RecordBatchWriter} from 'apache-arrow/Arrow.node';

import {rollupDatabase, purgeDatabase, rollupStartup, RollupDatabaseArgs} from './rollupworker';

import {allStationsDetails, updateStationStatus} from './stationstatus';

import {Accumulators, getCurrentAccumulators} from './accumulators';

import {gzipSync, createGzip} from 'zlib';

import {Epoch, StationName, StationId} from './types';

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
        databases: number;
        skippedStations: number;
    };
}
//
// Information about last rollup
export let rollupStats: RollupStats = {completed: 0, elapsed: 0};

//
// This iterates through all open databases and rolls them up.
export async function rollupAll(accumulators: Accumulators): Promise<RollupStats> {
    //
    // Make sure we have updated validStations
    const nowDate = new Date();
    const nowEpoch = Math.floor(Date.now() / 1000) as Epoch;
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

    let commonArgs: RollupDatabaseArgs = {
        now: nowEpoch,
        accumulators,
        validStations,
        needValidPurge,
        stationMeta: undefined
    };

    console.log(`performing rollup and output of ${validStations.size} stations + global, removing ${invalidStations} stations`);
    if (invalidStations / validStations.size > 0.02 /*%*/) {
        console.error(`there are too many invalid stations, not purging any`);
        commonArgs.needValidPurge = false;
    }

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
            if (stationMeta.outputEpoch && !stationMeta.moved && (stationMeta.lastPacket || 0) < stationMeta.outputEpoch) {
                rollupStats.last!.skippedStations++;
                return;
            }

            // If a station is not valid we are clearing the data from it from the registers
            if (stationMeta.station != 'global' && needValidPurge && !validStations.has(stationMeta.id)) {
                // empty the database... we could delete it but this is very simple and should be good enough
                console.log(`clearing database for ${station} as it is not valid`);
                await purgeDatabase(station);
                rollupStats.last!.databases++;
                return;
            }

            const r = await rollupDatabase(station, {...commonArgs, stationMeta});
            if (r) {
                rollupStats.last!.sumElapsed += r.elapsed;
                rollupStats.last!.operations += r.operations;
                rollupStats.last!.databases++;
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
    console.log(`rollup of ${accumulators.current.bucket} completed: ${JSON.stringify(rollupStats)}`);

    return rollupStats;
}

export async function rollupStartupAll() {
    const current = getCurrentAccumulators() || superThrow('no accumulators on startup');

    const allStations = allStationsDetails();
    console.log(`performing startup rollup and output of ${allStations.length} stations + global stations`);

    // Global is biggest and takes longest
    let promises = [];
    promises.push(
        new Promise<void>(async function (resolve) {
            await rollupStartup('global' as StationName, current);
            resolve();
        })
    );

    // each of the stations, capped at 20% of db cache (or 30 tasks) to reduce risk of purging the whole cache
    // mapAllCapped will not return till all have completed, but this doesn't block the processing
    // of the global db or other actions.
    // it is worth running them in parallel as there is a lot of IO which would block
    promises.push(
        mapAllCapped(
            'startup',
            allStations,
            async (stationMeta) => {
                const station = stationMeta.station;
                await rollupStartup(station, current, stationMeta);
            },
            MAX_SIMULTANEOUS_ROLLUPS
        )
    );
    await Promise.allSettled(promises);
    console.log(`done initial rollup`);
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
