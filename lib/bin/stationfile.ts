import {writeFileSync, unlinkSync, symlinkSync} from 'fs';
import {writeFile, rename} from 'fs/promises';
import {createWriteStream} from 'fs';
import {PassThrough} from 'stream';
import {Bool, Utf8, Uint8, Uint32, Float32, makeBuilder, Table, RecordBatchWriter} from 'apache-arrow/Arrow.node';
import {gzipSync, createGzip} from 'zlib';

import {OUTPUT_PATH, UNCOMPRESSED_ARROW_FILES} from '../common/config';

import type {Accumulators, AccumulatorTypeString} from './accumulators';
import {allStationsDetails} from './stationstatus';

//
// Dump the meta data for all the stations, we take from our in memory copy
// it will have been primed on start from the db and then we update and flush
// back to the disk
export async function produceStationFile(accumulators: Accumulators) {
    const stationDetailsArray = allStationsDetails();
    // Form a list of hashes
    let statusOutput = stationDetailsArray.filter((v) => {
        return !!v.lastPacket;
    });

    // Write this to the stations.json file
    try {
        const output = JSON.stringify(statusOutput);

        await Promise.allSettled([
            writeFile(OUTPUT_PATH + 'stations.json', output), //
            writeFile(OUTPUT_PATH + 'stations.json.gz', gzipSync(output))
        ]);
    } catch (err) {
        console.log('stations.json write error', err);
    }

    // Create an arrow version of the stations list - it will be smaller and quicker to
    // load
    try {
        const id = makeBuilder({type: new Uint32()}),
            name = makeBuilder({type: new Utf8()}),
            lat = makeBuilder({type: new Float32()}),
            lng = makeBuilder({type: new Float32()}),
            valid = makeBuilder({type: new Bool()}),
            lastPacket = makeBuilder({type: new Uint32()}),
            layerMask = makeBuilder({type: new Uint8()});

        // Output an id sorted list of stations
        for (const station of statusOutput.sort((a, b) => a.id - b.id)) {
            id.append(station.id);
            name.append(station.station);
            lat.append(station.lat);
            lng.append(station.lng);
            valid.append(station.valid);
            lastPacket.append(station.lastPacket);
            layerMask.append(station.layerMask ?? 0);
        }

        // Convert into output file
        const arrow = {
            id: id.finish().toVector(),
            name: name.finish().toVector(),
            lat: lat.finish().toVector(),
            lng: lng.finish().toVector(),
            valid: valid.finish().toVector(),
            lastPacket: lastPacket.finish().toVector(),
            layerMask: layerMask.finish().toVector()
        };
        const tableUpdates = new Table(arrow);

        // Helper to make all the stations file symlinks we need, this lets us keep history
        // for the arrow files, h3 output is the END of the period so the file for the last
        // day in the period is equivalent
        const symlinkAll = (compress: boolean) => {
            const sourceFile = `stations.day.${accumulators.day.file}.arrow${compress ? '.gz' : ''}`;
            Object.keys(accumulators)
                .filter((a) => a !== 'current' && a !== 'day')
                .forEach((a) => {
                    symlink(sourceFile, OUTPUT_PATH + `stations/stations.${a}.${accumulators[a as AccumulatorTypeString].file}.arrow${compress ? '.gz' : ''}`);
                    symlink(sourceFile, OUTPUT_PATH + `stations/stations.${a}.arrow${compress ? '.gz' : ''}`);
                });
            symlink(sourceFile, OUTPUT_PATH + `stations/stations.day.arrow${compress ? '.gz' : ''}`);
            symlink('stations/' + sourceFile, OUTPUT_PATH + `stations.arrow${compress ? '.gz' : ''}`); //legacy
        };

        // And write them out
        if (UNCOMPRESSED_ARROW_FILES) {
            const pt = new PassThrough({objectMode: true});
            const fileName = OUTPUT_PATH + `stations/stations.day.${accumulators.day.file}`;
            const ws = createWriteStream(fileName + `.working.arrow`);
            const finished = new Promise((resolve) => {
                ws.on('close', resolve);
            });

            pt.pipe(RecordBatchWriter.throughNode()).pipe(ws);
            pt.write(tableUpdates);
            pt.end();

            // Wait for completion and then rename from working file to actual name
            await finished;
            try {
                await rename(fileName + '.working.arrow', fileName + '.arrow');
            } catch (err) {
                if (err) {
                    console.error(`error renaming ${fileName}.working.arrow to ${fileName}.arrow:${err}`);
                }
            }
            console.log(`output uncompressed station file ${fileName}.arrow`);
            symlinkAll(false);
        }
        {
            const pt = new PassThrough({objectMode: true, emitClose: true});
            const fileName = OUTPUT_PATH + `stations/stations.day.${accumulators.day.file}`;
            const ws = createWriteStream(fileName + `.working.arrow.gz`);
            const finished = new Promise((resolve) => {
                ws.on('close', resolve);
            });
            pt.pipe(RecordBatchWriter.throughNode()).pipe(createGzip()).pipe(ws);
            pt.write(tableUpdates);
            pt.end();

            // Wait for completion and then rename from working file to actual name
            await finished;
            try {
                await rename(fileName + '.working.arrow.gz', fileName + '.arrow.gz');
            } catch (err) {
                if (err) {
                    console.error(`error renaming ${fileName}.working.arrow.gz to ${fileName}.arrow.gz:${err}`);
                }
            }
            console.log(`output compressed station file ${fileName}.arrow.gz`);
            symlinkAll(true);
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
