import {Worker, parentPort, isMainThread, SHARE_ENV} from 'node:worker_threads';

import {getDbThrow, DB, closeAllStationDbs} from './stationcache';

import {EpochMS, StationName, H3LockKey} from '../bin/types';

import {Accumulators} from '../bin/accumulators';
import {StationDetails} from '../bin/stationstatus';
import {Layer, ALL_LAYERS} from '../common/layers';
import {ENABLED_LAYERS} from '../common/config';

import {backupDatabase as backupDatabaseInternal} from './backupdatabase';
import {writeH3ToDB, flushH3DbOps} from './h3storage';

import {RollupDatabaseCommand, RollupResult, RollupDatabaseArgs, rollupDatabaseInternal, rollupDatabaseStartup} from './rollupdatabase';
export type {RollupDatabaseArgs, RollupResult} from './rollupdatabase';

export interface RollupWorkerResult {}

type RollupWorkerCommands =
    | {
          action: 'backup';
          station: StationName;
          now: EpochMS;
          accumulators: Accumulators;
      }
    | {
          action: 'flushBatch';
          now: EpochMS;
          accumulators: Accumulators;
          records: Array<{station: StationName; h3lockkey: H3LockKey; buffer: Uint8Array}>;
      }
    | {
          action: 'abortstartup';
      }
    | {
          action: 'shutdown';
      }
    | {
          action: 'purge';
          station: StationName;
      }
    | {
          station: 'all';
          action: 'abortstartup';
          now: EpochMS;
      }
    | RollupDatabaseCommand
    | {
          action: 'startup';
          station: StationName;
          now: EpochMS;
          accumulators: Accumulators;
          stationMeta?: StationDetails;
      };

interface RollupFlushResult extends RollupWorkerResult {
    databases: number;
}

//
// Record of all the outstanding transactions
const promises: Record<string, {resolve: Function}> = {};

//
// Start the worker thread
const worker = isMainThread ? new Worker(__filename, {env: SHARE_ENV}) : null;

// Send all dirty H3 records to the worker in one message; worker does one getMany+batch per station
export async function flushBatch(
    records: Array<{station: StationName; h3lockkey: H3LockKey; buffer: Uint8Array}>,
    accumulators: Accumulators
): Promise<RollupFlushResult> {
    if (!worker) return {databases: 0};
    const transferables = records.map((r) => r.buffer.buffer as ArrayBuffer);
    return postMessage({action: 'flushBatch', now: Date.now() as EpochMS, records, accumulators}, transferables) as Promise<RollupFlushResult>;
}

export async function shutdownRollupWorker() {
    // and anything else that is running
    await Promise.allSettled(Object.values(promises));
    // Do the sync in the worker thread
    return postMessage({action: 'shutdown'});
}

export async function rollupStartup(station: StationName, accumulators: Accumulators, stationMeta?: StationDetails): Promise<any> {
    // Do the sync in the worker thread
    return postMessage({station, action: 'startup', now: Date.now() as EpochMS, accumulators, stationMeta});
}

export async function rollupAbortStartup() {
    return postMessage({station: 'all', action: 'abortstartup', now: Date.now() as EpochMS});
}
export async function rollupDatabase(station: StationName, commonArgs: RollupDatabaseArgs, layer?: Layer): Promise<RollupResult | void> {
    return postMessage({station, action: 'rollup', commonArgs, now: Date.now() as EpochMS, ...(layer !== undefined ? {layer} : {})});
}

export async function purgeDatabase(station: StationName): Promise<any> {
    if (station === 'global') {
        throw new Error('attempt to purge global');
    }

    return postMessage({station, action: 'purge'});
}

export async function backupDatabase(station: StationName, accumulators: Accumulators): Promise<{rows: number; elapsed: EpochMS}> {
    if (!worker) {
        return {rows: 0, elapsed: 0 as EpochMS};
    }
    // Do the sync in the worker thread
    return postMessage({station, action: 'backup', now: Date.now() as EpochMS, accumulators});
}

export function dumpRollupWorkerStatus() {
    if (!worker || !Object.keys(promises).length) {
        return;
    }
    console.log('worker processing:', Object.keys(promises).join(','));
}

// block startup from continuing - variable in worker thread only
let abortStartup = false;

//
// Inbound in the thread Dispatch to the correct place
if (!isMainThread) {
    parentPort!.on('message', async (task) => {
        let out: any = {success: false};
        try {
            switch (task.action) {
                case 'flushBatch':
                    for (const {station, h3lockkey, buffer} of task.records) {
                        writeH3ToDB(station, h3lockkey, buffer);
                    }
                    out = await flushH3DbOps(task.accumulators);
                    parentPort!.postMessage({action: task.action, ...out, success: true});
                    return;
                case 'shutdown':
                    await closeAllStationDbs();
                    parentPort!.postMessage({action: task.action, ...out, station: task.station, success: true});
                    return;
            }

            let db: DB | undefined = undefined;
            try {
                db = await getDbThrow(task.station);
                switch (task.action) {
                    case 'rollup': {
                        const allLayers = ENABLED_LAYERS ? [...ENABLED_LAYERS] : [...ALL_LAYERS];
                        out = {elapsed: 0, operations: 0, recordsAccumulated: 0, retiredBuckets: 0, arrowRecords: 0, layersWithData: 0};
                        for (const layer of allLayers) {
                            const r = await rollupDatabaseInternal(db, task.commonArgs, layer);
                            out.elapsed += r.elapsed;
                            out.operations += r.operations;
                            out.recordsAccumulated += r.recordsAccumulated;
                            out.retiredBuckets += r.retiredBuckets;
                            out.arrowRecords += r.arrowRecords;
                            if (r.arrowRecords > 0) out.layersWithData++;
                        }
                        break;
                    }
                    case 'abortstartup':
                        out = {success: true};
                        abortStartup = true;
                        break;
                    case 'startup':
                        out = !abortStartup ? await rollupDatabaseStartup(db, task) : {success: false};
                        if (out.success && !abortStartup) {
                            await db.compactRange('0', 'Z');
                            out.datacompacted = true;
                        }
                        break;
                    case 'purge':
                        await purgeDatabaseInternal(db);
                        break;
                    case 'backup':
                        out = await backupDatabaseInternal(db, task);
                        break;
                }
            } catch (e) {
                console.error(e, '->', JSON.stringify(task, null, 4));
            }
            db?.close(); // done allow it to close
            parentPort!.postMessage({action: task.action, station: task.station, ...out});
        } catch (e) {
            console.error('task handling error', task, e);
        }
    });
}

//
// Response from the thread will finish the promise created when the message is called
// and pass the response values to the
else {
    worker!.on('message', (data) => {
        const promiseKey = (data.h3lockkey ?? data.station ?? 'all') + '_' + data.action;
        const resolver = promises[promiseKey]?.resolve;
        delete promises[promiseKey];
        if (resolver) {
            resolver(data);
        } else {
            console.error(`missing resolve function for ${promiseKey}`);
        }
    });
}

async function postMessage(command: RollupWorkerCommands, transferables: ArrayBuffer[] = []): Promise<any> {
    if (!worker) {
        return;
    }
    const key = `${'station' in command && command.station ? command.station : 'all'}_${command.action}`;

    if (promises[key]) {
        console.error(new Error(`duplicate postMessage for ${key}`), command);
        return;
    }

    return new Promise<RollupWorkerResult>((resolve) => {
        promises[key] = {resolve};
        worker.postMessage(command, transferables);
    });
}

async function purgeDatabaseInternal(db: DB) {
    await db.clear();
    return;
}

// Helpers for testing
export const exportedForTest = {
    purgeDatabaseInternal
};
