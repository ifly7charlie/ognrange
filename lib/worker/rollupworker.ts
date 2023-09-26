import {Worker, parentPort, isMainThread, SHARE_ENV} from 'node:worker_threads';

import {getDbThrow, DB, closeAllStationDbs} from './stationcache';

import {EpochMS, StationName, H3LockKey} from '../bin/types';

import {Accumulators} from '../bin/accumulators';
import {StationDetails} from '../bin/stationstatus';

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
          action: 'flushPending';
          now: EpochMS;
          accumulators: Accumulators;
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

// So we can wait for all of them
const h3promises: Promise<void>[] = [];
//
// Start the worker thread
const worker = isMainThread ? new Worker(__filename, {env: SHARE_ENV}) : null;

// Update the disk version of the H3 by transferring the buffer record to
// the worker thread, the buffer is NO LONGER VALID
export async function flushH3(station: StationName, h3lockkey: H3LockKey, buffer: Uint8Array) {
    if (!worker) {
        return;
    }
    // special post as it needs to transfer the buffer rather than copy it
    const donePromise = new Promise<void>((resolve) => {
        promises[h3lockkey + '_flushH3'] = {resolve};
        worker.postMessage({action: 'flushH3', now: Date.now(), station, h3lockkey, buffer}, [buffer.buffer]);
    });

    // Keep copy so we can wait for it and also return it so the caller can wait
    h3promises.push(donePromise);
    return donePromise;
}

// Send all the recently flushed operations to the disk, called after h3cache flush is done and
// before a rollup can start
export async function flushPendingH3s(accumulators: Accumulators): Promise<RollupFlushResult> {
    if (!worker) {
        return {databases: 0};
    }
    // Make sure all the h3promises are settled, then reset that - we don't
    // care what happens just want to make sure we don't flush too early
    await Promise.allSettled(h3promises);
    h3promises.length = 0;

    return postMessage({action: 'flushPending', now: Date.now() as EpochMS, accumulators}) as Promise<RollupFlushResult>;
}

export async function shutdownRollupWorker() {
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
export async function rollupDatabase(station: StationName, commonArgs: RollupDatabaseArgs): Promise<RollupResult | void> {
    // Safety check
    if (h3promises.length) {
        console.error(`rollupDatabase ${station} requested but h3s pending to disk`);
        throw new Error('sequence error - pendingH3s not flushed before rollup');
    }
    return postMessage({station, action: 'rollup', commonArgs, now: Date.now() as EpochMS});
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
                case 'flushH3':
                    await writeH3ToDB(task.station, task.h3lockkey, task.buffer);
                    parentPort!.postMessage({action: task.action, h3lockkey: task.h3lockkey, success: true});
                    return;
                case 'flushPending':
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
                    case 'rollup':
                        out = await rollupDatabaseInternal(db, task.commonArgs);
                        break;
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
                        await purgeDatabaseInternal(db, 'purge');
                        break;
                    case 'backup':
                        out = await backupDatabaseInternal(db, task);
                        break;
                }
                await db!.close();
            } catch (e) {
                console.error(e, '->', JSON.stringify(task, null, 4));
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
        const promiseKey = (data.h3lockkey ?? data.station ?? 'all') + '_' + data.action;
        const resolver = promises[promiseKey]?.resolve;
        delete promises[promiseKey];
        if (resolver) {
            resolver(data);
        } else {
            console.error(`missing resolve function for ${promiseKey}/`);
        }
    });
}

async function postMessage(command: RollupWorkerCommands): Promise<any> {
    if (!worker) {
        return;
    }
    return new Promise<RollupWorkerResult>((resolve) => {
        promises[`${'station' in command && command.station ? command.station : 'all'}_${command.action}`] = {resolve};
        worker.postMessage(command);
    });
}

async function purgeDatabaseInternal(db: DB, reason: string) {
    // empty the database... we could delete it but this is very simple and should be good enough
    console.log(`clearing database for ${db.ognStationName} because ${reason}`);
    await db.clear();
    return;
}

// Helpers for testing
export const exportedForTest = {
    purgeDatabaseInternal
};
