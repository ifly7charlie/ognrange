import {prefixWithZeros} from '../common/prefixwithzeros';

import {cloneDeep as _clonedeep, isEqual as _isequal, reduce as _reduce} from 'lodash';

import {
    ROLLUP_PERIOD_MINUTES, //
    DO_BACKUPS
} from '../common/config';

import {flushDirtyH3s} from './h3cache';
import {rollupAll} from './rollup';
import {backupDatabases} from './backupdatabases';

import type {Epoch} from './types';

export type {AccumulatorTypeString, AccumulatorBucket} from './coverageheader';
import {AccumulatorType, AccumulatorTypeString, AccumulatorBucket, formAccumulator} from './coverageheader';

//
export type Accumulators = {
    [key in AccumulatorTypeString]: {
        bucket: AccumulatorBucket;
        file: string;
        effectiveStart?: Epoch;
    };
};

let accumulators: Accumulators;

//
// Helper for getting current accumulator used as  ...getAccumulator() in
// calls to CoverageHeader

export function getAccumulator(): AccumulatorBucket {
    if (!accumulators) {
        throw new Error('getAccumulator() called before an accumulator is ready');
    }
    return accumulators.current.bucket;
}

export function getCurrentAccumulators(): undefined | Accumulators {
    return accumulators;
}

// Calculate the bucket and short circuit if it's not changed - we need to change
// accumulator every time we dump but we need a unique name for it...
//
// We need accumulator buckets that are basically unique so we don't rollup the wrong thing at the wrong time
// our goal is to make sure we survive restart without getting same code if it's not the same day...
//
// Same applies to the buckets we roll into, if it's unique then we can probably resume into it and still
// output a reasonable file. If it was simply 'day of month' then a one mount outage would break everything
//
// if you run this after a month gap then welcome back ;) and I'm sorry ;)  [to fit in 12bits]
//
// in this situation if it happens to be identical bucket it will resume into current month
// otherwise it will try and rollup into the buckets that existed at the time (which are valid
/// for several years) or discard the data.
//
// this takes effect immediately so all new packets will move to the new accumulator
// rolling over is minimum 12 minutes...
export function whatAccumulators(now: Date): Accumulators {
    const rolloverperiod = Math.floor((now.getUTCHours() * 60 + now.getUTCMinutes()) / ROLLUP_PERIOD_MINUTES);
    const newAccumulatorBucket = ((now.getUTCDate() & 0x1f) << 7) | (rolloverperiod & 0x7f);
    const n = {
        d: prefixWithZeros(2, String(now.getUTCDate())),
        m: prefixWithZeros(2, String(now.getUTCMonth() + 1)),
        y: now.getUTCFullYear()
    };
    return {
        current: {
            bucket: newAccumulatorBucket as AccumulatorBucket,
            file: '',
            effectiveStart: Math.trunc(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()) / 1000 + rolloverperiod * ROLLUP_PERIOD_MINUTES * 60) as Epoch
        },
        day: {
            bucket: (((now.getUTCFullYear() & 0x07) << 9) | ((now.getUTCMonth() & 0x0f) << 5) | (now.getUTCDate() & 0x1f)) as AccumulatorBucket, //
            file: `${n.y}-${n.m}-${n.d}`,
            effectiveStart: Math.trunc(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()) / 1000) as Epoch
        },
        month: {
            bucket: (((now.getUTCFullYear() & 0xff) << 4) | (now.getUTCMonth() & 0x0f)) as AccumulatorBucket, //
            file: `${n.y}-${n.m}`,
            effectiveStart: Math.trunc(Date.UTC(now.getUTCFullYear(), now.getUTCMonth()) / 1000) as Epoch
        },
        year: {
            bucket: now.getUTCFullYear() as AccumulatorBucket, //
            file: `${n.y}`,
            effectiveStart: Math.trunc(Date.UTC(now.getUTCFullYear(), 0) / 1000) as Epoch
        }
    };
}

export function initialiseAccumulators(): Accumulators {
    return (accumulators = whatAccumulators(new Date()));
}

export function describeAccumulators(a: Accumulators): [string, string] {
    const currentStart = new Date((a.current?.effectiveStart ?? 0) * 1000);
    const currentText = currentStart //
        ? prefixWithZeros(2, currentStart.getUTCHours().toString()) + ':' + prefixWithZeros(2, currentStart.getUTCMinutes().toString())
        : formAccumulator(AccumulatorType.current, a.current.bucket);

    const destinationFiles = Object.values(a)
        .map((a) => a.file)
        .filter((a) => !!a)
        .join(',');

    return [currentText, destinationFiles];
}

export async function updateAndProcessAccumulators() {
    const now = new Date();

    // Calculate the bucket and short circuit if it's not changed - we need to change
    const newAccumulators = whatAccumulators(now);
    if (accumulators.current.bucket == newAccumulators.current.bucket) {
        return;
    }

    // Make a copy
    const oldAccumulators = _clonedeep(accumulators);

    // Update the live ones
    accumulators = newAccumulators;

    // If we have a new accumulator (ignore startup when old is null)
    if (oldAccumulators && oldAccumulators !== newAccumulators) {
        console.log(`--------[ accumulator rotation ]--------`);
        console.log(describeAccumulators(oldAccumulators).join('/'), ' => ', describeAccumulators(accumulators).join('/'));

        // Now we need to make sure we have flushed our H3 cache and everything
        // inflight has finished before doing this. we could purge cache
        // but that doesn't ensure that all the inflight has happened
        const s = await flushDirtyH3s(oldAccumulators, true);
        console.log(`h3cache flushed written ${s.written} records for ${s.databases} stations`);

        // If we are chaging the day then we will do a backup before
        // a rollup - this could lose some data in a restore as it doesn't
        // capture the current accumulator and it hasn't been merged in
        const doBackup = DO_BACKUPS && oldAccumulators?.day?.file && oldAccumulators.day.file !== accumulators?.day?.file;

        if (doBackup) {
            await backupDatabases(oldAccumulators);
        }

        await rollupAll(oldAccumulators, newAccumulators);

        // We also backup after with the new date which is fully rolled up
        if (doBackup) {
            await backupDatabases(accumulators);
        }
    }
}
