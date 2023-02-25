import {writeFileSync} from 'fs';

import {prefixWithZeros} from './prefixwithzeros';

import _clonedeep from 'lodash.clonedeep';
import _isequal from 'lodash.isequal';
import _reduce from 'lodash.reduce';

import {
    ROLLUP_PERIOD_MINUTES, //
    OUTPUT_PATH
} from './config.js';

import {flushDirtyH3s, unlockH3sForReads} from './h3cache';
import {rollupAll} from './rollup';

import {CoverageHeader} from './coverageheader';

//
// What accumulators we are operating on these are internal
let currentAccumulator = undefined;

export type Accumulators =
    | Record<
          'current' | 'day' | 'month' | 'year',
          {
              bucket: number;
              file: string;
          }
      >
    | {};

let accumulators: Accumulators = {};

//
// Helper for getting current accumulator used as  ...getAccumulator() in
// calls to CoverageHeader
export function getAccumulator() {
    return currentAccumulator;
}
export function getAccumulatorForType(t) {
    if (t == 'current') {
        return currentAccumulator;
    } else {
        return [t, accumulators[t].bucket];
    }
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
// if you run this after a month gap then welcome back ;) and I'm sorry ;)  [it has to fit in 12bits]
//
// in this situation if it happens to be identical bucket it will resume into current month
// otherwise it will try and rollup into the buckets that existed at the time (which are valid
/// for several years) or discard the data.
//
// this takes effect immediately so all new packets will move to the new accumulator
// rolling over is maximum of 12 times an hour...
export function whatAccumulators(now) {
    const rolloverperiod = Math.floor((now.getUTCHours() * 60 + now.getUTCMinutes()) / ROLLUP_PERIOD_MINUTES);
    const newAccumulatorBucket = ((now.getUTCDate() & 0x1f) << 7) | (rolloverperiod & 0x7f);
    const n = {
        d: prefixWithZeros(2, String(now.getUTCDate())),
        m: prefixWithZeros(2, String(now.getUTCMonth() + 1)),
        y: now.getUTCFullYear()
    };
    accumulators = {
        day: {
            bucket: ((now.getUTCFullYear() & 0x07) << 9) | ((now.getUTCMonth() & 0x0f) << 5) | (now.getUTCDate() & 0x1f), //
            file: `${n.y}-${n.m}-${n.d}`
        },
        month: {bucket: ((now.getUTCFullYear() & 0xff) << 4) | (now.getUTCMonth() & 0x0f), file: `${n.y}-${n.m}`},
        year: {bucket: now.getUTCFullYear(), file: `${n.y}`}
    };
    return {current: newAccumulatorBucket, accumulators: accumulators};
}

export async function updateAndProcessAccumulators({globalDb, statusDb, stationDbCache, stations}) {
    const now = new Date();

    // Make a copy
    const oldAccumulators = _clonedeep(accumulators);
    const oldAccumulator = _clonedeep(currentAccumulator);

    // Calculate the bucket and short circuit if it's not changed - we need to change
    const {current: newAccumulatorBucket, accumulators: newAccumulators} = whatAccumulators(now);
    if (currentAccumulator?.[1] == newAccumulatorBucket) {
        return;
    }

    // Update the live ones
    currentAccumulator = ['current', newAccumulatorBucket];
    accumulators = newAccumulators;

    // If we have a new accumulator (ignore startup when old is null)
    if (oldAccumulator) {
        console.log(`accumulator rotation:`);
        console.log(JSON.stringify(oldAccumulators));
        console.log('----');
        console.log(JSON.stringify(accumulators));

        // Now we need to make sure we have flushed our H3 cache and everything
        // inflight has finished before doing this. we could purge cache
        // but that doesn't ensure that all the inflight has happened
        const s = await flushDirtyH3s({globalDb, stationDbCache, stations, allUnwritten: true, lockForRead: true});
        console.log(`accumulator rotation happening`, s);
        await rollupAll({current: oldAccumulator, processAccumulators: oldAccumulators, globalDb, statusDb, stationDbCache, stations, newAccumulatorFiles: !_isequal(accumulators, oldAccumulators)});
        unlockH3sForReads();
    }
}
