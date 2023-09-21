import {ClassicLevel} from 'classic-level';

import {ignoreStation} from './ignorestation';

import {CoverageRecord, bufferTypes} from './coveragerecord';
import {CoverageHeader} from './coverageheader';

import {StationId, StationName} from './types';

// h3 cache functions
import {flushDirtyH3s, updateCachedH3, getH3CacheSize} from './h3cache';

import {getAccumulator, getCurrentAccumulators, updateAndProcessAccumulators, CurrentAccumulator, AccumulatorTypeString} from './accumulators';
import {getStationDetails} from './stationstatus';

//import {rollupAll, rollupStats} from './rollup';
//import {rollupDatabase, rollupStartup} from './rollupworker';

import {filter as _filter, reduce as _reduce} from 'lodash';

import {DB_PATH, OUTPUT_PATH} from '../common/config';

let log = process.env.TEST_DEBUG ? console.log : () => 0;

// Set in global, we only have one db ;)
function set({h3, altitude, agl, crc, signal, gap, station, accumulator = getAccumulator(), now = Date.now()}: {h3: any; altitude: number; agl: number; signal: number; gap: number; crc: number; station: StationName; accumulator?: CurrentAccumulator; now?: number}) {
    accumulator ??= getAccumulator();
    now ??= Date.now();
    const stationDetails = getStationDetails(station);
    const h3k = new CoverageHeader(stationDetails.id, ...accumulator, h3);
    const buffer = new CoverageRecord(stationDetails.id === (0 as StationId) ? bufferTypes['global'] : bufferTypes['station']);
    updateCachedH3(station, h3k, altitude, agl, crc, signal, gap, stationDetails.id);
    //    log('set', accumulator, h3k.lockKey, altitude, agl, crc, signal, gap, stationid);
    //    buffer.update(altitude, agl, crc, signal, gap, stationid as StationId);
    //    (h3k.lockKey, {br: buffer, dirty: true, lastAccess: now, lastWrite: now});
}

async function get({h3, type, accumulator = null, inputAccumulators = null}) {
    const iAccumulator = inputAccumulators ?? getCurrentAccumulators();
    const acc: [AccumulatorTypeString, number] = accumulator ?? [type, iAccumulator[type].bucket];
    const h3k = new CoverageHeader(0 as StationId, ...acc, h3);
    log('get ', h3k.dbKey());
    try {
        return new CoverageRecord(await db.get(h3k.dbKey())).toObject();
    } catch (e) {
        return e;
    }
}

//
// Primary configuration loading and start the aprs receiver
let db = new ClassicLevel<string, Uint8Array>(DB_PATH + 'test');

let validStations = new Set();
validStations.add(1);

let validStations3 = new Set();
validStations3.add(3);

// Start empty
test('clear', async () => {
    await db.clear();
});

/*
// init rollup
test('updateAndProcessAccumulators', async (t) => {
    updateAndProcessAccumulators();
    t.pass();
});

// Set one row in the database and flush
test('set', async (t) => {
    set({h3: '87088619bffffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 1, station: 'a1' as StationName});
    t.is(getH3CacheSize(), 1, 'one item in cache');
});

test('flushed one', async (t) => {
    const flushStats = await flushDirtyH3s({allUnwritten: true});
    if (flushStats.written != 1) {
        t.fail();
    } else {
        t.pass();
    }
});

// roll it up ;)
test('rollup one item empty db', async (t) => {
    let meta = {};
    return rollupDatabase({db, stationName: 'global', stationMeta: meta, validStations}).then((data) => {
        if (meta.accumulators['day'].h3source == 1 && meta.accumulators['day'].h3missing == 1 && meta.accumulators['month'].h3missing == 1 && meta.accumulators['year'].h3missing == 1) {
            t.pass();
        } else {
            t.fail();
        }
    });
});

// shouldn't change with no new data
test('rollup nothing one item db', async (t) => {
    let meta = {};
    return rollupDatabase({db, stationName: 'global', stationMeta: meta, validStations}).then((data) => {
        t.is(meta.accumulators['day'].h3source, 0);
        t.is(meta.accumulators['day'].h3noChange, 1);
        t.is(meta.accumulators['month'].h3noChange, 1);
        t.is(meta.accumulators['year'].h3noChange, 1);
    });
});

test('check db is correct values', async (t) => {
    //	set({ h3: '87088619bffffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 1, stationid: 1 });
    return get({h3: '87088619bffffff', type: 'month'}).then((d) => {
        t.is(d.MaxSig, 10);
        t.is(d.SumGap, 1);
        t.is(d.stations[0].StationId, 1);
        t.is(d.stations[0].MinAltAgl, 100);
    });
});

function doTestMergeUniqueH3s({name, input, merge, output, finish, vs = validStations, inputAccumulators = undefined, emptyDb = false, stationName = 'global'}) {
    // Set one row in the database and flush
    test(name + ' (set)', async (t) => {
        flushDirtyH3s({allUnwritten: true, lockForRead: true});
        try {
            input.forEach((r) => {
                log(r);
                set({...r, accumulator: inputAccumulators ? inputAccumulators['current'] : undefined});
            });
        } catch (e) {
            log(e);
        }

        t.is(getH3CacheSize(), input.length, 'h3s in list');

        if (emptyDb) {
            await db.clear();
            t.pass();
        }
    });

    test(name + ' (flush/write)', async (t) => {
        const flushStats = await flushDirtyH3s({allUnwritten: true});
        t.is(flushStats.written, input.length, 'items written');
    });

    test(name + ' (rollup)', async (t) => {
        let meta = {};
        return rollupDatabase({
            db,
            stationName,
            stationMeta: meta,
            validStations: vs,
            needValidPurge: vs.size > 0,
            current: inputAccumulators ? inputAccumulators['current'] : undefined,
            processAccumulators: inputAccumulators
                ? _reduce(
                      inputAccumulators,
                      (a, v, k) => {
                          if (k != 'current') {
                              a[k] = v;
                          }
                          return a;
                      },
                      {}
                  )
                : undefined
        })
            .then((data) => {
                merge.forEach((r) => {
                    for (const [key, value] of Object.entries(r)) {
                        if (key != 'type') {
                            t.is(data.accumulators[r.type][key], value, `[${r.type}].${key} == ${value} {${JSON.stringify(data.accumulators[r.type])}}`);
                        }
                    }
                });
                t.is(true, true, 'rollup completed');
            })
            .catch((e) => {
                t.fail('unexpected exception' + e);
                console.log(e);
            });
    });

    test(name + ' (output)', async (t) => {
        for (const r of output) {
            await get({h3: r.h3, type: r.type, inputAccumulators: inputAccumulators})
                .then((d) => {
                    if (r.fail) {
                        t.is(!!Object.keys(d).length, false, 'expected failure');
                    }
                    for (const [key, value] of Object.entries(r)) {
                        if (key != 'h3' && key != 'type' && key != 'fail') {
                            t.is(d[key], value, `${r.h3}:${r.type}[${key}] == ${value} ${JSON.stringify(d)}`);
                        }
                    }
                })
                .catch((e) => {
                    t.pass('expected to fail');
                });
        }
    });

    test(name + 'finishes', (t) => {
        if (finish == true) {
            process.exit();
        }
        t.pass(finish);
    });
}

function doTestStartup({name, inputAccumulators, input, currentAccumulators, output, finish}) {
    test('db clear (startup prep)', async (t) => {
        await db.clear();
        expect(true).toBe(true);
    });

    // Set one row in the database and flush
    test(name + ' (startup/set)', async (t) => {
        cachedH3s.clear();
        input.forEach((r) => set(r));
        t.is(cachedH3s.size, input.length, 'h3s in list');
    });

    test(name + ' (set metadata)', async (t) => {
        await updateGlobalAccumulatorMetadata({
            globalDb: db,
            currentAccumulator: inputAccumulators['current'],
            current: inputAccumulators ? inputAccumulators['current'] : undefined,
            allAccumulators: _reduce(
                inputAccumulators,
                (a, v, k) => {
                    if (k != 'current') {
                        a[k] = v;
                    }
                    return a;
                },
                {}
            )
        });
        t.pass();
    });

    test(name + ' (startup/write)', async (t) => {
        const flushStats = await flushDirtyH3s({allUnwritten: true});
        expect(flushStats).toMatchObject({
            written: input.length
        });
    });

    test(name + ' (startup/rollup)', async (t) => {
        await rollupStartup({globalDb: db, statusDb: null, stationDbCache: {purgeStale: () => 0}, stations: {}});
        t.pass();
    });

    test(name + ' (output)', async (t) => {
        for (const r of output) {
            await get({h3: r.h3, type: r.type, accumulator: r.accumulator})
                .then((d) => {
                    if (r.fail) {
                        t.is(!!Object.keys(d).length, false, 'expected failure');
                    }
                    for (const [key, value] of Object.entries(r)) {
                        if (key != 'h3' && key != 'type' && key != 'fail' && key != 'accumulator') {
                            t.is(d[key], value, `${r.h3}:${r.type}[${key}] == ${value} ${JSON.stringify(d)}`);
                        }
                    }
                })
                .catch((e) => {
                    t.pass(r.fail, true, 'expected to fail');
                });
        }
    });

    test(name + 'finish', (t) => {
        if (finish == true) {
            process.exit();
        }
        t.pass(finish, false);
    });
}

doTestMergeUniqueH3s({
    name: 'update only h3',
    input: [{h3: '87088619bffffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 1, stationid: 1}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3updated: 1}
    ],
    output: [{type: 'day', h3: '87088619bffffff', Count: 2, SumSig: (10 >> 2) + (10 >> 2), SumGap: 2, MaxSig: 10}]
});

doTestMergeUniqueH3s({
    name: 'Add h3 before one',
    input: [{h3: '87088619affffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 1, stationid: 1}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3noChange: 1, h3missing: 1, h3extra: 1}
    ],
    output: [
        {type: 'day', h3: '87088619bffffff', Count: 2, SumSig: (10 >> 2) + (10 >> 2), SumGap: 2, MaxSig: 10},
        {type: 'day', h3: '87088619affffff', Count: 1, SumSig: 10 >> 2, SumGap: 1, MaxSig: 10}
    ]
});

doTestMergeUniqueH3s({
    name: 'Add h3 after two',
    input: [{h3: '87088619dffffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 1, stationid: 1}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3noChange: 2, h3missing: 1}
    ],
    output: [
        {type: 'day', h3: '87088619bffffff', Count: 2, SumSig: (10 >> 2) + (10 >> 2), SumGap: 2, MaxSig: 10},
        {type: 'day', h3: '87088619dffffff', Count: 1, SumSig: 10 >> 2, SumGap: 1, MaxSig: 10}
    ]
});

doTestMergeUniqueH3s({
    name: 'update last h3 after two',
    input: [{h3: '87088619dffffff', altitude: 50, agl: 50, crc: 0, signal: 10, gap: 1, stationid: 1}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3updated: 1}
    ],
    output: [{type: 'day', h3: '87088619dffffff', Count: 2, SumSig: (10 >> 2) + (10 >> 2), SumGap: 2, MaxSig: 10, MinAlt: 50}]
});

doTestMergeUniqueH3s({
    name: 'update first h3 before two',
    input: [{h3: '87088619affffff', altitude: 50, agl: 50, crc: 0, signal: 10, gap: 1, stationid: 1}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3updated: 1}
    ],
    output: [{type: 'day', h3: '87088619affffff', Count: 2, SumSig: (10 >> 2) + (10 >> 2), SumGap: 2, MaxSig: 10, MinAlt: 50}]
});

doTestMergeUniqueH3s({
    name: 'update middle h3',
    input: [{h3: '87088619bffffff', altitude: 50, agl: 50, crc: 0, signal: 10, gap: 1, stationid: 1}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3updated: 1}
    ],
    output: [{type: 'day', h3: '87088619bffffff', Count: 3, SumSig: (10 >> 2) * 3, SumGap: 3, MaxSig: 10, MinAlt: 50}]
});

doTestMergeUniqueH3s({
    name: 'update all h3s',
    input: [
        {h3: '87088619affffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 1, stationid: 1},
        {h3: '87088619bffffff', altitude: 50, agl: 50, crc: 0, signal: 20, gap: 2, stationid: 1},
        {h3: '87088619dffffff', altitude: 50, agl: 50, crc: 0, signal: 10, gap: 1, stationid: 1}
    ],
    merge: [
        {type: 'day', h3source: 3},
        {type: 'day', h3updated: 3, h3noChange: 0, h3missing: 0}
    ],
    output: [
        {type: 'day', h3: '87088619affffff', Count: 3, SumSig: (10 >> 2) * 3, SumGap: 3, MaxSig: 10, MinAlt: 50},
        {type: 'day', h3: '87088619bffffff', Count: 4, SumSig: (10 >> 2) * 3 + (20 >> 2), SumGap: 5, MaxSig: 20, MinAlt: 50, MinAltMaxSig: 20},
        {type: 'day', h3: '87088619dffffff', Count: 3, SumSig: (10 >> 2) * 3, SumGap: 3, MaxSig: 10, MinAlt: 50}
    ]
});

doTestMergeUniqueH3s({
    name: 'Add h3 in middle',
    input: [{h3: '87088619cffffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 1, stationid: 1}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3noChange: 3, h3missing: 1}
    ],
    output: [{type: 'day', h3: '87088619cffffff', Count: 1, SumSig: 10 >> 2, SumGap: 1, MaxSig: 10}]
});

doTestMergeUniqueH3s({
    name: 'Interleave h3s',
    input: [
        {h3: '87088619a0fffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 1, stationid: 1},
        {h3: '87088619b0fffff', altitude: 50, agl: 50, crc: 0, signal: 20, gap: 1, stationid: 1},
        {h3: '87088619c0fffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 2, stationid: 1},
        {h3: '87088619d0fffff', altitude: 50, agl: 50, crc: 0, signal: 10, gap: 1, stationid: 1},
        {h3: '87088619e0fffff', altitude: 50, agl: 50, crc: 0, signal: 10, gap: 1, stationid: 1}
    ],
    merge: [
        {type: 'day', h3source: 5},
        {type: 'day', h3noChange: 4, h3missing: 5}
    ],
    output: [
        {type: 'day', h3: '87088619a0fffff', Count: 1, SumSig: 10 >> 2, SumGap: 1, MaxSig: 10},
        {type: 'day', h3: '87088619b0fffff', Count: 1, SumSig: 20 >> 2, SumGap: 1, MaxSig: 20},
        {type: 'day', h3: '87088619c0fffff', Count: 1, SumSig: 10 >> 2, SumGap: 2, MaxSig: 10},
        {type: 'day', h3: '87088619d0fffff', Count: 1, SumSig: 10 >> 2, SumGap: 1, MaxSig: 10},
        {type: 'day', h3: '87088619e0fffff', Count: 1, SumSig: 10 >> 2, SumGap: 1, MaxSig: 10}
    ]
});

// This adds, but a follow up rollup will remove it
doTestMergeUniqueH3s({
    name: 'Add invalid station (only one in h3)',
    input: [{h3: '87088619a1fffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 1, stationid: 2}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3noChange: 9, h3missing: 1}
    ],
    output: [{type: 'day', h3: '87088619a1fffff', Count: 1, SumSig: 10 >> 2, SumGap: 1, MaxSig: 10}]
});

doTestMergeUniqueH3s({
    name: 'merge with invalid station in db',
    input: [{h3: '87088619a0fffff', altitude: 99, agl: 99, crc: 0, signal: 10, gap: 1, stationid: 1}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3noChange: 8, h3updated: 1, h3stationsRemoved: 1, h3emptied: 1}
    ],
    output: [
        {type: 'day', h3: '87088619a1fffff', fail: true},
        {type: 'day', h3: '87088619a0fffff', Count: 2, SumSig: (10 >> 2) * 2, SumGap: 2, MaxSig: 10}
    ]
});

// We don't filter invalids if they are in the source.. it's kind of a loophole
// but they probably shouldn't be in current accumulator if they are invalid... exception
// will be on station move!
doTestMergeUniqueH3s({
    name: 'Add invalid station (existing h3)',
    input: [{h3: '87088619a0fffff', altitude: 9, agl: 101, crc: 0, signal: 10, gap: 2, stationid: 2}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3updated: 1}
    ],
    output: [{type: 'day', h3: '87088619a0fffff', Count: 3, SumSig: (10 >> 2) * 3, SumGap: 4, MaxSig: 10, MinAlt: 9, NumStations: 2}]

});

doTestMergeUniqueH3s({
    name: 'merge with invalid station in same h3',
    input: [{h3: '87088619a0fffff', altitude: 90, agl: 90, crc: 0, signal: 10, gap: 10, stationid: 1}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3updated: 1, h3stationsRemoved: 0, h3emptied: 0}
    ],
    output: [
        {type: 'day', h3: '87088619a1fffff', fail: true},
        {type: 'day', h3: '87088619a0fffff', Count: 3, SumSig: (10 >> 2) * 3, SumGap: 12, MaxSig: 10, MinAlt: 90, NumStations: 1}
    ]
});

// All stations in DB at this point are #1, so we are going to zap them all by making id 1 invalid and add a new
// record for id4
doTestMergeUniqueH3s({
    name: 'invalidate s1',
    input: [{h3: '87088619a0fffff', altitude: 91, agl: 91, crc: 0, signal: 10, gap: 10, stationid: 3}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3updated: 1, h3stationsRemoved: 8, h3emptied: 8}
    ],
    output: [
        {type: 'day', h3: '87088619a1fffff', fail: true},
        {type: 'day', h3: '87088619b0fffff', fail: true},
        {type: 'day', h3: '87088619bffffff', fail: true},
        {type: 'day', h3: '87088619c0fffff', fail: true},
        {type: 'day', h3: '87088619a0fffff', Count: 1, SumSig: 10 >> 2, SumGap: 10, MaxSig: 10, MinAlt: 91, NumStations: 1}
    ],
    vs: validStations3
});

// And now make s3 invalid to ensure empty db
doTestMergeUniqueH3s({
    name: 'invalidate s3',
    input: [],
    merge: [
        {type: 'day', h3source: 0},
        {type: 'day', h3updated: 0, h3stationsRemoved: 1, h3emptied: 1}
    ],
    output: [
        {type: 'day', h3: '87088619a1fffff', fail: true},
        {type: 'day', h3: '87088619b0fffff', fail: true},
        {type: 'day', h3: '87088619bffffff', fail: true},
        {type: 'day', h3: '87088619c0fffff', fail: true},
        {type: 'day', h3: '87088619a0fffff', fail: true}
    ]
});

////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////
///
/// Tests without sub station records
///
////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////

test('station clear', async (t) => {
    await db.clear();
    t.pass();
});

doTestMergeUniqueH3s({
    name: 'station:  insert one h3',
    stationName: '!test',
    input: [{h3: '87088619bffffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 1}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3missing: 1}
    ],
    output: [{type: 'day', h3: '87088619bffffff', Count: 1, SumSig: 10 >> 2, SumGap: 1, MaxSig: 10}]
});

doTestMergeUniqueH3s({
    name: 'station: update same h3',
    stationName: '!test',
    input: [{h3: '87088619bffffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 1}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3updated: 1}
    ],
    output: [{type: 'day', h3: '87088619bffffff', Count: 2, SumSig: (10 >> 2) + (10 >> 2), SumGap: 2, MaxSig: 10}]
});

doTestMergeUniqueH3s({
    name: 'station:  Add h3 before one',
    stationName: '!test',
    input: [{h3: '87088619affffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 1}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3noChange: 1, h3missing: 1, h3extra: 1}
    ],
    output: [
        {type: 'day', h3: '87088619bffffff', Count: 2, SumSig: (10 >> 2) + (10 >> 2), SumGap: 2, MaxSig: 10},
        {type: 'day', h3: '87088619affffff', Count: 1, SumSig: 10 >> 2, SumGap: 1, MaxSig: 10}
    ]
});

doTestMergeUniqueH3s({
    name: 'station: Add h3 after two',
    stationName: '!test',
    input: [{h3: '87088619dffffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 1}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3noChange: 2, h3missing: 1}
    ],
    output: [
        {type: 'day', h3: '87088619bffffff', Count: 2, SumSig: (10 >> 2) + (10 >> 2), SumGap: 2, MaxSig: 10},
        {type: 'day', h3: '87088619dffffff', Count: 1, SumSig: 10 >> 2, SumGap: 1, MaxSig: 10}
    ]
});

doTestMergeUniqueH3s({
    name: 'station: update last h3 after two',
    stationName: '!test',
    input: [{h3: '87088619dffffff', altitude: 50, agl: 50, crc: 0, signal: 10, gap: 1}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3updated: 1}
    ],
    output: [{type: 'day', h3: '87088619dffffff', Count: 2, SumSig: (10 >> 2) + (10 >> 2), SumGap: 2, MaxSig: 10, MinAlt: 50}]
});

doTestMergeUniqueH3s({
    name: 'station: update first h3 before two',
    stationName: '!test',
    input: [{h3: '87088619affffff', altitude: 50, agl: 50, crc: 0, signal: 10, gap: 1}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3updated: 1}
    ],
    output: [{type: 'day', h3: '87088619affffff', Count: 2, SumSig: (10 >> 2) + (10 >> 2), SumGap: 2, MaxSig: 10, MinAlt: 50}]
});

doTestMergeUniqueH3s({
    name: 'station: update middle h3',
    stationName: '!test',
    input: [{h3: '87088619bffffff', altitude: 50, agl: 50, crc: 0, signal: 10, gap: 1}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3updated: 1}
    ],
    output: [{type: 'day', h3: '87088619bffffff', Count: 3, SumSig: (10 >> 2) * 3, SumGap: 3, MaxSig: 10, MinAlt: 50}]
});

doTestMergeUniqueH3s({
    name: 'station: update all h3s',
    stationName: '!test',
    input: [
        {h3: '87088619affffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 1},
        {h3: '87088619bffffff', altitude: 50, agl: 50, crc: 0, signal: 20, gap: 2},
        {h3: '87088619dffffff', altitude: 50, agl: 50, crc: 0, signal: 10, gap: 1}
    ],
    merge: [
        {type: 'day', h3source: 3},
        {type: 'day', h3updated: 3, h3noChange: 0, h3missing: 0}
    ],
    output: [
        {type: 'day', h3: '87088619affffff', Count: 3, SumSig: (10 >> 2) * 3, SumGap: 3, MaxSig: 10, MinAlt: 50},
        {type: 'day', h3: '87088619bffffff', Count: 4, SumSig: (10 >> 2) * 3 + (20 >> 2), SumGap: 5, MaxSig: 20, MinAlt: 50, MinAltMaxSig: 20},
        {type: 'day', h3: '87088619dffffff', Count: 3, SumSig: (10 >> 2) * 3, SumGap: 3, MaxSig: 10, MinAlt: 50}
    ]
});

doTestMergeUniqueH3s({
    name: 'station: Add h3 in middle',
    stationName: '!test',
    input: [{h3: '87088619cffffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 1}],
    merge: [
        {type: 'day', h3source: 1},
        {type: 'day', h3noChange: 3, h3missing: 1}
    ],
    output: [{type: 'day', h3: '87088619cffffff', Count: 1, SumSig: 10 >> 2, SumGap: 1, MaxSig: 10}]
});

doTestMergeUniqueH3s({
    name: 'station: Interleave h3s',
    stationName: '!test',
    input: [
        {h3: '87088619a0fffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 1},
        {h3: '87088619b0fffff', altitude: 50, agl: 50, crc: 0, signal: 20, gap: 1},
        {h3: '87088619c0fffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 2},
        {h3: '87088619d0fffff', altitude: 50, agl: 50, crc: 0, signal: 10, gap: 1},
        {h3: '87088619e0fffff', altitude: 50, agl: 50, crc: 0, signal: 10, gap: 1}
    ],
    merge: [
        {type: 'day', h3source: 5},
        {type: 'day', h3noChange: 4, h3missing: 5}
    ],
    output: [
        {type: 'day', h3: '87088619a0fffff', Count: 1, SumSig: 10 >> 2, SumGap: 1, MaxSig: 10},
        {type: 'day', h3: '87088619b0fffff', Count: 1, SumSig: 20 >> 2, SumGap: 1, MaxSig: 20},
        {type: 'day', h3: '87088619c0fffff', Count: 1, SumSig: 10 >> 2, SumGap: 2, MaxSig: 10},
        {type: 'day', h3: '87088619d0fffff', Count: 1, SumSig: 10 >> 2, SumGap: 1, MaxSig: 10},
        {type: 'day', h3: '87088619e0fffff', Count: 1, SumSig: 10 >> 2, SumGap: 1, MaxSig: 10}
    ]
});

// Try and reproduce the strange live bug
doTestMergeUniqueH3s({
    name: 'station: weirding A',
    stationName: '!test',
    emptyDb: true,
    input: [
        {h3: '881e01940dfffff', altitude: 1751, signal: 132, crc: 0, gap: 1},
        {h3: '881e019463fffff', altitude: 800, signal: 130, crc: 0, gap: 1}
    ],
    inputAccumulators: {current: ['current', 1], day: {bucket: 1}, month: {bucket: 1}},
    merge: [
        {type: 'day', h3source: 2, h3missing: 2},
        {type: 'month', h3source: 2, h3missing: 2}
    ],
    output: [
        {type: 'day', h3: '881e019463fffff', Count: 1, MaxSig: 130},
        {type: 'day', h3: '881e01940dfffff', Count: 1, MaxSig: 132}
    ]
});

doTestMergeUniqueH3s({
    name: 'station: weirding B',
    stationName: '!test',
    input: [{h3: '881e019463fffff', altitude: 800, signal: 130, crc: 0, gap: 1}],
    inputAccumulators: {current: ['current', 1], day: {bucket: 2}, month: {bucket: 1}},
    merge: [
        {type: 'day', h3source: 1, h3missing: 1},
        {type: 'month', h3source: 1, h3missing: 0, h3updated: 1}
    ],
    output: [
        {type: 'day', h3: '881e019463fffff', Count: 1, MaxSig: 130},
        {type: 'month', h3: '881e019463fffff', Count: 2, MaxSig: 130},
        {type: 'day', h3: '881e01940dfffff', Count: undefined}
    ]
});

doTestMergeUniqueH3s({
    name: 'station: weirding C',
    stationName: '!test',
    input: [{h3: '881e019463fffff', altitude: 892, signal: 160, crc: 0, gap: 1}],
    inputAccumulators: {current: ['current', 1], day: {bucket: 2}, month: {bucket: 1}},
    merge: [
        {type: 'day', h3source: 1, h3updated: 1, h3missing: 0},
        {type: 'month', h3source: 1, h3updated: 1, h3noChange: 1}
    ],
    output: [
        {type: 'day', h3: '881e019463fffff', Count: 2, MaxSig: 160},
        {type: 'month', h3: '881e019463fffff', Count: 3, MaxSig: 160},
        {type: 'day', h3: '881e01940dfffff', Count: undefined}
    ]
});

//
// We can also test to make sure we delete any hangings

////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////
///
/// Startup Tests... ish
///
/// This is really quite hard to test as it has a lot of date dependent state
///
////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////

// Lets generate a hanging current
doTestStartup({name: 'A', inputAccumulators: {current: [0, 1], day: {bucket: 1}, month: {bucket: 1}}, currentAccumulators: {current: [0, 3], day: {bucket: 2}, month: {bucket: 1}}, input: [{h3: '87088619cffffff', altitude: 100, agl: 100, crc: 0, signal: 10, gap: 1, accumulator: ['current', 1]}], output: [{h3: '87088619a0fffff', accumulator: ['current', 1], fail: true}]});

*/
