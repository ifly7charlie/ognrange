import {CoverageRecord, bufferTypes} from './coveragerecord';
import {CoverageHeader} from './coverageheader';
import {h3IndexToSplitLong} from 'h3-js';

import {sortBy as _sortby} from 'lodash';

//

const validStations1 = new Set<StationId>();
validStations1.add(1 as StationId);

const validStations2 = new Set<StationId>();
validStations2.add(2 as StationId);

const validStations12 = new Set<StationId>();
validStations12.add(1 as StationId);
validStations12.add(2 as StationId);

let cr: CoverageRecord;

import {CoverageRecordOut, bufferTypeNames} from './coveragerecord';
import {StationId} from './types';
interface Samples {
    type: bufferTypes;
    inputs: Sample[];
}

type Sample = [altitude: number, agl: number, crc: number, signal: number, gap: number, stationid: StationId] | [altitude: number, agl: number, crc: number, signal: number, gap: number];

function doCRTest({name, type, inputs, output}: {name: string; type: bufferTypes; inputs: Sample[]; output: CoverageRecordOut[]}) {
    // Set one row in the database and flush
    test(bufferTypeNames[type] + ': ' + name + ' update', () => {
        cr = new CoverageRecord(type);
        inputs.forEach((r: Sample) => cr.update(...r));
        const o = cr.toObject();
        for (const [key, value] of Object.entries(output)) {
            if (typeof value === 'number') {
                expect(o[key]).toBe(value);
            } else {
                const [station, pos] = key.split(':') || [null, null];
                if (station && pos) {
                    if (typeof o.stations === 'object') {
                        const subStations = o.stations[Number(pos)] || {};
                        for (const [keyn, valuen] of Object.entries(value)) {
                            expect(!!subStations).toBe(false);
                            expect(subStations[keyn]).toBe(valuen);
                        }
                    }
                }
            }
        }
    });
}
function doRollupTest({name, src, dest, validStations, output}: {name: string; src?: Samples; dest: Samples; validStations: Set<StationId>; output: CoverageRecordOut[]}) {
    // Set one row in the database and flush
    test((src ? bufferTypeNames[src.type] + '/' : 'removeInvalidStations/') + bufferTypeNames[dest.type] + ': ' + name + ' rollup', () => {
        let srccr = new CoverageRecord(src?.type || 0);
        let destcr = new CoverageRecord(dest.type);
        src?.inputs.forEach((r: Sample) => srccr.update(...r));
        dest.inputs.forEach((r) => destcr.update(...r));
        const out = src ? destcr.rollup(srccr, validStations) : destcr.removeInvalidStations(validStations);
        expect(out).not.toBeNull();

        const o = out!.toObject();
        for (const key in output) {
            const value = output[key];
            if (typeof value === 'number') {
                expect(o[key]).toBe(value);
            } else {
                const [station, pos] = [...key.split(':'), undefined, undefined];
                if (station && pos) {
                    expect(parseInt(pos)).toBeGreaterThanOrEqual(0);
                    expect(!!o.stations).toBe(true);
                    if (typeof o.stations === 'object') {
                        const subStations = o.stations[Number(pos)];
                        for (const [keyn, valuen] of Object.entries(value)) {
                            expect(!!subStations).toBe(true);
                            expect(subStations[keyn]).toBe(valuen);
                        }
                    }
                }
            }
        }
    });
}

// Check all basic permutations
//	update( altitude, agl, crc, signal, gap, stationid ) {
doCRTest({
    name: 'one entry',
    type: bufferTypes.station,
    inputs: [[10, 11, 1, 12, 0]],
    //
    output: [{MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: 12 >> 2, SumCrc: 1, Count: 1, SumGap: 0, NumStations: undefined}]
});

doCRTest({
    name: 'A two updates',
    type: bufferTypes.station,
    inputs: [
        [10, 11, 1, 12, 0],
        [10, 11, 1, 12, 0]
    ],
    output: [{MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: (12 >> 2) * 2, SumCrc: 2, Count: 2, SumGap: 0, NumStations: undefined}]
});

doCRTest({
    name: 'B two different updates',
    type: bufferTypes.station,
    inputs: [
        [10, 10, 10, 10, 10],
        [10, 10, 10, 10, 10]
    ],
    output: [{MinAlt: 10, MinAltMaxSig: 10, MinAltAgl: 10, SumSig: (10 >> 2) * 2, SumCrc: 20, Count: 2, SumGap: 20, NumStations: undefined}]
});

doCRTest({
    name: 'C two more different updates',
    type: bufferTypes.station,
    inputs: [
        [10, 10, 10, 10, 10],
        [1, 12, 0, 20, 0]
    ],
    output: [{MinAlt: 1, MinAltMaxSig: 20, MinAltAgl: 10, SumSig: (10 >> 2) + (20 >> 2), SumCrc: 10, Count: 2, SumGap: 10, NumStations: undefined}]
});

doCRTest({
    name: 'D two more different updates',
    type: bufferTypes.station,
    inputs: [
        [10, 10, 10, 10, 10],
        [12, 1, 9, 20, 0]
    ],

    output: [{MinAlt: 10, MinAltMaxSig: 10, MinAltAgl: 1, SumSig: (10 >> 2) + (20 >> 2), SumCrc: 19, Count: 2, SumGap: 10, NumStations: undefined}]
});

///
// Next test for global records (ie with substations)
doCRTest({
    name: 'A',
    type: bufferTypes.global,
    inputs: [[10, 11, 1, 12, 0, 1 as StationId]], //
    output: [{MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: 12 >> 2, SumCrc: 1, Count: 1, SumGap: 0, NumStations: 1}]
});

//doCRTest({name: 'B', type: bufferTypes.global, inputs: [[10, 11, 1, 12, 0, 1]], //
//        output: {MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: 12 >> 2, SumCrc: 1, Count: 1, SumGap: 0,
//                                                                                     'Station:0': {MinAlt: 10, Count: 1, MinAltMaxSig: 12}, NumStations: 1}});

//	update( altitude, agl, crc, signal, gap, stationid ) {
doCRTest({
    name: 'two from one',
    type: bufferTypes.global,
    inputs: [
        [10, 11, 1, 12, 0, 1 as StationId],
        [10, 11, 1, 12, 0, 1 as StationId]
    ],
    output: [
        {
            MinAlt: 10,
            MinAltMaxSig: 12,
            MinAltAgl: 11,
            SumSig: (12 >> 2) * 2,
            SumCrc: 2,
            Count: 2,
            SumGap: 0, //
            'Station:0': [{MinAlt: 10, Count: 2, MinAltMaxSig: 12, StationId: 1 as StationId}],
            NumStations: 1
        }
    ]
});

doCRTest({
    name: 'one from each of two',
    type: bufferTypes.global,
    inputs: [
        [10, 11, 1, 12, 0, 1 as StationId],
        [10, 11, 1, 12, 0, 2 as StationId]
    ],
    output: [
        {
            MinAlt: 10,
            MinAltMaxSig: 12,
            MinAltAgl: 11,
            SumSig: (12 >> 2) * 2,
            SumCrc: 2,
            Count: 2,
            SumGap: 0, //
            'Station:0': [{MinAlt: 10, Count: 1, MinAltMaxSig: 12, StationId: 1 as StationId}],
            'Station:1': [{MinAlt: 10, Count: 1, MinAltMaxSig: 12, StationId: 2 as StationId}],
            NumStations: 2
        }
    ]
});

doCRTest({
    name: 'two from one station, one from another',
    type: bufferTypes.global,
    inputs: [
        [10, 11, 1, 12, 0, 1 as StationId],
        [10, 11, 1, 12, 0, 1 as StationId],
        [10, 11, 1, 12, 0, 2 as StationId]
    ],
    output: [
        {
            MinAlt: 10,
            MinAltMaxSig: 12,
            MinAltAgl: 11,
            SumSig: (12 >> 2) * 3,
            SumCrc: 3,
            Count: 3,
            SumGap: 0, //
            'Station:0': [{MinAlt: 10, Count: 2, MinAltMaxSig: 12, StationId: 1 as StationId}],
            'Station:1': [{MinAlt: 10, Count: 1, MinAltMaxSig: 12, StationId: 2 as StationId}],
            NumStations: 2
        }
    ]
});

doCRTest({
    name: 'two from one station, one from another, sort order change',
    type: bufferTypes.global,
    inputs: [
        [10, 11, 1, 12, 0, 1 as StationId],
        [10, 11, 1, 12, 0, 2 as StationId],
        [10, 11, 1, 12, 0, 2 as StationId]
    ],
    output: [
        {
            MinAlt: 10,
            MinAltMaxSig: 12,
            MinAltAgl: 11,
            SumSig: (12 >> 2) * 3,
            SumCrc: 3,
            Count: 3,
            SumGap: 0, //
            'Station:0': [{MinAlt: 10, Count: 2, MinAltMaxSig: 12, StationId: 2 as StationId}],
            'Station:1': [{MinAlt: 10, Count: 1, MinAltMaxSig: 12, StationId: 1 as StationId}],
            NumStations: 2
        }
    ]
});

doCRTest({
    name: 'two from one station, one from another, sort order stable',
    type: bufferTypes.global,
    inputs: [
        [10, 11, 1, 12, 0, 1 as StationId],
        [10, 11, 1, 12, 0, 2 as StationId],
        [10, 11, 1, 12, 0, 2 as StationId],
        [10, 11, 1, 12, 0, 1 as StationId]
    ],
    output: [
        {
            'Station:0': [{Count: 2, StationId: 2 as StationId}],
            'Station:1': [{Count: 2, StationId: 1 as StationId}],
            NumStations: 2
        }
    ]
});

doCRTest({
    name: 'two from one station, one from another, sort order stable, add third',
    type: bufferTypes.global,
    inputs: [
        [10, 11, 1, 12, 0, 1 as StationId],
        [10, 11, 1, 12, 0, 2 as StationId],
        [10, 11, 1, 12, 0, 2 as StationId],
        [10, 11, 1, 12, 0, 1 as StationId],
        [10, 11, 1, 12, 0, 3 as StationId]
    ],
    output: [
        {
            'Station:0': [{Count: 2, StationId: 2 as StationId}],
            'Station:1': [{Count: 2, StationId: 1 as StationId}],
            'Station:2': [{Count: 1, StationId: 3 as StationId}],
            NumStations: 3
        }
    ]
});

doRollupTest({
    name: 'A',
    src: {type: bufferTypes.global, inputs: [[10, 11, 1, 12, 0, 1 as StationId]]},
    dest: {type: bufferTypes.global, inputs: [[10, 11, 1, 12, 0, 1 as StationId]]},
    validStations: validStations1, //
    output: [
        {
            MinAlt: 10,
            MinAltMaxSig: 12,
            MinAltAgl: 11,
            SumSig: (12 >> 2) * 2,
            SumCrc: 2,
            Count: 2,
            SumGap: 0, //
            'Station:0': [{MinAlt: 10, Count: 2, MinAltMaxSig: 12, StationId: 1 as StationId}],
            NumStations: 1
        }
    ]
});

doRollupTest({
    name: 'B',
    src: {
        type: bufferTypes.global,
        inputs: [
            [10, 11, 1, 12, 0, 1 as StationId],
            [10, 11, 1, 12, 0, 2 as StationId]
        ]
    },
    dest: {type: bufferTypes.global, inputs: [[10, 11, 1, 12, 0, 2 as StationId]]},
    validStations: validStations12,
    output: [
        {
            MinAlt: 10,
            MinAltMaxSig: 12,
            MinAltAgl: 11,
            SumSig: (12 >> 2) * 3,
            SumCrc: 3,
            Count: 3,
            SumGap: 0, //
            'Station:0': [{MinAlt: 10, Count: 2, MinAltMaxSig: 12, StationId: 2 as StationId}],
            'Station:1': [{MinAlt: 10, Count: 1, MinAltMaxSig: 12, StationId: 1 as StationId}],
            NumStations: 2
        }
    ]
});

// Order doesn't matter (1/2)
doRollupTest({
    name: 'C',
    src: {
        type: bufferTypes.global,
        inputs: [
            [10, 11, 1, 12, 0, 1 as StationId],
            [10, 11, 1, 12, 0, 2 as StationId],
            [10, 11, 1, 12, 0, 2 as StationId]
        ]
    },
    dest: {type: bufferTypes.global, inputs: []},
    validStations: validStations12,
    output: [
        {
            MinAlt: 10,
            MinAltMaxSig: 12,
            MinAltAgl: 11,
            SumSig: (12 >> 2) * 3,
            SumCrc: 3,
            Count: 3,
            SumGap: 0, //
            'Station:0': [{MinAlt: 10, Count: 2, MinAltMaxSig: 12, StationId: 2 as StationId}],
            'Station:1': [{MinAlt: 10, Count: 1, MinAltMaxSig: 12, StationId: 1 as StationId}],
            NumStations: 2
        }
    ]
});

doRollupTest({
    name: 'D',
    src: {type: bufferTypes.global, inputs: []},
    dest: {
        type: bufferTypes.global,
        inputs: [
            [10, 11, 1, 12, 0, 1 as StationId],
            [10, 11, 1, 12, 0, 2 as StationId],
            [10, 11, 1, 12, 0, 2 as StationId]
        ]
    },
    validStations: validStations12,
    output: [
        {
            MinAlt: 10,
            MinAltMaxSig: 12,
            MinAltAgl: 11,
            SumSig: (12 >> 2) * 3,
            SumCrc: 3,
            Count: 3,
            SumGap: 0, //
            'Station:0': [{MinAlt: 10, Count: 2, MinAltMaxSig: 12, StationId: 2 as StationId}],
            'Station:1': [{MinAlt: 10, Count: 1, MinAltMaxSig: 12, StationId: 1 as StationId}],
            NumStations: 2
        }
    ]
});

// Drop a station (3) in rollup (replacing it with identical row for simplicty of test)
// this makes empty plus whole new one, note sort order is not stable on rollup
// when stations have same count so don't check that!
doRollupTest({
    name: 'E',
    src: {
        type: bufferTypes.global,
        inputs: [
            [10, 11, 1, 12, 0, 1 as StationId],
            [10, 11, 1, 12, 0, 2 as StationId],
            [10, 11, 1, 12, 0, 2 as StationId],
            [10, 11, 1, 12, 0, 1 as StationId]
        ]
    },
    dest: {type: bufferTypes.global, inputs: [[10, 11, 1, 12, 0, 3 as StationId]]},
    validStations: validStations12,
    output: [
        {
            MinAlt: 10,
            MinAltMaxSig: 12,
            MinAltAgl: 11,
            SumSig: (12 >> 2) * 4,
            SumCrc: 4,
            Count: 4,
            SumGap: 0, //
            'Station:0': [{MinAlt: 10, Count: 2, MinAltMaxSig: 12}],
            'Station:1': [{MinAlt: 10, Count: 2, MinAltMaxSig: 12}],
            NumStations: 2
        }
    ]
});

doRollupTest({
    name: 'F',
    src: {
        type: bufferTypes.global,
        inputs: [
            [10, 11, 1, 12, 0, 1 as StationId],
            [10, 11, 1, 12, 0, 2 as StationId],
            [10, 11, 1, 12, 0, 2 as StationId],
            [10, 11, 1, 12, 0, 1 as StationId],
            [10, 11, 1, 12, 0, 1 as StationId]
        ]
    },
    dest: {type: bufferTypes.global, inputs: [[10, 11, 1, 12, 0, 3 as StationId]]},
    validStations: validStations12,
    output: [
        {
            SumCrc: 5,
            Count: 5,
            SumGap: 0, //
            'Station:0': [{MinAlt: 10, Count: 3, MinAltMaxSig: 12, StationId: 1 as StationId}],
            'Station:1': [{MinAlt: 10, Count: 2, MinAltMaxSig: 12, StationId: 2 as StationId}],
            NumStations: 2
        }
    ]
});

//
// Not specifying dest means remove
doRollupTest({
    name: 'A',
    dest: {
        type: bufferTypes.global,
        inputs: [
            [10, 11, 1, 12, 0, 1 as StationId],
            [10, 11, 1, 12, 0, 1 as StationId]
        ]
    },
    validStations: validStations1,
    output: [
        {
            MinAlt: 10,
            MinAltMaxSig: 12,
            MinAltAgl: 11,
            SumSig: (12 >> 2) * 2,
            SumCrc: 2,
            Count: 2,
            SumGap: 0, //
            'Station:0': [{MinAlt: 10, Count: 2, MinAltMaxSig: 12, StationId: 1 as StationId}],
            NumStations: 1
        }
    ]
});

//	update( altitude, agl, crc, signal, gap, stationid ) {
doRollupTest({
    name: 'B',
    dest: {
        type: bufferTypes.global,
        inputs: [
            [10, 11, 1, 12, 0, 1 as StationId],
            [9, 10, 2, 20, 1, 2 as StationId]
        ]
    },
    validStations: validStations1,
    output: [
        {
            MinAlt: 10,
            MinAltMaxSig: 12,
            MinAltAgl: 11,
            SumSig: 12 >> 2,
            SumCrc: 1,
            Count: 1,
            SumGap: 0, //
            'Station:0': [{MinAlt: 10, Count: 1, MinAltMaxSig: 12, StationId: 1 as StationId}],
            NumStations: 1
        }
    ]
});

doRollupTest({
    name: 'C',
    dest: {
        type: bufferTypes.global,
        inputs: [
            [10, 11, 1, 12, 0, 1 as StationId],
            [9, 10, 2, 20, 1, 2 as StationId]
        ]
    },
    validStations: validStations2,
    output: [
        {
            MinAlt: 9,
            MinAltMaxSig: 20,
            MinAltAgl: 10,
            SumSig: 20 >> 2,
            SumCrc: 2,
            Count: 1,
            SumGap: 1, //
            'Station:0': [{MinAlt: 9, Count: 1, MinAltMaxSig: 20, StationId: 2 as StationId}],
            NumStations: 1
        }
    ]
});

doRollupTest({
    name: 'D',
    dest: {
        type: bufferTypes.global,
        inputs: [
            [10, 11, 1, 12, 0, 1 as StationId],
            [10, 11, 1, 12, 0, 2 as StationId]
        ]
    },
    validStations: validStations12,
    output: [
        {
            MinAlt: 10,
            MinAltMaxSig: 12,
            MinAltAgl: 11,
            SumSig: (12 >> 2) * 2,
            SumCrc: 2,
            Count: 2,
            SumGap: 0, //
            'Station:0': [{MinAlt: 10, Count: 1, MinAltMaxSig: 12, StationId: 1 as StationId}],
            NumStations: 2
        }
    ]
});

///////////////////
// Station Rollups
//////////////////

doRollupTest({
    name: 'A',
    src: {type: bufferTypes.station, inputs: [[10, 11, 1, 12, 0]]},
    dest: {type: bufferTypes.station, inputs: [[10, 11, 1, 12, 0]]},
    validStations: validStations1, //
    output: [{MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: (12 >> 2) * 2, SumCrc: 2, Count: 2, SumGap: 0, NumStations: undefined}]
});

//	update( altitude, agl, crc, signal, gap, stationid ) {
doRollupTest({
    name: 'B',
    src: {type: bufferTypes.station, inputs: [[10, 11, 1, 12, 1]]},
    dest: {type: bufferTypes.station, inputs: [[5, 10, 2, 8, 1]]},
    validStations: validStations1, //
    output: [{MinAlt: 5, MinAltMaxSig: 8, MinAltAgl: 10, SumSig: (12 >> 2) + (8 >> 2), SumCrc: 3, Count: 2, SumGap: 2, NumStations: undefined}]
});

doRollupTest({
    name: 'C',
    src: {type: bufferTypes.station, inputs: [[10, 11, 1, 12, 1]]},
    dest: {type: bufferTypes.station, inputs: [[10, 10, 2, 8, 1]]},
    validStations: validStations1, //
    output: [{MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 10, SumSig: (12 >> 2) + (8 >> 2), SumCrc: 3, Count: 2, SumGap: 2, NumStations: undefined}]
});

doRollupTest({
    name: 'D',
    src: {type: bufferTypes.station, inputs: [[10, 11, 1, 12, 1]]},
    dest: {type: bufferTypes.station, inputs: [[10, 10, 2, 24, 1]]},
    validStations: validStations1, //
    output: [{MinAlt: 10, MinAltMaxSig: 24, MinAltAgl: 10, SumSig: (12 >> 2) + (24 >> 2), SumCrc: 3, Count: 2, SumGap: 2}]
});

test('CoverageHeader Basic', () => {
    let l = new CoverageHeader(0 as StationId, 'day', 0, '87088619affffff');
    let r = new CoverageHeader(0 as StationId, 'month', 0, '87088619affffff');

    expect(l.h3).toBe('87088619affffff');
    expect(r.h3).toBe('87088619affffff');
    expect(l.accumulator).toBe('1000');
    expect(r.accumulator).toBe('3000');
    expect(l.dbKey()).not.toBe(r.dbKey());
    expect(l.lockKey).not.toBe(r.lockKey);
    expect(l.accumulator).not.toBe(r.accumulator);
    expect(l.typeName).toBe('day');
    expect(r.typeName).toBe('month');
    expect(l.bucket).toBe(r.bucket);

    // These work because no stationid(dbid) set in header
    let p = new CoverageHeader(l.lockKey);
    expect(p.h3).toBe('87088619affffff');
    expect(p.dbid).toBe(0 as StationId);
    expect(p.accumulator).toBe('1000');
    expect(l.lockKey).toBe(p.lockKey);
    expect(p.accumulator).toBe(l.accumulator);
    expect(l.h3).toBe(p.h3);
    expect(l.dbid).toBe(p.dbid);

    p = new CoverageHeader(l.dbKey());
    expect(l.lockKey).toBe(p.lockKey);
    expect(p.accumulator).toBe(l.accumulator);
    expect(l.h3).toBe(p.h3);
});

test('CoverageHeader from Buffer', () => {
    let l = new CoverageHeader(Buffer.from('1005/87088619affffff'));
    let r = new CoverageHeader(Buffer.from('0/1020/87088619affffff'));

    expect(l.h3).toBe('87088619affffff');
    expect(r.h3).toBe('87088619affffff');
    expect(l.accumulator).toBe('1005');
    expect(r.accumulator).toBe('1020');
    expect(l.dbKey()).not.toBe(r.dbKey());
    expect(l.lockKey).not.toBe(r.lockKey);
    expect(l.accumulator).not.toBe(r.accumulator);
    expect(l.typeName).toBe('day');
    expect(r.typeName).toBe('day');
    expect(l.bucket).toBe(0x05);
    expect(r.bucket).toBe(0x20);
    expect(l.isMeta).toBe(false);
    expect(r.isMeta).toBe(false);
});

test('CoverageHeader set from Buffer', () => {
    let l = new CoverageHeader('0000/00_invalid');
    let r = new CoverageHeader('0000/00_invalid');

    l.fromDbKey(Buffer.from('1005/87088619affffff'));
    r.fromDbKey(Buffer.from('0/1020/87088619affffff'));

    expect(l.h3).toBe('87088619affffff');
    expect(r.h3).not.toBe('87088619affffff'); // not valid as we passed a lockKey not a dbKey
    expect(l.h3splitlong).toStrictEqual(h3IndexToSplitLong(l.h3));
    expect(l.h3splitlong).toStrictEqual([0x9affffff, 0x8708861]);

    expect(l.typeName).toBe('day');
    expect(l.bucket).toBe(0x05);
    expect(l.isMeta).toBe(false);
});

test('CoverageHeader from Buffer - Meta', () => {
    let l = new CoverageHeader(Buffer.from('2005/00_meta'));
    let r = new CoverageHeader(Buffer.from('30a0/80aff'));

    expect(l.h3).toBe('00_meta');
    expect(r.h3).toBe('80aff');
    expect(l.isMeta).toBe(true);
    expect(r.isMeta).toBe(true);
    expect(l.accumulator).toBe('2005');
    expect(r.accumulator).toBe('30a0');
});

test('CoverageHeader from dbKey', () => {
    let l = new CoverageHeader(1 as StationId, 'day', 0, '80dbfffffffffff');
    let r = new CoverageHeader(2 as StationId, 'day', 0, '80dbfffffffffff');

    expect(l.dbKey()).toBe(r.dbKey());
    expect(l.lockKey).not.toBe(r.lockKey);
    expect(l.accumulator).toBe(r.accumulator);
    expect(l.typeName).toBe('day');

    // These work because no stationid(dbid) set in header
    let p = new CoverageHeader(l.lockKey);
    expect(l.lockKey).toBe(p.lockKey);
    expect(p.accumulator).toBe(l.accumulator);
    expect(l.h3).toBe(p.h3);
    expect(l.dbid).toBe(p.dbid);

    // DbKey doesn't include the db...
    p = new CoverageHeader(l.dbKey());
    expect(l.dbKey()).toBe(p.dbKey());
    expect(l.lockKey).not.toBe(p.lockKey);
    expect(p.accumulator).toBe(l.accumulator);
    expect(l.h3).toBe(p.h3);
    expect(l.dbid).not.toBe(p.dbid);
});

test('CoverageHeader Accessors', () => {
    let l = new CoverageHeader(1 as StationId, 'year', 2, '80dbfffffffffff');

    expect(l.dbid).toBe(1 as StationId);
    expect(l.bucket).toBe(2);
    expect(l.typeName).toBe('year');
});

// Should do the ranges etc
