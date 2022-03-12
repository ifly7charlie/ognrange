

import { CoverageRecord, bufferTypes } from './coveragerecord.js';
import { CoverageHeader, accumulatorTypes } from './coverageheader.js';

import _sortby from 'lodash.sortby'

//

import test from 'ava';


const validStations1 = new Set();
validStations1.add( 1 );

const validStations2 = new Set();
validStations2.add( 2 );

const validStations12 = new Set();
validStations12.add( 1 );
validStations12.add( 2 );

let cr = null;

const typeNames = {
	0: 'station',
	1: 'global'
}

function doCRTest( { name, type, inputs, output } ) {
	// Set one row in the database and flush
	test( typeNames[type] +': '+ name+' update', async (t) => {
		cr = new CoverageRecord( type );
		inputs.forEach( (r) => cr.update( ...r ) );
		const o = cr.toObject();
		for (const [key, value] of Object.entries(output)) {
			const [station,pos] = key.split(':')||[null,null];
			if( station && pos ) {
				for (const [keyn, valuen] of Object.entries(value)) {
					t.is( o.stations[pos][keyn], valuen, `stations[${pos}].${keyn} == ${valuen} ${JSON.stringify(o)}` );
				}
			}
			else {
				t.is( o[key], value, `[${key}] == ${value} ${JSON.stringify(o)}` );
			}
		}
	});
}

function doRollupTest( { name, src, dest, validStations, output } ) {
	// Set one row in the database and flush
	test( (src?(typeNames[src.type]+ '/'):'removeInvalidStations/')  + typeNames[dest.type] +': '+ name+' rollup', async (t) => {
		let srccr = new CoverageRecord( src?.type||0 );
		let destcr = new CoverageRecord( dest.type );
		src?.inputs.forEach( (r) => srccr.update( ...r ) );
		dest.inputs.forEach( (r) => destcr.update( ...r ) );
		const out = src ? destcr.rollup( srccr, validStations ) : destcr.removeInvalidStations(validStations);
		if( output.invalid == true ) {
			t.is( out, null );
		}
		else {
			t.is( out != null, true )
		}
		const o = out.toObject();
		for (const [key, value] of Object.entries(output)) {
			const [station,pos] = key.split(':')||[null,null];
			if( station && pos ) {
				t.is( pos >= 0, true, 'no position specified in test' );
				t.is( !! o.stations, true, 'no stations in result object' );
				for (const [keyn, valuen] of Object.entries(value)) {
					t.is( !!o.stations[pos], true, 'no station found for position' );
					t.is( o.stations[pos][keyn], valuen, `stations[${pos}].${keyn} == ${valuen} ${JSON.stringify(o)}` );
				}
			}
			else {
				t.is( o[key], value, `[${key}] == ${value} ${JSON.stringify(o)}` );
			}
		}
	});
}

// Check all basic permutations
//	update( altitude, agl, crc, signal, gap, stationid ) {
doCRTest( { name: 'one entry',
			type: bufferTypes.station,
			inputs: [[ 10, 11, 1, 12, 0 ]],
			output: { MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: 12>>2, SumCrc: 1, Count: 1, SumGap: 0, NumStations: undefined }});

doCRTest( { name: 'A two updates',
			type: bufferTypes.station,
			inputs: [[ 10, 11, 1, 12, 0 ], [10, 11, 1, 12, 0 ]],
			output: { MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: (12>>2)*2, SumCrc: 2, Count: 2, SumGap: 0, NumStations: undefined }});

doCRTest( { name: 'B two different updates',
			type: bufferTypes.station,
			inputs: [[ 10, 10, 10, 10, 10 ], [10, 10, 10, 10, 10 ]],
			output: { MinAlt: 10, MinAltMaxSig: 10, MinAltAgl: 10, SumSig: (10>>2)*2, SumCrc: 20, Count: 2, SumGap: 20, NumStations: undefined }});

doCRTest( { name: 'C two more different updates',
			type: bufferTypes.station,
			inputs: [[ 10, 10, 10, 10, 10 ], [1, 12, 0, 20, 0 ]],
			output: { MinAlt: 1, MinAltMaxSig: 20, MinAltAgl: 10, SumSig: (10>>2)+(20>>2), SumCrc: 10, Count: 2, SumGap: 10, NumStations: undefined }});

doCRTest( { name: 'D two more different updates',
			type: bufferTypes.station,
			inputs: [[ 10, 10, 10, 10, 10 ], [12, 1, 9, 20, 0 ]],
			output: { MinAlt: 10, MinAltMaxSig: 10, MinAltAgl: 1, SumSig: (10>>2)+(20>>2), SumCrc: 19, Count: 2, SumGap: 10, NumStations: undefined }});


///
// Next test for global records (ie with substations)
doCRTest( { name: 'A',
			type: bufferTypes.global,
			inputs: [[ 10, 11, 1, 12, 0, 1 ]],
			output: { MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: 12>>2, SumCrc: 1, Count: 1, SumGap: 0, NumStations: 1 }});

doCRTest( { name: 'B',
			type: bufferTypes.global,
			inputs: [[ 10, 11, 1, 12, 0, 1 ]],
			output: { MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: 12>>2, SumCrc: 1, Count: 1, SumGap: 0,
					  'Station:0': { MinAlt: 10, Count:1, MinAltMaxSig: 12 },
					  NumStations: 1 }});

//	update( altitude, agl, crc, signal, gap, stationid ) {
doCRTest( { name: 'two from one',
			type: bufferTypes.global,
			inputs: [[ 10, 11, 1, 12, 0, 1 ],[ 10, 11, 1, 12, 0, 1 ]],
			output: { MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: (12>>2)*2, SumCrc: 2, Count: 2, SumGap: 0,
					  'Station:0': { MinAlt: 10, Count:2, MinAltMaxSig: 12, StationId: 1 },
					  NumStations: 1 }});

doCRTest( { name: 'one from each of two',
			type: bufferTypes.global,
			inputs: [[ 10, 11, 1, 12, 0, 1 ],[ 10, 11, 1, 12, 0, 2 ]],
			output: { MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: (12>>2)*2, SumCrc: 2, Count: 2, SumGap: 0,
					  'Station:0': { MinAlt: 10, Count:1, MinAltMaxSig: 12, StationId: 1 },
					  'Station:1': { MinAlt: 10, Count:1, MinAltMaxSig: 12, StationId: 2  },
					  NumStations: 2 }});

doCRTest( { name: 'two from one station, one from another',
			type: bufferTypes.global,
			inputs: [[ 10, 11, 1, 12, 0, 1 ],[ 10, 11, 1, 12, 0, 1 ],[ 10, 11, 1, 12, 0, 2 ]],
			output: { MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: (12>>2)*3, SumCrc: 3, Count: 3, SumGap: 0,
					  'Station:0': { MinAlt: 10, Count:2, MinAltMaxSig: 12, StationId: 1 },
					  'Station:1': { MinAlt: 10, Count:1, MinAltMaxSig: 12, StationId: 2  },
					  NumStations: 2 }});

doCRTest( { name: 'two from one station, one from another, sort order change',
			type: bufferTypes.global,
			inputs: [[ 10, 11, 1, 12, 0, 1 ],[ 10, 11, 1, 12, 0, 2 ],[ 10, 11, 1, 12, 0, 2 ]],
			output: { MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: (12>>2)*3, SumCrc: 3, Count: 3, SumGap: 0,
					  'Station:0': { MinAlt: 10, Count:2, MinAltMaxSig: 12, StationId: 2 },
					  'Station:1': { MinAlt: 10, Count:1, MinAltMaxSig: 12, StationId: 1  },
					  NumStations: 2 }});

doCRTest( { name: 'two from one station, one from another, sort order stable',
			type: bufferTypes.global,
			inputs: [[ 10, 11, 1, 12, 0, 1 ],[ 10, 11, 1, 12, 0, 2 ],[ 10, 11, 1, 12, 0, 2 ],[ 10, 11, 1, 12, 0, 1 ]],
			output: { 
					  'Station:0': { Count:2, StationId: 2 },
					  'Station:1': { Count:2, StationId: 1  },
					  NumStations: 2 }});

doCRTest( { name: 'two from one station, one from another, sort order stable, add third',
			type: bufferTypes.global,
			inputs: [[ 10, 11, 1, 12, 0, 1 ],[ 10, 11, 1, 12, 0, 2 ],[ 10, 11, 1, 12, 0, 2 ],[ 10, 11, 1, 12, 0, 1 ],[10,11,1,12,0,3]],
			output: { 
					  'Station:0': { Count:2, StationId: 2 },
					  'Station:1': { Count:2, StationId: 1  },
					  'Station:2': { Count:1, StationId: 3  },
					  NumStations: 3 }});



doRollupTest( { name: 'A',
				src: { type: bufferTypes.global,
					   inputs: [[ 10, 11, 1, 12, 0, 1 ]]},
				dest: { type: bufferTypes.global,
						inputs: [[ 10, 11, 1, 12, 0, 1 ]]},
				validStations: validStations1,
				output: { MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: (12>>2)*2, SumCrc: 2, Count: 2, SumGap: 0,
						  'Station:0': { MinAlt: 10, Count:2, MinAltMaxSig: 12, StationId: 1 },
						  NumStations: 1 }});

doRollupTest( { name: 'B',
				src: { type: bufferTypes.global,
					   inputs: [[ 10, 11, 1, 12, 0, 1 ],[ 10, 11, 1, 12, 0, 2 ]]},
				dest: { type: bufferTypes.global,
						inputs: [[ 10, 11, 1, 12, 0, 2 ]]},
				validStations: validStations12,
				output: { MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: (12>>2)*3, SumCrc: 3, Count: 3, SumGap: 0,
						  'Station:0': { MinAlt: 10, Count:2, MinAltMaxSig: 12, StationId: 2 },
						  'Station:1': { MinAlt: 10, Count:1, MinAltMaxSig: 12, StationId: 1  },
						  NumStations: 2 }});


// Order doesn't matter (1/2)
doRollupTest( { name: 'C',
				src: { type: bufferTypes.global,
					   inputs: [[ 10, 11, 1, 12, 0, 1 ],[ 10, 11, 1, 12, 0, 2 ],[ 10, 11, 1, 12, 0, 2 ]]},
				dest: { type: bufferTypes.global,
						inputs: []},
				validStations: validStations12,
				output: { MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: (12>>2)*3, SumCrc: 3, Count: 3, SumGap: 0,
						  'Station:0': { MinAlt: 10, Count:2, MinAltMaxSig: 12, StationId: 2 },
						  'Station:1': { MinAlt: 10, Count:1, MinAltMaxSig: 12, StationId: 1  },
						  NumStations: 2 }});


doRollupTest( { name: 'D',
				src: { type: bufferTypes.global,
					   inputs: []},
				dest: { type: bufferTypes.global,
						inputs: [[ 10, 11, 1, 12, 0, 1 ],[ 10, 11, 1, 12, 0, 2 ],[ 10, 11, 1, 12, 0, 2 ]]},
				validStations: validStations12,
				output: { MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: (12>>2)*3, SumCrc: 3, Count: 3, SumGap: 0,
						  'Station:0': { MinAlt: 10, Count:2, MinAltMaxSig: 12, StationId: 2 },
						  'Station:1': { MinAlt: 10, Count:1, MinAltMaxSig: 12, StationId: 1  },
						  NumStations: 2 }});

// Drop a station (3) in rollup (replacing it with identical row for simplicty of test)
// this makes empty plus whole new one, note sort order is not stable on rollup
// when stations have same count so don't check that!
doRollupTest( { name: 'E',
				src: { type: bufferTypes.global,
					   inputs: [[ 10, 11, 1, 12, 0, 1 ],[ 10, 11, 1, 12, 0, 2 ],[ 10, 11, 1, 12, 0, 2 ],[ 10, 11, 1, 12, 0, 1 ]]},
				dest: { type: bufferTypes.global,
						inputs: [[ 10, 11, 1, 12, 0, 3 ]]},
				validStations: validStations12,
				output: { MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: (12>>2)*4, SumCrc: 4, Count: 4, SumGap: 0,
						  'Station:0': { MinAlt: 10, Count:2, MinAltMaxSig: 12 },
						  'Station:1': { MinAlt: 10, Count:2, MinAltMaxSig: 12  },
						  NumStations: 2 }});

doRollupTest( { name: 'F',
				src: { type: bufferTypes.global,
					   inputs: [[ 10, 11, 1, 12, 0, 1 ],[ 10, 11, 1, 12, 0, 2 ],[ 10, 11, 1, 12, 0, 2 ],[ 10, 11, 1, 12, 0, 1 ],[ 10, 11, 1, 12, 0, 1 ]]},
				dest: { type: bufferTypes.global,
						inputs: [[ 10, 11, 1, 12, 0, 3 ]]},
				validStations: validStations12,
				output: { SumCrc: 5, Count: 5, SumGap: 0,
						  'Station:0': { MinAlt: 10, Count:3, MinAltMaxSig: 12, StationId:1 },
						  'Station:1': { MinAlt: 10, Count:2, MinAltMaxSig: 12, StationId:2  },
						  NumStations: 2 }});


//
// Not specifying dest means remove
doRollupTest( { name: 'A',
				dest: { type: bufferTypes.global,
						inputs: [[ 10, 11, 1, 12, 0, 1 ],[ 10, 11, 1, 12, 0, 1 ]]},
				validStations: validStations1,
				output: { MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: (12>>2)*2, SumCrc: 2, Count: 2, SumGap: 0,
						  'Station:0': { MinAlt: 10, Count:2, MinAltMaxSig: 12, StationId: 1 },
						  NumStations: 1 }});

//	update( altitude, agl, crc, signal, gap, stationid ) {
doRollupTest( { name: 'B',
				dest: { type: bufferTypes.global,
						inputs: [[ 10, 11, 1, 12, 0, 1 ],[ 9, 10, 2, 20, 1, 2 ]]},
				validStations: validStations1,
				output: { MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: (12>>2), SumCrc: 1, Count: 1, SumGap: 0,
						  'Station:0': { MinAlt: 10, Count:1, MinAltMaxSig: 12, StationId: 1 },
						  NumStations: 1 }});

doRollupTest( { name: 'C',
				dest: { type: bufferTypes.global,
						inputs: [[ 10, 11, 1, 12, 0, 1 ],[ 9, 10, 2, 20, 1, 2 ]]},
				validStations: validStations2,
				output: { MinAlt: 9, MinAltMaxSig: 20, MinAltAgl: 10, SumSig: (20>>2), SumCrc: 2, Count: 1, SumGap: 1,
						  'Station:0': { MinAlt: 9, Count:1, MinAltMaxSig: 20, StationId: 2 },
						  NumStations: 1 }});

doRollupTest( { name: 'D',
				dest: { type: bufferTypes.global,
						inputs: [[ 10, 11, 1, 12, 0, 1 ],[ 10, 11, 1, 12, 0, 2 ]]},
				validStations: validStations12,
				output: { MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: (12>>2)*2, SumCrc: 2, Count: 2, SumGap: 0,
						  'Station:0': { MinAlt: 10, Count:1, MinAltMaxSig: 12, StationId: 1 },
						  NumStations: 2 }});


///////////////////
// Station Rollups
//////////////////

doRollupTest( { name: 'A',
				src: { type: bufferTypes.station,
					   inputs: [[ 10, 11, 1, 12, 0 ]]},
				dest: { type: bufferTypes.station,
						inputs: [[ 10, 11, 1, 12, 0 ]]},
				validStations: validStations1,
				output: { MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 11, SumSig: (12>>2)*2, SumCrc: 2, Count: 2, SumGap: 0,
						  NumStations: undefined }});

//	update( altitude, agl, crc, signal, gap, stationid ) {
doRollupTest( { name: 'B',
				src: { type: bufferTypes.station,
					   inputs: [[ 10, 11, 1, 12, 1 ]]},
				dest: { type: bufferTypes.station,
						inputs: [[ 5, 10, 2, 8, 1 ]]},
				validStations: validStations1,
				output: { MinAlt: 5, MinAltMaxSig: 8, MinAltAgl: 10, SumSig: (12>>2)+(8>>2), SumCrc: 3, Count: 2, SumGap: 2,
						  NumStations: undefined }});

doRollupTest( { name: 'C',
				src: { type: bufferTypes.station,
					   inputs: [[ 10, 11, 1, 12, 1 ]]},
				dest: { type: bufferTypes.station,
						inputs: [[ 10, 10, 2, 8, 1 ]]},
				validStations: validStations1,
				output: { MinAlt: 10, MinAltMaxSig: 12, MinAltAgl: 10, SumSig: (12>>2)+(8>>2), SumCrc: 3, Count: 2, SumGap: 2,
						  NumStations: undefined }});

doRollupTest( { name: 'D',
				src: { type: bufferTypes.station,
					   inputs: [[ 10, 11, 1, 12, 1 ]]},
				dest: { type: bufferTypes.station,
						inputs: [[ 10, 10, 2, 24, 1 ]]},
				validStations: validStations1,
				output: { MinAlt: 10, MinAltMaxSig: 24, MinAltAgl: 10, SumSig: (12>>2)+(24>>2), SumCrc: 3, Count: 2, SumGap: 2 }});



test( 'CoverageHeader Basic', (t) => {
	let l = new CoverageHeader( 0, 'day', 0,  '87088619affffff' );
	let r = new CoverageHeader( 0, 'week', 0, '87088619affffff' );

	t.is( l.h3, '87088619affffff' );
	t.is( r.h3, '87088619affffff' );
	t.is( l.accumulator, '1000' );
	t.is( r.accumulator, '2000' );
	t.not( l.dbKey(), r.dbKey() );
	t.not( l.lockKey, r.lockKey );
	t.not( l.accumulator, r.accumulator );
	t.is( l.typeName, 'day' );
	t.is( r.typeName, 'week' );
	t.is( l.bucket, r.bucket );

	// These work because no stationid(dbid) set in header
	let p = new CoverageHeader( l.lockKey );
	t.is( p.h3, '87088619affffff' );
	t.is( p.dbid, 0 );
	t.is( p.accumulator, '1000' );
	t.is( l.lockKey, p.lockKey );
	t.is( p.accumulator, l.accumulator );
	t.is( l.h3, p.h3 );
	t.is( l.dbid, p.dbid );

	p = new CoverageHeader( l.dbKey() );
	t.is( l.lockKey, p.lockKey );
	t.is( p.accumulator, l.accumulator );
	t.is( l.h3, p.h3 );
});

test( 'CoverageHeader from Buffer', (t) => {
	let l = new CoverageHeader( Buffer.from('1005/87088619affffff'));
	let r = new CoverageHeader( Buffer.from('0/1020/87088619affffff'));

	t.is( l.h3, '87088619affffff' );
	t.is( r.h3, '87088619affffff' );
	t.is( l.accumulator, '1005' );
	t.is( r.accumulator, '1020' );
	t.not( l.dbKey(), r.dbKey() );
	t.not( l.lockKey, r.lockKey );
	t.not( l.accumulator, r.accumulator );
	t.is( l.typeName, 'day' );
	t.is( r.typeName, 'day' );
	t.is( l.bucket, 0x05 );
	t.is( r.bucket, 0x20 );
	t.is( l.isMeta, false );
	t.is( r.isMeta, false );
});

test( 'CoverageHeader from Buffer - Meta', (t) => {
	let l = new CoverageHeader( Buffer.from('2005/00_meta'));
	let r = new CoverageHeader( Buffer.from('30a0/80aff'));

	t.is( l.h3, '00_meta' );
	t.is( r.h3, '80aff' );
	t.is( l.isMeta, true );
	t.is( r.isMeta, true );
	t.is( l.accumulator, '2005' );
	t.is( r.accumulator, '30a0' );
});

test( 'CoverageHeader from dbKey', (t) => {
	let l = new CoverageHeader( 1, 'day', 0, '80dbfffffffffff' );
	let r = new CoverageHeader( 2, 'day', 0, '80dbfffffffffff' );

	t.is( l.dbKey(), r.dbKey() );
	t.not( l.lockKey, r.lockKey );
	t.is( l.accumulator, r.accumulator );
	t.is( l.typeName, 'day' );

	// These work because no stationid(dbid) set in header
	let p = new CoverageHeader( l.lockKey );
	t.is( l.lockKey, p.lockKey );
	t.is( p.accumulator, l.accumulator );
	t.is( l.h3, p.h3 );
	t.is( l.dbid, p.dbid );

	// DbKey doesn't include the db...
	p = new CoverageHeader( l.dbKey() );
	t.is( l.dbKey(), p.dbKey() );
	t.not( l.lockKey, p.lockKey );
	t.is( p.accumulator, l.accumulator );
	t.is( l.h3, p.h3 );
	t.not( l.dbid, p.dbid );
});

test( 'CoverageHeader Accessors', (t) => {
	let l = new CoverageHeader( 1, 'year', 2, '80dbfffffffffff' );

	t.is( l.dbid, 1 );
	t.is( l.bucket, 2 );
	t.is( l.typeName, 'year' );
});

// Should do the ranges etc
