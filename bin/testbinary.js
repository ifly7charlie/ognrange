

import { CoverageRecord, bufferTypes } from '../lib/bin/coveragerecord.js';
import { CoverageHeader, accumulatorTypes } from '../lib/bin/coverageheader.js';

import _sortby from 'lodash.sortby'

// Run stuff magically
main()
    .then("exiting");

//
async function main() {

	if(0)
    {
        let br = new CoverageRecord( bufferTypes.station );

        br.update( 10, 11, 1, 12, 0 );
        br.update( 10, 11, 1, 12, 0 )
        br.update( 20, 21, 22, 23, 24 );

        br.print();


        let xr = new CoverageRecord( br.buffer() );
        console.log('--');
        xr.print();
        xr.update( 1, 1, 1, 1, 0 );
        console.log('--');
        xr.print();
    }

	if(0)
    {
        console.log('=== global rollup ===');
        let gr = new CoverageRecord( bufferTypes.global );
        gr.update( 1, 2, 3, 4, 1);
        gr.update( 1, 2, 3, 4, 1);
        gr.print();
        console.log('-- second station');
        gr.update( 2, 4, 6, 8, 2);
        gr.print();
        console.log('-- remove first station');
        gr.removeStation(1);
        gr.print();
        console.log('-- remove second station');
        gr.removeStation(2);
        gr.print();
        console.log('-- rollup empty to empty');
        let gr2 = new CoverageRecord( bufferTypes.global );
        let r = gr2.rollup( gr );
        if( r != null ) {
            console.log( 'FAIL: expecting empty return' );
        }
        else {
            console.log( 'pass - null return' );
        }
        console.log('-- rollup one to empty');
        gr.update(1,2,3,4,1);
        r = gr2.rollup(gr);
        r.print();
        console.log('-- rollup one plus same station');
        let r2 = r.rollup(gr);
        r2.print();
        console.log('-- rollup one plus remove station');
        gr.removeStation(1);
        r = r2.rollup(gr);
        if( r != null ) {
            console.log( 'FAIL: expecting empty return' );
        }
        else {
            console.log( 'pass - null return' );
        }
        console.log('-- rollup one plus add 1, rollup add another, remove 1 rollup. should have {2,3} left');
		console.log("----" );
        gr.update(1,2,3,4,1);
        gr.update(1,2,3,4,2);
        r = r2.rollup(gr);
        gr.update(1,2,3,4,3);
        r2 = r.rollup(gr);
        gr.removeStation(1);
        r = r2.rollup(gr);
        r.print();
		console.log("!!!" );
	}
	if(0)
	{
        console.log('=================== rollup switch' );
		let gr = new CoverageRecord( bufferTypes.global );
        let gr2 = new CoverageRecord( bufferTypes.global );
        gr.update(1,2,3,4,1);
        gr2.update(10,20,30,40,2);
        let r = gr.rollup(gr2);
        r.print();
		console.log( '<==> (order dependent) should be opposite output' );
        r = gr2.rollup(gr);
        r.print();

		console.log( '-- remove station (should print count2 for station1)' );
		r.update(100,200,300,400,3);
		r.update(10,20,30,40,1);
		let s = new Set(); s.add(1); s.add(2);
		r = r.removeInvalidStations(s)
		r.print();
		
		console.log( '-- remove station (should print count2 for station1, count1 for station3)' );
		r.update(100,200,300,22,3);
        r.update(10,20,30,40,5);
		r.update(10,20,30,40,1);
		s.add(3);
		r = r.removeInvalidStations(s)
		s.delete(3);
		r = r.removeInvalidStations(s)
		r.print();
    }

	if(0)
	{
        console.log('=== station rollup ===');
		{
			let gr = new CoverageRecord( bufferTypes.station );
			gr.update( 10, 11, 1, 12 );
			gr.update( 10, 10, 2, 8 ) // note minagl lower not min alt so no impact on minaltmaxsig
			let br = new CoverageRecord( bufferTypes.station );
			br.update( 20, 21, 22, 23 );
			let r = br.rollup(gr);
			r.print()
		}
		console.log( '<==> (order dependent) should NOTbe same output' );
		// note global rollups are NOT order independent but station
		// ones should be
		{
			let gr = new CoverageRecord( bufferTypes.station );
			gr.update( 10, 11, 1, 12 );
			gr.update( 10, 10, 2, 8 )
			let br = new CoverageRecord( bufferTypes.station );
			br.update( 20, 21, 22, 23 );
			let r = gr.rollup(br);
			r.print()
		}

	}

	if( 0 )
	{
		console.log( '--- arrow ---' );
        let gr = new CoverageRecord( bufferTypes.global );
		gr.update( 1, 2, 3, 4, 1);
		gr.update( 1, 2, 3, 4, 1);
		gr.update( 2, 4, 6, 8, 2);
		gr.update( 2, 4, 6, 8, 150);
		gr.update( 2, 4, 6, 8, 1948);
		gr.update( 2, 4, 6, 8, 5948);
		
		let arrow = CoverageRecord.initArrow( bufferTypes.global );
		gr.appendToArrow('80dbfffffffffff',arrow);
		gr.appendToArrow('1',arrow);
		arrow = CoverageRecord.finalizeArrow(arrow);
		console.table( [...arrow].slice(0,100))
		
		console.log( '-- station record' );
		arrow = CoverageRecord.initArrow( bufferTypes.station );
		let br = new CoverageRecord( bufferTypes.station );
		br.update( 20, 21, 22, 23 );
		br.appendToArrow('80dbeffffffffff',arrow);
		arrow = CoverageRecord.finalizeArrow(arrow);
		console.table( [...arrow].slice(0,100))
		
		console.log( gr.buffer().toString() );
	}

	{
		let h = new CoverageHeader( 0, 'day', 0, '80dbfffffffffff' );
		console.log( h.toString() );
//		console.log( h.dbKey );
		h = new CoverageHeader( 0, 'day', 1, '80dbfffffffffff' );
		console.log( h.toString() );
//		console.log( h.dbKey );
		h = new CoverageHeader( 0, 'week', 51, '80dbfffffffffff' );
		console.log( h.toString() );
//		console.log( h.dbKey );

		let l = new CoverageHeader( h.lockKey );
//		console.log( l.dbKey );
		console.log( h.lockKey, l.lockKey );
		console.log( h.toString(), l.toString() );
	}
		if( 0 ) {
		let r = CoverageHeader.getDbSearchRangeForAccumulator('week',1);
		console.log( new CoverageHeader(r.gte).toString(), new CoverageHeader(r.lt).toString());
		console.log( r.lt )
		console.log( h.dbKey );
	}
}
