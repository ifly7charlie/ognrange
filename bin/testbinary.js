

import { BinaryRecord, bufferTypes } from '../lib/bin/binaryrecord.js';

import _sortby from 'lodash.sortby'

// Run stuff magically
main()
    .then("exiting");

//
async function main() {

	if(1)
    {
        let br = new BinaryRecord( bufferTypes.station );

        br.update( 10, 11, 1, 12, 0 );
        br.update( 10, 11, 1, 12, 0 )
        br.update( 20, 21, 22, 23, 24 );

        br.print();


        let xr = new BinaryRecord( br.buffer() );
        console.log('--');
        xr.print();
        xr.update( 1, 1, 1, 1, 0 );
        console.log('--');
        xr.print();
    }

	if(1)
    {
        console.log('=== global rollup ===');
        let gr = new BinaryRecord( bufferTypes.global );
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
        let gr2 = new BinaryRecord( bufferTypes.global );
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
        gr.update(1,2,3,4,1);
        gr.update(1,2,3,4,2);
        r = r2.rollup(gr);
        gr.update(1,2,3,4,3);
        r2 = r.rollup(gr);
        gr.removeStation(1);
        r = r2.rollup(gr);
        r.print();

        console.log('-- rollup switch' );
        gr = new BinaryRecord( bufferTypes.global );
        gr2 = new BinaryRecord( bufferTypes.global );
        gr.update(1,2,3,4,1);
        gr2.update(10,20,30,40,2);
        r = gr.rollup(gr2);
        r.print();
    }

	{
        console.log('=== station rollup ===');
		{
			let gr = new BinaryRecord( bufferTypes.station );
			gr.update( 10, 11, 1, 12 );
			gr.update( 10, 10, 2, 8 ) // note minagl lower not min alt so no impact on minaltmaxsig
			let br = new BinaryRecord( bufferTypes.station );
			br.update( 20, 21, 22, 23 );
			let r = br.rollup(gr);
			r.print()
		}
		console.log( '<==> (order independent) should be same output' );
		// note global rollups are NOT order independent but station
		// ones should be
		{
			let gr = new BinaryRecord( bufferTypes.station );
			gr.update( 10, 11, 1, 12 );
			gr.update( 10, 10, 2, 8 )
			let br = new BinaryRecord( bufferTypes.station );
			br.update( 20, 21, 22, 23 );
			let r = gr.rollup(br);
			r.print()
		}

	}

	if( 1 )
	{
		console.log( '--- arrow ---' );
        let gr = new BinaryRecord( bufferTypes.global );
		gr.update( 1, 2, 3, 4, 1);
		gr.update( 1, 2, 3, 4, 1);
		gr.update( 2, 4, 6, 8, 2);
		gr.update( 2, 4, 6, 8, 150);
		gr.update( 2, 4, 6, 8, 1948);
		gr.update( 2, 4, 6, 8, 5948);
		
		let arrow = BinaryRecord.initArrow( bufferTypes.global );
		gr.appendToArrow('80dbfffffffffff',arrow);
		gr.appendToArrow('1',arrow);
		arrow = BinaryRecord.finalizeArrow(arrow);
		console.table( [...arrow].slice(0,100))
		
		console.log( '-- station record' );
		arrow = BinaryRecord.initArrow( bufferTypes.station );
		let br = new BinaryRecord( bufferTypes.station );
		br.update( 20, 21, 22, 23 );
		br.appendToArrow('80dbfffffffffff',arrow);
		arrow = BinaryRecord.finalizeArrow(arrow);
		console.table( [...arrow].slice(0,100))
		
		console.log( gr.buffer().toString() );
	}
}
