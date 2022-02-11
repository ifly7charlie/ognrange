

import { BinaryRecord, bufferTypes } from '../lib/bin/binaryrecord.js';


// Run stuff magically
main()
    .then("exiting");

//
async function main() {

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

	console.log('=== global ===');
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

	console.log( '--- arrow ---' );
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
	br.appendToArrow('80dbfffffffffff',arrow);
	arrow = BinaryRecord.finalizeArrow(arrow);
	console.table( [...arrow].slice(0,100))

	

	console.log( gr.buffer().toString() );
}
