
//
// This class is stored in the h3cache and used to update the h3 records
// as they are stored in ram and also ON DISK
//

import { makeTable, 
		 Utf8, Uint8, Uint16, Uint32, Uint64,
		 makeBuilder } from 'apache-arrow';


// Global mapping structure
//   HEADER

const sU32 = 4;
const sU16 = 2;
const sU8 = 1;


// field to sized offset, so u32oCount is number of u32s to Count
const station = {
	// 32 bits
	u8oVersion:  0, // current version is 0
	
	u8oMinAltMaxSig: 2, // expanded
	u8oMaxSig:       3,

	// The big accumulators
	u32oCount:   1,
	u32oSumSig:  2,
	u32oSumCrc:  3, 

	// we have had 4 * u32s at this point
	u16oMinAltAgl: 8,
	u16oMinAlt:    9,

	// How long are we in bytes (multiple of 32!)
	length: (4*sU32)+(2*sU16),
	version: 0,
}

// Slightly different structure for the global
// it includes linked list of nodes and number of unique points
const globalNestedStation = {
	...station,
	u8oNext:     1, // next in linked list, only in stations referenced from globals

	u16oStationId:   (5*sU32)/sU16,
	
	length: (6*sU32), // unused +sU16,
	version: 2,
	
	clearData(_u8,o8=0) { _u8.fill( 0, o8+this.u8oMinAltMaxSig, o8+this.length )} // remove DATA not list
}

const globalHeader = {
	...station,

	u8oHead:         1, // immediately after version

	u32oUnique:      (5*sU32)/sU32,
	
	length: (6*sU32), 
	version: 1,
	nestedVersion: globalNestedStation.version,
	nestedAllocation: globalNestedStation.length,
	
	clearData(_u8,o8=0) { _u8.fill( 0, o8+this.u8oMinAltMaxSig, o8+this.length )} // remove DATA not list
}


const bufferVersion = {
	0: station,
	1: globalHeader,
	2: globalNestedStation, // must be referred to on the containing global!
};

export const bufferTypes = {
	'station': 0,
	'global': 1,
}
	
export class BinaryRecord {

	// Create a structure to work with existing data that is stored in buffer
	// we are using TypedArrays which means we are making a view not a copy
	constructor(i) {
		if( typeof(i) == 'string' ) {
			this._initType(bufferTypes[i]);
		}
		else if( typeof(i) == 'number' ) {
			this._initType(i);
		}
		else {
			this._use(i);
		}
	}
	
	//
	// Update a record, will update based on the type detected in the version field
	// this deals with accumulating and making sure values are correct
	// lazily it doesn't deal with overflows... probably should!
	update( altitude, agl, crc, signal, stationid ) {
		this._update( 0, 0, 0, this._sh, altitude, agl, crc, signal );

		if( this._sh.nestedVersion ) {
			this._updateStationList( stationid, altitude, agl, crc, signal );
		}
	}

	// Simple helper to store the unique number of seconds that have had
	// tracking - helps to determine how good the coverage actually is
	// This is done later as we need to collect to allowed for delayed packets
	updateUnique( number ) {
		this._u32[this._sh.u32oUnique] += number;
	}

	// Get the buffer for when we need to write it back to the disk
	buffer() {
		return this._buffer;
	}

	//
	// remove a station, will roll up the data into the parent again
	removeStation(stationid) {
		this._removeStationFromList(stationid);
	}

//////////////////////////////
	
	//
	// Dump for debugging
	print(o=0) {
		const sh = bufferVersion[this._u8[0+o]]; // version always first byte
		for( const k in sh ) {
			const v = sh[k]
			if( k.match(/^u8/)) {
				console.log( o ? '  ':'', k, this._u8[o+v] );
			}
			if( k.match(/^u16/)) {
				console.log( o ? '  ':'', k, this._u16[o/sU16+v] );
			}
			if( k.match(/^u32/)) {
				console.log( o ? '  ':'', k, this._u32[o/sU32+v] );
			}
		}
		// Figure out if there is more to dump, a next of 0 means end of the list
		const next = this._u8[o+(sh.u8oNext||sh.u8oHead||0)]
		if( next ) {
			console.log( '+ @', next );
			return this.print( this._calcOffset(next) );
		}
	}


	//
	// Create an arrow that can support specified format
	static initArrow(type) {
		let arrow = {
			h3: makeBuilder({type: new Uint64()}),
			minAgl: makeBuilder({type: new Uint16()}),
			minAlt: makeBuilder({type: new Uint16()}),
			minAltSig: makeBuilder({type: new Uint8()}),
			maxSig: makeBuilder({type: new Uint8()}),
			avgSig: makeBuilder({type: new Uint8()}),
			avgCrc: makeBuilder({type: new Uint8()}),
			count: makeBuilder({type: new Uint32()}),
		}
		if( type == bufferTypes.global ) {
			arrow.stations = makeBuilder({type: new Utf8()});
		}
		return arrow;
	}
	
	//
	// Called to convert the in progress buffers to finished vectors ready for streaming
	static finalizeArrow(arrow) {
		for( const k in arrow ) {
			arrow[k] = arrow[k].finish().toVector();
		}
		return makeTable(arrow);
	}

	//
	// Add ourselves to an arrow builder
	// takes hex (without 0x) for the h3 and adds it to the cell
	// index. The assumption is that each row corresponds to a h3
	// but the row in itself actually doesn't need to track this directly
	// (because the key pointing to the row does)
	// we need to emit it for the browser so it can render data in the
	// correct place.
	appendToArrow(h3,arrow) {
		const count = this._u32[this._sh.u32oCount];
		arrow.h3?.append('0x'+h3);
		arrow.minAgl?.append(this._u16[this._sh.u16oMinAltAgl]);
		arrow.minAlt?.append(this._u16[this._sh.u16oMinAlt]);
		arrow.minAltSig?.append(this._u8[this._sh.u8oMinAltMaxSig]);
		arrow.maxSig?.append(this._u8[this._sh.u8oMaxSig]);
		arrow.avgSig?.append((this._u32[this._sh.u32oSumSig]/count)*4);
		arrow.avgCrc?.append((this._u32[this._sh.u32oSumCrc]/count)*10);
		arrow.count?.append(count);

		if( this._sh.nestedVersion ) {
			const ish = bufferVersion[this._sh.nestedVersion];
			let i = this._u8[this._sh.u8oHead], sid = 99999;
			let o = undefined;
		
			// Iterate through the list, note we have mapped
			while( i != 0 && sid != 0 ) {

				// Calculate bytes from start for this item
				const startOffset = this._calcOffset(i);
				const o8 = startOffset, o16 = startOffset/sU16, o32 = startOffset/sU32;

				// Get sid to add to list
				sid = this._u16[o16 + ish.u16oStationId];
				let scount = this._u32[o32 + ish.u32oCount];
				let percentage = Math.trunc((10*scount)/count);
				
				// emit id base16 plus percentage (percentage will only be one digit
				// at the end 0-A (0%-100%)
				o = ((o ? o+',' : '') + (((sid<<4)|(percentage&0x0f)).toString(36)));
				
				i = this._u8[o8 + ish.u8oNext];
			}
			arrow.stations.append(o||'');
		}
	}

//////////////////////////

	
	//
	// Adjust station linked list to put us in the right place and
	// update our values at the same time
	_updateStationList( stationid, altitude, agl, crc, signal ) {

		// So we can calculate offsets etc
		const ish = bufferVersion[this._sh.nestedVersion];
		let firstEmpty = 0;
		let pCount = 0;

		let i = this._u8[this._sh.u8oHead],
			iPrevNext = 0,
			oPrevNext = this._sh.u8oHead, oPrevPrevNext = 0;
		
		// Iterate through the list, note we have mapped
		while( i != 0 ) {

			// Calculate bytes from start for this item
			const startOffset = this._calcOffset(i);
			const o8 = startOffset, o16 = startOffset/sU16, o32 = startOffset/sU32;

			let sid = this._u16[ o16 + ish.u16oStationId ];
			const iNext = this._u8[o8 + ish.u8oNext]; 

			// If it isn't set then it's a hole we can use, holes are always at end of the list as they
			// have no count!
			if( sid == 0 ) {
				sid = this._u16[ o16 + ish.u16oStationId ] = stationid;
			}

			// Did we find/add our station?
			if( sid == stationid ) {

				// Update the counters
				this._update( o8, o16, o32, this._sh, altitude, agl, crc, signal );

				// now we need to see if the previous count was higher or not, if it wasn't then we switch places
				// for this to work we need to be at least second point in the list (ie have already had a previous
				// the oPrevPrevNext is set to index for global header)
				if( pCount < this._u32[ o32 + ish.u32oCount ] && oPrevPrevNext ) {

					// previous is behind us so links to the one behind us
					this._u8[ oPrevNext ] = iNext;

					// we go to the previous one (index)
					this._u8[ o8 + ish.u8oNext ] = iPrevNext;

					// One before that goes to us
					this._u8[ oPrevPrevNext ] = i;
				}

				// Done
				return;

			}
			else {
				// So we can check if we need to move it forwards or not
				pCount = this._u32[ o32 + ish.u32oCount ];
			}

			// save where we are in the linked list so we can reorder
			// after these current node is previous
			iPrevNext = i;
			oPrevPrevNext = oPrevNext;
			oPrevNext = o8 + ish.u8oNext;

			// And move to the next one
			i = iNext;
		}

		// If we exit the loop then we didn't find it and we need to allocate more
		// space and immediately add it ;)
		if( i == 0 ) {

			// We are just appending so our current length will become the starting
			// point for our new station
			const currentLength = this._buffer.byteLength;
			const o8 = currentLength, o16 = currentLength/sU16, o32 = currentLength/sU32;
			this._allocationExtension();

			// Link us into the chain
			this._u8[ o8 ] = this._sh.nestedVersion;
			this._u8[ oPrevNext ] = this._reverseOffset( currentLength );

			// And update the values
			this._update( o8, o16, o32, ish, altitude, agl, crc, signal );
			 this._u16[ o16 + ish.u16oStationId ] = stationid;

			// no need to reoder list as we are 1 record and worst the previous could be is
			// 1 record
		}
	}		



		//
	// Adjust station linked list to put us in the right place and
	// update our values at the same time
	_removeStationFromList( stationid ) {

		// So we can calculate offsets etc
		const ish = bufferVersion[this._sh.nestedVersion];
		const sh  = this._sh;
		
		let i = this._u8[this._sh.u8oHead],
			oPrevNext = this._sh.u8oHead;

		let removedIndex = 0;

		// Start by resetting the counters for our object
		this._sh.clearData(this._u8); // preserves head
		
		// Iterate through the list, note we have mapped
		while( i != 0 ) {

			// Calculate bytes from start for this item
			const startOffset = this._calcOffset(i);
			const o8 = startOffset, o16 = startOffset/sU16, o32 = startOffset/sU32;

			let sid = this._u16[ o16 + ish.u16oStationId ];
			const iNext = this._u8[o8 + ish.u8oNext]; 

			// If we are keeping this station then accumulate the values for it
			// no offset as global header at the start, sh for global, ish for nested
			if( sid != stationid ) {
				
				// Deal with lowest point and signal for that
				if( ! this._u16[sh.u16oMinAlt] || this._u16[sh.u16oMinAlt] > this._u16[o16+ish.u16oMinAlt] ) {
					this._u16[sh.u16oMinAlt] = this._u16[o16+ish.u16oMinAlt];
					
					if( this._u8[sh.u8oMinAltMaxSig] < this._u8[o8+ish.u8oMinAltMaxSig] ) {
						this._u8[sh.u8oMinAltMaxSig] = this._u8[o8+ish.u8oMinAltMaxSig];
					}
				}
				// Capture lowest AGL (note this may not be same as altitude as terain can
				// vary across the h3 index)
				if( ! this._u16[sh.u16oMinAltAgl] || this._u16[sh.u16oMinAltAgl] > this._u16[o16+ish.u16oMinAltAgl] ) {
					this._u16[sh.u16oMinAltAgl] = this._u16[o16+ish.u16oMinAltAgl];
				}
				
				if( this._u8[sh.u8oMaxSig] < this._u8[o8+ish.u8oMaxSig] ) {
					this._u8[sh.u8oMaxSig] = this._u8[o8+ish.u8oMaxSig];
				}
				
				this._u32[sh.u32oSumSig] += this._u32[o32+ish.u32oSumSig];
				this._u32[sh.u32oSumCrc] += this._u32[o32+ish.u32oSumCrc];
				this._u32[sh.u32oCount] += this._u32[o32+ish.u32oCount];
			}
			
			else {

				// We put this at the end, this is used to after the loop
				removedIndex = i;

				// previous is behind us so links to the one after us
				this._u8[ oPrevNext ] = iNext;

				// we are going to the end so nothing beyond us
				// and we want to delete our data as it's no longer important or something
				this._u8[ o8 + ish.u8oNext ] = 0;
				ish.clearData( this._u8, o8 );
			}

			// save where we are in the linked list so we can reorder
			// after these current node is previous
			oPrevNext = o8 + ish.u8oNext;

			// And move to the next one
			i = iNext;
		}

		// At the end we need to link the one we removed if we removed one
		// they will be in allocation position in the buffer, but at the end
		// of the linked list with no station id
		if( removedIndex != 0 ) {
			this._u8[ oPrevNext ] = removedIndex;
		}
	}		




////////////////////////////////////



	
	// Update a record based on specific offset, used internally
	// for updating either station or global or sub records
	_update( o8, o16, o32, sh, altitude, agl, crc, signal, count = 1 ) {
		
		// Deal with lowest point and signal for that
		if( ! this._u16[o16 + sh.u16oMinAlt] || this._u16[o16 + sh.u16oMinAlt] > altitude ) {
			this._u16[o16 + sh.u16oMinAlt] = altitude;
			
			if( this._u8[o8 + sh.u8oMinAltMaxSig] < signal ) {
				this._u8[o8 + sh.u8oMinAltMaxSig] = signal;
			}
		}
		// Capture lowest AGL (note this may not be same as altitude as terain can
		// vary across the h3 index)
		if( ! this._u16[o16 + sh.u16oMinAltAgl] || this._u16[o16 + sh.u16oMinAltAgl] > agl ) {
			this._u16[o16 + sh.u16oMinAltAgl] = agl;
		}

		if( this._u8[o8 + sh.u8oMaxSig] < signal ) {
			this._u8[o8 + sh.u8oMaxSig] = signal;
		}

		// signal was expanded and we want to shrink it back down when accumulating as it mean
		// we won't overflow till we hit 2^24 packets which is quite a few
		this._u32[o32 + sh.u32oSumSig] += signal>>2; 

		// crc is 0-10 so 4 bits or 2^26 before overflow
		this._u32[o32 + sh.u32oSumCrc] += crc;

		// We can cant to 2^32, but other values will overflow a some point before then
		// depending on average perhaps we should stop accumulating before that happens
		this._u32[o32 + sh.u32oCount] += count;
	}

	//
	// Helpers for finding offset of extensions in nested allocation
	_calcOffset(i) {return this._sh.length + ((i-1) * this._sh.nestedAllocation)}
	_reverseOffset(o) { return ((o - this._sh.length)/this._sh.nestedAllocation)+1 }

	//
	// We need to be able to add new records to the end of the record
	_allocationExtension() {
		// Allocate bigger buffer
		const n = new Uint8Array( this._buffer.byteLength + this._sh.nestedAllocation );
		n.set( this._buffer );
		this._buffer = n;
		
		// Now set the type in the header otherwise all goes wrong
		this._u8 = new Uint8Array( this._buffer.buffer, 0, this._buffer.byteLength );
		this._u16 = new Uint16Array( this._buffer.buffer, 0, this._buffer.byteLength/sU16 );
		this._u32 = new Uint32Array( this._buffer.buffer, 0, this._buffer.byteLength/sU32 );
	}

	_use(buffer) {
		this._buffer = buffer;
		
		this._u8 = new Uint8Array( buffer.buffer, 0, buffer.byteLength );
		this._sh = bufferVersion[this._u8[0]]; // version always first byte

		// Note we are allowing access to all the memory not just the stuff we mapped
		// this is for global iteration
		this._u16 = new Uint16Array( buffer.buffer, 0, buffer.byteLength/sU16 );
		this._u32 = new Uint32Array( buffer.buffer, 0, buffer.byteLength/sU32 );
	}

	// Allocate a new structure. Used when no data has been accumulated yet
	_initType(bufferType) {
		// Find the correct structure based on passed in type and allocate
		// enough space for it. If nested also alloc
		this._sh = bufferVersion[bufferType]
		
		this._buffer = new Uint8Array( this._sh.length + (this._sh?.nestedAllocation||0));

		// Now set the type in the header otherwise all goes wrong
		this._u8 = new Uint8Array( this._buffer.buffer, 0, this._buffer.byteLength );
		this._u8[0] = this._sh.version; // version always first byte
		
		this._u16 = new Uint16Array( this._buffer.buffer, 0, this._buffer.byteLength/sU16 );
		this._u32 = new Uint32Array( this._buffer.buffer, 0, this._buffer.byteLength/sU32 );

		// If we have nested then point linked list to it
		// and set the version of the extension
		if( this._sh?.nestedAllocation ) {
			this._u8[this._sh.u8oHead] = 1;
			this._u8[this._sh.length+0] = this._sh.nestedVersion;
		}
	}
	
}
