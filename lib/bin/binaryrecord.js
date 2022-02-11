
//
// This class is stored in the h3cache and used to update the h3 records
// as they are stored in ram and also ON DISK
//

import { makeTable, 
		 Utf8, Uint8, Uint16, Uint32, Uint64,
		 makeBuilder } from 'apache-arrow';


import _sortby from 'lodash.sortby';

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
	clearData(_u8,o8=0) { _u8.fill( 0, o8+this.u8oMinAltMaxSig, o8+this.length )} // remove DATA not list
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

class ExtensionAllocationOptions {
	constructor(type,length) {
		this.type = type;
		this.length = length;
	}
};
	
export class BinaryRecord {

	// Create a structure to work with existing data that is stored in buffer
	// we are using TypedArrays which means we are making a view not a copy
	constructor(i) {
		if( i instanceof ExtensionAllocationOptions ) {
			this._initType(i.type,i.length);
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
			let i = this._u8[this._sh.u8oHead], sid = 99999;
			let o = undefined;
		
			// Iterate through the list, note we have mapped
			while( i != 0 && sid != 0 ) {

				// Calculate bytes from start for this item
				const startOffset = this._calcOffset(i);
				const o8 = startOffset, o16 = startOffset/sU16, o32 = startOffset/sU32;

				// Get sid to add to list
				sid = this._u16[o16 + this._ish.u16oStationId];
				let scount = this._u32[o32 + this._ish.u32oCount];
				let percentage = Math.trunc((10*scount)/count);
				
				// emit id base16 plus percentage (percentage will only be one digit
				// at the end 0-A (0%-100%)
				o = ((o ? o+',' : '') + (((sid<<4)|(percentage&0x0f)).toString(36)));
				
				i = this._u8[o8 + this._ish.u8oNext];
			}
			arrow.stations.append(o||'');
		}
	}

	//
	// The last action we need to be able to do is fold our data into another
	// BinaryRecord to allow aggregation. In this situation we are read only
	// the destination will be updated with our value and any stations no longer
	// listed will be removed from it
	//
	// NOTE: this can return a new BinaryRecord which replaces both src and dest!
	//
	// Also note that once you have completed rolling up then you should discard
	// the old records!
	rollup( /*BinaryRecord*/ srcRecord ) {

		// If we do not have sub stations then this is super easy we just
		// need to add the new values to our own and we are done
		if( ! this._sh.nestedVersion ) {
			if( srcRecord._sh.nestedVersion ) {
				throw( 'BinaryRecord: mixing global and station records...');
			}
			BinaryRecord._updateNested( this, 0, srcRecord, 0 );
			return this;
		}

		// Iterate through linked list for a BinaryRecord and produce a map
		// of stationid:index
		function makeStationMap( br ) { 
			const ish = br._ish;
			let stations = {};

			// pass 0 get list of all the existing stations
			for( let i = br._u8[br._sh.u8oHead]; i != 0; ) {

				// Calculate bytes from start for this item
				const startOffset = br._calcOffset(i);
				const o8 = startOffset, o16 = startOffset/sU16, o32 = startOffset/sU32
				const sid = br._u16[o16 + ish.u16oStationId];

				if( sid ) {
					stations[sid] = { 'id': sid,
									  'i': i,
									  'c': br._u32[o32+ish.u32oCount] };
				}

				// Move to next one
				i = br._u8[o8 + ish.u8oNext]; 
			}
			return stations;
		}

		const destStations = makeStationMap( this );
		const srcStations = makeStationMap( srcRecord );

		// After this we will have two maps but no duplicate stations
		// so we will take from one or the other. anything left in srcStations i
		// only there. Anything in destStations also has the si key for the
		// source index
		for ( const k in destStations ) {
			if( ! srcStations[k] ) { delete destStations[k] }
			else { destStations[k].c += srcStations[k].c; destStations[k].di = destStations[k].i; destStations[k].si = srcStations[k].i; destStations[k].i = undefined; delete srcStations[k] }
		}

		// Sort the pick list by size so we are optimal on writing but also, if there is nothing
		// to pick then we should delete the record so return null indicating nothing found
		const pickList = _sortby([...Object.values(destStations),...Object.values(srcStations)], (o)=>-o.c);
		if( ! pickList.length ) {
			return null;
		}

		// now we can allocate a new structure to fill we make it big enough for everything
		// that will be left over
		let newBr = new BinaryRecord( new ExtensionAllocationOptions( this._sh.version, pickList.length ));

		// Then we simply iterate through these picking and combining
		// we have allocated enough space and they are in order so next is always just +1 except at end when 0
		newBr._u8[newBr._sh.u8oHead] = 1;

		let i = 1;
		let remaining = pickList.length-1;

		for ( const pick of pickList ) {
			console.log( pick, i, remaining );

			const destOffset = newBr._calcOffset(i);
			const do8 = destOffset, do16 = destOffset/sU16, do32 = destOffset/sU32;

			// header records
			newBr._u16[ do16 + newBr._ish.u16oStationId ] = pick.id;
			newBr._u8[ do8 + newBr._ish.u8oNext ] = remaining ? i+1 : 0;
			newBr._u8[ do8 + newBr._ish.u8oVersion ] = newBr._ish.version;

			// now we need to merge which is easy
			// record will have a di if we have a dest index and either i or si for
			// source index, and we are writing into slot i
			if( pick.di ) {
				BinaryRecord._updateNested( newBr, i, this, pick.di );
			}
			if( pick.si || pick.i ) {
				BinaryRecord._updateNested( newBr, i, srcRecord, pick.si || pick.i );
			}
			// And then roll those up to the top level
			BinaryRecord._updateNested( newBr, 0, newBr, i );

			// adjust our position in the output
			i++; remaining--;
		}

		// We have made a new record which is what you should use going forwards..
		return newBr;
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

		// Start by resetting the counters for our global object
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
				
				// Update the global with the station
				BinaryRecord._updateNested( this, 0, this, i );
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
			this._u8[o8 + sh.u8oMinAltMaxSig] = signal;
		}
		else if( this._u16[o16 + sh.u16oMinAlt] == altitude ) {
			this._u8[o8 + sh.u8oMinAltMaxSig] = Math.max(this._u8[o8 + sh.u8oMinAltMaxSig],signal);
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
	
	// Generic rollup and update function updates our object from passed source
	static _updateNested (dest,di,src,si) {

		// dest can be parent or the station
		const destOffset = di ? dest._calcOffset(di) : 0;
		const dsh = di ? dest._ish : dest._sh;
		const do8 = destOffset, do16 = destOffset/sU16, do32 = destOffset/sU32;
		
		// Where we are reading from
		const srcOffset = si ? src._calcOffset(si) : 0;
		const ssh = si ? src._ish : src._sh;
		const so8 = srcOffset, so16 = srcOffset/sU16, so32 = srcOffset/sU32;
		
		// Deal with lowest point and signal for that, if we drop the point then we drop the signal
		if( ! dest._u16[do16+dsh.u16oMinAlt] || dest._u16[do16+dsh.u16oMinAlt] > src._u16[so16+ssh.u16oMinAlt] ) {
			dest._u16[do16+dsh.u16oMinAlt] = src._u16[so16+ssh.u16oMinAlt];
			dest._u8[do8+dsh.u8oMinAltMaxSig] = src._u8[so8+ssh.u8oMinAltMaxSig];
		}
		else if( dest._u16[do16+dsh.u16oMinAlt] == src._u16[so16+ssh.u16oMinAlt] ) {
			dest._u8[do8+dsh.u8oMinAltMaxSig] = Math.max(dest._u8[do8+dsh.u8oMinAltMaxSig],src._u8[so8+ssh.u8oMinAltMaxSig]);
		}
		// Capture lowest AGL (note this may not be same as altitude as terain can
		// vary across the h3 index)
		if( ! dest._u16[do16+dsh.u16oMinAltAgl] || dest._u16[do16+dsh.u16oMinAltAgl] > src._u16[so16+ssh.u16oMinAltAgl] ) {
			dest._u16[do16+dsh.u16oMinAltAgl] = src._u16[so16+ssh.u16oMinAltAgl];
		}
		
		dest._u8[do8+dsh.u8oMaxSig] = Math.max( dest._u8[do8+dsh.u8oMaxSig], src._u8[so8+ssh.u8oMaxSig] )
		
		dest._u32[do32+dsh.u32oSumSig] += src._u32[so32+ssh.u32oSumSig];
		dest._u32[do32+dsh.u32oSumCrc] += src._u32[so32+ssh.u32oSumCrc];
		dest._u32[do32+dsh.u32oCount] += src._u32[so32+ssh.u32oCount];
	}
			

	//
	// Helpers for finding offset of extensions in nested allocation
	_calcOffset(i) {return this._sh.length + ((i-1) * this._sh.nestedAllocation)}
	_reverseOffset(o) { return ((o - this._sh.length)/this._sh.nestedAllocation)+1 }

	//
	// We need to be able to add new records to the end of the record
	_allocationExtension(length) {
		// Allocate bigger buffer
		const n = new Uint8Array( this._buffer.byteLength + (length||this._sh.nestedAllocation) );
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
		this._ish = bufferVersion[this._sh.nestedVersion];

		// Note we are allowing access to all the memory not just the stuff we mapped
		// this is for global iteration
		this._u16 = new Uint16Array( buffer.buffer, 0, buffer.byteLength/sU16 );
		this._u32 = new Uint32Array( buffer.buffer, 0, buffer.byteLength/sU32 );
	}

	// Allocate a new structure. Used when no data has been accumulated yet
	_initType(bufferType,extensionItems=1) {
		// Find the correct structure based on passed in type and allocate
		// enough space for it. If nested also alloc
		this._sh = bufferVersion[bufferType]
		this._ish = bufferVersion[this._sh.nestedVersion];
		
		this._buffer = new Uint8Array( this._sh.length + ((this._sh?.nestedAllocation||0)*extensionItems));

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
