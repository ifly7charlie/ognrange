
//
// This class is used as a key for the h3cache and h3 databases and used to update the h3 records
// as they are stored in ram and also ON DISK
//

//

import { splitLongToh3Index, h3IndexToSplitLong } from 'h3-js';

// Global mapping structure
//   HEADER

const sU32 = 4;
const sU16 = 2;
const sU8 = 1;


// field to sized offset, so u32oCount is number of u32s to Count
const header = {

	u8oAccumulator:      0, // converted base36 (so 36 types available)
	u16oAccumulatorBucket: 1, 

	// where the h3 is stored
	u32oH3lo:   1,
	u32oH3hi:  2,

	// How long are we in bytes (multiple of 32!)
	length: (3*sU32),
	version: 0,
}

export const accumulatorTypes = {
	'current': 0,
	'day': 1,
	'week': 2,
	'month': 3,
	'year': 4,
}

export const accumulatorNames = [
	'current',
	'day',
	'week',
	'month',
	'year',
]

export class CoverageHeader {

	// Create a structure to work with existing data that is stored in buffer
	// we are using TypedArrays which means we are making a view not a copy
	// accepts either a buffer (for read from db)
	// or accumulatorType (string from accumulatorTypes), accumulatorBucket (16bit), h3key (hexstring)
	constructor(sid,t,b,h) {
		if( h ) {
			this._init(sid,t,b,h)
		}
		else {
			this._initFromLockKey(sid);
		}

	}
	
	// Get the buffer for when we need to write it back to the disk
	dbKey() {
		return this._tb.toString(36) + '/' + h3string;
	}

	// Helpers for working with the data, no setters for these because
	// the can only be set on construction (all the h3 functions can
	// handle split longs, though they all return strings)
	get h3() {
		return this._h3;
	}

	get h3bigint() {
		let h = h3IndexToSplitLong(this._h3);
		return BigInt(h[0])<<32n|BigInt(h[1]);
	}

	get accumulator() {
		return Number( this._tb );
	}

	get type() {
		return ((this._tb>>24)&0xff);
	}
	get typeName() {
		return accumulatorNames[((this._tb>>24)&0xff)];
	}

	get bucket() {
		return Number(this._tb&0xffff);
	}

	get dbid() {
		return this._dbid;
	}

	get lockKey() {
		return this._lockKey;
	}

	// Return the levelup query structure for iterating over an aggregation block
	static getDbSearchRangeForAccumulator(t,b) {
		const l = new CoverageHeader(0,t,b,'8000000000000000'); // first byte controls
		const h = new CoverageHeader(0,t,b,'9000000000000000'); // everything, will always be 8
		return { gte: l.lockKey, lt: h.lockKey };
	}

	// Compares in db order so by bytes, yes please make this better I'm tired
	static compare(a,b) {
		return a._lockKey < b._lockKey ? -1 :
							a._lockKey > b._lockKey ? 1 : 0
	}

	// Generate a new accumulator with 
	static newAccumulator(ch,t,b) {
		return new CoverageHeader(this._dbid,this._tb>>24,this._tb&0xffff,this._h3);
	}

//////////////////////////////
	
	//
	// Dump for debugging
	toString() {
		return this._lockKey;
	}

	_initFromLockKey(k) {
		this._lockKey = ''+k;
		const parts = this._lockKey.split('/');
		this._dbid = parseInt(parts[0],36);

		// Now set the type in the header otherwise all goes wrong
		this._tb = parseInt(parts[1],36);
		this._h3 = parts[2];
	}
	
	// Allocate a new structure. Used when no data has been accumulated yet
	_init(dbid,t,b,h) {
		this._dbid = dbid;
		this._tb = accumulatorTypes[t]<<24 | b&0xffff;
		this._h3 = h;
		this._generateLockKey(h);
	}

	_generateLockKey(h3string) {
		this._lockKey = this._dbid.toString(36) + '/' + this._tb.toString(36) + '/' + h3string;
	}

}
