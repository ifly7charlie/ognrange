
//
// This class is used as a key for the h3cache and h3 databases and used to update the h3 records
// as they are stored in ram and also ON DISK
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
	'day': 0,
	'week': 1,
	'month': 2,
	'year': 3,
}

export const accumulatorNames = [
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
		else if( t ) {
			this._use(sid,t);
		}
		else {
			if( typeof sid == 'string' ) {
				this._initFromLockKey(sid);
			}
			else {
				this._use(0,sid); // if no sid then use that as the buffer
			}
		}
	}
	
	// Get the buffer for when we need to write it back to the disk
	get dbKey() {
		return this._u32.buffer;
	}

	// Helpers for working with the data, no setters for these because
	// the can only be set on construction (all the h3 functions can
	// handle split longs, though they all return strings)
	get h3() {
		return CoverageHeader._h3SwapOrder(this._u32[header.u32oH3lo], this._u32[header.u32oH3hi]);
	}

	get h3bigint() {
		return BigInt(this._u32[header.u32oH3hi])<<32n|BigInt(this._u32[header.u32oH3lo]);
	}

	get accumulator() {
		return Number( this._u32[header.u8oAccumulator] );
	}

	get type() {
		return (this._u32[ header.u8oAccumulator ]&0xff);
	}
	get typeName() {
		return accumulatorNames[(this._u32[ header.u8oAccumulator ]&0xff)];
	}

	get bucket() {
		return Number((this._u32[ header.u8oAccumulator ]>>16)&0xffff);
	}

	get dbid() {
		return this._dbid;
	}

	get lockKey() {
		return this._lockKey;
	}

//////////////////////////////
	
	//
	// Dump for debugging
	toString() {
		return 'db:' +this.dbid + ':' + this.typeName + '/' + this.bucket + ':' + splitLongToh3Index(...this.h3);
	}

	_initFromLockKey(k) {
		const parts = k.split('/');
		this._u32 = new Uint32Array( header.length/sU32 );
		this._dbid = parseInt(parts[0],36);

		// Now set the type in the header otherwise all goes wrong
		this._u32[0] = parseInt(parts[1],36);
		this._u32.set( h3IndexToSplitLong(parts[2]), header.u32oH3lo );
		this._lockKey = k;
	}
	
	_use(dbid,buffer) {
		this._u32 = new Uint32Array( buffer.buffer, 0, buffer.byteLength/sU32 );
		this._dbid = dbid;
		this._generateLockKey(splitLongToh3Index(...this.h3))
	}

	// Allocate a new structure. Used when no data has been accumulated yet
	_init(dbid,t,b,h) {
		this._u32 = new Uint32Array( header.length/sU32 );
		this._dbid = dbid;

		// Now set the type in the header otherwise all goes wrong
		const _u8 = new Uint8Array( this._u32.buffer, 0, header.length );
		_u8[ header.u8oAccumulator ] = accumulatorTypes[t];
		_u8.set( [ b&0xff, (b>>8)&0xff ], header.u16oAccumulatorBucket*sU16 )
		this._u32.set( CoverageHeader._h3SwapOrder(...h3IndexToSplitLong(h)), header.u32oH3lo );
		this._generateLockKey(h);
	}

	_generateLockKey(h3string) {
		this._lockKey = this._dbid.toString(36) + '/' + this._u32[0].toString(36) + '/' + h3string;
	}

	static _h3SwapOrder(a,b) {
		return [ CoverageHeader._swap32(b), CoverageHeader._swap32(a) ];
	}

	static _swap32(val) {
		return ((val & 0xFF) << 24)
			 | ((val & 0xFF00) << 8)
			 | ((val >> 8) & 0xFF00)
			 | ((val >> 24) & 0xFF);
	}
}
