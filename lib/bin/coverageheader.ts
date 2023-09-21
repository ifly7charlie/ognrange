//
// This class is used as a key for the h3cache and h3 databases and used to update the h3 records
// as they are stored in ram and also ON DISK
//

//

import {H3Index} from 'h3-js';

import {prefixWithZeros} from '../common/prefixwithzeros';

import {H3LockKey, StationId, As} from './types';

export type AccumulatorBucket = number & As<'AccumulatorBucket'>;

export type AccumulatorTypeString = 'current' | 'day' | 'month' | 'year';
export enum AccumulatorType {
    current = 0,
    day = 1,
    //    week = 2,
    month = 3,
    year = 4
}

export const accumulatorTypes: Record<AccumulatorTypeString, AccumulatorType> = {
    current: AccumulatorType.current,
    day: AccumulatorType.day,
    //    week: AccumulatorType.week,
    month: AccumulatorType.month,
    year: AccumulatorType.year
};

export const accumulatorNames = ['current', 'day', 'week', 'month', 'year'];

export type AccumulatorTypeAndBucket = number & As<'AccumulatorTypeAndBucket'>;

export class CoverageHeader {
    _h3: H3Index;
    _tb: AccumulatorTypeAndBucket;
    _dbid: StationId;
    _lockKey: string;

    // Create a structure to work with existing data that is stored in buffer
    // we are using TypedArrays which means we are making a view not a copy
    // accepts either a buffer (for read from db)
    // or accumulatorType (string from accumulatorTypes), accumulatorBucket (16bit), h3key (hexstring)
    constructor(lockKey: string | Buffer);
    constructor(sidOrKey: StationId, t: AccumulatorTypeString | AccumulatorType, b: AccumulatorBucket, h: H3Index);
    constructor(sidOrKey: StationId | string | Buffer, t?: AccumulatorTypeString | AccumulatorType, b?: AccumulatorBucket, h?: H3Index) {
        // Explicit init types
        if (typeof sidOrKey == 'number') {
            if (typeof b == 'undefined' || typeof t == 'undefined' || typeof h == 'undefined') {
                throw new Error('invalid CoverageHeader construction');
            }
            this._dbid = sidOrKey;
            if (typeof t == 'string') {
                this._tb = (((accumulatorTypes[t] & 0x0f) << 12) | (b & 0x0fff)) as AccumulatorTypeAndBucket;
            } else {
                this._tb = (((t & 0x0f) << 12) | (b & 0x0fff)) as AccumulatorTypeAndBucket;
            }
            this._h3 = h;
            this._lockKey = this._dbid.toString(36) + '/' + prefixWithZeros(4, this._tb.toString(16)) + '/' + h;
        } else {
            // Initialise from a lockKey
            const s = sidOrKey.toString('latin1');
            if ((s.length || 30) <= 20) {
                this._dbid = 0 as StationId;
                this._tb = parseInt(s.slice(0, 4), 16) as AccumulatorTypeAndBucket;
                this._h3 = s.slice(5);
                this._lockKey = '0/' + s;
            } else {
                this._lockKey = s;
                this._dbid = parseInt(this._lockKey.slice(0, -21), 36) as StationId;
                this._tb = parseInt(this._lockKey.slice(-20, -16), 16) as AccumulatorTypeAndBucket;
                this._h3 = this._lockKey.slice(-15);
            }
        }
    }

    // Get the buffer for when we need to write it back to the disk
    dbKey() {
        return prefixWithZeros(4, this._tb.toString(16)) + '/' + this._h3;
    }

    // Helpers for working with the data, no setters for these because
    // the can only be set on construction (all the h3 functions can
    // handle split longs, though they all return strings)
    get h3(): H3Index {
        return this._h3;
    }

    get h3bigint(): BigInt {
        let h = this.h3splitlong;
        return (BigInt(h[1]) << BigInt(32)) | BigInt(h[0]);
    }

    get h3splitlong() {
        return [parseInt(this._h3.slice(-8), 16), parseInt(this._h3.slice(0, -8), 16)];
    }

    get accumulator() {
        return prefixWithZeros(4, this._tb.toString(16));
    }

    get type(): AccumulatorType {
        return (this._tb >> 12) & 0x0f;
    }
    get typeName(): AccumulatorTypeString {
        return accumulatorNames[(this._tb >> 12) & 0x0f] as AccumulatorTypeString;
    }

    get bucket(): AccumulatorBucket {
        return Number(this._tb & 0x0fff) as AccumulatorBucket;
    }

    get dbid() {
        return this._dbid;
    }

    get lockKey(): H3LockKey {
        return this._lockKey as H3LockKey;
    }

    get isMeta() {
        const prefix = this._h3.slice(0, 2);
        return prefix == '80' || prefix == '00';
    }

    // Easy swap of bucket
    getAccumulatorForBucket(t: AccumulatorType, b: AccumulatorBucket) {
        return new CoverageHeader(this._dbid, t, b, this._h3);
    }

    // Return the levelup query structure for iterating over an aggregation block
    static getDbSearchRangeForAccumulator(t: AccumulatorType | AccumulatorTypeString, b: AccumulatorBucket, includeMeta = false) {
        return {gte: CoverageHeader.getAccumulatorBegin(t, b, includeMeta), lt: CoverageHeader.getAccumulatorEnd(t, b)};
    }
    // Return the levelup query structure for iterating over an aggregation block
    static getAccumulatorEnd(t: AccumulatorType | AccumulatorTypeString, b: AccumulatorBucket) {
        const h = new CoverageHeader(0 as StationId, t, b, '9000000000000000'); // everything, will always be 8
        return h.dbKey();
    }
    // Return the levelup query structure for iterating over an aggregation block
    static getAccumulatorBegin(t: AccumulatorType | AccumulatorTypeString, b: AccumulatorBucket, includeMeta = false) {
        const h = new CoverageHeader(0 as StationId, t, b, includeMeta ? '00_' : '8000000000000000'); // everything, will always be 8
        return h.dbKey();
    }

    static getAccumulatorMeta(t: AccumulatorTypeString | AccumulatorType, b: AccumulatorBucket) {
        const h = new CoverageHeader(0 as StationId, t, b, '00_meta'); // should be out of the begin/end search range for accumulator
        return h;
    }

    // Compares in db order so by bytes, yes please make this better I'm tired
    static compareH3(a: CoverageHeader, b: CoverageHeader) {
        return a._h3 < b._h3 ? -1 : a._h3 > b._h3 ? 1 : 0;
    }

    //////////////////////////////

    //
    // Dump for debugging
    toString() {
        return this._lockKey;
    }

    fromDbKey(k: string | Buffer) {
        const s = k.toString('latin1');
        this._dbid = 0 as StationId;
        this._tb = parseInt(s.slice(0, 4), 16) as AccumulatorTypeAndBucket;
        this._h3 = s.slice(5);
        this._lockKey = '0/' + s;
    }
}
