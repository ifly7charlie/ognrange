//
// This class is used as a key for the h3cache and h3 databases and used to update the h3 records
// as they are stored in ram and also ON DISK
//

//

import {H3Index} from 'h3-js';

import {prefixWithZeros} from '../common/prefixwithzeros';
import {Layer, dbKeyPrefix, layerFromPrefix} from '../common/layers';

import {H3LockKey, StationId, As} from './types';

export type AccumulatorBucket = number & As<'AccumulatorBucket'>;

export type AccumulatorTypeString = 'current' | 'day' | 'month' | 'year' | 'yearnz';
export enum AccumulatorType {
    current = 0,
    day = 1,
    //    week = 2,
    month = 3,
    year = 4,
    yearnz = 5
}

export const accumulatorTypes: Record<AccumulatorTypeString, AccumulatorType> = {
    current: AccumulatorType.current,
    day: AccumulatorType.day,
    //    week: AccumulatorType.week,
    month: AccumulatorType.month,
    year: AccumulatorType.year,
    yearnz: AccumulatorType.yearnz
};

export const accumulatorNames = ['current', 'day', 'week', 'month', 'year', 'yearnz'];

export type AccumulatorTypeAndBucket = number & As<'AccumulatorTypeAndBucket'>;

//
export function formAccumulator(t: AccumulatorType, b: AccumulatorBucket): string {
    const tb = (((t & 0x0f) << 12) | (b & 0x0fff)) as AccumulatorTypeAndBucket;
    return prefixWithZeros(4, tb.toString(16));
}

export class CoverageHeader {
    _h3: H3Index;
    _tb: AccumulatorTypeAndBucket;
    _dbid: StationId;
    _lockKey: string;
    _layer: Layer;

    // Create a structure to work with existing data that is stored in buffer
    // we are using TypedArrays which means we are making a view not a copy
    // accepts either a buffer (for read from db)
    // or accumulatorType (string from accumulatorTypes), accumulatorBucket (16bit), h3key (hexstring)
    constructor(lockKey: string | Buffer);
    constructor(sidOrKey: StationId, t: AccumulatorTypeString | AccumulatorType, b: AccumulatorBucket, h: H3Index, layer?: Layer);
    constructor(sidOrKey: StationId | string | Buffer, t?: AccumulatorTypeString | AccumulatorType, b?: AccumulatorBucket, h?: H3Index, layer?: Layer) {
        // Explicit init types
        if (typeof sidOrKey == 'number') {
            if (typeof b == 'undefined' || typeof t == 'undefined' || typeof h == 'undefined') {
                throw new Error('invalid CoverageHeader construction');
            }
            this._dbid = sidOrKey;
            this._layer = layer ?? Layer.COMBINED;
            if (typeof t == 'string') {
                this._tb = (((accumulatorTypes[t] & 0x0f) << 12) | (b & 0x0fff)) as AccumulatorTypeAndBucket;
            } else {
                this._tb = (((t & 0x0f) << 12) | (b & 0x0fff)) as AccumulatorTypeAndBucket;
            }
            this._h3 = h;
            this._lockKey = this._dbid.toString(36) + '/' + dbKeyPrefix(this._layer) + prefixWithZeros(4, this._tb.toString(16)) + '/' + h;
        } else {
            // Initialise from a lockKey or dbKey
            const s = sidOrKey.toString('latin1');

            // Detect layer prefix: single letter followed by '/'
            // Check if this is a layer-prefixed key or a legacy unprefixed key
            const parsed = CoverageHeader._parseKeyString(s);
            this._layer = parsed.layer;
            this._dbid = parsed.dbid;
            this._tb = parsed.tb;
            this._h3 = parsed.h3;
            this._lockKey = parsed.lockKey;
        }
    }

    // Parse a key string (lockKey or dbKey) and extract all fields
    private static _parseKeyString(s: string): {layer: Layer; dbid: StationId; tb: AccumulatorTypeAndBucket; h3: H3Index; lockKey: string} {
        // dbKey formats:
        //   Legacy:  "1042/h3..."  (4 hex / 15 char h3 = 20 chars)
        //   New:     "c/1042/h3..." (2 + 4 + 1 + 15 = 22 chars)
        // lockKey formats:
        //   Legacy:  "dbid36/1042/h3..." or "0/1042/h3..."
        //   New:     "dbid36/c/1042/h3..." or "0/c/1042/h3..."

        // Layer-prefixed dbKey: "c/1042/h3..."
        const layerAtZero = layerFromPrefix(s[0]);
        if (layerAtZero && s[1] === '/') {
            // Check it's not a lockKey starting with a base36 digit that happens to be a layer letter
            // dbKeys don't have a second '/' after position 6 (c/1042/h3...), so if there's a '/' in the first char
            // and the next 4 chars parse as hex, treat it as a layer-prefixed dbKey
            const hexPart = s.slice(2, 6);
            if (/^[0-9a-f]{4}$/.test(hexPart) && s[6] === '/') {
                return {
                    layer: layerAtZero,
                    dbid: 0 as StationId,
                    tb: parseInt(hexPart, 16) as AccumulatorTypeAndBucket,
                    h3: s.slice(7) as H3Index,
                    lockKey: '0/' + s
                };
            }
        }

        // Legacy short-form dbKey: "1042/h3..." (length <= 20)
        if ((s.length || 30) <= 20) {
            return {
                layer: Layer.COMBINED,
                dbid: 0 as StationId,
                tb: parseInt(s.slice(0, 4), 16) as AccumulatorTypeAndBucket,
                h3: s.slice(5) as H3Index,
                lockKey: '0/' + s
            };
        }

        // lockKey format: "dbid/[layer/]accumulator/h3"
        const firstSlash = s.indexOf('/');

        // Check for layer prefix after dbid: "dbid/c/1042/h3..."
        if (firstSlash >= 0) {
            const afterSlash = firstSlash + 1;
            const layerAfterDbid = layerFromPrefix(s[afterSlash]);
            if (layerAfterDbid && s[afterSlash + 1] === '/') {
                const afterLayer = afterSlash + 2;
                return {
                    layer: layerAfterDbid,
                    dbid: parseInt(s.slice(0, firstSlash), 36) as StationId,
                    tb: parseInt(s.slice(afterLayer, afterLayer + 4), 16) as AccumulatorTypeAndBucket,
                    h3: s.slice(afterLayer + 5) as H3Index,
                    lockKey: s
                };
            }
        }

        // Legacy lockKey: "dbid/1042/h3..."
        return {
            layer: Layer.COMBINED,
            dbid: parseInt(s.slice(0, -21), 36) as StationId,
            tb: parseInt(s.slice(-20, -16), 16) as AccumulatorTypeAndBucket,
            h3: s.slice(-15) as H3Index,
            lockKey: s
        };
    }

    // Get the key string for when we need to write it back to the disk
    dbKey() {
        return dbKeyPrefix(this._layer) + prefixWithZeros(4, this._tb.toString(16)) + '/' + this._h3;
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

    get h3splitlong(): [number, number] {
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

    get layer(): Layer {
        return this._layer;
    }

    get isMeta() {
        const prefix = this._h3.slice(0, 2);
        return prefix == '80' || prefix == '00';
    }

    // Easy swap of bucket
    getAccumulatorForBucket(t: AccumulatorType, b: AccumulatorBucket) {
        return new CoverageHeader(this._dbid, t, b, this._h3, this._layer);
    }

    // Return the levelup query structure for iterating over an aggregation block
    static getDbSearchRangeForAccumulator(t: AccumulatorType | AccumulatorTypeString, b: AccumulatorBucket, includeMeta = false, layer: Layer = Layer.COMBINED) {
        return {gte: CoverageHeader.getAccumulatorBegin(t, b, includeMeta, layer), lt: CoverageHeader.getAccumulatorEnd(t, b, layer)};
    }
    // Return the levelup query structure for iterating over an aggregation block
    static getAccumulatorEnd(t: AccumulatorType | AccumulatorTypeString, b: AccumulatorBucket, layer: Layer = Layer.COMBINED) {
        const h = new CoverageHeader(0 as StationId, t, b, '9000000000000000', layer); // everything, will always be 8
        return h.dbKey();
    }
    // Return the levelup query structure for iterating over an aggregation block
    static getAccumulatorBegin(t: AccumulatorType | AccumulatorTypeString, b: AccumulatorBucket, includeMeta = false, layer: Layer = Layer.COMBINED) {
        const h = new CoverageHeader(0 as StationId, t, b, includeMeta ? '00_' : '8000000000000000', layer); // everything, will always be 8
        return h.dbKey();
    }

    static getAccumulatorMeta(t: AccumulatorTypeString | AccumulatorType, b: AccumulatorBucket, layer: Layer = Layer.COMBINED) {
        const h = new CoverageHeader(0 as StationId, t, b, '00_meta', layer); // should be out of the begin/end search range for accumulator
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
        // Detect layer prefix: a known layer letter followed by '/'
        // In a legacy dbKey, s[1] is always a hex char (part of 4-digit accumulator), never '/'
        const layerAtZero = s[1] === '/' ? layerFromPrefix(s[0]) : null;
        if (layerAtZero) {
            this._layer = layerAtZero;
            this._tb = parseInt(s.slice(2, 6), 16) as AccumulatorTypeAndBucket;
            this._h3 = s.slice(7);
        } else {
            this._layer = Layer.COMBINED;
            this._tb = parseInt(s.slice(0, 4), 16) as AccumulatorTypeAndBucket;
            this._h3 = s.slice(5);
        }
        this._lockKey = '0/' + s;
    }
}
