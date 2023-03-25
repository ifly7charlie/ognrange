//
// This class is stored in the h3cache and used to update the h3 records
// as they are stored in ram and also ON DISK
//

import {Utf8, Uint8, Uint16, Uint32, makeBuilder, makeTable, RecordBatchWriter, Uint8Builder, Utf8Builder, Uint16Builder, Uint32Builder} from 'apache-arrow/Arrow.node';
import {TypedArray} from 'apache-arrow/interfaces';

import {createWriteStream} from 'fs';
import {PassThrough} from 'stream';

import {CoverageHeader} from './coverageheader';

import {createGzip} from 'node:zlib';

import {sortBy as _sortby} from 'lodash';

import {UNCOMPRESSED_ARROW_FILES} from '../common/config';

import {StationId} from './types';

// Global mapping structure
//   HEADER

const sU32 = 4;
const sU16 = 2;
const sU8 = 1;

export enum bufferTypes {
    unknown = 9999,
    station = 0,
    global = 1,
    globalNested = 2
}

export const bufferTypeNames = ['station', 'global'];
export type bufferTypeString = 'unknown' | 'station' | 'global' | 'globalNested';

class CRBase {
    // first 32 bits
    u8oVersion: 0 = 0; // current version is 0
    version: bufferTypes = -1;

    u8oMinAltMaxSig: 2 = 2; // expanded
    u8oMaxSig: 3 = 3;

    // The big accumulators
    u32oCount: 1 = 1;
    u32oSumSig: 2 = 2;
    u32oSumCrc: 3 = 3;
    u32oSumGap: 4 = 4;

    // we have had 5 * u32s at this point
    u16oMinAltAgl: 10 = 10;
    u16oMinAlt: 11 = 11;

    nestedVersion = 0;
}

class CRStation extends CRBase {
    // first 32 bits

    // How long are we in bytes (multiple of 32!)
    static len: number = 5 * sU32 + 2 * sU16;
    length: number = CRStation.len;
    version: bufferTypes.station = bufferTypes.station;
    clearData(_u8: Uint8Array, o8 = 0): void {
        _u8.fill(0, o8 + this.u8oMinAltMaxSig, o8 + CRStation.len);
    } // remove DATA not list
}
const station: CRStation = new CRStation();

// Slightly different structure for the global
// it includes linked list of nodes and number of unique points
class CRGlobalNestedStation extends CRBase {
    u8oNext: 1 = 1; // next in linked list, only in stations referenced from globals

    u16oStationId = CRStation.len / sU16;

    static len = CRStation.len + sU32; // u16oStationId + unused sU16,
    length = CRGlobalNestedStation.len;
    version: bufferTypes.globalNested = bufferTypes.globalNested;

    clearData(_u8: Uint8Array, o8 = 0) {
        _u8.fill(0, o8 + this.u8oMinAltMaxSig, o8 + CRGlobalNestedStation.len);
    } // remove DATA not list
}
const globalNestedStation: CRGlobalNestedStation = new CRGlobalNestedStation();

class CRGlobalHeader extends CRBase {
    u8oHead: 1 = 1; // immediately after version

    static len = CRStation.len;
    length = CRGlobalHeader.len;
    version: bufferTypes.global = bufferTypes.global;
    nestedVersion = globalNestedStation.version;

    clearData(_u8: Uint8Array, o8 = 0) {
        _u8.fill(0, o8 + this.u8oMinAltMaxSig, o8 + CRGlobalHeader.len);
    } // remove DATA not list
}
const globalHeader: CRGlobalHeader = new CRGlobalHeader();

type primaryVersionTypes = CRGlobalHeader | CRStation;
type nestedVersionTypes = CRGlobalNestedStation;

//
const primaryBufferVersions: {[P in bufferTypes | number]?: primaryVersionTypes} = {
    0: station,
    1: globalHeader
};

const nestedBufferVersions: {[P in bufferTypes | number]?: nestedVersionTypes} = {
    2: globalNestedStation
};

const allBufferVersions: Record<bufferTypes, nestedVersionTypes | primaryVersionTypes> = {
    9999: station,
    0: station,
    1: globalHeader,
    2: globalNestedStation
};

interface ArrowType {
    readonly [index: string]: Uint8Builder | Uint16Builder | Uint32Builder | Utf8Builder | undefined;
    h3lo: Uint32Builder;
    h3hi: Uint32Builder;
    minAgl: Uint16Builder;
    minAlt: Uint16Builder;
    minAltSig: Uint8Builder;
    maxSig: Uint8Builder;
    avgSig: Uint8Builder;
    avgCrc: Uint8Builder;
    count: Uint32Builder;
    avgGap: Uint8Builder;
    stations?: Utf8Builder;
    expectedGap?: Uint8Builder;
    numStations?: Uint8Builder;
}

class ExtensionAllocationOptions {
    type: bufferTypes;
    length: number;

    constructor(type: bufferTypes, length: number) {
        this.type = type;
        this.length = length;
    }
}

export type CoverageRecordOut = Record<string, number | undefined | CoverageRecordOut[]>;
type ValidMapKeys = 'di' | 'si' | 'i';
interface StationMap {
    id: StationId; // stationid
    c: number; // count
    si?: number | undefined;
    di?: number | undefined;
    i?: number | undefined;
}

function superThrow(t: string): never {
    throw new Error(t);
}
export class CoverageRecord {
    _sh: primaryVersionTypes;
    _ish: nestedVersionTypes | undefined;
    _u32: Uint32Array;
    _u16: Uint16Array;
    _u8: Uint8Array;
    _buffer: Uint8Array;

    // Create a structure to work with existing data that is stored in buffer
    // we are using TypedArrays which means we are making a view not a copy
    constructor(i: ExtensionAllocationOptions | bufferTypes | Uint8Array) {
        if (i instanceof Uint8Array) {
            this._buffer = i;

            this._u8 = new Uint8Array(i.buffer, 0, i.byteLength);

            this._sh = primaryBufferVersions[this._u8[0]] || superThrow(`unknown bufferType ${this._u8[0]}`);
            this._ish = this._sh.nestedVersion ? nestedBufferVersions[this._sh.nestedVersion] : undefined;

            // Note we are allowing access to all the memory not just the stuff we mapped
            // this is for global iteration
            this._u16 = new Uint16Array(i.buffer, 0, i.byteLength / sU16);
            this._u32 = new Uint32Array(i.buffer, 0, i.byteLength / sU32);
        } else {
            const iIsObject = typeof i == 'object';
            const bufferType = iIsObject ? i.type : i;
            const extensionItems = iIsObject ? i.length : 1;

            // Find the correct structure based on passed in type and allocate
            // enough space for it. If nested also alloc
            this._sh = primaryBufferVersions[bufferType] || superThrow(`unknown bufferType ${bufferType}`);
            this._ish = this._sh.nestedVersion ? nestedBufferVersions[this._sh.nestedVersion] : undefined;

            this._buffer = this._u8 = new Uint8Array(this._sh.length + (this._ish?.length || 0) * extensionItems);

            // Now set the type in the header otherwise all goes wrong
            this._u8[0] = this._sh.version; // version always first byte

            this._u16 = new Uint16Array(this._buffer.buffer, 0, this._buffer.byteLength / sU16);
            this._u32 = new Uint32Array(this._buffer.buffer, 0, this._buffer.byteLength / sU32);

            // If we have nested then point linked list to it
            // and set the version of the extension
            if (this._ish) {
                this._u8[(this._sh as typeof globalHeader).u8oHead] = 1;
                this._u8[this._sh.length + 0] = (this._sh as typeof globalHeader).nestedVersion;
            }
        }
    }

    //
    // Update a record, will update based on the type detected in the version field
    // this deals with accumulating and making sure values are correct
    // lazily it doesn't deal with overflows... probably should!
    update(...args: [number, number, number, number, number] | [number, number, number, number, number, StationId]): void; // for testing
    update(altitude: number, agl: number, crc: number, signal: number, gap: number): void;
    update(altitude: number, agl: number, crc: number, signal: number, gap: number, stationid?: StationId): void {
        this._update(0, 0, 0, this._sh, altitude, agl, crc, signal, gap);
        if (this._ish && stationid !== undefined) {
            this._updateStationList(stationid, altitude, agl, crc, signal, gap);
        }
    }

    // Simple helper to store the unique number of seconds that have had
    // tracking - helps to determine how good the coverage actually is
    // This is done later as we need to collect to allowed for delayed packets
    updateSumGap(gap: number, stationid: StationId) {
        this._u32[this._sh.u32oSumGap] += gap;

        if (this._ish && stationid) {
            let i = this._u8[(this._sh as typeof globalHeader).u8oHead];

            // Iterate through the list, note we have mapped
            while (i != 0) {
                // Calculate bytes from start for this item
                const startOffset = this._calcOffset(i);
                const o8 = startOffset,
                    o16 = startOffset / sU16,
                    o32 = startOffset / sU32;

                // Check if matching station if there is then we need to update, and then
                // we are done
                const sid = this._u16[o16 + this._ish.u16oStationId];
                if (sid == stationid) {
                    this._u32[o32 + this._ish.u32oSumGap] += gap;
                    return;
                }
                i = this._u8[o8 + this._ish.u8oNext];
            }
            // theoretically we should have a use case here for missing station in
            // list but sum requested for it..
            console.log('!!! missing station in global when updating SumGap', stationid);
        }
    }

    // Get the buffer for when we need to write it back to the disk
    buffer() {
        return this._buffer;
    }

    //////////////////////////////

    //
    // Dump for debugging
    toObject(o = 0): CoverageRecordOut {
        const output: CoverageRecordOut = {};
        console.log('u80:', new Error('' + this._u8[0 + o]), o, this._u8);
        const sh = allBufferVersions[this._u8[0 + o] as bufferTypes] ?? superThrow('nope'); // version always first byte, this works nested rather than _ish/_sh
        console.log(sh);
        for (const k in Object.keys(sh)) {
            const v = Object(sh)[k];
            if (k.match(/^u8/)) {
                output[k.slice(3)] = this._u8[o + v];
            }
            if (k.match(/^u16/)) {
                output[k.slice(4)] = this._u16[o / sU16 + v];
            }
            if (k.match(/^u32/)) {
                output[k.slice(4)] = this._u32[o / sU32 + v];
            }
        }
        if (sh instanceof CRGlobalHeader && this._ish) {
            let i = this._u8[sh.u8oHead];
            output['stations'] = [];

            // Iterate through the list, note we have mapped
            while (i != 0) {
                const o8 = this._calcOffset(i);
                console.log('next offset:' + o8);
                output['stations'].push(this.toObject(o8));
                i = this._u8[o8 + this._ish.u8oNext];
            }
            output['NumStations'] = output['stations'].length;
        }
        return output;
    }

    //
    // Create an arrow that can support specified format
    static initArrow(type: bufferTypes) {
        let arrow: ArrowType = {
            h3lo: makeBuilder({type: new Uint32()}),
            h3hi: makeBuilder({type: new Uint32()}),
            minAgl: makeBuilder({type: new Uint16()}),
            minAlt: makeBuilder({type: new Uint16()}),
            minAltSig: makeBuilder({type: new Uint8()}),
            maxSig: makeBuilder({type: new Uint8()}),
            avgSig: makeBuilder({type: new Uint8()}),
            avgCrc: makeBuilder({type: new Uint8()}),
            count: makeBuilder({type: new Uint32()}),
            avgGap: makeBuilder({type: new Uint8()})
        };
        if (type == bufferTypes.global) {
            arrow.stations = makeBuilder({type: new Utf8()});
            arrow.expectedGap = makeBuilder({type: new Uint8()});
            arrow.numStations = makeBuilder({type: new Uint8()});
        }
        return arrow;
    }

    //
    // Called to convert the in progress buffers to finished vectors ready for streaming
    static finalizeArrow(arrow: ArrowType, fileName: string) {
        const output: Record<string, TypedArray> = {};
        for (const k in arrow) {
            const outputArray = arrow[k]?.finish()?.toVector()?.toArray();
            if (outputArray) {
                output[k] = outputArray;
            }
        }
        const outputTable = makeTable(output);

        if (UNCOMPRESSED_ARROW_FILES) {
            const pt = new PassThrough({objectMode: true});
            pt.pipe(RecordBatchWriter.throughNode()).pipe(createWriteStream(fileName));
            pt.write(outputTable);
            pt.end();
        }

        {
            const pt = new PassThrough({objectMode: true});
            pt.pipe(RecordBatchWriter.throughNode())
                .pipe(createGzip())
                .pipe(createWriteStream(fileName + '.gz'));
            pt.write(outputTable);
            pt.end();
        }
    }

    //
    // Add ourselves to an arrow builder
    // takes hex (without 0x) for the h3 and adds it to the cell
    // index. The assumption is that each row corresponds to a h3
    // but the row in itself actually doesn't need to track this directly
    // (because the key pointing to the row does)
    // we need to emit it for the browser so it can render data in the
    // correct place.
    appendToArrow(h3: CoverageHeader, arrow: ArrowType) {
        const count = this._u32[this._sh.u32oCount];
        const sl = h3.h3splitlong;
        arrow.h3lo.append(sl[0]);
        arrow.h3hi.append(sl[1]);
        arrow.minAgl.append(this._u16[this._sh.u16oMinAltAgl]);
        arrow.minAlt.append(this._u16[this._sh.u16oMinAlt]);
        arrow.minAltSig.append(this._u8[this._sh.u8oMinAltMaxSig]);
        arrow.maxSig.append(this._u8[this._sh.u8oMaxSig]);
        arrow.avgSig.append((this._u32[this._sh.u32oSumSig] / count) * 4);
        arrow.avgCrc.append((this._u32[this._sh.u32oSumCrc] / count) * 10);
        arrow.avgGap.append((this._u32[this._sh.u32oSumGap] / count) * 4);
        arrow.count.append(count);

        if (this._ish) {
            let i = this._u8[(this._sh as typeof globalHeader).u8oHead],
                sid = 99999;
            let o = undefined;
            let ns = 0;

            // Iterate through the list, note we have mapped
            while (i != 0 && sid != 0) {
                // Calculate bytes from start for this item
                const startOffset = this._calcOffset(i);
                const o8 = startOffset,
                    o16 = startOffset / sU16,
                    o32 = startOffset / sU32;

                // Get sid to add to list
                sid = this._u16[o16 + this._ish.u16oStationId];
                let scount = this._u32[o32 + this._ish.u32oCount];
                let percentage = Math.trunc((10 * scount) / count);

                // emit id base16 plus percentage (percentage will only be one digit
                // at the end 0-A (0%-100%) - only the first 30 stations
                if (ns < 30) {
                    o = (o ? o + ',' : '') + ((sid << 4) | (percentage & 0x0f)).toString(36);
                }

                i = this._u8[o8 + this._ish.u8oNext];
                ns++;
            }
            arrow.stations?.append(o || '');
            arrow.expectedGap?.append(((this._u32[this._sh.u32oSumGap] / count) * 4) / ns);
            arrow.numStations?.append(Math.min(ns, 255));
        }
    }

    //
    // The last action we need to be able to do is fold data into our
    // CoverageRecord to allow aggregation. In this situation we are the
    // the destination and will be updated with the value and any stations
    // no longer in validStationSet will be removed
    //
    // NOTE: this can return a new CoverageRecord which replaces us!!
    //
    // Also note that once you have completed rolling up then you should discard
    // the old records!
    rollup(srcRecord: CoverageRecord, validStationSet: Set<StationId> | undefined) {
        // If we do not have sub stations then this is super easy we just
        // need to add the new values to our own and we are done
        if (!this._ish) {
            if (srcRecord._ish) {
                throw 'CoverageRecord: mixing global and station records...';
            }
            CoverageRecord._updateNested(this, 0, srcRecord, 0);
            return this;
        }

        // Iterate through linked list for a CoverageRecord and produce a map
        // of stationid:index
        const destStations = CoverageRecord._makeStationArray(this, 'di');
        const srcStations = CoverageRecord._makeStationMap(srcRecord, 'si');

        // After this we will have two maps but no duplicate stations
        // so we will take from one or the other. anything left in srcStations i
        // only there. Anything in destStations also has the si key for the
        // source index
        // also filter to make sure it is a valid station but only for the dest
        // assumption is if it is in src it's valid
        for (let i = destStations.length - 1; i >= 0; i--) {
            const k = destStations[i].id;
            if (validStationSet && !validStationSet.has(k as StationId)) {
                destStations.splice(i, 1);
            } else if (srcStations[k]) {
                // id in both both so we pull them together
                destStations[i].c += srcStations[k].c;
                destStations[i].si = srcStations[k].si;
                delete srcStations[k];
            }
        }

        // Sort the pick list by size so we are optimal on writing but also, if there is nothing
        // to pick then we should delete the record so return null indicating nothing found
        const pickList = _sortby([...destStations, ...Object.values(srcStations)], (o) => -o.c);
        if (!pickList.length) {
            return null;
        }

        // now we can allocate a new structure to fill we make it big enough for everything
        // that will be left over
        let newBr = new CoverageRecord(new ExtensionAllocationOptions(this._sh.version, pickList.length));

        // Then we simply iterate through these picking and combining
        // we have allocated enough space and they are in order so next is always just +1 except at end when 0
        newBr._u8[(newBr._sh as typeof globalHeader).u8oHead] = 1;

        let i = 1;
        let remaining = pickList.length - 1;

        for (const pick of pickList) {
            CoverageRecord._updateNestedHeader(newBr, i, pick.id, remaining ? i + 1 : 0);

            // now we need to merge which is easy
            // record will have a di if we have a dest index and either i or si for
            // source index, and we are writing into slot i
            if (pick.di) {
                CoverageRecord._updateNested(newBr, i, this, pick.di);
            }
            if (pick.si) {
                CoverageRecord._updateNested(newBr, i, srcRecord, pick.si);
            }
            // And then roll those up to the top level
            CoverageRecord._updateNested(newBr, 0, newBr, i);

            // adjust our position in the output
            i++;
            remaining--;
        }

        // We have made a new record which is what you should use going forwards..
        return newBr;
    }

    //
    // This is an alternative path function that is used to make sure we dump any stations
    // from accumulators if they are not used any longer.
    removeInvalidStations(validStationSet: Set<StationId>) {
        if (!this._ish) {
            return this; // this gets called even if we are not nested just say 'no change' and carry on
        }

        // Get a list of all the stations
        const currentStations = CoverageRecord._makeStationArray(this, 'i');

        // Now compare this list to the stationSet of valid
        // stations, if it's not in there then we remove it from
        // the list which means it won't get copied to new record
        const countStations = currentStations.length;
        for (let i = countStations - 1; i >= 0; i--) {
            if (!validStationSet.has(currentStations[i].id)) {
                currentStations.splice(i, 1);
            }
        }

        // If we have nothing to remove then save writing to the database by returning ourselves
        if (currentStations.length == countStations) {
            return this;
        }

        // Sort the pick list by size so we are optimal on writing but also, if there is nothing
        // to pick then we should delete the record so return null indicating nothing found
        const pickList = _sortby(currentStations, (o) => -o.c);
        if (!pickList.length) {
            return null;
        }

        // now we can allocate a new structure to fill we make it big enough for everything
        // that will be left over
        let newBr = new CoverageRecord(new ExtensionAllocationOptions(this._sh.version, pickList.length));

        // Then we simply iterate through these picking and combining
        // we have allocated enough space and they are in order so next is always just +1 except at end when 0
        newBr._u8[(newBr._sh as typeof globalHeader).u8oHead] = 1;

        let i = 1;
        let remaining = pickList.length - 1;

        for (const pick of pickList) {
            // Update the header
            CoverageRecord._updateNestedHeader(newBr, i, pick.id, remaining ? i + 1 : 0);

            // now we need to copy, though merge is just as easy
            // ** (COULD BE OPTIMIZED)
            CoverageRecord._updateNested(newBr, i, this, pick.i || superThrow('i missing from pick'));

            // And then roll those up to the top level
            CoverageRecord._updateNested(newBr, 0, newBr, i);

            // adjust our position in the output
            i++;
            remaining--;
        }

        // We have made a new record which is what you should use going forwards..
        return newBr;
    }

    //////////////////////////
    //
    // Adjust station linked list to put us in the right place and
    // update our values at the same time
    protected _updateStationList(stationid: StationId, altitude: number, agl: number, crc: number, signal: number, gap: number) {
        const ish = this._ish || superThrow('this._ish undefined');

        // So we can calculate offsets etc
        let pCount = 0;

        let oPrevNext: number = (this._sh as typeof globalHeader).u8oHead;
        let i = this._u8[oPrevNext],
            iPrevNext = 0,
            oPrevPrevNext = 0;

        // Iterate through the list, note we have mapped
        while (i != 0) {
            // Calculate bytes from start for this item
            const startOffset = this._calcOffset(i);
            const o8 = startOffset,
                o16 = startOffset / sU16,
                o32 = startOffset / sU32;

            let sid = this._u16[o16 + ish.u16oStationId];
            const iNext = this._u8[o8 + ish.u8oNext];

            // If it isn't set then it's a hole we can use, holes are always at end of the list as they
            // have no count!
            if (sid == 0) {
                sid = this._u16[o16 + ish.u16oStationId] = stationid;
            }

            // Did we find/add our station?
            if (sid == stationid) {
                // Update the counters
                this._update(o8, o16, o32, this._sh, altitude, agl, crc, signal, gap);

                // now we need to see if the previous count was higher or not, if it wasn't then we switch places
                // for this to work we need to be at least second point in the list (ie have already had a previous
                // the oPrevPrevNext is set to index for global header)
                if (pCount < this._u32[o32 + ish.u32oCount] && oPrevPrevNext) {
                    // previous is behind us so links to the one behind us
                    this._u8[oPrevNext] = iNext;

                    // we go to the previous one (index)
                    this._u8[o8 + ish.u8oNext] = iPrevNext;

                    // One before that goes to us
                    this._u8[oPrevPrevNext] = i;
                }

                // Done
                return;
            } else {
                // So we can check if we need to move it forwards or not
                pCount = this._u32[o32 + ish.u32oCount];
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
        if (i == 0) {
            // We are just appending so our current length will become the starting
            // point for our new station
            const currentLength = this._buffer.byteLength;
            const o8 = currentLength,
                o16 = currentLength / sU16,
                o32 = currentLength / sU32;
            this._allocationExtension();

            // Link us into the chain
            this._u8[o8] = this._sh.nestedVersion;
            this._u8[oPrevNext] = this._reverseOffset(currentLength);

            // And update the values
            this._update(o8, o16, o32, ish, altitude, agl, crc, signal, gap);
            this._u16[o16 + ish.u16oStationId] = stationid;

            // no need to reoder list as we are 1 count and worst the previous could be is
            // 1 count
        }
    }

    ////////////////////////////////////

    // Update a record based on specific offset, used internally
    // for updating either station or global or sub records
    protected _update(o8: number, o16: number, o32: number, sh: CRBase, altitude: number, agl: number, crc: number, signal: number, gap: number) {
        // Deal with lowest point and signal for that
        if (!this._u16[o16 + sh.u16oMinAlt] || this._u16[o16 + sh.u16oMinAlt] > altitude) {
            this._u16[o16 + sh.u16oMinAlt] = altitude;
            this._u8[o8 + sh.u8oMinAltMaxSig] = signal;
        } else if (this._u16[o16 + sh.u16oMinAlt] == altitude) {
            this._u8[o8 + sh.u8oMinAltMaxSig] = Math.max(this._u8[o8 + sh.u8oMinAltMaxSig], signal);
        }
        // Capture lowest AGL (note this may not be same as altitude as terain can
        // vary across the h3 index)
        if (!this._u16[o16 + sh.u16oMinAltAgl] || this._u16[o16 + sh.u16oMinAltAgl] > agl) {
            this._u16[o16 + sh.u16oMinAltAgl] = agl;
        }

        if (this._u8[o8 + sh.u8oMaxSig] < signal) {
            this._u8[o8 + sh.u8oMaxSig] = signal;
        }

        // signal was expanded and we want to shrink it back down when accumulating as it mean
        // we won't overflow till we hit 2^24 packets which is quite a few (0-64) same with gap
        // which is only tracked to a minute
        this._u32[o32 + sh.u32oSumSig] += signal >> 2;
        this._u32[o32 + sh.u32oSumGap] += gap;

        // crc is 0-10 so 4 bits or 2^26 before overflow
        this._u32[o32 + sh.u32oSumCrc] += crc;

        // We can cant to 2^32, but other values will overflow a some point before then
        // depending on average perhaps we should stop accumulating before that happens
        this._u32[o32 + sh.u32oCount]++;
    }

    // Generic rollup and update function updates our object from passed source
    protected static _updateNested(dest: CoverageRecord, di: number, src: CoverageRecord, si: number) {
        // dest can be parent or the station
        const destOffset = di ? dest._calcOffset(di) : 0;
        const dsh = di ? dest._ish || superThrow('dest._ish undefined') : dest._sh;
        const do8 = destOffset,
            do16 = destOffset / sU16,
            do32 = destOffset / sU32;

        // Where we are reading from
        const srcOffset = si ? src._calcOffset(si) : 0;
        const ssh = si ? src._ish || superThrow('src._ish undefined') : src._sh;
        const so8 = srcOffset,
            so16 = srcOffset / sU16,
            so32 = srcOffset / sU32;

        // Deal with lowest point and signal for that, if we drop the point then we drop the signal
        if (!dest._u16[do16 + dsh.u16oMinAlt] || dest._u16[do16 + dsh.u16oMinAlt] > src._u16[so16 + ssh.u16oMinAlt]) {
            dest._u16[do16 + dsh.u16oMinAlt] = src._u16[so16 + ssh.u16oMinAlt];
            dest._u8[do8 + dsh.u8oMinAltMaxSig] = src._u8[so8 + ssh.u8oMinAltMaxSig];
        } else if (dest._u16[do16 + dsh.u16oMinAlt] == src._u16[so16 + ssh.u16oMinAlt]) {
            dest._u8[do8 + dsh.u8oMinAltMaxSig] = Math.max(dest._u8[do8 + dsh.u8oMinAltMaxSig], src._u8[so8 + ssh.u8oMinAltMaxSig]);
        }
        // Capture lowest AGL (note this may not be same as altitude as terain can
        // vary across the h3 index)
        if (!dest._u16[do16 + dsh.u16oMinAltAgl] || dest._u16[do16 + dsh.u16oMinAltAgl] > src._u16[so16 + ssh.u16oMinAltAgl]) {
            dest._u16[do16 + dsh.u16oMinAltAgl] = src._u16[so16 + ssh.u16oMinAltAgl];
        }

        dest._u8[do8 + dsh.u8oMaxSig] = Math.max(dest._u8[do8 + dsh.u8oMaxSig], src._u8[so8 + ssh.u8oMaxSig]);

        dest._u32[do32 + dsh.u32oSumSig] += src._u32[so32 + ssh.u32oSumSig];
        dest._u32[do32 + dsh.u32oSumCrc] += src._u32[so32 + ssh.u32oSumCrc];
        dest._u32[do32 + dsh.u32oCount] += src._u32[so32 + ssh.u32oCount];
        dest._u32[do32 + dsh.u32oSumGap] += src._u32[so32 + ssh.u32oSumGap];
    }

    protected static _updateNestedHeader(br: CoverageRecord, i: number, stationid: StationId, next: number) {
        if (br._ish) {
            const destOffset = br._calcOffset(i);
            const do8 = destOffset,
                do16 = destOffset / sU16;
            br._u16[do16 + br._ish.u16oStationId] = stationid;
            br._u8[do8 + br._ish.u8oNext] = next;
            br._u8[do8 + br._ish.u8oVersion] = br._ish.version;
        }
    }

    // Iterate through linked list for a CoverageRecord and produce a map
    // of stationid:index... note keys end up as strings, so we add
    // id to the map so we have a number as well (remember sort order for
    // strings may not be what you expect :)
    protected static _makeStationMap(br: CoverageRecord, key: ValidMapKeys): Record<StationId, StationMap> {
        let stations: Record<StationId, StationMap> = {};
        const ish = br._ish || superThrow(`_makeStationMap called on ${br._sh.version}`);

        // pass 0 get list of all the existing stations
        for (let i = br._u8[(br._sh as typeof globalHeader).u8oHead]; i != 0; ) {
            // Calculate bytes from start for this item
            const startOffset = br._calcOffset(i);
            const o8 = startOffset,
                o16 = startOffset / sU16,
                o32 = startOffset / sU32;
            const sid = br._u16[o16 + ish.u16oStationId] as StationId;

            if (sid) {
                stations[sid] = {id: sid, c: br._u32[o32 + ish.u32oCount]};
                stations[sid][key] = i;
            }

            // Move to next one
            i = br._u8[o8 + ish.u8oNext];
        }
        return stations;
    }

    // Iterate through linked list for a CoverageRecord and produce a map
    // of stationid:index... note keys end up as strings, so we add
    // id to the map so we have a number as well (remember sort order for
    // strings may not be what you expect :)
    protected static _makeStationArray(br: CoverageRecord, key: ValidMapKeys): StationMap[] {
        const ish = br._ish || superThrow(`_makeStationArray called on ${br._sh.version}`);
        let stations = new Array((br._u8.byteLength - br._sh.length) / ish.length);

        // pass 0 get list of all the existing stations
        let di = 0;
        for (let i = br._u8[(br._sh as typeof globalHeader).u8oHead]; i != 0; ) {
            // Calculate bytes from start for this item
            const startOffset = br._calcOffset(i);
            const o8 = startOffset,
                o16 = startOffset / sU16,
                o32 = startOffset / sU32;
            const sid = br._u16[o16 + ish.u16oStationId] as StationId;

            if (sid) {
                stations[di] = {id: sid, c: br._u32[o32 + ish.u32oCount]};
                stations[di][key] = i;
                di++;
            }

            // Move to next one
            i = br._u8[o8 + ish.u8oNext];
        }
        // handle case where there is an memory allocation but no head
        if (di != stations.length) {
            stations.splice(di);
        }
        return stations;
    }

    //
    // Helpers for finding offset of extensions in nested allocation
    private _calcOffset(i: number): number {
        return this._sh.length + (i - 1) * (this._ish?.length || 0);
    }
    private _reverseOffset(o: number): number {
        return (o - this._sh.length) / (this._ish?.length || 0) + 1;
    }

    //
    // We need to be able to add new records to the end of the record
    private _allocationExtension(length?: number) {
        // Allocate bigger buffer
        const n = new Uint8Array(this._buffer.byteLength + (length || this._ish?.length || superThrow(`no ish length`)));
        n.set(this._buffer);
        this._buffer = this._u8 = n;
        this._u16 = new Uint16Array(this._buffer.buffer, 0, this._buffer.byteLength / sU16);
        this._u32 = new Uint32Array(this._buffer.buffer, 0, this._buffer.byteLength / sU32);
    }
}
