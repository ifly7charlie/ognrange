import {AccumulatorBucket, CoverageHeader} from '../bin/coverageheader';

import {cloneDeep as _clonedeep, isEqual as _isequal, map as _map, reduce as _reduce, sortBy as _sortBy, filter as _filter, uniq as _uniq} from 'lodash';

import {Epoch, EpochMS} from '../bin/types';

import {DB} from './stationcache';

import {Accumulators, AccumulatorTypeString} from '../bin/accumulators';

export interface DBMetaRecord {
    start: Epoch;
    startUtc: string;
    accumulators: Accumulators;
    currentAccumulator: AccumulatorBucket;

    allStarts: {start: Epoch; startUtc: string}[];
}

export async function saveAccumulatorMetadata(db: DB, accumulators: Accumulators): Promise<DB> {
    const now = new Date();
    const nowEpoch = Math.trunc(now.valueOf() / 1000) as Epoch;

    const updateMeta = (existing: DBMetaRecord | {}): DBMetaRecord => {
        return {
            //
            allStarts: [...('allStarts' in existing ? existing.allStarts : []), {start: nowEpoch, startUtc: now.toISOString()}],
            start: nowEpoch,
            startUtc: now.toISOString(),
            ...existing,
            accumulators,
            currentAccumulator: accumulators.current.bucket
        };
    };

    // make sure we have an up to date header for each accumulator
    for (const typeString in accumulators) {
        const type = typeString as AccumulatorTypeString;
        const currentHeader = CoverageHeader.getAccumulatorMeta(type, accumulators[type].bucket);
        const dbkey = currentHeader.dbKey();
        await db
            .get(dbkey)
            .then((value) => {
                const meta = JSON.parse(String(value));
                db.put(dbkey, Uint8FromObject(updateMeta(meta)));
            })
            .catch((e) => {
                db.put(dbkey, Uint8FromObject(updateMeta({})));
            });
    }

    return db;
}

export function Uint8FromObject(o: Record<any, any>): Uint8Array {
    return Uint8Array.from(Buffer.from(JSON.stringify(o)));
}
