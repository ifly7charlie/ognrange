import {CoverageHeader} from './coverageheader';

import {cloneDeep as _clonedeep, isEqual as _isequal, map as _map, reduce as _reduce, sortBy as _sortBy, filter as _filter, uniq as _uniq} from 'lodash';

import {DB} from './stationcache';

import {CurrentAccumulator, Accumulators, AccumulatorTypeString} from './accumulators';

export async function saveAccumulatorMetadata(db: DB, currentAccumulator: CurrentAccumulator, allAccumulators: Accumulators): Promise<void> {
    const dbkey = CoverageHeader.getAccumulatorMeta(...currentAccumulator).dbKey();
    const now = new Date();
    const nowEpoch = Math.trunc(now.valueOf() / 1000);
    await db
        .get(dbkey)
        .then((value) => {
            const meta = JSON.parse(String(value));
            meta.oldStarts = [...meta?.oldStarts, {start: meta.start, startUtc: meta.startUtc}];
            meta.accumulators = allAccumulators;
            meta.start = nowEpoch;
            meta.startUtc = now.toISOString();
            db.put(dbkey, Uint8FromObject(meta));
        })
        .catch((e) => {
            db.put(
                dbkey,
                Uint8FromObject({
                    accumulators: allAccumulators,
                    oldStarts: [],
                    start: nowEpoch,
                    startUtc: now.toISOString()
                })
            );
        });
    // make sure we have an up to date header for each accumulator
    for (const typeString in allAccumulators) {
        const type = typeString as AccumulatorTypeString;
        const currentHeader = CoverageHeader.getAccumulatorMeta(type, allAccumulators[type]!.bucket);
        const dbkey = currentHeader.dbKey();
        await db
            .get(dbkey)
            .then((value) => {
                const meta = JSON.parse(String(value));
                db.put(dbkey, Uint8FromObject({...meta, accumulators: allAccumulators, currentAccumulator: currentAccumulator[1]}));
            })
            .catch((e) => {
                db.put(dbkey, Uint8FromObject({start: nowEpoch, startUtc: now.toISOString(), accumulators: allAccumulators, currentAccumulator: currentAccumulator[1]}));
            });
    }
}

export function Uint8FromObject(o: Record<any, any>): Uint8Array {
    return Uint8Array.from(Buffer.from(JSON.stringify(o)));
}
