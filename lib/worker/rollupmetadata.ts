import {AccumulatorBucket, CoverageHeader} from '../bin/coverageheader';

import {Epoch} from '../bin/types';

import {DB} from './stationcache';

import {Accumulators, AccumulatorTypeString} from '../bin/accumulators';

import {ALL_LAYERS, Layer} from '../common/layers';
import {ENABLED_LAYERS} from '../common/config';

export interface DBMetaRecord {
    start: Epoch;
    startUtc: string;
    accumulators: Accumulators;
    currentAccumulator: AccumulatorBucket;

    allStarts: {start: Epoch; startUtc: string}[];
}

export async function saveAccumulatorMetadata(db: DB, accumulators: Accumulators, onlyLayer?: Layer): Promise<DB> {
    const now = new Date();
    const nowEpoch = Math.trunc(now.valueOf() / 1000) as Epoch;

    const updateMeta = (existing: DBMetaRecord | {}): DBMetaRecord => {
        return {
            ...existing,
            allStarts: [...('allStarts' in existing ? existing.allStarts : []), {start: nowEpoch, startUtc: now.toISOString()}].slice(-250),
            start: nowEpoch,
            startUtc: now.toISOString(),
            accumulators,
            currentAccumulator: accumulators.current.bucket
        };
    };

    // When called from a rollup, only write metadata for the layer being rolled up.
    // When called from a flush (no layer), write for every enabled layer so the
    // startup rollup can find and properly handle hanging accumulators for all layers.
    const layers: Layer[] = onlyLayer ? [onlyLayer] : ENABLED_LAYERS ? [...ENABLED_LAYERS] : [...ALL_LAYERS];

    for (const layer of layers) {
        for (const typeString in accumulators) {
            const type = typeString as AccumulatorTypeString;
            const currentHeader = CoverageHeader.getAccumulatorMeta(type, accumulators[type].bucket, layer);
            const dbkey = currentHeader.dbKey();
            await db
                .get(dbkey)
                .then((value) => {
                    const meta = value ? JSON.parse(String(value)) : {};
                    return db.put(dbkey, Uint8FromObject(updateMeta(meta)));
                })
                .catch(() => {
                    return db.put(dbkey, Uint8FromObject(updateMeta({})));
                });
        }
    }

    return db;
}

export function Uint8FromObject(o: Record<any, any>): Uint8Array {
    return Uint8Array.from(Buffer.from(JSON.stringify(o)));
}
