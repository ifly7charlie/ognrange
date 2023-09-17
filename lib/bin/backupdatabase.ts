import {Utf8, Binary, makeBuilder, makeTable, RecordBatchWriter} from 'apache-arrow/Arrow.node';
import {TypedArray} from 'apache-arrow/interfaces';
import {BACKUP_PATH, UNCOMPRESSED_ARROW_FILES} from '../common/config';

import {Epoch, EpochMS} from './types';

import {CoverageHeader} from './coverageheader';

import {DB} from './stationcache';

import {mkdirSync} from 'fs';
import {Accumulators, AccumulatorTypeString} from './accumulators';

import {createWriteStream} from 'fs';
//import {PassThrough} from 'stream';
import {createGzip} from 'node:zlib';

import {Readable, pipeline} from 'node:stream';
import {RecordBatch, RecordBatchStreamWriter, Schema, Struct, Field, builderThroughAsyncIterable} from 'apache-arrow';
import {IterableBuilderOptions} from 'apache-arrow/factories';

interface BackupMessageType {
    h3k: string;
    cr: Uint8Array;
}

//
// Make an Arrow backup of the database
export async function backupDatabase(db: DB, {whatAccumulators, now}: {whatAccumulators: Accumulators; now: Epoch}): Promise<{elapsed: EpochMS; rows: number}> {
    //
    //
    const startTime = Date.now();
    const name = db.ognStationName;

    const fileName = BACKUP_PATH + name + '-' + new Date(now).toISOString().substring(0, 10) + '.backup.arrow';

    let rows = 0;

    // Finally if we have rollups with data after us then we need to update their invalidstations
    // now we go through them in step form
    const allRecords = async function* (): AsyncGenerator<BackupMessageType> {
        for (const accumulator of Object.keys(whatAccumulators)) {
            const r = accumulator as AccumulatorTypeString;
            const par = whatAccumulators[r];
            const iterator = db.iterator(CoverageHeader.getDbSearchRangeForAccumulator(r, par!.bucket));

            for await (const [prefixedh3r, rollupValue] of iterator) {
                rows++;
                yield {h3k: prefixedh3r, cr: rollupValue};
            }
        }
    };

    const message_type = new Struct([
        //Fields in output
        new Field('h3k', new Utf8(), false),
        new Field('cr', new Binary(), false)
    ]);

    const builderOptions: IterableBuilderOptions<typeof message_type> = {
        type: message_type,
        nullValues: [null, 'n/a', undefined],
        highWaterMark: 3000,
        queueingStrategy: 'count'
    };

    const messagesToBatches = async function* (source: AsyncIterable<BackupMessageType>) {
        let schema = undefined;
        const transform = builderThroughAsyncIterable<typeof message_type>(builderOptions);
        for await (const vector of transform(source)) {
            schema ??= new Schema(vector.type.children);
            for (const chunk of vector.data) {
                yield new RecordBatch(schema, chunk);
            }
        }
    };

    //    const pt = new PassThrough({objectMode: true});
    //    pt.pipe(createGzip()).pipe(createWriteStream(fileName + '.gz'));
    //    pt.end();

    await new Promise((resolve) =>
        pipeline(
            //
            Readable.from(messagesToBatches(allRecords())),
            RecordBatchStreamWriter.throughNode(),
            createGzip(),
            createWriteStream(fileName + '.gz'),
            resolve
        )
    );

    return {elapsed: (Date.now() - startTime) as EpochMS, rows};
}
