import {Builder, Schema, RecordBatch, Struct, Utf8, Field, Uint8, Uint16, Uint32, makeBuilder, RecordBatchWriter} from 'apache-arrow/Arrow.node';

import {createWriteStream, rename} from 'fs';
import {PassThrough} from 'stream';

import {CoverageHeader} from './coverageheader';
import {CoverageRecord, bufferTypes} from './coveragerecord';

import {createGzip} from 'node:zlib';
import {UNCOMPRESSED_ARROW_FILES} from '../common/config';

export class CoverageRecordWriter {
    private _schema: Schema;

    private _builder: Builder;
    private _recordsWritten: number;
    private _c: WS;
    private _uc?: WS;

    get recordsWritten(): number {
        return this._recordsWritten;
    }

    constructor(type: bufferTypes, fileName: string) {
        const message_type = new Struct([
            //Fields in output
            new Field('h3lo', new Uint32(), false),
            new Field('h3hi', new Uint32(), false),
            new Field('minAgl', new Uint16(), false),
            new Field('minAlt', new Uint16(), false),
            new Field('minAltSig', new Uint8(), false),
            new Field('maxSig', new Uint8(), false),
            new Field('avgSig', new Uint8(), false),
            new Field('avgCrc', new Uint8(), false),
            new Field('count', new Uint32(), false),
            new Field('avgGap', new Uint8(), false),

            ...(type == bufferTypes.global
                ? [
                      new Field('stations', new Utf8(), false), //
                      new Field('expectedGap', new Uint8(), false),
                      new Field('numStations', new Uint8(), false)
                  ]
                : [])
        ]);

        const builderOptions = {
            type: message_type,
            nullValues: null,
            highWaterMark: 10000,
            queueingStrategy: 'count'
        };

        this._schema = new Schema(message_type.children);
        this._builder = makeBuilder(builderOptions);
        this._recordsWritten = 0;

        // Open the write streams
        this._c = new WS(fileName, true);
        if (UNCOMPRESSED_ARROW_FILES) {
            this._uc = new WS(fileName, false);
        }
    }

    // Append to the builder, flushing if needed
    append(hr: CoverageHeader, cr: CoverageRecord) {
        this._builder.append(cr.arrowFormat(hr.h3splitlong));
        if (this._builder.length > 4500) {
            this.flushBlock();
        }
    }

    finalize(): Promise<number> {
        this.flushBlock();
        return Promise.allSettled([this._c.end(), this._uc ? this._uc.end() : Promise.resolve()]) //
            .then(() => this._recordsWritten);
    }

    private flushBlock() {
        this._recordsWritten += this._builder.length;
        const rb = new RecordBatch(this._schema, this._builder.flush());
        this._c.write(rb);
        this._uc?.write(rb);
    }
}

//
// Internal helper for the stream
class WS {
    private _pt: PassThrough;
    private _completionPromise: Promise<void>;

    constructor(fileName: string, compressed: boolean) {
        const extension = compressed ? '.gz' : '';
        const ws = createWriteStream(fileName + '.working' + extension);

        // Bind a promise to the close of the stream so we can wait for it
        this._completionPromise = new Promise((resolve) => {
            ws.on('close', () => {
                rename(
                    fileName + '.working' + extension,
                    fileName + extension, //
                    (err) => {
                        if (err) {
                            console.error(`error renaming ${fileName}.working to ${fileName}.gz:${err}`);
                        }
                        resolve();
                    }
                );
            });
        });

        this._pt = new PassThrough({objectMode: true});
        if (compressed) {
            this._pt.pipe(RecordBatchWriter.throughNode()).pipe(createGzip()).pipe(ws);
        } else {
            this._pt.pipe(RecordBatchWriter.throughNode()).pipe(ws);
        }
    }

    write(rb: RecordBatch) {
        this._pt.write(rb);
    }

    end(): Promise<void> {
        this._pt.end();
        return this._completionPromise;
    }
}
