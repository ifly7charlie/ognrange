import {tableFromIPC, type Table} from 'apache-arrow';

/**
 * Column-name → array map: a TypedArray for the numeric columns, a plain JS
 * array for Utf8/Bool. This is the shape @loaders.gl/arrow's ColumnarTable
 * used to return, but read straight from apache-arrow.
 *
 * We do the conversion ourselves because @loaders.gl/arrow 4.4 (pulled in by
 * deck.gl 9.3) throws `arrow type not supported: Int_` while converting the
 * schema — every integer column read back from IPC is an `Int_` instance, so
 * the whole parse fails and no coverage or station data ever arrives. See also
 * coveragedetails/arrowfetcher.ts, which avoids the same loader for a
 * different reason (it collapses nullable Float32 nulls to 0.0).
 */
export type ArrowColumns = Record<string, any>;

export function columnsFromTable(table: Table): ArrowColumns {
    const columns: ArrowColumns = {};
    for (const field of table.schema.fields) {
        const vector = table.getChild(field.name);
        if (vector) {
            columns[field.name] = vector.toArray();
        }
    }
    return columns;
}

export function columnsFromArrow(buffer: ArrayBuffer): ArrowColumns {
    return columnsFromTable(tableFromIPC(new Uint8Array(buffer)));
}
