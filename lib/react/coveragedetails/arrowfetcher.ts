import {tableFromIPC, Table} from 'apache-arrow';

// Shared SWR fetcher for the per-station arrow files (receive horizon, ground
// horizon). Loads with apache-arrow tableFromIPC, NOT @loaders.gl/arrow:
// loaders.gl's ColumnarTable collapses nullable Float32 nulls to 0.0
// (indistinguishable from a real 0° angle); Vector.get() respects the null
// bitmap so gaps render correctly. A 404 or parse failure returns null and the
// component renders nothing
export const arrowFetcher = async (url: string): Promise<Table | null> => {
    const res = await fetch(url);
    if (!res.ok) {
        return null;
    }
    const buffer = await res.arrayBuffer();
    try {
        return tableFromIPC(new Uint8Array(buffer));
    } catch (e) {
        console.log(`arrow: unable to parse ${url}: ${e}`);
        return null;
    }
};
