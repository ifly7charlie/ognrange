import {readFileSync} from 'fs';
import {gunzipSync} from 'zlib';

/** Read and parse a JSON (or gzip-compressed JSON) file. Returns null on any error. */
export function readJsonFile<T>(filePath: string): T | null {
    try {
        const raw = readFileSync(filePath);
        if (filePath.endsWith('.gz')) {
            return JSON.parse(gunzipSync(raw).toString()) as T;
        }
        return JSON.parse(raw.toString()) as T;
    } catch {
        return null;
    }
}
