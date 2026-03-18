#!/usr/bin/env npx ts-node
import dotenv from 'dotenv';
dotenv.config({path: '.env.local', override: true});

import {readdirSync, lstatSync, readFileSync, statSync, unlinkSync} from 'fs';
import {join} from 'path';
import {open} from 'node:fs/promises';
import {RecordBatchStreamReader} from 'apache-arrow/Arrow.node';
import {createGunzip} from 'node:zlib';
import {OUTPUT_PATH} from '../lib/common/config';
import yargs from 'yargs';

const args = yargs(process.argv.slice(2)) //
    .option('delete', {type: 'boolean', default: false, description: 'Actually delete files (default is dry-run)'})
    .option('path', {type: 'string', description: 'Override OUTPUT_PATH'})
    .help()
    .alias('help', 'h')
    .parseSync();

const basePath = (args.path ?? OUTPUT_PATH).replace(/\/?$/, '/');

interface FileEntry {
    path: string;
    size: number; // bytes; 0 for symlinks
}

function fmtSize(bytes: number): string {
    if (bytes < 1024) return `${bytes}B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}K`;
    return `${(bytes / (1024 * 1024)).toFixed(1)}M`;
}

// Returns true if an arrow file (compressed or not) has 0 data rows.
// Only reads until the first non-empty batch for efficiency.
async function isArrowEmpty(filePath: string, compressed: boolean): Promise<boolean> {
    const fd = await open(filePath);
    try {
        const stream = compressed ? fd.createReadStream().pipe(createGunzip()) : fd.createReadStream();
        const reader = await RecordBatchStreamReader.from(stream);
        for await (const batch of reader) {
            if (batch.numRows > 0) return false;
        }
        return true;
    } finally {
        await fd.close();
    }
}

// Returns arrowRecords from JSON sidecar, or null if unavailable.
function rowCountFromJson(jsonPath: string): number | null {
    try {
        const json = JSON.parse(readFileSync(jsonPath, 'utf8'));
        if (typeof json.arrowRecords === 'number') return json.arrowRecords;
    } catch {
        // missing or unparseable
    }
    return null;
}

function fileStat(p: string): {exists: boolean; size: number} {
    try {
        return {exists: true, size: lstatSync(p).size};
    } catch {
        return {exists: false, size: 0};
    }
}

async function isFileEmpty(filePath: string, compressed: boolean, jsonPath: string): Promise<boolean> {
    const jsonCount = rowCountFromJson(jsonPath);
    if (jsonCount !== null) return jsonCount === 0;
    return isArrowEmpty(filePath, compressed);
}

async function collectFilesInDir(dir: string): Promise<{
    toDelete: FileEntry[];
    staleSymlinks: FileEntry[];
}> {
    const toDelete: FileEntry[] = [];
    const staleSymlinks: FileEntry[] = [];
    let entries: string[];
    try {
        entries = readdirSync(dir);
    } catch {
        return {toDelete, staleSymlinks};
    }

    for (const entry of entries) {
        const fullPath = join(dir, entry);
        const stat = lstatSync(fullPath);

        if (stat.isSymbolicLink()) {
            try {
                statSync(fullPath); // follows symlink; throws if target missing
            } catch {
                staleSymlinks.push({path: fullPath, size: 0});
            }
            continue;
        }

        if (!stat.isFile()) continue;

        if (entry.endsWith('.arrow.gz')) {
            const jsonPath = fullPath.replace(/\.arrow\.gz$/, '.json');
            let empty: boolean;
            try {
                empty = await isFileEmpty(fullPath, true, jsonPath);
            } catch (e) {
                console.warn(`  Warning: could not read ${fullPath}: ${e}`);
                continue;
            }
            if (empty) {
                toDelete.push({path: fullPath, size: stat.size});
                const js = fileStat(jsonPath);
                if (js.exists) toDelete.push({path: jsonPath, size: js.size});
            }
            continue;
        }

        if (entry.endsWith('.arrow')) {
            const gzPath = fullPath + '.gz';
            const gz = fileStat(gzPath);
            if (gz.exists) {
                // .gz is authoritative — unconditionally remove uncompressed copy
                toDelete.push({path: fullPath, size: stat.size});
            } else {
                // No .gz — only remove if empty, along with its .json
                const jsonPath = fullPath.replace(/\.arrow$/, '.json');
                let empty: boolean;
                try {
                    empty = await isFileEmpty(fullPath, false, jsonPath);
                } catch (e) {
                    console.warn(`  Warning: could not read ${fullPath}: ${e}`);
                    continue;
                }
                if (empty) {
                    toDelete.push({path: fullPath, size: stat.size});
                    const js = fileStat(jsonPath);
                    if (js.exists) toDelete.push({path: jsonPath, size: js.size});
                }
            }
        }
    }

    return {toDelete, staleSymlinks};
}

async function main() {
    const isDryRun = !args.delete;
    if (isDryRun) {
        console.log(`Dry run — pass --delete to actually remove files\n`);
    }

    const allToDelete: FileEntry[] = [];
    const allStaleSymlinks: FileEntry[] = [];

    // Walk basePath and each immediate subdirectory
    const dirs = [basePath];
    for (const entry of readdirSync(basePath)) {
        const fullPath = join(basePath, entry);
        if (lstatSync(fullPath).isDirectory()) dirs.push(fullPath);
    }

    for (const dir of dirs) {
        const {toDelete, staleSymlinks} = await collectFilesInDir(dir);
        allToDelete.push(...toDelete);
        allStaleSymlinks.push(...staleSymlinks);
    }

    const allToRemove = [...allToDelete, ...allStaleSymlinks];

    if (allToRemove.length === 0) {
        console.log('Nothing to remove.');
        return;
    }

    const totalBytes = allToDelete.reduce((s, f) => s + f.size, 0);

    console.log(`Files to delete (${allToDelete.length}, ${fmtSize(totalBytes)}):`);
    for (const f of allToDelete) console.log(`  ${fmtSize(f.size).padStart(7)}  ${f.path}`);

    if (allStaleSymlinks.length > 0) {
        console.log(`\nStale symlinks to remove (${allStaleSymlinks.length}):`);
        for (const f of allStaleSymlinks) console.log(`         ${f.path}`);
    }

    if (!isDryRun) {
        console.log('\nDeleting...');
        let removed = 0;
        let failed = 0;
        for (const f of allToRemove) {
            try {
                unlinkSync(f.path);
                removed++;
            } catch (e) {
                console.error(`  Failed to remove ${f.path}: ${e}`);
                failed++;
            }
        }
        console.log(`Done. Removed ${removed} item(s)${failed ? `, ${failed} failed` : ''}.`);
    } else {
        console.log(`\n${allToRemove.length} item(s) would be removed. Run with --delete to proceed.`);
    }
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
