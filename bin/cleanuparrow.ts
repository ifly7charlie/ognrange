#!/usr/bin/env npx ts-node
import dotenv from 'dotenv';
dotenv.config({path: '.env.local', override: true});

import {readdirSync, lstatSync, readFileSync, statSync, readlinkSync, unlinkSync} from 'fs';
import {join, resolve} from 'path';
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
const isDryRun = !args.delete;

// Minimum file sizes below which a file might be empty (no data rows).
// Above these thresholds the file definitely contains data — skip opening it.
const MIN_DATA_SIZE_ARROW = 2048; // uncompressed .arrow
const MIN_DATA_SIZE_GZ = 512;     // compressed .arrow.gz

interface FileEntry {
    path: string;
    size: number;
}

function fmtSize(bytes: number): string {
    if (bytes < 1024) return `${bytes}B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}K`;
    return `${(bytes / (1024 * 1024)).toFixed(1)}M`;
}

// Returns true if the arrow file has 0 data rows.
// Only reads the first batch — sufficient to detect emptiness.
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

function tryStat(p: string): {exists: boolean; size: number} {
    try {
        return {exists: true, size: lstatSync(p).size};
    } catch {
        return {exists: false, size: 0};
    }
}

// Returns true if the file has no data rows, using size shortcut then JSON then arrow read.
async function isEmpty(filePath: string, fileSize: number, compressed: boolean): Promise<boolean> {
    const sizeThreshold = compressed ? MIN_DATA_SIZE_GZ : MIN_DATA_SIZE_ARROW;
    if (fileSize > sizeThreshold) return false; // definitely has data

    const jsonPath = filePath.replace(compressed ? /\.arrow\.gz$/ : /\.arrow$/, '.json');
    const jsonCount = rowCountFromJson(jsonPath);
    if (jsonCount !== null) return jsonCount === 0;

    return isArrowEmpty(filePath, compressed);
}

async function processDir(dir: string): Promise<{deleted: number; bytes: number; symlinks: number; failed: number}> {
    let entries: string[];
    try {
        entries = readdirSync(dir);
    } catch {
        return {deleted: 0, bytes: 0, symlinks: 0, failed: 0};
    }

    const toDelete: FileEntry[] = [];
    const toDeletePaths = new Set<string>();
    const symlinksToCheck: string[] = [];

    for (const entry of entries) {
        const fullPath = join(dir, entry);
        const stat = lstatSync(fullPath);

        if (stat.isSymbolicLink()) {
            symlinksToCheck.push(fullPath);
            continue;
        }

        if (!stat.isFile()) continue;

        if (entry.endsWith('.arrow.gz')) {
            let empty: boolean;
            try {
                empty = await isEmpty(fullPath, stat.size, true);
            } catch (e) {
                console.warn(`  Warning: could not read ${fullPath}: ${e}`);
                continue;
            }
            if (empty) {
                toDelete.push({path: fullPath, size: stat.size});
                toDeletePaths.add(fullPath);
                const js = tryStat(fullPath.replace(/\.arrow\.gz$/, '.json'));
                if (js.exists) {
                    const jsonPath = fullPath.replace(/\.arrow\.gz$/, '.json');
                    toDelete.push({path: jsonPath, size: js.size});
                    toDeletePaths.add(jsonPath);
                }
            }
            continue;
        }

        if (entry.endsWith('.arrow')) {
            const gzPath = fullPath + '.gz';
            if (tryStat(gzPath).exists) {
                // .gz exists — unconditionally remove uncompressed copy
                toDelete.push({path: fullPath, size: stat.size});
                toDeletePaths.add(fullPath);
            } else {
                // No .gz — remove only if empty
                let empty: boolean;
                try {
                    empty = await isEmpty(fullPath, stat.size, false);
                } catch (e) {
                    console.warn(`  Warning: could not read ${fullPath}: ${e}`);
                    continue;
                }
                if (empty) {
                    toDelete.push({path: fullPath, size: stat.size});
                    toDeletePaths.add(fullPath);
                    const jsonPath = fullPath.replace(/\.arrow$/, '.json');
                    const js = tryStat(jsonPath);
                    if (js.exists) {
                        toDelete.push({path: jsonPath, size: js.size});
                        toDeletePaths.add(jsonPath);
                    }
                }
            }
        }
    }

    // Collect stale symlinks: currently broken OR pointing to a file we're deleting
    const staleSymlinks: string[] = [];
    for (const fullPath of symlinksToCheck) {
        const target = resolve(dir, readlinkSync(fullPath));
        const currentlyBroken = (() => { try { statSync(fullPath); return false; } catch { return true; } })();
        if (currentlyBroken || toDeletePaths.has(target)) {
            staleSymlinks.push(fullPath);
        }
    }

    const totalBytes = toDelete.reduce((s, f) => s + f.size, 0);
    const totalItems = toDelete.length + staleSymlinks.length;

    if (totalItems === 0) return {deleted: 0, bytes: 0, symlinks: 0, failed: 0};

    if (isDryRun) {
        console.log(`\n${dir}:`);
        for (const f of toDelete) console.log(`  ${fmtSize(f.size).padStart(7)}  ${f.path}`);
        for (const s of staleSymlinks) console.log(`  [symlink]  ${s}`);
        return {deleted: toDelete.length, bytes: totalBytes, symlinks: staleSymlinks.length, failed: 0};
    }

    // Delete mode: remove files immediately, then stale symlinks
    let deleted = 0;
    let failed = 0;
    for (const f of toDelete) {
        try { unlinkSync(f.path); deleted++; } catch (e) { console.error(`  Failed: ${f.path}: ${e}`); failed++; }
    }
    for (const s of staleSymlinks) {
        try { unlinkSync(s); deleted++; } catch (e) { console.error(`  Failed: ${s}: ${e}`); failed++; }
    }
    const summary = `  ${deleted} removed (${fmtSize(totalBytes)})${staleSymlinks.length ? `, ${staleSymlinks.length} symlinks` : ''}${failed ? `, ${failed} FAILED` : ''}`;
    console.log(`${dir}: ${summary}`);

    return {deleted: toDelete.length, bytes: totalBytes, symlinks: staleSymlinks.length, failed};
}

async function main() {
    if (isDryRun) console.log(`Dry run — pass --delete to actually remove files`);

    const dirs = [basePath];
    for (const entry of readdirSync(basePath)) {
        const fullPath = join(basePath, entry);
        if (lstatSync(fullPath).isDirectory()) dirs.push(fullPath);
    }

    let totalDeleted = 0;
    let totalBytes = 0;
    let totalSymlinks = 0;
    let totalFailed = 0;
    let dirCount = 0;

    for (const dir of dirs) {
        const r = await processDir(dir);
        totalDeleted += r.deleted;
        totalBytes += r.bytes;
        totalSymlinks += r.symlinks;
        totalFailed += r.failed;
        dirCount++;
        if (!isDryRun && dirCount % 100 === 0) {
            console.log(`  [${dirCount}/${dirs.length} dirs processed]`);
        }
    }

    console.log(
        `\nTotal: ${totalDeleted} files (${fmtSize(totalBytes)}), ${totalSymlinks} symlinks` +
            (totalFailed ? `, ${totalFailed} failures` : '') +
            (isDryRun ? ' — run with --delete to proceed' : ' removed')
    );
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
