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
    .option('verbose', {alias: 'v', type: 'boolean', default: false, description: 'Show per-file detail in dry-run'})
    .option('path', {type: 'string', description: 'Override OUTPUT_PATH'})
    .help()
    .alias('help', 'h')
    .parseSync();

const basePath = (args.path ?? OUTPUT_PATH).replace(/\/?$/, '/');
const isDryRun = !args.delete;
const verbose = args.verbose;

// Minimum file sizes below which a file might be empty (no data rows).
// Above these thresholds the file definitely contains data — skip opening it.
const MIN_DATA_SIZE_ARROW = 2048; // uncompressed .arrow
const MIN_DATA_SIZE_GZ = 512;     // compressed .arrow.gz

interface FileEntry {
    path: string;
    size: number;
    reason: string;
}

interface DirResult {
    deletedArrow: number;
    deletedArrowGz: number;
    deletedJson: number;
    deletedBytes: number;
    deletedSymlinks: number;
    failed: number;
    keptArrow: number;
    keptArrowGz: number;
    keptJson: number;
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
// JSON arrowRecords==0 is trusted as empty, but arrowRecords>0 may be stale — always verify
// by reading the file when the file is small enough to possibly be empty.
async function isEmpty(filePath: string, fileSize: number, compressed: boolean): Promise<boolean> {
    const sizeThreshold = compressed ? MIN_DATA_SIZE_GZ : MIN_DATA_SIZE_ARROW;
    if (fileSize > sizeThreshold) return false;

    const jsonPath = filePath.replace(compressed ? /\.arrow\.gz$/ : /\.arrow$/, '.json');
    const jsonCount = rowCountFromJson(jsonPath);
    if (jsonCount === 0) return true; // JSON confirms empty — reliable

    return isArrowEmpty(filePath, compressed);
}

async function processDir(dir: string): Promise<DirResult> {
    const zero: DirResult = {deletedArrow: 0, deletedArrowGz: 0, deletedJson: 0, deletedBytes: 0, deletedSymlinks: 0, failed: 0, keptArrow: 0, keptArrowGz: 0, keptJson: 0};
    let entries: string[];
    try {
        entries = readdirSync(dir);
    } catch {
        return zero;
    }

    const toDelete: FileEntry[] = [];
    const toDeletePaths = new Set<string>();
    const symlinksToCheck: string[] = [];
    const jsonFiles: {path: string; size: number}[] = [];

    let keptArrow = 0;
    let keptArrowGz = 0;

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
                toDelete.push({path: fullPath, size: stat.size, reason: 'empty'});
                toDeletePaths.add(fullPath);
            } else {
                keptArrowGz++;
            }
            continue;
        }

        if (entry.endsWith('.arrow')) {
            const gzPath = fullPath + '.gz';
            if (tryStat(gzPath).exists) {
                toDelete.push({path: fullPath, size: stat.size, reason: 'has .gz'});
                toDeletePaths.add(fullPath);
            } else {
                let empty: boolean;
                try {
                    empty = await isEmpty(fullPath, stat.size, false);
                } catch (e) {
                    console.warn(`  Warning: could not read ${fullPath}: ${e}`);
                    continue;
                }
                if (empty) {
                    toDelete.push({path: fullPath, size: stat.size, reason: 'empty'});
                    toDeletePaths.add(fullPath);
                } else {
                    keptArrow++;
                }
            }
            continue;
        }

        if (entry.endsWith('.json')) {
            jsonFiles.push({path: fullPath, size: stat.size});
        }
    }

    // Delete JSON files only if no arrow files will remain in this directory
    const noArrowRemaining = keptArrow + keptArrowGz === 0;
    if (noArrowRemaining) {
        for (const j of jsonFiles) {
            toDelete.push({path: j.path, size: j.size, reason: 'no arrow files remain'});
            toDeletePaths.add(j.path);
        }
    }

    const keptJson = noArrowRemaining ? 0 : jsonFiles.length;

    // Collect stale symlinks: currently broken OR pointing to a file we're deleting
    const staleSymlinks: string[] = [];
    for (const fullPath of symlinksToCheck) {
        const target = resolve(dir, readlinkSync(fullPath));
        const currentlyBroken = (() => { try { statSync(fullPath); return false; } catch { return true; } })();
        if (currentlyBroken || toDeletePaths.has(target)) {
            staleSymlinks.push(fullPath);
        }
    }

    const deletedArrow = toDelete.filter(f => f.path.endsWith('.arrow') && !f.path.endsWith('.arrow.gz')).length;
    const deletedArrowGz = toDelete.filter(f => f.path.endsWith('.arrow.gz')).length;
    const deletedJson = toDelete.filter(f => f.path.endsWith('.json')).length;
    const deletedBytes = toDelete.reduce((s, f) => s + f.size, 0);
    const totalItems = toDelete.length + staleSymlinks.length;

    if (totalItems === 0) return {...zero, keptArrow, keptArrowGz, keptJson};

    function dirSummary(bytes: number, symlinks: number): string {
        const parts: string[] = [];
        if (deletedArrowGz) parts.push(`${deletedArrowGz} .arrow.gz`);
        if (deletedArrow) parts.push(`${deletedArrow} .arrow`);
        if (deletedJson) parts.push(`${deletedJson} .json`);
        if (symlinks) parts.push(`${symlinks} symlinks`);
        return `${parts.join(', ')} (${fmtSize(bytes)})`;
    }

    if (isDryRun) {
        if (verbose) {
            console.log(`\n${dir}:`);
            for (const f of toDelete) console.log(`  ${fmtSize(f.size).padStart(7)}  [${f.reason}]  ${f.path}`);
            for (const s of staleSymlinks) console.log(`  [symlink]  ${s}`);
        } else {
            console.log(`${dir}: ${dirSummary(deletedBytes, staleSymlinks.length)}`);
        }
        return {deletedArrow, deletedArrowGz, deletedJson, deletedBytes, deletedSymlinks: staleSymlinks.length, failed: 0, keptArrow, keptArrowGz, keptJson};
    }

    // Delete mode: remove files immediately, then stale symlinks
    let actualDeleted = 0;
    let failed = 0;
    for (const f of toDelete) {
        try { unlinkSync(f.path); actualDeleted++; } catch (e) { console.error(`  Failed: ${f.path}: ${e}`); failed++; }
    }
    for (const s of staleSymlinks) {
        try { unlinkSync(s); actualDeleted++; } catch (e) { console.error(`  Failed: ${s}: ${e}`); failed++; }
    }
    console.log(`${dir}: ${dirSummary(deletedBytes, staleSymlinks.length)} removed${failed ? ` — ${failed} FAILED` : ''}`);

    return {deletedArrow, deletedArrowGz, deletedJson, deletedBytes, deletedSymlinks: staleSymlinks.length, failed, keptArrow, keptArrowGz, keptJson};
}

async function main() {
    if (isDryRun) console.log(`Dry run — pass --delete to actually remove files${verbose ? '' : ' (use --verbose for per-file detail)'}\n`);

    const dirs = [basePath];
    for (const entry of readdirSync(basePath)) {
        const fullPath = join(basePath, entry);
        if (lstatSync(fullPath).isDirectory()) dirs.push(fullPath);
    }

    let totals: DirResult = {deletedArrow: 0, deletedArrowGz: 0, deletedJson: 0, deletedBytes: 0, deletedSymlinks: 0, failed: 0, keptArrow: 0, keptArrowGz: 0, keptJson: 0};
    let dirCount = 0;

    for (const dir of dirs) {
        const r = await processDir(dir);
        for (const k of Object.keys(totals) as (keyof DirResult)[]) (totals[k] as number) += r[k];
        dirCount++;
        if (!isDryRun && dirCount % 100 === 0) {
            console.log(`  [${dirCount}/${dirs.length} dirs processed]`);
        }
    }

    const action = isDryRun ? 'would be removed' : 'removed';
    console.log(`\n--- ${isDryRun ? 'Dry run' : 'Done'} ---`);
    console.log(`Deleted  (${action}): ${totals.deletedArrowGz} .arrow.gz, ${totals.deletedArrow} .arrow, ${totals.deletedJson} .json, ${totals.deletedSymlinks} symlinks — ${fmtSize(totals.deletedBytes)}`);
    console.log(`Kept:                 ${totals.keptArrowGz} .arrow.gz, ${totals.keptArrow} .arrow, ${totals.keptJson} .json`);
    if (totals.failed) console.log(`Failures: ${totals.failed}`);
    if (isDryRun) console.log(`Run with --delete to proceed.`);
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
