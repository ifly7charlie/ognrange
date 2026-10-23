#!/usr/bin/env npx ts-node
import dotenv from 'dotenv';
dotenv.config({path: '.env.local', override: true});

import {readdir, readFile, writeFile, lstat} from 'fs/promises';
import {join, basename} from 'path';
import {OUTPUT_PATH} from '../lib/common/config';
import yargs from 'yargs';

/**
 * Migrates per-station day JSON files from the old cumulative stats format to
 * per-day deltas matching the Rust server output.
 *
 * Old format (TypeScript server):
 *   - stats.count = accepted packets (cumulative since server startup)
 *   - no stats.accepted field
 *   - all rejection counters are cumulative
 *
 * New format (Rust server):
 *   - stats.count = all packets for that day (raw)
 *   - stats.accepted = accepted packets for that day
 *   - rejection counters are per-day
 *
 * Migration: walk day files chronologically, compute deltas between consecutive
 * cumulative snapshots, then set accepted = delta_count and recompute count as
 * accepted + sum(delta rejections). A baseline file is saved per station so
 * incremental re-runs can pick up where the last run left off.
 */

const STAT_FIELDS = [
    'count',
    'ignoredTracker',
    'invalidTracker',
    'invalidTimestamp',
    'ignoredStationary',
    'ignoredSignal0',
    'ignoredH3stationary',
    'ignoredElevation',
    'ignoredFutureTimestamp',
    'ignoredStaleTimestamp',
] as const;

const REJECTION_FIELDS = STAT_FIELDS.filter((f) => f !== 'count');

const BASELINE_FILE = '.migration-baseline.json';

const args = yargs(process.argv.slice(2))
    .option('apply', {type: 'boolean', default: false, description: 'Apply changes (default is dry-run)'})
    .option('path', {type: 'string', description: 'Override OUTPUT_PATH'})
    .option('station', {type: 'string', description: 'Migrate a single station only'})
    .option('concurrency', {type: 'number', default: 4, description: 'Number of stations to process in parallel'})
    .help()
    .alias('help', 'h')
    .parseSync();

const basePath = (args.path ?? OUTPUT_PATH).replace(/\/?$/, '/');
const isDryRun = !args.apply;

// Matches: {station}.day.{date}.json
const dayFilePattern = /^(.+)\.day\.(\d{4}-\d{2}-\d{2})\.json$/;

let totalScanned = 0;
let totalMigrated = 0;
let totalErrors = 0;

type CumulativeStats = Record<(typeof STAT_FIELDS)[number], number>;

function emptyStats(): CumulativeStats {
    const s = {} as any;
    for (const f of STAT_FIELDS) s[f] = 0;
    return s;
}

function extractCumulative(stats: any): CumulativeStats {
    const s = {} as any;
    for (const f of STAT_FIELDS) s[f] = stats[f] ?? 0;
    return s;
}

async function loadBaseline(stationDir: string): Promise<CumulativeStats | null> {
    try {
        const raw = await readFile(join(stationDir, BASELINE_FILE), 'utf8');
        return JSON.parse(raw) as CumulativeStats;
    } catch {
        return null;
    }
}

async function saveBaseline(stationDir: string, cumulative: CumulativeStats): Promise<void> {
    try {
        await writeFile(join(stationDir, BASELINE_FILE), JSON.stringify(cumulative) + '\n', 'utf8');
    } catch (e) {
        console.error(`  ERROR saving baseline ${stationDir}: ${e}`);
    }
}

async function processStation(stationDir: string): Promise<void> {
    let entries: string[];
    try {
        entries = await readdir(stationDir);
    } catch {
        return;
    }

    // Collect day files and sort chronologically (ascending)
    const dayFiles = entries
        .map((e) => {
            const m = e.match(dayFilePattern);
            return m ? {name: e, date: m[2]} : null;
        })
        .filter((x): x is {name: string; date: string} => x !== null)
        .sort((a, b) => a.date.localeCompare(b.date));

    if (dayFiles.length === 0) return;

    const stationName = basename(stationDir);
    let stationMigrated = 0;
    let stationErrors = 0;
    let stationScanned = 0;

    // Load baseline from previous migration run, or start from zeros
    let prev = (await loadBaseline(stationDir)) ?? emptyStats();
    let lastCumulative: CumulativeStats | null = null;

    for (const {name} of dayFiles) {
        const filePath = join(stationDir, name);
        stationScanned++;

        let raw: string;
        let json: any;
        try {
            raw = await readFile(filePath, 'utf8');
            json = JSON.parse(raw);
        } catch (e) {
            console.error(`  ERROR reading ${filePath}: ${e}`);
            stationErrors++;
            continue;
        }

        const stats = json.stats;
        if (!stats || typeof stats !== 'object') continue;

        // Already migrated or from Rust server — stop processing this station
        if ('accepted' in stats) break;

        // Extract cumulative values before we modify anything
        const cumulative = extractCumulative(stats);

        // Detect DB wipe: if any field went backwards, treat current as fresh
        const anyNegative = STAT_FIELDS.some((f) => cumulative[f] < prev[f]);
        if (anyNegative) {
            prev = emptyStats();
        }

        // Compute per-day deltas
        const delta = {} as CumulativeStats;
        for (const f of STAT_FIELDS) {
            delta[f] = cumulative[f] - prev[f];
        }

        // accepted = delta of old count (which tracked accepted packets)
        const accepted = delta.count;
        stats.accepted = accepted;

        // Overwrite each field with its daily delta
        for (const f of REJECTION_FIELDS) {
            stats[f] = delta[f];
        }

        // count = accepted + sum(daily rejection deltas)
        let rejected = 0;
        for (const f of REJECTION_FIELDS) {
            rejected += delta[f];
        }
        stats.count = accepted + rejected;

        // Remove vestigial field
        delete stats.ignoredPAW;

        if (!stats.hourly) {
            stats.hourly = {};
        }

        // Update prev for next file
        prev = cumulative;
        lastCumulative = cumulative;

        if (!isDryRun) {
            try {
                await writeFile(filePath, JSON.stringify(json, null, 2) + '\n', 'utf8');
            } catch (e) {
                console.error(`  ERROR writing ${filePath}: ${e}`);
                stationErrors++;
                continue;
            }
        }
        stationMigrated++;
    }

    // Save baseline for incremental re-runs
    if (lastCumulative && !isDryRun) {
        await saveBaseline(stationDir, lastCumulative);
    }

    totalScanned += stationScanned;
    totalMigrated += stationMigrated;
    totalErrors += stationErrors;

    if (stationMigrated > 0) {
        console.log(`${isDryRun ? '[dry-run] ' : ''}${stationName}: ${stationMigrated} file${stationMigrated !== 1 ? 's' : ''}`);
    }
}

async function runPool(items: string[], concurrency: number): Promise<void> {
    let index = 0;
    async function worker(): Promise<void> {
        while (index < items.length) {
            const i = index++;
            await processStation(items[i]);
        }
    }
    await Promise.all(Array.from({length: Math.min(concurrency, items.length)}, () => worker()));
}

async function main(): Promise<void> {
    if (isDryRun) {
        console.log('Dry-run mode — pass --apply to write changes');
    }

    if (args.station) {
        await processStation(join(basePath, args.station));
    } else {
        console.log(`Scanning: ${basePath}`);
        let dirs: string[];
        try {
            dirs = await readdir(basePath);
        } catch (e) {
            console.error(`Cannot read OUTPUT_PATH ${basePath}: ${e}`);
            process.exit(1);
        }

        const stationDirs: string[] = [];
        for (const entry of dirs) {
            const full = join(basePath, entry);
            try {
                if (!(await lstat(full)).isDirectory()) continue;
            } catch {
                continue;
            }
            stationDirs.push(full);
        }

        await runPool(stationDirs, args.concurrency);
    }

    console.log(`\nDone: ${totalScanned} scanned, ${totalMigrated} ${isDryRun ? 'would migrate' : 'migrated'}, ${totalErrors} errors`);
    if (isDryRun && totalMigrated > 0) {
        console.log('Run with --apply to write changes.');
    }
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
