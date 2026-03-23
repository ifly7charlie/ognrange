#!/usr/bin/env npx ts-node
import dotenv from 'dotenv';
dotenv.config({path: '.env.local', override: true});

import {readdirSync, readFileSync, writeFileSync, lstatSync} from 'fs';
import {join} from 'path';
import {OUTPUT_PATH} from '../lib/common/config';
import yargs from 'yargs';

/**
 * Migrates per-station day JSON files from the old stats format to the new format.
 *
 * Old format (TypeScript server): stats.count = accepted packets, no stats.accepted field
 * New format (Rust server):       stats.count = all packets (raw), stats.accepted = accepted packets
 *
 * Migration: if stats.accepted is absent, the old stats.count was the accepted count.
 * We set accepted = old count, then recompute count = accepted + all rejection counters
 * (the true raw count is unknown; this is the best approximation available).
 */

const args = yargs(process.argv.slice(2))
    .option('apply', {type: 'boolean', default: false, description: 'Apply changes (default is dry-run)'})
    .option('path', {type: 'string', description: 'Override OUTPUT_PATH'})
    .option('station', {type: 'string', description: 'Migrate a single station only'})
    .help()
    .alias('help', 'h')
    .parseSync();

const basePath = (args.path ?? OUTPUT_PATH).replace(/\/?$/, '/');
const isDryRun = !args.apply;

// Matches: {station}.day.{date}.json
const dayFilePattern = /^(.+)\.day\.(\d{4}-\d{2}-\d{2})\.json$/;

let scanned = 0;
let migrated = 0;
let skipped = 0;
let errors = 0;

function migrateFile(filePath: string): void {
    scanned++;
    let raw: string;
    let json: any;
    try {
        raw = readFileSync(filePath, 'utf8');
        json = JSON.parse(raw);
    } catch (e) {
        console.error(`  ERROR reading ${filePath}: ${e}`);
        errors++;
        return;
    }

    const stats = json.stats;
    if (!stats || typeof stats !== 'object') {
        skipped++;
        return;
    }

    // Already migrated: accepted key is present
    if ('accepted' in stats) {
        skipped++;
        return;
    }

    // Old format: count was the accepted counter
    const accepted = stats.count ?? 0;
    stats.accepted = accepted;
    // count is now the raw counter — approximate as accepted + all rejection counters
    const rejected =
        (stats.ignoredTracker ?? 0) +
        (stats.invalidTracker ?? 0) +
        (stats.invalidTimestamp ?? 0) +
        (stats.ignoredStationary ?? 0) +
        (stats.ignoredSignal0 ?? 0) +
        (stats.ignoredPAW ?? 0) +
        (stats.ignoredH3stationary ?? 0) +
        (stats.ignoredElevation ?? 0) +
        (stats.ignoredFutureTimestamp ?? 0) +
        (stats.ignoredStaleTimestamp ?? 0);
    stats.count = accepted + rejected;
    if (!stats.hourly) {
        stats.hourly = {};
    }

    console.log(`  ${isDryRun ? '[dry-run] ' : ''}${filePath}: count=${accepted + rejected} (accepted=${accepted} + rejected=${rejected})`);

    if (!isDryRun) {
        try {
            writeFileSync(filePath, JSON.stringify(json, null, 2) + '\n', 'utf8');
        } catch (e) {
            console.error(`  ERROR writing ${filePath}: ${e}`);
            errors++;
            return;
        }
    }
    migrated++;
}

function processStation(stationDir: string): void {
    let entries: string[];
    try {
        entries = readdirSync(stationDir);
    } catch {
        return;
    }
    for (const entry of entries) {
        if (!dayFilePattern.test(entry)) continue;
        migrateFile(join(stationDir, entry));
    }
}

if (isDryRun) {
    console.log('Dry-run mode — pass --apply to write changes');
}

if (args.station) {
    const stationDir = join(basePath, args.station);
    console.log(`Migrating station: ${stationDir}`);
    processStation(stationDir);
} else {
    console.log(`Scanning: ${basePath}`);
    let dirs: string[];
    try {
        dirs = readdirSync(basePath);
    } catch (e) {
        console.error(`Cannot read OUTPUT_PATH ${basePath}: ${e}`);
        process.exit(1);
    }
    for (const entry of dirs) {
        const full = join(basePath, entry);
        try {
            if (!lstatSync(full).isDirectory()) continue;
        } catch {
            continue;
        }
        processStation(full);
    }
}

console.log(`\nDone: ${scanned} scanned, ${migrated} ${isDryRun ? 'would migrate' : 'migrated'}, ${skipped} already new format, ${errors} errors`);
if (isDryRun && migrated > 0) {
    console.log('Run with --apply to write changes.');
}
