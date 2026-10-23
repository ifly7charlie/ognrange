#!/usr/bin/env npx ts-node
//
// Purge beacon/uptime data corrupted by the midnight archive race (fixed in the
// aprs-server): at day rollover the live bitvectors reset before the rollup
// archived the completed day, so:
//  - stats/global-uptime.<date>.json[.gz] files contain the NEXT day's first
//    slot (internal `date` field != filename date, activity "0100...", uptime 100)
//  - {station}/{station}.day.<date>.json files can carry a beaconActivityDate
//    of the following day with a near-empty bitvector
//
// Default is a dry-run report. --fix strips the bad station fields and deletes
// corrupt global-uptime files; add --rebuild to instead reconstruct each day's
// global uptime as the union of stations' (valid) beacon bitvectors for that
// day — the server was demonstrably up in any slot where any station's beacon
// was received.
//
// Rebuild only reads station files for dates whose global-uptime file is
// corrupt or missing, and stops scanning a date once the union is full or
// --quorum consecutive files add no new slots. Files it never reads are not
// checked for corrupt beacon fields — run --fix WITHOUT --rebuild for a
// complete station-file sweep.
//
import dotenv from 'dotenv';
dotenv.config({path: '.env.local', override: true});

import {readdirSync, readFileSync, writeFileSync, renameSync, unlinkSync, lstatSync, realpathSync, existsSync} from 'fs';
import {join} from 'path';
import {gzipSync, gunzipSync} from 'node:zlib';
import {OUTPUT_PATH} from '../lib/common/config';
import yargs from 'yargs';

const args = yargs(process.argv.slice(2)) //
    .option('fix', {type: 'boolean', default: false, description: 'Apply changes (default is dry-run)'})
    .option('rebuild', {type: 'boolean', default: false, description: 'Rebuild corrupt/missing global-uptime files from station beacon data instead of deleting them'})
    .option('quorum', {type: 'number', default: 50, description: 'Rebuild: stop scanning a date after this many consecutive station files add no new slots (0 = scan every file)'})
    .option('since', {type: 'string', description: 'Only process dates >= this (YYYY-MM-DD)'})
    .option('path', {type: 'string', description: 'Override OUTPUT_PATH'})
    .option('verbose', {alias: 'v', type: 'boolean', default: false, description: 'Show per-file detail'})
    .help()
    .alias('help', 'h')
    .parseSync();

const basePath = (args.path ?? OUTPUT_PATH).replace(/\/?$/, '/');
const isDryRun = !args.fix;
const today = new Date().toISOString().slice(0, 10);
const since = args.since ?? '0000-00-00';

const SLOT_BYTES = 18; // 144 slots
const FULL_SLOTS = 144;

function readMaybeGz(path: string): any | null {
    try {
        const raw = readFileSync(path);
        const text = path.endsWith('.gz') ? gunzipSync(raw).toString('utf8') : raw.toString('utf8');
        return JSON.parse(text);
    } catch {
        return null;
    }
}

function writeAtomic(path: string, content: Buffer | string) {
    const tmp = path + '.purge-tmp';
    writeFileSync(tmp, content);
    renameSync(tmp, path);
}

function realpathSafe(p: string): string {
    try {
        return realpathSync(p);
    } catch {
        return p;
    }
}

function popcount(bits: Buffer): number {
    let n = 0;
    for (const b of bits) {
        let v = b;
        while (v) {
            n += v & 1;
            v >>= 1;
        }
    }
    return n;
}

function orHexInto(bits: Buffer, hex: string) {
    for (let i = 0; i < SLOT_BYTES && i * 2 + 1 < hex.length; i++) {
        bits[i] |= parseInt(hex.substring(i * 2, i * 2 + 2), 16);
    }
}

// ---------------------------------------------------------------------------
// 1. Scan global-uptime dated files: internal date field must match filename.
// ---------------------------------------------------------------------------
const statsDir = join(basePath, 'stats');
const uptimePattern = /^global-uptime\.(\d{4}-\d{2}-\d{2})\.json(\.gz)?$/;

// Never delete whatever the live symlinks currently resolve to
const liveTargets = new Set<string>();
for (const live of ['global-uptime.json.gz', 'global-uptime.json']) {
    try {
        liveTargets.add(realpathSync(join(statsDir, live)));
    } catch {
        /* absent */
    }
}

interface UptimeFile {
    date: string;
    paths: string[]; // .json and/or .json.gz
    corrupt: boolean;
}
const uptimeFiles = new Map<string, UptimeFile>();
const liveSample: any = readMaybeGz(join(statsDir, 'global-uptime.json.gz'));

let statsFiles: string[] = [];
try {
    statsFiles = readdirSync(statsDir);
} catch {
    console.log(`No stats directory at ${statsDir}`);
}

for (const f of statsFiles) {
    const m = f.match(uptimePattern);
    if (!m) continue;
    const fileDate = m[1];
    if (fileDate >= today || fileDate < since) continue;

    const path = join(statsDir, f);
    const data = readMaybeGz(path);

    let entry = uptimeFiles.get(fileDate);
    if (!entry) {
        entry = {date: fileDate, paths: [], corrupt: false};
        uptimeFiles.set(fileDate, entry);
    }
    entry.paths.push(path);
    // Corrupt when the JSON covers a different day than the filename claims
    // (the archive race always stamped the new day's state into the old file).
    // Unreadable/undated files are left alone.
    if (data && data.date && data.date !== fileDate) {
        entry.corrupt = true;
    }
}

// ---------------------------------------------------------------------------
// 2. Index station day files (one readdir per station, no file reads yet)
// ---------------------------------------------------------------------------
interface DayFile {
    station: string;
    path: string;
}
const dateIndex = new Map<string, DayFile[]>();
const dayFilePattern = /\.day\.(\d{4}-\d{2}-\d{2})\.json$/;
let totalDayFiles = 0;

const dirEntries = readdirSync(basePath, {withFileTypes: true}).filter((d) => d.isDirectory() && d.name !== 'stations' && d.name !== 'stats');

for (const dir of dirEntries) {
    const stationDir = join(basePath, dir.name);
    let files: string[];
    try {
        files = readdirSync(stationDir);
    } catch {
        continue;
    }
    for (const f of files) {
        if (!f.startsWith(dir.name + '.day.')) continue;
        const m = f.match(dayFilePattern);
        if (!m) continue; // layer-suffixed metas etc
        const fileDate = m[1];
        if (fileDate >= today || fileDate < since) continue;
        let list = dateIndex.get(fileDate);
        if (!list) {
            list = [];
            dateIndex.set(fileDate, list);
        }
        list.push({station: dir.name, path: join(stationDir, f)});
        totalDayFiles++;
    }
}

// ---------------------------------------------------------------------------
// 3. Read + strip a single day file. Returns parsed data, or null when the
//    file is unreadable/a symlink/was corrupt (corrupt files carry no valid
//    activity for this date).
// ---------------------------------------------------------------------------
let stationFilesRead = 0;
let stationFilesCorrupt = 0;
let stationFilesFixed = 0;

function readAndStrip(df: DayFile, fileDate: string): any | null {
    try {
        if (!lstatSync(df.path).isFile()) return null; // skip symlinks
    } catch {
        return null;
    }
    const data = readMaybeGz(df.path);
    if (!data) return null;
    stationFilesRead++;

    const activityDate = data.beaconActivityDate;
    if (activityDate && activityDate !== fileDate) {
        stationFilesCorrupt++;
        if (args.verbose || isDryRun) {
            console.log(`  station ${df.station} day ${fileDate}: beaconActivityDate=${activityDate}${isDryRun ? ' (would strip)' : ''}`);
        }
        if (!isDryRun) {
            delete data.beaconActivity;
            delete data.beaconActivityDate;
            delete data.uptime;
            writeAtomic(df.path, JSON.stringify(data, null, 4));
            stationFilesFixed++;
        }
        return null;
    }
    return data;
}

// ---------------------------------------------------------------------------
// 4. Apply: rebuild scans only dates needing repair with a saturation
//    short-circuit; plain fix/dry-run sweeps every day file.
// ---------------------------------------------------------------------------
let globalCorrupt = 0;
let globalDeleted = 0;
let globalRebuilt = 0;
let skippedReads = 0;

const repairDates = new Set<string>();
for (const [date, entry] of uptimeFiles) {
    if (entry.corrupt) {
        globalCorrupt++;
        repairDates.add(date);
    }
}

if (args.rebuild) {
    // Dates with station data but no global-uptime file at all are rebuildable too
    for (const date of dateIndex.keys()) {
        if (!uptimeFiles.has(date)) repairDates.add(date);
    }

    for (const date of [...repairDates].sort()) {
        const files = dateIndex.get(date) ?? [];
        const union = Buffer.alloc(SLOT_BYTES);
        let setCount = 0;
        let noNew = 0;
        let read = 0;

        for (const df of files) {
            if (setCount === FULL_SLOTS || (args.quorum > 0 && noNew >= args.quorum)) break;
            read++;
            const data = readAndStrip(df, date);
            if (!data || !data.beaconActivity) continue;
            orHexInto(union, data.beaconActivity);
            const after = popcount(union);
            noNew = after > setCount ? 0 : noNew + 1;
            setCount = after;
        }
        skippedReads += files.length - read;

        const entry = uptimeFiles.get(date);
        if (setCount > 0) {
            const uptime = Math.round((setCount / FULL_SLOTS) * 1000) / 10;
            console.log(`  global-uptime ${date}: ${entry?.corrupt ? 'corrupt' : 'missing'} -> rebuild ${setCount}/144 slots (${uptime}%) from ${read}/${files.length} station files${isDryRun ? ' (dry-run)' : ''}`);
            if (!isDryRun) {
                const json = JSON.stringify(
                    {
                        generated: new Date().toISOString().replace(/\.\d+Z$/, 'Z'),
                        date,
                        server: liveSample?.server ?? '',
                        serverSoftware: liveSample?.serverSoftware ?? '',
                        serverAddress: liveSample?.serverAddress ?? '',
                        activity: union.toString('hex'),
                        uptime,
                        slot: FULL_SLOTS,
                        reconstructed: true
                    },
                    null,
                    2
                );
                writeAtomic(join(statsDir, `global-uptime.${date}.json.gz`), gzipSync(json));
                // Keep an uncompressed variant only where one already exists
                const plain = join(statsDir, `global-uptime.${date}.json`);
                if (entry?.paths.some((p) => !p.endsWith('.gz')) || existsSync(plain)) {
                    writeAtomic(plain, json);
                }
                globalRebuilt++;
            }
        } else if (entry?.corrupt) {
            // No station data to rebuild from — fall back to deleting
            deleteCorrupt(entry);
        }
    }
} else {
    // Full sweep: every day file gets its beacon fields verified
    for (const [date, files] of dateIndex) {
        for (const df of files) {
            readAndStrip(df, date);
        }
    }
    for (const date of [...repairDates].sort()) {
        const entry = uptimeFiles.get(date);
        if (entry) deleteCorrupt(entry);
    }
}

function deleteCorrupt(entry: UptimeFile) {
    for (const p of entry.paths) {
        if (liveTargets.has(realpathSafe(p))) {
            console.log(`  WARNING: live symlink points at corrupt ${p} — skipping`);
            continue;
        }
        console.log(`  global-uptime ${entry.date}: corrupt -> delete ${p}${isDryRun ? ' (dry-run)' : ''}`);
        if (!isDryRun) {
            unlinkSync(p);
            globalDeleted++;
        }
    }
}

console.log('');
console.log(`Indexed ${totalDayFiles} station day files across ${dirEntries.length} stations`);
console.log(`  read ${stationFilesRead}${args.rebuild ? ` (short-circuit skipped ${skippedReads} for repair dates, ${totalDayFiles - stationFilesRead - skippedReads} on dates not needing repair)` : ''}`);
console.log(`  corrupt beacon fields: ${stationFilesCorrupt}${isDryRun ? ' (dry-run, none changed)' : `, stripped ${stationFilesFixed}`}`);
console.log(`Global uptime days scanned: ${uptimeFiles.size}, corrupt: ${globalCorrupt}`);
if (isDryRun) {
    console.log('Dry-run only. Re-run with --fix to delete corrupt global-uptime files and strip bad station fields,');
    console.log('or --fix --rebuild to reconstruct global uptime from the union of station beacon activity.');
} else {
    console.log(`  deleted: ${globalDeleted}, rebuilt: ${globalRebuilt}`);
}
if (args.rebuild) {
    console.log('Note: --rebuild only reads station files on repair dates until saturation; skipped files were not');
    console.log('checked for corrupt beacon fields. Run --fix without --rebuild for a complete station-file sweep.');
}
