import dotenv from 'dotenv';
dotenv.config({path: '.env.local', override: true});

import {ClassicLevel} from 'classic-level';
import {rename, readFile} from 'fs/promises';
import {existsSync} from 'fs';

import {DB_PATH, OUTPUT_PATH} from '../lib/common/config';
import type {StationDetails} from '../lib/bin/stationstatus';

main().then(() => process.exit(0)).catch((e) => {
    console.error('Fatal:', e.message ?? e);
    process.exit(1);
});

async function main() {
    const dryRun = process.argv.includes('--dry-run');
    if (dryRun) {
        console.log('Dry run — no changes will be written.');
    }

    const jsonPath = OUTPUT_PATH + 'stations-complete.json';
    if (!existsSync(jsonPath)) {
        throw new Error(`stations-complete.json not found at ${jsonPath}\nCheck OUTPUT_PATH is set correctly.`);
    }

    console.log(`Reading station data from ${jsonPath}...`);
    const stations: StationDetails[] = JSON.parse(await readFile(jsonPath, 'utf8'));
    console.log(`Found ${stations.length} stations.`);

    const dbPath = DB_PATH + 'status';
    const backupPath = dbPath + '.corrupt';

    if (existsSync(dbPath)) {
        if (dryRun) {
            console.log(`Would rename ${dbPath} → ${backupPath}`);
        } else {
            if (existsSync(backupPath)) {
                throw new Error(`Backup path ${backupPath} already exists — remove it manually before retrying.`);
            }
            console.log(`Renaming existing ${dbPath} → ${backupPath}`);
            await rename(dbPath, backupPath);
        }
    }

    if (dryRun) {
        console.log(`Would write ${stations.length} stations to ${dbPath}`);
        return;
    }

    const db = new ClassicLevel<string, object>(dbPath, {valueEncoding: 'json', createIfMissing: true});
    await db.open();

    let written = 0;
    let failed = 0;
    const batch = db.batch();
    for (const station of stations) {
        const key = station.station as string;
        if (!key) {
            console.warn('Skipping station with no name:', station);
            failed++;
            continue;
        }
        batch.put(key, station);
        written++;
    }
    await batch.write();
    await db.close();

    console.log(`Recovery complete: ${written} stations written to ${dbPath}`);
    if (failed > 0) {
        console.warn(`${failed} stations skipped due to missing name.`);
    }
    console.log(`The previous database was moved to ${backupPath} — delete it once you have confirmed the server starts correctly.`);
}
