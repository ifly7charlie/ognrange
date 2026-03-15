import {readdirSync, readFileSync, realpathSync} from 'fs';
import {join} from 'path';

import {OUTPUT_PATH, ROLLUP_PERIOD_MINUTES} from '../../../../lib/common/config';
import {dateBounds} from '../../../../lib/common/datebounds';
import {ignoreStation} from '../../../../lib/common/ignorestation';

interface StationJson {
    id?: number;
    station?: string;
    lat?: number | null;
    lng?: number | null;
    status?: string;
    notice?: string;
    moved?: boolean;
    bouncing?: boolean;
    valid?: boolean;
    outputEpoch?: number;
    outputDate?: string;
    lastOutputEpoch?: number;
    lastOutputFile?: number;
    lastPacket?: number;
    lastLocation?: number;
    lastBeacon?: number;
    layerMask?: number;
    stats?: Record<string, number>;
    activity?: {
        ranges: {start: number; end: number; cells: number}[];
        totalRollups: number;
        activeRollups: number;
        totalCells: number;
        firstSeen: number;
        lastSeen: number;
        lastRollup: number;
    };
    beaconActivity?: string;
    beaconActivityDate?: string;
    uptime?: number | null;
    arrowRecords?: number;
}

// Match per-station daily JSON files: {station}.day.YYYY-MM-DD.json or with layer suffix
const dailyFilePattern = /\.day\.(\d{4}-\d{2}-\d{2})(?:\.\w+)?\.json$/;

/** Check if the requested file resolves to the same target as {station}.json (the latest symlink) */
function isLatestFile(stationDir: string, stationName: string, file: string): boolean {
    try {
        const latestReal = realpathSync(join(stationDir, `${stationName}.json`));
        const fileReal = realpathSync(join(stationDir, `${stationName}.${file}.json`));
        return latestReal === fileReal;
    } catch {
        return false;
    }
}

function readStationFile(filePath: string): StationJson | null {
    try {
        const raw = readFileSync(filePath, 'utf-8');
        return JSON.parse(raw);
    } catch {
        return null;
    }
}

function aggregateStationData(files: {date: string; data: StationJson}[]): StationJson {
    if (files.length === 0) return {};
    if (files.length === 1) return files[0].data;

    // Sort by date ascending so "latest" is last
    const sorted = [...files].sort((a, b) => a.date.localeCompare(b.date));
    const latest = sorted[sorted.length - 1].data;

    const result: StationJson = {
        // Latest file fields
        id: latest.id,
        station: latest.station,
        lat: latest.lat,
        lng: latest.lng,
        status: latest.status,
        notice: latest.notice,
        moved: latest.moved,
        bouncing: latest.bouncing,
        valid: latest.valid,
        outputEpoch: latest.outputEpoch,
        outputDate: latest.outputDate,
        lastOutputEpoch: latest.lastOutputEpoch,
        lastOutputFile: latest.lastOutputFile
    };

    // Max fields
    let maxLastPacket = 0;
    let maxLastLocation = 0;
    let maxLastBeacon = 0;

    // Bitwise OR for layerMask
    let layerMask = 0;

    // Sum stats
    const sumStats: Record<string, number> = {};

    // Activity aggregation
    let allRanges: {start: number; end: number; cells: number}[] = [];
    let totalRollups = 0;
    let activeRollups = 0;
    let totalCells = 0;
    let firstSeen = Infinity;
    let lastSeen = 0;
    let lastRollup = 0;

    // Uptime: average excluding null
    let uptimeSum = 0;
    let uptimeCount = 0;

    // Beacon activity: collect per-day entries for multi-day view
    const beaconActivityDays: {date: string; bitvector: string}[] = [];

    for (const {date, data} of sorted) {
        if (data.lastPacket && data.lastPacket > maxLastPacket) maxLastPacket = data.lastPacket;
        if (data.lastLocation && data.lastLocation > maxLastLocation) maxLastLocation = data.lastLocation;
        if (data.lastBeacon && data.lastBeacon > maxLastBeacon) maxLastBeacon = data.lastBeacon;

        if (data.layerMask) layerMask |= data.layerMask;

        if (data.stats) {
            for (const [key, val] of Object.entries(data.stats)) {
                sumStats[key] = (sumStats[key] ?? 0) + val;
            }
        }

        if (data.activity) {
            allRanges.push(...data.activity.ranges);
            totalRollups += data.activity.totalRollups;
            activeRollups += data.activity.activeRollups;
            totalCells += data.activity.totalCells;
            if (data.activity.firstSeen && data.activity.firstSeen < firstSeen) firstSeen = data.activity.firstSeen;
            if (data.activity.lastSeen && data.activity.lastSeen > lastSeen) lastSeen = data.activity.lastSeen;
            if (data.activity.lastRollup && data.activity.lastRollup > lastRollup) lastRollup = data.activity.lastRollup;
        }

        if (data.uptime != null) {
            uptimeSum += data.uptime;
            uptimeCount++;
        }

        if (data.beaconActivity) {
            beaconActivityDays.push({date: data.beaconActivityDate || date, bitvector: data.beaconActivity});
        }
    }

    if (maxLastPacket) result.lastPacket = maxLastPacket;
    if (maxLastLocation) result.lastLocation = maxLastLocation;
    if (maxLastBeacon) result.lastBeacon = maxLastBeacon;
    if (layerMask) result.layerMask = layerMask;
    if (Object.keys(sumStats).length) result.stats = sumStats;

    // Sort ranges by start time
    allRanges.sort((a, b) => a.start - b.start);

    if (totalRollups > 0 || allRanges.length > 0) {
        result.activity = {
            ranges: allRanges,
            totalRollups,
            activeRollups,
            totalCells,
            firstSeen: firstSeen === Infinity ? 0 : firstSeen,
            lastSeen,
            lastRollup
        };
    }

    if (uptimeCount > 0) {
        result.uptime = Math.round((uptimeSum / uptimeCount) * 10) / 10;
    }

    // For multi-day, return array of beacon activity days
    if (beaconActivityDays.length > 1) {
        (result as any).beaconActivityDays = beaconActivityDays;
    } else if (beaconActivityDays.length === 1) {
        result.beaconActivity = beaconActivityDays[0].bitvector;
        result.beaconActivityDate = beaconActivityDays[0].date;
    }

    return result;
}

export default async function handler(req, res) {
    const stationName: string = req.query.station;

    if (ignoreStation(stationName)) {
        res.status(404).json({error: 'invalid station name'});
        return;
    }

    const dateStart = (req.query.dateStart as string) || '';
    const dateEnd = (req.query.dateEnd as string) || '';
    const file = (req.query.file as string) || '';

    const stationDir = join(OUTPUT_PATH, stationName);

    // Determine which file to look up — prefer explicit file param, fall back to dateStart
    const fileParam = file || dateStart;

    if (!dateStart || dateStart === dateEnd) {
        if (fileParam && !isLatestFile(stationDir, stationName, fileParam)) {
            // Historical period — return only the dated file data (no station metadata)
            const datedData = readStationFile(join(stationDir, `${stationName}.${fileParam}.json`));
            res.setHeader('Cache-Control', `public, s-maxage=${ROLLUP_PERIOD_MINUTES * 60}, stale-while-revalidate=300`);
            res.status(200).json(datedData ?? {});
            return;
        }

        // Latest/current period — return full station metadata
        const latestData = readStationFile(join(stationDir, `${stationName}.json`));
        res.setHeader('Cache-Control', `public, s-maxage=${ROLLUP_PERIOD_MINUTES * 60}, stale-while-revalidate=300`);
        res.status(200).json(latestData ?? {});
        return;
    }

    // Range mode: list daily files and aggregate activity data only
    const startBounds = dateBounds(dateStart);
    const endBounds = dateBounds(dateEnd || dateStart);
    const rangeStart = startBounds?.start || '0000-00-00';
    const rangeEnd = endBounds?.end || '9999-99-99';

    let dirFiles: string[];
    try {
        dirFiles = readdirSync(stationDir);
    } catch {
        res.setHeader('Cache-Control', `public, s-maxage=${ROLLUP_PERIOD_MINUTES * 60}, stale-while-revalidate=300`);
        res.status(200).json({});
        return;
    }

    // Find daily JSON files within the date range
    const dailyFiles = dirFiles
        .map((f) => {
            const m = f.match(dailyFilePattern);
            return m ? {file: f, date: m[1]} : null;
        })
        .filter(Boolean)
        .filter((d) => d.date >= rangeStart && d.date <= rangeEnd)
        .sort((a, b) => a.date.localeCompare(b.date));

    const allData: {date: string; data: StationJson}[] = [];
    for (const day of dailyFiles) {
        const data = readStationFile(join(stationDir, day.file));
        if (data) {
            allData.push({date: day.date, data});
        }
    }

    const aggregated = aggregateStationData(allData);

    res.setHeader('Cache-Control', `public, s-maxage=${ROLLUP_PERIOD_MINUTES * 60}, stale-while-revalidate=300`);
    res.status(200).json(aggregated);
}
