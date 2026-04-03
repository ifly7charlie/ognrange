import {readdirSync} from 'fs';
import {join} from 'path';

import {OUTPUT_PATH, ROLLUP_PERIOD_MINUTES} from '../../lib/common/config';
import {dateBounds} from '../../lib/common/datebounds';
import {readJsonFile} from '../../lib/common/statsio';
import type {AprsPacketStatsJson, StationStatsApiResponse} from '../../lib/common/stationstats';

const statsDir = join(OUTPUT_PATH, 'stats');

// Match dated daily files: station-stats.YYYY-MM-DD.json.gz
const dailyFilePattern = /^station-stats\.(\d{4}-\d{2}-\d{2})\.json\.gz$/;

function todayDate(): string {
    return new Date().toISOString().slice(0, 10);
}

function aggregateStatsFiles(
    files: {date: string; stats: AprsPacketStatsJson}[]
): AprsPacketStatsJson {
    let count = 0, accepted = 0, delaySumSecs = 0;
    let ignoredTracker = 0, invalidTracker = 0, invalidTimestamp = 0;
    let ignoredStationary = 0, ignoredSignal0 = 0;
    let ignoredH3stationary = 0, ignoredElevation = 0;
    let ignoredFutureTimestamp = 0, ignoredStaleTimestamp = 0;
    const hourly: Record<string, number[]> = {};
    let startTime = '';
    let generated = '';
    let totalUptime = 0;

    for (const {stats} of files) {
        if (!startTime || stats.startTime < startTime) startTime = stats.startTime;
        if (!generated || stats.generated > generated) generated = stats.generated;
        totalUptime += stats.uptimeSeconds;
        count += stats.count ?? 0;
        accepted += stats.accepted ?? 0;
        delaySumSecs += stats.delaySumSecs ?? 0;
        ignoredTracker += stats.ignoredTracker ?? 0;
        invalidTracker += stats.invalidTracker ?? 0;
        invalidTimestamp += stats.invalidTimestamp ?? 0;
        ignoredStationary += stats.ignoredStationary ?? 0;
        ignoredSignal0 += stats.ignoredSignal0 ?? 0;
        ignoredH3stationary += stats.ignoredH3stationary ?? 0;
        ignoredElevation += stats.ignoredElevation ?? 0;
        ignoredFutureTimestamp += stats.ignoredFutureTimestamp ?? 0;
        ignoredStaleTimestamp += stats.ignoredStaleTimestamp ?? 0;

        for (const [layer, hours] of Object.entries(stats.hourly ?? {})) {
            if (!hourly[layer]) hourly[layer] = new Array(24).fill(0);
            for (let i = 0; i < hours.length && i < 24; i++) {
                hourly[layer][i] += hours[i];
            }
        }
    }

    return {
        generated: generated || new Date().toISOString(),
        startTime: startTime || new Date().toISOString(),
        uptimeSeconds: totalUptime,
        count, accepted, delaySumSecs,
        ignoredTracker, invalidTracker, invalidTimestamp,
        ignoredStationary, ignoredSignal0,
        ignoredH3stationary, ignoredElevation,
        ignoredFutureTimestamp, ignoredStaleTimestamp,
        hourly
    };
}

/** Truncate future hours in today's hourly data */
function truncateFutureHours(hourly: Record<string, number[]>): Record<string, number[]> {
    const currentHour = new Date().getUTCHours();
    const result: Record<string, number[]> = {};
    for (const [layer, hours] of Object.entries(hourly)) {
        result[layer] = hours.map((v, i) => (i <= currentHour ? v : 0));
    }
    return result;
}

export default async function handler(req: any, res: any) {
    const response: StationStatsApiResponse = {
        current: null,
        hourlyHistory: [],
        dailyAccepted: []
    };

    let files: string[];
    try {
        files = readdirSync(statsDir);
    } catch {
        res.setHeader('Cache-Control', `public, max-age=${ROLLUP_PERIOD_MINUTES * 60}, s-maxage=${ROLLUP_PERIOD_MINUTES * 60}, stale-while-revalidate=300`);
        res.status(200).json(response);
        return;
    }

    const dateStart = (req.query.dateStart as string) || '';
    const dateEnd = (req.query.dateEnd as string) || '';
    const hasRange = !!dateStart;

    // Collect all dated daily files sorted by date descending
    const allDated = files
        .map((f) => {
            const m = f.match(dailyFilePattern);
            return m ? {file: f, date: m[1]} : null;
        })
        .filter(Boolean)
        .sort((a, b) => b!.date.localeCompare(a!.date)) as {file: string; date: string}[];

    const today = todayDate();

    let rangeStart = '0000-00-00';
    let rangeEnd = '9999-99-99';
    if (hasRange) {
        const startBounds = dateBounds(dateStart);
        const endBounds = dateBounds(dateEnd || dateStart);
        rangeStart = startBounds?.start || rangeStart;
        rangeEnd = endBounds?.end || rangeEnd;
    }

    // Filter files to the date range
    const inRange = allDated.filter(({date}) => date >= rangeStart && date <= rangeEnd);

    // Always read the live symlink for today's current data
    let liveStats: AprsPacketStatsJson | null = null;
    if (today >= rangeStart && today <= rangeEnd) {
        liveStats = readJsonFile<AprsPacketStatsJson>(join(statsDir, 'station-stats.json.gz'));
    }

    // Collect stats per date (live overrides the daily file for today)
    const statsEntries: {date: string; stats: AprsPacketStatsJson}[] = [];
    for (const {file, date} of inRange) {
        let stats: AprsPacketStatsJson | null;
        if (date === today && liveStats) {
            stats = liveStats;
        } else {
            stats = readJsonFile<AprsPacketStatsJson>(join(statsDir, file));
        }
        if (stats) statsEntries.push({date, stats});
    }

    if (liveStats && !inRange.some((e) => e.date === today)) {
        statsEntries.push({date: today, stats: liveStats});
        statsEntries.sort((a, b) => b.date.localeCompare(a.date));
    }

    // Build current aggregate
    if (statsEntries.length > 0) {
        const aggregated = aggregateStatsFiles(statsEntries);
        // Truncate future hours for today's data
        if (statsEntries.length === 1 && statsEntries[0].date === today) {
            aggregated.hourly = truncateFutureHours(aggregated.hourly);
        }
        response.current = aggregated;
    }

    // Build hourlyHistory and dailyAccepted from individual days (oldest first)
    const sorted = [...statsEntries].sort((a, b) => a.date.localeCompare(b.date));
    for (const {date, stats} of sorted) {
        const hourly = date === today ? truncateFutureHours(stats.hourly ?? {}) : (stats.hourly ?? {});
        response.hourlyHistory.push({date, hourly});
        response.dailyAccepted.push({date, accepted: stats.accepted ?? 0, count: stats.count ?? 0});
    }

    res.setHeader('Cache-Control', `public, max-age=${ROLLUP_PERIOD_MINUTES * 60}, s-maxage=${ROLLUP_PERIOD_MINUTES * 60}, stale-while-revalidate=300`);
    res.status(200).json(response);
}
