import {readdirSync, readFileSync, readlinkSync} from 'fs';
import {gunzipSync} from 'zlib';
import {join} from 'path';

import {OUTPUT_PATH, ROLLUP_PERIOD_MINUTES} from '../../lib/common/config';
import {dateBounds, parsePeriodParam} from '../../lib/common/datebounds';
import type {ProtocolStatsJson, ProtocolStatsApiResponse, DailyDevicesEntry, ProtocolEntry, GlobalUptimeData, GlobalUptimeHistoryEntry} from '../../lib/common/protocolstats';

const statsDir = join(OUTPUT_PATH, 'stats');

// Match dated daily files: protocol-stats.YYYY-MM-DD.json.gz
const dailyFilePattern = /^protocol-stats\.(\d{4}-\d{2}-\d{2})\.json\.gz$/;
const uptimeFilePattern = /^global-uptime\.(\d{4}-\d{2}-\d{2})\.json\.gz$/;

function readJsonFile<T>(filePath: string): T | null {
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

function readStatsFile(filePath: string): ProtocolStatsJson | null {
    return readJsonFile<ProtocolStatsJson>(filePath);
}

function readPeriodStatsFile(suffix: string): ProtocolStatsJson | null {
    return readStatsFile(join(statsDir, `protocol-stats.${suffix}.json.gz`));
}

function todayDate(): string {
    return new Date().toISOString().slice(0, 10);
}

function currentMonth(): string {
    return new Date().toISOString().slice(0, 7);
}

// Aggregate multiple days of stats into one summary.
// For `devices`, computes the average across days (rounded to nearest integer)
// rather than summing, since device counts are unique-per-period and summing
// would inflate the total across day boundaries.
function aggregateStats(statsList: {date: string; stats: ProtocolStatsJson}[]): ProtocolStatsJson {
    const protocols: Record<string, ProtocolEntry> = {};
    // Track per-day device counts per protocol for averaging
    const deviceDayCounts: Record<string, number[]> = {};
    const hourly: Record<string, number[]> = {};
    let totalRestarts = 0;
    let startTime = '';
    let generated = '';
    let totalUptime = 0;

    for (const {stats} of statsList) {
        if (!startTime || stats.startTime < startTime) startTime = stats.startTime;
        if (!generated || stats.generated > generated) generated = stats.generated;
        totalUptime += stats.uptimeSeconds;
        totalRestarts += stats.restarts;

        for (const [proto, entry] of Object.entries(stats.protocols)) {
            if (!protocols[proto]) {
                protocols[proto] = {raw: 0, accepted: 0, devices: 0, regions: {}, altitudes: {}};
                deviceDayCounts[proto] = [];
            }
            protocols[proto].raw += entry.raw;
            protocols[proto].accepted += entry.accepted;
            // Collect per-day device counts for later averaging
            deviceDayCounts[proto].push(entry.devices);
            for (const [region, count] of Object.entries(entry.regions ?? {})) {
                protocols[proto].regions[region] = (protocols[proto].regions[region] ?? 0) + count;
            }
            for (const [alt, count] of Object.entries(entry.altitudes ?? {})) {
                protocols[proto].altitudes[alt] = (protocols[proto].altitudes[alt] ?? 0) + count;
            }
        }

        for (const [layer, hours] of Object.entries(stats.hourly ?? {})) {
            if (!hourly[layer]) hourly[layer] = new Array(24).fill(0);
            for (let i = 0; i < hours.length && i < 24; i++) {
                hourly[layer][i] += hours[i];
            }
        }
    }

    // Replace device sum with average (rounded to nearest integer)
    for (const [proto, counts] of Object.entries(deviceDayCounts)) {
        if (counts.length > 0) {
            const avg = counts.reduce((a, b) => a + b, 0) / counts.length;
            protocols[proto].devices = Math.round(avg);
        }
    }

    return {generated, startTime, uptimeSeconds: totalUptime, restarts: totalRestarts, protocols, hourly};
}

function dailyEntryFromStats(date: string, stats: ProtocolStatsJson): DailyDevicesEntry {
    const entry: DailyDevicesEntry = {date, devices: {}, accepted: {}, restarts: stats.restarts};
    for (const [proto, data] of Object.entries(stats.protocols)) {
        entry.devices[proto] = data.devices;
        entry.accepted[proto] = data.accepted;
    }
    return entry;
}

/** Generate list of YYYY-MM strings from startYYYYMM to endYYYYMM inclusive. */
function monthsInRange(startYYYYMM: string, endYYYYMM: string): string[] {
    const [sy, sm] = startYYYYMM.split('-').map(Number);
    const [ey, em] = endYYYYMM.split('-').map(Number);
    const result: string[] = [];
    let cy = sy, cm = sm;
    while (cy < ey || (cy === ey && cm <= em)) {
        result.push(`${cy}-${String(cm).padStart(2, '0')}`);
        cm++;
        if (cm > 12) {
            cm = 1;
            cy++;
        }
    }
    return result;
}

export default async function handler(req, res) {
    const response: ProtocolStatsApiResponse = {
        current: null,
        hourlyHistory: [],
        dailyDevices: [],
        devicesExact: false
    };

    // List all files in the stats directory
    let files: string[];
    try {
        files = readdirSync(statsDir);
    } catch {
        res.setHeader('Cache-Control', `public, max-age=${ROLLUP_PERIOD_MINUTES * 60}, s-maxage=${ROLLUP_PERIOD_MINUTES * 60}, stale-while-revalidate=300`);
        res.status(200).json(response);
        return;
    }

    // Check for date range query params
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
        .sort((a, b) => b.date.localeCompare(a.date));

    if (hasRange) {
        const {type: periodType, value: periodValue} = parsePeriodParam(dateStart);
        const isSinglePeriod = !dateEnd || dateEnd === dateStart;
        response.currentPeriod = isSinglePeriod ? periodType as ProtocolStatsApiResponse['currentPeriod'] : 'range';

        const startBounds = dateBounds(dateStart);
        const endBounds = dateBounds(dateEnd || dateStart);
        const rangeStart = startBounds?.start || '0000-00-00';
        const rangeEnd = endBounds?.end || '9999-99-99';

        // For today's live data, use the symlink if today is in range
        const today = todayDate();
        let liveStats: ProtocolStatsJson | null = null;
        if (today >= rangeStart && today <= rangeEnd) {
            liveStats = readStatsFile(join(statsDir, 'protocol-stats.json.gz'));
        }

        // Pre-aggregated file for single period of month/year/yearnz
        if (isSinglePeriod && (periodType === 'month' || periodType === 'year' || periodType === 'yearnz')) {
            let suffix: string;
            if (periodType === 'month') {
                suffix = periodValue ?? currentMonth();
            } else if (periodType === 'year') {
                suffix = periodValue ?? new Date().getFullYear().toString();
            } else {
                // yearnz
                suffix = (periodValue ?? new Date().getFullYear().toString()) + 'nz';
            }
            const periodStats = readPeriodStatsFile(suffix);
            if (periodStats) {
                response.current = periodStats;
                response.devicesExact = true;
            }
        }

        if (isSinglePeriod && periodType === 'day') {
            // Single day: context days + the day itself
            const isoDate = periodValue ?? today;
            const dayFile = allDated.find((d) => d.date === isoDate);
            const dayStats = isoDate === today && liveStats ? liveStats : (dayFile ? readStatsFile(join(statsDir, dayFile.file)) : null);

            if (dayStats) {
                response.current = dayStats;
                response.devicesExact = true;
            }

            // 4 context days before this day
            const contextDays = allDated.filter((d) => d.date < isoDate).slice(0, 4).reverse();
            for (const day of contextDays) {
                const stats = day.date === today && liveStats ? liveStats : readStatsFile(join(statsDir, day.file));
                if (stats) response.dailyDevices.push(dailyEntryFromStats(day.date, stats));
            }
            if (dayStats) response.dailyDevices.push(dailyEntryFromStats(isoDate, dayStats));

            // Hourly history: 4 previous daily files
            const historyDays = allDated.filter((d) => d.date < isoDate).slice(0, 4);
            for (const day of historyDays) {
                const stats = day.date === today && liveStats ? liveStats : readStatsFile(join(statsDir, day.file));
                if (stats?.hourly) response.hourlyHistory.push({date: day.date, hourly: stats.hourly});
            }
        } else if (isSinglePeriod && periodType === 'month') {
            // Daily bars for the month
            const rangeDays = allDated.filter((d) => d.date >= rangeStart && d.date <= rangeEnd).reverse();
            const allStats: {date: string; stats: ProtocolStatsJson}[] = [];
            for (const day of rangeDays) {
                const stats = day.date === today && liveStats ? liveStats : readStatsFile(join(statsDir, day.file));
                if (!stats) continue;
                allStats.push({date: day.date, stats});
                response.dailyDevices.push(dailyEntryFromStats(day.date, stats));
            }
            if (!response.current && allStats.length > 0) {
                response.current = aggregateStats(allStats);
            }

            // Hourly history: previous 3 monthly files
            const yearMonth = periodValue ?? currentMonth();
            const [y, m] = yearMonth.split('-').map(Number);
            for (let i = 1; i <= 3; i++) {
                let pm = m - i;
                let py = y;
                if (pm <= 0) {
                    pm += 12;
                    py--;
                }
                const monthStr = `${py}-${String(pm).padStart(2, '0')}`;
                const monthStats = readPeriodStatsFile(monthStr);
                if (monthStats?.hourly) response.hourlyHistory.push({date: monthStr, hourly: monthStats.hourly});
            }
        } else if (isSinglePeriod && (periodType === 'year' || periodType === 'yearnz')) {
            // Monthly bars for year/yearnz
            let months: string[];
            if (periodType === 'year') {
                const yr = periodValue ?? new Date().getFullYear().toString();
                months = Array.from({length: 12}, (_, i) => `${yr}-${String(i + 1).padStart(2, '0')}`);
            } else {
                months = monthsInRange(rangeStart.slice(0, 7), rangeEnd.slice(0, 7));
            }

            for (const month of months) {
                const monthStats = readPeriodStatsFile(month);
                if (!monthStats) continue;
                response.dailyDevices.push(dailyEntryFromStats(month, monthStats));
            }

            if (!response.current) {
                // Fall back to daily aggregation if pre-aggregated file was missing
                const rangeDays = allDated.filter((d) => d.date >= rangeStart && d.date <= rangeEnd);
                const allStats: {date: string; stats: ProtocolStatsJson}[] = [];
                for (const d of [...rangeDays].reverse()) {
                    const stats = d.date === today && liveStats ? liveStats : readStatsFile(join(statsDir, d.file));
                    if (stats) allStats.push({date: d.date, stats});
                }
                if (allStats.length > 0) {
                    response.current = aggregateStats(allStats);
                }
            }

            // Hourly history: most recent 3 monthly files within the period
            const recentMonths = [...months].reverse().slice(0, 3);
            for (const month of recentMonths) {
                const monthStats = readPeriodStatsFile(month);
                if (monthStats?.hourly) response.hourlyHistory.push({date: month, hourly: monthStats.hourly});
            }
        } else {
            // Custom date range: aggregate daily files
            const rangeDays = allDated.filter((d) => d.date >= rangeStart && d.date <= rangeEnd);
            const allStats: {date: string; stats: ProtocolStatsJson}[] = [];
            for (const day of [...rangeDays].reverse()) {
                const stats = day.date === today && liveStats ? liveStats : readStatsFile(join(statsDir, day.file));
                if (!stats) continue;
                allStats.push({date: day.date, stats});
                response.dailyDevices.push(dailyEntryFromStats(day.date, stats));
            }
            if (allStats.length > 0) {
                response.current = aggregateStats(allStats);
            }
            // devicesExact stays false

            // Hourly history: most recent daily files BEFORE the range (up to 3),
            // so the history lines show context outside the selected period rather
            // than duplicating days already included in the aggregate.
            const beforeRange = allDated.filter((d) => d.date < rangeStart).slice(0, 3);
            for (const day of beforeRange) {
                const stats = day.date === today && liveStats ? liveStats : readStatsFile(join(statsDir, day.file));
                if (stats?.hourly) response.hourlyHistory.push({date: day.date, hourly: stats.hourly});
            }
        }
    } else {
        // Default mode: current day's live data
        response.currentPeriod = 'day';

        const symlinkPath = join(statsDir, 'protocol-stats.json.gz');
        let currentDate = todayDate();
        try {
            const target = readlinkSync(symlinkPath);
            const match = target.match(/protocol-stats\.(\d{4}-\d{2}-\d{2})\.json\.gz$/);
            if (match) {
                currentDate = match[1];
            }
            response.current = readStatsFile(symlinkPath);
        } catch {
            const datedFiles = allDated.slice(); // already sorted desc
            if (datedFiles.length > 0) {
                currentDate = datedFiles[0].date;
                response.current = readStatsFile(join(statsDir, datedFiles[0].file));
            }
        }
        response.devicesExact = true;

        // Hourly history: last 4 days before today (5 total with today)
        const historyDays = allDated.filter((d) => d.date < currentDate).slice(0, 4);
        for (const day of historyDays) {
            const stats = readStatsFile(join(statsDir, day.file));
            if (stats?.hourly) {
                response.hourlyHistory.push({
                    date: day.date,
                    hourly: stats.hourly
                });
            }
        }

        // Daily devices: all files in the current month
        const month = currentMonth();
        const monthDays = allDated.filter((d) => d.date.startsWith(month)).reverse();

        for (const day of monthDays) {
            if (day.date === currentDate && response.current) {
                response.dailyDevices.push(dailyEntryFromStats(day.date, response.current));
                continue;
            }
            const stats = readStatsFile(join(statsDir, day.file));
            if (stats) response.dailyDevices.push(dailyEntryFromStats(day.date, stats));
        }
    }

    // Truncate future hours from today's live hourly data so the chart doesn't
    // show zero-filled buckets for times that haven't happened yet.
    if (response.current?.startTime.slice(0, 10) === todayDate()) {
        const currentHour = new Date().getUTCHours();
        const truncatedHourly: Record<string, number[]> = {};
        for (const [layer, hours] of Object.entries(response.current.hourly ?? {})) {
            truncatedHourly[layer] = hours.slice(0, currentHour + 1);
        }
        response.current = {...response.current, hourly: truncatedHourly};
    }

    // Global uptime: read live file
    response.globalUptime = readJsonFile<GlobalUptimeData>(join(statsDir, 'global-uptime.json.gz'));

    // Global uptime history: scan dated files in the stats dir
    const uptimeHistory: GlobalUptimeHistoryEntry[] = [];
    const uptimeRangeStart = hasRange ? (dateBounds(dateStart)?.start || '0000-00-00') : currentMonth();
    const uptimeRangeEnd = hasRange ? (dateBounds(dateEnd || dateStart)?.end || '9999-99-99') : '9999-99-99';

    for (const f of files) {
        const m = f.match(uptimeFilePattern);
        if (!m) continue;
        const fDate = m[1];
        if (fDate < uptimeRangeStart || fDate > uptimeRangeEnd) continue;
        const data = readJsonFile<GlobalUptimeData>(join(statsDir, f));
        if (data) {
            uptimeHistory.push({date: fDate, activity: data.activity, uptime: data.uptime});
        }
    }
    uptimeHistory.sort((a, b) => a.date.localeCompare(b.date));
    response.globalUptimeHistory = uptimeHistory;

    // Populate globalUptimeAggregate for non-today periods
    if (hasRange) {
        const {type: upPeriodType, value: upPeriodValue} = parsePeriodParam(dateStart);
        const upIsSingle = !dateEnd || dateEnd === dateStart;
        const today = todayDate();

        if (upIsSingle && upPeriodType === 'day') {
            const isoDate = upPeriodValue ?? today;
            if (isoDate !== today) {
                // Past single day: read dated file; server/software from that day
                const dated = readJsonFile<GlobalUptimeData>(join(statsDir, `global-uptime.${isoDate}.json.gz`));
                if (dated) {
                    response.globalUptimeAggregate = {
                        uptime: dated.uptime,
                        coverageStart: isoDate,
                        coverageEnd: isoDate,
                        activity: dated.activity,
                        server: dated.server,
                        serverSoftware: dated.serverSoftware,
                    };
                }
            }
            // else: today → no aggregate, GlobalUptimeCard falls through to globalUptime
        } else {
            // Range / non-day period: aggregate uptime from history + live if today is in range
            const allEntries: {date: string; uptime: number}[] = [...uptimeHistory];
            const liveUptime = response.globalUptime;
            if (liveUptime) {
                const ld = liveUptime.date;
                if (ld >= uptimeRangeStart && ld <= uptimeRangeEnd && !allEntries.some(e => e.date === ld)) {
                    allEntries.push({date: ld, uptime: liveUptime.uptime});
                    allEntries.sort((a, b) => a.date.localeCompare(b.date));
                }
            }
            if (allEntries.length > 0) {
                const avg = allEntries.reduce((sum, e) => sum + e.uptime, 0) / allEntries.length;
                response.globalUptimeAggregate = {
                    uptime: avg,
                    coverageStart: allEntries[0].date,
                    coverageEnd: allEntries[allEntries.length - 1].date,
                };
            }
        }
    }

    res.setHeader('Cache-Control', `public, max-age=${ROLLUP_PERIOD_MINUTES * 60}, s-maxage=${ROLLUP_PERIOD_MINUTES * 60}, stale-while-revalidate=300`);
    res.status(200).json(response);
}
