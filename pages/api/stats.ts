import {readdirSync, readFileSync, readlinkSync} from 'fs';
import {gunzipSync} from 'zlib';
import {join} from 'path';

import {OUTPUT_PATH, ROLLUP_PERIOD_MINUTES} from '../../lib/common/config';
import {dateBounds} from '../../lib/common/datebounds';
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

function todayDate(): string {
    return new Date().toISOString().slice(0, 10);
}

function currentMonth(): string {
    return new Date().toISOString().slice(0, 7);
}

// Aggregate multiple days of stats into one summary
function aggregateStats(statsList: {date: string; stats: ProtocolStatsJson}[]): ProtocolStatsJson {
    const protocols: Record<string, ProtocolEntry> = {};
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
            }
            protocols[proto].raw += entry.raw;
            protocols[proto].accepted += entry.accepted;
            protocols[proto].devices += entry.devices;
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

    return {generated, startTime, uptimeSeconds: totalUptime, restarts: totalRestarts, protocols, hourly};
}

export default async function handler(req, res) {
    const response: ProtocolStatsApiResponse = {
        current: null,
        hourlyHistory: [],
        dailyDevices: []
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

    // Collect all dated files sorted by date descending
    const allDated = files
        .map((f) => {
            const m = f.match(dailyFilePattern);
            return m ? {file: f, date: m[1]} : null;
        })
        .filter(Boolean)
        .sort((a, b) => b.date.localeCompare(a.date));

    if (hasRange) {
        // Date-range mode: filter files to the requested range
        const startBounds = dateBounds(dateStart);
        const endBounds = dateBounds(dateEnd || dateStart);
        const rangeStart = startBounds?.start || '0000-00-00';
        const rangeEnd = endBounds?.end || '9999-99-99';

        const rangeDays = allDated.filter((d) => d.date >= rangeStart && d.date <= rangeEnd);
        const isSingleDay = startBounds?.start === startBounds?.end && (!dateEnd || dateEnd === dateStart);

        // For today's live data, use the symlink if today is in range
        const today = todayDate();
        let liveStats: ProtocolStatsJson | null = null;
        if (today >= rangeStart && today <= rangeEnd) {
            try {
                liveStats = readStatsFile(join(statsDir, 'protocol-stats.json.gz'));
            } catch {
                // ignore
            }
        }

        // For single-day selection, include 4 previous days in dailyDevices for context
        const contextDays = isSingleDay
            ? allDated.filter((d) => d.date < (rangeDays[0]?.date ?? rangeEnd)).slice(0, 4).reverse()
            : [];
        for (const day of contextDays) {
            const stats = day.date === today && liveStats ? liveStats : readStatsFile(join(statsDir, day.file));
            if (!stats) continue;
            const entry: DailyDevicesEntry = {date: day.date, devices: {}, accepted: {}, restarts: stats.restarts};
            for (const [proto, data] of Object.entries(stats.protocols)) {
                entry.devices[proto] = data.devices;
                entry.accepted[proto] = data.accepted;
            }
            response.dailyDevices.push(entry);
        }

        // Read all files in range, build dailyDevices and aggregate current
        const allStats: {date: string; stats: ProtocolStatsJson}[] = [];
        const chronological = [...rangeDays].reverse();

        for (const day of chronological) {
            const stats = day.date === today && liveStats ? liveStats : readStatsFile(join(statsDir, day.file));
            if (!stats) continue;

            allStats.push({date: day.date, stats});

            const entry: DailyDevicesEntry = {
                date: day.date,
                devices: {},
                accepted: {},
                restarts: stats.restarts
            };
            for (const [proto, data] of Object.entries(stats.protocols)) {
                entry.devices[proto] = data.devices;
                entry.accepted[proto] = data.accepted;
            }
            response.dailyDevices.push(entry);
        }

        // Aggregate only the selected range into current (not the context days)
        if (allStats.length > 0) {
            response.current = aggregateStats(allStats);
        }

        // Hourly history: for single-day show previous 4 days;
        // for multi-day ranges show the most recent days in the range
        const historyDays = isSingleDay
            ? allDated.filter((d) => d.date < (rangeDays[0]?.date ?? rangeEnd)).slice(0, 4)
            : rangeDays.slice(0, 3);
        for (const day of historyDays) {
            const stats = day.date === today && liveStats ? liveStats : readStatsFile(join(statsDir, day.file));
            if (stats?.hourly) {
                response.hourlyHistory.push({
                    date: day.date,
                    hourly: stats.hourly
                });
            }
        }
    } else {
        // Default mode: current month (existing behavior)

        // Read current stats from symlink or latest dated file
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
                const entry: DailyDevicesEntry = {
                    date: day.date,
                    devices: {},
                    accepted: {},
                    restarts: response.current.restarts
                };
                for (const [proto, data] of Object.entries(response.current.protocols)) {
                    entry.devices[proto] = data.devices;
                    entry.accepted[proto] = data.accepted;
                }
                response.dailyDevices.push(entry);
                continue;
            }
            const stats = readStatsFile(join(statsDir, day.file));
            if (stats) {
                const entry: DailyDevicesEntry = {
                    date: day.date,
                    devices: {},
                    accepted: {},
                    restarts: stats.restarts
                };
                for (const [proto, data] of Object.entries(stats.protocols)) {
                    entry.devices[proto] = data.devices;
                    entry.accepted[proto] = data.accepted;
                }
                response.dailyDevices.push(entry);
            }
        }
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

    res.setHeader('Cache-Control', `public, max-age=${ROLLUP_PERIOD_MINUTES * 60}, s-maxage=${ROLLUP_PERIOD_MINUTES * 60}, stale-while-revalidate=300`);
    res.status(200).json(response);
}
