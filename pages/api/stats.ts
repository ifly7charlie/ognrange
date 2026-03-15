import {readdirSync, readFileSync, readlinkSync} from 'fs';
import {gunzipSync} from 'zlib';
import {join} from 'path';

import {OUTPUT_PATH, ROLLUP_PERIOD_MINUTES} from '../../lib/common/config';
import type {ProtocolStatsJson, ProtocolStatsApiResponse, DailyDevicesEntry, ProtocolEntry} from '../../lib/common/protocolstats';

const statsDir = join(OUTPUT_PATH, 'stats');

// Match dated daily files: protocol-stats.YYYY-MM-DD.json.gz
const dailyFilePattern = /^protocol-stats\.(\d{4}-\d{2}-\d{2})\.json\.gz$/;

function readStatsFile(filePath: string): ProtocolStatsJson | null {
    try {
        const raw = readFileSync(filePath);
        if (filePath.endsWith('.gz')) {
            return JSON.parse(gunzipSync(raw).toString());
        }
        return JSON.parse(raw.toString());
    } catch {
        return null;
    }
}

function todayDate(): string {
    return new Date().toISOString().slice(0, 10);
}

function currentMonth(): string {
    return new Date().toISOString().slice(0, 7);
}

// Convert app date param (e.g. "year", "month.2026-03", "day.2026-03-15") to YYYY-MM-DD bounds
function dateBounds(param: string): {start: string; end: string} | null {
    const dot = param.indexOf('.');
    const type = dot === -1 ? param : param.slice(0, dot);
    const date = dot === -1 ? null : param.slice(dot + 1);

    const now = new Date();
    switch (type) {
        case 'year': {
            const y = date || now.getFullYear().toString();
            return {start: `${y}-01-01`, end: `${y}-12-31`};
        }
        case 'yearnz': {
            const y = date ? parseInt(date.replace('nz', '')) : now.getFullYear();
            return {start: `${y - 1}-07-01`, end: `${y}-06-30`};
        }
        case 'month': {
            const m = date || now.toISOString().slice(0, 7);
            const [y2, m2] = m.split('-').map(Number);
            const lastDay = new Date(y2, m2, 0).getDate();
            return {start: `${m}-01`, end: `${m}-${String(lastDay).padStart(2, '0')}`};
        }
        case 'day': {
            const d = date || now.toISOString().slice(0, 10);
            return {start: d, end: d};
        }
    }
    return null;
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
        res.setHeader('Cache-Control', `public, s-maxage=${ROLLUP_PERIOD_MINUTES * 60}, stale-while-revalidate=300`);
        res.status(200).json(response);
        return;
    }

    // Check for date range query params
    const dateStart = (req.query.dateStart as string) || '';
    const dateEnd = (req.query.dateEnd as string) || '';
    const hasRange = dateStart && dateStart !== 'year' && dateStart !== 'yearnz';

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

        // Read all files in range, build dailyDevices and aggregate current
        const allStats: {date: string; stats: ProtocolStatsJson}[] = [];
        const chronological = [...rangeDays].reverse();

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

        // Aggregate all days into current
        if (allStats.length > 0) {
            response.current = aggregateStats(allStats);
        }

        // Hourly history: last 3 individual days in range for the chart overlay
        const historyDays = rangeDays.slice(0, 3);
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

        // Hourly history: last 3 days before today
        const historyDays = allDated.filter((d) => d.date < currentDate).slice(0, 3);
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

    res.setHeader('Cache-Control', `public, s-maxage=${ROLLUP_PERIOD_MINUTES * 60}, stale-while-revalidate=300`);
    res.status(200).json(response);
}
