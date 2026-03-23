import {useMemo} from 'react';
import useSWR from 'swr';
import {useTranslation} from 'next-i18next';
import {LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer} from 'recharts';

import {WaitForGraph} from './waitforgraph';
import type {StationStatsApiResponse, AprsPacketStats} from '../../common/stationstats';

const fetcher = (url: string) => fetch(url).then((r) => r.json());

/** Hourly chart for station stats (total across all layers, or selected layer) */
function StationHourlyChart({hourly, currentHour}: {hourly: Record<string, number[]>; currentHour?: number}) {
    const {t} = useTranslation('common', {keyPrefix: 'stats'});

    const data = useMemo(() => {
        const result = [];
        for (let h = 0; h < 24; h++) {
            let sum = 0;
            for (const counts of Object.values(hourly)) {
                sum += counts[h] ?? 0;
            }
            result.push({hour: h, accepted: currentHour != null && h > currentHour ? null : sum});
        }
        return result;
    }, [hourly, currentHour]);

    return (
        <>
            <b style={{fontSize: 'small'}}>{t('station_traffic_by_hour')}</b>
            <ResponsiveContainer width="100%" height={150}>
                <LineChart data={data} margin={{top: 5, right: 5, left: -10, bottom: 5}}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="hour" style={{fontSize: '0.7rem'}} />
                    <YAxis style={{fontSize: '0.7rem'}} />
                    <Tooltip />
                    <Line name={t('accepted')} type="monotone" dataKey="accepted" stroke="#4488cc" strokeWidth={2} dot={{r: 1}} connectNulls={false} isAnimationActive={false} />
                </LineChart>
            </ResponsiveContainer>
        </>
    );
}

/** Daily accepted/raw bar chart */
function StationDailyChart({dailyAccepted}: {dailyAccepted: {date: string; accepted: number; count: number}[]}) {
    const {t} = useTranslation('common', {keyPrefix: 'stats'});

    const data = useMemo(() => {
        return [...dailyAccepted]
            .sort((a, b) => a.date.localeCompare(b.date))
            .map((d) => ({date: d.date.slice(5), accepted: d.accepted, count: d.count}));
    }, [dailyAccepted]);

    if (data.length === 0) return null;

    return (
        <>
            <b style={{fontSize: 'small'}}>{t('accepted_by_day')}</b>
            <ResponsiveContainer width="100%" height={150}>
                <BarChart data={data} margin={{top: 5, right: 5, left: -10, bottom: 5}} maxBarSize={50}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" style={{fontSize: '0.7rem'}} />
                    <YAxis style={{fontSize: '0.7rem'}} />
                    <Tooltip />
                    <Bar name={t('accepted')} dataKey="accepted" fill="#4488cc" isAnimationActive={false} />
                </BarChart>
            </ResponsiveContainer>
        </>
    );
}

/** Exception stats table for station stats */
const EXCEPTION_KEYS: (keyof AprsPacketStats)[] = [
    'count', 'accepted',
    'ignoredTracker', 'invalidTracker', 'invalidTimestamp',
    'ignoredStationary', 'ignoredSignal0', 'ignoredH3stationary',
    'ignoredElevation', 'ignoredFutureTimestamp', 'ignoredStaleTimestamp'
];

function StationExceptionTable({stats}: {stats: AprsPacketStats}) {
    const {t} = useTranslation('common', {keyPrefix: 'statistics'});

    return (
        <table>
            <tbody>
                {EXCEPTION_KEYS.filter((k) => (stats[k] as number) > 0).map((key) => (
                    <tr key={key}>
                        <td>{t(key)}</td>
                        <td>{stats[key] as number}</td>
                    </tr>
                ))}
            </tbody>
        </table>
    );
}

/** Global station traffic stats dashboard — shown in the "Stations" tab of the global stats panel. */
export function StationStatsDashboard({dateRange}: {dateRange?: {start: string; end: string}}) {
    const {t} = useTranslation('common', {keyPrefix: 'stats'});

    const statsUrl = useMemo(() => {
        const params = new URLSearchParams();
        if (dateRange?.start) params.set('dateStart', dateRange.start);
        if (dateRange?.end) params.set('dateEnd', dateRange.end);
        const qs = params.toString();
        return qs ? `/api/station-stats?${qs}` : '/api/station-stats';
    }, [dateRange?.start, dateRange?.end]);

    const {data} = useSWR<StationStatsApiResponse>(statsUrl, fetcher);

    if (!data) return <WaitForGraph />;

    if (!data.current) {
        return (
            <div>
                <b>{t('station_stats_title')}</b>
                <br />
                <span style={{color: 'gray', fontStyle: 'italic'}}>{t('no_data')}</span>
            </div>
        );
    }

    const currentHour = new Date().getUTCHours();

    return (
        <div>
            <b>{t('station_stats_title')}</b>
            <br />
            {data.current.hourly && Object.keys(data.current.hourly).length > 0 && (
                <StationHourlyChart hourly={data.current.hourly} currentHour={currentHour} />
            )}
            {data.dailyAccepted.length > 1 && (
                <StationDailyChart dailyAccepted={data.dailyAccepted} />
            )}
            <StationExceptionTable stats={data.current} />
        </div>
    );
}

/** Per-station hourly chart shown in the station detail panel. */
export function StationHourlyDetailChart({hourly, isToday}: {hourly: Record<string, number[]>; isToday: boolean}) {
    const currentHour = isToday ? new Date().getUTCHours() : undefined;
    return <StationHourlyChart hourly={hourly} currentHour={currentHour} />;
}
