import {useMemo} from 'react';
import {useTranslation} from 'next-i18next';
import {LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer} from 'recharts';
import type {ProtocolStatsApiResponse} from '../../common/protocolstats';
import {INDIVIDUAL_LAYERS} from './protocolstatsutil';

const HISTORY_COLORS = ['#999', '#aaa', '#bbb', '#ccc'];

export function HourlyTrafficChart({data, selectedTab, color}: {data: ProtocolStatsApiResponse; selectedTab: string; color: string}) {
    const {t} = useTranslation('common', {keyPrefix: 'stats'});

    const hourlyData = useMemo(() => {
        if (!data?.current?.hourly) return null;
        const hourly = data.current.hourly;

        const result: Record<string, unknown>[] = [];
        for (let h = 0; h < 24; h++) {
            const point: Record<string, unknown> = {hour: h};

            if (selectedTab === 'all') {
                let sum = 0;
                for (const layer of INDIVIDUAL_LAYERS) {
                    sum += hourly[layer]?.[h] ?? 0;
                }
                point.today = sum;
            } else {
                point.today = hourly[selectedTab]?.[h] ?? 0;
            }

            for (let d = 0; d < data.hourlyHistory.length; d++) {
                const hist = data.hourlyHistory[d];
                if (selectedTab === 'all') {
                    let sum = 0;
                    for (const layer of INDIVIDUAL_LAYERS) {
                        sum += hist.hourly[layer]?.[h] ?? 0;
                    }
                    point[`d${d + 1}`] = sum;
                } else {
                    point[`d${d + 1}`] = hist.hourly[selectedTab]?.[h] ?? 0;
                }
            }

            result.push(point);
        }
        return result;
    }, [data, selectedTab]);

    const fmtDate = (iso: string) => new Date(iso + 'T00:00:00').toLocaleDateString(undefined, {month: 'short', day: 'numeric'});
    const periodKey = data.currentPeriod && data.currentPeriod !== 'day' ? `period_${data.currentPeriod}` : null;
    const currentDateLabel = periodKey ? t(periodKey) : fmtDate(data.current.startTime.slice(0, 10));

    if (!hourlyData) return null;

    return (
        <>
            <b style={{fontSize: 'small'}}>{t('traffic_by_hour')}</b>
            <ResponsiveContainer width="100%" height={150}>
                <LineChart data={hourlyData} margin={{top: 5, right: 5, left: -10, bottom: 5}}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="hour" style={{fontSize: '0.7rem'}} />
                    <YAxis style={{fontSize: '0.7rem'}} />
                    <Tooltip />
                    <Line name={currentDateLabel} type="monotone" dataKey="today" stroke={color} strokeWidth={2} dot={{r: 1}} isAnimationActive={false} />
                    {data.hourlyHistory.map((hist, i) => (
                        <Line key={hist.date} name={fmtDate(hist.date)} type="monotone" dataKey={`d${i + 1}`} stroke={HISTORY_COLORS[i]} strokeWidth={1} dot={false} isAnimationActive={false} />
                    ))}
                </LineChart>
            </ResponsiveContainer>
        </>
    );
}
