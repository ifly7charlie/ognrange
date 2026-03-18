import {useMemo} from 'react';
import {useTranslation} from 'next-i18next';
import {BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell} from 'recharts';
import {layerFromDestCallsign} from '../../common/layers';
import type {DailyDevicesEntry} from '../../common/protocolstats';

/** Format a date string for chart X-axis labels.
 *  YYYY-MM → abbreviated month name in browser locale (e.g. "Mar")
 *  YYYY-MM-DD → MM-DD */
function dateLabel(date: string): string {
    if (date.length === 7) {
        return new Intl.DateTimeFormat(undefined, {month: 'short'}).format(new Date(date + '-01'));
    }
    return date.slice(5);
}

export function AcceptedByDayChart({dailyDevices, selectedTab, color}: {dailyDevices: DailyDevicesEntry[]; selectedTab: string; color: string}) {
    const {t} = useTranslation('common', {keyPrefix: 'stats'});

    const acceptedData = useMemo(() => {
        return dailyDevices.map((day) => {
            let total = 0;
            if (selectedTab === 'all') {
                for (const count of Object.values(day.accepted ?? {})) {
                    total += count;
                }
            } else {
                for (const [tocall, count] of Object.entries(day.accepted ?? {})) {
                    if (layerFromDestCallsign(tocall) === selectedTab) {
                        total += count;
                    }
                }
            }
            return {date: dateLabel(day.date), accepted: total};
        });
    }, [dailyDevices, selectedTab]);

    if (acceptedData.length === 0) return null;

    return (
        <>
            <b style={{fontSize: 'small'}}>{t('accepted_by_day')}</b>
            <ResponsiveContainer width="100%" height={150}>
                <BarChart data={acceptedData} margin={{top: 5, right: 5, left: -10, bottom: 5}} maxBarSize={50}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" style={{fontSize: '0.7rem'}} />
                    <YAxis style={{fontSize: '0.7rem'}} />
                    <Tooltip />
                    <Bar name={t('accepted')} dataKey="accepted" fill={color} isAnimationActive={false} />
                </BarChart>
            </ResponsiveContainer>
        </>
    );
}

export function DevicesByDayChart({dailyDevices, selectedTab, color}: {dailyDevices: DailyDevicesEntry[]; selectedTab: string; color: string}) {
    const {t} = useTranslation('common', {keyPrefix: 'stats'});

    const devicesData = useMemo(() => {
        return dailyDevices.map((day) => {
            let total = 0;
            if (selectedTab === 'all') {
                for (const count of Object.values(day.devices)) {
                    total += count;
                }
            } else {
                for (const [tocall, count] of Object.entries(day.devices)) {
                    if (layerFromDestCallsign(tocall) === selectedTab) {
                        total += count;
                    }
                }
            }
            return {date: dateLabel(day.date) + (day.restarts > 0 ? ` (${t('partial')})` : ''), devices: total, restarts: day.restarts};
        });
    }, [dailyDevices, selectedTab]);

    if (devicesData.length === 0) return null;

    return (
        <>
            <b style={{fontSize: 'small'}}>{t('devices_by_day')}</b>
            <ResponsiveContainer width="100%" height={150}>
                <BarChart data={devicesData} margin={{top: 5, right: 5, left: -10, bottom: 5}} maxBarSize={50}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="date" style={{fontSize: '0.7rem'}} />
                    <YAxis style={{fontSize: '0.7rem'}} />
                    <Tooltip
                        formatter={(value: number, _name: string, props: any) => {
                            const r = props?.payload?.restarts;
                            return r > 0 ? [`${value} ${t('partial_restart')}`, t('devices')] : [value, t('devices')];
                        }}
                    />
                    <Bar name={t('devices')} dataKey="devices" isAnimationActive={false}>
                        {devicesData.map((entry, index) => (
                            <Cell key={index} fill={color} stroke={entry.restarts > 0 ? 'red' : undefined} strokeWidth={entry.restarts > 0 ? 2 : 0} />
                        ))}
                    </Bar>
                </BarChart>
            </ResponsiveContainer>
            <hr />
        </>
    );
}
