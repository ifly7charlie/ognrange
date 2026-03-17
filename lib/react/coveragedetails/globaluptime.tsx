import {useMemo, useCallback} from 'react';
import useSWR from 'swr';
import {useTranslation} from 'next-i18next';

import graphcolours from '../graphcolours';
import {SlotStrip, HourLabels} from './slotstrip';
import {UptimeBar} from './uptimebar';
import type {ProtocolStatsApiResponse} from '../../common/protocolstats';

const fetcher = (url: string) => fetch(url).then((res) => res.json());

export function GlobalUptimeCard({dateRange}: {dateRange?: {start: string; end: string}}) {
    const {t} = useTranslation('common', {keyPrefix: 'serverUptime'});
    const {t: tDetails} = useTranslation('common', {keyPrefix: 'details.server'});

    const statsUrl = useMemo(() => {
        const params = new URLSearchParams();
        if (dateRange?.start) params.set('dateStart', dateRange.start);
        if (dateRange?.end) params.set('dateEnd', dateRange.end);
        const qs = params.toString();
        return qs ? `/api/stats?${qs}` : '/api/stats';
    }, [dateRange?.start, dateRange?.end]);

    const {data} = useSWR<ProtocolStatsApiResponse>(statsUrl, fetcher);

    const uptime = data?.globalUptime;
    // Slots are 1-indexed (1–144); index i is future when i >= uptime.slot
    const uptimeColorFn = useCallback(
        (slot: number, active: boolean): string => {
            if (uptime && slot >= uptime.slot) return '#cad5e1';
            return active ? graphcolours[0] : '#eee';
        },
        [uptime?.slot]
    );

    if (!uptime) return null;

    return (
        <div style={{marginTop: '8px'}}>
            <hr />
            <b>{t('title')}</b>
            <div style={{margin: '4px 0'}}>
                <HourLabels />
                <SlotStrip hex={uptime.activity} cellHeight={16} colorFn={uptimeColorFn} />
            </div>
            <UptimeBar uptime={uptime.uptime} label={tDetails('uptime_title')} sublabel={t('uptime', {percent: uptime.uptime.toFixed(1)})} />
            <div style={{fontSize: '0.8rem', color: '#888', marginTop: '4px'}}>
                {t('server', {server: uptime.server})}
                <br />
                {t('software', {software: uptime.serverSoftware})}
            </div>
        </div>
    );
}
