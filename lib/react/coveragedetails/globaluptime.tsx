import {useMemo, useCallback} from 'react';
import useSWR from 'swr';
import {useTranslation} from 'next-i18next';

import graphcolours from '../graphcolours';
import {SlotStrip, HourLabels} from './slotstrip';
import {UptimeBar} from './uptimebar';
import type {ProtocolStatsApiResponse, GlobalUptimeData} from '../../common/protocolstats';

const fetcher = (url: string) => fetch(url).then((res) => res.json());

function fmtDate(iso: string): string {
    return new Date(iso + 'T00:00:00').toLocaleDateString(undefined, {
        month: 'short', day: 'numeric', year: 'numeric'
    });
}

function TodaySection({uptime, uptimeColorFn}: {
    uptime: GlobalUptimeData;
    uptimeColorFn: (slot: number, active: boolean) => string;
}) {
    const {t} = useTranslation('common', {keyPrefix: 'serverUptime'});
    const {t: tDetails} = useTranslation('common', {keyPrefix: 'details.server'});
    return (
        <div style={{marginTop: '8px'}}>
            <span style={{fontSize: '0.85rem', color: '#888'}}>{fmtDate(uptime.date)}</span>
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

    // Declared unconditionally (rules-of-hooks); used only in today's slot strip
    const uptimeColorFn = useCallback(
        (slot: number, active: boolean): string => {
            const s = data?.globalUptime?.slot;
            if (s !== undefined && slot >= s) return '#cad5e1';
            return active ? graphcolours[0] : '#eee';
        },
        [data?.globalUptime?.slot]
    );

    const agg = data?.globalUptimeAggregate;
    const uptime = data?.globalUptime;

    // Single past day: activity field present → show that day's slot strip, then today's
    if (agg?.activity) {
        return (
            <div style={{marginTop: '8px'}}>
                <hr />
                <b>{t('title')}</b>
                <span style={{fontSize: '0.85rem', color: '#888'}}> ({fmtDate(agg.coverageStart)})</span>
                <div style={{margin: '4px 0'}}>
                    <HourLabels />
                    <SlotStrip hex={agg.activity} cellHeight={16} />
                </div>
                <UptimeBar uptime={agg.uptime} label={tDetails('uptime_title')} sublabel={t('uptime', {percent: agg.uptime.toFixed(1)})} />
                {agg.server && (
                    <div style={{fontSize: '0.8rem', color: '#888', marginTop: '4px'}}>
                        {t('server', {server: agg.server})}
                        <br />
                        {agg.serverSoftware && t('software', {software: agg.serverSoftware})}
                    </div>
                )}
                {uptime && <TodaySection uptime={uptime} uptimeColorFn={uptimeColorFn} />}
            </div>
        );
    }

    // Range / non-day aggregate: bar + coverage label, then today's slot strip
    if (agg) {
        const label = agg.coverageStart === agg.coverageEnd
            ? fmtDate(agg.coverageStart)
            : `${fmtDate(agg.coverageStart)} – ${fmtDate(agg.coverageEnd)}`;
        return (
            <div style={{marginTop: '8px'}}>
                <hr />
                <b>{t('title')}</b>
                <span style={{fontSize: '0.85rem', color: '#888'}}> ({label})</span>
                <UptimeBar uptime={agg.uptime} label={tDetails('uptime_title')} sublabel={t('uptime', {percent: agg.uptime.toFixed(1)})} />
                {uptime && <TodaySection uptime={uptime} uptimeColorFn={uptimeColorFn} />}
            </div>
        );
    }

    // Today / no date range: existing behaviour unchanged
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
