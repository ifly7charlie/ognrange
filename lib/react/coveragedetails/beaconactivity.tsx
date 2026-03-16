import {useMemo, useCallback} from 'react';
import {useTranslation} from 'next-i18next';

import graphcolours from '../graphcolours';
import {isSlotActive, SlotStrip, HourLabels} from './slotstrip';

const SERVER_DOWN_COLOR = '#f4c87c';

function Legend() {
    const {t} = useTranslation('common', {keyPrefix: 'details.beacon'});
    const items: [string, string][] = [
        [graphcolours[0], t('legend_active')],
        ['#eee', t('legend_inactive')],
        [SERVER_DOWN_COLOR, t('legend_server_down')]
    ];
    return (
        <div style={{display: 'flex', gap: '10px', fontSize: '0.75rem', color: '#666', marginTop: '4px'}}>
            {items.map(([color, label]) => (
                <span key={label} style={{display: 'flex', alignItems: 'center', gap: '3px'}}>
                    <span style={{display: 'inline-block', width: '10px', height: '10px', background: color, borderRadius: '2px'}} />
                    {label}
                </span>
            ))}
        </div>
    );
}

function makeServerOverlayColorFn(serverHex: string): (slot: number, active: boolean) => string {
    return (slot: number, active: boolean) => {
        if (active) return graphcolours[0];
        const serverUp = isSlotActive(serverHex, slot);
        return serverUp ? '#eee' : SERVER_DOWN_COLOR;
    };
}

/** Single-day beacon activity: horizontal strip of 144 slots */
function SingleDayView({hex, date, serverUptimeHex}: {hex: string; date?: string; serverUptimeHex?: string}) {
    const {t} = useTranslation('common', {keyPrefix: 'details.beacon'});
    const colorFn = useMemo(() => (serverUptimeHex ? makeServerOverlayColorFn(serverUptimeHex) : undefined), [serverUptimeHex]);
    return (
        <>
            <br />
            <b>{t('title')}</b>
            {date ? <span style={{fontSize: '0.85rem', color: '#888'}}> ({date})</span> : null}
            <div style={{margin: '4px 0'}}>
                <HourLabels />
                <SlotStrip hex={hex} cellHeight={16} colorFn={colorFn} />
            </div>
            {serverUptimeHex ? <Legend /> : null}
        </>
    );
}

/** Multi-day beacon activity: punchcard with one row per day */
function MultiDayView({days, serverUptime}: {days: {date: string; bitvector: string}[]; serverUptime?: {date: string; activity: string; uptime: number}[] | null}) {
    const {t} = useTranslation('common', {keyPrefix: 'details.beacon'});
    const sorted = [...days].sort((a, b) => a.date.localeCompare(b.date));

    const serverMap = useMemo(() => {
        if (!serverUptime) return null;
        const map = new Map<string, string>();
        for (const entry of serverUptime) {
            map.set(entry.date, entry.activity);
        }
        return map;
    }, [serverUptime]);

    const hasOverlay = serverMap && sorted.some((day) => serverMap.has(day.date));

    const colorFnForDay = useCallback(
        (date: string) => {
            const hex = serverMap?.get(date);
            return hex ? makeServerOverlayColorFn(hex) : undefined;
        },
        [serverMap]
    );

    return (
        <>
            <br />
            <b>{t('title_range')}</b>
            <div style={{margin: '4px 0'}}>
                <HourLabels />
                {sorted.map((day) => (
                    <div key={day.date} style={{display: 'flex', alignItems: 'center', gap: '4px', marginBottom: '1px'}}>
                        <span style={{fontSize: '0.7rem', color: '#888', minWidth: '60px', textAlign: 'right'}}>{day.date.slice(5)}</span>
                        <div style={{flex: 1}}>
                            <SlotStrip hex={day.bitvector} cellHeight={10} colorFn={colorFnForDay(day.date)} />
                        </div>
                    </div>
                ))}
            </div>
            {hasOverlay ? <Legend /> : null}
        </>
    );
}

export function BeaconActivity({
    data,
    date,
    days,
    serverUptime
}: {
    data?: string;
    date?: string;
    days?: {date: string; bitvector: string}[];
    serverUptime?: {date: string; activity: string; uptime: number}[] | null;
}) {
    // Find matching server uptime entry for single-day view
    const serverUptimeHex = useMemo(() => {
        if (!date || !serverUptime) return undefined;
        const entry = serverUptime.find((e) => e.date === date);
        return entry?.activity;
    }, [date, serverUptime]);

    if (days && days.length > 1) {
        return <MultiDayView days={days} serverUptime={serverUptime} />;
    }
    if (data) {
        return <SingleDayView hex={data} date={date} serverUptimeHex={serverUptimeHex} />;
    }
    return null;
}
