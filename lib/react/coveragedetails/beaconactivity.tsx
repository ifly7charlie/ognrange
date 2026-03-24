import {useMemo, useCallback} from 'react';
import {useTranslation} from 'next-i18next';

import graphcolours from '../graphcolours';
import {isSlotActive, SlotStrip, HourLabels} from './slotstrip';

const INACTIVE_COLOR = '#e67e22';   // FLARM orange — server up, station not beaconing
const SERVER_DOWN_COLOR = '#f4c87c'; // pale amber — server was down
const FUTURE_COLOR = '#f5f5f5';      // near-white — slot not yet elapsed

function Legend() {
    const {t} = useTranslation('common', {keyPrefix: 'details.beacon'});
    const items: [string, string][] = [
        [graphcolours[0], t('legend_active')],
        [INACTIVE_COLOR, t('legend_inactive')],
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

// currentSlot is 1-indexed (1–144); slot index i is future when i >= currentSlot.
// We also treat the in-progress slot (currentSlot - 1) as not-yet-elapsed: a missing
// server bit there just means the slot isn't complete, not that the server was down.
// exportedSlot is the last 0-indexed slot covered by the station JSON export — slots after
// it may simply not have been written yet, so we treat them the same as future.
function makeColorFn(serverHex?: string, currentSlot?: number, exportedSlot?: number): (slot: number, active: boolean) => string {
    return (slot: number, active: boolean) => {
        if (currentSlot !== undefined && slot >= currentSlot - 1) return FUTURE_COLOR;
        if (exportedSlot !== undefined && slot > exportedSlot) return FUTURE_COLOR;
        if (active) return graphcolours[0];
        if (serverHex && !isSlotActive(serverHex, slot)) return SERVER_DOWN_COLOR;
        return INACTIVE_COLOR;
    };
}

/** Single-day beacon activity: horizontal strip of 144 slots */
function SingleDayView({hex, date, serverUptimeHex, currentSlot, exportedAt}: {hex: string; date?: string; serverUptimeHex?: string; currentSlot?: number; exportedAt?: number}) {
    const {t} = useTranslation('common', {keyPrefix: 'details.beacon'});
    const today = new Date().toISOString().slice(0, 10);
    const slotForDay = date === today ? currentSlot : undefined;
    // Compute the last slot covered by the export (only meaningful for today's data)
    const exportedSlot = date === today && exportedAt != null
        ? Math.floor((exportedAt % 86400) / 600)
        : undefined;
    const colorFn = useMemo(() => makeColorFn(serverUptimeHex, slotForDay, exportedSlot), [serverUptimeHex, slotForDay, exportedSlot]);
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
function MultiDayView({days, serverUptime, currentSlot}: {days: {date: string; bitvector: string}[]; serverUptime?: {date: string; activity: string; uptime: number}[] | null; currentSlot?: number}) {
    const {t} = useTranslation('common', {keyPrefix: 'details.beacon'});
    const sorted = [...days].sort((a, b) => a.date.localeCompare(b.date));
    const today = new Date().toISOString().slice(0, 10);

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
            const slotForDay = date === today ? currentSlot : undefined;
            return makeColorFn(hex, slotForDay);
        },
        [serverMap, currentSlot, today]
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
    serverUptime,
    currentSlot,
    exportedAt
}: {
    data?: string;
    date?: string;
    days?: {date: string; bitvector: string}[];
    serverUptime?: {date: string; activity: string; uptime: number}[] | null;
    currentSlot?: number;
    exportedAt?: number;
}) {
    // Find matching server uptime entry for single-day view
    const serverUptimeHex = useMemo(() => {
        if (!date || !serverUptime) return undefined;
        const entry = serverUptime.find((e) => e.date === date);
        return entry?.activity;
    }, [date, serverUptime]);

    if (days && days.length > 1) {
        return <MultiDayView days={days} serverUptime={serverUptime} currentSlot={currentSlot} />;
    }
    if (data) {
        return <SingleDayView hex={data} date={date} serverUptimeHex={serverUptimeHex} currentSlot={currentSlot} exportedAt={exportedAt} />;
    }
    return null;
}
