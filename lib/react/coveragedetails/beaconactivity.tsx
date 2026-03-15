import {useMemo} from 'react';
import {useTranslation} from 'next-i18next';

import graphcolours from '../graphcolours';

const SLOTS_PER_DAY = 144;
const SLOTS_PER_HOUR = 6;

function isSlotActive(hex: string, slot: number): boolean {
    const byteIndex = Math.floor(slot / 8);
    const bitIndex = slot % 8;
    const byte = parseInt(hex.substring(byteIndex * 2, byteIndex * 2 + 2), 16);
    return (byte & (1 << bitIndex)) !== 0;
}

function SlotStrip({hex, cellHeight}: {hex: string; cellHeight: number}) {
    const cells = useMemo(() => {
        const result = [];
        for (let i = 0; i < SLOTS_PER_DAY; i++) {
            const active = isSlotActive(hex, i);
            result.push(
                <div
                    key={i}
                    style={{
                        flex: '1 1 0',
                        height: cellHeight,
                        background: active ? graphcolours[0] : '#eee',
                        borderRight: i % SLOTS_PER_HOUR === SLOTS_PER_HOUR - 1 ? '1px solid #ccc' : undefined
                    }}
                />
            );
        }
        return result;
    }, [hex, cellHeight]);
    return <div style={{display: 'flex', borderRadius: '2px', overflow: 'hidden'}}>{cells}</div>;
}

const HOUR_LABELS = [0, 3, 6, 9, 12, 15, 18, 21];

function HourLabels() {
    return (
        <div style={{display: 'flex'}}>
            {HOUR_LABELS.map((h) => (
                <span key={h} style={{flex: `${SLOTS_PER_HOUR * 3} 0 0`, fontSize: '0.7rem', color: '#888'}}>
                    {String(h).padStart(2, '0')}
                </span>
            ))}
        </div>
    );
}

/** Single-day beacon activity: horizontal strip of 144 slots */
function SingleDayView({hex, date}: {hex: string; date?: string}) {
    const {t} = useTranslation('common', {keyPrefix: 'details.beacon'});
    return (
        <>
            <br />
            <b>{t('title')}</b>
            {date ? <span style={{fontSize: '0.85rem', color: '#888'}}> ({date})</span> : null}
            <div style={{margin: '4px 0'}}>
                <HourLabels />
                <SlotStrip hex={hex} cellHeight={16} />
            </div>
        </>
    );
}

/** Multi-day beacon activity: punchcard with one row per day */
function MultiDayView({days}: {days: {date: string; bitvector: string}[]}) {
    const {t} = useTranslation('common', {keyPrefix: 'details.beacon'});
    const sorted = [...days].sort((a, b) => a.date.localeCompare(b.date));

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
                            <SlotStrip hex={day.bitvector} cellHeight={10} />
                        </div>
                    </div>
                ))}
            </div>
        </>
    );
}

export function BeaconActivity({
    data,
    date,
    days
}: {
    data?: string;
    date?: string;
    days?: {date: string; bitvector: string}[];
}) {
    if (days && days.length > 1) {
        return <MultiDayView days={days} />;
    }
    if (data) {
        return <SingleDayView hex={data} date={date} />;
    }
    return null;
}
