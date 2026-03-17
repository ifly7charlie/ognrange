import {useMemo} from 'react';

import graphcolours from '../graphcolours';

export const SLOTS_PER_DAY = 144;
export const SLOTS_PER_HOUR = 6;

export function isSlotActive(hex: string, slot: number): boolean {
    const byteIndex = Math.floor(slot / 8);
    const bitIndex = slot % 8;
    const byte = parseInt(hex.substring(byteIndex * 2, byteIndex * 2 + 2), 16);
    return (byte & (1 << bitIndex)) !== 0;
}

export function SlotStrip({hex, cellHeight, colorFn}: {hex: string; cellHeight: number; colorFn?: (slot: number, active: boolean) => string}) {
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
                        background: colorFn ? colorFn(i, active) : active ? graphcolours[0] : '#eee',
                        borderRight: i % (SLOTS_PER_HOUR * 3) === SLOTS_PER_HOUR * 3 - 1 ? '1px solid #ccc' : undefined
                    }}
                />
            );
        }
        return result;
    }, [hex, cellHeight, colorFn]);
    return <div style={{display: 'flex', borderRadius: '2px', overflow: 'hidden'}}>{cells}</div>;
}

export const HOUR_LABELS = [0, 3, 6, 9, 12, 15, 18, 21];

export function HourLabels() {
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
