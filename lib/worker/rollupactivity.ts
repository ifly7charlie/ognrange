import {Epoch} from '../bin/types';

export interface ActivityRange {
    start: Epoch;
    end: Epoch;
    cells: number;
}

export interface RollupActivity {
    ranges: ActivityRange[];
    totalRollups: number;
    activeRollups: number;
    totalCells: number;
    firstSeen: Epoch;
    lastSeen: Epoch;
    lastRollup: Epoch;
}

const MAX_ACTIVITY_RANGES = 500;

export function updateActivity(meta: any, h3source: number, periodStart: Epoch, periodEnd: Epoch, now: Epoch): void {
    if (!meta.activity) {
        meta.activity = {
            ranges: [],
            totalRollups: 0,
            activeRollups: 0,
            totalCells: 0,
            firstSeen: 0 as Epoch,
            lastSeen: 0 as Epoch,
            lastRollup: 0 as Epoch
        } as RollupActivity;
    }

    const a: RollupActivity = meta.activity;
    a.totalRollups++;
    a.lastRollup = now;

    if (h3source > 0) {
        a.activeRollups++;
        a.totalCells += h3source;

        if (!a.firstSeen) {
            a.firstSeen = periodStart;
        }
        a.lastSeen = periodEnd;

        const last = a.ranges.length > 0 ? a.ranges[a.ranges.length - 1] : null;
        if (last && last.end === periodStart) {
            last.end = periodEnd;
            last.cells += h3source;
        } else {
            a.ranges.push({start: periodStart, end: periodEnd, cells: h3source});
        }

        if (a.ranges.length > MAX_ACTIVITY_RANGES) {
            a.ranges = a.ranges.slice(-MAX_ACTIVITY_RANGES);
        }
    }
}
