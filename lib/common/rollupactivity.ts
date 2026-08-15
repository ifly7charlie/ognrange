// Shape of the per-station rollup activity summary produced by the
// aprs-server and returned by the station details API
export interface ActivityRange {
    start: number; // epoch seconds
    end: number;
    cells: number;
}

export interface RollupActivity {
    ranges: ActivityRange[];
    totalRollups: number;
    activeRollups: number;
    totalCells: number;
    firstSeen: number;
    lastSeen: number;
    lastRollup: number;
}
