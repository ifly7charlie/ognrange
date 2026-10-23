import type {Table} from 'apache-arrow';

// Matches the writer in aprs-server/src/horizon.rs: 720 half-degree bearing
// bins, each carrying a monotone envelope of up to MAX_BREAKPOINTS
// (distance, angle) breakpoints - the Pareto frontier of everything received
// on that bearing. bp{j}Km/bp{j}Angle column pairs, null = no breakpoint j;
// ascending in both distance and angle across j
export const BIN_COUNT = 720;
export const BIN_DEG = 360 / BIN_COUNT;
export const MAX_BREAKPOINTS = 5;

// How far below the terrain skyline a breakpoint may sit before the floor
// computation ignores it as physically implausible - mirror of the writer's
// filter (aprs-server/src/horizon.rs SKYLINE_TOLERANCE_DEG, keep in sync) for
// files written before the station had a ground-horizon file
export const SKYLINE_TOLERANCE_DEG = 0.25;

// One envelope step: the lowest angle proven at or beyond km is angle
export type Breakpoint = {km: number; angle: number};

const EFFECTIVE_EARTH_RADIUS_M = (4 / 3) * 6_371_000;
const EARTH_RADIUS_KM = 6371;

export const COMPASS = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW'];

// North in the middle: x is the signed offset from north, S(-180) W(-90) N(0) E(90) S(180)
export const X_TICKS = [-180, -90, 0, 90, 180];
export const X_TICK_LABELS: Record<number, string> = {[-180]: 'S', [-90]: 'W', 0: 'N', 90: 'E', 180: 'S'};

// Hovered bearing bin, shared between the receive-horizon charts, the ground
// chart and the map so they all track the same bearing. source says which
// chart the mouse is over: 'receive' flips the ground chart into profile mode
// and draws the breakpoint staircase on the map; 'ground' fills the receive
// charts' readouts and draws a plain terrain-coloured line. distanceKm is the
// bin's furthest proven distance ('receive', the last breakpoint) or the
// terrain ridge distance ('ground'), null when the bin is empty
export type HorizonHover = {source: 'receive' | 'ground'; bearing: number; distanceKm: number | null; breakpoints?: Breakpoint[]; frequency?: number} | null;

// Great-circle destination from (lat, lng) along a bearing.
// Returns [lng, lat] to match deck.gl position order
export function destinationPoint(lat: number, lng: number, bearingDeg: number, distanceKm: number): [number, number] {
    const d = distanceKm / EARTH_RADIUS_KM;
    const brng = (bearingDeg * Math.PI) / 180;
    const lat1 = (lat * Math.PI) / 180;
    const lng1 = (lng * Math.PI) / 180;
    const lat2 = Math.asin(Math.sin(lat1) * Math.cos(d) + Math.cos(lat1) * Math.sin(d) * Math.cos(brng));
    const lng2 = lng1 + Math.atan2(Math.sin(brng) * Math.sin(d) * Math.cos(lat1), Math.cos(d) - Math.sin(lat1) * Math.sin(lat2));
    return [(((lng2 * 180) / Math.PI + 540) % 360) - 180, (lat2 * 180) / Math.PI];
}

// Inverse of the writer's angle formula: the height above station ground that
// an elevation angle corresponds to at a distance, k=4/3 curvature dip included
export function heightAtDistance(angleDeg: number, distanceKm: number): number {
    const d = distanceKm * 1000;
    const curvatureDip = d / (2 * EFFECTIVE_EARTH_RADIUS_M);
    return Math.tan((angleDeg * Math.PI) / 180 + curvatureDip) * d;
}

// The writer's angle formula (aprs-server/src/horizon.rs elevation_angle_deg):
// elevation angle from the station to a point deltaHM above station ground at
// distanceKm, k=4/3 curvature dip included
export function elevationAngleDeg(deltaHM: number, distanceKm: number): number {
    const d = distanceKm * 1000;
    const curvatureDip = d / (2 * EFFECTIVE_EARTH_RADIUS_M);
    return ((Math.atan2(deltaHM, d) - curvatureDip) * 180) / Math.PI;
}

export interface HorizonPoint {
    x: number; // signed offset from north, S(-180) W(-90) N(0) E(90) - north in the middle
    bearing: number;
    count: number | null;
    // The bin's envelope, ascending in both km and angle; [] = empty bin
    breakpoints: Breakpoint[];
    // Chart series: bp{j} is breakpoints[j]?.angle - recharts needs flat keys
    bp0: number | null;
    bp1: number | null;
    bp2: number | null;
    bp3: number | null;
    bp4: number | null;
}

export const BP_KEYS = ['bp0', 'bp1', 'bp2', 'bp3', 'bp4'] as const;

export function emptyPoint(x: number, bearing: number): HorizonPoint {
    return {x, bearing, count: null, breakpoints: [], bp0: null, bp1: null, bp2: null, bp3: null, bp4: null};
}

// Which horizon file covers the requested period - horizon files exist only for
// month/year/yearnz, so a day view falls back to the month containing that day
export function horizonFileFor(period: string | undefined): string {
    const m = period?.match(/^(day|month|yearnz|year)(?:\.(.+))?$/);
    const type = m?.[1] ?? 'year';
    const date = m?.[2];
    if (type === 'day') {
        return date && date.length >= 7 ? `month.${date.slice(0, 7)}` : 'month';
    }
    return date ? `${type}.${date}` : type;
}

// The bp{j}Km/bp{j}Angle column pairs actually present, probed until missing
// so a future breakpoint-cap bump degrades gracefully
export function breakpointColumns(table: Table) {
    const cols: {km: NonNullable<ReturnType<Table['getChild']>>; angle: NonNullable<ReturnType<Table['getChild']>>}[] = [];
    for (let j = 0; ; j++) {
        const km = table.getChild(`bp${j}Km`);
        const angle = table.getChild(`bp${j}Angle`);
        if (!km || !angle) {
            break;
        }
        cols.push({km, angle});
    }
    return cols;
}

export function breakpointsForRow(cols: ReturnType<typeof breakpointColumns>, row: number): Breakpoint[] {
    const out: Breakpoint[] = [];
    for (const c of cols) {
        const km = c.km.get(row);
        const angle = c.angle.get(row);
        if (km == null || angle == null) {
            break;
        }
        out.push({km, angle});
    }
    return out;
}

export function chartsFromTable(table: Table): {frequency: number; data: HorizonPoint[]}[] {
    const frequency = table.getChild('frequency');
    const bearing = table.getChild('bearing');
    const count = table.getChild('count');
    const cols = breakpointColumns(table);

    // Pre-envelope files (no bp columns) render nothing - greenfield format
    if (!frequency || !bearing || !cols.length) {
        return [];
    }

    const byFreq = new Map<number, Map<number, HorizonPoint>>();
    for (let i = 0; i < table.numRows; i++) {
        const f = frequency.get(i) as number;
        const b = bearing.get(i) as number;
        let freqBins = byFreq.get(f);
        if (!freqBins) {
            byFreq.set(f, (freqBins = new Map()));
        }
        const point = emptyPoint(b >= 180 ? b - 360 : b, b);
        point.count = count?.get(i) ?? null;
        point.breakpoints = breakpointsForRow(cols, i);
        for (let j = 0; j < BP_KEYS.length; j++) {
            point[BP_KEYS[j]] = point.breakpoints[j]?.angle ?? null;
        }
        freqBins.set(b, point);
    }

    // Emit every bin in S->W->N->E->S order so missing bins become gaps in the lines
    return [...byFreq.entries()]
        .sort((a, b) => a[0] - b[0])
        .map(([freq, freqBins]) => {
            const data: HorizonPoint[] = [];
            for (let i = 0; i < BIN_COUNT; i++) {
                const x = -180 + i * BIN_DEG;
                const b = x < 0 ? x + 360 : x;
                data.push(freqBins.get(b) ?? emptyPoint(x, b));
            }
            return {frequency: freq, data};
        });
}
