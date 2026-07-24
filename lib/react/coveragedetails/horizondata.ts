import type {Table} from 'apache-arrow';

// Matches the writer in aprs-server/src/horizon.rs: 720 half-degree bearing bins,
// band columns are the minimum elevation angle within each distance band (nullable)
export const BIN_COUNT = 720;
export const BIN_DEG = 360 / BIN_COUNT;
export const BAND_KEYS = ['angle5km', 'angle10km', 'angle20km', 'angle30km', 'angle50km', 'angle90km'] as const;

// Distance range (km) each band column covers; the first band starts at the
// writer's 2 km minimum-distance window
export const BAND_RANGES_KM: Record<(typeof BAND_KEYS)[number], [number, number]> = {
    angle5km: [2, 5],
    angle10km: [5, 10],
    angle20km: [10, 20],
    angle30km: [20, 30],
    angle50km: [30, 50],
    angle90km: [50, 90]
};

const EFFECTIVE_EARTH_RADIUS_M = (4 / 3) * 6_371_000;
const EARTH_RADIUS_KM = 6371;

// Hovered bin on the horizon chart, used to draw a bearing line on the map.
// distanceKm is the bin's furthest received cell, null for empty bins
export type HorizonHover = {bearing: number; distanceKm: number | null} | null;

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

export interface HorizonPoint {
    x: number; // signed offset from north, S(-180) W(-90) N(0) E(90) - north in the middle
    bearing: number;
    lowestAngle: number | null;
    lowestAgl: number | null;
    lowestDistance: number | null;
    maxDistance: number | null;
    count: number | null;
    angle5km: number | null;
    angle10km: number | null;
    angle20km: number | null;
    angle30km: number | null;
    angle50km: number | null;
    angle90km: number | null;
}

export function emptyPoint(x: number, bearing: number): HorizonPoint {
    return {
        x,
        bearing,
        lowestAngle: null,
        lowestAgl: null,
        lowestDistance: null,
        maxDistance: null,
        count: null,
        angle5km: null,
        angle10km: null,
        angle20km: null,
        angle30km: null,
        angle50km: null,
        angle90km: null
    };
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

export function chartsFromTable(table: Table): {frequency: number; data: HorizonPoint[]}[] {
    const frequency = table.getChild('frequency');
    const bearing = table.getChild('bearing');
    const lowestAngle = table.getChild('lowestAngle');
    const lowestAgl = table.getChild('lowestAgl');
    const lowestDistance = table.getChild('lowestDistance');
    const maxDistance = table.getChild('maxDistance');
    const count = table.getChild('count');
    const bands = BAND_KEYS.map((k) => table.getChild(k));

    if (!frequency || !bearing || !lowestAngle) {
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
        point.lowestAngle = lowestAngle.get(i);
        point.lowestAgl = lowestAgl?.get(i) ?? null;
        point.lowestDistance = lowestDistance?.get(i) ?? null;
        point.maxDistance = maxDistance?.get(i) ?? null;
        point.count = count?.get(i) ?? null;
        for (let bi = 0; bi < BAND_KEYS.length; bi++) {
            point[BAND_KEYS[bi]] = bands[bi]?.get(i) ?? null;
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
