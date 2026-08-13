import type {Table} from 'apache-arrow';

import {elevationAngleDeg} from './horizondata';

// Matches the writer in aprs-server/src/ground_horizon.rs: 720 half-degree
// bearing bins, each row carrying a FixedSizeList of terrain elevations
// (m MSL) sampled every 0.5km out to 120km. Sample 0 is the station's own
// ground elevation. Everything angular (skyline, visible crests, shadow) is
// derived here from the elevations with the writer's k=4/3 angle formula -
// the horizonAngle/horizonDistance columns older files carry are ignored
export const GROUND_BIN_COUNT = 720;
export const GROUND_BIN_DEG = 360 / GROUND_BIN_COUNT;
export const GROUND_STEP_KM = 0.5;
export const GROUND_MAX_KM = 120;
export const GROUND_SAMPLES = 241;

// Antenna height assumed when the file carries no beacon-derived height
// (stationAgl NaN, or absent in pre-capture files). Keep in sync with
// GROUND_STATION_AGL_M in aprs-server/src/config.rs
export const DEFAULT_STATION_AGL_M = 3;

// How many foreground crest lines the bearing chart draws in front of the skyline
export const RIDGE_SERIES_MAX = 5;
// Foreground crests closer than this are the station's own ground rolling
// away (flat terrain always makes ground contact at the first sample), not a
// ridge worth drawing; the final skyline is exempt
export const RIDGE_MIN_KM = 2;
// A crest must shadow at least this many samples behind it to count as a
// distinct ridge rather than sampling noise on a single face
const RIDGE_MIN_SHADOW_SAMPLES = 2;

export interface Crest {
    angle: number;
    km: number;
}

// Walk a terrain ray outward keeping the running-max elevation angle from the
// antenna (sample 0 + agl). Every ground contact the ray subsequently leaves
// for a real shadow stretch is a visible crest; returned nearest-first, and
// the last entry is always the skyline - the final terrain horizon. Shallow
// dips shorter than RIDGE_MIN_SHADOW_SAMPLES merge into the following contact
export function visibleCrests(elevations: ArrayLike<number>, agl: number): Crest[] {
    const viewpoint = elevations[0] + agl;
    const crests: Crest[] = [];
    let candidate: Crest | null = null;
    let maxAngle = -Infinity;
    let shadowRun = 0;
    for (let i = 1; i < elevations.length; i++) {
        const km = i * GROUND_STEP_KM;
        const angle = elevationAngleDeg(elevations[i] - viewpoint, km);
        if (angle > maxAngle) {
            if (candidate && shadowRun >= RIDGE_MIN_SHADOW_SAMPLES) {
                crests.push(candidate);
            }
            candidate = {angle, km};
            maxAngle = angle;
            shadowRun = 0;
        } else {
            shadowRun++;
        }
    }
    if (candidate) {
        crests.push(candidate);
    }
    return crests;
}

// One bearing bin of the ground-horizon chart, x convention identical to
// HorizonPoint (signed offset from north) so the series could later be
// overlaid on the receive-horizon chart. horizonAngle/horizonDistance are the
// computed skyline; ridge0..ridgeN are foreground visible crests, nearest first
export interface GroundPoint {
    x: number;
    bearing: number;
    horizonAngle: number | null;
    horizonDistance: number | null;
    [ridge: `ridge${number}`]: number | null;
}

export interface GroundChart {
    points: GroundPoint[];
    // Highest foreground-crest count across all bearings: how many ridge
    // series the chart needs to render
    ridgeCount: number;
    // Station ground elevation (m MSL) - sample 0 of the rays
    stationElevation: number | null;
}

export interface GroundProfile {
    bearing: number;
    // Terrain elevation (m MSL) every GROUND_STEP_KM from the station outward
    points: {km: number; elevation: number}[];
    stationElevation: number;
    horizonAngle: number | null;
    horizonDistance: number | null;
}

// The ground-horizon file is per-station and non-accumulator (terrain is
// static) - no period in the name; the server rewrites .arrow to the .gz
export function groundUrl(dataUrl: string, station: string): string {
    return `${dataUrl}${station}/${station}.ground-horizon.arrow`;
}

export function groundChartFromTable(table: Table): GroundChart | null {
    const bearing = table.getChild('bearing');
    const elevations = table.getChild('elevations');
    if (!bearing || !elevations) {
        return null;
    }
    const agl = groundMeta(table).stationAgl ?? DEFAULT_STATION_AGL_M;

    const byBearing = new Map<number, GroundPoint>();
    let ridgeCount = 0;
    let stationElevation: number | null = null;
    for (let i = 0; i < table.numRows; i++) {
        const cell = elevations.get(i);
        if (!cell) {
            continue;
        }
        const b = bearing.get(i) as number;
        const values = cell.toArray() as ArrayLike<number>;
        stationElevation = stationElevation ?? values[0] ?? null;
        const crests = visibleCrests(values, agl);
        const skyline = crests[crests.length - 1] ?? null;
        const ridges = crests
            .slice(0, -1)
            .filter((c) => c.km >= RIDGE_MIN_KM)
            .slice(0, RIDGE_SERIES_MAX);
        ridgeCount = Math.max(ridgeCount, ridges.length);
        const point: GroundPoint = {
            x: b >= 180 ? b - 360 : b,
            bearing: b,
            horizonAngle: skyline?.angle ?? null,
            horizonDistance: skyline ? Math.round(skyline.km) : null
        };
        ridges.forEach((c, ri) => {
            point[`ridge${ri}`] = c.angle;
        });
        byBearing.set(b, point);
    }

    // Same S->W->N->E->S emission as the receive horizon: a valid file always
    // has all 720 bins, but any gaps become line gaps rather than interpolation
    const points: GroundPoint[] = [];
    for (let i = 0; i < GROUND_BIN_COUNT; i++) {
        const x = -180 + i * GROUND_BIN_DEG;
        const b = x < 0 ? x + 360 : x;
        points.push(byBearing.get(b) ?? {x, bearing: b, horizonAngle: null, horizonDistance: null});
    }
    return {points, ridgeCount, stationElevation};
}

// Terrain side profile along the bin containing `bearingDeg` (the hovered
// receive-horizon bin). Row lookup is by the bearing column, trying the
// natural row order first
export function profileForBearing(table: Table, bearingDeg: number): GroundProfile | null {
    const bearing = table.getChild('bearing');
    const elevations = table.getChild('elevations');
    if (!bearing || !elevations) {
        return null;
    }

    const idx = ((Math.round(bearingDeg / GROUND_BIN_DEG) % GROUND_BIN_COUNT) + GROUND_BIN_COUNT) % GROUND_BIN_COUNT;
    const target = idx * GROUND_BIN_DEG;
    let row = bearing.get(idx) === target ? idx : -1;
    if (row < 0) {
        for (let i = 0; i < table.numRows; i++) {
            if (bearing.get(i) === target) {
                row = i;
                break;
            }
        }
    }
    if (row < 0) {
        return null;
    }

    const cell = elevations.get(row);
    if (!cell) {
        return null;
    }
    const values = Array.from(cell.toArray() as ArrayLike<number>);
    const crests = visibleCrests(values, groundMeta(table).stationAgl ?? DEFAULT_STATION_AGL_M);
    const skyline = crests[crests.length - 1] ?? null;
    return {
        bearing: target,
        points: values.map((elevation, i) => ({km: i * GROUND_STEP_KM, elevation})),
        stationElevation: values[0] ?? 0,
        horizonAngle: skyline?.angle ?? null,
        horizonDistance: skyline ? Math.round(skyline.km) : null
    };
}

// Lowest terrain sample across every bearing ray (sample 0 - the station
// ground - included). The profile charts pin their y-axis bottom to this so
// the baseline doesn't jump as the hover sweeps from bearing to bearing
export function minGroundElevation(table: Table): number | null {
    const elevations = table.getChild('elevations');
    if (!elevations) {
        return null;
    }
    let min = Infinity;
    for (let i = 0; i < table.numRows; i++) {
        const cell = elevations.get(i);
        if (!cell) {
            continue;
        }
        const values = cell.toArray() as ArrayLike<number>;
        for (let s = 0; s < values.length; s++) {
            min = Math.min(min, values[s]);
        }
    }
    return Number.isFinite(min) ? min : null;
}

export interface GroundMeta {
    stationLat: number | null;
    stationLng: number | null;
    // Beacon-derived antenna height above ground (m); null when the writer
    // assumed the default height (persisted NaN) or the file predates height
    // capture - consumers then fall back to DEFAULT_STATION_AGL_M
    stationAgl: number | null;
    stepKm: number;
    maxKm: number;
    generatedAt: number | null;
}

export function groundMeta(table: Table): GroundMeta {
    const num = (key: string): number | null => {
        const v = table.schema.metadata.get(key);
        const n = v != null ? Number(v) : NaN;
        return Number.isFinite(n) ? n : null;
    };
    return {
        stationLat: num('stationLat'),
        stationLng: num('stationLng'),
        stationAgl: num('stationAgl'),
        stepKm: num('stepKm') ?? GROUND_STEP_KM,
        maxKm: num('maxKm') ?? GROUND_MAX_KM,
        generatedAt: num('generatedAt')
    };
}
