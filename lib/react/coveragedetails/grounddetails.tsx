import {useMemo, useState, useCallback, useRef} from 'react';
import useSWR from 'swr';
import {useTranslation} from 'next-i18next/pages';
import {Line, ComposedChart, Area, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, ReferenceLine, ReferenceDot, ResponsiveContainer} from 'recharts';

import {cellToLatLng, greatCircleDistance, splitLongToH3Index} from 'h3-js';

import {NEXT_PUBLIC_DATA_URL} from '../../common/config';
import {FLOOR_DISPLAY_MAX_M, floorFrequencyFor, floorHorizonFileFor} from '../../common/floor';

import graphcolours from '../graphcolours';

import {useStationMeta} from '../stationmeta';
import {useDisplayedH3s} from '../displayedh3s';
import {initialBearingDeg, terrainGridFromTable, receiveEnvelopeFromTable, envelopeMargins, envelopeAngleAt, binSpan, binSpansOverlap, sampleIndex} from '../floordata';
import type {PickableFloorDetails} from '../pickabledetails';
import {arrowFetcher} from './arrowfetcher';
import {COMPASS, X_TICKS, X_TICK_LABELS, heightAtDistance, elevationAngleDeg, HorizonHover, Breakpoint} from './horizondata';
import {groundChartFromTable, profileForBearing, groundUrl, groundMeta, minGroundElevation, GroundChart, GroundProfile, DEFAULT_STATION_AGL_M, GROUND_BIN_DEG, GROUND_MAX_KM, GROUND_STEP_KM, GROUND_SAMPLES} from './grounddata';

// Earth tones for the terrain itself, deliberately outside the Tableau-10
// palette - the profile's staircase steps reuse the receive chart's series
// colours (graphcolours) and must not be confusable with the ground
const TERRAIN_LINE = '#7a5c44';
const TERRAIN_STROKE = '#6b4f3a';
const TERRAIN_FILL = '#a0785a';
const RIDGE_MARKER = '#cc4444';
const COVERAGE_FILL = '#4caf50';
// Observed minimum altitudes from the coverage data, dotted over the floor
const ALTITUDE_DOT = '#3366cc';
// The floor line where no envelope step governs (and the dashed inference)
const SIGHT_LINE = '#333333';
// Atmospheric perspective for the panorama: foreground crest lines darken the
// nearer they are, the pale skyline fill sits furthest away (indexes align
// with RIDGE_SERIES_MAX, nearest first)
const RIDGE_SHADES = ['#41301f', '#553f2a', '#6b4f3a', '#83644a', '#9a7a5c'];

// Row keys for the staircase steps, matching graphcolours by index
const STEP_KEYS = ['step0', 'step1', 'step2', 'step3', 'step4'];

const noTooltipContent = () => null;

const compassFor = (bearing: number) => COMPASS[Math.round(bearing / 22.5) % 16];

// Shadow envelope for a terrain profile: walk outward keeping the max
// elevation angle seen so far (from the antenna); where the ray from that
// angle sits above the ground the terrain is radio-shadowed. maxAngle
// deliberately lags one sample so crest samples themselves read as ground
// contact. Returns the per-sample envelope value to draw, null where there is
// nothing to draw: the trailing stretch behind the final crest (the horizon)
// never returns to ground so it has no endpoint, and anything above yCap is
// off the terrain scale
function shadowSeries(points: GroundProfile['points'], viewpoint: number, yCap: number): (number | null)[] {
    let maxAngle = -Infinity;
    const shadowed: boolean[] = [];
    const envelope: number[] = [];
    points.forEach(({km, elevation}, i) => {
        const ray = i > 0 && maxAngle > -Infinity ? viewpoint + heightAtDistance(maxAngle, km) : elevation;
        shadowed.push(ray > elevation + 0.01);
        envelope.push(Math.max(ray, elevation));
        if (i > 0) {
            maxAngle = Math.max(maxAngle, elevationAngleDeg(elevation - viewpoint, km));
        }
    });
    let lastContact = shadowed.length - 1;
    while (lastContact > 0 && shadowed[lastContact]) {
        lastContact--;
    }
    // Ground-contact samples on either side are included so each shadow
    // segment starts and ends on the terrain
    return points.map((_p, i) => (i <= lastContact && (shadowed[i] || shadowed[i - 1] || shadowed[i + 1]) && envelope[i] <= yCap ? envelope[i] : null));
}

// Per-sample running-max elevation angle along a profile (including the
// current sample) - the skyline an aircraft must clear at each distance.
// Same math as floordata's terrainGridFromTable, for one bearing
function profilePrefixMax(points: GroundProfile['points'], viewpoint: number): number[] {
    let maxAngle = -Infinity;
    return points.map(({km, elevation}, i) => {
        if (i > 0) {
            maxAngle = Math.max(maxAngle, elevationAngleDeg(elevation - viewpoint, km));
        }
        return maxAngle;
    });
}

// Mode A: skyline panorama by bearing - the same north-centred degree axis as
// the receive horizon so the two are directly comparable. The shaded area's
// top line is the final skyline (the furthest terrain horizon); darker lines
// inside it are foreground crests visible in front of it, nearest darkest.
// Hover is shared upward tagged source:'ground' so the receive charts above
// track the same bearing; only receive-sourced hovers flip this chart into
// profile mode, so sharing is safe
function GroundHorizonChart({chart, onHover, t}: {chart: GroundChart; onHover: (h: HorizonHover) => void; t: (key: string, opts?: any) => string}) {
    const [hoverPoint, setHoverPoint] = useState<GroundChart['points'][number] | null>(null);

    const chartMouseMove = useCallback(
        (state: any) => {
            const x = Number(state?.activeLabel);
            const point = (state?.isTooltipActive && Number.isFinite(x) ? chart.points[Math.round((x + 180) / GROUND_BIN_DEG)] : null) ?? null;
            setHoverPoint(point);
            onHover(point ? {source: 'ground', bearing: point.bearing, distanceKm: point.horizonDistance ?? null} : null);
        },
        [chart, onHover]
    );
    const chartMouseLeave = useCallback(() => {
        setHoverPoint(null);
        onHover(null);
    }, [onHover]);

    return (
        <>
            <b>{t('title')}</b>
            <br />
            <ResponsiveContainer width="100%" height={190}>
                <ComposedChart data={chart.points} margin={{top: 5, right: 5, left: -10, bottom: 5}} onMouseMove={chartMouseMove} onMouseLeave={chartMouseLeave}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis
                        dataKey="x"
                        type="number"
                        domain={[-180, 180]}
                        ticks={X_TICKS}
                        tickFormatter={(v: number) => X_TICK_LABELS[v] ?? ''}
                        style={{fontSize: '0.7rem'}}
                    />
                    <YAxis domain={['auto', 'auto']} tickFormatter={(v: number) => `${v}°`} style={{fontSize: '0.7rem'}} />
                    <ReferenceLine y={0} stroke="#888888" strokeDasharray="3 3" />
                    <Tooltip content={noTooltipContent} cursor={{stroke: '#888888'}} />
                    <Area
                        dataKey="horizonAngle"
                        stroke={TERRAIN_LINE}
                        strokeWidth={2}
                        fill={TERRAIN_FILL}
                        fillOpacity={0.35}
                        baseValue="dataMin"
                        connectNulls={false}
                        isAnimationActive={false}
                    />
                    {Array.from({length: chart.ridgeCount}, (_, i) => (
                        <Line key={i} dataKey={`ridge${i}`} stroke={RIDGE_SHADES[i] ?? TERRAIN_STROKE} strokeWidth={1} dot={false} connectNulls={false} isAnimationActive={false} />
                    ))}
                </ComposedChart>
            </ResponsiveContainer>
            <div style={{fontSize: '0.75rem', lineHeight: 1.4, marginBottom: '0.75em', minHeight: '1.4em'}}>
                {hoverPoint?.horizonAngle != null ? (
                    <span style={{color: TERRAIN_LINE}}>
                        {t('readout', {
                            bearing: hoverPoint.bearing.toFixed(1),
                            compass: compassFor(hoverPoint.bearing),
                            angle: hoverPoint.horizonAngle.toFixed(2),
                            distance: hoverPoint.horizonDistance ?? '—'
                        })}
                    </span>
                ) : (
                    <span style={{color: '#888888'}}>{t('readout_hint')}</span>
                )}
            </div>
        </>
    );
}

// One point of a profile's floor curve, m MSL at a distance. stair marks a
// proven envelope step (drawn solid in the step's series colour); inferred
// floors (the skyline+margin extension, or anything beyond the last proven
// breakpoint) draw dashed; a plain solid line covers curves with no step
// attribution (the floor-cell arc curve, terrain-only floors)
export type ProfileFloorPoint = {floorMsl: number; stair: {step: number; msl: number} | null; inferred: boolean} | null;

// The shared terrain side-profile: filled terrain silhouette with km/metre
// axes, red-dotted radio-shadow envelope, the floor curve with green likely
// coverage shading above it, and the observed minimum altitudes from the
// displayed coverage data dotted over it. Used both while hovering the
// receive-horizon chart (staircase along that bearing) and when hovering a
// coverage-floor cell on the map (the cell's per-distance floor + marker) -
// one style for every side view. The y-axis is capped at the same
// station-relative ceiling as the map so Alpine skylines clip instead of
// crushing the useful range
function ProfileChart({
    profile,
    stationAgl,
    minElevation,
    maxRangeKm,
    floorAt,
    marker,
    dots,
    t
}: {
    profile: GroundProfile;
    stationAgl: number;
    // Lowest terrain sample across ALL bearings: the y-axis bottom, so the
    // baseline holds still as the hover sweeps from bearing to bearing
    minElevation: number | null;
    // Capability-derived maximum useful range: the floor curve stops there
    // and a dashed marker is drawn when it's under the chart's full extent
    maxRangeKm: number;
    floorAt: ((km: number) => ProfileFloorPoint) | null;
    // Hovered-cell marker (floor-cell mode): reference line + dot at its floor
    marker: {km: number; msl: number} | null;
    dots: {km: number; alt: number}[];
    t: (key: string, opts?: any) => string;
}) {
    const data = useMemo(() => {
        const maxElevation = profile.points.reduce((m, p) => Math.max(m, p.elevation), profile.stationElevation);
        const viewpoint = profile.stationElevation + stationAgl;
        const floors = profile.points.map((p) => (floorAt && p.km <= maxRangeKm ? floorAt(p.km) : null));

        // Y axis locked to min(display clip, highest thing on the graph):
        // floors above station ground + FLOOR_DISPLAY_MAX_M are transparent
        // on the map so the chart never scales past that ceiling, while over
        // flat terrain the floor curve - not the terrain - sets the scale
        const terrainCap = maxElevation + Math.max(50, (maxElevation - profile.stationElevation) * 0.1);
        const floorMax = floors.reduce((m, f) => (f ? Math.max(m, f.floorMsl) : m), -Infinity);
        const dotMax = dots.reduce((m, p) => Math.max(m, p.alt), -Infinity);
        // Round up to a clean 50m step: the domain endpoint is rendered verbatim as the top tick
        const yCap = Math.min(profile.stationElevation + FLOOR_DISPLAY_MAX_M, Math.ceil(Math.max(terrainCap, floorMax, dotMax) / 50) * 50);
        const shadow = shadowSeries(profile.points, viewpoint, yCap);

        const rows = profile.points.map(({km, elevation}, i) => {
            const f = floors[i] && floors[i]!.floorMsl <= yCap ? floors[i] : null;
            const row: Record<string, number | [number, number] | null> = {
                km,
                elevation,
                shadow: shadow[i],
                coverage: f ? [f.floorMsl, yCap] : null,
                dashedFloor: f?.inferred ? f.floorMsl : null,
                solidFloor: f && !f.inferred && !f.stair ? f.floorMsl : null
            };
            for (let j = 0; j < STEP_KEYS.length; j++) {
                row[STEP_KEYS[j]] = f?.stair && f.stair.step === j && f.stair.msl <= yCap ? f.stair.msl : null;
            }
            return row;
        });
        return {yCap, rows, dots: dots.filter((p) => p.alt <= yCap)};
    }, [profile, stationAgl, floorAt, dots, maxRangeKm]);

    return (
        <>
            <b>{t('profile_title', {bearing: profile.bearing.toFixed(1), compass: compassFor(profile.bearing)})}</b>
            <br />
            <ResponsiveContainer width="100%" height={190}>
                <ComposedChart data={data.rows} margin={{top: 5, right: 5, left: 0, bottom: 5}}>
                    <XAxis dataKey="km" type="number" domain={[0, GROUND_MAX_KM]} ticks={[0, 30, 60, 90, 120]} tickFormatter={(v: number) => `${v}km`} style={{fontSize: '0.7rem'}} />
                    <YAxis domain={[minElevation ?? 'dataMin', data.yCap]} allowDataOverflow tickFormatter={(v: number) => `${Math.round(v)}m`} style={{fontSize: '0.7rem'}} />
                    <Area dataKey="coverage" stroke="none" fill={COVERAGE_FILL} fillOpacity={0.18} connectNulls={false} isAnimationActive={false} />
                    <Area dataKey="elevation" stroke={TERRAIN_STROKE} fill={TERRAIN_FILL} fillOpacity={0.9} baseValue={minElevation ?? 'dataMin'} isAnimationActive={false} />
                    <Line dataKey="shadow" stroke={RIDGE_MARKER} strokeWidth={1} strokeDasharray="1 3" dot={false} connectNulls={false} isAnimationActive={false} />
                    {STEP_KEYS.map((k, j) => (
                        <Line key={k} dataKey={k} stroke={graphcolours[j]} strokeWidth={1.5} dot={false} connectNulls={false} isAnimationActive={false} />
                    ))}
                    <Line dataKey="solidFloor" stroke={SIGHT_LINE} strokeWidth={1.5} dot={false} connectNulls={false} isAnimationActive={false} />
                    <Line dataKey="dashedFloor" stroke={SIGHT_LINE} strokeWidth={1} strokeDasharray="5 3" dot={false} connectNulls={false} isAnimationActive={false} />
                    {data.dots.length ? <Scatter data={data.dots} dataKey="alt" isAnimationActive={false} shape={(p: any) => <circle cx={p.cx} cy={p.cy} r={2} fill={ALTITUDE_DOT} fillOpacity={0.75} />} /> : null}
                    {maxRangeKm < GROUND_MAX_KM ? <ReferenceLine x={maxRangeKm} stroke="#888888" strokeDasharray="3 3" /> : null}
                    {marker ? <ReferenceLine x={marker.km} stroke="#888888" strokeDasharray="3 3" /> : null}
                    {marker && marker.msl <= data.yCap ? <ReferenceDot x={marker.km} y={marker.msl} r={4} fill={RIDGE_MARKER} stroke="#ffffff" isFront /> : null}
                </ComposedChart>
            </ResponsiveContainer>
            <div style={{fontSize: '0.75rem', lineHeight: 1.4, marginBottom: '0.75em', minHeight: '1.4em'}}>{profile.horizonAngle != null && profile.horizonDistance != null ? <span style={{color: TERRAIN_STROKE}}>{t('profile_horizon', {angle: profile.horizonAngle.toFixed(2), distance: profile.horizonDistance})}</span> : null}</div>
        </>
    );
}

// Observed minimum altitudes from the displayed coverage data along a
// bearing: polar coordinates computed once per data load, then filtered per
// bearing bin. A cell counts when the bin window it subtends overlaps the one
// the floor curve reads at the same distance - the same binSpan on both sides,
// so every cell that can set the curve is drawn under it. (Cells still go
// missing when the displayed layer/period isn't the one the envelope was
// written from: the floor always reads the year file.) Presence-only layers
// carry synthetic values, not real altitudes
function useObservedDots(station: string, bearing: number | null, maxRangeKm: number): {km: number; alt: number}[] {
    const stationMeta = useStationMeta(station ?? '');
    const hasPos = stationMeta && !isNaN(stationMeta.lat);
    const h3data = useDisplayedH3s();
    const polar = useMemo(() => {
        const d = h3data.d;
        if (!d || h3data.isPresenceOnly || !hasPos) {
            return null;
        }
        const n = d.h3lo.length;
        const dist = new Float32Array(n);
        const brg = new Float32Array(n);
        for (let i = 0; i < n; i++) {
            const [lat, lng] = cellToLatLng(splitLongToH3Index(d.h3lo[i], d.h3hi[i]));
            dist[i] = greatCircleDistance([stationMeta.lat, stationMeta.lng], [lat, lng], 'km');
            brg[i] = initialBearingDeg(stationMeta.lat, stationMeta.lng, lat, lng);
        }
        return {dist, brg};
    }, [h3data.d, h3data.isPresenceOnly, hasPos, hasPos ? stationMeta.lat : 0, hasPos ? stationMeta.lng : 0]);

    return useMemo(() => {
        if (!polar || !h3data.d || bearing == null) {
            return [];
        }
        const out: {km: number; alt: number}[] = [];
        for (let i = 0; i < polar.dist.length; i++) {
            const dKm = polar.dist[i];
            if (dKm > maxRangeKm) {
                continue;
            }
            if (binSpansOverlap(bearing, polar.brg[i], dKm)) {
                out.push({km: dKm, alt: h3data.d.minAlt[i]});
            }
        }
        return out;
    }, [polar, h3data.d, bearing, maxRangeKm]);
}

// Floor curve along a hovered receive-horizon bearing, built from that bin's
// envelope breakpoints against the profile's own skyline: solid coloured
// steps are the proven staircase (first breakpoint at or beyond each
// distance), the dashed line is the inferred floor - the skyline-plus-margin
// extension where the station demonstrably hears down to its terrain
// horizon, and the outward continuation beyond the last proven breakpoint.
// Same margin rule as floordata's envelopeAngleAt, so this profile matches
// what the coverage-floor map would claim on this bearing
function staircaseFloorAt(profile: GroundProfile, stationAgl: number, breakpoints: Breakpoint[]): (km: number) => ProfileFloorPoint {
    const viewpoint = profile.stationElevation + stationAgl;
    const prefix = profilePrefixMax(profile.points, viewpoint);
    let margin = NaN;
    for (const b of breakpoints) {
        const p = prefix[Math.min(Math.round(b.km / GROUND_STEP_KM), prefix.length - 1)];
        if (p > -Infinity) {
            const v = Math.max(b.angle - p, 0);
            if (Number.isNaN(margin) || v < margin) {
                margin = v;
            }
        }
    }
    return (km: number) => {
        if (!breakpoints.length) {
            return null;
        }
        const stairIdx = breakpoints.findIndex((b) => b.km >= km);
        const stairAngle = stairIdx >= 0 ? breakpoints[stairIdx].angle : breakpoints[breakpoints.length - 1].angle;
        let angle = stairAngle;
        let inferred = stairIdx < 0;
        const p = prefix[Math.min(Math.round(km / GROUND_STEP_KM), prefix.length - 1)];
        if (!Number.isNaN(margin) && p > -Infinity && p + margin < angle) {
            angle = p + margin;
            inferred = true;
        }
        return {
            floorMsl: viewpoint + heightAtDistance(angle, km),
            stair: stairIdx >= 0 ? {step: stairIdx, msl: viewpoint + heightAtDistance(stairAngle, km)} : null,
            inferred
        };
    };
}

// Terrain side profile along the bearing of a hovered coverage-floor cell:
// the same silhouette as the receive-hover profile, with the per-distance
// floor curve the map computes (terrain-only or terrain+receive depending on
// the active visualisation) and a marker at the hovered cell's floor. The
// curve is evaluated with the same envelope/margin/arc rules as the floor
// disc, from the same two arrow files, so curve, marker and readout all
// agree with the map. Renders nothing until the terrain table is loaded
export function FloorProfileChart({
    details,
    station,
    visualisation,
    maxRangeKm,
    dateStart,
    layers,
    env
}: {
    details: PickableFloorDetails;
    station: string;
    visualisation?: string;
    // Capability-derived disc radius, threaded from the same source the map
    // uses so the chart never disagrees with the disc it explains
    maxRangeKm: number;
    // Selected period / layer set - picks the same horizon file and frequency
    // the floor disc was computed from
    dateStart?: string;
    layers?: string;
    env?: {NEXT_PUBLIC_DATA_URL?: string};
}) {
    const {t} = useTranslation('common', {keyPrefix: 'details.ground'});
    const DATA_URL = env?.NEXT_PUBLIC_DATA_URL || NEXT_PUBLIC_DATA_URL;
    const stationMeta = useStationMeta(station ?? '');
    const url = station ? groundUrl(DATA_URL, station) : null;
    const {data: table} = useSWR(url, arrowFetcher, {revalidateOnFocus: false});
    const horizonUrl = station ? `${DATA_URL}${station}/${station}.${floorHorizonFileFor(dateStart)}.horizon.arrow` : null;
    const {data: horizonTable} = useSWR(horizonUrl, arrowFetcher, {revalidateOnFocus: false});

    const hasPos = stationMeta && !isNaN(stationMeta.lat);
    const cellPos = cellToLatLng(details.h);
    const bearing = hasPos ? initialBearingDeg(stationMeta.lat, stationMeta.lng, cellPos[0], cellPos[1]) : null;
    const distanceKm = hasPos ? greatCircleDistance([stationMeta.lat, stationMeta.lng], cellPos, 'km') : null;

    // Keyed on the 0.5° bin, not the raw bearing, so sweeping across cells in
    // the same direction doesn't recompute the profile
    const bin = bearing == null ? null : Math.round(bearing / GROUND_BIN_DEG);
    const profile = useMemo(() => (table && bearing != null ? profileForBearing(table, bearing) : null), [table, bin]);
    const stationAgl = useMemo(() => (table ? groundMeta(table).stationAgl ?? DEFAULT_STATION_AGL_M : 0), [table]);
    const minElevation = useMemo(() => (table ? minGroundElevation(table) : null), [table]);

    // The same reshaped inputs the floor worker uses, on the same tables
    const grid = useMemo(() => (table ? terrainGridFromTable(table) : null), [table]);
    const frequency = floorFrequencyFor(layers);
    const receive = useMemo(() => (horizonTable && grid ? receiveEnvelopeFromTable(horizonTable, frequency, grid) : null), [horizonTable, grid, frequency]);
    const margins = useMemo(() => (receive && grid ? envelopeMargins(receive, grid) : null), [receive, grid]);

    const terrainActive = visualisation === 'terrainFloor';
    const floorAt = useMemo(() => {
        if (!grid || bearing == null) {
            return null;
        }
        // Terrain skyline across the cell arc at each distance - max across
        // the subtended bins, exactly like computeFloorDisc
        const terrainAngleAt = (km: number): number => {
            const [firstBin, binCount] = binSpan(bearing, km);
            const s = sampleIndex(km);
            let m = -Infinity;
            for (let i = 0; i < binCount; i++) {
                const v = grid.prefixMax[((firstBin + i) % 720) * GROUND_SAMPLES + s];
                if (!Number.isNaN(v) && v > m) {
                    m = v;
                }
            }
            return m;
        };
        return (km: number): ProfileFloorPoint => {
            const terrainAngle = terrainAngleAt(km);
            if (terrainAngle === -Infinity) {
                return null;
            }
            if (terrainActive || !receive) {
                // Terrain-only floor (or no receive data at all, where the
                // coverage floor falls back to terrain)
                return {floorMsl: grid.viewpoint + heightAtDistance(terrainAngle, km), stair: null, inferred: false};
            }
            const [firstBin, binCount] = binSpan(bearing, km);
            const rx = envelopeAngleAt(receive, margins, grid, firstBin, binCount, km);
            if (rx.angle === -Infinity) {
                // Nothing ever heard over this arc: no likely-coverage claim
                return null;
            }
            const governed = Math.max(terrainAngle, rx.angle);
            // Where the receive constraint governs, it is only proof inside
            // the measured range: past the winning bin's last breakpoint the
            // curve is that breakpoint's angle carried outward, exactly the
            // case staircaseFloorAt marks with stairIdx < 0
            const inferred = rx.angle > terrainAngle && (rx.extended || km > rx.provenKm);
            return {floorMsl: grid.viewpoint + heightAtDistance(governed, km), stair: null, inferred};
        };
    }, [grid, receive, margins, bearing, terrainActive]);

    // The hovered cell's own bearing, not the 0.5deg profile bin: floorAt
    // spreads its arc from there too, so both sides of the chart agree on
    // which bins are in play
    const dots = useObservedDots(station, bearing, maxRangeKm);

    if (!profile || distanceKm == null) {
        return null;
    }

    const floorMsl = terrainActive ? details.terrainFloor : details.coverageFloor;
    return <ProfileChart profile={profile} stationAgl={stationAgl} minElevation={minElevation} maxRangeKm={maxRangeKm} floorAt={floorAt} marker={{km: distanceKm, msl: floorMsl}} dots={dots} t={t} />;
}

// Ground (terrain) views for the station details panel, fed by the static
// per-station ground-horizon.arrow written by the rollup. Normally shows the
// skyline panorama by bearing; while the receive-horizon chart above is being
// hovered it swaps to the terrain side profile along that bearing (both modes
// keep the same vertical footprint so the swap never moves the hovered chart).
// Renders nothing when the file doesn't exist yet
export function GroundDetails({
    station,
    horizonHover,
    setHorizonHover,
    beaconAltitude,
    maxRangeKm,
    env
}: {
    station: string;
    horizonHover?: HorizonHover;
    setHorizonHover?: (h: HorizonHover) => void;
    // Registered receiver altitude (m MSL) from the station's location beacon,
    // via the details API - null/undefined when the station never beaconed one
    beaconAltitude?: number | null;
    // Capability-derived maximum useful range on 868MHz; the profile chart
    // keeps the full extent when the hover comes from a 1090 receive chart
    maxRangeKm?: number;
    env?: {NEXT_PUBLIC_DATA_URL?: string};
}) {
    const {t} = useTranslation('common', {keyPrefix: 'details.ground'});
    const DATA_URL = env?.NEXT_PUBLIC_DATA_URL || NEXT_PUBLIC_DATA_URL;

    const url = station ? groundUrl(DATA_URL, station) : null;
    const {data: table} = useSWR(url, arrowFetcher, {revalidateOnFocus: false});

    const chart = useMemo(() => (table ? groundChartFromTable(table) : null), [table]);
    // null = no beacon-derived height in the file (default assumed by the
    // writer, or a pre-capture file) - the fallback keeps the sight lines on
    // the same viewpoint grounddata uses for the skyline
    const aglMeta = useMemo(() => (table ? groundMeta(table).stationAgl : null), [table]);
    const stationAgl = aglMeta ?? DEFAULT_STATION_AGL_M;
    const minElevation = useMemo(() => (table ? minGroundElevation(table) : null), [table]);

    // Mouse-move fires per pixel but bins are 0.5°, so only push changes upward
    const lastHover = useRef<string>('');
    const onHover = useCallback(
        (h: HorizonHover) => {
            const key = h ? `${h.bearing}|${h.distanceKm}` : '';
            if (key === lastHover.current) {
                return;
            }
            lastHover.current = key;
            setHorizonHover?.(h);
        },
        [setHorizonHover]
    );

    // Only a hover on the receive charts swaps this panel to profile mode -
    // our own shared hover (source 'ground') must leave mode A in place.
    // Keyed on the hovered bearing (not the whole hover object) so per-pixel
    // moves within a 0.5° bin don't recompute the profile
    const hoverBearing = horizonHover?.source === 'receive' ? horizonHover.bearing : undefined;
    const profile = useMemo(() => (table && hoverBearing != null ? profileForBearing(table, hoverBearing) : null), [table, hoverBearing]);

    const profileMaxRangeKm = horizonHover?.frequency === 1090 ? GROUND_MAX_KM : maxRangeKm ?? GROUND_MAX_KM;
    const breakpoints = horizonHover?.source === 'receive' ? horizonHover.breakpoints : undefined;
    const floorAt = useMemo(() => (profile && breakpoints?.length ? staircaseFloorAt(profile, stationAgl, breakpoints) : null), [profile, stationAgl, breakpoints]);
    const dots = useObservedDots(station, profile?.bearing ?? null, profileMaxRangeKm);

    if (!chart) {
        return null;
    }

    // A receiver registered at/below its own terrain is almost certainly
    // misconfigured (e.g. beaconing /A=000000) - both horizon charts are
    // computed from a default antenna height in that case, so warn on them
    const ground = chart.stationElevation;
    const registeredLow = beaconAltitude != null && ground != null && beaconAltitude - ground < 2;

    return (
        <>
            {profile ? <ProfileChart profile={profile} stationAgl={stationAgl} minElevation={minElevation} maxRangeKm={profileMaxRangeKm} floorAt={floorAt} marker={null} dots={dots} t={t} /> : <GroundHorizonChart chart={chart} onHover={onHover} t={t} />}
            {registeredLow ? <div style={{fontSize: '0.75rem', lineHeight: 1.4, marginBottom: '0.25em', color: RIDGE_MARKER}}>{t('antenna_warning', {beacon: Math.round(beaconAltitude!), ground: Math.round(ground!)})}</div> : null}
            {ground != null ? <div style={{fontSize: '0.75rem', lineHeight: 1.4, marginBottom: '0.75em'}}>{t(aglMeta != null ? 'antenna_info' : 'antenna_info_default', {agl: Math.round(stationAgl), ground: Math.round(ground)})}</div> : null}
        </>
    );
}
