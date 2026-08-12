import {useMemo, useState, useCallback, useRef} from 'react';
import useSWR from 'swr';
import {useTranslation} from 'next-i18next';
import {Line, ComposedChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ReferenceLine, ResponsiveContainer} from 'recharts';

import {NEXT_PUBLIC_DATA_URL} from '../../common/config';

import graphcolours from '../graphcolours';

import {arrowFetcher} from './arrowfetcher';
import {COMPASS, X_TICKS, X_TICK_LABELS, heightAtDistance, elevationAngleDeg, BAND_KEYS, BAND_RANGES_KM, HorizonHover} from './horizondata';
import {groundChartFromTable, profileForBearing, groundUrl, groundMeta, GroundChart, GroundProfile, GROUND_BIN_DEG, GROUND_MAX_KM, GROUND_STEP_KM} from './grounddata';

// Earth tones for the terrain itself, deliberately outside the Tableau-10
// palette - the profile's per-band sight segments reuse the receive chart's
// band colours (graphcolours) and must not be confusable with the ground
const TERRAIN_LINE = '#7a5c44';
const TERRAIN_STROKE = '#6b4f3a';
const TERRAIN_FILL = '#a0785a';
const RIDGE_MARKER = '#cc4444';
// The predicted sight line matches the receive chart's any-distance series colour
const SIGHT_LINE = '#333333';
// Atmospheric perspective for the panorama: foreground crest lines darken the
// nearer they are, the pale skyline fill sits furthest away (indexes align
// with RIDGE_SERIES_MAX, nearest first)
const RIDGE_SHADES = ['#41301f', '#553f2a', '#6b4f3a', '#83644a', '#9a7a5c'];

const noTooltipContent = () => null;

const compassFor = (bearing: number) => COMPASS[Math.round(bearing / 22.5) % 16];

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

// Mode B: terrain side profile along the hovered receive-horizon bearing - a
// filled terrain-mass silhouette with km/metre axes, visually unmistakable
// from the degree-axis line charts. Solid sight segments show each distance
// band's lowest receive angle across that band's range only, in the band's
// series colour - a single min-of-all-bands line would wrongly imply the
// close-in low angle carries on behind terrain the far bands actually clear.
// The dashed black line extends the outermost measured band's angle beyond
// the furthest received cell as prediction. Any sight line that climbs off
// the top of the terrain scale simply stops. Red dotted segments are radio
// shadow: from each crest that sets a new maximum elevation angle, the
// antenna ray continues until the ground next rises through it - terrain
// under a segment is hidden from the antenna. Behind the final crest (the
// horizon) the ray never returns to ground, so that stretch is not drawn
function GroundProfileChart({
    profile,
    bands,
    lowestAngle,
    receivedKm,
    stationAgl,
    t
}: {
    profile: GroundProfile;
    bands: (number | null)[] | undefined;
    lowestAngle: number | null;
    receivedKm: number | null;
    stationAgl: number;
    t: (key: string, opts?: any) => string;
}) {
    const data = useMemo(() => {
        const maxElevation = profile.points.reduce((m, p) => Math.max(m, p.elevation), profile.stationElevation);
        const yCap = maxElevation + Math.max(50, (maxElevation - profile.stationElevation) * 0.1);
        // The sight lines leave from the antenna, not the ground - the same
        // viewpoint the stored horizon angles use
        const viewpoint = profile.stationElevation + stationAgl;
        const at = (angle: number, km: number): number | null => {
            const h = viewpoint + heightAtDistance(angle, km);
            return h <= yCap ? h : null;
        };

        // The prediction continues the outermost measured band's angle from
        // where its solid segment ends; with no band data at all fall back to
        // the bin's overall lowest angle across the full width
        let predictedAngle: number | null = null;
        let predictedFrom = 0;
        for (let i = BAND_KEYS.length - 1; i >= 0; i--) {
            const a = bands?.[i];
            if (a != null) {
                predictedAngle = a;
                predictedFrom = Math.min(BAND_RANGES_KM[BAND_KEYS[i]][1], receivedKm ?? Infinity);
                break;
            }
        }
        if (predictedAngle == null) {
            predictedAngle = lowestAngle;
        }

        // Shadow envelope: walk outward keeping the max elevation angle seen so
        // far (from the antenna); where the ray from that angle sits above the
        // ground the terrain is shadowed. maxAngle deliberately lags one sample
        // so crest samples themselves read as ground contact
        let maxAngle = -Infinity;
        const shadowed: boolean[] = [];
        const envelope: number[] = [];
        profile.points.forEach(({km, elevation}, i) => {
            const ray = i > 0 && maxAngle > -Infinity ? viewpoint + heightAtDistance(maxAngle, km) : elevation;
            shadowed.push(ray > elevation + 0.01);
            envelope.push(Math.max(ray, elevation));
            if (i > 0) {
                maxAngle = Math.max(maxAngle, elevationAngleDeg(elevation - viewpoint, km));
            }
        });
        // A shadow segment runs crest -> next ground intersection. Behind the
        // final crest (the horizon) the ray never meets ground again, so there
        // is no endpoint to draw to - left in, it just climbs to the top of
        // the chart and stretches the y-axis with it
        let lastContact = shadowed.length - 1;
        while (lastContact > 0 && shadowed[lastContact]) {
            lastContact--;
        }

        return profile.points.map(({km, elevation}, i) => {
            const row: Record<string, number | null> = {
                km,
                elevation,
                // Ground-contact samples on either side are included so each
                // shadow segment starts and ends on the terrain
                shadow: i <= lastContact && (shadowed[i] || shadowed[i - 1] || shadowed[i + 1]) && envelope[i] <= yCap ? envelope[i] : null,
                // Starts one sample early so it joins the end of the solid segment
                predicted: predictedAngle != null && km > predictedFrom - GROUND_STEP_KM ? at(predictedAngle, km) : null
            };
            BAND_KEYS.forEach((k, i) => {
                const a = bands?.[i];
                const [start, end] = BAND_RANGES_KM[k];
                row[k] = a != null && km >= start && km <= Math.min(end, receivedKm ?? end) ? at(a, km) : null;
            });
            return row;
        });
    }, [profile, bands, lowestAngle, receivedKm, stationAgl]);

    return (
        <>
            <b>{t('profile_title', {bearing: profile.bearing.toFixed(1), compass: compassFor(profile.bearing)})}</b>
            <br />
            <ResponsiveContainer width="100%" height={190}>
                <ComposedChart data={data} margin={{top: 5, right: 5, left: 0, bottom: 5}}>
                    <XAxis
                        dataKey="km"
                        type="number"
                        domain={[0, GROUND_MAX_KM]}
                        ticks={[0, 30, 60, 90, 120]}
                        tickFormatter={(v: number) => `${v}km`}
                        style={{fontSize: '0.7rem'}}
                    />
                    <YAxis domain={['dataMin', 'auto']} tickFormatter={(v: number) => `${v}m`} style={{fontSize: '0.7rem'}} />
                    <Area dataKey="elevation" stroke={TERRAIN_STROKE} fill={TERRAIN_FILL} fillOpacity={0.9} baseValue="dataMin" isAnimationActive={false} />
                    <Line dataKey="shadow" stroke={RIDGE_MARKER} strokeWidth={1} strokeDasharray="1 3" dot={false} connectNulls={false} isAnimationActive={false} />
                    {BAND_KEYS.map((k, i) => (
                        <Line key={k} dataKey={k} stroke={graphcolours[i]} strokeWidth={1.5} dot={false} connectNulls={false} isAnimationActive={false} />
                    ))}
                    <Line dataKey="predicted" stroke={SIGHT_LINE} strokeWidth={1} strokeDasharray="5 3" dot={false} connectNulls={false} isAnimationActive={false} />
                </ComposedChart>
            </ResponsiveContainer>
            <div style={{fontSize: '0.75rem', lineHeight: 1.4, marginBottom: '0.75em', minHeight: '1.4em'}}>
                {profile.horizonAngle != null && profile.horizonDistance != null ? (
                    <span style={{color: TERRAIN_STROKE}}>
                        {t('profile_horizon', {angle: profile.horizonAngle.toFixed(2), distance: profile.horizonDistance})}
                    </span>
                ) : null}
            </div>
        </>
    );
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
    env
}: {
    station: string;
    horizonHover?: HorizonHover;
    setHorizonHover?: (h: HorizonHover) => void;
    // Registered receiver altitude (m MSL) from the station's location beacon,
    // via the details API - null/undefined when the station never beaconed one
    beaconAltitude?: number | null;
    env?: {NEXT_PUBLIC_DATA_URL?: string};
}) {
    const {t} = useTranslation('common', {keyPrefix: 'details.ground'});
    const DATA_URL = env?.NEXT_PUBLIC_DATA_URL || NEXT_PUBLIC_DATA_URL;

    const url = station ? groundUrl(DATA_URL, station) : null;
    const {data: table} = useSWR(url, arrowFetcher, {revalidateOnFocus: false});

    const chart = useMemo(() => (table ? groundChartFromTable(table) : null), [table]);
    const stationAgl = useMemo(() => (table ? groundMeta(table).stationAgl : 0), [table]);

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
    const lowestAngle = useMemo(() => {
        const angles = (horizonHover?.bands ?? []).filter((a): a is number => a != null);
        return angles.length ? Math.min(...angles) : null;
    }, [horizonHover?.bands]);

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
            {registeredLow ? (
                <div style={{fontSize: '0.75rem', lineHeight: 1.4, marginBottom: '0.5em', color: RIDGE_MARKER}}>
                    {t('antenna_warning', {beacon: Math.round(beaconAltitude!), ground: Math.round(ground!)})}
                </div>
            ) : null}
            {profile ? (
                <GroundProfileChart profile={profile} bands={horizonHover?.bands} lowestAngle={lowestAngle} receivedKm={horizonHover?.distanceKm ?? null} stationAgl={stationAgl} t={t} />
            ) : (
                <GroundHorizonChart chart={chart} onHover={onHover} t={t} />
            )}
            {ground != null ? (
                <div style={{fontSize: '0.75rem', lineHeight: 1.4, marginBottom: '0.75em'}}>{t('antenna_info', {agl: Math.round(stationAgl), ground: Math.round(ground)})}</div>
            ) : null}
        </>
    );
}
