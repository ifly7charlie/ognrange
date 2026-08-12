import {memo, useMemo, useState, useCallback, useEffect, useRef} from 'react';
import useSWR from 'swr';
import {useTranslation} from 'next-i18next';
import {LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ReferenceLine, ResponsiveContainer} from 'recharts';

import {NEXT_PUBLIC_DATA_URL} from '../../common/config';
import graphcolours from '../graphcolours';

import {arrowFetcher} from './arrowfetcher';
import {chartsFromTable, horizonFileFor, heightAtDistance, BAND_RANGES_KM, BAND_KEYS, BIN_DEG, COMPASS, X_TICKS, X_TICK_LABELS, HorizonPoint, HorizonHover} from './horizondata';

const SERIES: {key: string; label: string; colour: string; width: number}[] = [
    {key: 'lowestAngle', label: 'any_distance', colour: '#333333', width: 2},
    {key: 'angle10km', label: 'band_10', colour: graphcolours[0], width: 1},
    {key: 'angle20km', label: 'band_20', colour: graphcolours[1], width: 1},
    {key: 'angle30km', label: 'band_30', colour: graphcolours[2], width: 1},
    {key: 'angle50km', label: 'band_50', colour: graphcolours[3], width: 1},
    {key: 'angle90km', label: 'band_90', colour: graphcolours[4], width: 1}
];

// Stable no-op tooltip content - keeps the recharts hover cursor and active
// dots without drawing a box over the plot; the data goes to HorizonReadout
const noTooltipContent = () => null;

// Combined legend and hover readout below the chart - clicking a series name
// toggles it, values fill in for the hovered bearing bin. All rows are always
// rendered so hovering doesn't reflow the panel
const HorizonReadout = ({point, hidden, toggleSeries, t}: {point: HorizonPoint | null; hidden: Record<string, boolean>; toggleSeries: (key: string) => void; t: (key: string, opts?: any) => string}) => {
    const compass = point ? COMPASS[Math.round(point.bearing / 22.5) % 16] : null;
    return (
        <div style={{fontSize: '0.75rem', lineHeight: 1.4, marginBottom: '0.75em'}}>
            <div>
                {point ? (
                    <b>
                        {point.bearing.toFixed(1)}&deg; ({compass})
                    </b>
                ) : (
                    <span style={{color: '#888888'}}>{t('readout_hint')}</span>
                )}
            </div>
            {SERIES.map((s) => {
                const value = point?.[s.key as keyof HorizonPoint] as number | null | undefined;
                // For band series, show the height window the angle sweeps
                // across the band's distance range (relative to station ground)
                const range = BAND_RANGES_KM[s.key as keyof typeof BAND_RANGES_KM];
                const heights = value != null && range ? ([heightAtDistance(value, range[0]), heightAtDistance(value, range[1])] as const) : null;
                return (
                    <div key={s.key} style={{color: s.colour}}>
                        <span onClick={() => toggleSeries(s.key)} style={{cursor: 'pointer', ...(hidden[s.key] ? {color: '#bbbbbb', textDecoration: 'line-through'} : {})}}>
                            {t(s.label)}
                        </span>
                        : {value != null ? <>{value.toFixed(2)}&deg;</> : '—'}
                        {heights ? <> {t('tooltip_heights', {low: Math.round(heights[0] / 10) * 10, high: Math.round(heights[1] / 10) * 10})}</> : null}
                    </div>
                );
            })}
            <div style={{minHeight: '4.2em'}}>
                {point?.lowestAgl != null && point.lowestDistance != null ? <div>{t('tooltip_lowest', {agl: point.lowestAgl, distance: point.lowestDistance})}</div> : null}
                {point?.maxDistance != null ? <div>{t('tooltip_max', {distance: point.maxDistance})}</div> : null}
                {point?.count != null ? <div>{t('tooltip_count', {count: point.count})}</div> : null}
            </div>
        </div>
    );
};

function HorizonChart({
    frequency,
    data,
    hidden,
    toggleSeries,
    onHover,
    groundBearing,
    t
}: {
    frequency: number;
    data: HorizonPoint[];
    hidden: Record<string, boolean>;
    toggleSeries: (key: string) => void;
    onHover: (h: HorizonHover) => void;
    groundBearing: number | null;
    t: (key: string, opts?: any) => string;
}) {
    const [hoverPoint, setHoverPoint] = useState<HorizonPoint | null>(null);

    // Resolve the hovered bin from the x-axis label (signed offset from north) -
    // data always holds all 720 bins in x order, and reusing the stable bin
    // objects means repeat events on the same bin don't re-render anything.
    // Feeds both the readout below the chart and the bearing line on the map
    const chartMouseMove = useCallback(
        (state: any) => {
            const x = Number(state?.activeLabel);
            const point = (state?.isTooltipActive && Number.isFinite(x) ? data[Math.round((x + 180) / BIN_DEG)] : null) ?? null;
            setHoverPoint(point);
            onHover(point ? {source: 'receive', bearing: point.bearing, distanceKm: point.maxDistance ?? null, bands: BAND_KEYS.map((k) => point[k])} : null);
        },
        [data, onHover]
    );
    const chartMouseLeave = useCallback(() => {
        setHoverPoint(null);
        onHover(null);
    }, [onHover]);

    // While the ground chart below is hovered, track its bearing here too:
    // same 0.5° bins, so the shared bearing maps straight to a bin index.
    // The mouse can't be over both charts, so local hover always wins
    const groundPoint = groundBearing != null ? (data[Math.round(((groundBearing + 180) % 360) / BIN_DEG) % data.length] ?? null) : null;
    const point = hoverPoint ?? groundPoint;

    return (
        <>
            <b style={{fontSize: 'small'}}>{t('frequency', {mhz: frequency})}</b>
            <ResponsiveContainer width="100%" height={190}>
                <LineChart data={data} margin={{top: 5, right: 5, left: -10, bottom: 5}} onMouseMove={chartMouseMove} onMouseLeave={chartMouseLeave}>
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
                    {/* Stand-in for the hover cursor when the bearing comes from the ground chart */}
                    {!hoverPoint && groundPoint ? <ReferenceLine x={groundPoint.x} stroke="#888888" /> : null}
                    <Tooltip content={noTooltipContent} cursor={{stroke: '#888888'}} />
                    {SERIES.map((s) => (
                        <Line
                            key={s.key}
                            name={t(s.label)}
                            dataKey={s.key}
                            stroke={s.colour}
                            strokeWidth={s.width}
                            dot={false}
                            connectNulls={false}
                            isAnimationActive={false}
                            hide={!!hidden[s.key]}
                        />
                    ))}
                </LineChart>
            </ResponsiveContainer>
            <HorizonReadout point={point} hidden={hidden} toggleSeries={toggleSeries} t={t} />
        </>
    );
}

// Receive-horizon chart(s) for the station details panel - one chart per RF
// frequency group present in the station's horizon.arrow file.
// Memoized: the parent re-renders on every hovered horizon bin (horizonHover
// feeds the ground chart below), and these LineCharts are the heaviest thing
// in the panel; while a receive chart is hovered all props are referentially
// stable (groundHoverBearing only changes while the ground chart is hovered,
// when these charts do need to redraw the synced cursor)
export const HorizonDetails = memo(function HorizonDetails({
    station,
    period,
    env,
    setHorizonHover,
    groundHoverBearing
}: {
    station: string;
    period?: string;
    env?: {NEXT_PUBLIC_DATA_URL?: string};
    setHorizonHover?: (h: HorizonHover) => void;
    groundHoverBearing?: number | null;
}) {
    const {t} = useTranslation('common', {keyPrefix: 'details.horizon'});
    const DATA_URL = env?.NEXT_PUBLIC_DATA_URL || NEXT_PUBLIC_DATA_URL;

    // Mouse-move fires per pixel but bins are 0.5°, so only push changes upward
    const lastHover = useRef<string>('');
    const onHover = useCallback(
        (h: HorizonHover) => {
            const key = h ? `${h.bearing}|${h.distanceKm}|${h.bands}` : '';
            if (key === lastHover.current) {
                return;
            }
            lastHover.current = key;
            setHorizonHover?.(h);
        },
        [setHorizonHover]
    );

    // The ground chart writes to the same shared hover state, so once it has
    // hovered, our last-pushed key no longer describes that state - without
    // this, re-hovering the same receive bin as before would be deduped away
    // and the profile swap / map line wouldn't come back
    useEffect(() => {
        if (groundHoverBearing != null) {
            lastHover.current = '';
        }
    }, [groundHoverBearing]);

    // Don't leave a stale bearing line when the station changes or the panel closes
    useEffect(() => {
        return () => {
            lastHover.current = '';
            setHorizonHover?.(null);
        };
    }, [station, setHorizonHover]);

    const url = useMemo(() => {
        if (!station) {
            return null;
        }
        return `${DATA_URL}${station}/${station}.${horizonFileFor(period)}.horizon.arrow`;
    }, [DATA_URL, station, period]);

    const {data: table} = useSWR(url, arrowFetcher, {revalidateOnFocus: false});

    const charts = useMemo(() => (table ? chartsFromTable(table) : null), [table]);

    const [hidden, setHidden] = useState<Record<string, boolean>>({});
    const toggleSeries = useCallback((key: string) => {
        setHidden((h) => ({...h, [key]: !h[key]}));
    }, []);

    if (!charts?.length) {
        return null;
    }

    return (
        <>
            <b>{t('title')}</b>
            <br />
            {charts.map(({frequency, data}) => (
                <HorizonChart key={frequency} frequency={frequency} data={data} hidden={hidden} toggleSeries={toggleSeries} onHover={onHover} groundBearing={groundHoverBearing ?? null} t={t} />
            ))}
        </>
    );
});
