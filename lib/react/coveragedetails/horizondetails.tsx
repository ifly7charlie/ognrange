import {memo, useMemo, useState, useCallback, useEffect, useRef} from 'react';
import useSWR from 'swr';
import {useTranslation} from 'next-i18next/pages';
import {LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ReferenceLine, ResponsiveContainer} from 'recharts';

import {NEXT_PUBLIC_DATA_URL} from '../../common/config';
import graphcolours from '../graphcolours';

import {arrowFetcher} from './arrowfetcher';
import {chartsFromTable, horizonFileFor, heightAtDistance, BP_KEYS, BIN_DEG, COMPASS, X_TICKS, X_TICK_LABELS, HorizonPoint, HorizonHover} from './horizondata';

// One line per envelope-breakpoint index: bp0 is the bin's lowest proven
// angle (bold), later indices are the rising outer steps. The envelope is
// monotone, so higher indices always plot at or above lower ones - lines can
// occlude but never cross. Colours are shared with the map bearing segments
// and the profile staircase
const SERIES = BP_KEYS.map((key, j) => ({key, index: j, colour: graphcolours[j], width: j === 0 ? 2 : 1}));

// Stable no-op tooltip content - keeps the recharts hover cursor and active
// dots without drawing a box over the plot; the data goes to HorizonReadout
const noTooltipContent = () => null;

// Combined legend and hover readout below the chart - clicking a series row
// toggles it. Unlike the old fixed distance bands, the rows describe the
// hovered bearing's ACTUAL envelope steps: "angle out to distance". All rows
// are always rendered so hovering doesn't reflow the panel
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
                const bp = point?.breakpoints[s.index];
                return (
                    <div key={s.key} style={{color: s.colour}}>
                        <span onClick={() => toggleSeries(s.key)} style={{cursor: 'pointer', ...(hidden[s.key] ? {color: '#bbbbbb', textDecoration: 'line-through'} : {})}}>
                            {t('step', {n: s.index + 1})}
                        </span>
                        :{' '}
                        {bp ? (
                            <>
                                {bp.angle.toFixed(2)}&deg; {t('step_reach', {distance: Math.round(bp.km), height: Math.round(heightAtDistance(bp.angle, bp.km) / 10) * 10})}
                            </>
                        ) : (
                            '—'
                        )}
                    </div>
                );
            })}
            <div style={{minHeight: '1.4em'}}>{point?.count != null ? <div>{t('tooltip_count', {count: point.count})}</div> : null}</div>
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
            onHover(
                point
                    ? {
                          source: 'receive',
                          bearing: point.bearing,
                          distanceKm: point.breakpoints.length ? point.breakpoints[point.breakpoints.length - 1].km : null,
                          breakpoints: point.breakpoints,
                          frequency
                      }
                    : null
            );
        },
        [data, onHover, frequency]
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
            const key = h ? `${h.bearing}|${h.frequency}|${h.breakpoints?.map((b) => `${b.km}:${b.angle}`).join(',')}` : '';
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
