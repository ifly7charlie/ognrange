import {useMemo, useState, useCallback} from 'react';
import useSWR from 'swr';
import {useTranslation} from 'next-i18next';
import {LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ReferenceLine, ResponsiveContainer} from 'recharts';
import {tableFromIPC, Table} from 'apache-arrow';

import {NEXT_PUBLIC_DATA_URL} from '../../common/config';
import graphcolours from '../graphcolours';

import {chartsFromTable, horizonFileFor, heightAtDistance, BAND_RANGES_KM, HorizonPoint} from './horizondata';

const SERIES: {key: string; label: string; colour: string; width: number}[] = [
    {key: 'lowestAngle', label: 'any_distance', colour: '#333333', width: 2},
    {key: 'angle5km', label: 'band_5', colour: graphcolours[0], width: 1},
    {key: 'angle10km', label: 'band_10', colour: graphcolours[1], width: 1},
    {key: 'angle20km', label: 'band_20', colour: graphcolours[2], width: 1},
    {key: 'angle30km', label: 'band_30', colour: graphcolours[3], width: 1},
    {key: 'angle50km', label: 'band_50', colour: graphcolours[4], width: 1},
    {key: 'angle90km', label: 'band_90', colour: graphcolours[5], width: 1}
];

const COMPASS = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE', 'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW'];

// North in the middle: x is the signed offset from north, S(-180) W(-90) N(0) E(90) S(180)
const X_TICKS = [-180, -90, 0, 90, 180];
const X_TICK_LABELS: Record<number, string> = {[-180]: 'S', [-90]: 'W', 0: 'N', 90: 'E', 180: 'S'};

const arrowFetcher = async (url: string): Promise<Table | null> => {
    const res = await fetch(url);
    if (!res.ok) {
        return null;
    }
    const buffer = await res.arrayBuffer();
    try {
        return tableFromIPC(new Uint8Array(buffer));
    } catch (e) {
        console.log(`horizon: unable to parse ${url}: ${e}`);
        return null;
    }
};

const HorizonTooltip = ({active, payload, t}: {active?: boolean; payload?: any[]; t: (key: string, opts?: any) => string}) => {
    if (!active || !payload?.length) {
        return null;
    }
    const point = payload[0].payload as HorizonPoint;
    const compass = COMPASS[Math.round(point.bearing / 22.5) % 16];
    return (
        <div style={{background: 'white', padding: '2px 10px 4px 10px', border: '1px solid grey', fontSize: '0.75rem'}}>
            <p className="label">
                <b>
                    {point.bearing.toFixed(1)}&deg; ({compass})
                </b>
            </p>
            {payload.map((entry) => {
                if (entry.value == null) {
                    return null;
                }
                // For band series, show the height window the angle sweeps
                // across the band's distance range (relative to station ground)
                const range = BAND_RANGES_KM[entry.dataKey as keyof typeof BAND_RANGES_KM];
                const heights = range ? ([heightAtDistance(entry.value, range[0]), heightAtDistance(entry.value, range[1])] as const) : null;
                return (
                    <div key={entry.dataKey} style={{color: entry.color}}>
                        {entry.name}: {entry.value.toFixed(2)}&deg;
                        {heights ? <> {t('tooltip_heights', {low: Math.round(heights[0] / 10) * 10, high: Math.round(heights[1] / 10) * 10})}</> : null}
                    </div>
                );
            })}
            {point.lowestAgl != null && point.lowestDistance != null ? <div>{t('tooltip_lowest', {agl: point.lowestAgl, distance: point.lowestDistance})}</div> : null}
            {point.maxDistance != null ? <div>{t('tooltip_max', {distance: point.maxDistance})}</div> : null}
            {point.count != null ? <div>{t('tooltip_count', {count: point.count})}</div> : null}
        </div>
    );
};

function HorizonChart({
    frequency,
    data,
    hidden,
    toggleSeries,
    t
}: {
    frequency: number;
    data: HorizonPoint[];
    hidden: Record<string, boolean>;
    toggleSeries: (e: any) => void;
    t: (key: string, opts?: any) => string;
}) {
    return (
        <>
            <b style={{fontSize: 'small'}}>{t('frequency', {mhz: frequency})}</b>
            <ResponsiveContainer width="100%" height={190}>
                <LineChart data={data} margin={{top: 5, right: 5, left: -10, bottom: 5}}>
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
                    <Tooltip content={<HorizonTooltip t={t} />} />
                    <Legend
                        onClick={toggleSeries}
                        wrapperStyle={{fontSize: '0.7rem', cursor: 'pointer'}}
                        formatter={(value: string, entry: any) => (
                            <span style={hidden[entry.dataKey] ? {color: '#bbbbbb', textDecoration: 'line-through'} : undefined}>{value}</span>
                        )}
                    />
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
        </>
    );
}

// Receive-horizon chart(s) for the station details panel - one chart per RF
// frequency group present in the station's horizon.arrow file
export function HorizonDetails({station, period, env}: {station: string; period?: string; env?: {NEXT_PUBLIC_DATA_URL?: string}}) {
    const {t} = useTranslation('common', {keyPrefix: 'details.horizon'});
    const DATA_URL = env?.NEXT_PUBLIC_DATA_URL || NEXT_PUBLIC_DATA_URL;

    const url = useMemo(() => {
        if (!station) {
            return null;
        }
        return `${DATA_URL}${station}/${station}.${horizonFileFor(period)}.horizon.arrow`;
    }, [DATA_URL, station, period]);

    const {data: table} = useSWR(url, arrowFetcher, {revalidateOnFocus: false});

    const charts = useMemo(() => (table ? chartsFromTable(table) : null), [table]);

    const [hidden, setHidden] = useState<Record<string, boolean>>({});
    const toggleSeries = useCallback((e: any) => {
        if (e?.dataKey) {
            setHidden((h) => ({...h, [e.dataKey]: !h[e.dataKey]}));
        }
    }, []);

    if (!charts?.length) {
        return null;
    }

    return (
        <>
            <b>{t('title')}</b>
            <br />
            {charts.map(({frequency, data}) => (
                <HorizonChart key={frequency} frequency={frequency} data={data} hidden={hidden} toggleSeries={toggleSeries} t={t} />
            ))}
        </>
    );
}
