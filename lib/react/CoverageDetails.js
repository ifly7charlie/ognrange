import {useRouter} from 'next/router';
import {useMemo, useState, useCallback} from 'react';
import useSWR from 'swr';

import Link from 'next/link';

import {stationMeta} from './stationMeta';
import {h3ToGeo, pointDist} from 'h3-js';

import _find from 'lodash.find';
import _reduce from 'lodash.reduce';
import _debounce from 'lodash.debounce';
import _map from 'lodash.map';

import {LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, LabelList} from 'recharts';
import {BarChart, Bar} from 'recharts';

import VisibilitySensor from 'react-visibility-sensor';

import {prefixWithZeros} from '../bin/prefixwithzeros.js';

import graphcolours from './graphcolours.js';

const fetcher = (...args) => fetch(...args).then((res) => res.json());

//
// Used to generate the tooltip or the information to display in the details panel
export function CoverageDetails({details, station, setHighlightStations, highlightStations, file}) {
    let stationList = undefined;
    let stationCount = undefined;
    const router = useRouter();
    const [doFetch, setDoFetch] = useState(station + details?.h);
    const [extraVisible, setExtraVisible] = useState(false);

    const delayedUpdateFrom = useCallback(
        _debounce((x) => {
            setDoFetch(x);
        }, 750),
        [true]
    );

    const {data: byDay, error} = useSWR(
        station + details?.h == doFetch && details?.h //
            ? `/api/station/${station || 'global'}/h3details/${prefixWithZeros(8, details.h[1].toString(16))}${prefixWithZeros(8, details.h[0].toString(16))}?file=${file}&lockedH3=${details?.locked ? 1 : 0}`
            : null,
        fetcher
    );

    // Find the station not ideal as linear search so memoize it
    const sd = useMemo(() => {
        return _find(stationMeta, {station: station});
    }, [station, stationMeta != undefined]);

    if (!details?.station && !details?.h) {
        return (
            <>
                Hover over somewhere on the map to see details.
                <br />
                Click to lock the sidebar display to that location.
                <br />
                Click on a station marker to show coverage records only for that station.
                <br />
                You can resize the sidebar by dragging the edge - if you resize it to zero then you will see tooltips with the information
                <br />
            </>
        );
    }

    delayedUpdateFrom(station + details?.h);

    // See if we have a list of stations, they are base36 encocoded along with % of packets
    // ie: (stationid*10+(percentpackets%10)).toString(36)
    if (stationMeta && details.s) {
        const parts = details.s.split(',');

        stationList = _reduce(
            parts,
            (acc, x) => {
                const decoded = parseInt(x, 36);
                const sid = decoded >> 4;
                const percentage = (decoded & 0x0f) * 10;
                const meta = stationMeta[sid];
                const dist = meta?.lat ? pointDist(h3ToGeo(details.h), [meta.lat, meta.lng], 'km').toFixed(0) + ' km' : '';
                acc.push(
                    <tr key={sid}>
                        <td>
                            <Link
                                replace
                                href={{
                                    pathname: '/',
                                    query: {...router.query, station: meta?.station || ''}
                                }}
                            >
                                <a>{meta?.station || 'Unknown'}</a>
                            </Link>
                        </td>
                        <td>{dist}</td>
                        <td>{percentage > 10 ? percentage.toFixed(0) + '%' : ''}</td>
                    </tr>
                );
                return acc;
            },
            []
        );

        stationList = (
            <table className="stationList">
                <tbody>{stationList}</tbody>
            </table>
        );
        stationCount = details.t || parts.length;
    }

    // Either a station
    if (details.station) {
        return (
            <>
                <b>{details.station}</b>
                <br />
                {details.status && (
                    <div
                        style={{
                            width: '350px',
                            overflowWrap: 'anywhere',
                            fontSize: 'small'
                        }}
                    >
                        {details.status}
                    </div>
                )}
            </>
        );
    }

    if (details.h) {
        return (
            <div>
                {details?.length ? (
                    <>
                        <hr />
                        {details.length} coverage cells
                        <br />
                        {Math.round(details.length * 0.0737327598) * 10} sq km
                        <br />
                        <hr />
                    </>
                ) : null}
                <b>Details at {details.locked ? 'specific point' : 'mouse point'}</b>
                <br />
                {sd?.lat && sd?.lng ? (
                    <>
                        <br />
                        Distance to {station}: {pointDist(h3ToGeo(details.h), [sd.lat, sd.lng], 'km').toFixed(0)}km
                    </>
                ) : null}
                <hr />
                <LowestPointDetails d={details.d} b={details.b} g={details.g} byDay={byDay} />
                <SignalDetails a={details.a} e={details.e} byDay={byDay} />
                <GapDetails p={details.p} q={details.q} stationCount byDay={byDay} />
                Avg CRC errors: {details.f / 10}
                <br />
                <hr />
                <CountDetails c={details.c} byDay={byDay} />
                {details.locked && byDay ? ( //
                    <VisibilitySensor onChange={setExtraVisible}>
                        <>
                            <div height="300px">
                                <br />
                            </div>
                            {extraVisible ? ( //
                                <OtherStationsDetails h={details.h} file={file} station={station} locked={details.locked} />
                            ) : (
                                <span>Loading...</span>
                            )}
                        </>
                    </VisibilitySensor>
                ) : null}
                <br />
                {stationList ? (
                    <>
                        <b>Stations ({stationCount})</b>
                        <br />
                        {stationList}
                    </>
                ) : null}
            </div>
        );
    }

    return <div>there</div>;
}

function GapDetails(props) {
    return (
        <>
            <b>Time between received packets</b>
            <br />
            Avg Gap: {props.p >> 2}s{' '}
            {(props.q ?? true) !== true && props.stationCount > 1 ? (
                <>
                    (expected: {props.q >> 2}s)
                    <br />
                </>
            ) : (
                <br />
            )}
            {props.byDay && props.byDay.length > 0 ? (
                <>
                    <br />
                    <ResponsiveContainer width="100%" height={150}>
                        <LineChart width={480} height={180} data={props.byDay} margin={{top: 5, right: 30, left: 20, bottom: 5}} syncId="date">
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="date" style={{fontSize: '0.8rem'}} />
                            <YAxis unit="s" style={{fontSize: '0.8rem'}} />
                            <Tooltip />
                            <Legend />
                            <Line name="Average Gap" isAnimationActive={false} type="monotone" dataKey="avgGap" stroke={graphcolours[0]} dot={{r: 1}} />
                            {'expectedGap' in props.byDay[0] ? <Line type="monotone" isAnimationActive={false} dataKey="expectedGap" stroke={graphcolours[1]} dot={{r: 1}} /> : null}
                        </LineChart>
                    </ResponsiveContainer>
                </>
            ) : null}
            <hr />
        </>
    );
}

function SignalDetails(props) {
    return (
        <>
            <b>Signal Strength</b>
            <br />
            Average: {(props.a / 4).toFixed(1)} dB, Max: {(props.e / 4).toFixed(1)} dB
            <br />
            {props.byDay && props.byDay.length > 0 ? (
                <>
                    <br />
                    <ResponsiveContainer width="100%" height={150}>
                        <LineChart width={480} height={180} data={props.byDay} margin={{top: 5, right: 30, left: 20, bottom: 5}} syncId="date">
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="date" style={{fontSize: '0.8rem'}} />
                            <YAxis unit="dB" style={{fontSize: '0.8rem'}} />
                            <Tooltip />
                            <Legend />
                            <Line name="Average" isAnimationActive={false} type="monotone" dataKey="avgSig" stroke={graphcolours[0]} dot={{r: 1}} />
                            <Line name="Maximum" isAnimationActive={false} type="monotone" dataKey="maxSig" stroke={graphcolours[1]} dot={{r: 1}} />
                            <Line name="At Minimum Altitude" isAnimationActive={false} type="monotone" dataKey="minAltSig" stroke={graphcolours[2]} dot={{r: 1}} />
                        </LineChart>
                    </ResponsiveContainer>
                </>
            ) : null}
            <hr />
        </>
    );
}

function LowestPointDetails(props) {
    return (
        <>
            <b>Lowest Point</b>
            <br />
            {(props.d / 4).toFixed(1)} dB @ {props.b} m ({props.g} m agl)
            <br />
            {props.byDay && props.byDay.length > 0 ? (
                <>
                    <br />
                    <ResponsiveContainer width="100%" height={150}>
                        <LineChart width={480} height={180} data={props.byDay} margin={{top: 5, right: 30, left: 20, bottom: 5}} syncId="date">
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="date" style={{fontSize: '0.8rem'}} />
                            <YAxis unit="dB" yAxisId={0} orientation="left" style={{fontSize: '0.8rem'}} />
                            <YAxis unit=" m" yAxisId={1} orientation="right" style={{fontSize: '0.8rem'}} />
                            <Tooltip />
                            <Legend />
                            <Line name="Strength" isAnimationActive={false} type="monotone" dataKey="minAltSig" stroke={graphcolours[0]} dot={{r: 1}} />
                            <Line name="Height Above Ground" yAxisId={1} isAnimationActive={false} type="monotone" dataKey="minAgl" stroke={graphcolours[1]} dot={{r: 1}} />
                        </LineChart>
                    </ResponsiveContainer>
                </>
            ) : null}
            <hr />
        </>
    );
}

function CountDetails(props) {
    return (
        <>
            Number of packets: {props.c}
            <br />
            {props.byDay && props.byDay.length > 0 ? (
                <>
                    <br />
                    <ResponsiveContainer width="100%" height={150}>
                        <LineChart width={480} height={180} data={props.byDay} margin={{top: 5, right: 30, left: 20, bottom: 5}} syncId="date">
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="date" style={{fontSize: '0.8rem'}} />
                            <YAxis style={{fontSize: '0.8rem'}} />
                            <Tooltip />
                            <Legend />
                            <Line name="Count" isAnimationActive={false} type="monotone" dataKey="count" stroke={graphcolours[0]} dot={{r: 1}} />
                        </LineChart>
                    </ResponsiveContainer>
                </>
            ) : null}
        </>
    );
}

const doLabel = ({x, y, s, c}) => {
    return (
        <text x={x} y={y} fill="black" textAnchor="end" dominantBaseline="central">
            {s}: {c}
        </text>
    );
};
//
// Other detailed coverage for this cell
// COSTLY
function OtherStationsDetails(props) {
    const [doFetch, setDoFetch] = useState(null);

    //
    // Get the grouped value - we use a delay for this so we don't hit server unless they wait
    const delayedUpdateFrom = useCallback(
        _debounce((x) => {
            setDoFetch(x);
        }, 750),
        [true]
    );

    const isGlobal = (props.station || 'global') == 'global'; //

    // What URL to use - both return same but act differently
    const url = () => (isGlobal ? `/api/station/global/h3summary/${prefixWithZeros(8, props.h[1].toString(16))}${prefixWithZeros(8, props.h[0].toString(16))}?file=${props.file}&lockedH3=${props?.locked ? 1 : 0}` : `/api/station/${props.station}/h3others/${prefixWithZeros(8, props.h[1].toString(16))}${prefixWithZeros(8, props.h[0].toString(16))}?file=${props.file}&lockedH3=${props?.locked ? 1 : 0}`);

    // Actually load the data when it's time
    const {data: byDay, error} = useSWR(
        props.station + props?.h == doFetch && props?.h //
            ? url()
            : null,
        fetcher
    );

    function safeZero(x) {
        return isNaN(x) ? 0 : x || 0;
    }

    const percentages = useMemo(() => {
        return _map(byDay?.data || [], (v) => {
            const dayTotal = _reduce(
                v,
                (r, count) => {
                    return (r = r + (typeof count == 'number' ? count : 0));
                },
                0
            );
            const newObject = {date: v['date']};
            for (const key of Object.keys(v)) {
                if (key != 'date') {
                    if (isNaN(v[key]) || v[key] == null) {
                        //                       delete v[key];
                    } else {
                        newObject[key] = Math.round((safeZero(v[key]) * 1000) / dayTotal) / 10;
                    }
                }
            }
            return newObject;
        });
    }, [props?.h, props.station, !!byDay]);

    delayedUpdateFrom(props.station + props.h);

    return (
        <>
            {isGlobal ? <>Top 5 stations by percentage:</> : <>Top 5 other stations total packet count:</>}
            <br />
            {byDay?.data?.length > 0 ? (
                <>
                    <ResponsiveContainer width="100%" height={150} key="counttotal">
                        <PieChart width={480} height={180} margin={{top: 5, right: 30, left: 20, bottom: 5}}>
                            <Pie
                                data={_map(byDay.series, (v, i) => {
                                    return {...v, fill: graphcolours[i]};
                                })}
                                dataKey="c"
                                nameKey="s"
                                isAnimationActive={false}
                                outerRadius={50}
                                innerRadius={10}
                            />
                            <Legend />
                            <Tooltip />
                        </PieChart>
                    </ResponsiveContainer>
                    <br />
                    {!isGlobal ? (
                        <>
                            Top 5 other stations packet count by day:
                            <br />
                            <br />
                            <ResponsiveContainer width="100%" height={150} key="countovertime">
                                <LineChart width={480} height={180} data={byDay.data} margin={{top: 5, right: 30, left: 20, bottom: 5}} syncId="date">
                                    <CartesianGrid strokeDasharray="3 3" />
                                    <XAxis dataKey="date" style={{fontSize: '0.8rem'}} />
                                    <YAxis style={{fontSize: '0.8rem'}} />
                                    <Tooltip key="ttcot" />
                                    <Legend />
                                    {_map(byDay.series, (v, i) => (
                                        <Line key={v.s} isAnimationActive={false} type="monotone" dataKey={v.s} stroke={graphcolours[i]} dot={{r: 1}} />
                                    ))}
                                </LineChart>
                            </ResponsiveContainer>
                            <br />
                        </>
                    ) : null}
                    Top 5 other stations percentage by day:
                    <br />
                    <br />
                    <ResponsiveContainer width="100%" height={150} key="percentageovertime">
                        <BarChart width={480} height={180} data={percentages} margin={{top: 5, right: 30, left: 20, bottom: 5}} syncId="date">
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="date" style={{fontSize: '0.8rem'}} />
                            <YAxis unit="%" style={{fontSize: '0.8rem'}} allowDecimals={false} />
                            <Tooltip />
                            <Legend />
                            {_map(byDay.series, (v, i) => (
                                <Bar key={'ot' + v.s} isAnimationActive={false} type="monotone" stackId="a" dataKey={v.s} stroke={graphcolours[i]} fill={graphcolours[i]} dot={{r: 1}} />
                            ))}
                        </BarChart>
                    </ResponsiveContainer>
                </>
            ) : null}
            <hr />
        </>
    );
}
