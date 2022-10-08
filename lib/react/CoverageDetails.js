import {useRouter} from 'next/router';
import {useMemo, useState, useCallback} from 'react';
import useSWR from 'swr';

import Link from 'next/link';

import {stationMeta} from './deckgl';
import {h3ToGeo, pointDist} from 'h3-js';

import _find from 'lodash.find';
import _reduce from 'lodash.reduce';
import _debounce from 'lodash.debounce';

import {LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer} from 'recharts';

import {prefixWithZeros} from '../bin/prefixwithzeros.js';

//
// Used to generate the tooltip or the information to display in the details panel
export function CoverageDetails({details, station, setHighlightStations, highlightStations, file}) {
    let stationList = undefined;
    let stationCount = undefined;
    const router = useRouter();
    const [doFetch, setDoFetch] = useState(station + details?.h);

    const delayedUpdateFrom = useCallback(
        _debounce((x) => {
            setDoFetch(x);
        }, 750),
        [true]
    );

    const fetcher = (...args) => fetch(...args).then((res) => res.json());
    const {data: byDay, error} = useSWR(
        station + details?.h == doFetch && details?.h //
            ? `/api/station/${station || 'global'}/h3details/${prefixWithZeros(8, details.h[1].toString(16))}${prefixWithZeros(8, details.h[0].toString(16))}?file=${file}`
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
        stationCount = parts.length;
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
                <b>Details at {details.locked ? 'specific point' : 'mouse point'}</b>
                <br />
                <hr />
                <LowestPointDetails d={details.d} b={details.b} g={details.g} byDay={byDay} />
                <SignalDetails a={details.a} e={details.e} byDay={byDay} />
                <GapDetails p={details.p} q={details.q} stationCount byDay={byDay} />
                Avg CRC errors: {details.f / 10}
                <br />
                <hr />
                <CountDetails c={details.c} byDay={byDay} />
                <br />
                {stationList ? (
                    <>
                        <hr />
                        <b>Stations ({stationCount})</b>
                        <br />
                        {stationList}
                    </>
                ) : null}
                {sd?.lat && sd?.lng ? (
                    <>
                        <hr />
                        Distance to {station} {pointDist(h3ToGeo(details.h), [sd.lat, sd.lng], 'km').toFixed(0)}km
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
                            <Line name="Average Gap" isAnimationActive={false} type="monotone" dataKey="avgGap" stroke="#8884d8" dot={{r: 1}} />
                            {'expectedGap' in props.byDay[0] ? <Line type="monotone" dataKey="expectedGap" stroke="#82ca9d" dot={{r: 1}} /> : null}
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
                            <Line name="Average" isAnimationActive={false} type="monotone" dataKey="avgSig" stroke="#8884d8" dot={{r: 1}} />
                            <Line name="Maximum" isAnimationActive={false} type="monotone" dataKey="maxSig" stroke="#82ca9d" dot={{r: 1}} />
                            <Line name="At Minimum Altitude" isAnimationActive={false} type="monotone" dataKey="minAltSig" stroke="#ca8282" dot={{r: 1}} />
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
                            <Line name="Strength" isAnimationActive={false} type="monotone" dataKey="minAltSig" stroke="#82ca9d" dot={{r: 1}} />
                            <Line name="Height Above Ground" yAxisId={1} isAnimationActive={false} type="monotone" dataKey="minAgl" stroke="#8884d8" dot={{r: 1}} />
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
                            <Line name="Count" isAnimationActive={false} type="monotone" dataKey="count" stroke="#82ca9d" dot={{r: 1}} />
                        </LineChart>
                    </ResponsiveContainer>
                </>
            ) : null}
            <hr />
        </>
    );
}
