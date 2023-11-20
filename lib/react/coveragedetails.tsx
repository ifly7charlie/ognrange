import {useMemo, useState, useCallback} from 'react';
import useSWR from 'swr';

import {useStationMeta} from './stationmeta';
import type {PickableDetails} from './pickabledetails';

import {cellToLatLng, greatCircleDistance, getResolution, getHexagonAreaAvg, UNITS} from 'h3-js';

import {IoLockOpenOutline} from 'react-icons/io5';

import {findIndex as _findIndex, reduce as _reduce, debounce as _debounce, map as _map} from 'lodash';

import VisibilitySensor from 'react-visibility-sensor';

import {StationList} from './stationlist';

import {GapDetails} from './coveragedetails/gapdetails';
import {OtherStationsDetails} from './coveragedetails/otherstationdetails';
import {CountDetails} from './coveragedetails/countdetails';
import {SignalDetails} from './coveragedetails/signaldetails';
import {LowestPointDetails} from './coveragedetails/lowestpointdetails';
import {AvailableFiles} from './coveragedetails/availablefiles';

import {NEXT_PUBLIC_DATA_URL} from '../common/config';

const fetcher = (url: string) => fetch(url).then((res) => res.json());

export function CoverageDetailsToolTip({details, station}) {
    //
    const stationMeta = useStationMeta();
    const sd = useMemo(() => {
        const index = _findIndex<string>(stationMeta?.name, station ?? 'global');
        return index != -1 ? [stationMeta.lng[index], stationMeta.lat[index]] : null;
    }, [station, stationMeta != undefined]);

    if (details.type === 'station') {
        return (
            <div>
                <b>{details.name}</b>
                <br />
                <>
                    {details.status ? (
                        <div
                            style={{
                                width: '350px',
                                overflowWrap: 'anywhere',
                                fontSize: 'small'
                            }}
                        >
                            {details.status}
                        </div>
                    ) : null}
                </>
                {details?.length ? (
                    <>
                        <hr />
                        {details.length} coverage cells
                        <br />
                        {Math.round(details.length * getHexagonAreaAvg(getResolution(details.h[0]), UNITS.km2))} sq km
                        <br />
                        <hr />
                    </>
                ) : null}
            </div>
        );
    } else if (details.type === 'hexagon') {
        return (
            <div>
                {sd ? (
                    <>
                        {greatCircleDistance(cellToLatLng(details.h), sd, 'km').toFixed(0)}km to <b>{station}</b>
                        <hr />
                    </>
                ) : null}
                <b>Lowest Point</b>
                <br />
                {(details.d / 4).toFixed(1)} dB @ {details.b} m ({details.g} m agl)
                <hr />
                <b>Signal Strength</b>
                <br />
                Average: {(details.a / 4).toFixed(1)} dB, Max: {(details.e / 4).toFixed(1)} dB
                <hr />
                Avg Gap between packets: {details.p >> 2}s{' '}
                {(details.q ?? true) !== true && details.stationCount > 1 ? (
                    <>
                        (expected: {details.q >> 2}s)
                        <br />
                    </>
                ) : (
                    <br />
                )}
                Avg CRC errors: {details.f / 10}
                <br />
                <hr />
                Number of packets: {details.c}
            </div>
        );
    }
    return <div> </div>;
}

//
// Used to generate the tooltip or the information to display in the details panel
export function CoverageDetails({
    details,
    locked,
    setSelectedDetails,
    station,
    setStation,
    file,
    setFile,
    env
}: //
{
    details: PickableDetails;
    locked: boolean;
    setSelectedDetails: (sd?: PickableDetails) => void;
    station: string;
    setStation: (s: string) => void;
    file: string;
    setFile: (s: string) => void;
    env: any;
}) {
    // Tidy up code later by simplifying typescript types
    const h3 = details.type === 'hexagon' ? details.h3 : '';
    const key = station + (details.type === 'hexagon' ? details.h3 + (locked ? 'L' : '') : details.type);
    const isLocked = details.type === 'hexagon' && locked;

    //
    const [doFetch, setDoFetch] = useState(key);
    const [extraVisible, setExtraVisible] = useState(false);

    const updateExtraVisibility = useCallback((visible: boolean) => {
        if (!extraVisible && visible) {
            setExtraVisible(true);
        }
    }, []);

    const delayedUpdateFrom = useCallback(
        _debounce(
            (x) => {
                setDoFetch(x);
            },
            isLocked ? 50 : 500
        ),
        [isLocked]
    );

    const {data: byDay} = useSWR(
        key == doFetch && h3 //
            ? `/api/station/${station || 'global'}/h3details/${h3}?file=${file}&lockedH3=${isLocked ? 1 : 0}`
            : null,
        fetcher
    );

    // Find the station not ideal as linear search so memoize it
    const stationMeta = useStationMeta();
    const sd = useMemo(() => {
        const index = stationMeta && station ? _findIndex<string>(stationMeta?.name, (a) => a === station) : -1;
        return index != -1 ? [stationMeta.lat[index], stationMeta.lng[index]] : null;
    }, [station, stationMeta.length]);

    const {data: stationDataDate, error} = useSWR(
        !h3 && station //
            ? `${env.NEXT_PUBLIC_DATA_URL ?? NEXT_PUBLIC_DATA_URL}${station}/${station}.${file}.json`
            : null,
        fetcher
    );

    //
    const {data: stationDataLatest} = useSWR(
        !h3 && station && (error || !stationDataDate?.lastOutputEpoch) //
            ? `${env.NEXT_PUBLIC_DATA_URL ?? NEXT_PUBLIC_DATA_URL}${station}/${station}.json`
            : null,
        fetcher
    );

    // Find the one that actually has some content
    const stationData = stationDataDate && stationDataDate.lastOutputEpoch ? stationDataDate : stationDataLatest;

    const clearSelectedH3 = useCallback(() => setSelectedDetails({type: 'none'}), [false]);

    delayedUpdateFrom(key);

    if (details.type === 'hexagon') {
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
                <b>Details at {locked ? 'specific point' : 'mouse point'}</b>
                {locked ? (
                    <button style={{float: 'right', padding: '10px'}} onClick={clearSelectedH3}>
                        <IoLockOpenOutline style={{paddingTop: '2px'}} />
                        &nbsp;<span> Unlock</span>
                    </button>
                ) : null}
                {sd ? (
                    <>
                        <br />
                        Distance to {station}: {greatCircleDistance(cellToLatLng(details.h), sd, 'km').toFixed(0)}km
                    </>
                ) : null}
                <br style={{clear: 'both'}} />
                <hr />
                <LowestPointDetails d={details.d} b={details.b} g={details.g} byDay={byDay} />
                <SignalDetails a={details.a} e={details.e} byDay={byDay} />
                <GapDetails p={details.p} q={details.q} stationCount={details.t} byDay={byDay} />
                Avg CRC errors: {details.f / 10}
                <br />
                <hr />
                <CountDetails c={details.c} byDay={byDay} />
                <StationList encodedList={details.s} selectedH3={details.h} setStation={setStation} />
                <br />
                {locked && byDay ? ( //
                    <VisibilitySensor onChange={updateExtraVisibility}>
                        <>
                            <div style={{height: '10px'}}></div>
                            {extraVisible ? ( //
                                <OtherStationsDetails h3={details.h3} file={file} station={station} locked={locked} />
                            ) : (
                                <span>Loading...</span>
                            )}
                        </>
                    </VisibilitySensor>
                ) : null}
            </div>
        );
    }

    if (stationData) {
        return (
            <>
                <b>{station}</b>
                <br />
                <br />
                <AvailableFiles station={station} setFile={setFile} displayType="day" />
                <AvailableFiles station={station} setFile={setFile} displayType="month" />
                <AvailableFiles station={station} setFile={setFile} displayType="year" />

                {stationData?.stats ? (
                    <>
                        <b>Statistics</b> at {new Date(stationData.outputEpoch * 1000).toISOString().replace('.000', '')}
                        <table>
                            <tbody>
                                {Object.keys(stationData.stats).map((key) => (
                                    <tr key={key}>
                                        <td>{key}</td>
                                        <td>{stationData.stats[key]}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </>
                ) : null}
                {stationData?.notice ? (
                    <>
                        <br />
                        <b>Notice</b>
                        <br />
                        {stationData.notice}
                    </>
                ) : null}
                <>
                    <br />
                    <b>Times</b>
                    <table>
                        <tbody>
                            {stationData?.lastLocation ? (
                                <tr key="location">
                                    <td>Location</td>
                                    <td>{new Date((stationData.lastLocation ?? 0) * 1000).toISOString().replace('.000', '')}</td>
                                </tr>
                            ) : null}
                            {stationData?.lastPacket ? (
                                <tr key="packet">
                                    <td>Packet</td>
                                    <td>{new Date((stationData.lastPacket ?? 0) * 1000).toISOString().replace('.000', '')}</td>
                                </tr>
                            ) : null}
                            {stationData?.outputDate ? (
                                <tr key="output">
                                    <td>Output File</td>
                                    <td>{stationData.outputDate?.replace(/\.[0-9]*Z/, 'Z')}</td>
                                </tr>
                            ) : null}
                        </tbody>
                    </table>
                </>
                {stationData?.status ? (
                    <>
                        <br />
                        <b>Last Status Message</b>
                        <br />
                        <div
                            style={{
                                width: '350px',
                                overflowWrap: 'anywhere',
                                fontSize: 'small'
                            }}
                        >
                            {stationData.status}
                        </div>
                    </>
                ) : null}

                <p style={{height: '5rem'}} />
            </>
        );
    }

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
