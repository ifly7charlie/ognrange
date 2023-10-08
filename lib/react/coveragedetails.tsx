import {useMemo, useState, useCallback} from 'react';
import useSWR from 'swr';

import {stationMeta} from './stationMeta';
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

const fetcher = (args0, args1) => fetch(args0, args1).then((res) => res.json());

export function CoverageDetailsToolTip({details, station}) {
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
    setDetails,
    station,
    setStation,
    file
}: //
{
    details: PickableDetails;
    setDetails: (sd?: PickableDetails) => void;
    station: string;
    setStation: (s: string) => void;
    file: string;
}) {
    // Tidy up code later by simplifying typescript types
    const h3 = details.type === 'hexagon' ? details.h3 : '';
    const key = station + (details.type === 'hexagon' ? details.h3 + (details.locked ? 'L' : '') : details.type);
    const isLocked = details.type === 'hexagon' && details.locked;

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
    const sd = useMemo(() => {
        const index = stationMeta ? _findIndex<string>(stationMeta?.name, station) : -1;
        return index != -1 ? [stationMeta.lng[index], stationMeta.lat[index]] : null;
    }, [station, stationMeta != undefined]);

    const clearSelectedH3 = useCallback(() => setDetails(), [false]);

    if (details.type === 'none' || !h3) {
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

    delayedUpdateFrom(key);

    // Either a station
    if (details.type === 'station') {
        return (
            <>
                <b>{details.name}</b>
                <br />
            </>
        );

        /*                {false && details.status && (
                    <div
                        style={{
                            width: '350px',
                            overflowWrap: 'anywhere',
                            fontSize: 'small'
                        }}
                    >
                        {details.status}
                    </div>
                )}*/
    }

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
                <b>Details at {details.locked ? 'specific point' : 'mouse point'}</b>
                {details.locked ? (
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
                {details.locked && byDay ? ( //
                    <VisibilitySensor onChange={updateExtraVisibility}>
                        <>
                            <div style={{height: '10px'}}></div>
                            {extraVisible ? ( //
                                <OtherStationsDetails h3={details.h3} file={file} station={station} locked={details.locked} />
                            ) : (
                                <span>Loading...</span>
                            )}
                        </>
                    </VisibilitySensor>
                ) : null}
            </div>
        );
    }

    return <div>there</div>;
}
