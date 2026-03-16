import {useState, useCallback, useEffect} from 'react';
import useSWR from 'swr';

import {useTranslation, Trans} from 'next-i18next';

import {useStationMeta} from './stationmeta';
import type {PickableDetails} from './pickabledetails';
import {LayerBadges} from './layerbadges';

import {cellToLatLng, greatCircleDistance, getResolution, getHexagonAreaAvg, UNITS} from 'h3-js';

import {IoLockOpenOutline} from 'react-icons/io5';

import {debounce as _debounce} from 'lodash';

import VisibilitySensor from 'react-visibility-sensor';

import {StationList} from './stationlist';
import {LayerTabs} from './layertabs';

import {GapDetails} from './coveragedetails/gapdetails';
import {OtherStationsDetails} from './coveragedetails/otherstationdetails';
import {CountDetails} from './coveragedetails/countdetails';
import {SignalDetails} from './coveragedetails/signaldetails';
import {LowestPointDetails} from './coveragedetails/lowestpointdetails';
import {AvailableFiles} from './coveragedetails/availablefiles';
import {ActivityDetails} from './coveragedetails/activitydetails';
import {UptimeBar} from './coveragedetails/uptimebar';
import {BeaconActivity} from './coveragedetails/beaconactivity';
import {StationPosition} from './coveragedetails/stationposition';
import {ProtocolStatsDashboard} from './coveragedetails/protocolstats';

import {formatEpoch} from './formatdate';

const fetcher = (url: string) => fetch(url).then((res) => res.json());

export function CoverageDetailsToolTip({details, station}) {
    //
    const {t} = useTranslation();
    const stationMeta = useStationMeta(station ?? '');

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
                {stationMeta ? (
                    <>
                        {greatCircleDistance(cellToLatLng(details.h), [stationMeta.lng, stationMeta.lat], 'km').toFixed(0)}km to <b>{station}</b>
                        <hr />
                    </>
                ) : null}
                <b>{t('lowest.title')}</b>
                <br />
                {t('lowest.summary', {strength: (details.d / 4).toFixed(1), altitude: details.b, agl: details.g})}
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
                {t('crc.summary', {crc: details.f / 10})}
                <br />
                <hr />
                {t('packets.summary', {count: details.c})}
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
    layers,
    setLayers,
    dateRange,
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
    layers?: string[];
    setLayers?: (l: string[]) => void;
    dateRange?: {start: string; end: string};
    env: any;
}) {
    // Tidy up code later by simplifying typescript types
    const h3 = details.type === 'hexagon' ? details.h3 : '';
    const key = station + (details.type === 'hexagon' ? details.h3 + (locked ? 'L' : '') : details.type);
    const isLocked = details.type === 'hexagon' && locked;
    const layersParam = layers?.join(',') || 'combined';
    const displayType = (file?.match(/^(day|month|year|yearnz)/)?.[1] ?? 'year') as 'day' | 'month' | 'year' | 'yearnz';

    //
    const [doFetch, setDoFetch] = useState(key);
    const [extraVisible, setExtraVisible] = useState(false);
    const [selectedLayer, setSelectedLayer] = useState('all');

    // Reset layer selection when the hexagon changes
    useEffect(() => {
        setSelectedLayer('all');
    }, [key]);
    const {t} = useTranslation('common', {keyPrefix: 'details'});
    const {t: tLayer} = useTranslation('common', {keyPrefix: 'layers'});

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
            ? `/api/station/${station || 'global'}/h3details/${h3}?dateStart=${dateRange?.start || file}&dateEnd=${dateRange?.end || file}&lockedH3=${isLocked ? 1 : 0}&layers=${layersParam}`
            : null,
        fetcher
    );

    // Tabs are driven by the requested layers, not what the API happened to return
    const showTabs = (layers?.length ?? 0) > 1;
    const tabKeys = showTabs ? ['all', ...(layers ?? [])] : [];
    const activeByDay = showTabs ? byDay?.layers?.[selectedLayer] : byDay?.layers?.[Object.keys(byDay?.layers ?? {})[0]] ?? byDay;

    const stationMeta = useStationMeta(station ?? '');

    const isRange = dateRange && dateRange.start !== dateRange.end;

    // Always use the API for station details
    const stationDetailsUrl = !h3 && station
        ? isRange
            ? `/api/station/${station}/details?dateStart=${dateRange.start}&dateEnd=${dateRange.end}`
            : `/api/station/${station}/details?file=${file}`
        : null;

    const {data: stationDataRaw} = useSWR(stationDetailsUrl, fetcher);

    const stationData = stationDataRaw && Object.keys(stationDataRaw).length > 0 ? stationDataRaw : null;

    const clearSelectedH3 = useCallback(() => setSelectedDetails({type: 'none'}), [false]);

    delayedUpdateFrom(key);

    if (details.type === 'hexagon') {
        return (
            <div>
                {details?.length ? (
                    <>
                        <hr />
                        {t('cells.number', {count: details.length})}
                        <br />
                        {t('cells.area', {area: Math.round(details.length * 0.0737327598) * 10})}
                        <br />
                        <hr />
                    </>
                ) : null}
                <b>{t(locked ? 'title_specific' : 'title_mouse')}</b>
                {locked ? (
                    <button style={{float: 'right', padding: '10px'}} onClick={clearSelectedH3}>
                        <IoLockOpenOutline style={{paddingTop: '2px'}} />
                        &nbsp;<span>{t('unlock')}</span>
                    </button>
                ) : null}
                {stationMeta ? (
                    <>
                        <br />
                        {t('distance', {station, km: greatCircleDistance(cellToLatLng(details.h), [stationMeta.lat, stationMeta.lng], 'km').toFixed(0)})}
                    </>
                ) : null}
                <br style={{clear: 'both'}} />
                <hr />
                {showTabs ? (
                    <LayerTabs
                        tabs={tabKeys}
                        selectedTab={selectedLayer}
                        setSelectedTab={setSelectedLayer}
                        hasData={(tab) => !!byDay?.layers?.[tab]}
                    />
                ) : null}
                <AvailableFiles station={station || 'global'} setFile={setFile} displayType={displayType} layer={selectedLayer} />
                {showTabs && byDay && !activeByDay?.length ? (
                    <p style={{color: 'gray', fontStyle: 'italic'}}>{t('no_layer_data', {layer: tLayer(selectedLayer, selectedLayer)})}</p>
                ) : (
                    <>
                        <LowestPointDetails d={details.d} b={details.b} g={details.g} byDay={activeByDay} />
                        <SignalDetails a={details.a} e={details.e} byDay={activeByDay} />
                        <GapDetails p={details.p} q={details.q} stationCount={details.t} byDay={activeByDay} />
                        {t('crc.summary', {crc: details.f / 10})}
                        <br />
                        <hr />
                        <CountDetails c={details.c} byDay={activeByDay} />
                    </>
                )}
                <StationList encodedList={details.s} selectedH3={details.h} setStation={setStation} />
                <br />
                {locked && byDay ? ( //
                    <VisibilitySensor onChange={updateExtraVisibility}>
                        <>
                            <div style={{height: '10px'}}></div>
                            {extraVisible ? ( //
                                <OtherStationsDetails h3={details.h3} file={file} station={station} locked={locked} layers={layers} selectedLayer={selectedLayer} dateRange={dateRange} />
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
                <LayerBadges layerMask={stationData.layerMask} />
                <br />
                <AvailableFiles station={station} setFile={setFile} displayType="day" />
                <AvailableFiles station={station} setFile={setFile} displayType="month" />
                <AvailableFiles station={station} setFile={setFile} displayType="year" />

                {stationData?.purgedAt ? (
                    <>
                        <hr />
                        <b>{'\u26A0\uFE0F '}{t('purge.title')}</b>
                        <br />
                        {t(`purge.reason_${stationData.purgeReason || 'unknown'}`)}
                        <br />
                        {t('purge.when', {when: formatEpoch(stationData.purgedAt)})}
                        <hr />
                    </>
                ) : null}
                <ActivityDetails activity={stationData?.activity} />
                <UptimeBar uptime={stationData?.uptime} />
                <BeaconActivity data={stationData?.beaconActivity} date={stationData?.beaconActivityDate} days={stationData?.beaconActivityDays} />
                <br />
                {stationData?.stats ? (
                    <>
                        <b>{isRange ? t('statistics.title_range') : t('statistics.title', {when: formatEpoch(stationData.outputEpoch)})}</b>
                        <table>
                            <tbody>
                                {Object.keys(stationData.stats).filter((key) => key !== 'ignoredPAW').map((key) => (
                                    <tr key={key}>
                                        <td>{t(`statistics.${key}`)}</td>
                                        <td>{stationData.stats[key]}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </>
                ) : null}
                {(stationData?.mobile || stationData?.moved || stationData?.bouncing) ? (
                    <StationPosition
                        mobile={stationData.mobile}
                        moved={stationData.moved}
                        bouncing={stationData.bouncing}
                        primaryLocation={stationData.primary_location}
                        previousLocation={stationData.previous_location}
                        lastSeenAtPrimary={stationData.lastSeenAtPrimary}
                        lastSeenAtPrevious={stationData.lastSeenAtPrevious}
                        mapboxToken={env?.NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN || ''}
                    />
                ) : null}
                {stationData?.status ? (
                    <>
                        <br />
                        <b>{t('status.title')}</b>
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
                {stationData?.lastLocation || stationData?.lastPacket || stationData?.lastBeacon || stationData?.outputDate ? (
                    <>
                        <br />
                        <b>{t('times.title')}</b>
                        <table>
                            <tbody>
                                {stationData.lastLocation ? (
                                    <tr key="location">
                                        <td>{t('times.location')}</td>
                                        <td>{formatEpoch(stationData.lastLocation)}</td>
                                    </tr>
                                ) : null}
                                {stationData.lastPacket ? (
                                    <tr key="packet">
                                        <td>{t('times.packet')}</td>
                                        <td>{formatEpoch(stationData.lastPacket)}</td>
                                    </tr>
                                ) : null}
                                {stationData.lastBeacon ? (
                                    <tr key="beacon">
                                        <td>{t('times.beacon')}</td>
                                        <td>{formatEpoch(stationData.lastBeacon)}</td>
                                    </tr>
                                ) : null}
                                {stationData.outputDate ? (
                                    <tr key="output">
                                        <td>{t('times.output')}</td>
                                        <td>{formatEpoch(stationData.outputEpoch)}</td>
                                    </tr>
                                ) : null}
                            </tbody>
                        </table>
                    </>
                ) : null}

                <p style={{height: '5rem'}} />
            </>
        );
    }

    return (
        <>
            <Trans t={t} i18nKey="help">
                Hover over somewhere on the map to see details.
                <br />
                Click to lock the sidebar display to that location.
                <br />
                Click on a station marker to show coverage records only for that station.
                <br />
                You can resize the sidebar by dragging the edge - if you resize it to zero then you will see tooltips with the information
            </Trans>
            {!station && (
                <>
                    <hr />
                    <ProtocolStatsDashboard layers={layers ?? ['combined']} setLayers={setLayers ?? (() => {})} dateRange={dateRange} />
                </>
            )}
        </>
    );
}
