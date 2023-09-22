import {useRouter} from 'next/router';
import dynamic from 'next/dynamic';
import Head from 'next/head';

import useSWR from 'swr';
import {useState, useRef, useMemo, useEffect} from 'react';

// Helpers for loading contest information etc
import {Nbsp, Icon} from '../lib/react/htmlhelper';
import Select from 'react-select';
import AsyncSelect from 'react-select/async';

import {Settings} from '../lib/react/settings';

// And connect to websockets...
import {stationMeta} from '../lib/react/deckgl';
import {CoverageDetails} from '../lib/react/CoverageDetails';

const CoverageMap = dynamic(() => import('../lib/react/deckgl').then((mod) => mod.CoverageMap), {
    ssr: false,
    loading: () => (
        <div style={{width: '100vw', marginTop: '20vh', position: 'absolute'}}>
            <div style={{display: 'block', margin: 'auto', width: '100px'}}>
                <img width="100" height="100" src="http://ognproject.wdfiles.com/local--files/logos/ogn-logo-150x150.png" alt="OGN Network" title="OGN Network" />
            </div>
        </div>
    )
});

import Router from 'next/router';
import {Dock} from 'react-dock';

import {debounce as _debounce, map as _map, find as _find, filter as _filter} from 'lodash';

export function IncludeJavascript() {
    return (
        <>
            <link rel="stylesheet" href="/bootstrap/css/font-awesome.min.css" />
            <link href="//api.mapbox.com/mapbox-gl-js/v1.11.0/mapbox-gl.css" rel="stylesheet" />
        </>
    );
}

const normalVisualisations = [
    {label: 'Average Signal Strength', value: 'avgSig'},
    {label: 'Maximum Signal Strength', value: 'maxSig'},
    {label: 'Count', value: 'count'},
    {label: 'Minimum Altitude', value: 'minAlt'},
    {label: 'Minimum Altitude AGL', value: 'minAgl'},
    {label: 'Max Signal @ Minimum Altitude', value: 'minAltSig'},
    {label: 'Avg CRC errors', value: 'avgCrc'},
    {label: 'Average between packet gap', value: 'avgGap'}
];
const globalVisualisations = [
    {label: 'Expected between packet gap', value: 'expectedGap'},
    {label: 'Number of stations', value: 'stations'},
    {label: 'Primary station', value: 'primaryStation'}
];

// Convert list of files into a select
const fetcher = (...args) => fetch(...args).then((res) => res.json());
const DATA_URL = process.env.NEXT_PUBLIC_DATA_URL || '/data/';

//
// Main page rendering :)
export default function CombinePage(props) {
    // First step is to extract the class from the query, we use
    // query because that stops page reload when switching between the
    // classes. If no class is set then assume the first one
    const router = useRouter();
    let {station, visualisation, mapType, lat, lng, zoom, file} = router.query;
    mapType = mapType ? parseInt(mapType) : 3;
    if (!visualisation) {
        visualisation = 'avgSig';
    }

    // Where to put the dock
    const hasWindow = typeof window !== 'undefined';
    const dockPosition = hasWindow && window.innerWidth < window.innerHeight ? 'bottom' : 'right';

    // What the map is looking at
    const [viewport, setViewport] = useState({
        latitude: parseFloat(lat || 0) || 49.50305,
        longitude: parseFloat(lng || 0) || 13.27524,
        zoom: parseFloat(zoom || 0) || 3.3,
        minZoom: 2.5,
        maxZoom: 13,
        bearing: 0,
        minPitch: 0,
        maxPitch: 85,
        altitude: 1.5,
        pitch: !(mapType % 2) ? 70 : 0
    });

    useEffect(() => {
        if (router.isReady && lat && lng) {
            setViewport({
                ...viewport,
                longitude: parseFloat(lng),
                latitude: parseFloat(lat),
                zoom: parseFloat(zoom || 0) || 3.3
            });
        }
    }, [router.isReady]);

    // Load the associated index
    const {data, error} = useSWR(DATA_URL + (station || 'global') + '/' + (station || 'global') + '.index.json', fetcher);

    // Display the right ones to the user
    const [availableFiles, selectedFile] = useMemo(
        (_) => {
            const files = data?.files || {year: {current: 'year', all: ['year']}};
            const selects = _map(files, (value, key) => {
                return {
                    label: key,
                    options: _map(value.all, (cfile) => {
                        // latest is also symbolic linked, we use that instead
                        if (cfile == value.current) {
                            return {
                                label: 'Current ' + key + ' (' + (cfile.match(/([0-9-]+)$/) || [cfile])[0] + ')',
                                value: key
                            };
                        } else {
                            return {
                                label: (cfile.match(/([0-9-]+)$/) || [cfile])[0],
                                value: (cfile.match(/((day|month|year)\.[0-9-]+)$/) || [cfile])[0]
                            };
                        }
                    }).reverse()
                };
            }).reverse();
            const effectiveFile = file && file != '' ? file : files.year.current;
            const [type] = file?.split('.') || ['year'];
            const selected = selects
                ? _find(_find(selects, {label: type})?.options || [], (o) => {
                      return effectiveFile.slice(-o.value.length) == o.value;
                  })
                : null;

            return [selects, selected];
        },
        [file, data?.files?.day?.current]
    );

    // Figure out our visualisations
    const [visualisations, selectedVisualisation] = useMemo(
        (_) => {
            const vis = [...normalVisualisations, ...((station || 'global') == 'global' ? globalVisualisations : [])];
            return [vis, _find(vis, {value: visualisation})];
        },
        [visualisation, station]
    );

    // Tooltip or sidebar
    const [expanded, setExpanded] = useState(true);
    const [details, setDetails] = useState({});
    const [size, setSize] = useState(0.25);

    // For highlight which station we are talking about
    const defaultHighlight = {};
    if (station) {
        defaultHighlight[station] = 1;
    }
    const [highlightStations, setHighlightStations] = useState(defaultHighlight);

    const selectedStation = {
        value: station || 'global',
        label: station || 'All Stations (global)'
    };

    const defaultStationSelection = [
        {label: 'Start typing to search', value: ''},
        {label: 'All Stations (global)', value: ''}
    ];

    async function findStation(s) {
        if (s.length > 2) {
            try {
                let re = new RegExp(s, 'i');

                const p = _map(
                    _filter(stationMeta, (v) => v.station.match(re)),
                    (o) => {
                        return {value: o.station, label: o.station};
                    }
                );
                return p;
            } catch (e) {
                return [];
            }
        }
        return [{value: '', label: 'All Stations (global)'}];
    }

    // Update the station by updating the query url preserving all parameters but station
    function setStation(newStation) {
        if (!stationMeta) {
            return;
        }
        if (station === newStation) {
            newStation = '';
        }
        router.push({pathname: '/', query: {...router.query, station: newStation}}, undefined, {shallow: true});
    }

    function setFile(newFile) {
        if (file === newFile) {
            newFile = '';
        }
        router.push({pathname: '/', query: {...router.query, file: newFile}}, undefined, {shallow: true});
    }

    function setVisualisation(newVisualisation) {
        if (visualisation == newVisualisation) {
            return;
        }
        router.push(
            {
                pathname: '/',
                query: {...router.query, visualisation: newVisualisation}
            },
            undefined,
            {shallow: true}
        );
    }

    // Debounced updating of the URL when the viewport is changed, this is a performance optimisation
    function updateUrl(query, vs) {
        router.replace(
            {
                pathname: '/',
                query: {
                    ...query,
                    lat: vs.latitude.toFixed(5),
                    lng: vs.longitude.toFixed(5),
                    zoom: vs.zoom.toFixed(1)
                }
            },
            undefined,
            {shallow: true}
        );
    }
    const delayedUpdate = useRef(_debounce((query, vs) => updateUrl(query, vs), 300)).current;

    // Synchronise it back to the url
    function setViewportUrl(vs) {
        vs.bearing = 0;
        if (router.isReady) {
            delayedUpdate(router.query, vs);
        }
        setViewport(vs);
    }

    function onDockResize(size) {
        setExpanded(size > 0.02);
        setSize(size);
    }
    function onDockVisibleChange(isVisible) {
        setExpanded(isVisible);
    }

    return (
        <>
            <Head>
                <title>OGN Range (Beta)</title>
                <meta name="viewport" content="width=device-width, minimal-ui" />
                <IncludeJavascript />
            </Head>

            <div>
                <div>
                    <CoverageMap //
                        station={station}
                        file={file}
                        setStation={setStation}
                        visualisation={visualisation}
                        viewport={viewport}
                        setViewport={setViewportUrl}
                        mapType={mapType}
                        setMapType={() => {}}
                        highlightStations={highlightStations}
                        setHighlightStations={setHighlightStations}
                        tooltips={!expanded}
                        setDetails={setDetails}
                    ></CoverageMap>
                </div>
                {router.isReady && (
                    <Dock isVisible={expanded && process.browser} size={size} style={{border: '10px solid black'}} dimMode="none" position={dockPosition} onVisibleChange={onDockVisibleChange} onSizeChange={onDockResize}>
                        <div>
                            <span style={{padding: '0px', border: '5px solid white'}}>
                                <img width="100" height="100" src="http://ognproject.wdfiles.com/local--files/logos/ogn-logo-150x150.png" alt="OGN Network" title="OGN Network" />
                            </span>
                        </div>

                        <div style={{padding: '7px'}}>
                            <div>
                                <b>Select station to display:</b>
                                <AsyncSelect loadOptions={findStation} value={selectedStation} defaultOptions={defaultStationSelection} onChange={(v) => setStation(v.value)} noOptionsMessage={() => 'Start typing to search'} />
                                <br />
                                <b>Select available time period to display:</b>
                                <Select options={availableFiles} value={selectedFile} onChange={(v) => setFile(v.value)} />
                                <br />
                                <b>Select visualisation:</b>
                                <Select options={visualisations} value={selectedVisualisation} onChange={(v) => setVisualisation(v.value)} />
                            </div>
                            <hr />
                            {expanded && (
                                <>
                                    <CoverageDetails //
                                        details={details}
                                        station={station}
                                        highlightStations={highlightStations}
                                        setHighlightStations={setHighlightStations}
                                        file={file}
                                    />
                                </>
                            )}
                        </div>
                        <Settings />
                    </Dock>
                )}
            </div>
        </>
    );
}
