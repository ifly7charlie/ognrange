import {useRouter} from 'next/router';
import {useSearchParams} from 'next/navigation';

import dynamic from 'next/dynamic';
import Head from 'next/head';
import type {GetServerSideProps} from 'next';

import {useState, useRef, useCallback} from 'react';

import {debounce as _debounce} from 'lodash';

import {stationMeta} from '../lib/react/stationMeta';

import getConfig from 'next/config';
const {serverRuntimeConfig} = getConfig();

const CoverageMap = dynamic(() => import('../lib/react/deckgl').then((mod) => mod.CoverageMap), {
    ssr: false,
    loading: () => (
        <div style={{width: '100vw', marginTop: '20vh', position: 'absolute'}}>
            <div style={{display: 'block', margin: 'auto', width: '100px'}}>
                <img width="100" height="100" src="https://ognproject.wdfiles.com/local--files/logos/ogn-logo-150x150.png" alt="OGN Network" title="OGN Network" />
            </div>
        </div>
    )
});

const Dock = dynamic(() => import('../lib/react/dock').then((mod) => mod.Dock), {
    ssr: false,
    loading: () => <></>
});

export function IncludeJavascript() {
    return (
        <>
            <link href="//api.mapbox.com/mapbox-gl-js/v2.15.0/mapbox-gl.css" rel="stylesheet" />
        </>
    );
}

//
// Main page rendering :)
export default function CombinePage(props) {
    // First step is to extract the class from the query, we use
    // query because that stops page reload when switching between the
    // classes. If no class is set then assume the first one
    const router = useRouter();
    const params = useSearchParams();

    const station = params.get('station');
    const visualisation = params.get('visualisation') || 'avgSig';
    const mapType = parseInt(params.get('mapType') || '3');
    const lat = params.get('lat');
    const lng = params.get('lng');
    const zoom = params.get('zoom');
    const file = params.get('file');

    // What the map is looking at
    const [viewport, setViewport] = useState({
        latitude: parseFloat(lat || '0') || 49.50305,
        longitude: parseFloat(lng || '0') || 13.27524,
        zoom: parseFloat(zoom || '0') || 3.3,
        minZoom: 2.5,
        maxZoom: 13,
        bearing: 0,
        minPitch: 0,
        maxPitch: 85,
        altitude: 1.5,
        pitch: !(mapType % 2) ? 70 : 0
    });

    // Tooltip or sidebar
    const [details, setDetails] = useState({});

    // For highlight which station we are talking about
    const defaultHighlight = [];
    const [highlightStations, setHighlightStations] = useState<[number, number][]>(defaultHighlight);

    // Update the station by updating the query url preserving all parameters but station
    const setStation = useCallback(
        (newStation: string) => {
            if (!stationMeta) {
                return;
            }
            if (station === newStation) {
                newStation = '';
            }
            updateUrl({station: newStation});
        },
        [stationMeta, station]
    );

    const setFile = useCallback(
        (newFile: string) => {
            if (file === newFile) {
                newFile = '';
            }
            updateUrl({file: newFile});
        },
        [file]
    );

    const setVisualisation = useCallback(
        (newVisualisation: string) => {
            if (visualisation == newVisualisation) {
                return;
            }
            updateUrl({visualisation: newVisualisation});
        },
        [visualisation]
    );

    const updateUrl = useCallback(
        (updates: any) => {
            const newParams = new URLSearchParams(window.location.search);
            let changed = false;
            for (const key of Object.keys(updates)) {
                if (updates[key] !== params.get(key)) {
                    if (updates[key]) {
                        newParams.set(key, updates[key]);
                    } else {
                        newParams.delete(key);
                    }
                    changed = true;
                }
            }
            if (changed) {
                router.push(
                    {
                        pathname: '/',
                        query: newParams.toString()
                    },
                    undefined,
                    {shallow: true}
                );
            }
        },
        [params]
    );

    const delayedUpdate = useRef(
        _debounce((vs) => {
            const updates = {
                ...(vs.latitude && vs.longitude
                    ? {
                          lat: vs.latitude.toFixed(5),
                          lng: vs.longitude.toFixed(5)
                      }
                    : {}),
                ...(vs.zoom ? {zoom: vs.zoom.toFixed(1)} : {})
            };
            updateUrl(updates);
        }, 300)
    ).current;

    // Synchronise it back to the url
    const setViewportUrl = useCallback(
        (vs: any) => {
            delayedUpdate(vs);
            setViewport(vs);
        },
        [false]
    );

    const [dockSplit, setDockSplit] = useState<number>(0.25);
    const [dockPosition, setDockPosition] = useState<'right' | 'bottom'>('right');

    const dockSplitW = dockPosition === 'right' ? ((1 - Math.max(dockSplit, 0)) * 100).toFixed(0) : '100';
    const dockSplitH = dockPosition === 'bottom' ? ((1 - Math.max(dockSplit, 0)) * 100).toFixed(0) : '100';

    return (
        <>
            <Head>
                <title>OGN Range</title>
            </Head>

            <div>
                <div style={{width: `${dockSplitW}vw`, height: `${dockSplitH}vh`}}>
                    <CoverageMap //
                        env={props.env}
                        station={station}
                        file={file}
                        setStation={setStation}
                        visualisation={visualisation}
                        viewport={viewport}
                        setViewport={setViewportUrl}
                        mapType={mapType}
                        tooltips={false}
                        highlightStations={highlightStations}
                        setHighlightStations={setHighlightStations}
                        details={details}
                        setDetails={setDetails}
                        dockSplit={dockSplit}
                    ></CoverageMap>
                </div>
                <Dock //
                    env={props.env}
                    station={station}
                    setStation={setStation}
                    visualisation={visualisation}
                    setVisualisation={setVisualisation}
                    file={file}
                    setFile={setFile}
                    details={details}
                    setDetails={setDetails}
                    updateUrl={updateUrl}
                    dockSplit={dockSplit}
                    setDockSplit={setDockSplit}
                    dockPosition={dockPosition}
                    setDockPosition={setDockPosition}
                />
            </div>
        </>
    );
}

// Just to force server side rendering
export const getServerSideProps = (async () => {
    return {
        props: {
            env: {
                NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN: serverRuntimeConfig.NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN,
                NEXT_PUBLIC_SITEURL: serverRuntimeConfig.NEXT_PUBLIC_SITEURL,
                NEXT_PUBLIC_DATA_URL: serverRuntimeConfig.NEXT_PUBLIC_DATA_URL,
                NEXT_PUBLIC_AIRSPACE_API_KEY: serverRuntimeConfig.NEXT_PUBLIC_AIRSPACE_API_KEY
            }
        }
    };
}) satisfies GetServerSideProps<{env: Record<string, string>}>;
