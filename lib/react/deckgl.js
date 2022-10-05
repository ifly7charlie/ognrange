import React, {useState, useCallback, useMemo, useRef} from 'react';

import DGL from '@deck.gl/react';
import {IconLayer, MapView, ColumnLayer} from '@deck.gl/layers';
import {H3HexagonLayer} from '@deck.gl/geo-layers';

import {FlyToInterpolator} from '@deck.gl/core';
import {StaticMap, Source, Layer} from 'react-map-gl';
import {LngLat} from 'mapbox-gl';
import {MercatorCoordinate} from 'mapbox-gl';
import {MapboxLayer} from '@deck.gl/mapbox';
import {AttributionControl} from 'react-map-gl';
import {Matrix4} from '@math.gl/core';

import {ArrowLoader} from '@loaders.gl/arrow';
import {JSONLoader} from '@loaders.gl/json';

import ReactDOMServer from 'react-dom/server';

import Link from 'next/link';
import {useRouter} from 'next/router';

import {progressFetch} from './progressFetch';

import _map from 'lodash.map';
import _reduce from 'lodash.reduce';
import _chunk from 'lodash.chunk';
import _zip from 'lodash.zip';
import _keyby from 'lodash.keyby';
import _filter from 'lodash.filter';
import _find from 'lodash.find';

import {splitLongToh3Index, h3ToGeo, pointDist} from 'h3-js';

import {defaultFromColour, defaultToColour, defaultBaseMap} from './defaults';

// Lookup the URL for data
const DATA_URL = process.env.NEXT_PUBLIC_DATA_URL || '/data/';

const steps = 8;

const altitudeFunctions = {
    minAlt: (f) => f.b,
    minAgl: (f) => f.g
};

export let stationMeta = undefined;
let maxCount = 0;

//
// Responsible for generating the deckGL layers
//
function makeLayers(station, file, setStation, highlightStations, visualisation, map2d, onClick, lockedH3, setProgress, getProgress, colourise, colours) {
    const ICON_MAPPING = {
        marker: {x: 0, y: 0, width: 128, height: 128, mask: true}
    };

    // Colouring and display options
    let getStationColor = (d) => (highlightStations[d.id] ? [255, 16, 240] : [0, 0, 192]);
    if (station) {
        getStationColor = (d) => (d.station == station ? [255, 16, 240] : [0, 0, 255]);
    }
    let getStationSize = (d) => (highlightStations[d.id] || d.station == station ? 7 : 5);

    const visualisationFunctions = {
        count: (f) => colourise(Math.log2(f.c) * (254 / maxCount)),
        avgSig: (f) => colourise(Math.min(f.a, 254)),
        maxSig: (f) => colourise(Math.min(f.e, 254)),
        minAlt: (f) => colourise(Math.min(f.b / 20, 254)),
        minAgl: (f) => colourise(Math.min(f.g / 20, 254)),
        minAltSig: (f) => colourise(f.d),
        avgCrc: (f) => colourise(255 - Math.min(f.f * 5, 254)),
        avgGap: (f) => colourise(Math.log2(Math.max(f.p, 4) - 3) * 31.75), // it shouldn't be less than 3 seconds so offset but no log2(0) => inf
        expectedGap: (f) => colourise(Math.log2((f.q ?? f.p) || 1) * 31.75),
        stations: (f) => colourise(Math.min(((f.t || 1) - 1) * 9, 254)),
        primaryStation: (f) => (f.s ? [parseInt(f.s.slice(0, 1), 36) * 7, parseInt(f.s.slice(1, 2), 36) * 7, parseInt(f.s.slice(2, 3), 36) * 7, 128] : [255, 0, 255, 128])
    };

    // How do we choose what to show in the hexagon layer
    const visualisationFunction = visualisationFunctions[visualisation] || visualisationFunctions['avgSig'];
    const altitudeFunction = altitudeFunctions[visualisation] || (() => 0);

    // are we showing circles
    const locations = _filter(
        _map(Object.keys(highlightStations), (f) => (stationMeta?.[f]?.lat && stationMeta?.[f]?.lng ? stationMeta?.[f] : undefined)),
        (v) => !!v
    );
    const l10k = _map([10, 20, 30], (r) =>
        locations
            ? new ColumnLayer({
                  id: 'stationk' + r,
                  data: locations,
                  diskResolution: 50,
                  radius: r * 1000,
                  radiusUnits: 'meters',
                  extruded: false,
                  filled: false,
                  stroked: true,
                  pickable: false,
                  elevationScale: 1,
                  getPosition: (d) => [d.lng, d.lat],
                  getLineColor: [255, 16, 240, 128],
                  getLineWidth: 10,
                  getElevation: 1000,
                  getFillColor: [, 150],
                  lineWidthMinPixels: 2,
                  lineWidthMaxPixels: 5
              })
            : null
    );

    const lockedH3l = lockedH3
        ? new H3HexagonLayer({
              id: 'lockedH3',
              data: [{hex: lockedH3}],
              pickable: false,
              wireframe: false,
              filled: false,
              stroked: true,
              extruded: false,
              elevationScale: 0,
              lineWidthMinPixels: 5,
              getHexagon: (d) => d.hex,
              getFillColor: (d) => [0, 0, 0],
              getElevation: (d) => 0,
              onClick: onClick
          })
        : undefined;

    // So we can check loading status
    const hexLayer = new H3HexagonLayer({
        id: (station || 'global') + (file || 'year'),
        data: `${DATA_URL}${station || 'global'}/${station || 'global'}.${file || 'year'}.arrow`,
        loadOptions: {
            fetch: (...o) => {
                return fetch(...o).then(progressFetch(setProgress));
            }
        },
        loaders: ArrowLoader,
        dataTransform: (d) => {
            const stringed =
                'h3lo' in d
                    ? _map(d.h3lo, (p, i) => {
                          return splitLongToh3Index(p, d.h3hi[i]);
                      })
                    : _map(_chunk(d.h3, 2), (p) => {
                          return splitLongToh3Index(p[0], p[1]);
                      });
            maxCount = 0;
            const r = _map(stringed, (v, i) => {
                maxCount = Math.max(maxCount, d.count[i]);
                return {
                    h: v,
                    a: d.avgSig[i],
                    b: d.minAlt[i],
                    c: d.count[i],
                    d: d.minAltSig[i],
                    e: d.maxSig[i],
                    f: d.avgCrc[i],
                    g: d.minAgl[i],
                    p: d.avgGap[i],
                    q: d.expectedGap?.[i],
                    s: d.stations?.[i] || 0,
                    t: d.numStations?.[i] ?? d.stations?.[i]?.split(',')?.length ?? 0
                };
            });
            maxCount = Math.log2(maxCount);
            return r;
        },
        pickable: true,
        wireframe: false,
        filled: true,
        extruded: false,
        elevationScale: 0,
        getHexagon: (d) => d.h,
        getFillColor: (d) => visualisationFunction(d),
        getElevation: map2d ? (d) => 0 : (d) => altitudeFunction(d),
        updateTriggers: {
            getFillColor: [visualisation, colours],
            getElevation: [map2d ? false : visualisation]
        },
        onClick: onClick
    });

    // Add a layer for the recent points for each pilot
    let layers = [
        ...l10k,

        // Normal hex layer
        hexLayer,

        // locked H3 if there is one
        lockedH3l,

        // Stations
        new IconLayer({
            id: 'icon-layer',
            data: `${DATA_URL}stations.json`,
            loaders: [JSONLoader],
            dataTransform: (d) => {
                if (d) {
                    stationMeta = _keyby(d, 'id');
                }
                return d;
            },
            // allow hover and click
            pickable: true,
            onClick: (i) => {
                setStation(i.object?.station || '');
            },
            // What icon to display
            iconAtlas: 'https://raw.githubusercontent.com/visgl/deck.gl-data/master/website/icon-atlas.png',
            iconMapping: ICON_MAPPING,
            getIcon: (d) => 'marker',
            // How big
            sizeScale: 500,
            sizeMinPixels: 5,
            sizeMaxPixels: 50,
            sizeUnits: 'meters',
            getSize: getStationSize,
            // where and what colour
            getPosition: (d) => [d.lng, d.lat],
            getColor: getStationColor,
            updateTriggers: {
                getColor: [station, highlightStations],
                getSize: [station, highlightStations]
            }
        })
    ];
    return {hexLayer, layers: layers};
}

export function CoverageMap(props) {
    const router = useRouter();
    const [lockedH3, setLockedH3] = useState(null);
    const [isLoaded, setLoaded] = useState(null);

    const toColour = router.query.toColour || defaultToColour;
    const fromColour = router.query.fromColour || defaultFromColour;

    // For remote updating of the map
    const mapRef = useRef(null);

    // Map display style
    const map2d = props.mapType > 1;
    const mapStreet = props.mapType % 2;

    const onClick = useCallback(
        (o) => {
            if (lockedH3 && (o?.object?.h == lockedH3 || !o?.object?.h)) {
                props.setDetails({locked: false});
                setLockedH3(null);
            } else {
                setLockedH3(o?.object?.h);
                props.setDetails({...o.object, locked: true});
            }
        },
        [lockedH3]
    );

    const colourMaps = useMemo(
        (_) => {
            const f = router.query.fromColour || defaultFromColour;
            const t = router.query.toColour || defaultToColour;
            return _zip(
                progression(f, t, 0), // R
                progression(f, t, 1), // G
                progression(f, t, 2), // B
                progression(f, t, 3)
            ); // A
        },
        [router.isReady, router.query.fromColour, router.query.toColour]
    );

    //    console.log(colourMaps);

    const colourise = (v) => {
        if (v > 255) {
            console.log(v);
        }
        return colourMaps[Math.trunc(((v / 255) * steps) % steps)];
    };

    //
    // Generate the deckGL layers
    // We don't need to do this unless parameters change
    const {hexLayer, layers} = useMemo(
        (_) =>
            makeLayers(
                props.station,
                props.file,
                props.setStation, //
                router.query.highlightStations == '0' ? '' : props.highlightStations,
                props.visualisation,
                map2d,
                onClick,
                lockedH3,
                setLoaded,
                () => isLoaded,
                colourise,
                fromColour + toColour
            ),
        [props.station, props.file, map2d, props.visualisation, props.highlightStations, lockedH3, fromColour, toColour]
    );

    const onMapLoad = useCallback(
        (evt) => {
            console.log('onMapLoad', map2d);
            if (!map2d) {
                const map = evt.target;
                console.log(map);
                map.setTerrain({source: 'mapbox-dem'});
            }
        },
        [map2d]
    );

    // Update the view and synchronise with mapbox
    const onViewStateChange = ({viewState}) => {
        if (map2d) {
            viewState.minPitch = 0;
            viewState.maxPitch = 0;
        } else {
            viewState.minPitch = 0;
            viewState.maxPitch = 85;
        }

        const map = mapRef?.current?.getMap();
        if (map && map.transform.elevation && !map2d) {
            const mapbox_elevation = map.transform.elevation.getAtPoint(MercatorCoordinate.fromLngLat(map.getCenter()));
            //const mapbox_elevation = map.transform.elevation.getAtPoint(MercatorCoordinate.fromLngLat(new LngLat(viewState.longitude, viewState.latitude)));
            props.setViewport({
                ...viewState,
                ...{position: [0, 0, mapbox_elevation]}
            });
        } else {
            props.setViewport(viewState);
        }
    };

    let attribution = `<a href="//www.glidernet.org/">Data from OGN</a> | `;
    if (props.station) {
        attribution += `Currently showing station ${props.station}`;
    } else {
        attribution += `Currently showing all stations`;
    }

    //
    // Generate tooltip text, and update side panel
    function useToolTipSelection({object, picked}) {
        if (lockedH3) {
            if (object?.station) {
                return {html: object.station};
            }
            return null;
        }

        if (!picked) {
            if (props.highlightStations) {
                props.setHighlightStations({});
            }
            props.setDetails(null);
            return null;
        }
        if (object) {
            props.setDetails(object);

            if (!props.tooltip && object.s) {
                const parts = object.s.split(',');
                props.setHighlightStations(
                    _reduce(
                        parts,
                        (acc, x) => {
                            const sid = parseInt(x, 36) >> 4;
                            acc[sid] = !!stationMeta[sid]?.lat;
                            return acc;
                        },
                        {}
                    )
                );
            }
        }

        if (props.tooltips) {
            const html = ReactDOMServer.renderToStaticMarkup(
                <CoverageDetails
                    details={object} //
                    station={props.station}
                    highlightStations={props.highlightStations}
                    setHighlightStations={props.setHighlightStations}
                />
            );
            return {html};
        }
    }

    const loadingLayer = isLoaded ? (
        <div className="progress-bar">
            {(isLoaded * 100).toFixed(0)}%
            <div className="progress" style={{transform: `scaleX(${isLoaded})`}} />
        </div>
    ) : null;

    const airspaceLayer = {
        id: 'airspaceLayer',
        type: 'raster',
        source: 'airspace'
    };

    return (
        <div>
            <DGL viewState={props.viewport} controller={{scrollZoom: {smooth: false}, touchRotate: true}} onViewStateChange={(e) => onViewStateChange(e)} getTooltip={(x) => useToolTipSelection(x)} onClick={onClick} layers={layers}>
                <StaticMap
                    mapboxApiAccessToken={process.env.NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN} //
                    mapStyle={'mapbox://styles/' + (router.query.mapStyle || defaultBaseMap)}
                    onLoad={onMapLoad}
                    ref={mapRef}
                    attributionControl={false}
                >
                    {!map2d && (
                        <>
                            <Source id="mapbox-dem" type="raster-dem" url="mapbox://mapbox.mapbox-terrain-dem-v1" tileSize={512} maxzoom={14} />
                            <Layer {...hillshade} />
                            <Layer {...skyLayer} />
                        </>
                    )}
                    {router.query?.airspace && (
                        <>
                            <Source id="airspace" type="raster" tiles={['https://api.tiles.openaip.net/api/data/openaip/{z}/{x}/{y}.png?apiKey=760e6a3ccde9c9f277f3de723169934b']} maxzoom={14} tileSize={256} />
                            <Layer {...airspaceLayer} minzoom={0} maxzoom={14} />
                        </>
                    )}
                    <AttributionControl key={props.station} customAttribution={attribution} style={attributionStyle} />
                </StaticMap>
            </DGL>
            {loadingLayer}
        </div>
    );
}

//
// Used to generate the tooltip or the information to display in the details panel

export function CoverageDetails({details, station, setHighlightStations, highlightStations}) {
    let stationList = undefined;
    let stationCount = undefined;
    const router = useRouter();

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
                <br />
                <b>Signal</b>
                <br />
                Average: {(details.a / 4).toFixed(1)} dB, Max: {(details.e / 4).toFixed(1)} dB
                <br />
                <hr />
                <b>Lowest Point</b>
                <br />
                {(details.d / 4).toFixed(1)} dB @ {details.b} m ({details.g} m agl)
                <br />
                <hr />
                Avg CRC errors: {details.f / 10}
                <br />
                <hr />
                Avg Gap: {details.p >> 2}s{' '}
                {(details.q ?? true) !== true && stationCount > 1 ? (
                    <>
                        (expected: {details.q >> 2}s)
                        <br />
                    </>
                ) : (
                    <br />
                )}
                <hr />
                Number of packets: {details.c}
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

//
// Produce the array of colours for the display
function progression(f, t, offset) {
    const start = parseInt(f.slice(offset * 2, offset * 2 + 2) || 'ff', 16);
    const end = parseInt(t.slice(offset * 2, offset * 2 + 2) || 'ff', 16);
    const step = (end - start) / steps;
    return Array(steps)
        .fill()
        .map((_e, index) => start + Math.round(index * step));
}

const attributionStyle = {
    right: 0,
    bottom: 0,
    fontSize: '13px'
};

const hillshade = {
    id: 'hillshade',
    source: 'mapbox-dem',
    type: 'hillshade',
    paint: {
        'hillshade-shadow-color': 'orange',
        'hillshade-illumination-anchor': 'map',
        'hillshade-illumination-direction': 176,
        'hillshade-highlight-color': 'green'
    }
};
const skyLayer = {
    id: 'sky',
    type: 'sky',
    paint: {
        'sky-opacity': ['interpolate', ['linear'], ['zoom'], 0, 0, 5, 0.3, 8, 1],
        // set up the sky layer for atmospheric scattering
        'sky-type': 'atmosphere',
        // explicitly set the position of the sun rather than allowing the sun to be attached to the main light source
        'sky-atmosphere-sun': [0, 0],
        // set the intensity of the sun as a light source (0-100 with higher values corresponding to brighter skies)
        'sky-atmosphere-sun-intensity': 5,
        'sky-atmosphere-color': 'rgba(135, 206, 235, 1.0)'
    }
};
