import React, {useState, useCallback, useMemo, useRef, useEffect} from 'react';
import {MapboxOverlay, MapboxOverlayProps} from '@deck.gl/mapbox';

import Map, {Source, Layer, useControl, NavigationControl, ScaleControl} from 'react-map-gl';

import {IconLayer, ColumnLayer} from '@deck.gl/layers';
import {H3HexagonLayer} from '@deck.gl/geo-layers';

import {AttributionControl} from 'react-map-gl';

import {ArrowLoader} from '@loaders.gl/arrow';

import ReactDOMServer from 'react-dom/server';

import {getObjectFromIndex} from './pickabledetails';

import {useRouter} from 'next/router';

import {progressFetch} from './progressFetch';

import {map as _map, sortedIndexOf as _sortedIndexOf, reduce as _reduce, chunk as _chunk, zip as _zip, keyBy as _keyby, filter as _filter, find as _find, debounce as _debounce} from 'lodash';

import {CoverageDetailsToolTip} from './CoverageDetails';

import {defaultFromColour, defaultToColour, defaultBaseMap} from './defaults';

// Lookup the URL for data
import {NEXT_PUBLIC_DATA_URL} from '../common/config';

const steps = 8;

const altitudeFunctions = {
    minAlt: (f) => f.b,
    minAgl: (f) => f.g
};

import {stationMeta, setStationMeta} from './stationMeta';
let maxCount = 0;

function DeckGLOverlay(
    props: MapboxOverlayProps & {
        interleaved?: boolean;
    }
) {
    const overlay = useControl<MapboxOverlay>(() => new MapboxOverlay(props));
    overlay.setProps(props);
    return null;
}

type HighlightStationIndicies = number[];

//
// Responsible for generating the deckGL layers
//
function makeLayers(station, file, setStation, highlightStations: HighlightStationIndicies, visualisation, map2d, onClick, lockedH3, setProgress, getProgress, colourise, colours, env) {
    const ICON_MAPPING = {
        marker: {x: 0, y: 0, width: 128, height: 128, mask: true}
    };

    // Colouring and display options
    let getStationColor = (d, {index}) => (stationMeta.name[index] == station || highlightStations[index] ? [255, 16, 240] : [0, 0, 192]);
    //    if (station) {
    //        getStationColor = (d) => (d.station == station ? [255, 16, 240] : [0, 0, 255]);
    //    }
    let getStationSize = (d, {index}) => (stationMeta.name[index] == station || highlightStations[index] ? 7 : 5);
    const visualisationFunctions = {
        count: (f, i) => colourise(Math.log2(f.count[i]) * (254 / maxCount)),
        avgSig: (f, i) => colourise(Math.min(f.avgSig[i] * 3, 254)),
        maxSig: (f, i) => colourise(Math.min(f.maxSig[i] * 1.3, 254)),
        minAlt: (f, i) => colourise(Math.min(f.minAlt[i] / 20, 254)),
        minAgl: (f, i) => colourise(Math.min(f.minAgl[i] / 20, 254)),
        minAltSig: (f, i) => colourise(f.minAltSig[i]),
        avgCrc: (f, i) => colourise(255 - Math.min(f.avgCrc[i] * 5, 254)),
        avgGap: (f, i) => colourise(Math.log2(Math.max(f.avgGap[i], 4) - 3) * 31.75), // it shouldn't be less than 3 seconds so offset but no log2(0) => inf
        expectedGap: (f, i) => colourise(Math.log2((f.expectedGap?.[i] ?? f.avgGap?.[i]) || 1) * 31.75),
        stations: (f, i) => colourise(Math.min(Math.log2(f.numStations?.[i] || 1) * 32, 254)),
        primaryStation: (f, i) => {
            const s = f.stations?.[i];
            return s ? [parseInt(s.slice(0, 1), 36) * 7, parseInt(s.slice(1, 2), 36) * 7, parseInt(s.slice(2, 3), 36) * 7, 128] : [255, 0, 255, 128];
        }
    };

    // How do we choose what to show in the hexagon layer
    const visualisationFunction = visualisationFunctions[visualisation] || visualisationFunctions['avgSig'];
    const altitudeFunction = altitudeFunctions[visualisation] || (() => 0);

    const l10k = _map([10, 20, 30], (r) =>
        //        locations
        highlightStations?.length
            ? new ColumnLayer({
                  id: 'stationk' + r,
                  data: highlightStations, // locations,
                  diskResolution: 50,
                  radius: r * 1000,
                  radiusUnits: 'meters',
                  extruded: false,
                  filled: false,
                  stroked: true,
                  pickable: false,
                  elevationScale: 1,
                  getPosition: (d, {index, data}) => [stationMeta.lng[d], stationMeta.lat[d]], // data.l[index], //.data.h3s[index],
                  //                {l: [stationMeta?.lng[index], stationMeta?.lat[index]], index});
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
        data: `${env.NEXT_PUBLIC_DATA_URL || NEXT_PUBLIC_DATA_URL}${station || 'global'}/${station || 'global'}.${file || 'year'}.arrow`,
        loadOptions: {
            fetch: (input, init) => {
                return fetch(input, init).then(progressFetch(setProgress));
            }
        },
        loaders: ArrowLoader,
        dataTransform: (d: any & {count: number[]}) => {
            maxCount = 0;
            for (const v of d.count) {
                maxCount = Math.max(maxCount, v);
            }
            return {length: d.avgSig.length, d, maxCount: Math.log2(maxCount)};
        },
        pickable: true,
        wireframe: false,
        filled: true,
        stroked: false,
        extruded: false,
        elevationScale: 0,
        getHexagon: (d, {index, data}) => [data.d.h3lo[index], data.d.h3hi[index]], //.data.h3s[index],
        getFillColor: (d, {index, data}) => visualisationFunction(data.d, index),
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
            data: `${env.NEXT_PUBLIC_DATA_URL ?? NEXT_PUBLIC_DATA_URL}stations.arrow`,
            loaders: [ArrowLoader],
            dataTransform: (d) => {
                if (d) {
                    setStationMeta(d);
                }
                d.length = d.id.length;
                return d;
            },
            // allow hover and click
            pickable: true,
            onClick,
            // What icon to display
            iconAtlas: 'https://raw.githubusercontent.com/visgl/deck.gl-data/master/website/icon-atlas.png',
            iconMapping: ICON_MAPPING,
            getIcon: (d) => 'marker',
            // How big
            sizeScale: 750,
            sizeMinPixels: 6,
            //            sizeMaxPixels: 30,
            sizeUnits: 'meters',
            getSize: getStationSize,
            // where and what colour
            getPosition: (d, {index, data}) => [data.lng[index], data.lat[index]], //.data.h3s[index],
            getColor: getStationColor,
            updateTriggers: {
                getColor: [station, highlightStations],
                getSize: [station, highlightStations]
            }
        })
    ];
    return {hexLayer, layers: layers};
}

export function CoverageMap(props: {
    //
    env: Record<string, string>;
    mapType: number;
    details: any;
    setDetails: (d: any) => void;
    highlightStations?: HighlightStationIndicies;
    setHighlightStations: (h: HighlightStationIndicies) => void;
    station?: string;
    visualisation: string;
    setStation: (s: any) => void;
    file?: string;
    tooltips: boolean;
    viewport: any;
    setViewport: Function;
    dockSplit: number;
}) {
    const router = useRouter();
    const [isLoaded, setLoaded] = useState(null);
    const details = props.details;

    const airspaceKey = props.env.NEXT_PUBLIC_AIRSPACE_API_KEY || process.env.NEXT_PUBLIC_AIRSPACE_API_KEY;

    const toColour = router.query.toColour || defaultToColour;
    const fromColour = router.query.fromColour || defaultFromColour;

    // For remote updating of the map
    const mapRef = useRef(null);

    // Map display style
    const map2d = props.mapType > 1;

    const onClick = useCallback(
        (o) => {
            const object = getObjectFromIndex(o.index, o.layer);
            if (details.locked && !object) {
                props.setDetails({});
            }

            if (!object) {
                return;
            }
            if (object.type === 'hexagon') {
                if (details.locked && (!object || object.i == props.details.i)) {
                    props.setDetails({});
                } else {
                    props.setDetails({...object, locked: true});
                }
            } else if (object.type === 'station') {
                props.setStation(object.name);
            }
        },
        [props.details, props.details.h?.[0], props.details.locked]
    );

    // Focus any selected station
    useEffect(() => {
        if (stationMeta) {
            const meta = _find(stationMeta, {station: props.station});
            if (mapRef?.current && meta) {
                mapRef.current.getMap().flyTo({center: [meta.lng, meta.lat]});
                props.setViewport({latitude: meta.lat, longitude: meta.lng});
            }
        }
    }, [props.station, stationMeta, mapRef.current]);

    const colourMaps = useMemo(() => {
        const f = router.query.fromColour || defaultFromColour;
        const t = router.query.toColour || defaultToColour;
        return _zip(
            progression(f, t, 0), // R
            progression(f, t, 1), // G
            progression(f, t, 2), // B
            progression(f, t, 3)
        ); // A
    }, [router.isReady, router.query.fromColour, router.query.toColour]);

    const colourise = (v: number): [number, number, number, number] => {
        return colourMaps[Math.trunc(((v / 255) * steps) % steps)];
    };

    //
    // Generate the deckGL layers
    // We don't need to do this unless parameters change
    const {layers} = useMemo(
        () =>
            makeLayers(
                props.station,
                props.file,
                props.setStation, //
                router.query.highlightStations == '0' ? [] : props.highlightStations,
                props.visualisation,
                map2d,
                onClick,
                details.locked ? details?.h : null,
                setLoaded,
                () => isLoaded,
                colourise,
                fromColour.toString() + toColour.toString(),
                props.env
            ),
        [props.station, props.file, map2d, props.visualisation, props.highlightStations, props.details.locked, props.details.h?.[0], props.details.h?.[1], fromColour, toColour]
    );

    let attribution = `<a href="//www.glidernet.org/">Data from OGN</a> | `;
    if (props.station) {
        attribution += `Currently showing station ${props.station}`;
    } else {
        attribution += `Currently showing all stations`;
    }

    //
    // Generate tooltip text, and update side panel
    const useToolTipSelection = useCallback(
        ({index: i, layer, picked}) => {
            if (props.details?.locked) {
                return null;
            }
            if (!picked) {
                props.setHighlightStations([]);
                props.setDetails({});
                return null;
            }

            const object = getObjectFromIndex(i, layer);
            if (!object) {
                return null;
            }

            // Add highlight to all stations that are referenced by the point
            if (object.type === 'hexagon' && object.i != props.details.i) {
                props.setDetails(object);

                if (!props.tooltips && object.s && stationMeta) {
                    const parts = object.s.split(',');
                    props.setHighlightStations(
                        _reduce(
                            parts,
                            (acc, x) => {
                                const sid = parseInt(x, 36) >> 4;
                                const index = _sortedIndexOf(stationMeta?.id, sid);
                                if (index != -1 && !isNaN(stationMeta?.lat[index])) {
                                    acc.push(index);
                                }
                                return acc;
                            },
                            [] as HighlightStationIndicies
                        )
                    );
                }
            }

            if (props.tooltips || object.type === 'station') {
                const html = ReactDOMServer.renderToStaticMarkup(
                    <CoverageDetailsToolTip
                        details={object} //
                        station={props.station}
                    />
                );
                return {html};
            }
        },
        [props.tooltips, props.details, props.details.i, props.details.locked, props.station, props.file, stationMeta]
    );

    const loadingLayer = isLoaded ? (
        <div className="progress-bar">
            {(isLoaded * 100).toFixed(0)}%
            <div className="progress" style={{transform: `scaleX(${isLoaded})`}} />
        </div>
    ) : null;

    // We keep our saved viewstate up to date in case of re-render
    const onViewStateChange = useCallback(({viewState}) => {
        props.setViewport(viewState);
    }, []);

    useEffect(() => {
        mapRef?.current?.resize();
    }, [props.dockSplit]);
    //                <div style={{width: `${((1 - dockSplit) * 100).toFixed(0)}vw`, height: '100vh'}}>

    const viewOptions = map2d ? {minPitch: 0, maxPitch: 0, pitch: 0} : {minPitch: 0, maxPitch: 85, pitch: 70};
    return (
        <>
            <Map
                mapStyle={'mapbox://styles/' + (router.query.mapStyle || defaultBaseMap)}
                //        mapStyle={'mapbox://styles/ifly7charlie/clmbzpceq01au01r7abhp42mm'}
                ref={mapRef}
                initialViewState={{...props.viewport, ...viewOptions}}
                onMove={onViewStateChange}
                mapboxAccessToken={props.env.NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN}
                reuseMaps={true}
                attributionControl={false}
            >
                <DeckGLOverlay
                    getTooltip={useToolTipSelection}
                    onClick={onClick}
                    layers={layers} //
                    interleaved={true}
                />
                {router.query?.airspace == '1' && airspaceKey ? (
                    <>
                        <Source id="airspace" type="raster" tiles={[`https://api.tiles.openaip.net/api/data/openaip/{z}/{x}/{y}.png?apiKey=${airspaceKey}`]} maxzoom={14} tileSize={256} />
                        <Layer type={'raster'} source={'airspace'} minzoom={0} maxzoom={14} />
                    </>
                ) : null}
                <AttributionControl key={props.station} customAttribution={attribution} style={attributionStyle} />
                <ScaleControl position="bottom-left" />
                <NavigationControl showCompass showZoom position="bottom-left" />
            </Map>
            {loadingLayer}
        </>
    );
}
//
// Produce the array of colours for the display
function progression(f, t, offset) {
    const start = parseInt(f.slice(offset * 2, offset * 2 + 2) || 'ff', 16);
    const end = parseInt(t.slice(offset * 2, offset * 2 + 2) || 'ff', 16);
    const step = (end - start) / steps;
    return Array(steps)
        .fill(0)
        .map((_e, index) => start + Math.round(index * step));
}

const attributionStyle = {
    right: 0,
    bottom: 0,
    fontSize: '13px'
};

/*
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
*/
