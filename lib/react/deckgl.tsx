import React, {useState, useCallback, useMemo, useRef, useEffect} from 'react';
import {MapboxOverlay, MapboxOverlayProps} from '@deck.gl/mapbox';

import Map, {Source, Layer, LayerProps, useControl, NavigationControl, ScaleControl} from 'react-map-gl';

import {IconLayer, ColumnLayer} from '@deck.gl/layers';
import {H3HexagonLayer} from '@deck.gl/geo-layers';

import {AttributionControl} from 'react-map-gl';

import {ArrowLoader} from '@loaders.gl/arrow';
import {JSONLoader} from '@loaders.gl/json';

import ReactDOMServer from 'react-dom/server';

import {useRouter} from 'next/router';

import {progressFetch} from './progressFetch';

import {map as _map, reduce as _reduce, chunk as _chunk, zip as _zip, keyBy as _keyby, filter as _filter, find as _find, debounce as _debounce} from 'lodash';

import {CoverageDetails} from './CoverageDetails';

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
        count: (f, i) => colourise(Math.log2(f.count[i]) * (254 / maxCount)),
        avgSig: (f, i) => colourise(Math.min(f.avgSig[i] * 3, 254)),
        maxSig: (f, i) => colourise(Math.min(f.maxSig[i] * 1.3, 254)),
        minAlt: (f, i) => colourise(Math.min(f.minAlt[i] / 20, 254)),
        minAgl: (f, i) => colourise(Math.min(f.minAgl[i] / 20, 254)),
        minAltSig: (f, i) => colourise(f.minAltSig[i]),
        avgCrc: (f, i) => colourise(255 - Math.min(f.avgCrc[i] * 5, 254)),
        avgGap: (f, i) => colourise(Math.log2(Math.max(f.avgGap[i], 4) - 3) * 31.75), // it shouldn't be less than 3 seconds so offset but no log2(0) => inf
        expectedGap: (f, i) => colourise(Math.log2((f.expectedGap?.[i] ?? f.avgGap?.[i]) || 1) * 31.75),
        stations: (f, i) => colourise(Math.min(Math.log2(f.numStations?.[i] - 1 || 0) * 32, 254)),
        primaryStation: (f, i) => {
            const s = f.stations?.[i];
            return s ? [parseInt(s.slice(0, 1), 36) * 7, parseInt(s.slice(1, 2), 36) * 7, parseInt(s.slice(2, 3), 36) * 7, 128] : [255, 0, 255, 128];
        }
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
        data: `${NEXT_PUBLIC_DATA_URL}${station || 'global'}/${station || 'global'}.${file || 'year'}.arrow`,
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
            data: `${NEXT_PUBLIC_DATA_URL}stations.json`,
            loaders: [JSONLoader],
            dataTransform: (d) => {
                if (d) {
                    setStationMeta(_keyby(d, 'id'));
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

function getObjectFromIndex(i, layer) {
    const d = layer?.props?.data.d;
    return d
        ? {
              h: [d.h3lo[i], d.h3hi[i]], //layer.props.data.h3s[i],
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
              t: d.numStations?.[i] ?? d.stations?.[i]?.split(',')?.length ?? 0,
              length: d.avgSig.length
          }
        : null;
}

export function CoverageMap(props: {
    //
    mapType: number;
    setDetails: (d: any) => void;
    setHighlightStations: (h: any) => void;
    station?: string;
    visualisation: string;
    setStation: (s: any) => void;
    highlightStations?: any;
    file?: string;
    tooltips: boolean;
    viewport: any;
    setViewport: Function;
}) {
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
            const object = getObjectFromIndex(o.index, o.layer);
            if (lockedH3 && (object?.h == lockedH3 || !object?.h)) {
                props.setDetails({locked: false});
                setLockedH3(null);
            } else {
                setLockedH3(object?.h);
                props.setDetails({...object, locked: true});
            }
        },
        [lockedH3]
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
                router.query.highlightStations == '0' ? '' : props.highlightStations,
                props.visualisation,
                map2d,
                onClick,
                lockedH3,
                setLoaded,
                () => isLoaded,
                colourise,
                fromColour.toString() + toColour.toString()
            ),
        [props.station, props.file, map2d, props.visualisation, props.highlightStations, lockedH3, fromColour, toColour]
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
            if (lockedH3) {
                return null;
            }
            if (!picked) {
                if (props.highlightStations) {
                    props.setHighlightStations({});
                }
                props.setDetails(null);
                return null;
            }

            const object = getObjectFromIndex(i, layer);

            if (object) {
                props.setDetails(object);

                if (!props.tooltips && object.s) {
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
                        file={props.file}
                    />
                );
                return {html};
            }
        },
        [props.tooltips, lockedH3, props.highlightStations]
    );

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

    // We keep our saved viewstate up to date in case of re-render
    const onViewStateChange = useCallback(({viewState}) => {
        props.setViewport(viewState);
    }, []);

    const viewOptions = map2d ? {minPitch: 0, maxPitch: 0, pitch: 0} : {minPitch: 0, maxPitch: 85, pitch: 70};
    return (
        <>
            <Map
                mapStyle={'mapbox://styles/' + (router.query.mapStyle || defaultBaseMap)}
                //        mapStyle={'mapbox://styles/ifly7charlie/clmbzpceq01au01r7abhp42mm'}
                ref={mapRef}
                initialViewState={{...props.viewport, ...viewOptions}}
                onMove={onViewStateChange}
                mapboxAccessToken={process.env.NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN}
                reuseMaps={true}
                attributionControl={false}
            >
                <DeckGLOverlay
                    getTooltip={useToolTipSelection}
                    onClick={onClick}
                    layers={layers} //
                    interleaved={true}
                />
                {router.query?.airspace == '1' ? (
                    <>
                        <Source id="airspace" type="raster" tiles={['https://api.tiles.openaip.net/api/data/openaip/{z}/{x}/{y}.png?apiKey=760e6a3ccde9c9f277f3de723169934b']} maxzoom={14} tileSize={256} />
                        <Layer {...airspaceLayer} minzoom={0} maxzoom={14} />
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
