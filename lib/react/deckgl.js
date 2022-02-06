import React, {useState, useCallback, useMemo, useRef} from 'react';


//import dynamic from 'next/dynamic'
//const DeckGL  = dynamic(() => import( '@deckgl/react' ),
  //                       { loading: () => <b>Loading</b>});

import DGL from '@deck.gl/react';
import { IconLayer, MapView } from '@deck.gl/layers';
import { H3HexagonLayer } from '@deck.gl/geo-layers';
//import { PickableHexagonLayer } from './pickablehexagonlayer.js'
import { FlyToInterpolator } from '@deck.gl/core'
import { StaticMap,Source,Layer } from 'react-map-gl';
import { LngLat } from 'mapbox-gl';
import { MercatorCoordinate } from 'mapbox-gl';
import {MapboxLayer} from '@deck.gl/mapbox'
import { Matrix4 } from "@math.gl/core";

import {ArrowLoader} from '@loaders.gl/arrow'
import {JSONLoader} from '@loaders.gl/json'

import _map from 'lodash.map';
import _chunk from 'lodash.chunk';
import _keyby from 'lodash.keyby';

import { splitLongToh3Index } from 'h3-js';

const formats = {
	'c': (f) => `${f} points`,
	'avgSig': (f) => `avg ${f} dB`,
	'maxSig': (f) => `max ${f} dB`,
	'minAlt': (f) => `min ${f}m altitude`,
	'minAgl': (f) => `min ${f}m agl`,
};

const scaling = {
	'c': (f) => `${f} points`,
	'avgSig': (f) => `avg ${f} dB`,
	'maxSig': (f) => `max ${f} dB`,
	'minAlt': (f) => `min ${f}m altitude`,
	'minAgl': (f) => `min ${f}m agl`,
};

let stationMeta = undefined;

//
// Responsible for generating the deckGL layers
//
function makeLayers( station, setStation, visualisation, map2d ) {

	const ICON_MAPPING = {
		marker: {x: 0, y: 0, width: 128, height: 128, mask: true}
	};

	// Add a layer for the recent points for each pilot
	let layers = [
		new H3HexagonLayer({
			id: station||'global',
			data: '/data/'+(station||'global')+'.arrow',
			loaders: [ ArrowLoader ],
//			loadOptions: {
//				arrow: {
//				}
			//			}
			dataTransform: (d) => {
				const chunked = _chunk(d.h3,2);
				const stringed = _map( chunked, (p) => { return splitLongToh3Index(p[0],p[1])});
				return _map( stringed, (v,i) => { return {
					h: v,
					a: d.avgSig[i],
					b:d.minAlt[i],
					c:d.count[i],
					'd':d.minAltSig[i],
					e:d.maxSig[i],
					f:d.avgCrc[i],
					s:d.stations[i]
					}})
			},
			visualisation: visualisation,
			pickable: true,
			wireframe: false,
			filled: true,
			extruded: false,
			elevationScale: 0,
			getHexagon: (d) => d.h,
			getFillColor: d => [255, (1 - d.e/ 255) * 255, 0, 192],
			getElevation: d => 0,
		}),

		new IconLayer({
			id: 'icon-layer',
			data: '/data/stations.json',
			loaders: [ JSONLoader ],
			dataTransform: (d) => {
				stationMeta = _keyby(d,'id');
				return d;
			},
			// allow hover and click
			pickable: true,
			onClick: (i) => { setStation( i.object?.station||''); },
			// What icon to display
			iconAtlas: 'https://raw.githubusercontent.com/visgl/deck.gl-data/master/website/icon-atlas.png',
			iconMapping: ICON_MAPPING,
			getIcon: d => 'marker',
			// How big
			sizeScale: 500,
			sizeUnits: 'meters',
			getSize: d => 5,
			// where and what colour
			getPosition: d => [d.lng,d.lat],
			getColor: d => [0, 0, 255]
		})

	];
	return layers;
}

export default function CoverageMap(props) {

	// For remote updating of the map
    const mapRef = useRef(null);

	// Map display style
	const map2d = props.options.mapType > 1;
	const mapStreet = props.options.mapType % 2;

	const layers = useMemo( _ => makeLayers(props.station, props.setStation, props.visualisation, map2d),
							[ props.station, map2d, props.visualisation ]);

	const onMapLoad = useCallback(evt => {
		if( ! map2d ) {
			const map = evt.target;
			map.setTerrain({source: 'mapbox-dem'});
		}
	}, [map2d]);
	

	// Update the view and synchronise with mapbox
	const onViewStateChange = ({ viewState }) => {
		if( map2d ) {
			viewState.minPitch = 0;
			viewState.maxPitch = 0;
		}
		else {
			viewState.minPitch = 0;
			viewState.maxPitch = 85;
		}
		
        const map = mapRef?.current?.getMap()
        if (map && map.transform.elevation && ! map2d ) {
            const mapbox_elevation = map.transform.elevation.getAtPoint(MercatorCoordinate.fromLngLat(map.getCenter()));
			//const mapbox_elevation = map.transform.elevation.getAtPoint(MercatorCoordinate.fromLngLat(new LngLat(viewState.longitude, viewState.latitude)));
            props.setViewport({
                ...viewState,
                ...{ position: [0, 0, mapbox_elevation] }
            });
        } else {
            props.setViewport(viewState);
        }
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

	// "mapbox://styles/mapbox/cjaudgl840gn32rnrepcb9b9g"
	return (
		<div >
			<DGL
				viewState={props.viewport}
				controller={{scrollZoom: { smooth: false }, touchRotate: true}}
				onViewStateChange={ e => onViewStateChange(e) }
				getTooltip={toolTip}
				layers={layers}>
				
				<StaticMap  mapboxApiAccessToken={process.env.NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN}
							mapStyle={mapStreet ? "mapbox://styles/mapbox/cj44mfrt20f082snokim4ungi" : "mapbox://styles/ifly7charlie/cksj3g4jgdefa17peted8w05m" }
							onLoad={onMapLoad}
							ref={mapRef}
				>
					{! map2d &&
					 <>
						 <Source
							 id="mapbox-dem"
							 type="raster-dem"
							 url="mapbox://mapbox.mapbox-terrain-dem-v1"
							 tileSize={512}
							 maxzoom={14}
						 />
						 <Layer {...hillshade} />
					 </>
					}
				</StaticMap>
			</DGL>
		</div>
	);
}


//
// Figure out what tooltip to display
//
function toolTip({object,picked,layer}) {
	if( ! picked ) {
		return null;
	}
	if( object ) {
		let response = '';
		if( object.station ) {
			response = `<b>${object.station}</b>`;
		}

		if( object.status ) {
			response += `<br/><div style="width:350px; overflow-wrap:anywhere; font-size:small">${object.status}</div>`;
		}

		if( object.h ) {
			//			stations = _map( object.s
			const stationList = stationMeta?_map(object.s.split(','),(x)=>(stationMeta[parseInt(x,16)]?.station||'?')):'*loading*';
			
			response = `<b>Signal</b><br/>Average: ${object.a} dB, Max: ${object.e} dB<br/>` +
					   `<hr/><b>Lowest Point</b><br/>${object.d} dB @ ${object.b} m<br/>` +
					   `<hr/>Avg CRC errors: ${object.f}<br/>`+
					   `<hr/>Number of packets: ${object.c}<br/>`+
					   `<hr/>station list: ${stationList}`;
		}

		return { html: response };
	}
	else if( layer && layer.props.tt == true ) {
		return layer.id;
	}
	else {
		return null;
	}
}
