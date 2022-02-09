import React, {useState, useCallback, useMemo, useRef} from 'react';


//import dynamic from 'next/dynamic'
//const DeckGL  = dynamic(() => import( '@deckgl/react' ),
  //                       { loading: () => <b>Loading</b>});

import DGL from '@deck.gl/react';
import { IconLayer, MapView, ColumnLayer } from '@deck.gl/layers';
import { H3HexagonLayer } from '@deck.gl/geo-layers';
//import { PickableHexagonLayer } from './pickablehexagonlayer.js'
import { FlyToInterpolator } from '@deck.gl/core'
import { StaticMap,Source,Layer } from 'react-map-gl';
import { LngLat } from 'mapbox-gl';
import { MercatorCoordinate } from 'mapbox-gl';
import {MapboxLayer} from '@deck.gl/mapbox'
import {AttributionControl} from 'react-map-gl';
import { Matrix4 } from "@math.gl/core";

import {ArrowLoader} from '@loaders.gl/arrow'
import {JSONLoader} from '@loaders.gl/json'

import _map from 'lodash.map';
import _reduce from 'lodash.reduce';
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

const visualisationFunctions = {
	'count': (f) => Math.max(f.c/100,255),
	'avgSig': (f) => f.a*4,
	'maxSig': (f) => f.e*4,
	'minAlt': (f) => Math.max(f.b/20,255),
	'minAgl': (f) => Math.max(f.g/20,255),
	'minAltSig': (f) => f.d*4,
	'avgCrc': (f) => f.f*100,
	'stations': (f) => f.t*10,
};

let stationMeta = undefined;

//
// Responsible for generating the deckGL layers
//
function makeLayers( station, setStation, highlightStations, visualisation, map2d ) {

	const ICON_MAPPING = {
		marker: {x: 0, y: 0, width: 128, height: 128, mask: true}
	};

	// Colouring and display options
	let getStationColor = (d) => highlightStations[d.id] ? [255,16,240] : [0, 0, 192];
	if( station ) {
		getStationColor = (d) => d.station == station ? [255,16,240] : [0,0,255];
	}
	let getStationSize = (d) => ( highlightStations[d.id] || d.station == station ? 7 : 5 )

	// How do we choose what to show in the hexagon layer
	const visualisationFunction = visualisationFunctions[visualisation]||visualisationFunctions['avgSig'];


	// are we showing circles
	const locations = _map( Object.keys(highlightStations), (f) => (stationMeta?.[f]?.lat&&stationMeta?.[f]?.lng) ? stationMeta?.[f] : null );
	const l10k = _map( [10,20,30], (r) => locations ? new ColumnLayer({
		id: 'stationk'+r,
		data: locations,
		diskResolution: 50,
		radius: r*1000,
		radiusUnits: 'meters',
		extruded: false,
		filled: false,
		stroked: true,
		pickable: false,
		elevationScale: 1,
		getPosition: d => [d.lng,d.lat],
		getLineColor: [255,16,240,128],
		getLineWidth: 10,
		getElevation: 1000,
		getFillColor: [, 150],
		lineWidthMinPixels: 2,
		lineWidthMaxPixels: 5,
	}) : null);

	console.log( locations, highlightStations );

	
	
	// Add a layer for the recent points for each pilot
	let layers = [
		...l10k,
		
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
					b: d.minAlt[i],
					c: d.count[i],
					'd':d.minAltSig[i],
					e: d.maxSig[i],
					f: d.avgCrc[i],
					g: d.minAgl[i],
					s: d.stations?.[i],
					t: d.scount?.[i]
					}})
			},
			pickable: true,
			wireframe: false,
			filled: true,
			extruded: false,
			elevationScale: 0,
			getHexagon: (d) => d.h,
			getFillColor: d => [255, (1 - visualisationFunction(d)/ 255) * 255, 0, 192],
			getElevation: d => 0,
			updateTriggers: {
				getColor: [ visualisation ],
			}
		}),

		new IconLayer({
			id: 'icon-layer',
			data: '/data/stations.json',
			loaders: [ JSONLoader ],
			dataTransform: (d) => {
				if( d ) { stationMeta = _keyby(d,'id'); }
				return d;
			},
			// allow hover and click
			pickable: true,
			onClick: (i) => { setStation( i.object?.station||'');},
			// What icon to display
			iconAtlas: 'https://raw.githubusercontent.com/visgl/deck.gl-data/master/website/icon-atlas.png',
			iconMapping: ICON_MAPPING,
			getIcon: d => 'marker',
			// How big
			sizeScale: 500,
			sizeMinPixels: 5,
			sizeMaxPixels: 50,
			sizeUnits: 'meters',
			getSize: getStationSize,
			// where and what colour
			getPosition: d => [d.lng,d.lat],
			getColor: getStationColor,
			updateTriggers: {
				getColor: [ station, highlightStations ],
				getSize: [ station, highlightStations ]
			}
		}),

	];
	return layers;
}

export default function CoverageMap(props) {

	// For remote updating of the map
    const mapRef = useRef(null);

	// For highlight which station we are talking about
	const defaultHighlight = {};
	if( props.station ) { defaultHighlight[props.station]=1 }
	const [highlightStations, setHighlightStations] = useState(defaultHighlight)

	// Map display style
	const map2d = props.options.mapType > 1;
	const mapStreet = props.options.mapType % 2;

	const layers = useMemo( _ => makeLayers(props.station, props.setStation, highlightStations, props.visualisation, map2d),
							[ props.station, map2d, props.visualisation, highlightStations ]);

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

	let attribution =  `<a href="//www.glidernet.org/">Data from OGN</a> | `;
	if( props.station ) {
		attribution += `Currently showing station ${props.station}`;
	}
	else {
		attribution += `Currently showing all stations`;
	}
		


	// "mapbox://styles/mapbox/cjaudgl840gn32rnrepcb9b9g"
	return (
		<div >
			<DGL
				viewState={props.viewport}
				controller={{scrollZoom: { smooth: false }, touchRotate: true}}
				onViewStateChange={ e => onViewStateChange(e) }
				getTooltip={(x)=>toolTip(x,highlightStations,setHighlightStations)}
				layers={layers}>
				
				<StaticMap  mapboxApiAccessToken={process.env.NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN}
							mapStyle={mapStreet ? "mapbox://styles/mapbox/cj44mfrt20f082snokim4ungi" : "mapbox://styles/ifly7charlie/cksj3g4jgdefa17peted8w05m" }
							onLoad={onMapLoad}
							ref={mapRef}
							attributionControl={false}>
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
					<AttributionControl key={props.station}
										customAttribution={attribution} style={attributionStyle}/>
				</StaticMap>
			</DGL>
		</div>
	);
}


//
// Figure out what tooltip to display
//
function toolTip({object,picked,layer}, highlightStations, setHighlightStations ) {
	if( ! picked ) {
		if( highlightStations ) {
			setHighlightStations({})
		}
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
			let stationList = undefined;
			if( stationMeta && object.s ) {
				const parts = object.s.split(',')
				stationList = _map(parts,(x)=>(stationMeta[parseInt(x,16)]?.station||'?'))
				setHighlightStations( _reduce(parts,(acc,x)=>{acc[parseInt(x,16)]=1; return acc;}, {} ));
			}
			response = `<b>Signal</b><br/>Average: ${object.a} dB, Max: ${object.e} dB<br/>` +
					   `<hr/><b>Lowest Point</b><br/>${object.d} dB @ ${object.b} m (${object.g} m agl)<br/>` +
					   `<hr/>Avg CRC errors: ${object.f}<br/>`+
					   `<hr/>Number of packets: ${object.c}<br/>`+
						(stationList ? 
						 `<hr/>station list: ${stationList}` : '');
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

const attributionStyle= {
	right: 0,
	bottom: 0,
	fontSize: '13px'
};
