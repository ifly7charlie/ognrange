import React, {useState, useCallback, useMemo} from 'react';
import DeckGL from '@deck.gl/react';
import {MapView, GeoJsonLayer,PathLayer,TextLayer,IconLayer} from '@deck.gl/layers';
import {FlyToInterpolator} from '@deck.gl/core'
import {StaticMap,Source,Layer} from 'react-map-gl';
import { LngLat } from 'mapbox-gl';
import { MercatorCoordinate } from 'mapbox-gl';
import {MapboxLayer} from '@deck.gl/mapbox'
import { Matrix4 } from "@math.gl/core";

import { useTaskGeoJSON, Spinner, Error } from './loaders.js';

import { gapLength } from '../constants.js';

// Height/Climb helpers
import { displayHeight, displayClimb } from './displayunits.js';

// Figure out where the sun should be
import SunCalc from 'suncalc';

// For displaying rain radar
import {AttributionControl} from 'react-map-gl';
import { RadarOverlay } from './rainradar';

import { point } from '@turf/helpers';
import bearing from '@turf/bearing';
import nearestPointOnLine from '@turf/nearest-point-on-line';
import polygonToLine from '@turf/polygon-to-line';

import _map from 'lodash/map'
import _reduce from 'lodash/reduce'
import _find from 'lodash/find';

//
// Responsible for generating the deckGL layers
//
function makeLayers( props, map2d ) {
	if( ! props.trackData ) {
		return [];
	}

	// Add a layer for the recent points for each pilot
	let layers = _reduce( props.trackData,
						  (result,p,compno) => {
							  if( compno == props.selectedCompno ) {
								  return result;
							  }
							  result.push( new OgnPathLayer(
								  { id: compno,
									compno: compno,
									data: { length: 1,
											startIndices: new Uint32Array([0,p.recentIndices[1]-p.recentIndices[0]]),
											timing:p.t.subarray(p.recentIndices[0],p.recentIndices[1]),
											climbRate:p.climbRate.subarray(p.recentIndices[0],p.recentIndices[1]),
											agl: p.agl.subarray(p.recentIndices[0],p.recentIndices[1]),
											attributes: {
												getPath: { value: p.positions.subarray(p.recentIndices[0]*3,p.recentIndices[1]*3),
														   size: map2d ? 2 : 3,
														   stride: map2d ? 4*3 : 0}
											}
									},
									_pathType: 'open',
									positionFormat: map2d ? 'XY' : 'XYZ',
									getWidth: 5,
									getColor: [220,220,220,128],
									jointRounded: true,
									fp64: false,
									widthMinPixels: 2,
									billboard: true,
									onClick: (i) => { props.setSelectedCompno(compno); },
									updateTriggers: {
										getPath: p.posIndex
									},
									pickable: true,
									tt: true
								  }));
							  return result;
						  }, []);

	//
	// Generate the labels data, this is fairly simple and is extracted from the positions
	// data set rather than pilots so that the marker always aligns with the tracking points
	// we are adding more data so we get a nice tool tip, text colour is determined by how old
	// the point is
	const data = _map( props.trackData, (p) => {
		return { name: p.compno,
				 compno: p.compno,
				 climbRate: p.climbRate[p.posIndex-1],
				 agl: p.agl[p.posIndex-1],
				 alt: p.positions[(p.posIndex-1)*3+2],
				 time: p.t[p.posIndex-1],
				 coordinates: p.positions.subarray((p.posIndex-1)*3,(p.posIndex)*3) }
	});
	layers.push( new TextLayer({ id: 'labels',
								 data: data,
								 getPosition: d => d.coordinates,
								 getText: d => d.name,
								 getTextColor: d => props.t - d.time > gapLength ? [ 192, 192, 192 ] : [ 0, 0, 0 ],
								 getTextAnchor: 'middle',
								 getSize: d => d.name == props.selectedCompno ? 20 : 16,
								 pickage: true,
								 background: true,
								 backgroundPadding: [ 3, 3, 3, 0 ],
								 onClick: (i) => { props.setSelectedCompno(i.object?.name||''); },
								 pickable: true
							   }));

	//
	// If there is a selected pilot then we need to add the full track for that pilot
	// 
	if( props.selectedCompno && props.trackData[ props.selectedCompno ] ) {
		const p = props.trackData[ props.selectedCompno ];
		layers.push( new OgnPathLayer(
			{ id: 'selected',
			  compno: props.selectedCompno,
			  data: { length: p.segmentIndex, startIndices:p.indices, timing:p.t,
					  climbRate:p.climbRate, agl: p.agl,
					  attributes: {
						  getPath: { value: p.positions,
									 size: map2d ? 2 : 3,
									 stride: map2d ? 4*3 : 0}
					  },
			  },
			  _pathType: 'open',
			  positionFormat: map2d ? 'XY' : 'XYZ',
			  getWidth: () => 5,
			  billboard: true,
			  getColor: [255,0,255,192],
			  jointRounded: true,
			  widthMinPixels: 3,
			  fp64: false,
			  pickable: true,
			  tt: true,
			  updateTriggers: {
				  getPath: p.posIndex
			  }
			}));
	}

	return layers;
}

export default function MAP(props) {

	// Map display style
	const map2d = props.options.mapType > 1;
	const mapStreet = props.options.mapType % 2;

	// DeckGL layer generation
    const { taskGeoJSON, isTLoading, Terror } = useTaskGeoJSON(props.vc);
	const layers = useMemo( _ => makeLayers(props, map2d),
							[ props.station, map2d ]);

	const onMapLoad = useCallback(evt => {
		if( ! map2d ) {
			const map = evt.target;
			map.setTerrain({source: 'mapbox-dem'});
		}
	}, [map2d]);
	

    // Do we have a loaded set of details?
    const valid = !( isTLoading || Terror );

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
		
        const map = props.mapRef?.current?.getMap()
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

	return (
		<DeckGL
			viewState={props.viewport}
			controller={{scrollZoom: { smooth: false }, touchRotate: true}}
			onViewStateChange={ e => onViewStateChange(e) }
			getTooltip={toolTip}
			layers={layers}>
			
				<StaticMap          mapboxApiAccessToken={process.env.NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN}
									mapStyle={mapStreet ? "mapbox://styles/mapbox/cjaudgl840gn32rnrepcb9b9g" : "mapbox://styles/ifly7charlie/cksj3g4jgdefa17peted8w05m" }
									onLoad={onMapLoad}
									ref={props.mapRef}
									>
					{props.selectedPilot&&props.selectedPilot.scoredGeoJSON?
					 <Source type="geojson" data={props.selectedPilot.scoredGeoJSON} key={props.selectedPilot.compno}>
						 <Layer {...scoredLineStyle}/>
					 </Source>:null}
					{! map2d &&
					 <>
						 <Source
							 id="mapbox-dem"
							 type="raster-dem"
							 url="mapbox://mapbox.mapbox-terrain-dem-v1"
							 tileSize={512}
							 maxzoom={14}
						 />
						 <Layer {...skyLayer} />
						 <Layer {...hillshade} />
					 </>
					}
					
					{attribution}
					{radarOverlay.layer}
				</StaticMap>
		</DeckGL>
  );
}

