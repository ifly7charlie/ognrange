import next from 'next'
import { useRouter } from 'next/router'
import Head from 'next/head'

import { useState, useRef } from 'react';

// Helpers for loading contest information etc
import { Nbsp, Icon } from '../lib/react/htmlhelper.js';

// And connect to websockets...
import CoverageMap from '../lib/react/deckgl.js';

import Router from 'next/router';

import _debounce from 'lodash.debounce';

export function IncludeJavascript() {
    return (
        <>
            <link rel="stylesheet" href="/bootstrap/css/font-awesome.min.css"/>
            <link href='//api.mapbox.com/mapbox-gl-js/v1.11.0/mapbox-gl.css' rel='stylesheet' />
        </>
    );
}


//
// Main page rendering :)
export default function CombinePage( props ) {

    // First step is to extract the class from the query, we use
    // query because that stops page reload when switching between the
    // classes. If no class is set then assume the first one
    const router = useRouter()
    let { station, visualisation, mapType, lat, lng, zoom } = router.query;
	if( mapType ) {
		props.options.mapType = parseInt(mapType);
	}
	if( ! visualisation ) {
		visualisation = 'avgSig';
	}

	// Update the station by updating the query url preserving all parameters but station
	function setStation( newStation ) {
		if( station === newStation ) {
			newStation = '';
		}
		router.push( { pathname: '/', query: { ...router.query, 'station': newStation }}, undefined, { shallow: true });
	}

	
	// What the map is looking at
    const [viewport, setViewport] = useState({
        latitude: parseFloat(lat||0)||51.87173333,
        longitude: parseFloat(lng||0)||-0.551233333,
        zoom: parseFloat(zoom||0)||6,
		minZoom: 2.5,
		maxZoom: 13,
        bearing: 0,
		minPitch: 0,
		maxPitch: 85,
		altitude: 1.5,
        pitch: (! (props.options.mapType % 2)) ? 70 : 0
    });

	// Debounced updating of the URL when the viewport is changed, this is a performance optimisation
	function updateUrl(existing,vs,s) {
		router.replace( { pathname: '/',
						  query: { ...existing,
								   'lat': (vs.latitude).toFixed(5), 'lng': (vs.longitude).toFixed(5), 'zoom': (vs.zoom).toFixed(1) }
		}, undefined, { shallow: true,  });
	}
	const delayedUpdate = useRef(_debounce((existing,vs,s) => updateUrl(existing,vs,s), 300)).current;
	
	// Synchronise it back to the url
	function setViewportUrl(vs) {
		delayedUpdate(router.query,vs,station)
		setViewport(vs)
	}
	return (
			<>
				<Head>
					<title>OGN Range (Beta)</title>
					<meta name='viewport' content='width=device-width, minimal-ui'/>
					<IncludeJavascript/>
				</Head>

				<div>
 					<CoverageMap station={station} setStation={setStation} visualisation={visualisation}
								 viewport={viewport} setViewport={setViewportUrl}
								 options={props.options} setOptions={props.setOptions}>
					</CoverageMap>
					
				</div>
			</>
    );
}
export async function getServerSideProps(context) {
  return {
      props: { options: { mapType: 3 }}
  };
}


