import next from 'next'
import { useRouter } from 'next/router'
import Head from 'next/head'

// What do we need to render the bootstrap part of the page
import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import Collapse from 'react-bootstrap/Collapse';
import Navbar from 'react-bootstrap/Navbar'
import Nav from 'react-bootstrap/Nav'
import NavDropdown from 'react-bootstrap/NavDropdown'

import { useState } from 'react';

// Helpers for loading contest information etc
import { Nbsp, Icon } from '../lib/react/htmlhelper.js';

// And connect to websockets...
import CoverageMap from '../lib/react/deckgl.js';
import cookies from 'next-cookies';

import Router from 'next/router'


export function IncludeJavascript() {
    return (
        <>
            <link rel="stylesheet" href="/bootstrap/css/font-awesome.min.css"/>
            <link href='//api.mapbox.com/mapbox-gl-js/v1.11.0/mapbox-gl.css' rel='stylesheet' />
        </>
    );
}


// Requires: classes, link, contestname, contestdates

export function Menu( props ) {

	// Try and extract a short form of the name, only letters and spaces stop at first number
    return (
        <>
            <Navbar bg="light" fixed="top">
                <Nav fill variant="tabs" defaultActiveKey={props.vc} style={{width:'100%'}}>
					<Nav.Item key="sspot" style={{paddingTop:0,paddingBottom:0}}>
						<Nav.Link href="http://glidernet.org" className="d-md-none">
							OGN Range (Beta)
						</Nav.Link>
						<Nav.Link href='#' className="d-none d-md-block"  style={{paddingTop:0,paddingBottom:0}}>
							{props.station||'All Stations'}
						</Nav.Link>
					</Nav.Item>
					<Nav.Item key="settings">
						<Nav.Link href='#' key='navlinksettings' eventKey='settings'
								  onClick={() => { Router.push(props.override ? '/settings?mapType='+props.override : '/settings', undefined, {shallow:true}); }}>
						</Nav.Link>
					</Nav.Item>
				</Nav>
            </Navbar>
            <br style={{clear:'both'}}/>
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
    let { station, visualisation, mapType } = router.query;
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
        latitude: 51.87173333,
        longitude: 	-0.551233333,
        zoom: 6,
		minZoom: 3.5,
		maxZoom: 10,
        bearing: 0,
		minPitch: 0,
		maxPitch: 85,
		altitude: 1.5,
        pitch: (! (props.options.mapType % 2)) ? 70 : 0
    });

	return (
        <>
            <Head>
                <title>OGN Range (Beta)</title>
				<meta name='viewport' content='width=device-width, minimal-ui'/>
				<IncludeJavascript/>
            </Head>
            <Menu station={station} setStation={setStation} override={mapType} visualisation={visualisation}/>
 			<CoverageMap station={station} setStation={setStation} visualisation={visualisation}
						 viewport={viewport} setViewport={setViewport}
						 options={props.options} setOptions={props.setOptions}>
			</CoverageMap>
		</>
    );
}

export async function getServerSideProps(context) {
  return {
      props: { options: { mapType: 3 }}
  };
}


