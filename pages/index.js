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
import { Spinner, Error } from '../lib/loaders.js';
import { Nbsp, Icon } from '../lib/react/htmlhelper.js';

// And connect to websockets...
import { MAP } from '../lib/deckgl.js';

import Router from 'next/router'


function IncludeJavascript() {
    return (
        <>
            <link rel="stylesheet" href="/bootstrap/css/font-awesome.min.css"/>
            <link href='//api.mapbox.com/mapbox-gl-js/v1.11.0/mapbox-gl.css' rel='stylesheet' />
        </>
    );
}


// Requires: classes, link, contestname, contestdates

function Menu( props ) {

	// Try and extract a short form of the name, only letters and spaces stop at first number
    return (
        <>
            <Navbar bg="light" fixed="top">
                <Nav fill variant="tabs" defaultActiveKey={props.vc} style={{width:'100%'}}>
					<Nav.Item key="sspot" style={{paddingTop:0,paddingBottom:0}}>
						<Nav.Link href="http://glidernet.org" className="d-md-none">
							OGN Range (Beta)<Nbsp/><Icon type='external-link'/>
						</Nav.Link>
						<Nav.Link href='#' className="d-none d-md-block"  style={{paddingTop:0,paddingBottom:0}}>
							{props.station||'All Stations'}
						</Nav.Link>
					</Nav.Item>
					<Nav.Item key="settings">
						<Nav.Link href='#' key='navlinksettings' eventKey='settings'
								  onClick={() => { Router.push(props.override ? '/settings?mapType='+props.override : '/settings', undefined, {shallow:true}); }}>
							<Icon type='cog'/>
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
function CombinePage( props ) {

    // First step is to extract the class from the query, we use
    // query because that stops page reload when switching between the
    // classes. If no class is set then assume the first one
    const router = useRouter()
    let { station, mapType } = router.query;
	if( mapType ) {
		props.options.mapType = parseInt(mapType);
	}

    // Next up load the contest and the pilots, we can use defaults for pilots
    // if the className matches
    const { comp, isLoading, error } = useContest();

	// Update the station by updating the query url preserving all parameters but station
	function setSelectedStation( station ) {
		router.push( { pathname: '/', query: { ...router.query, 'station': station }}, undefined, { shallow: true });
	}
	
	// What the map is looking at
    const [viewport, setViewport] = useState({
        latitude: props.lat,
        longitude: props.lng,
        zoom: 11.5,
		minZoom: 6.5,
		maxZoom: 14,
        bearing: 0,
		minPitch: 0,
		maxPitch: 85,
		altitude: 1.5,
        pitch: (! (props.options.mapType % 2)) ? 70 : 0
    });

	// 
    // And display in progress until they are loaded
    if (isLoading)
        return (<div className="loading">
                    <div className="loadinginner"/>
                </div>) ;
	
	return (
        <>
            <Head>
                <title>OGN Range (Beta)</title>
				<meta name='viewport' content='width=device-width, minimal-ui'/>
                <IncludeJavascript/>
            </Head>
            <Menu station={station} setSelectedStation={setSelectedStation} override={mapType}/>
			<div className="resizingContainer" >
				<MAP station={station} setSelectedStation={setSelectedStation}
					 viewport={viewport} setViewport={setViewport}
					 options={props.options} setOptions={props.setOptions}
				/>
			</div>
		</>
    );
}

export default CombinePage;
