'use client';

import {Settings} from './settings';

import {CoverageDetails} from './coveragedetails';
import {VisualisationSelector} from './visualisationselector';
import {FileSelector} from './fileselector';
import {StationSelector} from './stationselector';

import type {PickableDetails} from './pickabledetails';

export function Dock(props: {
    setStation: (station: string) => void; //
    station: string;
    setFile: (file: string) => void;
    file: string;
    hoverDetails: PickableDetails;
    selectedDetails: PickableDetails;
    setSelectedDetails: (sd?: PickableDetails) => void;
    setVisualisation: (visualisation: string) => void;
    visualisation: string;
    updateUrl: (a: Record<string, string>) => void;
    expanded: boolean;
    env: any;
}) {
    return (
        <>
            <div>
                <span style={{padding: '0px', border: '5px solid white'}}>
                    <img width="100" height="100" src="https://ognproject.wdfiles.com/local--files/logos/ogn-logo-150x150.png" alt="OGN Network" title="OGN Network" />
                </span>
            </div>

            <div style={{padding: '7px'}}>
                <StationSelector station={props.station} setStation={props.setStation} updateUrl={props.updateUrl} />
                <br />
                <FileSelector station={props.station} setFile={props.setFile} file={props.file} />
                <br />
                <VisualisationSelector station={props.station} setVisualisation={props.setVisualisation} visualisation={props.visualisation} />
                <br />
                <hr />
                {props.expanded ? (
                    <CoverageDetails //
                        details={props.selectedDetails.type !== 'none' ? props.selectedDetails : props.hoverDetails}
                        locked={props.selectedDetails.type !== 'none'}
                        setSelectedDetails={props.setSelectedDetails}
                        station={props.station}
                        setStation={props.setStation}
                        file={props.file}
                        setFile={props.setFile}
                        env={props.env}
                    />
                ) : null}
            </div>
            <Settings updateUrl={props.updateUrl} env={props.env} />
        </>
    );
}
