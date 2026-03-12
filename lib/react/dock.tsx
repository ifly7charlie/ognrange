'use client';

import {Settings} from './settings';

import {CoverageDetails} from './coveragedetails';
import {VisualisationSelector} from './visualisationselector';
import {FileSelector} from './fileselector';
import {StationSelector} from './stationselector';
import {LayerSelector} from './layerselector';
import {useStationMeta} from './stationmeta';

import type {PickableDetails} from './pickabledetails';

export function Dock(props: {
    setStation: (station: string) => void; //
    station: string;
    dateRange: {start: string; end: string};
    setDateRange: (r: {start: string; end: string}) => void;
    layers: string[];
    setLayers: (l: string[]) => void;
    hoverDetails: PickableDetails;
    selectedDetails: PickableDetails;
    setSelectedDetails: (sd?: PickableDetails) => void;
    setVisualisation: (visualisation: string) => void;
    visualisation: string;
    isPresenceOnly?: boolean;
    updateUrl: (a: Record<string, string>) => void;
    expanded: boolean;
    env: any;
}) {
    // Derive file/setFile for CoverageDetails backward compat
    const file = props.dateRange?.start ?? 'year';
    const setFile = (f: string) => props.setDateRange({start: f, end: f});

    const stationMeta = useStationMeta(props.station ?? '');

    return (
        <>
            <div style={{padding: '1px'}}>
                <span style={{border: '5px solid white'}}>
                    <img width="100" height="100" src="https://ognproject.wdfiles.com/local--files/logos/ogn-logo-150x150.png" alt="OGN Network" title="OGN Network" />
                </span>
            </div>

            <div style={{padding: '7px'}}>
                <StationSelector station={props.station} setStation={props.setStation} updateUrl={props.updateUrl} />
                <br />
                <LayerSelector layers={props.layers} setLayers={props.setLayers} stationLayerMask={stationMeta?.layerMask ?? 0} />
                <br />
                <FileSelector station={props.station} dateRange={props.dateRange} setDateRange={props.setDateRange} layers={props.layers} />
                <br />
                <VisualisationSelector station={props.station} setVisualisation={props.setVisualisation} visualisation={props.visualisation} isPresenceOnly={props.isPresenceOnly} layers={props.layers} setLayers={props.setLayers} />
                <br />
                <hr />
                {props.expanded ? (
                    <CoverageDetails //
                        details={props.selectedDetails.type !== 'none' ? props.selectedDetails : props.hoverDetails}
                        locked={props.selectedDetails.type !== 'none'}
                        setSelectedDetails={props.setSelectedDetails}
                        station={props.station}
                        setStation={props.setStation}
                        file={file}
                        setFile={setFile}
                        layers={props.layers}
                        env={props.env}
                    />
                ) : null}
            </div>
            <Settings updateUrl={props.updateUrl} env={props.env} />
        </>
    );
}
