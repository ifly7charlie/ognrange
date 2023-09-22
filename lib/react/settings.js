import {IoSettingsOutline} from 'react-icons/io5';

import {useState} from 'react';
import {useCallback} from 'react';
import {useRouter} from 'next/router';

import Select from 'react-select';

import {find as _find, debounce as _debounce} from 'lodash';

import {HexAlphaColorPicker} from 'react-colorful';

import {defaultFromColour, defaultToColour, defaultBaseMap} from './defaults';

const Checkbox = ({children, ...props}) => (
    <label style={{marginRight: '1em'}}>
        <input type="checkbox" {...props} />
        {children}
    </label>
);

export function Settings(props) {
    const router = useRouter();
    const [settingsVisible, setSettingsVisible] = useState(false);

    const toggleSettings = () => {
        setSettingsVisible(!settingsVisible);
    };

    //    console.log('render', router.query, settingsVisible);

    function setSetting(name, value) {
        console.log(name, value);
        if (router.query[name] != value) {
            const newObject = {};
            newObject[name] = value;
            router.replace(
                {
                    pathname: '/',
                    query: {...router.query, ...newObject}
                },
                undefined,
                {shallow: false}
            );
            console.log('after', router.query);
        }
    }

    const delayedUpdateFrom = useCallback(
        _debounce((name, value) => setSetting(name, value), 300),
        [router.query]
    );
    const delayedUpdateTo = useCallback(
        _debounce((name, value) => setSetting(name, value), 300),
        [router.query]
    );

    const baseMaps = [
        {label: 'OGN Light', value: defaultBaseMap},
        {label: 'North Star', value: 'mapbox/cj44mfrt20f082snokim4ungi'},
        {label: 'Cali Terrain', value: 'mapbox/cjerxnqt3cgvp2rmyuxbeqme7'},
        {label: 'Outdoors', value: 'mapbox/outdoors-v11'},
        {label: 'Streets', value: 'mapbox/streets-v11'},
        {label: 'Navigation Guidance Night', value: 'mapbox/navigation-guidance-night-v4'},
        {label: 'Navigation Guidance Day', value: 'mapbox/navigation-guidance-day'},
        {label: 'Dark', value: 'mapbox/dark-v10'}
    ];

    return !settingsVisible ? (
        <div style={{padding: '10px', position: 'absolute', bottom: '10px', right: '20px'}}>
            <button style={{padding: '5px'}} onClick={toggleSettings}>
                <IoSettingsOutline style={{paddingTop: '2px'}} />
                &nbsp;<span> Settings</span>
            </button>
        </div>
    ) : (
        <div style={{padding: '10px', position: 'absolute', bottom: '10px', left: '20px', width: '90%', minHeight: '800px', overflow: 'vertical', background: 'white', borderStyle: 'ridge'}}>
            <h4>Settings</h4>
            <hr style={{paddingTop: '0px'}} />
            <b>Data Visualisation Colouring:</b>
            <table style={{width: '100%'}}>
                <thead>
                    <tr>
                        <td style={{width: '50%'}}>
                            <b>From</b>
                        </td>
                        <td style={{width: '50%'}}>
                            <b>To</b>
                        </td>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>
                            <HexAlphaColorPicker color={'#' + (router.query.fromColour || defaultFromColour)} onChange={(v) => delayedUpdateFrom('fromColour', v.slice(1))} style={{width: '80%'}} id="fromColour" />
                        </td>
                        <td>
                            <HexAlphaColorPicker color={'#' + (router.query.toColour || defaultToColour)} onChange={(v) => delayedUpdateTo('toColour', v.slice(1))} style={{width: '80%'}} key="toColour" />
                        </td>
                    </tr>
                </tbody>
            </table>
            <b>Select base map style:</b>
            <Select options={baseMaps} value={_find(baseMaps, {value: router.query.mapStyle || defaultBaseMap})} onChange={(v) => setSetting('mapStyle', v.value)} />
            <br />
            <Checkbox checked={parseInt(router.query.highlightStations ?? '1') ? true : false} onChange={(v) => setSetting('highlightStations', v.target.checked ? '1' : '0')}>
                Show distance circles
            </Checkbox>
            <Checkbox checked={parseInt(router.query.airspace ?? '0') ? true : false} onChange={(v) => setSetting('airspace', v.target.checked ? '1' : '0')}>
                Show airspace
            </Checkbox>
            <hr />

            <button style={{padding: '5px'}} onClick={toggleSettings}>
                &nbsp;<span> Close</span>
            </button>
        </div>
    );
}
