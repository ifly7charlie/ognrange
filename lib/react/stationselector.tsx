import {useCallback} from 'react';
import AsyncSelect from 'react-select/async';

import {stationMeta} from './stationMeta';

import {debounce as _debounce, map as _map, find as _find, filter as _filter} from 'lodash';

export function StationSelector({station, setStation}) {
    const selectedStation = {
        value: station || 'global',
        label: station || 'All Stations (global)'
    };

    const defaultStationSelection = [
        {label: 'Start typing to search', value: ''},
        {label: 'All Stations (global)', value: ''}
    ];

    const findStation = useCallback(
        async (s: string) => {
            if (s.length >= 2) {
                try {
                    let re = new RegExp(s, 'i');

                    const p = _map(
                        _filter(stationMeta, (v) => v.station.match(re)),
                        (o) => {
                            return {value: o.station, label: o.station};
                        }
                    );
                    return p;
                } catch (e) {
                    return [];
                }
            }
            return [{value: '', label: 'All Stations (global)'}];
        },
        [false]
    );
    const selectStationOnChange = useCallback((v) => setStation(v.value), [false]);

    return (
        <>
            <b>Select station to display:</b>
            <AsyncSelect
                loadOptions={findStation} //
                value={selectedStation}
                defaultOptions={defaultStationSelection}
                onChange={selectStationOnChange}
                noOptionsMessage={() => 'Start typing to search'}
            />
        </>
    );
}
