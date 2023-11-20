import {useCallback} from 'react';
import {useSearchParams} from 'next/navigation';
import AsyncSelect from 'react-select/async';

import {Checkbox} from './checkbox';

import {useStationMeta} from './stationmeta';

import {debounce as _debounce, map as _map, find as _find, filter as _filter} from 'lodash';

export function StationSelector({
    station, //
    setStation,
    updateUrl
}: {
    station: string;
    setStation: (name: string) => void;
    updateUrl: (a: Record<string, string>) => void;
}) {
    const selectedStation = {
        value: station || 'global',
        label: station || 'All Stations (global)'
    };

    const defaultStationSelection = [
        {label: 'Start typing to search', value: ''},
        {label: 'All Stations (global)', value: ''}
    ];

    const stationMeta = useStationMeta();
    const params = useSearchParams();
    const allStations = !!parseInt(params.get('allStations')?.toString() ?? '0');

    const setAllStations = useCallback(
        (value: boolean) => {
            if (allStations != value) {
                updateUrl({allStations: value ? 1 : 0});
            }
        },
        [allStations]
    );

    const findStation = useCallback(
        async (s: string) => {
            if (s.length >= 2) {
                try {
                    let re = new RegExp(s, 'i');

                    const p = _map(
                        _filter(stationMeta.name, (v) => v.match(re)),
                        (station) => {
                            return {value: station, label: station};
                        }
                    );
                    return p;
                } catch (e) {
                    return [];
                }
            }
            return [{value: '', label: 'All Stations (global)'}];
        },
        [stationMeta.length]
    );
    const selectStationOnChange = useCallback((v: {value: string}) => setStation(v.value), [false]);

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

            <Checkbox checked={allStations} onChange={(v) => setAllStations(!!v.target.checked)}>
                Show offline stations
            </Checkbox>
            <br />
        </>
    );
}
