import {useCallback} from 'react';
import {useSearchParams} from 'next/navigation';
import AsyncSelect from 'react-select/async';

import {useTranslation} from 'next-i18next';

import {Checkbox} from './checkbox';

import {useStationMeta} from './stationmeta';

export function StationSelector({
    station, //
    setStation,
    updateUrl
}: {
    station: string;
    setStation: (name: string) => void;
    updateUrl: (a: Record<string, string>) => void;
}) {
    const {t} = useTranslation();

    const selectedStation = {
        value: station || 'global',
        label: station || t('stations.global')
    };

    const defaultStationSelection = [
        {label: t('stations.search'), value: ''},
        {label: t('stations.global'), value: ''}
    ];

    const stationMeta = useStationMeta();
    const params = useSearchParams();
    const allStations = !!parseInt(params.get('allStations')?.toString() ?? '0');

    const setAllStations = useCallback(
        (value: boolean) => {
            if (allStations != value) {
                updateUrl({allStations: value ? '1' : undefined});
            }
        },
        [allStations]
    );

    const findStation = useCallback(
        async (s: string): Promise<{value: string; label: string}[]> => {
            if (s.length >= 2) {
                try {
                    let re = new RegExp(s, 'i');

                    const p = stationMeta.name
                        .filter((v) => v.match(re))
                        .map((station) => {
                            return {value: station, label: station};
                        });
                    console.log(p);
                    return p;
                } catch (e) {
                    return [{value: '', label: t('stations.global')}];
                }
            }
            return [{value: '', label: t('stations.global')}];
        },
        [stationMeta.length]
    );
    const selectStationOnChange = useCallback((v: {value: string}) => setStation(v.value), [false]);

    return (
        <>
            <b>{t('selectors.station')}:</b>
            <AsyncSelect
                loadOptions={findStation} //
                value={selectedStation}
                defaultOptions={defaultStationSelection}
                onChange={selectStationOnChange}
                noOptionsMessage={() => t('stations.search')}
            />

            <Checkbox checked={allStations} onChange={(v) => setAllStations(!!v.target.checked)}>
                {t('stations.offline')}
            </Checkbox>
            <br />
        </>
    );
}
