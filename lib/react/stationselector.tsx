import {useCallback} from 'react';
import {useSearchParams} from 'next/navigation';
import AsyncSelect from 'react-select/async';

import {useTranslation} from 'next-i18next';

import {Checkbox} from './checkbox';

import {useStationListMetaUnfiltered} from './stationmeta';
import {LAYER_BIT, LAYER_COLOR, Layer} from '../common/layers';

const LAYER_DOT_ORDER = [Layer.FLARM, Layer.ADSB, Layer.ADSL, Layer.FANET, Layer.OGNTRK, Layer.PAW, Layer.SAFESKY] as const;
function rgbToHex([r, g, b]: [number, number, number]): string {
    return '#' + [r, g, b].map((c) => c.toString(16).padStart(2, '0')).join('');
}

type StationOption = {value: string; label: string; layerMask: number};

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

    const stationMeta = useStationListMetaUnfiltered();
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
        async (s: string): Promise<StationOption[]> => {
            if (s.length >= 2 && stationMeta) {
                try {
                    const re = new RegExp(s, 'i');
                    const combinedBit = 1 << LAYER_BIT[Layer.COMBINED];
                    const results: StationOption[] = [];
                    for (let index = 0; index < stationMeta.name.length; index++) {
                        const name = stationMeta.name[index];
                        if (!allStations && stationMeta.valid && !stationMeta.valid[index]) continue;
                        if (!name.match(re)) continue;
                        const mask = stationMeta.layerMask ? stationMeta.layerMask[index] : 0;
                        results.push({value: name, label: name, layerMask: mask === 0 ? combinedBit : mask});
                    }
                    return results;
                } catch (e) {
                    return [{value: '', label: t('stations.global'), layerMask: 0}];
                }
            }
            return [{value: '', label: t('stations.global'), layerMask: 0}];
        },
        [stationMeta, allStations]
    );

    const formatOptionLabel = useCallback(
        ({label, layerMask}: StationOption) => {
            const dots = LAYER_DOT_ORDER.filter((l) => layerMask & (1 << LAYER_BIT[l]));
            return (
                <span style={{display: 'flex', alignItems: 'center', gap: '6px'}}>
                    {label}
                    <span style={{display: 'flex', gap: '2px', flexShrink: 0, marginLeft: 'auto'}}>
                        {dots.map((l) => (
                            <span
                                key={l}
                                title={l}
                                style={{
                                    display: 'inline-block',
                                    width: '6px',
                                    height: '6px',
                                    borderRadius: '50%',
                                    background: rgbToHex(LAYER_COLOR[l])
                                }}
                            />
                        ))}
                    </span>
                </span>
            );
        },
        []
    );
    const selectStationOnChange = useCallback(
        (v: {value: string}) => {
            console.log('[StationSelector] onChange fired, value:', JSON.stringify(v));
            setStation(v.value);
        },
        [false]
    );

    return (
        <>
            <b>{t('selectors.station')}:</b>
            <AsyncSelect
                loadOptions={findStation}
                value={selectedStation}
                defaultOptions={defaultStationSelection}
                onChange={selectStationOnChange}
                formatOptionLabel={formatOptionLabel}
                noOptionsMessage={() => t('stations.search')}
            />

            <Checkbox checked={allStations} onChange={(v) => setAllStations(!!v.target.checked)}>
                {t('stations.offline')}
            </Checkbox>
            <br />
        </>
    );
}
