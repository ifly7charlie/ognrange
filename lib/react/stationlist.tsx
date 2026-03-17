import Link from 'next/link';
import {useMemo, useCallback} from 'react';
import {useTranslation} from 'next-i18next';
import {useSearchParams} from 'next/navigation';

import {reduce as _reduce, sortedIndexOf as _sortedIndexOf} from 'lodash';

import {useStationListMetaUnfiltered} from './stationmeta';
import {LAYER_BIT, Layer, layerMaskFromSet, ALL_LAYER_NAMES} from '../common/layers';
import {cellToLatLng, greatCircleDistance} from 'h3-js';

export function StationList({
    encodedList,
    selectedH3,
    setStation
}: //
{
    encodedList: string | undefined;
    selectedH3: [number, number];
    setStation: (a: string) => void;
}) {
    const {t} = useTranslation('common', {keyPrefix: 'details.stations'});
    const stationMeta = useStationListMetaUnfiltered();
    const params = useSearchParams();
    const selectedLayerMask = useMemo(() => {
        const layersParam = params.get('layers');
        if (!layersParam) return null;
        const layerValues = layersParam.split(',').map((s) => s.trim()).filter((s) => ALL_LAYER_NAMES.has(s)) as Layer[];
        return layerValues.length > 0 ? layerMaskFromSet(layerValues) : null;
    }, [params]);
    const combinedBit = 1 << LAYER_BIT[Layer.COMBINED];

    const selectStation = useCallback(
        (e) => {
            setStation(e.currentTarget?.id);
        },
        [false]
    );

    const splitList = useMemo(() => encodedList?.split(','), [encodedList]);

    // Convert from comma list that is encoded using: stationid << 4 | percentage & 0x0f
    const stationList = useMemo(
        () =>
            _reduce(
                splitList,
                (acc, x) => {
                    const decoded = parseInt(x, 36);
                    const sid = decoded >> 4;
                    const percentage = (decoded & 0x0f) * 10;
                    const index = _sortedIndexOf(stationMeta.id, sid);
                    const loc = index != -1 && !isNaN(stationMeta.lat[index]) ? [stationMeta.lat[index], stationMeta.lng[index]] : null;
                    const name = index != -1 ? stationMeta.name[index] : null;
                    const dist = loc ? greatCircleDistance(cellToLatLng(selectedH3), loc, 'km').toFixed(0) + ' km' : '';
                    const mask = index != -1 && stationMeta.layerMask ? stationMeta.layerMask[index] : 0;
                    const effectiveMask = mask === 0 ? combinedBit : mask;
                    const layerSupported = selectedLayerMask === null || (effectiveMask & selectedLayerMask) !== 0;
                    acc.push(
                        <tr key={sid} style={layerSupported ? undefined : {opacity: 0.4}}>
                            <td>
                                <Link replace onClick={selectStation} href={'#'} id={name}>
                                    {name || t('unknown')}
                                </Link>
                            </td>
                            <td>{dist}</td>
                            <td>{percentage > 10 ? percentage.toFixed(0) + '%' : ''}</td>
                        </tr>
                    );
                    return acc;
                },
                []
            ),
        [encodedList, stationMeta, selectedLayerMask]
    );

    // A station is selected so do nothing
    if (encodedList === '') {
        return null;
    }

    return (
        <>
            <b>{t('title', {count: splitList.length})}</b>
            <br />
            <table className="stationList">
                <tbody>{stationList}</tbody>
            </table>
        </>
    );
}
