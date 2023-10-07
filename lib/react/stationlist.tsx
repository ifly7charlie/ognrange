import Link from 'next/link';
import {useMemo, useCallback} from 'react';

import {reduce as _reduce, sortedIndexOf as _sortedIndexOf} from 'lodash';

import {stationMeta} from './stationMeta';
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
                    acc.push(
                        <tr key={sid}>
                            <td>
                                <Link replace onClick={selectStation} href={'#'} id={name}>
                                    {name || 'Unknown'}
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
        [encodedList]
    );

    // A station is selected so do nothing
    if (encodedList === '') {
        return null;
    }

    return (
        <>
            <b>Stations ({splitList.length})</b>
            <br />
            <table className="stationList">
                <tbody>{stationList}</tbody>
            </table>
        </>
    );
}
