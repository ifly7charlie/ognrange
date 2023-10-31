import {useState, useRef, useMemo, useEffect, useCallback} from 'react';

import useSWR from 'swr';
const fetcher = (url: string) => fetch(url).then((res) => res.json());

import {map as _map, find as _find} from 'lodash';

import Select from 'react-select';

export function FileSelector({station, file, setFile}) {
    // Load the associated index
    //    const DATA_URL = env.NEXT_PUBLIC_DATA_URL || process.env.NEXT_PUBLIC_DATA_URL || '/data/';
    const {data} = useSWR(`/api/station/${station || 'global'}`, fetcher);

    // Display the right ones to the user
    const [availableFiles, selectedFile] = useMemo((): [any, any] => {
        const files = data?.files || {year: {current: 'year', all: ['year']}};
        const selects = _map(files, (value, key) => {
            return {
                label: key,
                options: _map(value.all, (cfile) => {
                    // latest is also symbolic linked, we use that instead
                    if (cfile == value.current) {
                        return {
                            label: 'Current ' + key + ' (' + (cfile.match(/([0-9-]+)$/) || [cfile])[0] + ')',
                            value: key
                        };
                    } else {
                        return {
                            label: (cfile.match(/([0-9-]+)$/) || [cfile])[0],
                            value: (cfile.match(/((day|month|year)\.[0-9-]+)$/) || [cfile])[0]
                        };
                    }
                }).reverse()
            };
        }).reverse();
        const effectiveFile = file && file != '' ? file : 'year';
        const [type] = effectiveFile.split('.') || [effectiveFile];
        const selected = selects
            ? _find(_find(selects, {label: type})?.options || [], (o) => {
                  return effectiveFile.slice(-o.value.length) == o.value;
              })
            : null;

        return [selects, selected];
    }, [file, data?.files?.day?.current, station]);

    const selectFileOnChange = useCallback((v) => setFile(v.value), [false]);

    return (
        <>
            <b>Select available time period to display:</b>
            <Select options={availableFiles} value={selectedFile} onChange={selectFileOnChange} />
        </>
    );
}
