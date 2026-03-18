'use client';

import {useCallback, useEffect, useMemo} from 'react';
import {useTranslation} from 'next-i18next';

import Select, {type StylesConfig} from 'react-select';

import {ALL_LAYERS, LAYER_BIT, LAYER_COLOR} from '../common/layers';

interface LayerOption {
    value: string;
    label: string;
    color: [number, number, number];
}

const selectStyles: StylesConfig<LayerOption, true> = {
    multiValue: (base, {data}) => ({
        ...base,
        backgroundColor: `rgba(${data.color[0]},${data.color[1]},${data.color[2]},0.2)`
    }),
    multiValueLabel: (base) => ({
        ...base
    }),
    multiValueRemove: (base, {data}) => ({
        ...base,
        color: `rgb(${data.color[0]},${data.color[1]},${data.color[2]})`,
        ':hover': {
            backgroundColor: `rgb(${data.color[0]},${data.color[1]},${data.color[2]})`,
            color: 'white'
        }
    })
};

function ColorDot({color}: {color: [number, number, number]}) {
    return (
        <span
            style={{
                width: '10px',
                height: '10px',
                borderRadius: '2px',
                display: 'inline-block',
                marginRight: '6px',
                backgroundColor: `rgb(${color[0]},${color[1]},${color[2]})`
            }}
        />
    );
}

function formatOptionLabel(option: LayerOption) {
    return (
        <span style={{display: 'flex', alignItems: 'center'}}>
            <ColorDot color={option.color} />
            {option.label}
        </span>
    );
}

export function LayerSelector({layers, setLayers, stationLayerMask}: {layers: string[]; setLayers: (l: string[]) => void; stationLayerMask?: number}) {
    const {t} = useTranslation('common');

    const options = useMemo(
        () =>
            ALL_LAYERS.filter((layer) => {
                if (stationLayerMask) return stationLayerMask & (1 << LAYER_BIT[layer]);
                return true;
            }).map((layer) => ({
                value: layer as string,
                label: t(`layers.${layer}`),
                color: LAYER_COLOR[layer]
            })),
        [t, stationLayerMask]
    );

    const selectedOptions = useMemo(() => options.filter((o) => layers.includes(o.value)), [options, layers]);

    // When the station changes and the new mask excludes some currently-selected layers,
    // drop them from state so downstream components (FileSelector) don't check them
    useEffect(() => {
        if (!stationLayerMask) return;
        const validValues = new Set(options.map((o) => o.value));
        const filtered = layers.filter((l) => validValues.has(l));
        if (filtered.length !== layers.length) {
            setLayers(filtered.length ? filtered : ['combined']);
        }
    }, [stationLayerMask, options]);

    const onChange = useCallback(
        (selected: readonly LayerOption[]) => {
            const vals = selected.map((o) => o.value);
            setLayers(vals.length ? vals : ['combined']);
        },
        [setLayers]
    );

    return (
        <>
            <b>{t('selectors.layers')}:</b>
            <Select isMulti options={options} value={selectedOptions} onChange={onChange} styles={selectStyles} formatOptionLabel={formatOptionLabel} />
        </>
    );
}
