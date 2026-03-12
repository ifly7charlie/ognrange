'use client';

import {useCallback, useMemo} from 'react';
import {useTranslation} from 'next-i18next';

import Select from 'react-select';

import {ALL_LAYERS, COMBINED_LAYERS, LAYER_BIT} from '../common/layers';

export function LayerSelector({layers, setLayers, stationLayerMask}: {layers: string[]; setLayers: (l: string[]) => void; stationLayerMask?: number}) {
    const {t} = useTranslation('common');

    const hasCombined = layers.includes('combined');

    const options = useMemo(
        () =>
            ALL_LAYERS.filter((layer) => {
                if (hasCombined && COMBINED_LAYERS.has(layer)) return false;
                if (stationLayerMask) return stationLayerMask & (1 << LAYER_BIT[layer]);
                return true;
            }).map((layer) => ({
                value: layer as string,
                label: t(`layers.${layer}`)
            })),
        [t, hasCombined, stationLayerMask]
    );

    const selectedOptions = useMemo(() => options.filter((o) => layers.includes(o.value)), [options, layers]);

    const onChange = useCallback(
        (selected: readonly {value: string; label: string}[]) => {
            const vals = selected.map((o) => o.value);
            setLayers(vals.length ? vals : ['combined']);
        },
        [setLayers]
    );

    return (
        <>
            <b>{t('selectors.layers')}:</b>
            <Select isMulti options={options} value={selectedOptions} onChange={onChange} />
        </>
    );
}
