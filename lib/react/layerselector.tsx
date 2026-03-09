'use client';

import {useCallback, useMemo} from 'react';
import {useTranslation} from 'next-i18next';

import Select from 'react-select';

import {ALL_LAYERS} from '../common/layers';

export function LayerSelector({layers, setLayers}: {layers: string[]; setLayers: (l: string[]) => void}) {
    const {t} = useTranslation('common');

    const options = useMemo(
        () =>
            ALL_LAYERS.map((layer) => ({
                value: layer as string,
                label: t(`layers.${layer}`)
            })),
        [t]
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
