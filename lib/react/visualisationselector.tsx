import {useMemo, useCallback, useEffect} from 'react';
import {useTranslation} from 'next-i18next';

import Select from 'react-select';
import {LAYER_COLOR, Layer} from '../common/layers';

const normalVisualisations = ['avgSig', 'maxSig', 'count', 'minAlt', 'minAgl', 'minAltSig', 'avgCrc', 'avgGap'];
const signalVisualisations = new Set(['avgSig', 'maxSig', 'minAltSig', 'avgCrc']);

const globalVisualisations = ['expectedGap', 'stations', 'primaryStation'];

const defaultVisualisation = 'avgSig';

function LayerCoverageLegend({layers}: {layers: string[]}) {
    const {t} = useTranslation();
    return (
        <div style={{marginTop: '6px', fontSize: '0.85em'}}>
            {layers.map((l) => {
                const color = LAYER_COLOR[l as Layer];
                return (
                    <div key={l} style={{display: 'flex', alignItems: 'center', gap: '6px', marginBottom: '3px'}}>
                        <div
                            style={{
                                width: '14px',
                                height: '14px',
                                borderRadius: '2px',
                                flexShrink: 0,
                                backgroundColor: color ? `rgb(${color[0]},${color[1]},${color[2]})` : '#808080'
                            }}
                        />
                        <span>{t(`layers.${l}`)}</span>
                    </div>
                );
            })}
            <div style={{color: '#888', marginTop: '2px', fontStyle: 'italic'}}>{t('visualisation.layerCoverageBlend')}</div>
        </div>
    );
}

export function VisualisationLegend({visualisation, layers}: {visualisation?: string | null; layers?: string[]}) {
    if (visualisation === 'layerCoverage' && layers && layers.length > 1) {
        return <LayerCoverageLegend layers={layers} />;
    }
    return null;
}

export function VisualisationSelector({
    visualisation,
    station,
    setVisualisation,
    isPresenceOnly,
    layers
}: {
    visualisation?: string | null;
    station?: string | null;
    setVisualisation: (a: string) => void;
    isPresenceOnly?: boolean;
    layers?: string[];
}) {
    const {t} = useTranslation();
    const multiLayer = (layers?.length ?? 0) > 1;

    const [visualisations, selectedVisualisation] = useMemo((): [any, any] => {
        const allVis = [
            ...normalVisualisations,
            ...((station || 'global') == 'global' ? globalVisualisations : []),
            ...(multiLayer ? ['layerCoverage'] : [])
        ];
        const filtered = isPresenceOnly ? allVis.filter((v) => !signalVisualisations.has(v)) : allVis;
        const vis = filtered.map((value) => ({label: t(`visualisation.${value}`), value}));
        return [vis, vis.find((a) => a.value === (visualisation || defaultVisualisation))];
    }, [visualisation, station, isPresenceOnly, multiLayer]);

    // Auto-switch away from signal or multi-layer-only visualisations when conditions change
    useEffect(() => {
        if (isPresenceOnly && visualisation && signalVisualisations.has(visualisation)) {
            setVisualisation('count');
        }
    }, [isPresenceOnly]);

    useEffect(() => {
        if (!multiLayer && visualisation === 'layerCoverage') {
            setVisualisation(defaultVisualisation);
        }
    }, [multiLayer]);

    const selectVisualisationOnChange = useCallback((v) => setVisualisation(v.value), [false]);

    return (
        <>
            <b>{t('selectors.visualisation')}:</b>
            <Select options={visualisations} value={selectedVisualisation} onChange={selectVisualisationOnChange} />
            <VisualisationLegend visualisation={visualisation} layers={layers} />
        </>
    );
}
