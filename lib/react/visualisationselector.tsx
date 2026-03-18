import {useMemo, useCallback, useEffect} from 'react';
import {useTranslation} from 'next-i18next';

import Select from 'react-select';
import {Layer, layerMaskFromSet, ALL_LAYER_NAMES, PRESENCE_ONLY} from '../common/layers';
import {LayerBadges} from './layerbadges';

const normalVisualisations = ['avgSig', 'maxSig', 'count', 'minAlt', 'minAgl', 'minAltSig', 'avgCrc', 'avgGap'];
const signalVisualisations = new Set(['avgSig', 'maxSig', 'minAltSig', 'avgCrc']);

const globalVisualisations = ['expectedGap', 'stations', 'primaryStation'];

const defaultVisualisation = 'avgSig';

function LayerCoverageLegend({layers, setLayers}: {layers: string[]; setLayers?: (l: string[]) => void}) {
    const {t} = useTranslation();
    const validLayers = layers.filter((l) => ALL_LAYER_NAMES.has(l)) as Layer[];
    const mask = validLayers.length > 0 ? layerMaskFromSet(validLayers) : undefined;
    return (
        <div style={{marginTop: '6px'}}>
            <LayerBadges layerMask={mask} layers={layers} setLayers={setLayers} />
            <div style={{color: '#888', marginTop: '2px', fontStyle: 'italic', fontSize: '0.85em'}}>{t('visualisation.layerCoverageBlend')}</div>
        </div>
    );
}

export function VisualisationLegend({visualisation, layers, setLayers}: {visualisation?: string | null; layers?: string[]; setLayers?: (l: string[]) => void}) {
    if (visualisation === 'layerCoverage' && layers && layers.length > 1) {
        return <LayerCoverageLegend layers={layers} setLayers={setLayers} />;
    }
    return null;
}

export function VisualisationSelector({
    visualisation,
    station,
    setVisualisation,
    isPresenceOnly,
    layers,
    setLayers
}: {
    visualisation?: string | null;
    station?: string | null;
    setVisualisation: (a: string) => void;
    isPresenceOnly?: boolean;
    layers?: string[];
    setLayers?: (l: string[]) => void;
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

    const selectVisualisationOnChange = useCallback((v) => setVisualisation(v.value), [setVisualisation]);

    const presenceOnlyLayers = !isPresenceOnly ? (layers?.filter((l) => PRESENCE_ONLY.has(l as Layer)) ?? []) : [];
    const showPresenceWarning = presenceOnlyLayers.length > 0 && visualisation && signalVisualisations.has(visualisation);

    return (
        <>
            <b>{t('selectors.visualisation')}:</b>
            <Select options={visualisations} value={selectedVisualisation} onChange={selectVisualisationOnChange} />
            {showPresenceWarning && (
                <div style={{marginTop: '4px', padding: '4px 8px', background: '#fff3cd', border: '1px solid #f0c040', borderRadius: '4px', fontSize: '0.85em'}}>
                    ⚠ {t('visualisation.presenceOnlySignalWarning', {layers: presenceOnlyLayers.map((l) => t(`layers.${l}`)).join(', ')})}
                </div>
            )}
            <VisualisationLegend visualisation={visualisation} layers={layers} setLayers={setLayers} />
        </>
    );
}
