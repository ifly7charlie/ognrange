import {useMemo, useCallback, useEffect} from 'react';
import {useTranslation} from 'next-i18next';

import Select from 'react-select';

const normalVisualisations = ['avgSig', 'maxSig', 'count', 'minAlt', 'minAgl', 'minAltSig', 'avgCrc', 'avgGap'];
const signalVisualisations = new Set(['avgSig', 'maxSig', 'minAltSig', 'avgCrc']);

const globalVisualisations = ['expectedGap', 'stations', 'primaryStation'];

const defaultVisualisation = 'avgSig';

export function VisualisationSelector({
    visualisation,
    station,
    setVisualisation,
    isPresenceOnly
}: //
{
    visualisation?: string | null;
    station?: string | null;
    setVisualisation: (a: string) => void;
    isPresenceOnly?: boolean;
}) {
    const {t} = useTranslation();

    // Figure out our visualisations
    const [visualisations, selectedVisualisation] = useMemo((): [any, any] => {
        const allVis = [...normalVisualisations, ...((station || 'global') == 'global' ? globalVisualisations : [])];
        const filtered = isPresenceOnly ? allVis.filter((v) => !signalVisualisations.has(v)) : allVis;
        const vis = filtered.map((value) => ({label: t(`visualisation.${value}`), value}));
        return [vis, vis.find((a) => a.value === (visualisation || defaultVisualisation))];
    }, [visualisation, station, isPresenceOnly]);

    // Auto-switch away from signal visualisations when presence-only
    useEffect(() => {
        if (isPresenceOnly && visualisation && signalVisualisations.has(visualisation)) {
            setVisualisation('count');
        }
    }, [isPresenceOnly]);

    const selectVisualisationOnChange = useCallback((v) => setVisualisation(v.value), [false]);

    return (
        <>
            <b>{t('selectors.visualisation')}:</b>
            <Select options={visualisations} value={selectedVisualisation} onChange={selectVisualisationOnChange} />
        </>
    );
}
