import {useMemo, useCallback} from 'react';
import {useTranslation} from 'next-i18next';

import Select from 'react-select';

const normalVisualisations = ['avgSig', 'maxSig', 'count', 'minAlt', 'minAgl', 'minAltSig', 'avgCrc', 'avgGap'];

const globalVisualisations = ['expectedGap', 'stations', 'primaryStation'];

const defaultVisualisation = 'avgSig';

export function VisualisationSelector({
    visualisation,
    station,
    setVisualisation
}: //
{
    visualisation?: string | null;
    station?: string | null;
    setVisualisation: (a: string) => void;
}) {
    const {t} = useTranslation();

    // Figure out our visualisations
    const [visualisations, selectedVisualisation] = useMemo((): [any, any] => {
        const vis = [...normalVisualisations, ...((station || 'global') == 'global' ? globalVisualisations : [])] //
            .map((value) => ({label: t(`visualisation.${value}`), value}));
        return [vis, vis.find((a) => a.value === (visualisation || defaultVisualisation))];
    }, [visualisation, station]);

    const selectVisualisationOnChange = useCallback((v) => setVisualisation(v.value), [false]);

    return (
        <>
            <b>{t('selectors.visualisation')}:</b>
            <Select options={visualisations} value={selectedVisualisation} onChange={selectVisualisationOnChange} />
        </>
    );
}
