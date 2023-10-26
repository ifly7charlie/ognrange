import {useState, useRef, useMemo, useEffect, useCallback} from 'react';
import Select from 'react-select';

const normalVisualisations = [
    {label: 'Average Signal Strength', value: 'avgSig'},
    {label: 'Maximum Signal Strength', value: 'maxSig'},
    {label: 'Count', value: 'count'},
    {label: 'Minimum Altitude', value: 'minAlt'},
    {label: 'Minimum Altitude AGL', value: 'minAgl'},
    {label: 'Max Signal @ Minimum Altitude', value: 'minAltSig'},
    {label: 'Avg CRC errors', value: 'avgCrc'},
    {label: 'Average between packet gap', value: 'avgGap'}
];

const globalVisualisations = [
    {label: 'Expected between packet gap', value: 'expectedGap'},
    {label: 'Number of stations', value: 'stations'},
    {label: 'Primary station', value: 'primaryStation'}
];

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
    // Figure out our visualisations
    const [visualisations, selectedVisualisation] = useMemo((): [any, any] => {
        const vis = [...normalVisualisations, ...((station || 'global') == 'global' ? globalVisualisations : [])];
        return [vis, vis.find((a) => a.value === (visualisation || defaultVisualisation))];
    }, [visualisation, station]);

    const selectVisualisationOnChange = useCallback((v) => setVisualisation(v.value), [false]);

    return (
        <>
            <b>Select visualisation:</b>
            <Select options={visualisations} value={selectedVisualisation} onChange={selectVisualisationOnChange} />
        </>
    );
}
