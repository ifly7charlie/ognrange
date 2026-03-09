import {useMemo, useCallback, useState} from 'react';

import useSWR from 'swr';
import {useTranslation} from 'next-i18next';
import Select, {SingleValue} from 'react-select';
import {DayPicker, MonthPicker} from './datepicker';

const fetcher = (url: string) => fetch(url).then((res) => res.json());

type PeriodType = 'year' | 'yearnz' | 'month' | 'day';
const ALL_PERIOD_TYPES: PeriodType[] = ['year', 'yearnz', 'month', 'day'];

type Option = {value: string; label: string; isPartial?: boolean};

function parseDateParam(s: string): {type: PeriodType; date: string | null} {
    const dot = s.indexOf('.');
    if (dot === -1) return {type: s as PeriodType, date: null};
    return {type: s.slice(0, dot) as PeriodType, date: s.slice(dot + 1)};
}

function dateToInput(type: PeriodType, date: string): string {
    return type === 'yearnz' ? date.replace('nz', '') : date;
}

function inputToDate(type: PeriodType, value: string): string {
    return type === 'yearnz' ? value + 'nz' : value;
}

function labelFor(type: PeriodType, inputVal: string): string {
    if (type === 'month') {
        const [y, m] = inputVal.split('-');
        return new Date(+y, +m - 1).toLocaleDateString(undefined, {month: 'short', year: 'numeric'});
    }
    if (type === 'day') {
        // Append midday to avoid timezone-driven date-shift
        return new Date(inputVal + 'T12:00:00').toLocaleDateString(undefined, {weekday: 'short', day: 'numeric', month: 'short', year: 'numeric'});
    }
    return inputVal; // year / yearnz: just show the year number
}

function FormatPartialOption({opt, title}: {opt: Option; title: string}) {
    return opt.isPartial ? (
        <span title={title}>
            {opt.label} <span style={{color: '#f90'}}>⚠</span>
        </span>
    ) : (
        <>{opt.label}</>
    );
}

export function FileSelector({station, dateRange, setDateRange, layers}: {
    station: string | null;
    dateRange: {start: string; end: string};
    setDateRange: (r: {start: string; end: string}) => void;
    layers?: string[];
}) {
    const {data} = useSWR(`/api/station/${station || 'global'}`, fetcher, {revalidateOnFocus: false});
    const {t} = useTranslation('common', {keyPrefix: 'period'});
    const {t: tLayer} = useTranslation('common', {keyPrefix: 'layers'});
    const formatPartialOption = (opt: Option) => <FormatPartialOption opt={opt} title={t('partial_option_title')} />;

    const [showRange, setShowRange] = useState(() => dateRange.start !== dateRange.end);

    const from = parseDateParam(dateRange.start);
    const to = parseDateParam(dateRange.end);
    const currentType = from.type;

    const {availableTypes, datesByType, partialDatesByType, missingLayersByType} = useMemo(() => {
        const files = data?.files || {};
        const datesByType: Record<string, string[]> = {};
        const partialDatesByType: Record<string, Set<string>> = {};
        const missingLayersByType: Record<string, Set<string>> = {};
        const availableTypes: PeriodType[] = [];

        const layersToCheck = layers?.length ? layers : ['combined'];

        for (const type of ALL_PERIOD_TYPES) {
            const layerData = files[type];
            if (!layerData) continue;

            // For each layer, collect its available dates
            const layerDateSets: Record<string, Set<string>> = {};
            for (const layer of layersToCheck) {
                const lData = (layer === 'combined'
                    ? (layerData?.combined ?? layerData)
                    : layerData?.[layer]) as {all?: string[]} | undefined;
                const all = (lData?.all || []) as string[];
                const inputVals = all
                    .map((path: string) => {
                        const m = path.match(/\.(day|month|year|yearnz)\.([0-9-]+[nz]*)(?:\.|$)/);
                        return m ? dateToInput(type, m[2]) : null;
                    })
                    .filter(Boolean) as string[];
                layerDateSets[layer] = new Set(inputVals);
            }

            const allSets = Object.values(layerDateSets);
            if (!allSets.length || !allSets.some((s) => s.size > 0)) continue;

            // Union of all layers' dates (selectable)
            const unionSet = new Set<string>();
            for (const s of allSets) for (const d of s) unionSet.add(d);

            // Intersection of all layers' dates (fully covered)
            const fullSet = new Set<string>([...allSets[0]].filter((d) => allSets.every((s) => s.has(d))));

            // Partial = union − full (some layers missing)
            const partialSet = new Set<string>([...unionSet].filter((d) => !fullSet.has(d)));

            // Which layers are missing for partial dates
            const missingLayers = new Set<string>();
            for (const d of partialSet) {
                for (const layer of layersToCheck) {
                    if (!layerDateSets[layer].has(d)) missingLayers.add(layer);
                }
            }

            const sortedUnion = [...unionSet].sort();
            if (!sortedUnion.length) continue;

            availableTypes.push(type);
            datesByType[type] = sortedUnion;
            partialDatesByType[type] = partialSet;
            missingLayersByType[type] = missingLayers;
        }

        if (!availableTypes.length) availableTypes.push('year');
        return {availableTypes, datesByType, partialDatesByType, missingLayersByType};
    }, [data?.files, layers]);

    const dates = datesByType[currentType] || [];
    const latestMax = dates[dates.length - 1];

    // '' means latest/current (symlink)
    const fromVal = from.date !== null ? dateToInput(currentType, from.date) : '';
    const toVal = to.date !== null ? dateToInput(to.type, to.date) : '';

    const latestLabel = latestMax ? t('current_' + currentType, {value: latestMax}) : t(currentType);

    const typeOptions: Option[] = availableTypes.map((pt) => ({value: pt, label: t(pt)}));
    const selectedType = typeOptions.find((o) => o.value === currentType) ?? null;

    const partialSet = partialDatesByType[currentType];

    const fromOptions: Option[] = useMemo(
        () => [{value: '', label: latestLabel}, ...[...dates].reverse().map((d) => ({value: d, label: labelFor(currentType, d), isPartial: partialSet?.has(d) ?? false}))],
        [dates, currentType, latestLabel, partialSet]
    );

    const toOptions: Option[] = useMemo(
        () => [...(fromVal ? dates.filter((d) => d >= fromVal) : dates)].reverse().map((d) => ({value: d, label: labelFor(currentType, d), isPartial: partialSet?.has(d) ?? false})),
        [dates, currentType, fromVal, partialSet]
    );

    const selectedFrom = fromOptions.find((o) => o.value === fromVal) ?? null;
    const selectedTo = toOptions.find((o) => o.value === toVal) ?? null;

    const onTypeChange = useCallback(
        (opt: SingleValue<Option>) => {
            if (opt) {
                setDateRange({start: opt.value, end: opt.value});
                setShowRange(false);
            }
        },
        [setDateRange]
    );

    const onFromChange = useCallback(
        (opt: SingleValue<Option>) => {
            const value = opt?.value ?? '';
            if (!value) {
                setDateRange({start: currentType, end: currentType});
            } else {
                const param = `${currentType}.${inputToDate(currentType, value)}`;
                setDateRange({start: param, end: showRange ? dateRange.end : param});
            }
        },
        [currentType, showRange, dateRange.end, setDateRange]
    );

    const onToChange = useCallback(
        (opt: SingleValue<Option>) => {
            const value = opt?.value ?? '';
            if (!value) {
                setDateRange({start: dateRange.start, end: currentType});
            } else {
                setDateRange({start: dateRange.start, end: `${currentType}.${inputToDate(currentType, value)}`});
            }
        },
        [currentType, dateRange.start, setDateRange]
    );

    const toggleRange = useCallback(() => {
        if (showRange) {
            setDateRange({start: dateRange.start, end: dateRange.start});
        }
        setShowRange((r) => !r);
    }, [showRange, dateRange.start, setDateRange]);

    // Shared picker callback (day and month both use YYYY-MM-DD / YYYY-MM string API)
    const onFromChangePicker = useCallback(
        (v: string | null) => {
            const param = v ? `${currentType}.${v}` : currentType;
            setDateRange({start: param, end: showRange ? dateRange.end : param});
        },
        [currentType, showRange, dateRange.end, setDateRange]
    );
    const onToChangePicker = useCallback(
        (v: string | null) => {
            setDateRange({start: dateRange.start, end: v ? `${currentType}.${v}` : currentType});
        },
        [currentType, dateRange.start, setDateRange]
    );

    // Available date/month sets for picker disabled-day highlighting
    const availableDates = useMemo(() => (currentType === 'day' ? new Set(dates) : undefined), [currentType, dates]);
    const toAvailableDates = useMemo(() => (currentType === 'day' ? new Set(fromVal ? dates.filter((d) => d >= fromVal) : dates) : undefined), [currentType, dates, fromVal]);
    const availableMonths = useMemo(() => (currentType === 'month' ? new Set(dates) : undefined), [currentType, dates]);
    const toAvailableMonths = useMemo(() => (currentType === 'month' ? new Set(fromVal ? dates.filter((d) => d >= fromVal) : dates) : undefined), [currentType, dates, fromVal]);

    // Partial date/month sets for picker orange-indicator
    const partialDates = useMemo(() => (currentType === 'day' ? (partialDatesByType['day'] ?? undefined) : undefined), [currentType, partialDatesByType]);
    const partialMonths = useMemo(() => (currentType === 'month' ? (partialDatesByType['month'] ?? undefined) : undefined), [currentType, partialDatesByType]);

    // Warning banner: does the selected range contain any partial-coverage dates?
    const hasPartialInRange = useMemo(() => {
        if (!from.date || !to.date) return false;
        const partials = partialDatesByType[currentType];
        if (!partials?.size) return false;
        const f = dateToInput(currentType, from.date);
        const t2 = dateToInput(currentType, to.date);
        for (const d of partials) {
            if (d >= f && d <= t2) return true;
        }
        return false;
    }, [from.date, to.date, currentType, partialDatesByType]);

    const missingLayersInRange = missingLayersByType[currentType];

    const btnStyle = (active: boolean): React.CSSProperties => ({
        flexShrink: 0,
        cursor: 'pointer',
        background: active ? '#d0e8ff' : 'transparent',
        border: '1px solid #ccc',
        borderRadius: '4px',
        padding: '4px 6px',
        fontSize: '13px',
        lineHeight: 1,
        whiteSpace: 'nowrap'
    });

    const fromPicker = (placeholder?: string) => {
        if (currentType === 'day') return <DayPicker value={fromVal || null} onChange={onFromChangePicker} availableDates={availableDates} partialDates={partialDates} placeholder={placeholder ?? latestLabel} />;
        if (currentType === 'month') return <MonthPicker value={fromVal || null} onChange={onFromChangePicker} availableMonths={availableMonths} partialMonths={partialMonths} placeholder={placeholder ?? latestLabel} />;
        return <Select options={fromOptions} value={selectedFrom} onChange={onFromChange} placeholder={placeholder} formatOptionLabel={formatPartialOption} />;
    };

    const toPicker = (placeholder?: string) => {
        if (currentType === 'day') return <DayPicker value={toVal || null} onChange={onToChangePicker} availableDates={toAvailableDates} partialDates={partialDates} placeholder={placeholder} />;
        if (currentType === 'month') return <MonthPicker value={toVal || null} onChange={onToChangePicker} availableMonths={toAvailableMonths} partialMonths={partialMonths} placeholder={placeholder} />;
        return <Select options={toOptions} value={selectedTo} onChange={onToChange} placeholder={placeholder} formatOptionLabel={formatPartialOption} />;
    };

    return (
        <>
            <b>{t('title')}:</b>
            {/* Row 1: type selector + date picker (single mode only) + range expand button */}
            <div style={{display: 'flex', alignItems: 'center', gap: '4px', marginTop: '4px'}}>
                <div style={{flex: '0 0 auto', minWidth: '100px'}}>
                    <Select options={typeOptions} value={selectedType} onChange={onTypeChange} isSearchable={false} />
                </div>
                {!showRange ? <div style={{flex: 1, minWidth: 0}}>{fromPicker()}</div> : null}
            </div>
            {/* Row 2 (range mode): from → to + collapse button */}
            <div style={{display: 'flex', alignItems: 'center', gap: '4px', marginTop: '4px'}}>
                {showRange ? (
                    <>
                        <div style={{flex: 1, minWidth: 0}}>{fromPicker(t('from'))}</div>
                        <span style={{flexShrink: 0}}>→</span>
                        <div style={{flex: 1, minWidth: 0}}>{toPicker(t('to'))}</div>
                        <button onClick={toggleRange} title={t('range_collapse')} style={btnStyle(true)}>
                            ✕
                        </button>
                    </>
                ) : (
                    <>
                        <button onClick={toggleRange} title={t('range_expand')} style={{...btnStyle(false), marginLeft: 'auto'}}>
                            📅→📅
                        </button>
                    </>
                )}
            </div>
            {/* Warning banner: partial coverage in selected range */}
            {hasPartialInRange && missingLayersInRange?.size ? (
                <div style={{marginTop: '4px', padding: '4px 8px', background: '#fff3cd', border: '1px solid #f0c040', borderRadius: '4px', fontSize: '0.85em'}}>
                    ⚠ {t('partial_warning', {layers: [...missingLayersInRange].map((l) => tLayer(l, l)).join(', ')})}
                </div>
            ) : null}
        </>
    );
}
