import {useState, useMemo, useEffect, useCallback, useRef, createContext, useContext} from 'react';
import Select, {GroupProps, OptionProps, components as SelectComponents} from 'react-select';

// ---- Date helpers ----

function isSameDay(a: Date, b: Date): boolean {
    return a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth() && a.getDate() === b.getDate();
}

function isSameMonth(a: Date, b: Date): boolean {
    return a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth();
}

export function toDateStr(d: Date): string {
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

export function toMonthStr(d: Date): string {
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
}

// ---- Intl helpers (evaluated once in the browser with the user's locale) ----

// Narrow weekday names Sunday→Saturday (Jan 1 2023 was a Sunday)
const WEEKDAY_NARROW = Array.from({length: 7}, (_, i) => new Intl.DateTimeFormat(undefined, {weekday: 'narrow'}).format(new Date(2023, 0, 1 + i)));

// Short month names for the month-grid display
const SHORT_MONTHS = Array.from({length: 12}, (_, i) => new Intl.DateTimeFormat(undefined, {month: 'short'}).format(new Date(2000, i, 1)));

// Prefix-autocomplete map built from locale month names
const suggestions: Record<string, string> = {};
for (const name of SHORT_MONTHS) {
    for (let i = 1; i < name.length; i++) suggestions[name.slice(0, i)] ??= name;
}
const suggest = (str: string) =>
    str
        .split(/\b/)
        .map((w) => suggestions[w] || w)
        .join('');

function parseInputAsDate(input: string): Date | null {
    const lower = suggest(input.trim().toLowerCase());
    // ISO: 2023-01 or 2023-01-15
    const iso = lower.match(/^(\d{4})-(\d{2})/);
    if (iso) return new Date(+iso[1], +iso[2] - 1, 1);
    // Locale month name prefix match
    const stripped = lower.replace(/\s+/g, '');
    const mIdx = SHORT_MONTHS.findIndex((m) => m.replace(/\s+/g, '').startsWith(stripped));
    if (mIdx !== -1) {
        const today = new Date();
        return new Date(today.getMonth() > mIdx ? today.getFullYear() + 1 : today.getFullYear(), mIdx, 1);
    }
    return null;
}

// ---- Nav context (shared prev/next for both pickers) ----

const NavContext = createContext<{prev: () => void; next: () => void} | null>(null);

// ---- Types ----

interface DateOption {
    date: Date;
    value: Date;
    label: string;
    display?: string;
    isDisabled?: boolean;
}

interface CalendarGroup {
    label: string;
    options: readonly DateOption[];
}

// ---- Option factories ----

function createOptionForDate(d: Date, availableDates?: Set<string>): DateOption {
    return {
        date: d,
        value: d,
        label: d.toLocaleDateString(undefined, {day: 'numeric', month: 'short', year: 'numeric'}),
        isDisabled: availableDates ? !availableDates.has(toDateStr(d)) : false
    };
}

function monthToDateOption(value: string, availableMonths?: Set<string>): DateOption {
    const d = new Date(value + '-01T12:00:00');
    return {
        date: d,
        value: d,
        label: d.toLocaleDateString(undefined, {month: 'short', year: 'numeric'}),
        isDisabled: availableMonths ? !availableMonths.has(value) : false
    };
}

function createCalendarOptions(date: Date, availableDates?: Set<string>): CalendarGroup {
    const year = date.getFullYear();
    const month = date.getMonth();
    const count = new Date(year, month + 1, 0).getDate();
    const options: DateOption[] = [];
    for (let i = 1; i <= count; i++) {
        const d = new Date(year, month, i);
        options.push({
            date: d,
            value: d,
            label: d.toLocaleDateString(undefined, {day: 'numeric', month: 'short', year: 'numeric'}),
            display: 'calendar-day',
            isDisabled: availableDates ? !availableDates.has(toDateStr(d)) : false
        });
    }
    return {label: date.toLocaleDateString(undefined, {month: 'short', year: 'numeric'}), options};
}

function createMonthOptions(year: number, availableMonths?: Set<string>): CalendarGroup {
    const options: DateOption[] = Array.from({length: 12}, (_, i) => {
        const d = new Date(year, i, 1);
        return {
            date: d,
            value: d,
            label: d.toLocaleDateString(undefined, {month: 'short', year: 'numeric'}),
            display: 'calendar-month',
            isDisabled: availableMonths ? !availableMonths.has(toMonthStr(d)) : false
        };
    });
    return {label: String(year), options};
}

// ---- Shared nav button style ----

const navBtnStyle: React.CSSProperties = {
    background: 'none',
    border: 'none',
    cursor: 'pointer',
    fontSize: '18px',
    lineHeight: 1,
    padding: '0 8px',
    color: '#555'
};

// ---- Group: month-nav header + weekday row (DayPicker) ----
// Each day cell uses width:'12%' margin:'1px 1%' → 14% per cell × 7 = 98% + 2% left padding = 100%

const DayGroup = (props: GroupProps<DateOption, false>) => {
    const nav = useContext(NavContext);
    const {children, label} = props;
    return (
        <div aria-label={label as string}>
            <div style={{display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '2px 4px'}}>
                <button
                    type="button"
                    onMouseDown={(e) => {
                        e.preventDefault();
                        nav?.prev();
                    }}
                    style={navBtnStyle}
                >
                    ‹
                </button>
                <span style={{fontWeight: 600, fontSize: '0.9em'}}>{label}</span>
                <button
                    type="button"
                    onMouseDown={(e) => {
                        e.preventDefault();
                        nav?.next();
                    }}
                    style={navBtnStyle}
                >
                    ›
                </button>
            </div>
            <div style={{paddingTop: 4, paddingLeft: '2%', borderTop: '1px solid #eee'}}>
                {WEEKDAY_NARROW.map((day, i) => (
                    <span key={i} style={{color: '#999', fontSize: '75%', fontWeight: 500, display: 'inline-block', width: '12%', margin: '0 1%', textAlign: 'center'}}>
                        {day}
                    </span>
                ))}
            </div>
            <div style={{paddingTop: 2, paddingLeft: '2%'}}>{children}</div>
        </div>
    );
};

// ---- Group: year-nav header (MonthPicker) ----

const MonthGroup = (props: GroupProps<DateOption, false>) => {
    const nav = useContext(NavContext);
    const {children, label} = props;
    return (
        <div aria-label={label as string}>
            <div style={{display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '2px 4px', borderBottom: '1px solid #eee'}}>
                <button
                    type="button"
                    onMouseDown={(e) => {
                        e.preventDefault();
                        nav?.prev();
                    }}
                    style={navBtnStyle}
                >
                    ‹
                </button>
                <span style={{fontWeight: 600, fontSize: '0.9em'}}>{label}</span>
                <button
                    type="button"
                    onMouseDown={(e) => {
                        e.preventDefault();
                        nav?.next();
                    }}
                    style={navBtnStyle}
                >
                    ›
                </button>
            </div>
            <div style={{paddingTop: 4}}>{children}</div>
        </div>
    );
};

// ---- Option ----

const Option = (props: OptionProps<DateOption, false>) => {
    const {data, getStyles, innerRef, innerProps} = props;

    if (data.display === 'calendar-day') {
        const {label: _label, ...base} = getStyles('option', props) as any;
        const style: React.CSSProperties = {
            ...base,
            display: 'inline-block',
            width: '12%',
            margin: '1px 1%',
            // Override base padding (react-select default ~8px 12px) — must come after ...base spread
            padding: '3px 0',
            textAlign: 'center',
            borderRadius: 4,
            boxSizing: 'border-box',
            ...(data.isDisabled ? {opacity: 0.3, cursor: 'not-allowed', pointerEvents: 'none'} : {})
        };
        if (data.date.getDate() === 1) {
            const indentBy = data.date.getDay();
            // Each cell = 14% (12% width + 1% each side margin); add 1% for the cell's own left margin
            if (indentBy) style.marginLeft = `${indentBy * 14 + 1}%`;
        }
        return (
            <span ref={innerRef} {...(data.isDisabled ? {} : innerProps)} style={style}>
                {data.date.getDate()}
            </span>
        );
    }

    if (data.display === 'calendar-month') {
        const {label: _label, ...base} = getStyles('option', props) as any;
        const style: React.CSSProperties = {
            ...base,
            display: 'inline-flex',
            alignItems: 'center',
            justifyContent: 'center',
            // 4 per row: (25% - margin*2) × 4 = 100%
            width: 'calc(25% - 6px)',
            margin: '3px',
            padding: '5px 0',
            textAlign: 'center',
            borderRadius: 4,
            fontSize: '0.85em',
            boxSizing: 'border-box',
            ...(data.isDisabled ? {opacity: 0.3, cursor: 'not-allowed', pointerEvents: 'none'} : {})
        };
        return (
            <span ref={innerRef} {...(data.isDisabled ? {} : innerProps)} style={style}>
                {SHORT_MONTHS[data.date.getMonth()]}
            </span>
        );
    }

    return <SelectComponents.Option {...props} />;
};

// ---- DatePicker (internal, days) ----

interface DatePickerProps {
    value: DateOption | null;
    onChange: (v: DateOption | null) => void;
    availableDates?: Set<string>;
    placeholder?: string;
}

function DatePicker({value, onChange, availableDates, placeholder}: DatePickerProps) {
    const [calendarDate, setCalendarDate] = useState<Date>(() => {
        if (value) return new Date(value.date.getFullYear(), value.date.getMonth(), 1);
        if (availableDates?.size) return new Date([...availableDates].sort().pop()! + 'T12:00:00');
        return new Date();
    });

    useEffect(() => {
        if (value) setCalendarDate(new Date(value.date.getFullYear(), value.date.getMonth(), 1));
    }, [value]);

    // Navigate to latest available month when data loads (only if no value yet)
    useEffect(() => {
        if (value || !availableDates?.size) return;
        setCalendarDate(new Date([...availableDates].sort().pop()! + 'T12:00:00'));
    }, [availableDates]); // eslint-disable-line react-hooks/exhaustive-deps

    const prev = useCallback(() => setCalendarDate((d) => new Date(d.getFullYear(), d.getMonth() - 1, 1)), []);
    const next = useCallback(() => setCalendarDate((d) => new Date(d.getFullYear(), d.getMonth() + 1, 1)), []);
    const nav = useMemo(() => ({prev, next}), [prev, next]);

    const calendarOptions = useMemo(() => createCalendarOptions(calendarDate, availableDates), [calendarDate, availableDates]);

    const handleInputChange = (input: string) => {
        if (!input) return;
        const date = parseInputAsDate(input);
        if (date) setCalendarDate(new Date(date.getFullYear(), date.getMonth(), 1));
    };

    return (
        <NavContext.Provider value={nav}>
            <Select<DateOption, false> components={{Group: DayGroup, Option}} filterOption={null} isMulti={false} isOptionDisabled={(o) => !!o.isDisabled} isOptionSelected={(o, v) => v.some((i) => isSameDay(i.date, o.date))} maxMenuHeight={380} onChange={onChange} onInputChange={handleInputChange} options={[calendarOptions]} placeholder={placeholder} value={value} />
        </NavContext.Provider>
    );
}

// ---- MonthPickerInternal (internal, months) ----

interface MonthPickerInternalProps {
    value: DateOption | null;
    onChange: (v: DateOption | null) => void;
    availableMonths?: Set<string>;
    placeholder?: string;
}

function MonthPickerInternal({value, onChange, availableMonths, placeholder}: MonthPickerInternalProps) {
    const [calendarYear, setCalendarYear] = useState<number>(() => {
        if (value) return value.date.getFullYear();
        if (availableMonths?.size) return parseInt([...availableMonths].sort().pop()!.slice(0, 4));
        return new Date().getFullYear();
    });

    useEffect(() => {
        if (value) setCalendarYear(value.date.getFullYear());
    }, [value]);

    useEffect(() => {
        if (value || !availableMonths?.size) return;
        setCalendarYear(parseInt([...availableMonths].sort().pop()!.slice(0, 4)));
    }, [availableMonths]); // eslint-disable-line react-hooks/exhaustive-deps

    const prev = useCallback(() => setCalendarYear((y) => y - 1), []);
    const next = useCallback(() => setCalendarYear((y) => y + 1), []);
    const nav = useMemo(() => ({prev, next}), [prev, next]);

    const monthOptions = useMemo(() => createMonthOptions(calendarYear, availableMonths), [calendarYear, availableMonths]);

    // Allow typing a year number to navigate
    const handleInputChange = (input: string) => {
        if (!input) return;
        const year = parseInt(input);
        if (!isNaN(year) && year >= 1900 && year <= 2100) setCalendarYear(year);
    };

    return (
        <NavContext.Provider value={nav}>
            <Select<DateOption, false> components={{Group: MonthGroup, Option}} filterOption={null} isMulti={false} isOptionDisabled={(o) => !!o.isDisabled} isOptionSelected={(o, v) => v.some((i) => isSameMonth(i.date, o.date))} maxMenuHeight={220} onChange={onChange} onInputChange={handleInputChange} options={[monthOptions]} placeholder={placeholder} value={value} />
        </NavContext.Provider>
    );
}

// ---- DayPicker: YYYY-MM-DD string API ----

export function DayPicker({value, onChange, availableDates, placeholder}: {value: string | null; onChange: (v: string | null) => void; availableDates?: Set<string>; placeholder?: string}) {
    // Auto-select the latest available date on first load when no value is set
    const didAutoSelect = useRef(false);
    useEffect(() => {
        if (didAutoSelect.current || value !== null || !availableDates?.size) return;
        didAutoSelect.current = true;
        onChange([...availableDates].sort().pop()!);
    }, [availableDates]); // eslint-disable-line react-hooks/exhaustive-deps

    const dateOption = value ? createOptionForDate(new Date(value + 'T12:00:00'), availableDates) : null;
    return <DatePicker value={dateOption} onChange={(opt) => onChange(opt ? toDateStr(opt.date) : null)} availableDates={availableDates} placeholder={placeholder} />;
}

// ---- MonthPicker: YYYY-MM string API ----

export function MonthPicker({value, onChange, availableMonths, placeholder}: {value: string | null; onChange: (v: string | null) => void; availableMonths?: Set<string>; placeholder?: string}) {
    // Auto-select the latest available month on first load when no value is set
    const didAutoSelect = useRef(false);
    useEffect(() => {
        if (didAutoSelect.current || value !== null || !availableMonths?.size) return;
        didAutoSelect.current = true;
        onChange([...availableMonths].sort().pop()!);
    }, [availableMonths]); // eslint-disable-line react-hooks/exhaustive-deps

    const dateOption = value ? monthToDateOption(value, availableMonths) : null;
    return <MonthPickerInternal value={dateOption} onChange={(opt) => onChange(opt ? toMonthStr(opt.date) : null)} availableMonths={availableMonths} placeholder={placeholder} />;
}

export default function DatePickerDemo() {
    const [value, setValue] = useState<DateOption | null>(null);
    return <DatePicker value={value} onChange={setValue} />;
}
