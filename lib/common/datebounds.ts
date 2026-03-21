export type PeriodType = 'year' | 'yearnz' | 'month' | 'day';
export const ALL_PERIOD_TYPES: PeriodType[] = ['year', 'yearnz', 'month', 'day'];

/** Parse a period param like "month.2026-03", "year.2026", "day.2026-03-18"
 *  into its type and optional value. Used by all stats API handlers. */
export function parsePeriodParam(param: string): {type: string; value: string | null} {
    const dot = param.indexOf('.');
    return dot === -1 ? {type: param, value: null} : {type: param.slice(0, dot), value: param.slice(dot + 1)};
}

// Convert app date param (e.g. "year", "month.2026-03", "day.2026-03-15") to YYYY-MM-DD bounds
export function dateBounds(param: string): {start: string; end: string} | null {
    const dot = param.indexOf('.');
    const type = dot === -1 ? param : param.slice(0, dot);
    const date = dot === -1 ? null : param.slice(dot + 1);

    const now = new Date();
    switch (type) {
        case 'year': {
            const y = date || now.getFullYear().toString();
            return {start: `${y}-01-01`, end: `${y}-12-31`};
        }
        case 'yearnz': {
            const y = date ? parseInt(date.replace('nz', '')) : now.getFullYear();
            return {start: `${y - 1}-07-01`, end: `${y}-06-30`};
        }
        case 'month': {
            const m = date || now.toISOString().slice(0, 7);
            const [y2, m2] = m.split('-').map(Number);
            const lastDay = new Date(y2, m2, 0).getDate();
            return {start: `${m}-01`, end: `${m}-${String(lastDay).padStart(2, '0')}`};
        }
        case 'day': {
            const d = date || now.toISOString().slice(0, 10);
            return {start: d, end: d};
        }
    }
    return null;
}
