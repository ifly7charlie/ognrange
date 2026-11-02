import {describe, it, expect} from 'vitest';
import {parseQuery} from '../../lib/api/sanitise';

const cell = '881f826dddfffff';

describe('parseQuery', () => {
    it('defaults missing parameters', () => {
        expect(parseQuery({})).toEqual({query: {station: '', h3: '', file: '', dateStart: '', dateEnd: '', layers: ['combined'], locked: false}});
    });

    describe('station', () => {
        it('accepts callsign characters', () => expect(parseQuery({station: 'LFLE-2_x'}).query?.station).toBe('LFLE-2_x'));
        it('accepts global', () => expect(parseQuery({station: 'global'}).query?.station).toBe('global'));
        it('rejects path traversal', () => expect(parseQuery({station: '../etc'}).error).toBeDefined());
        it('rejects separators', () => expect(parseQuery({station: 'a/b'}).error).toBeDefined());
        it('rejects repeated parameter', () => expect(parseQuery({station: ['A', 'B']}).error).toBeDefined());
        it('rejects missing when required', () => expect(parseQuery({}, {station: true}).error).toBeDefined());
    });

    describe('h3', () => {
        it('accepts a valid cell', () => expect(parseQuery({h3: cell}).query?.h3).toBe(cell));
        it('rejects garbage', () => expect(parseQuery({h3: 'zzz'}).error).toBeDefined());
        it('rejects missing when required', () => expect(parseQuery({}, {h3: true}).error).toBeDefined());
    });

    describe('periods', () => {
        it('accepts period tokens', () => {
            const {query} = parseQuery({file: 'year', dateStart: 'month.2026-03', dateEnd: 'day.2026-03-12.flarm'});
            expect(query).toMatchObject({file: 'year', dateStart: 'month.2026-03', dateEnd: 'day.2026-03-12.flarm'});
        });
        it('accepts the undefined placeholder the frontend sends', () => expect(parseQuery({file: 'undefined'}).error).toBeUndefined());
        it('rejects ..', () => expect(parseQuery({file: 'year..2026'}).error).toBeDefined());
        it('rejects separators in a date', () => expect(parseQuery({dateStart: 'year.2026/../../x'}).error).toBeDefined());
        it('rejects a leading dot', () => expect(parseQuery({dateEnd: '.hidden'}).error).toBeDefined());
    });

    describe('layers', () => {
        it('splits and trims', () => expect(parseQuery({layers: 'combined, flarm'}).query?.layers).toEqual(['combined', 'flarm']));
        it('drops unknown layers', () => expect(parseQuery({layers: 'flarm,bogus'}).query?.layers).toEqual(['flarm']));
    });

    describe('lockedH3', () => {
        it('is true for 1', () => expect(parseQuery({lockedH3: '1'}).query?.locked).toBe(true));
        it('is false for 0', () => expect(parseQuery({lockedH3: '0'}).query?.locked).toBe(false));
    });
});
