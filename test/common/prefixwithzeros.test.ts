import {describe, it, expect} from 'vitest';
import {prefixWithZeros} from '../../lib/common/prefixwithzeros';

describe('prefixWithZeros', () => {
    it('pads shorter strings to requested length', () => {
        expect(prefixWithZeros(4, 'ab')).toBe('00ab');
        expect(prefixWithZeros(8, '1')).toBe('00000001');
        expect(prefixWithZeros(3, 'x')).toBe('00x');
    });

    it('returns string unchanged if already at target length', () => {
        expect(prefixWithZeros(4, 'abcd')).toBe('abcd');
    });

    it('returns string unchanged if beyond target length', () => {
        expect(prefixWithZeros(2, 'abcd')).toBe('abcd');
    });

    it('handles empty string input', () => {
        expect(prefixWithZeros(4, '')).toBe('0000');
        expect(prefixWithZeros(0, '')).toBe('');
    });
});
