import {describe, it, expect} from 'vitest';
import {Layer, layerFromDestCallsign, dbKeyPrefix, layerFromPrefix, getWriteLayers, parseEnabledLayers, PRESENCE_ONLY, ALL_LAYERS} from '../../lib/common/layers';

describe('layers', () => {
    describe('layerFromDestCallsign', () => {
        it('maps OGFLR to FLARM', () => expect(layerFromDestCallsign('OGFLR')).toBe(Layer.FLARM));
        it('maps OGADSB to ADSB', () => expect(layerFromDestCallsign('OGADSB')).toBe(Layer.ADSB));
        it('maps OGADSL to ADSL', () => expect(layerFromDestCallsign('OGADSL')).toBe(Layer.ADSL));
        it('maps OGNFNT to FANET', () => expect(layerFromDestCallsign('OGNFNT')).toBe(Layer.FANET));
        it('maps OGNTRK to OGNTRK', () => expect(layerFromDestCallsign('OGNTRK')).toBe(Layer.OGNTRK));
        it('maps OGPAW to PAW', () => expect(layerFromDestCallsign('OGPAW')).toBe(Layer.PAW));
        it('returns null for unknown callsigns', () => expect(layerFromDestCallsign('OGNSDR')).toBeNull());
        it('returns null for empty string', () => expect(layerFromDestCallsign('')).toBeNull());
    });

    describe('dbKeyPrefix', () => {
        it('returns a/ for ADSB', () => expect(dbKeyPrefix(Layer.ADSB)).toBe('a/'));
        it('returns c/ for COMBINED', () => expect(dbKeyPrefix(Layer.COMBINED)).toBe('c/'));
        it('returns d/ for ADSL', () => expect(dbKeyPrefix(Layer.ADSL)).toBe('d/'));
        it('returns f/ for FLARM', () => expect(dbKeyPrefix(Layer.FLARM)).toBe('f/'));
        it('returns n/ for FANET', () => expect(dbKeyPrefix(Layer.FANET)).toBe('n/'));
        it('returns p/ for PAW', () => expect(dbKeyPrefix(Layer.PAW)).toBe('p/'));
        it('returns t/ for OGNTRK', () => expect(dbKeyPrefix(Layer.OGNTRK)).toBe('t/'));
    });

    describe('layerFromPrefix', () => {
        it('round-trips through dbKeyPrefix', () => {
            for (const layer of ALL_LAYERS) {
                const prefix = dbKeyPrefix(layer);
                expect(layerFromPrefix(prefix[0])).toBe(layer);
            }
        });
        it('returns null for unknown prefix', () => expect(layerFromPrefix('x')).toBeNull());
    });

    describe('getWriteLayers', () => {
        it('FLARM writes to COMBINED and FLARM', () => {
            const layers = getWriteLayers(Layer.FLARM);
            expect(layers).toContain(Layer.COMBINED);
            expect(layers).toContain(Layer.FLARM);
            expect(layers).toHaveLength(2);
        });

        it('OGNTRK writes to COMBINED and OGNTRK', () => {
            const layers = getWriteLayers(Layer.OGNTRK);
            expect(layers).toContain(Layer.COMBINED);
            expect(layers).toContain(Layer.OGNTRK);
            expect(layers).toHaveLength(2);
        });

        it('ADSB writes only to ADSB', () => {
            expect(getWriteLayers(Layer.ADSB)).toStrictEqual([Layer.ADSB]);
        });

        it('PAW writes only to PAW', () => {
            expect(getWriteLayers(Layer.PAW)).toStrictEqual([Layer.PAW]);
        });

        it('ADSL writes only to ADSL', () => {
            expect(getWriteLayers(Layer.ADSL)).toStrictEqual([Layer.ADSL]);
        });

        it('FANET writes only to FANET', () => {
            expect(getWriteLayers(Layer.FANET)).toStrictEqual([Layer.FANET]);
        });
    });

    describe('PRESENCE_ONLY', () => {
        it('includes ADSB and PAW', () => {
            expect(PRESENCE_ONLY.has(Layer.ADSB)).toBe(true);
            expect(PRESENCE_ONLY.has(Layer.PAW)).toBe(true);
        });
        it('does not include FLARM', () => {
            expect(PRESENCE_ONLY.has(Layer.FLARM)).toBe(false);
        });
    });

    describe('ALL_LAYERS', () => {
        it('contains 7 layers', () => expect(ALL_LAYERS).toHaveLength(7));
        it('is in DB sort order (alphabetical by prefix)', () => {
            const prefixes = ALL_LAYERS.map((l) => dbKeyPrefix(l));
            const sorted = [...prefixes].sort();
            expect(prefixes).toStrictEqual(sorted);
        });
    });

    describe('parseEnabledLayers', () => {
        it('returns null for undefined', () => expect(parseEnabledLayers(undefined)).toBeNull());
        it('returns null for empty string', () => expect(parseEnabledLayers('')).toBeNull());
        it('parses single layer', () => {
            const result = parseEnabledLayers('combined');
            expect(result).not.toBeNull();
            expect(result!.has(Layer.COMBINED)).toBe(true);
            expect(result!.size).toBe(1);
        });
        it('parses multiple layers', () => {
            const result = parseEnabledLayers('combined,flarm,adsb');
            expect(result).not.toBeNull();
            expect(result!.has(Layer.COMBINED)).toBe(true);
            expect(result!.has(Layer.FLARM)).toBe(true);
            expect(result!.has(Layer.ADSB)).toBe(true);
            expect(result!.size).toBe(3);
        });
        it('ignores unknown names', () => {
            const result = parseEnabledLayers('combined,unknown');
            expect(result!.size).toBe(1);
        });
        it('handles spaces', () => {
            const result = parseEnabledLayers(' combined , flarm ');
            expect(result!.has(Layer.COMBINED)).toBe(true);
            expect(result!.has(Layer.FLARM)).toBe(true);
        });
    });
});
