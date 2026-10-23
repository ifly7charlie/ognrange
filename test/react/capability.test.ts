import {describe, it, expect} from 'vitest';

import {capabilityMaxRangeKm, capabilityRange, K_DECADES_PER_DB, S_REF_DB, CAPABILITY_MAX_RANGE_KM, CAPABILITY_MIN_RANGE_KM} from '../../lib/common/capability';
import {GROUND_MAX_KM} from '../../lib/react/coveragedetails/grounddata';

describe('capabilityMaxRangeKm', () => {
    it('keeps the full range when capability is unknown', () => {
        expect(capabilityMaxRangeKm(null)).toBe(CAPABILITY_MAX_RANGE_KM);
        expect(capabilityMaxRangeKm(undefined)).toBe(CAPABILITY_MAX_RANGE_KM);
        // The stations arrow encodes "never reported" as NaN in the values buffer
        expect(capabilityMaxRangeKm(NaN)).toBe(CAPABILITY_MAX_RANGE_KM);
    });

    it('gives the network-median receiver the full range', () => {
        expect(capabilityMaxRangeKm(S_REF_DB)).toBe(CAPABILITY_MAX_RANGE_KM);
    });

    it('clamps strong receivers at the terrain-ray limit', () => {
        expect(capabilityMaxRangeKm(S_REF_DB + 10)).toBe(CAPABILITY_MAX_RANGE_KM);
    });

    it('scales weak receivers down by the fitted decades-per-dB', () => {
        const r = capabilityMaxRangeKm(S_REF_DB - 10);
        expect(r).toBeCloseTo(CAPABILITY_MAX_RANGE_KM * Math.pow(10, -10 * K_DECADES_PER_DB), 6);
        expect(r).toBeGreaterThan(CAPABILITY_MIN_RANGE_KM);
        expect(r).toBeLessThan(CAPABILITY_MAX_RANGE_KM);
    });

    it('clamps very deaf receivers at the floor', () => {
        expect(capabilityMaxRangeKm(-40)).toBe(CAPABILITY_MIN_RANGE_KM);
    });

    it('is monotonic non-decreasing in capability', () => {
        let prev = 0;
        for (let db = -30; db <= 40; db += 0.5) {
            const r = capabilityMaxRangeKm(db);
            expect(r).toBeGreaterThanOrEqual(prev);
            prev = r;
        }
    });

    it('pins the deliberate GROUND_MAX_KM duplication', () => {
        // lib/common must not import from lib/react, so the constant is
        // duplicated - this fails if the two ever diverge
        expect(CAPABILITY_MAX_RANGE_KM).toBe(GROUND_MAX_KM);
    });
});

describe('capabilityRange', () => {
    it('reports no model output when capability is unknown', () => {
        for (const db of [null, undefined, NaN]) {
            const r = capabilityRange(db);
            expect(r.km).toBe(CAPABILITY_MAX_RANGE_KM);
            expect(r.modelKm).toBeNull();
            expect(r.clamped).toBeNull();
        }
    });

    it('passes an in-range model value through unclamped', () => {
        const r = capabilityRange(S_REF_DB - 10);
        expect(r.clamped).toBeNull();
        expect(r.modelKm).toBe(r.km);
        expect(r.km).toBeCloseTo(CAPABILITY_MAX_RANGE_KM * Math.pow(10, -10 * K_DECADES_PER_DB), 6);
    });

    it('flags the terrain-data cap for strong receivers, keeping the raw model value', () => {
        const r = capabilityRange(S_REF_DB + 10);
        expect(r.clamped).toBe('max');
        expect(r.km).toBe(CAPABILITY_MAX_RANGE_KM);
        expect(r.modelKm).toBeCloseTo(CAPABILITY_MAX_RANGE_KM * Math.pow(10, 10 * K_DECADES_PER_DB), 6);
    });

    it('flags the minimum floor for very deaf receivers', () => {
        const r = capabilityRange(-40);
        expect(r.clamped).toBe('min');
        expect(r.km).toBe(CAPABILITY_MIN_RANGE_KM);
        expect(r.modelKm).toBeLessThan(CAPABILITY_MIN_RANGE_KM);
    });

    it('treats the exact median as unclamped', () => {
        const r = capabilityRange(S_REF_DB);
        expect(r.clamped).toBeNull();
        expect(r.km).toBe(CAPABILITY_MAX_RANGE_KM);
    });

    it('agrees with capabilityMaxRangeKm everywhere', () => {
        for (let db = -30; db <= 40; db += 1) {
            expect(capabilityRange(db).km).toBe(capabilityMaxRangeKm(db));
        }
    });
});
