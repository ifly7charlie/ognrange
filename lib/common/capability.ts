// Receiver-capability range model, from the 2026-08 RF analysis: the
// cumulative +X.XdB@10km status-beacon figure predicts achieved range at
// ~0.044 decades/dB (~89% of the free-space 0.050), so
// R = R_REF * 10^(K * (s - S_REF)), clamped. Applies to the 868 MHz frequency
// group only - the figure describes the OGN SDR chain, not an ADS-B receiver

export const K_DECADES_PER_DB = 0.044;
// Network median capability; a station at the median keeps the full range
export const S_REF_DB = 13.1;
// Must equal grounddata.ts GROUND_MAX_KM (the terrain rays' physical end);
// duplicated because lib/common must not import from lib/react - pinned by a
// test in test/react/capability.test.ts
export const CAPABILITY_MAX_RANGE_KM = 120;
export const CAPABILITY_MIN_RANGE_KM = 10;

export interface CapabilityRange {
    /** Range the map disc and charts use (model output clamped into [MIN, MAX]) */
    km: number;
    /** Raw model output before clamping; null when the station never reported a value */
    modelKm: number | null;
    /** Which clamp bound is in force; null when the model value is used directly (or no data) */
    clamped: 'min' | 'max' | null;
}

/**
 * Range model for a receiver's reported average signal at 10 km, with the
 * clamp state kept visible so the UI can say whether the shown range is the
 * model output, the 120 km visualisation limit, or the display floor.
 * null/undefined/NaN (never reported, or an old stations file without the
 * column) keeps the previous fixed-range behaviour.
 */
export function capabilityRange(db: number | null | undefined): CapabilityRange {
    if (db == null || Number.isNaN(db)) {
        return {km: CAPABILITY_MAX_RANGE_KM, modelKm: null, clamped: null};
    }
    const modelKm = CAPABILITY_MAX_RANGE_KM * Math.pow(10, K_DECADES_PER_DB * (db - S_REF_DB));
    const km = Math.max(CAPABILITY_MIN_RANGE_KM, Math.min(CAPABILITY_MAX_RANGE_KM, modelKm));
    return {km, modelKm, clamped: modelKm > CAPABILITY_MAX_RANGE_KM ? 'max' : modelKm < CAPABILITY_MIN_RANGE_KM ? 'min' : null};
}

/** Maximum useful range (km) for a receiver of the given capability */
export function capabilityMaxRangeKm(db: number | null | undefined): number {
    return capabilityRange(db).km;
}
