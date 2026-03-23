/**
 * APRS packet statistics — used for both per-station stats and the global aggregate.
 *
 * Written by the Rust server as dated daily JSON files:
 *   - Per-station: included in {station}.day.{date}.json under the `stats` key
 *   - Global: station-stats.{date}.json.gz in the stats directory
 *
 * The `hourly` field is a per-layer array of 24 hourly counts (UTC 0–23).
 */
export interface AprsPacketStats {
    /** All packets seen before any filtering */
    count: number;
    /** Packets that passed all filters and were written to coverage data */
    accepted: number;
    /** Sum of packet ages at receipt (server receive time − packet timestamp) in seconds,
     *  for accepted packets only. Divide by `accepted` to get mean packet age. */
    delaySumSecs: number;
    ignoredTracker: number;
    invalidTracker: number;
    invalidTimestamp: number;
    ignoredStationary: number;
    ignoredSignal0: number;
    ignoredPAW: number;
    ignoredH3stationary: number;
    ignoredElevation: number;
    ignoredFutureTimestamp: number;
    ignoredStaleTimestamp: number;
    /** Accepted packet counts by layer and hour-of-day (0–23). */
    hourly: Record<string, number[]>;
}

/** Shape of the JSON written to disk by the Rust server (includes timing metadata). */
export interface AprsPacketStatsJson extends AprsPacketStats {
    generated: string;
    startTime: string;
    uptimeSeconds: number;
}

/** Response shape returned by /api/station-stats */
export interface StationStatsApiResponse {
    /** Current day (or latest available) aggregate stats */
    current: AprsPacketStatsJson | null;
    /** Per-day hourly breakdown for charting */
    hourlyHistory: {date: string; hourly: Record<string, number[]>}[];
    /** Per-day accepted/raw counts for bar chart */
    dailyAccepted: {date: string; accepted: number; count: number}[];
}
