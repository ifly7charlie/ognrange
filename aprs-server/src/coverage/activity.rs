//! Activity tracking for rollup operations.
//!
//! Tracks contiguous coverage ranges with firstSeen/lastSeen timestamps,
//! capped at 500 ranges. Mirrors `lib/worker/rollupactivity.ts`.

use crate::types::Epoch;
use serde::{Deserialize, Serialize};

const MAX_ACTIVITY_RANGES: usize = 500;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ActivityRange {
    pub start: u32,
    pub end: u32,
    pub cells: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RollupActivity {
    pub ranges: Vec<ActivityRange>,
    pub total_rollups: u32,
    pub active_rollups: u32,
    pub total_cells: u32,
    pub first_seen: u32,
    pub last_seen: u32,
    pub last_rollup: u32,
}

impl Default for RollupActivity {
    fn default() -> Self {
        RollupActivity {
            ranges: Vec::new(),
            total_rollups: 0,
            active_rollups: 0,
            total_cells: 0,
            first_seen: 0,
            last_seen: 0,
            last_rollup: 0,
        }
    }
}

/// Update activity tracking after a rollup period.
///
/// - `activity`: mutable activity state (loaded from DB or fresh)
/// - `h3source`: number of H3 cells with data (0 = no data this period)
/// - `period_start`: start of the rollup period (epoch seconds)
/// - `period_end`: end of the rollup period (epoch seconds)
/// - `now`: current timestamp (epoch seconds)
pub fn update_activity(
    activity: &mut RollupActivity,
    h3source: u32,
    period_start: Epoch,
    period_end: Epoch,
    now: Epoch,
) {
    activity.total_rollups += 1;
    activity.last_rollup = now.0;

    if h3source > 0 {
        activity.active_rollups += 1;
        activity.total_cells += h3source;

        if activity.first_seen == 0 {
            activity.first_seen = period_start.0;
        }
        activity.last_seen = period_end.0;

        // Extend existing range if contiguous, otherwise create new
        if let Some(last) = activity.ranges.last_mut() {
            if last.end == period_start.0 {
                last.end = period_end.0;
                last.cells += h3source;
            } else {
                activity.ranges.push(ActivityRange {
                    start: period_start.0,
                    end: period_end.0,
                    cells: h3source,
                });
            }
        } else {
            activity.ranges.push(ActivityRange {
                start: period_start.0,
                end: period_end.0,
                cells: h3source,
            });
        }

        // Cap at MAX_ACTIVITY_RANGES, dropping oldest
        if activity.ranges.len() > MAX_ACTIVITY_RANGES {
            let excess = activity.ranges.len() - MAX_ACTIVITY_RANGES;
            activity.ranges.drain(..excess);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PERIOD: u32 = 12 * 60; // 12 minutes in seconds

    #[test]
    fn test_first_rollup_with_data() {
        let mut activity = RollupActivity::default();
        update_activity(&mut activity, 5, Epoch(1000), Epoch(1000 + PERIOD), Epoch(2000));

        assert_eq!(activity.ranges.len(), 1);
        assert_eq!(activity.ranges[0], ActivityRange { start: 1000, end: 1000 + PERIOD, cells: 5 });
        assert_eq!(activity.total_rollups, 1);
        assert_eq!(activity.active_rollups, 1);
        assert_eq!(activity.total_cells, 5);
        assert_eq!(activity.first_seen, 1000);
        assert_eq!(activity.last_seen, 1000 + PERIOD);
        assert_eq!(activity.last_rollup, 2000);
    }

    #[test]
    fn test_contiguous_rollup_extends_range() {
        let mut activity = RollupActivity::default();
        let t0 = 1000u32;
        let t1 = t0 + PERIOD;
        let t2 = t1 + PERIOD;

        update_activity(&mut activity, 3, Epoch(t0), Epoch(t1), Epoch(2000));
        update_activity(&mut activity, 7, Epoch(t1), Epoch(t2), Epoch(3000));

        assert_eq!(activity.ranges.len(), 1);
        assert_eq!(activity.ranges[0], ActivityRange { start: 1000, end: t2, cells: 10 });
        assert_eq!(activity.active_rollups, 2);
        assert_eq!(activity.total_cells, 10);
        assert_eq!(activity.last_seen, t2);
    }

    #[test]
    fn test_gap_creates_new_range() {
        let mut activity = RollupActivity::default();
        let t0 = 1000u32;
        let t1 = t0 + PERIOD;
        let t2 = t1 + PERIOD;
        let t3 = t2 + PERIOD;

        update_activity(&mut activity, 2, Epoch(t0), Epoch(t1), Epoch(2000));
        update_activity(&mut activity, 0, Epoch(t1), Epoch(t2), Epoch(3000)); // no data
        update_activity(&mut activity, 4, Epoch(t2), Epoch(t3), Epoch(4000));

        assert_eq!(activity.ranges.len(), 2);
        assert_eq!(activity.ranges[0], ActivityRange { start: 1000, end: t1, cells: 2 });
        assert_eq!(activity.ranges[1], ActivityRange { start: t2, end: t3, cells: 4 });
        assert_eq!(activity.total_rollups, 3);
        assert_eq!(activity.active_rollups, 2);
    }

    #[test]
    fn test_no_data_increments_total_only() {
        let mut activity = RollupActivity::default();
        update_activity(&mut activity, 0, Epoch(1000), Epoch(1000 + PERIOD), Epoch(2000));

        assert_eq!(activity.ranges.len(), 0);
        assert_eq!(activity.total_rollups, 1);
        assert_eq!(activity.active_rollups, 0);
        assert_eq!(activity.total_cells, 0);
        assert_eq!(activity.first_seen, 0);
        assert_eq!(activity.last_seen, 0);
        assert_eq!(activity.last_rollup, 2000);
    }

    #[test]
    fn test_caps_at_500_dropping_oldest() {
        let mut activity = RollupActivity::default();
        // Create 501 non-contiguous ranges (gap between each)
        for i in 0..501u32 {
            let start = i * PERIOD * 2; // gap between each
            let end = start + PERIOD;
            update_activity(&mut activity, 1, Epoch(start), Epoch(end), Epoch(end));
        }

        assert_eq!(activity.ranges.len(), 500);
        // First range should be the second one we added (index 1), not index 0
        assert_eq!(activity.ranges[0].start, 1 * PERIOD * 2);
        assert_eq!(activity.total_rollups, 501);
        assert_eq!(activity.active_rollups, 501);
        assert_eq!(activity.total_cells, 501);
    }
}
