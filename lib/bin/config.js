

import dotenv from 'dotenv';

// Load the configuration from a file
dotenv.config({ path: '.env.local', override: true })

/* c8 ignore start */
function fixTrailingSlash(o) {
	return o + ((!o.match(/\/$/))? '/' : '');
}


// Database and output paths
export let DB_PATH = fixTrailingSlash(process.env.DB_PATH||'./db');
export let OUTPUT_PATH = fixTrailingSlash(process.env.OUTPUT_PATH||'./data');


// APRS Server Keep Alive
export const APRS_KEEPALIVE_PERIOD_MS = (process.env.APRS_KEEPALIVE_PERIOD_MINUTES||2) * 60 * 1000;
export const APRS_TRAFFIC_FILTER = (process.env.APRS_TRAFFIC_FILTER||'t/spuoimnwt');
export const APRS_SERVER = (process.env.APRS_TRAFFIC_FILTER||'aprs.glidernet.org:14580');

/*
# ROLLUP is when the current accumulators are merged with the daily/monthly/annual
# accumulators. All are done at the same time and the accumulators are 'rolled'
# over to prevent double counting. This is a fairly costly activity so if the
# disk or cpu load goes too high during this process (it potentially reads and 
# writes EVERYTHING in every database) you should increase this number */
export const ROLLUP_PERIOD_MINUTES = (process.env.ROLLUP_PERIOD_MINUTES||(3*60));
export const ACCUMULATOR_CHANGEOVER_CHECK_PERIOD_MS = (process.env.ACCUMULATOR_CHANGEOVER_CHECK_PERIOD_SECONDS||60)*1000;

// how much detail to collect, bigger numbers = more cells! goes up fast see
// https://h3geo.org/docs/core-library/restable for what the sizes mean
export const H3_STATION_CELL_LEVEL = (process.env.H3_STATION_CELL_LEVEL||8);
export const H3_GLOBAL_CELL_LEVEL = (process.env.H3_GLOBAL_CELL_LEVEL||7);

// # We keep maps of when we last saw aircraft and where so we can determine the
// # timegap prior to the packet, this is sort of a proxy for the 'edge' of coverage
// # however we don't need to know this forever so we should forget them after
// # a while. The signfigence of forgetting is we will assume no gap before the
// # first packet for the first aircraft/station pair. Doesn't start running
// # until approximately this many hours have passed
export const FORGET_AIRCRAFT_AFTER_SEC = (process.env.FORGET_AIRCRAFT_AFTER_HOURS||12)*3600;

// How far a station is allowed to move without resetting the history for it
export const STATION_MOVE_THRESHOLD_KM = (process.env.STATION_MOVE_THRESHOLD_KM||2);

// If we haven't had traffic in this long then we expire the station
export const STATION_EXPIRY_TIME_SECS = (process.env.STATION_EXPIRY_TIME_DAYS||31)*3600*24;


export const MAX_STATION_DBS =  parseInt(process.env.MAX_STATION_DBS)||3200
export const STATION_DB_EXPIRY_MS = (process.env.STATION_DB_EXPIRY_HOURS||12) * 3600 * 1000

/*
# Cache control - we cache the datablocks by station and h3 to save us needing
# to read/write them from/to the DB constantly. Note that this can use quite a lot
# of memory, but is a lot easier on the computer (in MINUTES)
# - flush period is how long they need to have been unused to be written
#   it is also the period of time between checks for flushing. Increasing this
#   will reduce the number of DB writes when there are lots of points being
#   tracked
# - MAXIMUM_DIRTY_PERIOD ensures that they will be written at least this often
# - expirytime is how long they can remain in memory without being purged. If it
#   is in memory then it will be used rather than reading from the db.
#   purges happen normally at flush period intervals (so 5 and 16 really it will
#   be flushed at the flush run at 20min)
*/
export const H3_CACHE_FLUSH_PERIOD_MS = (process.env.H3_CACHE_FLUSH_PERIOD_MINUTES||1)*60*1000;
export const H3_CACHE_EXPIRY_TIME_MS = (process.env.H3_CACHE_EXPIRY_TIME_MINUTES||4)*60*1000;
export const H3_CACHE_MAXIMUM_DIRTY_PERIOD_MS = (process.env.H3_CACHE_MAXIMUM_DIRTY_PERIOD_MINUTES||30)*60*1000;

console.log( 'configuration loaded' );
