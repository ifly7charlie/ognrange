/* c8 ignore start */
function fixTrailingSlash(o) {
    return o + (!o.match(/\/$/) ? '/' : '');
}

export const NEXT_PUBLIC_SITEURL = process.env.NEXT_PUBLIC_SITEURL || 'unknown';
export const NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN = process.env.NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN || '';
export const NEXT_PUBLIC_DATA_URL = process.env.NEXT_PUBLIC_DATA_URL || '/data/';

// Database and output paths
export let DB_PATH = fixTrailingSlash(process.env.DB_PATH || './db');
export let OUTPUT_PATH = fixTrailingSlash(process.env.OUTPUT_PATH || './data');

// Do we want to keep the uncompressed files or not - not needed if behind webserver
export const UNCOMPRESSED_ARROW_FILES = !!(parseInt(process.env.UNCOMPRESSED_ARROW_FILES) || 1);

// APRS Server Keep Alive
export const APRS_KEEPALIVE_PERIOD_MS = (parseInt(process.env.APRS_KEEPALIVE_PERIOD_SECONDS) || 45) * 1000;
export const APRS_TRAFFIC_FILTER = process.env.APRS_TRAFFIC_FILTER || 't/spuoimnwt';
export const APRS_SERVER = process.env.APRS_SERVER || 'aprs.glidernet.org:14580';

/*
# ROLLUP is when the current accumulators are merged with the daily/monthly/annual
# accumulators. All are done at the same time and the accumulators are 'rolled'
# over to prevent double counting. This is a fairly costly activity so if the
# disk or cpu load goes too high during this process (it potentially reads and 
# writes EVERYTHING in every database) you should increase this number */
export const ROLLUP_PERIOD_MINUTES = (parseFloat(process.env.ROLLUP_PERIOD_MINUTES) || 3) * 60;
export const ACCUMULATOR_CHANGEOVER_CHECK_PERIOD_MS = (parseInt(process.env.ACCUMULATOR_CHANGEOVER_CHECK_PERIOD_SECONDS) || 60) * 1000;

/* # how many databases we can process at once when doing a rollup, if
 * # your system drops the APRS connection when it is busy then you should
 * # set this number lower */
export const MAX_SIMULTANEOUS_ROLLUPS = parseInt(process.env.MAX_SIMULTANEOUS_ROLLUPS) || 100;

// how much detail to collect, bigger numbers = more cells! goes up fast see
// https://h3geo.org/docs/core-library/restable for what the sizes mean
//
// *** DO NOT CHANGE WITHOUT DELETING ALL DATA IT WILL BREAK MANY THINGS
export const H3_STATION_CELL_LEVEL = parseInt(process.env.H3_STATION_CELL_LEVEL) || 8;
export const H3_GLOBAL_CELL_LEVEL = parseInt(process.env.H3_GLOBAL_CELL_LEVEL) || 7;

// # We keep maps of when we last saw aircraft and where so we can determine the
// # timegap prior to the packet, this is sort of a proxy for the 'edge' of coverage
// # however we don't need to know this forever so we should forget them after
// # a while. The signfigence of forgetting is we will assume no gap before the
// # first packet for the first aircraft/station pair. Doesn't start running
// # until approximately this many hours have passed
export const FORGET_AIRCRAFT_AFTER_SEC = (parseInt(process.env.FORGET_AIRCRAFT_AFTER_HOURS) || 12) * 3600;

// How far a station is allowed to move without resetting the history for it
export const STATION_MOVE_THRESHOLD_KM = parseInt(process.env.STATION_MOVE_THRESHOLD_KM) || 2;

// If we haven't had traffic in this long then we expire the station
export const STATION_EXPIRY_TIME_SECS = (parseInt(process.env.STATION_EXPIRY_TIME_DAYS) || 31) * 3600 * 24;

/* # control the database handle caching for the accumulators
 * # by default we will keep a few hundred open at a time, unlike tile cache
 * # dbs will be flushed if they expire. (theory being that flying windows
 * # might be short and less open is less risk of problems)
 * # note that each DB uses 4 or 5 file handles MINIMUM so ulimit must be
 * # large enough! You want to set the number to be at least 20% larger
 * # than the maximum number of stations that are likely to be receiving
 * # simultaneously on a busy day */
export const MAX_STATION_DBS = parseInt(process.env.MAX_STATION_DBS) || 3200;
export const STATION_DB_EXPIRY_MS = (parseInt(process.env.STATION_DB_EXPIRY_HOURS) || 12) * 3600 * 1000;

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
export const H3_CACHE_FLUSH_PERIOD_MS = (parseInt(process.env.H3_CACHE_FLUSH_PERIOD_MINUTES) || 1) * 60 * 1000;
export const H3_CACHE_EXPIRY_TIME_MS = (parseInt(process.env.H3_CACHE_EXPIRY_TIME_MINUTES) || 4) * 60 * 1000;
export const H3_CACHE_MAXIMUM_DIRTY_PERIOD_MS = (parseInt(process.env.H3_CACHE_MAXIMUM_DIRTY_PERIOD_MINUTES) || 30) * 60 * 1000;

/* # control the elevation tile cache, note tiles are not evicted on expiry
 * # so it will fill to MAX before anything happens. These tiles don't change so
 * # if this is too low you'll just be hammering your mapbox account. Flip side
 * # is the data will occupy ram or swap */
export const MAX_ELEVATION_TILES = parseInt(process.env.MAX_ELEVATION_TILES) || 32000;
export const ELEVATION_TILE_EXPIRY_HOURS = parseInt(process.env.ELEVATION_TILE_EXPIRY_HOURS) || 0;

/* # nextjs caches arrow files for the api requests, they can be quite big so control
 * # maximum number here. LRU cache */
export const MAX_ARROW_FILES = parseInt(process.env.MAX_ARROW_FILES) || 5000;

/* # control how precise the ground altitude is, difficult balance for mountains..
 * # see https://docs.mapbox.com/help/glossary/zoom-level/,
 * # resolution 11 gives ~30m per pixel at 40 degrees which should be good enough
 * # if you are memory constrained then increase this number before you drop the
 * # number of tiles! */
export const ELEVATION_TILE_RESOLUTION = parseInt(process.env.ELEVATION_TILE_RESOLUTION) || 11;

/* # version number if exported during build - normally calls git to get it for development
 * # environment */
export const GIT_REF = process.env.GIT_REF || process.env.NEXT_PUBLIC_GIT_REF || null;

console.log('configuration loaded');
