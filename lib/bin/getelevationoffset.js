//
// Taken from:
//   https://github.com/scijs/get-pixels/blob/master/node-pixels.js
//   https://github.com/mcwhittemore/mapbox-elevation/blob/master/index.js
//
// Modules not used because they include a LOAD of things we don't need, some of which
// sound more like a rootkit than something useful.
//

import tilebelt from '@mapbox/tilebelt';
import ndarray from 'ndarray';
import {PNG} from 'pngjs';

import {existsSync, mkdirSync, readFileSync, writeFileSync, renameSync, unlinkSync, utimesSync, readdirSync, statSync} from 'fs';

import LRU from 'lru-cache';

import {MAX_ELEVATION_TILES, ELEVATION_TILE_EXPIRY_HOURS, ELEVATION_TILE_RESOLUTION, DB_PATH} from '../common/config.js';

// Track duplicate requests for the same tile and service them together from one response
const pending = {};

// Terrarium-encoded PNG DEM tiles. AWS Open Data public bucket is the default; override
// via NEXT_PUBLIC_DEM_TILE_URL to front it through your own CDN.
const DEM_TILE_URL = process.env.NEXT_PUBLIC_DEM_TILE_URL || 'https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png';

// On-disk tile cache. DEM tiles are immutable raw PNG bytes keyed by z/x/y, so a
// restarted daemon can read them back from disk instead of re-fetching from S3.
const DEM_CACHE_DIR = `${DB_PATH}dem-tiles/`;

// Tiles untouched (mtime not refreshed) for this long are pruned once a day.
const DEM_CACHE_MAX_AGE_MS = 10 * 24 * 3600 * 1000;
const DEM_CACHE_PRUNE_INTERVAL_MS = 24 * 3600 * 1000;

try {
    mkdirSync(DEM_CACHE_DIR, {recursive: true});
} catch (e) {
    console.error(`unable to create DEM tile cache directory ${DEM_CACHE_DIR}`, e);
}

const options = {
    max: MAX_ELEVATION_TILES,
    updateAgeOnGet: true,
    allowStale: true,
    ttl: ELEVATION_TILE_EXPIRY_HOURS * 3600 * 1000
};

const cache = new LRU(options);

// Statistics export
export function getCacheSize() {
    return cache.size;
}

//
// For a given lat, lng lookup the elevation
// NOTE: there is a race condition here - as we are async we could have two requests for the same
//       point at the same time and do more work.  It won't cause it to fail it just wastes CPU and
//       memory as we keep fetching the same item
//
export async function getElevationOffset(lat, lng, cb) {
    if (!cb) {
        return new Promise((r) => _getElevationOffset(lat, lng, r));
    }
    return _getElevationOffset(lat, lng, cb);
}

// Terrarium-decode a single pixel at integer (x, y) in the tile.
function decodeHeight(npixels, x, y) {
    const R = npixels.get(x, y, 0);
    const G = npixels.get(x, y, 1);
    const B = npixels.get(x, y, 2);
    // Terrarium encoding: height = (R*256 + G + B/256) - 32768
    return R * 256 + G + B / 256 - 32768;
}

function _getElevationOffset(lat, lng, cb) {
    loadTile(lat, lng, (npixels, tile, tf) => {
        if (!npixels) {
            cb(0);
            return;
        }
        const xp = tf[0] - tile[0];
        const yp = tf[1] - tile[1];
        const x = Math.floor(xp * npixels.shape[0]);
        const y = Math.floor(yp * npixels.shape[1]);
        cb(Math.floor(decodeHeight(npixels, x, y)));
    });
}

// Shared tile loader. Resolves the (lat, lng) to a tile, returns its
// pixel ndarray via cb(npixels, tile, tf). Hits RAM cache first, then on-disk
// cache, then network. Concurrent requests for the same tile are coalesced via
// `pending`; the queued thunks each bind their own (tile, tf) so callers
// looking up different points in the same tile each get their own coordinates.
// On any fetch/decode failure cb is called with (null, tile, tf) so the caller
// can decide what to return — callers must not drop the request.
function loadTile(lat, lng, cb) {
    const tf = tilebelt.pointToTileFraction(lng, lat, ELEVATION_TILE_RESOLUTION);
    const tile = tf.map(Math.floor);
    const url = DEM_TILE_URL.replace('{z}', String(tile[2])).replace('{x}', String(tile[0])).replace('{y}', String(tile[1]));

    const pixels = cache.get(url);
    if (pixels) {
        cb(pixels, tile, tf);
        return;
    }

    // Bind tile/tf per-caller so coalesced callbacks each see their own coords.
    const thunk = (npixels) => cb(npixels, tile, tf);

    if (url in pending) {
        pending[url].push(thunk);
        return;
    }
    pending[url] = [thunk];

    // Tile is keyed by z/x/y on disk — independent of the (configurable) URL.
    const tilePath = `${DEM_CACHE_DIR}${tile[2]}-${tile[0]}-${tile[1]}.png`;

    // Decode a parsed PNG into the NDArray, cache it in RAM and service every
    // pending callback waiting on this tile.
    function deliver(img_data) {
        const npixels = ndarray(new Uint8Array(img_data.data), [img_data.width | 0, img_data.height | 0, 4], [4, (4 * img_data.width) | 0, 1], 0);
        cache.set(url, npixels);
        const callbacks = pending[url];
        delete pending[url];
        callbacks.forEach((cbp) => cbp(npixels));
    }

    function failAll() {
        const callbacks = pending[url];
        delete pending[url];
        callbacks.forEach((cbp) => cbp(null));
    }

    // Fetch the tile from the (S3) DEM endpoint, persist it to the disk
    // cache, decode and deliver.
    function fetchFromS3() {
        fetch(url)
            .then((res) => {
                if (res.status != 200) {
                    throw `DEM tile fetch returned ${res.status}: ${res.statusText} for ${url}`;
                } else {
                    return res.arrayBuffer();
                }
            })
            .then((data) => {
                // Persist the raw PNG to the disk cache. Best-effort: write to a
                // temp file then rename so a concurrent reader never sees a
                // partial PNG.
                try {
                    const tmp = `${tilePath}.${process.pid}.tmp`;
                    writeFileSync(tmp, Buffer.from(data));
                    renameSync(tmp, tilePath);
                } catch (e) {
                    console.error(`unable to persist DEM tile ${tilePath}: ${e}`);
                }
                new PNG().parse(data, (err, img_data) => {
                    if (err) {
                        throw err;
                    }
                    deliver(img_data);
                });
            })
            .catch((err) => {
                // We still call the callback on an error as we don't want to drop the packet
                // Node's fetch wraps the real network error on err.cause (ENOTFOUND, ECONNRESET,
                // ETIMEDOUT, UND_ERR_*, TLS errors, etc.) — surface it so the log is actionable.
                const cause = err && err.cause;
                const causeStr = cause ? ` (cause: ${cause.code || cause.name || ''} ${cause.message || cause})`.trimEnd() : '';
                console.error(`unable to read elevation for ${url}: ${err}${causeStr}`);
                failAll();
            });
    }

    // Check the disk cache before going to the network.
    if (existsSync(tilePath)) {
        try {
            const diskData = readFileSync(tilePath);
            // Refresh mtime so the daily pruner treats this tile as recently
            // used (atime is unreliable under noatime/relatime mounts).
            const now = new Date();
            utimesSync(tilePath, now, now);
            new PNG().parse(diskData, (err, img_data) => {
                if (err) {
                    // Corrupt cache file — drop it and fall back to S3.
                    console.error(`corrupt DEM tile cache ${tilePath}, re-fetching: ${err}`);
                    try {
                        unlinkSync(tilePath);
                    } catch (e) {
                        /* already gone */
                    }
                    fetchFromS3();
                    return;
                }
                deliver(img_data);
            });
        } catch (e) {
            console.error(`unable to read DEM tile cache ${tilePath}, re-fetching: ${e}`);
            fetchFromS3();
        }
    } else {
        fetchFromS3();
    }
}

//
// Prune the on-disk DEM tile cache: delete any tile untouched (mtime not
// refreshed) for DEM_CACHE_MAX_AGE_MS. Every disk cache hit bumps the tile's
// mtime, so this removes only genuinely cold tiles. Best-effort.
//
function pruneDemTileCache() {
    try {
        const cutoff = Date.now() - DEM_CACHE_MAX_AGE_MS;
        let pruned = 0;
        for (const name of readdirSync(DEM_CACHE_DIR)) {
            if (!name.endsWith('.png')) continue;
            const file = `${DEM_CACHE_DIR}${name}`;
            try {
                if (statSync(file).mtimeMs < cutoff) {
                    unlinkSync(file);
                    pruned++;
                }
            } catch (e) {
                /* file vanished or unreadable — skip */
            }
        }
        if (pruned) {
            console.log(`pruned ${pruned} stale DEM tile(s) from ${DEM_CACHE_DIR}`);
        }
    } catch (e) {
        console.error(`unable to prune DEM tile cache ${DEM_CACHE_DIR}: ${e}`);
    }
}

// Prune at startup (cleans up after previous runs), then daily. unref so the
// timer never holds the process open.
pruneDemTileCache();
const pruneTimer = setInterval(pruneDemTileCache, DEM_CACHE_PRUNE_INTERVAL_MS);
pruneTimer.unref?.();
