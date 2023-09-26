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
//import fetch from 'node:fetch';

import {debounce} from 'lodash';

// Track duplicate requests for the same time and service them together from one response
let pending = [];
let referrer = undefined;
let accessToken = undefined;
let resolution = 11;

import LRU from 'lru-cache';

import {MAX_ELEVATION_TILES, ELEVATION_TILE_EXPIRY_HOURS, ELEVATION_TILE_RESOLUTION, NEXT_PUBLIC_SITEURL, NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN} from '../common/config.js';

const options = {
        max: MAX_ELEVATION_TILES,
        dispose: function (n, key) {
            const ttl = cache.getTtl(key);
            if (ttl > ELEVATION_TILE_EXPIRY_HOURS / 3) {
                console.log(`flushed elevation ${key} from cache [${ttl.toFixed(0)}s ttl remaining]`);
            }
        },
        updateAgeOnGet: true,
        allowStale: true,
        ttl: ELEVATION_TILE_EXPIRY_HOURS * 3600 * 1000
    },
    cache = new LRU(options);

cache.getTtl = (k) => {
    return (typeof performance === 'object' && performance && typeof performance.now === 'function' ? performance : Date).now() - cache.starts[cache.keyMap.get(k)];
};

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
    // Checking process.env is expensive so cache this
    if (!referrer) {
        referrer = 'https://' + NEXT_PUBLIC_SITEURL + '/';
        accessToken = NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN;
        resolution = ELEVATION_TILE_RESOLUTION;
    }
    if (!accessToken) {
        cb(0);
        return;
    }

    // Figure out what tile it is (obvs same order as geojson)
    // see https://docs.mapbox.com/help/glossary/zoom-level/,
    // zoom 12 gives 14m per pixel at 40 degrees which should be good enough
    let tf = tilebelt.pointToTileFraction(lng, lat, resolution);
    let tile = tf.map(Math.floor);
    let domain = 'https://api.mapbox.com/v4/';
    let source = `mapbox.terrain-rgb/${tile[2]}/${tile[0]}/${tile[1]}.pngraw`;
    let url = `${domain}${source}?access_token=${accessToken}`;

    // Have we cached it
    let pixels = cache.get(url);

    // Convert to elevation
    function pixelsToElevation(npixels) {
        let xp = tf[0] - tile[0];
        let yp = tf[1] - tile[1];
        let x = Math.floor(xp * npixels.shape[0]);
        let y = Math.floor(yp * npixels.shape[1]);

        let R = npixels.get(x, y, 0);
        let G = npixels.get(x, y, 1);
        let B = npixels.get(x, y, 2);

        let height = -10000 + (R * 256 * 256 + G * 256 + B) * 0.1;
        return Math.floor(height);
    }

    // If it isn't in the cache then we need to fetch it, cache it
    // and do the CB with the elevation
    if (!pixels) {
        // Make sure we don't fetch same thing twice at the same time
        if (url in pending) {
            pending[url].push(cb);
            return;
        } else {
            pending[url] = [cb];
        }

        // With a PNG from fetch we can create the NDArray we need
        // to calculate the elevation
        function parsePNG(err, img_data) {
            if (err) {
                throw err;
            }
            // Save it away
            const npixels = ndarray(new Uint8Array(img_data.data), [img_data.width | 0, img_data.height | 0, 4], [4, (4 * img_data.width) | 0, 1], 0);

            cache.set(url, npixels);
            pending[url].forEach((cbp) => cbp(pixelsToElevation(npixels)));
            delete pending[url];
        }

        // Go and get the URL
        fetch(url, {headers: {Referer: referrer}})
            .then((res) => {
                if (res.status != 200) {
                    if (res.status === 403) {
                        accessToken = null;
                        throw `MapBox API returns ${res.status}: ${res.statusText}. Disabling Elevation lookup`;
                    }
                    if (res.status != 404 && res.status != 422) {
                        debounce(() => {
                            referrer = null;
                        }, 30_000);
                        accessToken = null;
                        throw `MapBox API returns ${res.status}: ${res.statusText}, ensure the value for NEXT_PUBLIC_SITEURL set in .env.local is in the allowed ACL on mapbox, (lat:${lat},lng:${lng}), elevation lookup disabled for 30 seconds`;
                    } else {
                        throw undefined;
                    }
                } else {
                    return res.arrayBuffer();
                }
            })
            .then((data) => {
                new PNG().parse(data, parsePNG);
            })
            .catch((err) => {
                // We still call the callback on an error as we don't want to drop the packet
                if (err) {
                    console.error('unable to read elevation: ', err, url);
                }
                pending[url].forEach((cbp) => cbp(0));
                delete pending[url];
            });
    } else {
        cb(pixelsToElevation(pixels));
    }
}
