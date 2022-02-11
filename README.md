
## Getting Started


```bash
npm run dev
# or
yarn dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

You can start editing the page by modifying `pages/index.js`. The page auto-updates as you edit the file.

[API routes](https://nextjs.org/docs/api-routes/introduction) can be accessed on [http://localhost:3000/api/hello](http://localhost:3000/api/hello). This endpoint can be edited in `pages/api/hello.js`.

The `pages/api` directory is mapped to `/api/*`. Files in this directory are treated as [API routes](https://nextjs.org/docs/api-routes/introduction) instead of React pages.

## Learn More


## configuration

config file is .env.local for aprs collector, for the front end it follows nextjs naming convention

````
# url of website
NEXT_PUBLIC_SITEURL=

# mapbox token, used for elevation tiles on server and for the map on the client
NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN=

# where to store the accumulator databases
DB_PATH=/Users/melissa/ognrange/db/

# where to output the files used to render the map, this directory should be served
# as /data/ on the webserver, or set NEXT_PUBLIC_DATA_URL to point to the correct
# location if it's a different one from the server
OUTPUT_PATH=/Users/melissa/ognrange/output/
NEXT_PUBLIC_DATA_URL=

# control the elevation tile cache, note tiles are not evicted on expiry 
# so it will fill to MAX before anything happens
MAX_ELEVATION_TILES=7200
ELEVATION_TILE_EXPIRY_HOURS=8

# control how precise the ground altitude is, difficult balance for mountains..
# see https://docs.mapbox.com/help/glossary/zoom-level/,
# resolution 12 gives 14m per pixel at 40 degrees which should be good enough
ELEVATION_TILE_RESOLUTION=12

# control the database handle caching for the accumulators
# by default we will keep a few hundred open at a time, unlike tile cache
# dbs will be flushed if they expire. (theory being that flying windows
# might be short and less open is less risk of problems)
MAX_STATION_DBS=800
STATION_DB_EXPIRY_HOURS=4

# Cache control - we cache the datablocks by station and h3 to save us needing
# to read/write them from/to the DB constantly. Note that this can use quite a lot
# of memory, but is a lot easier on the computer (in MINUTES)
# - flush period is how long they can remain in memory without being written
# - expirytime is how long they can remain in memory without being purged. If it
#   is in memory then it will be used rather than reading from the db.
H3_CACHE_FLUSH_PERIOD=5
H3_CACHE_EXPIRY_TIME=16

````


## rebuilding protobuf

cd lib; pbjs --target json --wrap es6 range.proto -o range-protobuf.mjs; cd -
cd lib; pbjs --target json range.proto -o range-protobuf.js; cd -

then you need to frig it so they export - recommend to check what is missing from top and bottom of file after rebuilding
