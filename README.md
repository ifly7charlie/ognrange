# OGN Range Version 2.0

This is a complete replacement for the old glidernet/ognrange project. You can run it totally self contained in docker if you like

## Viewing Data

You can see this project running at https://ognrange.glidernet.org or https://ognrange.onglide.com

## Getting Started

### Using docker

Images are published to `ghcr.io/ifly7charlie/ognrange/{next,aprs,apache}` on every push to `main`.
They contain no site configuration - everything, including the `NEXT_PUBLIC_*` values, is read
from the container environment at runtime, so changing a value only needs a restart, not a rebuild.

Put your configuration in `./conf/.env.local`, which compose mounts into the `aprs` container
and loads into `next` at runtime. `DB_PATH` and `OUTPUT_PATH` are set in `docker-compose.yml` to match the
volume mounts and override any values in that file. Apache serves the arrow volume at `/arrow/`, so set
`NEXT_PUBLIC_DATA_URL=/arrow/`.

```
docker compose pull     # or: docker compose build
docker compose up -d
```

If you want to store the data files locally you should change the volume config in `docker-compose.yml`

The maintenance scripts in `bin/` (`cleanuparrow`, `dumparrow`, `migrate_station_stats`, `purgeuptime`)
are published as `ghcr.io/ifly7charlie/ognrange/tools`. Run them on demand with the data volumes and
config mounted, as the same user that owns the data:

```
docker run --rm --user <uid>:<gid> \
    -v <arrow volume>:/ognrange/data/arrow \
    -v <path to>/.env.local:/ognrange/.env.local:ro \
    ghcr.io/ifly7charlie/ognrange/tools dist/bin/purgeuptime.js --help
```

### Locally without docker, perhaps testing etc.

Copy the .env.local file from ./conf/.env.local to the current directory and update to suit

For development you need to install the packages `npm ci`

Then in different windows you'll want: `npm run dev` and the collector, `cargo run` in `./aprs-server`

There are some complexities - in particular the .arrow files need to be served to the browser
somehow. Easiest for development is to use `npm run dev` and output the arrow files into `./public/data`
You will need to configure `.env.local` to point to this in `NEXT_PUBLIC_DATA_URL` and `OUTPUT_PATH`. For serving
in production see `conf/docker-httpd.conf` for the sections you need to handle this encoding. It's much quicker
and uses less disk space saving and serving as .gz, otherwise your webserver will always be compressing
a file that never changes.

Assuming you have checked out in the directory `~ognrange` this could look like:

```
# You need to configure your access token if you don't pass on the command line
#NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN=<my token>

# replace with fully qualified domain name, this is reported to aprs servers
NEXT_PUBLIC_SITEURL=<ognrange.mydomain>

# either relative to the site or full URL (https)
NEXT_PUBLIC_DATA_URL=/data/arrow/

# Path to files, these are in the container volumes
DB_PATH=~ognrange/data/db/
OUTPUT_PATH=~ognrange/public/data/arrow/

# for serving locally you need to have the .arrow files not the .gz ones
# as normally the webserver serves .gz using 'content-encoding gzip'
UNCOMPRESSED_ARROW_FILES=1
```

To build & run - note that this is a summary not a set of command lines ;)

```
npm ci
npx next build
npx next start
cd aprs-server && cargo build --release
./aprs-server/target/release/aprs-server
```

## Learn More

- [Protocol Statistics](docs/STATISTICS.md) — per-protocol packet counts, regions, altitude bands, hourly activity
- [Station Files](docs/STATIONS.md) — station list JSON and Arrow format, fields, move detection
- [Per-Station JSON](docs/STATION.md) — individual station detail files with beacon activity and rollup history

## Overview

The most important thing to note is that no information about specific
aircraft is persisted. It is possible that individual flights can be
identified based on being the only aircraft to cover a set of cells on
a short accumulation period.

This system is designed to monitor the full OGN APRS feed and use the
information from that to record information about signal strength
globally.

It aggregates information into multiple buckets, yearly, monthly and
daily, across multiple protocol layers (FLARM, ADS-B, ADS-L, FANET,
OGN Tracker, PilotAware, SafeSky, and a combined view). Layers are
configurable via `ENABLED_LAYERS` in `.env.local`; see
`lib/common/layers.ts` for definitions.

For each aggregation static output files are produced that can be
displayed by a Next.JS (and deck.gl) front end. These files compress
and cache well so performance is optimal if behind a webcache (eg
cloudflare) or if the files are transferred to a CDN or S3 bucket.

Only active files will changed, meaning that historical data will
remain in this directory unless deleted. Although the front end has no
way of selecting it this feature could be easily added.

### Terminology

H3 is the Hexagon layout. It's from Uber see https://h3geo.org/ for
information but it's generally heirarchical (or close to) and a fairly
light weight way to correlate coordinates. It doesn't suffer from the
same edge cases MGRS had even if the cells are not quite so nicely
sized.

### Metrics collected

OGNRange collects the following per station:

-   H3 cellid
-   lowest AGL
-   lowest ALT
-   strongest signal at ALT
-   strongest signal
-   sum of signal strength (clamped at 64 db) [6bit]
-   sum of crc errors (max 10)
-   sum of between-packet gap (max 60) [6bit]
-   count of packets

The global cells collect this information per station as well as a sum.

Not all of this information is accumulated to the browser. In particular the sums are converted
to averages (sum/count).

### Interpreting displayed metrics

_Lowest AGL_ (meters) is the lowest height data above ground that data was received.

-   agl varies across the cell and this is taken from the lat/long looked up on a DEM elevation tile
-   it's possible that lowest AGL point is actually above lowest ALT

_Lowest ALT_ (meters) is the lowest height AMSL that data was received

_Strongest Signal at ALT_ is the strongest signal at _Lowest ALT_

_Strongest Signal_ is the strongest signal at any altitude

_Average Signal_ is the average for all packets regardless of altitude

_Average CRC_ is the average number of CRC errors correct for all packets in the cell.

-   a high number here may indicate poor coverage

_Average Gap_ (seconds) is the average gap from the previous packet for a receiver

-   recorded on the cell that sees the aircraft.
-   could indicate cell is on the edge of coverage and been missing points
-   edge cases exist, in particular the first time a plane is seen
-   clamped at 60, which effectively means 60 means not seen before
-   gap can vary for aircraft that aren't moving very much or are very
    predictable because the APRS network reduces the reporting interval for these

_Expected Gap_ is only available on global view and is the _Average Gap_ divided by number
of stations.

-   This is more useful for looking at missing sections in the overall coverage as
    cells with coverage from multiple receivers should have a very low expected value
    even if the coverage of some is poor

### Packets considered for coverage mapping

APRS packets are NOT recorded in the following situations:

1. Do not parse correctly

-   including not having AX25 addresses,
-   not having both latitude & longitude values,
-   not having a comment that stats with 'id'

2. Their station name is excluded by the file [`ignorestation.ts`](lib/common/ignorestation.ts) this includes (for example)

-   ones that start with FNB, XCG, XCC, OGN, RELAY, RND, FLR, bSky, AIRS, N0TEST-
-   a list of ones matching TEST, UNKNOWN, GLOBAL, STATIONS, and others etc
-   numeric only station names
-   any station that has a non ANSI alphanumeric character (eg not A-Z & 0-9)

3. Have a source callsign length < 6 characters or the last 6 characters are not valid hex
4. Have no timestamp
5. Their first Digipeaters entry is not qA (relayed packets)
6. Are stationary or nearly stationary (more specifically bouncing between a small number of locations)
7. Have no signal strength
8. Have invalid coordinates
9. Come from a device on the blocklist in `aprs-server/src/ignore_list.rs` (devices that send corrupt positions)

If you wish to update the list of stations that are excluded please raise a pull request to change the file `aprs-server/src/ignore_list.rs` (and its frontend copy `lib/common/ignorestation.ts`). For changes to the other criteria please send me a message, or raise an issue or PR for the code - they are all enforced in the `aprs-server`

In the user interface the stats are:

```
ignoredTracker: ignored a *repeated* packet from an OGN tracker (digipeaters don't start qA)
invalidTracker: tracker id (alarm id) is not 6 characters
invalidTimestamp: packet doesn't have a reasonable timestamp
ignoredSignal0: means there is no signal strength in the received packet
ignoredStationary: means that a device isn't moving, repeated packets are ignored
ignoredH3stationary: means that a device is moving but only between a very small number of locations (eg due to poor GPS reception - indoors etc)
ignoredElevation: Unable to do elevation lookup of packet - probably means lat/lng is not valid
ignoredDevice: the device is on the blocklist in aprs-server/src/ignore_list.rs (sends corrupt positions)
count: total number of packets
```

### Ignoring a station

If you wish to ignore a station please add it to `ignorestation.ts` and raise a PR for the repo.

You can also deleting the datafiles, but without doing both you will not remove it from the DB!

Generally speaking if the station is not receiving packets very little disk space will be used.

## Datafiles

There are two primary types of files - a collection of databases
located in the DB_PATH directory.

### Databases

All the databases are stored using leveldb and accessed directly from
node with no database server. They are simple KeyValue stores and contain
a string database key pointing to a binary CoverageRecord structure.

_global/_ is the global database, it contains all the h3s captured
globally, at a lower resolution. Each record contains a rollup as well
as a list of all stations (and received data that have been received
in the cell). In the event that a station is retired it is removed from
this list and the aggregator is updated

_stations/.../_ a subdirectory containing a database for each
station. This database is more detailed than global but the format is
basically the same, except the record is a simple CoverageRecord

_status/_ contains summary records for each station in JSON

### Output files (Arrow)

These are in the OUTPUT_PATH directory. It contains a set of
subdirectories one for each station and one for the global data.

```
ognrange/output/global/global.day.15.arrow.gz
ognrange/output/global/global.month.1.arrow.gz
ognrange/output/global/global.year.2022.arrow.gz
ognrange/output/global/global.day.15.arrow.json.gz
ognrange/output/global/global.month.1.arrow.json.gz
ognrange/output/global/global.year.2022.arrow.json.gz
ognrange/output/global/global.day.arrow.gz
ognrange/output/global/global.month.arrow.gz
ognrange/output/global/global.year.arrow.gz
```

These files are produced during the aggregation process and contain
information from the time period. Once the aggregation period is finished the
file will remain unchanged and can serve as an archive of the data or
be deleted. For example keeping previous month would allow you to
compare coverage over time in the browser. (in browser because the
server does not perform rollup operations after the aggregator is
closed)

Symbolic links are created for the latest files so there will
always be a XXX.month.arrow, XXX.day.arrow and XXX.year.arrow
that contains the latest accumulator data

The .json file contains meta data with information about the file,
when it was created and some diagnostic information

`ognrange/output/stations.json`

This is the overall stations list used for displaying stations on the
map and information about them

It is safe to delete files if they are no longer needed. Be aware that the dates
on the files are not updated once they have been finished, for example with

```
find ogrange/output/ -name "*day*arrow.gz" -mtime +90 -delete
```

Note the use of `*day*arrow.gz` to filter it just to arrow files. There
are also .json files that contain meta data you may want to remove at some
point as well. Run without the `-delete` to see what will be deleted

You probably want to keep the `*month*` and `*year*` files

The front end will only display available files so deleting them from the
disk is sufficient to change what is displayed to the user, though there
may be a lag if a web content cache is being used. (eg cloudflare)

## Aggregation & accumulators

The current configuration is to have 4 levels of aggregation -
'current', 'day', 'month', 'year'.

Each aggregation is stored in an 'accumulator', this is a prefix to
the h3id stored in the database. The DBKEY uses a 16 bit hex number,
first letter being the type, and last three being the bucket.

As the database is keyed using this, so is the h3cache. Switching to a
new aggregator bucket is simply changing the prefix. All new read and
write requests will be looking for the new DBKEY value and will leave
the previous accumulator alone.

When the aggregator buckets change a rollup is forced. The h3Cache is
flushed to ensure that the disk database is up to date. The database
is then iterated to merge all the accumulators using a O(n) process at
the same time as the output files (.arrow) are produced.

_current_ is like working memory - it's accumulator type 0 and it is
where all records are stored until they are rolled up into the real
accumulators. It is not output for the browser and is deleted once it
has been rolled up. The bucket for this is generated in the same way
as the day accumulator so should be unique even if the process doesn't
run for a month or two.

note: current accumulators are deleted if they are no longer active on
server startup. This could be changed to roll them into the
aggregators they were related to if they are still active

current is rolled into ALL of the other active aggregators in one
operation. Once this has been done the data in it is removed.

All databases containing H3 information is rolledup during a rollup
operation. Up to 1/5 of the number configured in MAX_STATION_DBS or
will be run at the same time. This executes in a different thread. As
the classic-level database can't share the handles the main thread
flushes all data and closes all databases before starting the rollup.

During the rollup no cell updates are written to the DB, and any changes
will be kept in memory until it completes. This can mean a lot more H3s
in memory compared to normal operation. Once the rollup completes these
will be flushed as normal.

(As a new accumulator was started prior to commencing the rollup
there will be nothing in the DB for that accumulator so the main thread
doesn't need to read records as long as all dirty H3s are still in memory)

Actual time to complete a rollup operation is primarily dependent on
the number of H3s in the database. This means the global database will
take longer to run than the stations, it's also why stations are separated
and global uses a lower resolution H3.

## Logging

Logging defaults to `info` level. You can control verbosity via the
`RUST_LOG` environment variable (see
[`tracing-subscriber` EnvFilter](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html)
for full syntax).

```
# show debug output for the aprs-server only
RUST_LOG=aprs_server=debug

# enable trace logging for everything (very noisy)
RUST_LOG=trace
```

Output destination is controlled by `LOG_STDOUT` (default 1) and
`LOG_SYSLOG` (default 0, Unix only).

## Configuration

Configuration is read from `.env.local` in the working directory, by both the
aprs collector and the front end. For the front end it follows the nextjs naming
convention, NEXT_PUBLIC_XX goes to the browser!

`conf/.env.local` lists every option, commented out and showing the default. Copy
it to `./.env.local` when running locally; docker compose uses it directly.

See `aprs-server/src/config.rs` and `lib/common/config.ts` for the code.

```
#
# ognrange configuration - read by aprs-server and the next front end
#
# Everything is commented out and shows the default. Uncomment and change
# what you need. NEXT_PUBLIC_XX values are sent to the browser!
#
# Docker: compose mounts this file into aprs and loads it into next.
# DB_PATH and OUTPUT_PATH are set in docker-compose.yml to match the volume
# mounts and override anything set here.
#
# See aprs-server/src/config.rs and lib/common/config.ts for the code.
#

### Site

# url of website, this is also reported to the aprs servers
#NEXT_PUBLIC_SITEURL=unknown

# mapbox token, used for the map on the client
#NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN=

# api key for the airspace overlay, the airspace setting is hidden if unset
#NEXT_PUBLIC_AIRSPACE_API_KEY=

# where the browser fetches the arrow files from, either relative to the site
# or a full URL (https). With docker apache serves the arrow volume at /arrow/
#NEXT_PUBLIC_DATA_URL=/data/

### Paths

# where to store the accumulator databases
#DB_PATH=./db/

# where to output the files used to render the map, this directory should be
# served at NEXT_PUBLIC_DATA_URL. It's not recommended to use the NEXT public
# directory for this because NEXTJS doesn't refresh the directory listing
# until you rebuild the app. I use a directory served directly by the webserver
#OUTPUT_PATH=./data/

# aprs-server always writes .gz files, this also writes uncompressed twins.
# next reads the uncompressed files when set to 1 and the .gz ones otherwise.
# For serving locally without a webserver that handles 'content-encoding gzip'
# you need the uncompressed files
#UNCOMPRESSED_ARROW_FILES=1

### Layers

# which coverage layers to capture, comma-separated. Defaults to all of them:
# combined,flarm,adsb,adsl,fanet,ogntrk,paw,safesky
#ENABLED_LAYERS=

### APRS

# APRS-IS login callsign, max 8 characters
#APRS_CALLSIGN=OGNRANGE

# Server and port to connect to
#APRS_SERVER=aprs.glidernet.org:14580

# Filter what packets are being received - eg to set up regional range tool
# uses standard APRS filters, this one basically means send me everything
#APRS_TRAFFIC_FILTER=t/spuoimnwt

# how often do we need to report to the APRS server, also how often
# we check for lost connection to the server
#APRS_KEEPALIVE_PERIOD_SECONDS=45

### Packet filtering

# Packets claiming a position further than this (in km) from the station's
# known location are rejected as corrupt - well beyond real reception range
# even for mountain/ducting records (~450km)
#MAX_PACKET_DISTANCE_KM=500

# Count (but still process) packets with timestamps this far in the future
# or this old
#FUTURE_PACKET_CUTOFF_SECS=300
#STALE_PACKET_CUTOFF_SECS=3600

# Optional reject log for debugging filtered packets. Set to a file path
# to enable. Each line is: reason<TAB>raw_packet
# The log rotates when it exceeds REJECT_LOG_MAX_MB, renaming to .1
#REJECT_LOG=
#REJECT_LOG_MAX_MB=50

### Coverage resolution

# how much detail to collect, bigger numbers = more cells! goes up fast see
# https://h3geo.org/docs/core-library/restable for what the sizes mean
# 8 is 0.7sqkm/cell, 7 is 5.2sqkm/cell
# *** DO NOT CHANGE WITHOUT DELETING ALL DATA - old values are never updated
# and you will end up with a map with two sized cells and overlaps
#H3_STATION_CELL_LEVEL=8
#H3_GLOBAL_CELL_LEVEL=7

### Rollup

# ROLLUP is when the current accumulators are merged with the daily/monthly/annual
# accumulators. All are done at the same time and the accumulators are 'rolled'
# over to prevent double counting. This is a fairly costly activity so if the
# disk or cpu load goes too high during this process (it potentially reads and
# writes EVERYTHING in every database) you should increase this number
#
# This ALSO controls how often to write output files. Updated display files
# are produced during rollups - so at startup, at rollup period, and at exit
#
# ROLLUP_PERIOD_MINUTES wins if both are set, it must be more than 12 minutes
#ROLLUP_PERIOD_HOURS=3
#ROLLUP_PERIOD_MINUTES=

# how many databases we can process at once when doing a rollup, if
# your system drops the APRS connection when it is busy then you should
# set this number lower. Capped at MAX_STATION_DBS / 2
#MAX_SIMULTANEOUS_ROLLUPS=100

# how often to write the server/station status files for the front end,
# independent of rollup
#STATUS_WRITE_PERIOD_MINUTES=60

### Databases

# how many station databases to keep open at once. You want this to be at
# least 20% larger than the maximum number of stations that are likely to be
# receiving simultaneously on a busy day. Each DB can hold up to
# STATION_MAX_OPEN_FILES file handles plus a few for its logs, so ulimit
# must be large enough for MAX_STATION_DBS of them
#MAX_STATION_DBS=800

# open file and block cache budget per station DB, multiplied by up to
# MAX_STATION_DBS so keep them modest
#STATION_MAX_OPEN_FILES=40
#STATION_BLOCK_CACHE_MB=16

# the global DB is a single large DB so it gets a much bigger budget
#GLOBAL_MAX_OPEN_FILES=2000
#GLOBAL_BLOCK_CACHE_MB=256

# target SST file size for every DB
#DB_MAX_FILE_SIZE_MB=16

# after this many records written + deleted to a DB the next rollup does a
# full compaction to reclaim space in the bottom level, 0 disables
#FULL_COMPACT_WRITE_THRESHOLD=5000000

### H3 cache

# Cache control - we cache the datablocks by station and h3 to save us needing
# to read/write them from/to the DB constantly. Note that this can use quite a lot
# of memory, but is a lot easier on the computer (in MINUTES)
# - flush period is how long they need to have been unused to be written
#   it is also the period of time between checks for flushing. Increasing this
#   will reduce the number of DB writes when there are lots of points being
#   tracked
# - MAXIMUM_DIRTY_PERIOD ensures that they will be written at least this often
#H3_CACHE_FLUSH_PERIOD_MINUTES=5
#H3_CACHE_MAXIMUM_DIRTY_PERIOD_MINUTES=30

### Elevation

# terrarium-encoded DEM tiles used for server elevation lookups, defaults to the
# free AWS Open Data bucket; override to front it through your own CDN
#NEXT_PUBLIC_DEM_TILE_URL=https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png

# maximum elevation tiles held in memory. These tiles don't change so if this
# is too low you'll just be re-fetching the same tiles (though fetches usually
# hit the on-disk cache in DB_PATH/dem-tiles rather than the network).
# Flip side is the data will occupy ram or swap
#MAX_ELEVATION_TILES=32000

# control how precise the ground altitude is, difficult balance for mountains..
# (this is a web-mercator tile zoom level)
# resolution 11 gives ~30m per pixel at 40 degrees which should be good enough
# if you are memory constrained then increase this number before you drop the
# number of tiles!
#ELEVATION_TILE_RESOLUTION=11

# antenna height above ground (m) used when a station's beaconed altitude is
# missing or implausible
#GROUND_STATION_AGL_M=3

# set to 1 to stop the background ground-horizon generation
#GROUND_HORIZON_PAUSED=0

### Stations and aircraft

# We keep maps of when we last saw aircraft and where so we can determine the
# timegap prior to the packet, this is sort of a proxy for the 'edge' of coverage
# however we don't need to know this forever so we should forget them after
# a while. The signfigence of forgetting is we will assume no gap before the
# first packet for the first aircraft/station pair. Doesn't start running
# until approximately this many hours have passed
#FORGET_AIRCRAFT_AFTER_HOURS=12

# How far a station is allowed to move (in km) without triggering move detection
#STATION_MOVE_THRESHOLD_KM=0.2

# How many days a station must remain at a new location (with no packets from
# the original location) before the move is confirmed and old data is purged
#STATION_MOVE_CONFIRM_DAYS=7

# If we haven't had traffic in this long then we expire the station and
# delete all the history
#STATION_EXPIRY_TIME_DAYS=31

# Stations with "test" as a word in their callsign (TEST1, LFLE-test, MyTestRx
# - not Contest or Testwood) expire after this much silence instead
#TEST_STATION_EXPIRY_TIME_DAYS=2

### Outage notifications (ntfy)

# set to 0 to stop sending, topic urls are still generated for the front end
#NTFY_ENABLED=1
#NTFY_BASE_URL=https://ntfy.sh
# topics are {prefix}-{station}-{random suffix}
#NTFY_TOPIC_PREFIX=onglide-status
# max notifications per check, keeps inside the ntfy.sh rate limit
#NTFY_SENDS_PER_CYCLE=50
#OUTAGE_CHECK_PERIOD_MINUTES=30
# no beacons for this long is an outage (receiver down)
#OUTAGE_BEACON_DAYS=1
# no aircraft traffic for this long is an outage (hearing nothing)
#OUTAGE_TRAFFIC_DAYS=7
# a feed gap (server down or APRS-IS disconnected) at least this long drops
# pending notifications for outages that began before it ended; shorter
# gaps such as restarts keep them
#OUTAGE_MAX_DOWNTIME_HOURS=2

### Front end

# how far back the scans go for year when generating graphs
#MAXIMUM_GRAPH_AGE_DAYS=60

# nextjs caches arrow files for the api requests, they can be quite big so
# control the maximum number here. LRU cache
#MAX_ARROW_FILES=5000

### Logging

# aprs-server log level, see the Logging section of the README
#RUST_LOG=info
#LOG_STDOUT=1
#LOG_SYSLOG=0
```
