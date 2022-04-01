
## Getting Started


````
yarn install
yarn next build
yarn next start
yarn aprs
````

## Learn More


## Overview

The most important thing to note is that no information about specific
aircraft is persisted. It is possible that individual flights can be
identified based on being the only aircraft to cover a set of cells on
a short accumulation period.

This system is designed to monitor the full OGN APRS feed and use the
information from that to record information about signal strength
globally.

It aggregates information into multiple buckets, yearly, monthly and
daily.

For each aggregation static output files are produced that can be
displayed by a Next.JS (and deck.gl) front end. These files compress
and cache well so performance is optimal if behind a webcache (eg
cloudflare) or if the files are transferred to a CDN or S3 bucket.

Only active files will changed, meaning that historical data will
remain in this directory unless deleted. Although the front end has no
way of selecting it this feature could be easily added.

## Terms

H3 is the Hexagon layout. It's from Uber see https://h3geo.org/ for
information but it's generally heirarchical (or close to) and a fairly
light weight way to correlate coordinates. It doesn't suffer from the
same edge cases MGRS had even if the cells are not quite so nicely
sized.

### Metrics collected

OGNRange collects the following per station:

- H3 cellid
- lowest AGL
- lowest ALT
- strongest signal at ALT
- strongest signal
- sum of signal strength (clamped at 64 db) [6bit]
- sum of crc errors (max 10)
- sum of pre-packet gap (max 60) [6bit]
- count of packets

The global cells collect this information per station as well as a sum.

Not all of this information is accumulated to the browser. In particular the sums are converted
to averages (sum/count). 

### Interpreting displayed metrics

*Lowest AGL* (meters) is the lowest height data above ground that data was received. 
- agl varies across the cell and this is taken from the lat/long looked up on mapbox elevation tile
- it's possible that lowest AGL point is actually above lowest ALT

*Lowest ALT* (meters) is the lowest height AMSL that data was received

*Strongest Signal at ALT* is the strongest signal at *Lowest ALT*

*Strongest Signal* is the strongest signal at any altitude

*Average Signal* is the average for all packets regardless of altitude

*Average CRC* is the average number of CRC errors correct for all packets in the cell.
- a high number here may indicate poor coverage

*Average Gap* (seconds) is the average gap from the previous packet for a receiver
- recorded on the cell that sees the aircraft. 
- could indicate cell is on the edge of coverage and been missing points
- edge cases exist, in particular the first time a plane is seen
- clamped at 60, which effectively means 60 means not seen before
- gap can vary for aircraft that aren't moving very much or are very
  predictable because the APRS network reduces the reporting interval for these

*Expected Gap* is only available on global view and is the *Average Gap* divided by number
of stations. 
- This is more useful for looking at missing sections in the overall coverage as 
  cells with coverage from multiple receivers should have a very low expected value
  even if the coverage of some is poor
  
## Datafiles

There are two primary types of files - a collection of databases
located in the DB_PATH directory.

### Databases

All the databases are stored using leveldb and accessed directly from
node with no database server. They are simple KeyValue stores and contain
a string database key pointing to a binary CoverageRecord structure.

*global/* is the global database, it contains all the h3s captured
globally, at a lower resolution. Each record contains a rollup as well
as a list of all stations (and received data that have been received
in the cell). In the event that a station is retired it is removed from 
this list and the aggregator is updated

*stations/.../* a subdirectory containing a database for each
station. This database is more detailed than global but the format is
basically the same, except the record is a simple CoverageRecord

*status/* contains summary records for each station in JSON

### Output files

These are in the OUTPUT_PATH directory. It contains a set of
subdirectories one for each station and one for the global data.

```ognrange/output/global/global.day.15.arrow		ognrange/output/global/global.month.1.arrow		ognrange/output/global/global.year.2022.arrow
ognrange/output/global/global.day.15.arrow.json		ognrange/output/global/global.month.1.arrow.json	ognrange/output/global/global.year.2022.arrow.json
ognrange/output/global/global.day.arrow			ognrange/output/global/global.month.arrow		ognrange/output/global/global.year.arrow```

These files are produced during the aggregation process and contain
information from the time period. Once the aggregation is done the
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

```ognrange/output/stations.json```

This is the overall stations list used for displaying stations on the
map and information about them


## Aggregation & accumulators

The current configuration is to have 4 levels of aggregation -
'current', 'day', 'month', 'year'.

Each aggregation is stored in an 'accumulator', this is a prefix to
the h3id stored in the database.  The DBKEY uses a 16 bit hex number,
first letter being the type, and last three being the bucket.

As the database is keyed using this, so is the h3cache. Switching to a
new aggregator bucket is simply changing the prefix. All new read and
write requests will be looking for the new DBKEY value and will leave
the previous accumulator alone.

When the aggregator buckets change a rollup is forced. The h3Cache is
flushed to ensure that the disk database is up to date. The database
is then iterated to merge all the accumulators using a O(n) process at
the same time as the output files (.arrow) are produced. 
 
*current* is like working memory - it's accumulator type 0 and it is
where all records are stored until they are rolled up into the real
accumulators. It is not output for the browser and is deleted once it
has been rolled up. The bucket for this is generated in the same way
as the day accumulator so should be unique even if the process doesn't
run for a month or two.

note: current accumulators are deleted if they are no longer active on
server startup. This could be changed to roll them into the
aggregators they were related to if they are still active (TODO?)

current is rolled into ALL of the other active aggregators in one
operation. Once this has been done the data in it is removed.

All databases containing H3 information is rolledup during a rollup
operation. Up to 1/5 of the number configured in MAX_STATION_DBS or
will be run at the same time. Although this isn't run in a thread
there are a lot of advantages due to the large amount of IO wait time. 

Actual time to complete a rollup operation is primarily dependent on
the number of H3s in the database. This means the global database will
take longer to run than the stations, it's also why stations are separated
and global uses a lower resolution H3.

## configuration

config file is .env.local for aprs collector, for the front end it
follows nextjs naming convention NEXT_PUBLIC_XX goes to browser!

````
# url of website
NEXT_PUBLIC_SITEURL=

# mapbox token, used for elevation tiles on server and for the map on the client
NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN=

# where to store the accumulator databases
DB_PATH=/Users/melissa/ognrange/db/

# where to output the files used to render the map, this directory should be served
# as /data/ on the webserver, or set NEXT_PUBLIC_DATA_URL to point to the correct
# location if it's a different one from the server. It's not recommended to use
# the NEXT public directory for this because NEXTJS doesn't refresh the directory
# listing until you rebuild the app. I use a directory served directly by the webserver
OUTPUT_PATH=/Users/melissa/ognrange/output/
NEXT_PUBLIC_DATA_URL=

# control the elevation tile cache, note tiles are not evicted on expiry 
# so it will fill to MAX before anything happens. These tiles don't change so
# if this is too low you'll just be hammering your mapbox account. Flip side
# is the data will occupy ram or swap, 0 means no expiry
MAX_ELEVATION_TILES=32000
ELEVATION_TILE_EXPIRY_HOURS=0

# control how precise the ground altitude is, difficult balance for mountains..
# see https://docs.mapbox.com/help/glossary/zoom-level/,
# resolution 11 gives ~30m per pixel at 40 degrees which should be good enough
# if you are memory constrained then increase this number before you drop the
# number of tiles!
ELEVATION_TILE_RESOLUTION=11

# control the database handle caching for the accumulators
# by default we will keep a few hundred open at a time, unlike tile cache
# dbs will be flushed if they expire. (theory being that flying windows
# might be short and less open is less risk of problems)
# note that each DB uses 4 or 5 file handles MINIMUM so ulimit must be
# large enough! You want to set the number to be at least 20% larger
# than the maximum number of stations that are likely to be receiving
# simultaneously on a busy day
MAX_STATION_DBS=1200
STATION_DB_EXPIRY_HOURS=4

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
H3_CACHE_FLUSH_PERIOD_MINUTES=1
H3_CACHE_MAXIMUM_DIRTY_PERIOD_MINUTES=30
H3_CACHE_EXPIRY_TIME_MINUTES=4

# how much detail to collect, bigger numbers = more cells! goes up fast see
# https://h3geo.org/docs/core-library/restable for what the sizes mean
# 8 is 0.7sqkm/cell, 7 is 5.2sqkm/cell
# Although you can change this without breaking the database no old values
# will be updated after the change (as cell changed) you will also end
# up with a map with two sized cells and overlaps... I'd recommend NOT
# doing that.
H3_STATION_CELL_LEVEL=8
H3_GLOBAL_CELL_LEVEL=7


# We keep maps of when we last saw aircraft and where so we can determine the
# timegap prior to the packet, this is sort of a proxy for the 'edge' of coverage
# however we don't need to know this forever so we should forget them after
# a while. The signfigence of forgetting is we will assume no gap before the
# first packet for the first aircraft/station pair. Doesn't start running
# until approximately this many hours have passed
FORGET_AIRCRAFT_AFTER_HOURS=12

# How far a station is allowed to move without resetting the history for it
STATION_MOVE_THRESHOLD_KM = 2

# If we haven't had traffic in this long then we expire the station and
# delete all the history
STATION_EXPIRY_TIME_DAYS=31


# ROLLUP is when the current accumulators are merged with the daily/monthly/annual
# accumulators. All are done at the same time and the accumulators are 'rolled'
# over to prevent double counting. This is a fairly costly activity so if the
# disk or cpu load goes too high during this process (it potentially reads and 
# writes EVERYTHING in every database) you should increase this number
#
# This ALSO controls how often to write output files. Updated display files
# are produced during rollups - so at startup, at rollup period, and at exit
ROLLUP_PERIOD_HOURS=2

# how many databases we can process at once when doing a rollup, if
# your system drops the APRS connection when it is busy then you should
# set this number lower... I blame Node for having one thread and webworkers
# for not allowing you to share DB handles even if underlying C++ code is
# threadsafe
MAX_SIMULTANEOUS_ROLLUPS=100

# How often to check for change of accumulators - this is basically how long
# it can take to notice the day has changed, could be solved better but this 
# helps with testing
ACCUMULATOR_CHANGEOVER_CHECK_PERIOD_SECONDS=60

# how often do we need to report to the APRS server, also how often
# we check for lost connection to the server
APRS_KEEPALIVE_PERIOD_SECONDS=45

# Filter what packets are being received - eg to set up regional range tool
# uses standard APRS filters, this one basically means send me everything
APRS_TRAFFIC_FILTER=t/spuoimnwt

# Server and port to connect to 
APRS_SERVER=aprs.glidernet.org:14580

````

