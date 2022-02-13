
## Getting Started


````
yarn install
yarn next build
yarn next start
yarn aprs
````

## Learn More


## Overview

The most important thing to note is that no information about specific aircraft is 
persisted. It is possible that individual flights can be identified based on being
the only aircraft to cover a set of cells on a short accumulation period.

This system is designed to monitor the full OGN APRS feed and use the information 
from that to record information about signal strength globally.

It aggregates information into multiple buckets, yearly, monthly and daily. 

For each aggregation static output files are produced that can be displayed by a Next.JS
(and deck.gl) front end. These files compress and cache well so performance is 
optimal if behind a webcache (eg cloudflare) or if the files are transferred to a 
CDN or S3 bucket. 

Only active files will changed, meaning that historical data will remain in this
directory unless deleted. Although the front end has no way of selecting it this feature
could be easily added.

### Metrics collected

OGNRange collects the following per station:

- h3 cellid
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


## configuration

config file is .env.local for aprs collector, for the front end it follows nextjs naming convention NEXT_PUBLIC_XX goes to browser!

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
# is the data will occupy ram or swap
MAX_ELEVATION_TILES=10000
ELEVATION_TILE_EXPIRY_HOURS=36

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
# large enough!
MAX_STATION_DBS=1200
STATION_DB_EXPIRY_HOURS=4

# Cache control - we cache the datablocks by station and h3 to save us needing
# to read/write them from/to the DB constantly. Note that this can use quite a lot
# of memory, but is a lot easier on the computer (in MINUTES)
# - flush period is how long they need to have been unused to be written
# - expirytime is how long they can remain in memory without being purged. If it
#   is in memory then it will be used rather than reading from the db.
H3_CACHE_FLUSH_PERIOD_MINUTES=5
H3_CACHE_EXPIRY_TIME_MINUTES=16

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
# first packet for the first aircraft/station pair
FORGET_AIRCRAFT_AFTER_HOURS=12

# ROLLUP is when the current accumulators are merged with the daily/monthly/annual
# accumulators. All are done at the same time and the accumulators are 'rolled'
# over to prevent double counting. This is a fairly costly activity so if the
# disk or cpu load goes too high during this process (it potentially reads and 
# writes EVERYTHING in every database) you should increase this number
ROLLUP_PERIOD_HOURS=2

# how often do we need to report to the APRS server
APRS_KEEPALIVE_PERIOD_MINUTES=2
````

