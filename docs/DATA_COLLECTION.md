# Data Collection

This document describes how ognrange collects and calculates the data shown on the map and in station details.

## Uptime Calculator

### What "uptime" means

Uptime is shown as a percentage and answers the question: *"How often was this station heard today?"*

Each day is divided into 144 ten-minute windows (slots). For each window, ognrange records a simple yes/no — did a packet from this station arrive during that window? Uptime is then the percentage of elapsed windows today where at least one packet was heard.

Note that uptime reflects whether the station was *heard*, not whether any aircraft were in range. A station with 100% uptime has been continuously connected and transmitting to the network all day, regardless of how much traffic it received.

For example, if 60 windows have passed since midnight UTC and a station was heard in 54 of them, its uptime is 90%.

Uptime resets at midnight UTC each day. If a station was very active yesterday but has been silent since midnight, it will show no uptime for today.

### Server uptime

Alongside each station's uptime, ognrange also tracks *server uptime* — how reliably the ognrange server itself was connected to the APRS-IS network. This uses the same ten-minute window system, but marks a window as active only when a keepalive message is received from the upstream APRS-IS server. A keepalive is a short heartbeat message the server sends periodically to confirm the connection is alive — it does not mean any aircraft or ground station traffic was received during that window.

### Why a station's uptime can be higher than the server's uptime

You may occasionally notice that a station shows a higher uptime percentage than the server itself. This is not a bug — it reflects a real difference in what each value measures.

When the ognrange server disconnects from APRS-IS, it clears its own record for the current window (to avoid counting a partial connection as full coverage). Station activity records are stored separately and are not cleared on disconnect. So if a station was heard earlier in a window, and then the server lost its connection before the window ended, the station retains credit for that window but the server does not.

Over several brief disconnections during a day, this can add up to a noticeably higher uptime percentage for stations than for the server.

## Coverage Layers

ognrange separates coverage data by the protocol each aircraft or device uses to transmit its position. This lets you see, for example, whether a ground station can hear FLARM-equipped gliders specifically, or only picks up ADS-B traffic.

### Available layers

| Layer | Protocol | Notes |
|-------|----------|-------|
| **Combined** | FLARM + OGN Tracker | See below |
| **FLARM** | FLARM / OGN-FLARM | Most gliders, sailplanes, and light aircraft in Europe |
| **ADS-B** | ADS-B (via OGN) | Transponder-equipped aircraft; presence only (see below) |
| **ADSL** | ADS-L | Lightweight ADS-B alternative for non-transponder aircraft |
| **FANET** | FANET+ | Paragliders, hang gliders, and drones using the FANET protocol |
| **OGN Tracker** | OGN Tracker | Generic OGN tracking devices |
| **PilotAware** | PilotAware (PAW) | UK-focused collision awareness devices; presence only (see below) |
| **SafeSky** | SafeSky | SafeSky app users |

### The Combined layer

The Combined layer aggregates FLARM and OGN Tracker traffic into a single view. It is the default layer shown on the map.

When a FLARM or OGN Tracker packet is received, it is recorded in *both* that protocol's own layer and the Combined layer. Other protocols (ADS-B, ADSL, FANET, PilotAware, SafeSky) are only recorded in their own layer and do not contribute to Combined.

#### Legacy data

Before per-protocol layers were introduced, all coverage data was stored without a protocol label. That older data has been imported as Combined, so historical Combined coverage may reflect a broader mix of traffic than the current definition strictly implies.

### Not all stations receive all protocols

Ground stations are configured and maintained independently, and different hardware and software setups support different protocols. A station running only FLARM-capable software will have no ADS-B or FANET data, for example.

Because of this, the layer selector only offers protocols that a given station has actually received traffic for. If a protocol does not appear in the selector for a station, that station has no recorded coverage for it.

### Presence-only layers

ADS-B and PilotAware do not carry a signal strength value in the way that FLARM does. Coverage for these layers records only that the station *heard* the aircraft — not how well. On the map, presence-only layers use a fixed signal value rather than a measured one, so the colour scale reflects coverage extent rather than signal quality.

## Receive Horizon

The station details panel shows a *receive horizon* chart: for each compass direction, the lowest elevation angle at which the station has actually received traffic. Nothing is received below the local skyline, so the lowest observed angle is a good estimate of how much of the sky is blocked in that direction — a ridge to the west shows up as a raised line over westerly bearings, while an open valley lets the line drop to (or below) zero degrees.

### How the angles are computed

The circle around the station is divided into 720 half-degree bearing bins. Every coverage cell contributes its lowest received point: the elevation angle is calculated from that point's altitude relative to the station's antenna viewpoint, over the direct distance, with a standard-refraction (k=4/3) earth-curvature correction.

Each bin keeps a monotone **envelope** of up to five (distance, angle) breakpoints — the surviving cells are the ones no other cell beats by being both farther away *and* lower-angle. Reception proven at an angle holds at every closer distance too (same angle, stronger signal), so the envelope reads as a staircase: "proven down to this angle out to this distance", rising step by step with distance. The chart draws one line per step; because the envelope only rises, outer steps always plot at or above inner ones, and hovering a bearing lists that bearing's actual steps ("0.33° out to 40 km") rather than a fixed set of distance bands.

To keep corrupted packets from wrecking the chart, cells are excluded when they are closer than 5 km (received below the true skyline on near-field signal strength alone) or further than 120 km from the station, when their angle falls outside −3° to +50°, when the cell only ever heard a single packet, or — once the station's terrain horizon has been computed — when they claim reception more than 0.25° below the terrain skyline at their distance, which is physically implausible and marks a corrupt position or altitude.

### Likely coverage extension

The coverage-floor map compares the envelope against the terrain skyline on each bearing: the smallest gap between any proven breakpoint and the skyline at its distance is that bearing's *measured margin*. Where the margin is small the receiver demonstrably hears down to its physical horizon, so the absence of low receptions further out is traffic distribution, not radio — the likely floor follows the skyline plus that margin instead of jumping to the angle of whatever high-flying traffic happened to be heard far away. Bearings that only ever heard high traffic keep a large margin and stay evidence-bound. Floors set by this extension rather than by a proven breakpoint are flagged in the cell details.

### Frequencies and periods

Cells from all protocol layers are merged into two charts by RF frequency — 868 MHz (FLARM, OGN Tracker, FANET, ADS-L, PilotAware) and 1090 MHz (ADS-B) — since the horizon is a property of the antenna and frequency, not the protocol. SafeSky is excluded because it is network-sourced rather than received over RF. Horizon charts exist for month and year periods and are rebuilt from the full coverage data at every rollup; a day view shows the containing month.

## Station Lifecycle

### How stations are added

A station only enters the system when it has successfully received and forwarded at least one valid aircraft packet. For a packet to count, it must carry a measured signal strength, a valid timestamp, the correct protocol identifier, originate from a moving aircraft, and claim a position within plausible reception range of the station (500 km by default) — anything further is treated as a corrupted position and rejected.

A receiver that connects to the APRS-IS network and sends its own beacon announcements — but has no aircraft in range — does not appear in ognrange. This prevents stations that are online but idle from accumulating in the database as 0-traffic entries.

### How stations are removed

During each rollup, ognrange checks every known station against two expiry conditions:

- **Inactivity**: if a station's last packet or last beacon is older than `STATION_EXPIRY_TIME_DAYS` (default: 31 days), it is marked expired and its coverage database is deleted. Stations with "test" as a word in their callsign (`TEST1`, `LFLE-test`, `MyTestRx`, but not `Contest` or `Testwood`) use the shorter `TEST_STATION_EXPIRY_TIME_DAYS` (default: 2 days).
- **Relocation**: if a station's position moves significantly, ognrange tracks both old and new locations. Once the old location has been silent for `STATION_MOVE_CONFIRM_DAYS` (default: 7 days), the old coverage data is purged.

A safety valve prevents mass data loss: if more than 2% of active stations would expire in a single rollup — which can happen if the server loses connectivity for an extended period — the purge is deferred and re-evaluated at the next rollup. Test-station expiries don't count towards that 2%, so retiring a backlog of quiet test receivers can't trip it.

After purge, the station's metadata record (name, last known location, purge reason, purge timestamp) is retained. Only the coverage data (H3 observation records) is deleted.
