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
