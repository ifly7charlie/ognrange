# Per-Station JSON Files

The Rust rollup writes a per-station JSON file for each non-global station that has traffic. These files contain the full station details, beacon bitvector, uptime, protocol layers, and rollup activity.

## Output Files

Written to `{OUTPUT_PATH}stations/{name}/`:

| File | Description |
|------|-------------|
| `{name}.day.{date}.json` | Dated daily JSON (the real file) |
| `{name}.json` | Symlink to latest day file (frontend fallback) |
| `{name}.day.json` | Symlink to latest day file |
| `{name}.month.{month}.json` | Symlink to latest day file (month-dated) |
| `{name}.month.json` | Symlink to latest day file |
| `{name}.year.{year}.json` | Symlink to latest day file (year-dated) |
| `{name}.year.json` | Symlink to latest day file |
| `{name}.yearnz.{yearnz}.json` | Symlink to latest day file (NZ-year-dated) |
| `{name}.yearnz.json` | Symlink to latest day file |

All symlinks point to the same dated day file. The various dated and undated symlinks allow the frontend to request station data using any accumulator period.

## When Written

Each rollup cycle, for every non-global station that was processed (i.e. had traffic and was not skipped).

## JSON Format

The file contains all `StationDetails` fields (see [STATIONS.md](./STATIONS.md#station-fields)) plus additional computed fields:

```json
{
  "id": 42,
  "station": "LFLE",
  "lat": 45.5629,
  "lng": 5.9126,
  "primary_location": [45.5629, 5.9126],
  "lastPacket": 1741968000,
  "lastLocation": 1741968000,
  "lastBeacon": 1741967400,
  "status": "v0.2.8.RPI-GPU CPU:0.7 ...",
  "moved": false,
  "bouncing": false,
  "mobile": false,
  "lastSeenAtPrimary": 1741968000,
  "purgedAt": null,
  "purgeReason": null,
  "lastSeenAtPrevious": null,
  "valid": true,
  "layerMask": 3,
  "outputEpoch": 1741968000,
  "outputDate": "2026-03-14",
  "exportedAt": 1741969800,
  "stats": { ... },
  "beaconActivity": "ff7f00e0ff1f00000000000000000000000000",
  "beaconActivityDate": "2026-03-14",
  "uptime": 83.3,
  "layers": ["combined", "flarm"],
  "activity": {
    "ranges": [
      { "start": 1741900000, "end": 1741968000, "rollups": 12, "cells": 450 }
    ],
    "totalRollups": 48,
    "activeRollups": 42,
    "totalCells": 18000,
    "firstSeen": 1741800000,
    "lastSeen": 1741968000,
    "lastRollup": 1741968000
  }
}
```

## Fields

All fields from `StationDetails` are included (see [STATIONS.md](./STATIONS.md#station-fields)), plus:

| Field | Type | Description |
|-------|------|-------------|
| `beaconActivity` | `string?` | Daily beacon activity bitvector, hex-encoded (see [STATIONS.md Beacon activity](./STATIONS.md#beacon-activity)). Updated every rollup cycle (3 hours by default); slots after `exportedAt` may not yet be reflected |
| `beaconActivityDate` | `string?` | UTC date (`YYYY-MM-DD`) the beacon activity bitvector covers. Reset to the new date at the first rollup after UTC midnight |
| `uptime` | `f32?` | Percentage (0.0-100.0) of 10-minute slots active relative to elapsed slots at write time (`exportedAt`). `null` if no beacon activity today |
| `layers` | `string[]` | Array of protocol layer names this station has received data for, derived from `layerMask` (e.g. `["combined", "flarm"]`) |
| `activity` | `object?` | Combined-layer day `RollupActivity` (see below). Only present if the station had combined-layer traffic |

### RollupActivity

The `activity` object describes the station's coverage rollup history for the combined layer's day accumulator:

| Field | Type | Description |
|-------|------|-------------|
| `ranges` | `array` | Time ranges when the station was active |
| `ranges[].start` | `u32` | Range start epoch |
| `ranges[].end` | `u32` | Range end epoch |
| `ranges[].rollups` | `u32` | Number of rollup periods in this range |
| `ranges[].cells` | `u32` | Total H3 cells contributed during this range |
| `totalRollups` | `u32` | Total rollup periods tracked |
| `activeRollups` | `u32` | Rollup periods where the station contributed data |
| `totalCells` | `u32` | Total H3 cells across all rollup periods |
| `firstSeen` | `u32` | Epoch of first rollup with data |
| `lastSeen` | `u32` | Epoch of most recent rollup with data |
| `lastRollup` | `u32` | Epoch of most recent rollup (with or without data) |

Note: This `activity` field (RollupActivity) is distinct from `beaconActivity` (beacon bitvector). `beaconActivity` tracks 10-minute beacon slots; `activity` tracks coverage rollup periods.
