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
  "rfCapabilityDb": 10.7,
  "rfCapabilityN": 23481,
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

## Horizon Arrow Files

Alongside the coverage arrow files, the rollup writes a per-station receive-horizon file for the month, year, and yearnz accumulators (never day/current):

| File | Description |
|------|-------------|
| `{name}.{acc}.{fileid}.horizon.arrow.gz` | Dated horizon file (e.g. `LFLE.month.2026-07.horizon.arrow.gz`) |
| `{name}.{acc}.horizon.arrow.gz` | Symlink to the latest dated file per accumulator |
| `...horizon.arrow` | Uncompressed twin when `UNCOMPRESSED_ARROW_FILES` is set |

Requires the station to have a position and a resolved `elevation` (unresolved elevation → no horizon files). Cells from all protocol layers are merged into two RF frequency groups, since the horizon is an antenna/frequency property: **868 MHz** (combined + fanet + adsl + paw; flarm/ogntrk are already inside combined) and **1090 MHz** (adsb). Safesky is excluded (network-sourced, not RF). The mapping lives in `Layer::frequency_group()` (`aprs-server/src/layers.rs`).

The circle is divided into 720 half-degree bearing bins. Each H3 cell contributes to every bin its ~0.8 km width subtends (arc spreading), using the elevation angle from the station to the cell's lowest received point: the point's minimum MSL altitude vs the station's **antenna viewpoint** (ground `elevation` plus the antenna height derived from `beaconAltitude`, see [STATIONS.md](./STATIONS.md#station-fields)) over the great-circle distance, with a k=4/3 effective-earth curvature/refraction correction. The ground horizon uses the same viewpoint, so the two charts are directly comparable. Selection is by **minimum angle** — nothing is received below the skyline, so the lowest observed angle is the tightest bound on the horizon occlusion in that direction. Cells closer than 5 km are excluded: near the station the signal is strong enough to be received well below the true skyline, so close-in minimum angles do not reflect the horizon. Cells further than 120 km are excluded too, as are cells whose angle falls outside −3°…+50° (below the floor the altitude is corrupt; above the ceiling the cell is nearly overhead and says nothing about the horizon). Min-angle selection is very sensitive to corrupted data, hence all three windows.

### Schema

One row per non-empty (frequency, bearing) bin:

| Column | Type | Description |
|--------|------|-------------|
| `frequency` | `u16` | Frequency group: `868` or `1090` |
| `bearing` | `f32` | Bin start in degrees: 0.0, 0.5, ... 359.5 |
| `lowestAngle` | `f32` | Minimum elevation angle (degrees) at any distance within the 5–120 km window |
| `lowestAgl` | `u16` | AGL metres of the lowest-angle cell's lowest point |
| `lowestDistance` | `u16` | Distance (km) to the lowest-angle cell |
| `maxDistance` | `u16` | Distance (km) to the furthest contributing cell |
| `angle10km` … `angle90km` | `f32?` | Minimum angle within each distance band (upper edges 10/20/30/50/90 km; the first band starts at the 5 km minimum); null when the band has no cells. The 90 km top edge is where a 0.5° bin arc matches the cell width. A lowest-angle cell beyond 90 km appears in `lowestAngle` but in no band |
| `count` | `u32` | Cell-arc contributions to the bin |

Horizon files have no shrink guard (bin counts can legitimately shrink) and no metadata JSON sidecar. They are regenerated whenever the station's coverage rolls up; the startup mop-up does not produce them. The `.horizon` suffix cannot collide with layer suffixes, so the coverage file-listing API ignores these files.

## Ground Horizon Arrow File

Alongside the receive horizon, the rollup writes a per-station **terrain** horizon file. Terrain is static, so the file is non-accumulator — one per station, no dated variants and no symlink:

| File | Purpose |
|------|---------|
| `{name}.ground-horizon.arrow.gz` | Terrain horizon + side-profile rays |
| `{name}.ground-horizon.arrow` | Uncompressed twin when `UNCOMPRESSED_ARROW_FILES` is set |

The DEM (Terrarium tiles, same `ElevationService` as AGL checks) is sampled along 720 half-degree bearing rays, every 0.5 km from the station out to 120 km (241 samples per ray; matches the receive horizon's 120 km window). Terrarium includes ocean **bathymetry**, so every DEM lookup (AGL, coarse filter, horizons alike) clamps to a per-location floor: sea level everywhere except a small built-in table of genuine below-sea-level land regions (`BELOW_SEA_LEVEL_REGIONS` in `elevation.rs` — Dead Sea, Caspian shore, Death Valley, Dutch polders, ...), each floored at its known lowest surface so a box that brushes ocean can only expose seabed down to that floor. Sea reads as the water surface, not the seabed. Files generated before the clamp show the seabed in over-water profiles; delete them to have the queue regenerate.

Terrarium also has **corrupt pixels** at land/water seams (observed: +795 m and −1079 m side by side over a flat Baltic bay, permanently in the source tiles). Fine-zoom tiles are therefore **despiked once at load**, before entering the RAM cache (`ElevationTile::despike`, called from `load_tile`): a pixel that towers over — or undercuts — its entire 3×3 neighbourhood by ≥250 m is a one-pixel-wide tower/shaft no real terrain produces at ~40 m pitch, and is clamped to the neighbourhood. All comparisons use the original pixel values so fixes cannot cascade; the disk cache keeps the pristine source PNG; every consumer of the cached tile (packet AGL, station elevation, horizon rays) reads cleaned data at no per-read cost. Coarse zoom-7 tiles stay raw: at ~600 m/pixel real monoliths are single-pixel, and a corrupt-high coarse sample only makes the bogus-altitude filter more lenient / gets re-read at fine zoom by the horizon refine pass. Known limits: the window cannot cross tile borders, and a ≥2-pixel garbage blob shields itself (the observed class is single-pixel). Ground-horizon files written before the guard may carry baked-in spikes (visible as a one-bin needle on the skyline chart); delete them to regenerate. Per-cell AGL data accumulated from a corrupt pixel heals only with new traffic. Sample 0 of every ray is the station's own ground elevation — this is how the frontend learns the station ground level. Sampling is by exact lat/lng along each great-circle ray; H3 cells are not involved. Angles are computed from the antenna viewpoint (ground + AGL from `beaconAltitude`, same rule as the receive horizon).

Sampling is coarse-to-fine per ray: a full-length pass at zoom 7 (~1 km pixels, the same tiles the packet path's coarse AGL check already caches) establishes the provisional skyline, then only buckets whose coarse angle plus a worst-case under-read margin could still beat that skyline are re-read at zoom 11. The margin is `atan(max(50 m, 6 × local coarse relief) / distance)`: flat coarse terrain cannot hide a mountain (its signature would show in the coarse samples), while cliffs and steep faces — where one averaged pixel spans hundreds of metres of height — scale the margin to match. Refined buckets are supersampled at ~pixel pitch (up to 16 sub-reads per 0.5 km) and store the **maximum**, so a ridge crest between two 500 m samples can no longer slip through; pruned buckets keep their zoom-7 value (they are provably below the horizon, so this only smooths occluded stretches of the profile chart). Near terrain is effectively always refined (the margin is huge at short range); flat or occluded far field prunes away. A ray's tiles are walked with a held-tile cursor, so the tile cache is only consulted on tile transitions. Typical cost is ~5–20 tiles per station (bounded above by the old all-z11 cost of ~250–500 for heavily mountainous skylines), nearly all shared with the packet path's existing z11/z7 lookups.

### Schema

Exactly 720 rows (one per bearing bin), no nullable columns:

| Column | Type | Description |
|--------|------|-------------|
| `bearing` | `f32` | Bin start in degrees: 0.0, 0.5, ... 359.5 |
| `elevations` | `FixedSizeList<i16>[241]` | Terrain elevation (m MSL) every 0.5 km from 0 to 120 km; refined buckets hold the max over their supersampled sub-reads, pruned buckets the zoom-7 sample |

Everything angular — the final skyline angle/distance, foreground visible crests, radio-shadow stretches — is derived client-side from the elevations with the same k=4/3 formula the receive horizon uses (`visibleCrests` in `lib/react/coveragedetails/grounddata.ts`), so those are no longer stored. Files written before 2026-08-11 carry additional `horizonAngle`/`horizonDistance` columns; the frontend ignores them.

Schema metadata (strings): `stationLat`, `stationLng`, `stationAgl` (beacon-derived antenna height above ground the angles were computed from; `NaN` when the beacon was missing or implausible and the `GROUND_STATION_AGL_M` default was assumed — the frontend treats that like older files that lack the key), `stepKm`, `maxKm`, `samples`, `generatedAt` (epoch secs).

Generation runs as a **serial background queue** in the daemon (`ground_horizon_task`), strictly between rollups — it pauses whenever a rollup is in progress. Every minute (and after each station) the queue rescans for valid, non-mobile stations whose file is missing, whose position has moved beyond `STATION_MOVE_THRESHOLD_KM` since the file was written (`groundHorizonPos` records the generation position), or whose beaconed antenna altitude has changed by ≥2 m since generation (`groundHorizonBeacon` records the beacon used); newcomers join the back of the queue and stations are processed one at a time — serial generation is the throttle. Generation always uses the station's **primary location**, not the live fix: a bouncing station (two receivers sharing a callsign) toggles `lat`/`lng` between sites on every packet, but the primary only changes on a genuine rotation, so the file doesn't churn — and the beaconed altitude is likewise only updated by packets at the primary site. A just-rotated primary (a single packet at a brand-new location) is skipped until a second packet confirms it, so mobile blips don't trigger 120 km terrain sweeps. `GROUND_HORIZON_PAUSED=1` disables the queue entirely (the old `MAX_GROUND_HORIZONS_PER_ROLLUP` cap is gone; setting it logs a startup warning). Sampling is all-or-nothing: any failed tile fetch aborts that station's file and requeues it at the back, with a short back-off so a dead tile endpoint is cycled slowly. On a confirmed move-purge the old-location file is deleted alongside the active-period coverage outputs. The startup mop-up does not produce these files. No shrink guard, no sidecar; the coverage file-listing API ignores the `.ground-horizon` suffix like `.horizon`.

The `groundhorizon` bin tool (`cargo run --bin groundhorizon -- <NAME> <LAT> <LNG> [BEACON_ALT_M]`) generates a file offline with the same code path — useful for local frontend fixtures and post-deploy spot checks. Without the altitude argument the viewpoint falls back to ground + `GROUND_STATION_AGL_M` (default 3 m), exactly as the daemon does for a station that never beaconed one.

The frontend (`lib/react/coveragedetails/grounddetails.tsx`) renders this as a "Ground Horizon" skyline panorama (shaded area topped by the final skyline angle per bearing, with darker lines for visible foreground crests, same north-centred axis as the receive horizon) that swaps to a terrain side profile along the hovered bearing while the receive-horizon chart is hovered.
