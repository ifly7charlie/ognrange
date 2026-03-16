# Protocol Statistics

The aprs-server tracks protocol usage statistics from the live APRS feed and writes JSON files during each rollup cycle. These provide per-protocol packet counts, unique device counts, geographic distribution, altitude band breakdowns, and hourly activity by layer.

## Output Files

All files are written to `{OUTPUT_PATH}/stats/`:

| File | Description |
|------|-------------|
| `protocol-stats.json.gz` | Symlink to the latest daily `.gz` file |
| `protocol-stats.json` | Symlink to latest daily `.json` (only if `UNCOMPRESSED_ARROW_FILES` is set) |
| `protocol-stats.2026-03-14.json.gz` | Daily snapshot (cumulative within the month) |
| `protocol-stats.2026-03-14.json` | Uncompressed daily (only if `UNCOMPRESSED_ARROW_FILES`) |
| `protocol-stats.2026-03.json.gz` | Monthly final snapshot (written on month rotation) |
| `protocol-stats.state.json` | Internal persistence file (always uncompressed, not for consumption) |

- **Daily files** are written each rollup and contain cumulative stats since the start of the month.
- **Monthly files** are written once when the month changes, containing the final totals for that month.
- **Symlinks** always point to the most recent daily file. Use these for live dashboards.
- **State file** is for internal use only — it contains device sets for accurate unique counts and is loaded on restart.

## JSON Format

```json
{
  "generated": "2026-03-14T12:00:00Z",
  "startTime": "2026-03-01T00:00:00Z",
  "uptimeSeconds": 1209600,
  "restarts": 0,
  "protocols": {
    "OGFLR": {
      "raw": 150000,
      "accepted": 120000,
      "devices": 4500,
      "regions": { "eu": 95000, "na": 15000 },
      "altitudes": { "low": 100000, "mid": 15000, "high": 5000 }
    }
  },
  "hourly": {
    "flarm": [0, 0, 0, 5, 20, 150, 800, 2000, ...],
    "adsb": [50, 30, 20, 10, 5, 100, ...]
  }
}
```

### Top-level fields

| Field | Description |
|-------|-------------|
| `generated` | ISO 8601 timestamp when this file was written |
| `startTime` | When accumulation started (beginning of current month) |
| `uptimeSeconds` | Seconds since `startTime` |
| `restarts` | Number of process restarts today (0 = no restarts). Resets daily alongside `devices`. If non-zero, today's `devices` counts may be lower than actual |
| `protocols` | Per-TOCALL statistics (see below) |
| `hourly` | Per-layer accepted packet counts by hour of day (see below) |

### `protocols` object

Keyed by TOCALL (e.g. `OGFLR`, `OGADSB`, `OGNTRK`). Use the `protocols.*` i18n keys in `public/locales/en/common.json` to get human-readable names (e.g. `OGFLR` -> "FLARM").

Protocols are sorted by `raw` count descending.

| Field | Description |
|-------|-------------|
| `raw` | Total aircraft packets seen on the feed for this TOCALL |
| `accepted` | Packets that passed all ognrange filters (station, signal, stationary, altitude, etc.) |
| `devices` | Unique 6-char flarm/ICAO hex ID count (deduplicated across callsign prefixes). Resets daily on day rotation |
| `regions` | Packet counts by geographic region (see [Regions](#regions)) |
| `altitudes` | Accepted packet counts by altitude band (see [Altitude Bands](#altitude-bands)) |

Infrastructure TOCALLs are excluded: `OGNSDR`, `OGNSXR`, `OGNDELAY`, `OGNDVS`, `OGNTTN`, `OGMSHT`, `OGNHEL`, `OGNDSX`, `OGAVZ`.

### `hourly` object

Keyed by protocol layer name (e.g. `flarm`, `adsb`, `combined`). Each value is a 24-element array of accepted packet counts indexed by UTC hour (0-23).

These counters **reset daily** (on day rotation), so they represent the current day's activity. Use the `layers.*` i18n keys for display names.

Layer names: `combined`, `flarm`, `adsb`, `adsl`, `fanet`, `ogntrk`, `paw`, `safesky`.

## Regions

Geographic regions are determined by the packet's latitude/longitude using bounding boxes. Regions are checked in order; the first match wins.

| Code | Name | Bounding box |
|------|------|-------------|
| `eu` | Europe | 35-72°N, 25°W-45°E |
| `na` | North America | 15-72°N, 170°W-50°W |
| `sa` | South America | 60°S-15°N, 90°W-30°W |
| `af` | Africa | 35°S-37°N, 20°W-55°E |
| `as` | Asia | 0-75°N, 45°E-180°E |
| `oc` | Oceania | 50°S-5°N, 100°E-180°E |
| `ot` | Other | Everything else |

Use `regions.*` i18n keys for display names. Empty region entries are omitted from the output.

## Altitude Bands

Altitude bands are based on AGL (Above Ground Level) in meters, using a conservative ground elevation estimate — the maximum terrain elevation within an approximately 10km area around the packet position rather than the precise point elevation.

| Code | Range | Description |
|------|-------|-------------|
| `low` | 0-3000m | Below 3000m AGL (~0-10,000ft) |
| `mid` | 3000-4500m | 3000-4500m AGL (~10,000-15,000ft) |
| `high` | 4500m+ | Above 4500m AGL (~15,000ft+) |

Use `altitudes.*` i18n keys for display names. Only non-zero bands appear in the output.

Note: ADS-B packets above 4500m AGL and all packets above 10,000m AGL are filtered out before reaching the stats (and before coverage recording) as they indicate bad altitude data.

## Lifecycle

1. **Startup**: Loads `protocol-stats.state.json`. If the saved `startTime` is in the current month, counters and device sets are restored and the `restarts` counter is incremented. Otherwise the state is discarded and accumulation starts fresh.
2. **Each rollup**: Writes daily file + state file. Updates symlinks. If the day has rotated, hourly counters, device sets, and the restarts counter are reset.
3. **Month rotation**: Writes a monthly final file with the completed month's totals, then resets all counters, device sets, and hourly data.
4. **Shutdown**: Writes the state file for persistence across restarts.

## Global Uptime

The server tracks its own connectivity to the upstream APRS-IS server using the same 144-bit bitvector format as [station beacon activity](STATIONS.md#beacon-activity). Each keepalive received from the upstream aprsc server sets a bit for the current 10-minute UTC slot.

### Output Files

All files are written to `{OUTPUT_PATH}/stats/`:

| File | Description |
|------|-------------|
| `global-uptime.json` | Symlink to the latest dated file (updated each rollup). Between rollups, written directly on each keepalive (~every 45 seconds) |
| `global-uptime.2026-03-16.json` | Dated daily snapshot (written each rollup) |

On startup, today's state is restored from the live file if the date matches.

### JSON Format

```json
{
  "generated": "2026-03-16T10:31:00Z",
  "date": "2026-03-16",
  "server": "GLIDERN5",
  "serverSoftware": "aprsc 2.1.19-g730c5c0",
  "serverAddress": "148.251.228.229:14580",
  "activity": "ff7f00e0ff1f00000000000000000000000000",
  "uptime": 85.5,
  "slot": 64
}
```

| Field | Description |
|-------|-------------|
| `generated` | ISO 8601 timestamp when this file was written |
| `date` | UTC date (`YYYY-MM-DD`) the bitvector covers |
| `server` | Alias of the upstream APRS-IS server (e.g. `GLIDERN5`) |
| `serverSoftware` | Server software and version (e.g. `aprsc 2.1.19-g730c5c0`) |
| `serverAddress` | IP address and port of the upstream server |
| `activity` | 144-bit bitvector as 36 hex chars (same encoding as station [beacon activity](STATIONS.md#beacon-activity)) |
| `uptime` | Percentage (0.0-100.0) of active slots relative to elapsed slots today |
| `slot` | Number of elapsed 10-minute slots today (1-144, 1-based) |

The bitvector resets at UTC midnight. A set bit means the system had an active APRS-IS connection during that 10-minute window. Only keepalives from the upstream `aprsc` server are tracked — the system's own keepalive echoes and login responses are ignored.

On graceful shutdown (SIGINT/SIGTERM) the current slot's bit is cleared before the file is written, since the slot was not fully covered. On restart, today's state is restored from this file so previously completed slots are preserved.

## Internationalisation

Translation keys for the frontend are in `public/locales/en/common.json`:

- `protocols.*` — TOCALL to human-readable protocol name (e.g. `protocols.OGFLR` = "FLARM")
- `regions.*` — Region code to name (e.g. `regions.eu` = "Europe")
- `altitudes.*` — Altitude band code to label (e.g. `altitudes.low` = "Below 3000m")
- `layers.*` — Layer name to display name (e.g. `layers.flarm` = "FLARM") — used for the `hourly` keys
