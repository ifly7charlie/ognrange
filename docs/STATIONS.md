# Station Files

The aprs-server maintains a list of all known OGN ground stations and writes station metadata files during each rollup cycle. Station data is persisted in LevelDB and survives restarts.

## Output Files

All files are written to `{OUTPUT_PATH}`:

| File | Description |
|------|-------------|
| `stations.json` | Active stations (those with at least one packet received) |
| `stations.json.gz` | Gzipped version of `stations.json` |
| `stations-complete.json` | All known stations, including inactive ones |
| `stations-complete.json.gz` | Gzipped version of `stations-complete.json` |
| `stations/stations.day.{date}.arrow.gz` | Daily Arrow snapshot (compressed) |
| `stations/stations.day.{date}.arrow` | Daily Arrow snapshot (only if `UNCOMPRESSED_ARROW_FILES` is set) |
| `stations/stations.day.arrow.gz` | Symlink to latest daily Arrow file |
| `stations.arrow.gz` | Legacy symlink to latest daily Arrow file |

Additional symlinks are created for month, year, and yearnz (New Zealand year) Arrow files.

## JSON Format

`stations.json` contains an array of station objects. Only stations with a `lastPacket` timestamp are included. `stations-complete.json` includes all stations.

```json
[
  {
    "id": 42,
    "station": "LFLE",
    "lat": 45.5629,
    "lng": 5.9126,
    "primary_location": [45.5629, 5.9126],
    "lastPacket": 1741968000,
    "lastLocation": 1741968000,
    "lastBeacon": 1741967400,
    "status": "v0.2.8.RPI-GPU CPU:0.7 RAM:292.2/970.5MB NTP:0.5ms/-4.3ppm +44.8C 2/2Acfts[1h] RF:+38+2.5ppm/-0.3dB/+10.7dB@10km[23481]/+12.3dB@10km[7/13]",
    "moved": false,
    "bouncing": false,
    "valid": true,
    "layerMask": 3,
    "outputEpoch": 1741968000,
    "outputDate": "2026-03-14",
    "stats": {
      "ignoredTracker": 0,
      "invalidTracker": 12,
      "invalidTimestamp": 0,
      "ignoredStationary": 450,
      "ignoredSignal0": 3,
      "ignoredPAW": 0,
      "ignoredH3stationary": 120,
      "ignoredElevation": 5,
      "count": 85000
    },
    "beaconActivity": "ff7f00e0ff1f00000000000000000000000000",
    "beaconActivityDate": "2026-03-14",
    "uptime": 83.3
  }
]
```

### Station fields

| Field | Type | Description |
|-------|------|-------------|
| `id` | `u16` | Unique station ID, assigned sequentially starting from 1 |
| `station` | `string` | Station callsign |
| `lat` | `f64?` | Current latitude (from most recent Location packet) |
| `lng` | `f64?` | Current longitude |
| `primary_location` | `[f64, f64]?` | Reference location `[lat, lng]` used for move detection |
| `previous_location` | `[f64, f64]?` | Prior location before the most recent move |
| `lastPacket` | `u32?` | Unix timestamp of the last packet processed for coverage |
| `lastLocation` | `u32?` | Unix timestamp of the last Location beacon |
| `lastBeacon` | `u32?` | Unix timestamp of the last Status beacon |
| `status` | `string?` | Body text from the most recent Status beacon |
| `notice` | `string?` | Human-readable move/bounce notice (empty string if no significant movement) |
| `moved` | `bool` | Station moved beyond the threshold since `primary_location` was set |
| `bouncing` | `bool` | Station is bouncing between two locations (histories merged) |
| `valid` | `bool` | Station is actively contributing to coverage data |
| `layerMask` | `u8?` | Bitmask of protocol layers this station has received data for (see [Layer mask](#layer-mask)) |
| `outputEpoch` | `u32?` | Unix timestamp of the last rollup that included this station |
| `outputDate` | `string?` | Date string (`YYYY-MM-DD`) of the last rollup output |
| `lastOutputFile` | `u32?` | Epoch of the last output file written for this station |
| `stats` | `object` | Packet filtering statistics (see [Stats](#stats)) |
| `beaconActivity` | `string?` | Daily beacon activity bitvector, hex-encoded (see [Beacon activity](#beacon-activity)). **Not included in `stations.json`** — only in per-station `{name}/{name}.json` files |
| `beaconActivityDate` | `string?` | UTC date (`YYYY-MM-DD`) the beacon activity bitvector covers. **Not included in `stations.json`** — only in per-station files |
| `uptime` | `f32?` | Percentage (0.0–100.0) of 10-minute slots active relative to elapsed slots today. Computed at output time, not persisted. Included in both `stations.json` and per-station files |

Fields marked with `?` are optional and omitted from JSON when null/absent.

### Stats

Per-station packet filtering counters. These accumulate over the lifetime of the station and are not reset.

| Field | Description |
|-------|-------------|
| `ignoredTracker` | Packets ignored because the tracker is on the ignore list |
| `invalidTracker` | Packets with invalid tracker data |
| `invalidTimestamp` | Packets with invalid or out-of-range timestamps |
| `ignoredStationary` | Packets ignored because the aircraft was stationary |
| `ignoredSignal0` | Packets ignored due to zero signal strength |
| `ignoredPAW` | PilotAware packets ignored |
| `ignoredH3stationary` | Packets ignored because the H3 cell hasn't changed |
| `ignoredElevation` | Packets ignored due to elevation filter (bad altitude data) |
| `count` | Total packets accepted for coverage recording |

### Layer mask

Bitmask indicating which protocol layers this station has received data for. Test with `layerMask & (1 << bit)`.

| Bit | Layer | Description |
|-----|-------|-------------|
| 0 | `combined` | Combined (all protocols merged) |
| 1 | `flarm` | FLARM |
| 2 | `adsb` | ADS-B |
| 3 | `adsl` | ADS-L |
| 4 | `fanet` | FANET |
| 5 | `paw` | PilotAware |
| 6 | `ogntrk` | OGN Tracker |
| 7 | `safesky` | SafeSky |

### Beacon activity

The `beaconActivity` field is a 144-bit bitvector encoded as 36 lowercase hex characters, representing station beacon activity over a 24-hour UTC day. Each bit corresponds to one 10-minute slot (24 hours x 6 slots/hour = 144 slots). This field is only present in per-station `{name}/{name}.json` files, not in the station list (`stations.json`).

The bitvector is stored as 3 little-endian 64-bit words, serialised as 18 bytes (8 + 8 + 2) in little-endian byte order:

- Bytes 0-7 (hex chars 0-15): bits 0-63 (slots 0-63, 00:00-10:30)
- Bytes 8-15 (hex chars 16-31): bits 64-127 (slots 64-127, 10:40-21:10)
- Bytes 16-17 (hex chars 32-35): bits 128-143 (slots 128-143, 21:20-23:50)

**Slot calculation:** `slot = hour * 6 + minute / 10`

**Bit extraction (JavaScript):**
```javascript
function isSlotActive(hex, slot) {
  const byteIndex = Math.floor(slot / 8);
  const bitIndex = slot % 8;
  const byte = parseInt(hex.substr(byteIndex * 2, 2), 16);
  return (byte & (1 << bitIndex)) !== 0;
}
```

The bitvector resets when the UTC date changes. `beaconActivityDate` indicates which date the bitvector covers. If `beaconActivityDate` does not match the current UTC date, the bitvector is stale and should be treated as empty.

A set bit means the station sent at least one beacon (Location or Status packet) during that 10-minute window. This can be used to visualise station uptime patterns.

## Arrow Format

The Arrow IPC stream files contain the same station data in columnar format, with a reduced set of columns for efficient loading:

| Column | Arrow Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `id` | `UInt32` | no | Station ID |
| `name` | `Utf8` | no | Station callsign |
| `lat` | `Float32` | no | Latitude (0.0 if unknown) |
| `lng` | `Float32` | no | Longitude (0.0 if unknown) |
| `valid` | `Boolean` | no | Station is valid |
| `lastPacket` | `UInt32` | no | Last packet Unix timestamp (0 if never) |
| `layerMask` | `UInt8` | no | Protocol layer bitmask |
| `uptime` | `Float32` | yes | Today's uptime percentage (null if no activity today) |

Stations are sorted by `id` ascending in the Arrow output.

## Move Detection

Station location is tracked from Location beacons. When a station reports a position more than 2km (configurable via `STATION_MOVE_THRESHOLD_KM`) from its `primary_location`:

- **Large move** (both primary and previous distance exceed threshold): `moved = true`, history is reset, `primary_location` updated
- **Bouncing** (new position is close to `previous_location`): `bouncing = true`, locations are merged
- **Small move** (< threshold): location is updated, history is preserved

Moved stations are marked `valid = false` during rollup and excluded from coverage processing until their data is re-established at the new location.

## Lifecycle

1. **Startup**: Station data is loaded from LevelDB (`{DB_PATH}status`). Station IDs are preserved across restarts.
2. **Live processing**: Each station Location or Status beacon updates the in-memory state and persists to LevelDB via an async writer thread.
3. **Each rollup**: `stations.json`, `stations-complete.json`, and Arrow files are written. Symlinks are updated.
4. **Shutdown**: All station data is flushed to LevelDB.

Stations that have not received any packets for 31 days (configurable via `STATION_EXPIRY_TIME_DAYS`) are considered expired and may be excluded from active station output.
