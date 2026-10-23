#!/usr/bin/env python3
"""
OGN Station Inspector

Connects to OGN APRS-IS and collects packets for a specific station, then
prints a diagnostic report about its behavior and data quality. Useful for
understanding why a station might (or might not) be on the ignore list.

Usage:
    python3 ogn_station_inspect.py DL4MEA-8
    python3 ogn_station_inspect.py SAFESKY --duration 300
"""

import argparse
import math
import re
import socket
import statistics
import sys
import time
from collections import Counter
from datetime import datetime, timezone

from ogn_stats import (
    parse_message, parse_position, ignore_reason, ignore_source_reason,
    PROTOCOLS,
)

# ── Packet field extraction ──────────────────────────────────────────────────

SIGNAL_VALUE_RE = re.compile(r"(\d+\.?\d*)dB")
TIMESTAMP_RE = re.compile(r"/(\d{6})h")
GPS_QUALITY_RE = re.compile(r"gps(\d+)x(\d+)")
ALT_RE = re.compile(r"/A=(\d{6})")


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km between two points."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def parse_delay(payload, recv_time):
    """Parse APRS HHMMSS timestamp and return delay in seconds, or None."""
    m = TIMESTAMP_RE.search(payload)
    if not m:
        return None
    ts = m.group(1)
    try:
        h, mi, s = int(ts[:2]), int(ts[2:4]), int(ts[4:6])
        pkt_time = recv_time.replace(hour=h, minute=mi, second=s, microsecond=0)
        delay = (recv_time - pkt_time).total_seconds()
        if delay < 0:
            delay += 86400
        return delay if delay < 86400 else None
    except (ValueError, OverflowError):
        return None


def parse_packet(source, tocall, relay, payload):
    """Extract diagnostic fields from a parsed APRS packet."""
    lat, lon = parse_position(payload)

    m = SIGNAL_VALUE_RE.search(payload)
    signal_db = float(m.group(1)) if m else None

    m = ALT_RE.search(payload)
    alt_ft = int(m.group(1)) if m else None

    m = GPS_QUALITY_RE.search(payload)
    gps_h, gps_v = (int(m.group(1)), int(m.group(2))) if m else (None, None)

    recv_time = datetime.now(timezone.utc)
    delay = parse_delay(payload, recv_time)

    return {
        "source": source, "tocall": tocall, "relay": relay,
        "payload": payload, "recv_time": recv_time,
        "lat": lat, "lon": lon, "signal_db": signal_db,
        "alt_ft": alt_ft, "gps_h": gps_h, "gps_v": gps_v,
        "delay_s": delay,
    }


# ── APRS-IS connection ──────────────────────────────────────────────────────


def connect_aprs(server, port, station):
    """Connect to APRS-IS with entry-station and budlist filters."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(30)
    sock.connect((server, port))

    banner = sock.recv(1024).decode("ascii", errors="replace").strip()
    print(f"  Server: {banner}")

    login = (
        f"user N0CALL pass -1 vers ogn-inspect 1.0 "
        f"filter e/{station} b/{station}\r\n"
    )
    sock.sendall(login.encode())

    resp = sock.recv(1024).decode("ascii", errors="replace").strip()
    print(f"  Login:  {resp}")

    sock.settimeout(10)
    return sock


# ── Data collection ──────────────────────────────────────────────────────────


def collect(sock, station, duration):
    """Collect packets for `duration` seconds. Returns (relayed, sourced) lists."""
    station_upper = station.upper()
    relayed = []
    sourced = []
    deadline = time.monotonic() + duration
    buf = ""

    try:
        while time.monotonic() < deadline:
            try:
                data = sock.recv(4096)
                if not data:
                    break
                buf += data.decode("ascii", errors="replace")
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    result = parse_message(line)
                    if not result:
                        continue
                    source, tocall, relay, payload = result
                    pkt = parse_packet(source, tocall, relay, payload)

                    if relay.upper() == station_upper:
                        relayed.append(pkt)
                    if source.upper() == station_upper:
                        sourced.append(pkt)

                    total = len(relayed) + len(sourced)
                    remaining = max(0, deadline - time.monotonic())
                    print(
                        f"\r  Packets: {total}, {remaining:.0f}s remaining...   ",
                        end="", flush=True,
                    )

            except socket.timeout:
                try:
                    sock.sendall(b"#keepalive\r\n")
                except OSError:
                    break
    except (KeyboardInterrupt, OSError):
        pass

    print()
    return relayed, sourced


# ── Report ───────────────────────────────────────────────────────────────────

# Strip APRS timestamp prefix (e.g. "/132201h" or ">132201h") to get content
BEACON_TS_RE = re.compile(r"^[/>]\d{6}h\s*")
# Position comment: everything after the standard position fields
# Position format: /HHMMSShDDMM.MMN.DDDMM.MME./A=NNNNNN then optional comment
POS_COMMENT_RE = re.compile(
    r"^/\d{6}h"                           # timestamp
    r"\d{4}\.\d{2}[NS].\d{5}\.\d{2}[EW]"  # lat/lon
    r"./A=\d{6}\s*"                        # symbol + altitude
)


def _print_beacons(sourced):
    """Print station beacon details: status messages and position comments."""
    status_payloads = []
    position_comments = []

    for p in sourced:
        payload = p["payload"]
        if payload.startswith(">"):
            # Status beacon — strip timestamp prefix
            content = BEACON_TS_RE.sub("", payload)
            if content:
                status_payloads.append(content)
        elif payload.startswith("/"):
            # Position beacon — extract comment after standard fields
            m = POS_COMMENT_RE.match(payload)
            if m:
                comment = payload[m.end():].strip()
                if comment:
                    position_comments.append(comment)

    if not status_payloads and not position_comments:
        return

    print()
    print("  Station Beacons:")

    if status_payloads:
        # Deduplicate: strip the timestamp which changes each beacon,
        # keep unique content patterns
        seen = []
        for s in status_payloads:
            if s not in seen:
                seen.append(s)
        for s in seen[:5]:
            print(f"    status: {s}")
        if len(seen) > 5:
            print(f"    ... and {len(seen) - 5} more unique status messages")

    if position_comments:
        seen = []
        for c in position_comments:
            if c not in seen:
                seen.append(c)
        for c in seen[:5]:
            print(f"    comment: {c}")


def print_report(station, relayed, sourced, duration):
    W = 72
    print()
    print("=" * W)
    print(f"  Station Inspection: {station}")
    print(f"  Collection: {duration}s, {len(relayed)} relayed, "
          f"{len(sourced)} from station")

    reason = ignore_reason(station)
    src_reason = ignore_source_reason(station)
    if reason or src_reason:
        parts = []
        if reason:
            parts.append(f"relay: {reason}")
        if src_reason:
            parts.append(f"source: {src_reason}")
        print(f"  Ignore list: YES ({', '.join(parts)})")
    else:
        print(f"  Ignore list: no")
    print("=" * W)

    # ── Station's own position ───────────────────────────────────────────
    print()
    print("  Station Position:")
    src_positions = [(p["lat"], p["lon"]) for p in sourced if p["lat"] is not None]
    if src_positions:
        avg_lat = statistics.mean(lat for lat, _ in src_positions)
        avg_lon = statistics.mean(lon for _, lon in src_positions)
        print(f"    Self-reported: {avg_lat:.4f}, {avg_lon:.4f} "
              f"(from {len(src_positions)} beacon(s))")
        station_pos = (avg_lat, avg_lon)
    else:
        print("    No self-reported position")
        station_pos = None

    # ── Station beacons ──────────────────────────────────────────────────
    if sourced:
        _print_beacons(sourced)

    if not relayed:
        print()
        print("  No relayed packets collected.")
        print("  Station may be offline or not acting as a relay.")
        print("=" * W)
        return

    # ── Relay overview ───────────────────────────────────────────────────
    devices = set(p["source"] for p in relayed)
    protocols = Counter(p["tocall"] for p in relayed)

    print()
    print(f"  Relay Activity: {len(relayed)} packets, {len(devices)} device(s)")
    proto_strs = [
        f"{PROTOCOLS.get(t, t)} x{c}" for t, c in protocols.most_common(10)
    ]
    print(f"    Protocols: {', '.join(proto_strs)}")

    # Device type prefixes (e.g. FLR, ICA, OGN, SKY, NAV)
    # OGN device IDs are 3-letter prefix + 6-char hex ID
    prefix_counts = Counter()
    for d in devices:
        if len(d) >= 3 and d[:3].isalpha():
            prefix_counts[d[:3]] += 1
        else:
            prefix_counts[d] += 1
    prefix_strs = [f"{p} x{c}" for p, c in prefix_counts.most_common(10)]
    print(f"    Device types: {', '.join(prefix_strs)}")

    # ── Coverage ─────────────────────────────────────────────────────────
    positions = [(p["lat"], p["lon"]) for p in relayed if p["lat"] is not None]
    print()
    print("  Coverage:")
    if positions:
        lats = [lat for lat, _ in positions]
        lons = [lon for _, lon in positions]

        if station_pos:
            ref_lat, ref_lon = station_pos
            label = "station"
        else:
            ref_lat = statistics.mean(lats)
            ref_lon = statistics.mean(lons)
            label = "centroid"

        distances = [
            haversine_km(ref_lat, ref_lon, lat, lon) for lat, lon in positions
        ]
        max_dist = max(distances)
        med_dist = statistics.median(distances)

        print(f"    Ref point ({label}): {ref_lat:.4f}, {ref_lon:.4f}")
        print(f"    Bounding box: {min(lats):.2f} to {max(lats):.2f} lat, "
              f"{min(lons):.2f} to {max(lons):.2f} lon")
        print(f"    Max radius: {max_dist:.1f} km, Median: {med_dist:.1f} km")
        no_pos = len(relayed) - len(positions)
        if no_pos:
            print(f"    No position: {no_pos}/{len(relayed)} "
                  f"({no_pos / len(relayed) * 100:.0f}%)")
    else:
        print(f"    No positions in any of {len(relayed)} packets")

    # ── Signal strength ──────────────────────────────────────────────────
    signals = [p["signal_db"] for p in relayed if p["signal_db"] is not None]
    print()
    print("  Signal Strength:")
    if signals:
        print(f"    Range: {min(signals):.1f} - {max(signals):.1f} dB")
        print(f"    Median: {statistics.median(signals):.1f} dB, "
              f"Mean: {statistics.mean(signals):.1f} dB")
        if len(signals) > 1:
            print(f"    Std dev: {statistics.stdev(signals):.1f} dB")
        no_sig = len(relayed) - len(signals)
        if no_sig:
            print(f"    Missing: {no_sig}/{len(relayed)} "
                  f"({no_sig / len(relayed) * 100:.0f}%)")
    else:
        print(f"    None in any of {len(relayed)} packets")

    # ── GPS quality ──────────────────────────────────────────────────────
    gps_vals = [
        (p["gps_h"], p["gps_v"]) for p in relayed if p["gps_h"] is not None
    ]
    if gps_vals:
        print()
        print("  GPS Quality:")
        gps_strs = Counter(f"{h}x{v}" for h, v in gps_vals)
        top = gps_strs.most_common(5)
        print(f"    Distribution: {', '.join(f'{s} ({c})' for s, c in top)}")

    # ── Timing / delay ───────────────────────────────────────────────────
    delays = [p["delay_s"] for p in relayed if p["delay_s"] is not None]
    if delays:
        sorted_delays = sorted(delays)
        p90_idx = min(int(len(sorted_delays) * 0.9), len(sorted_delays) - 1)
        print()
        print("  Packet Delay:")
        print(f"    Median: {statistics.median(delays):.1f}s, "
              f"90th pct: {sorted_delays[p90_idx]:.1f}s")
        over_10 = sum(1 for d in delays if d > 10)
        over_60 = sum(1 for d in delays if d > 60)
        if over_10:
            print(f"    >10s: {over_10}/{len(delays)} "
                  f"({over_10 / len(delays) * 100:.1f}%)")
        if over_60:
            print(f"    >60s: {over_60}/{len(delays)} "
                  f"({over_60 / len(delays) * 100:.1f}%)")

    # ── Anomaly detection ────────────────────────────────────────────────
    print()
    print("  Anomalies:")
    anomalies = []

    # No signal strength at all
    if not signals:
        anomalies.append(
            "All packets lack signal strength — likely a gateway, not RF"
        )
    elif len(signals) > 5 and statistics.stdev(signals) < 1.0:
        anomalies.append(
            f"Signal strength unusually uniform "
            f"(stdev={statistics.stdev(signals):.1f}dB)"
        )

    # No positions
    no_pos_pct = (len(relayed) - len(positions)) / len(relayed) * 100
    if no_pos_pct > 90:
        anomalies.append(
            f"{no_pos_pct:.0f}% of packets lack position data"
        )

    # Station doesn't beacon its own position
    if not station_pos and positions:
        anomalies.append("Station does not broadcast its own position")

    # Coverage anomalies
    if positions:
        ref = station_pos or (statistics.mean(lats), statistics.mean(lons))
        dists = [haversine_km(ref[0], ref[1], la, lo) for la, lo in positions]
        if max(dists) > 500:
            anomalies.append(
                f"Coverage radius {max(dists):.0f}km — unusually large"
            )
        if max(dists) < 1 and len(positions) > 10:
            anomalies.append(
                "All traffic from <1km radius — possible single-point injection"
            )

    # Single device
    if len(devices) == 1 and len(relayed) > 10:
        anomalies.append(
            f"Only relays one device ({next(iter(devices))})"
        )

    # Single protocol with multiple devices
    if len(protocols) == 1 and len(relayed) > 10 and len(devices) > 1:
        only_proto = next(iter(protocols))
        anomalies.append(
            f"Only relays {PROTOCOLS.get(only_proto, only_proto)} — "
            "may be protocol-specific gateway"
        )

    # High delay
    if delays:
        med_delay = statistics.median(delays)
        if med_delay > 30:
            anomalies.append(f"Median delay {med_delay:.0f}s — significantly delayed")
        over_60_pct = sum(1 for d in delays if d > 60) / len(delays) * 100
        if over_60_pct > 50:
            anomalies.append(f"{over_60_pct:.0f}% of packets delayed >60s")

    if not anomalies:
        print("    None detected")
    else:
        for a in anomalies:
            print(f"    * {a}")

    print()
    print("=" * W)


# ── Main ─────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Inspect an OGN APRS station's behavior and data quality"
    )
    parser.add_argument("station", help="Station callsign to inspect")
    parser.add_argument(
        "--duration", type=int, default=120,
        help="Collection duration in seconds (default: 120)",
    )
    parser.add_argument(
        "--server", default="aprs.glidernet.org",
        help="APRS-IS server (default: aprs.glidernet.org)",
    )
    parser.add_argument(
        "--port", type=int, default=14580,
        help="APRS-IS port (default: 14580)",
    )
    args = parser.parse_args()

    print("OGN Station Inspector")
    print(f"  Inspecting: {args.station}")
    print(f"  Collecting for {args.duration}s (Ctrl+C for early report)...")
    print()

    try:
        sock = connect_aprs(args.server, args.port, args.station)
    except OSError as e:
        print(f"  Connection error: {e}")
        sys.exit(1)

    relayed, sourced = collect(sock, args.station, args.duration)

    try:
        sock.close()
    except Exception:
        pass

    print_report(args.station, relayed, sourced, args.duration)


if __name__ == "__main__":
    main()
