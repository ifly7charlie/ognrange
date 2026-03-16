#!/usr/bin/env python3
"""
OGN APRS Protocol Statistics Monitor

Connects to the OGN APRS-IS feed and tracks protocol usage and geographic
distribution of devices defined in this repository.

State is saved periodically and on exit, so the script can be restarted
without losing accumulated data.

Usage:
    python3 ogn_stats.py [--state FILE] [--interval SECONDS]
"""

import argparse
import json
import os
import re
import signal
import socket
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# ── Protocol definitions from this repository ────────────────────────────────

PROTOCOLS = {
    "OGFLR":    "Flarm",
    "OGFLR6":   "Flarm (v6)",
    "OGFLR7":   "Flarm (v7)",
    "OGNTRK":   "OGN Tracker",
    "OGADSL":   "ADS-L",
    "OGADSB":   "ADS-B",
    "OGNFNT":   "FANET",
    "OGPAW":    "PilotAware",
    "OGNPAW":   "PilotAware",
    "OGSPOT":   "SPOT",
    "OGSPID":   "Spider",
    "OGLT24":   "LiveTrack24",
    "OGSKYL":   "Skylines",
    "OGCAPT":   "Capture",
    "OGNAVI":   "Naviter",
    "OGNMAV":   "MAVlink",
    "OGFLYM":   "Flymaster",
    "OGNINRE":  "InReach",
    "OGEVARIO": "eVario",
    "OGNWMN":   "Wingman",
    "OGNMTK":   "MicroTrack",
    "OGNMKT":   "MicroTrack",
    "OGNMYC":   "MyCloudbase",
    "OGNFNO":   "Flying Neurons",
    "OGNSKY":   "SafeSky",
    "OGAPIK":   "APIK",
    "OGNWGL":   "WeGlide",
    "OGNPUR":   "PureTrack",
    "OGAIRM":   "Airmate",
    "OGNEMO":   "Nemo",
    "OGNVOL":   "Volandoo",
    "OGSTUX":   "Stratux",
    "FXCAPP":   "flyxc",
    "OGNT":     "OGN (legacy)",
    "APRS":     "Legacy APRS",
}

# Ground station / infrastructure TOCALLs — excluded from stats
INFRASTRUCTURE = {
    "OGNSDR", "OGNSXR", "OGNDELAY", "OGNDVS", "OGNTTN",
    "OGMSHT", "OGNHEL", "OGNDSX", "OGAVZ",
}

# ── Gateway / ignored station detection ──────────────────────────────────────
# Ported from ognrange ignorestation.ts — these relay stations inject data via
# internet gateways rather than receiving it over RF.

IGNORED_STATIONS = {
    "SPOT", "SPIDER", "INREACH", "FLYMASTER", "NAVITER", "CAPTURS", "LT24",
    "SKYLINES", "NEMO", "ADSBEXCH", "MICROTRACK", "ANDROID",
    "IGCDROID", "APRSPUSH", "TEST", "DLY2APRS", "TTN2OGN", "TTN3OGN",
    "OBS2OGN", "HELIU2OGN", "AKASB", "CV32QG", "DL4MEA-8", "JETV-OGN",
    "GIGA01", "UNSET", "UNKNOWN", "STATIONS", "GLOBAL", "RELAY", "PWUNSET",
    "GLIDERNA", "X", "N1",
    # Individual Nemo stations
    "CYZR1", "CYCK1", "CYQS1", "CYSA3", "CYKF2", "CNZ8A", "CYHS1",
    "CNC4A", "CPC3A", "CZBA3", "AUBR2", "CNC3C", "CYEE1", "CNK4A",
    "CYOO1", "CNF4A",
}

_IGNORE_FULL_RE = re.compile(r"^[0-9]+$")
_IGNORE_START_RE = re.compile(
    r"^(FNB|XCG|XCC|OGN|RELAY|RND|bSky|AIRS[0-9]+|N0TEST-)", re.IGNORECASE
)
_IGNORE_ANY_RE = re.compile(r"[^A-Za-z0-9_-]")


def ignore_reason(name):
    """Return a short reason string if name matches the ignore list, else None.

    Uses both the explicit set and regex patterns — appropriate for relay/ground
    station names where e.g. a FLR-prefixed callsign indicates a misconfigured
    device acting as a receiver.
    """
    if name.upper() in IGNORED_STATIONS:
        return "explicit"
    if _IGNORE_FULL_RE.match(name):
        return "all-numeric"
    m = _IGNORE_START_RE.match(name)
    if m:
        return f"prefix: {m.group(1)}"
    if _IGNORE_ANY_RE.search(name):
        return "non-alnum char"
    return None


def ignore_source_reason(name):
    """Return a reason if a source/device name matches the explicit ignore list.

    Only checks the explicit set — regex patterns like FLR/OGN prefixes are
    normal device IDs when used as source names, not indicators of bad data.
    """
    if name.upper() in IGNORED_STATIONS:
        return "explicit"
    return None


# ── Geographic regions (lat_min, lat_max, lon_min, lon_max) ──────────────────

REGIONS = [
    ("Europe",         35,  72,  -25,   45),
    ("North America",  15,  72, -170,  -50),
    ("South America", -60,  15,  -90,  -30),
    ("Africa",        -35,  37,  -20,   55),
    ("Asia",            0,  75,   45,  180),
    ("Australasia",   -50,   5,  100,  180),
]

REGION_NAMES = [r[0] for r in REGIONS]

REGION_SHORT = {
    "Europe":        "Europe",
    "North America": "N.Amer",
    "South America": "S.Amer",
    "Africa":        "Africa",
    "Asia":          "Asia",
    "Australasia":   "Oceania",
}


def get_region(lat, lon):
    for name, lat_min, lat_max, lon_min, lon_max in REGIONS:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return name
    return "Other"


# ── APRS parsing ─────────────────────────────────────────────────────────────

POSITION_RE = re.compile(r"(\d{4}\.\d{2})([NS]).(\d{5}\.\d{2})([EW])")
MSG_RE = re.compile(r"^([^>]+)>([^,*]+)[,*](.+?):(.+)$")
RELAY_RE = re.compile(r"qA[A-Z],([^,*:]+)")
SIGNAL_RE = re.compile(r"\d+\.?\d*dB")
ALT_RE = re.compile(r"/A=(-?\d+)")


def parse_position(payload):
    """Extract lat/lon from APRS position payload."""
    m = POSITION_RE.search(payload)
    if not m:
        return None, None
    lat_str, lat_ns, lon_str, lon_ew = m.groups()
    lat = float(lat_str[:2]) + float(lat_str[2:]) / 60.0
    if lat_ns == "S":
        lat = -lat
    lon = float(lon_str[:3]) + float(lon_str[3:]) / 60.0
    if lon_ew == "W":
        lon = -lon
    return lat, lon


def parse_message(line):
    """Parse an APRS-IS line. Returns (source, tocall, relay, payload) or None."""
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    m = MSG_RE.match(line)
    if not m:
        return None
    source, tocall, path, payload = m.groups()
    relay_m = RELAY_RE.search(path)
    relay = relay_m.group(1) if relay_m else ""
    return source, tocall, relay, payload


# ── Statistics ───────────────────────────────────────────────────────────────


class Stats:
    def __init__(self, debug_filters=False):
        self.debug_filters = debug_filters
        self.start_time = datetime.now(timezone.utc).isoformat()
        self.total = 0
        self.counts = defaultdict(int)
        self.regions = defaultdict(lambda: defaultdict(int))
        self.devices = defaultdict(set)
        self.hourly = defaultdict(int)
        self.gateway_counts = defaultdict(int)
        self.no_position_counts = defaultdict(int)
        self.no_signal_counts = defaultdict(int)
        self.low_alt_counts = defaultdict(int)
        self.stations = defaultdict(set)

    def record(self, source, tocall, relay, payload):
        if tocall in INFRASTRUCTURE:
            return

        self.total += 1
        self.counts[tocall] += 1
        self.devices[tocall].add(source)

        if relay:
            gw_reason = ignore_reason(relay)
            if gw_reason:
                self.gateway_counts[tocall] += 1
                if self.debug_filters:
                    print(f"  [Gw]  {relay} ({gw_reason})  {source}>{tocall}")
            else:
                self.stations[tocall].add(relay)

        lat, lon = parse_position(payload)
        if lat is None:
            self.no_position_counts[tocall] += 1
        else:
            self.regions[tocall][get_region(lat, lon)] += 1

        if not SIGNAL_RE.search(payload):
            self.no_signal_counts[tocall] += 1

        alt_m = ALT_RE.search(payload)
        if alt_m and int(alt_m.group(1)) < 18000:
            self.low_alt_counts[tocall] += 1

        hour = datetime.now(timezone.utc).strftime("%Y-%m-%d %H")
        self.hourly[hour] += 1

    def save(self, path):
        data = {
            "start_time": self.start_time,
            "last_update": datetime.now(timezone.utc).isoformat(),
            "total": self.total,
            "protocols": {},
            "hourly": dict(self.hourly),
        }
        for tocall in self.counts:
            data["protocols"][tocall] = {
                "count": self.counts[tocall],
                "devices": sorted(self.devices[tocall]),
                "regions": dict(self.regions[tocall]),
                "gateway_count": self.gateway_counts[tocall],
                "no_position_count": self.no_position_counts[tocall],
                "no_signal_count": self.no_signal_counts[tocall],
                "low_alt_count": self.low_alt_counts[tocall],
                "stations": sorted(self.stations[tocall]),
            }
        tmp = path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)

    @classmethod
    def load(cls, path, debug_filters=False):
        stats = cls(debug_filters=debug_filters)
        try:
            with open(path) as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return stats
        stats.start_time = data.get("start_time", stats.start_time)
        stats.total = data.get("total", 0)
        stats.hourly = defaultdict(int, data.get("hourly", {}))
        for tocall, pdata in data.get("protocols", {}).items():
            stats.counts[tocall] = pdata["count"]
            stats.devices[tocall] = set(pdata.get("devices", []))
            for region, count in pdata.get("regions", {}).items():
                stats.regions[tocall][region] = count
            stats.gateway_counts[tocall] = pdata.get("gateway_count", 0)
            stats.no_position_counts[tocall] = pdata.get("no_position_count", 0)
            stats.no_signal_counts[tocall] = pdata.get("no_signal_count", 0)
            stats.low_alt_counts[tocall] = pdata.get("low_alt_count", 0)
            stats.stations[tocall] = set(pdata.get("stations", []))
        return stats

    def uptime_str(self):
        try:
            start = datetime.fromisoformat(self.start_time)
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            delta = datetime.now(timezone.utc) - start
        except Exception:
            delta = timedelta(0)
        days = delta.days
        hours, rem = divmod(delta.seconds, 3600)
        minutes = rem // 60
        parts = []
        if days:
            parts.append(f"{days}d")
        if hours or days:
            parts.append(f"{hours}h")
        parts.append(f"{minutes}m")
        return " ".join(parts)

    def rate_per_min(self):
        try:
            start = datetime.fromisoformat(self.start_time)
            if start.tzinfo is None:
                start = start.replace(tzinfo=timezone.utc)
            elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        except Exception:
            elapsed = 0
        if elapsed < 60:
            return 0.0
        return self.total / (elapsed / 60)

    @staticmethod
    def _fmt_pct(num, denom):
        """Format a count as a percentage of denom, right-aligned to 6 chars."""
        if denom == 0:
            return "    --"
        pct = num / denom * 100
        return f"{pct:5.0f}%"

    def print_summary(self):
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        total_devices = sum(len(d) for d in self.devices.values())
        rate = self.rate_per_min()

        # Build region header with short names
        rh_parts = [f"{REGION_SHORT[r]:>7s}" for r in REGION_NAMES]
        rh_parts.append(f"{'<18k':>7s}")
        rh = "  ".join(rh_parts)
        qh = f"{'Gw%':>6s} {'NoPos%':>6s} {'NodB%':>6s} {'Stns':>6s}"
        hdr = f"  {'Protocol':<20s} {'TOCALL':<10s} {'Messages':>10s} {'Devices':>8s}  {qh}  {rh}"
        W = max(len(hdr) + 2, 90)

        print()
        print("=" * W)
        print(f"  OGN APRS Protocol Statistics  --  {now_str}")
        print(f"  Running since: {self.start_time[:19]} UTC ({self.uptime_str()})")
        print(
            f"  Messages: {self.total:,}  |  Devices: {total_devices:,}"
            f"  |  Rate: ~{rate:,.0f}/min"
        )
        print("=" * W)

        sorted_tocalls = sorted(
            self.counts.keys(), key=lambda t: self.counts[t], reverse=True
        )

        if not sorted_tocalls:
            print("  No data yet...")
            print("=" * W)
            return

        print(hdr)
        print("  " + "-" * (W - 4))

        for tocall in sorted_tocalls:
            name = PROTOCOLS.get(tocall, f"({tocall})")
            count = self.counts[tocall]
            ndevices = len(self.devices[tocall])

            gw_s = self._fmt_pct(self.gateway_counts[tocall], count)
            nopos_s = self._fmt_pct(self.no_position_counts[tocall], count)
            nodb_s = self._fmt_pct(self.no_signal_counts[tocall], count)
            nstations = len(self.stations[tocall])

            total_regional = sum(self.regions[tocall].values())

            region_strs = []
            for rname in REGION_NAMES:
                rc = self.regions[tocall].get(rname, 0)
                if total_regional > 0:
                    pct = rc / total_regional * 100
                    if pct >= 0.05:
                        region_strs.append(f"{pct:6.1f}%")
                    else:
                        region_strs.append(f"{'':>7s}")
                else:
                    region_strs.append(f"{'--':>7s}")

            # Low altitude percentage (< 18,000 ft)
            if count > 0:
                low_pct = self.low_alt_counts[tocall] / count * 100
                if low_pct >= 0.05:
                    region_strs.append(f"{low_pct:6.1f}%")
                else:
                    region_strs.append(f"{'':>7s}")
            else:
                region_strs.append(f"{'--':>7s}")

            rstr = "  ".join(region_strs)

            print(
                f"  {name:<20s} {tocall:<10s} {count:>10,d} {ndevices:>8,d}"
                f"  {gw_s} {nopos_s:>6s} {nodb_s:>6s} {nstations:>6,d}  {rstr}"
            )

        print("=" * W)

        # Recent hourly rates
        hours = sorted(self.hourly.keys())
        if hours:
            recent = hours[-min(8, len(hours)) :]
            max_h = max(self.hourly[h] for h in recent) or 1
            print()
            print("  Recent hourly message counts:")
            for h in recent:
                c = self.hourly[h]
                bar_len = int(c / max_h * 40)
                print(f"    {h}  {'█' * bar_len} {c:,d}")
            print()


# ── APRS-IS connection ───────────────────────────────────────────────────────


def connect_aprs(server, port):
    """Connect to APRS-IS, login read-only, set global filter. Returns socket."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(30)
    sock.connect((server, port))

    # Read server banner
    banner = sock.recv(1024).decode("ascii", errors="replace").strip()
    print(f"  Server: {banner}")

    # Login read-only with broad filters for global coverage
    # Multiple range filters centred on each major region
    login = (
        "user N0CALL pass -1 vers ogn-stats 1.0 "
        "filter r/48/10/3000 r/40/-100/3000 r/-30/140/3000 "
        "r/5/30/3000 r/35/105/3000 r/-15/-55/2500\r\n"
    )
    sock.sendall(login.encode())

    resp = sock.recv(1024).decode("ascii", errors="replace").strip()
    print(f"  Login:  {resp}")

    sock.settimeout(90)
    return sock


# ── Main loop ────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="OGN APRS Protocol Statistics Monitor"
    )
    parser.add_argument(
        "--state",
        default="ogn_stats.json",
        help="State file path (default: ogn_stats.json)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Summary interval in seconds (default: 300)",
    )
    parser.add_argument(
        "--server",
        default="aprs.glidernet.org",
        help="APRS-IS server (default: aprs.glidernet.org)",
    )
    parser.add_argument(
        "--port", type=int, default=14580, help="APRS-IS port (default: 14580)"
    )
    parser.add_argument(
        "--debug-filters",
        action="store_true",
        help="Print each packet matched by Gw/Src ignore rules with the reason",
    )
    args = parser.parse_args()

    stats = Stats.load(args.state, debug_filters=args.debug_filters)
    running = True

    def handle_signal(sig, _frame):
        nonlocal running
        print(f"\n  Received signal {sig}, shutting down...")
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    print("OGN APRS Protocol Statistics Monitor")
    print(f"  State file: {os.path.abspath(args.state)}")
    print(f"  Summary every {args.interval}s")
    if stats.total > 0:
        print(f"  Resumed with {stats.total:,} existing messages")
    print()

    while running:
        try:
            print(f"  Connecting to {args.server}:{args.port}...")
            sock = connect_aprs(args.server, args.port)
            print("  Connected. Receiving messages...\n")

            buf = ""
            last_summary = time.monotonic()
            last_save = time.monotonic()
            msg_since_summary = 0

            while running:
                try:
                    data = sock.recv(4096)
                    if not data:
                        print("  Connection closed by server.")
                        break

                    buf += data.decode("ascii", errors="replace")
                    while "\n" in buf:
                        line, buf = buf.split("\n", 1)
                        result = parse_message(line)
                        if result:
                            source, tocall, relay, payload = result
                            stats.record(source, tocall, relay, payload)
                            msg_since_summary += 1

                    now = time.monotonic()

                    # Periodic summary
                    if now - last_summary >= args.interval:
                        stats.print_summary()
                        msg_since_summary = 0
                        last_summary = now

                    # Save state every 60s
                    if now - last_save >= 60:
                        stats.save(args.state)
                        last_save = now

                except socket.timeout:
                    try:
                        sock.sendall(b"#keepalive\r\n")
                    except OSError:
                        break

        except OSError as e:
            print(f"  Connection error: {e}")

        try:
            sock.close()
        except Exception:
            pass

        if running:
            print(f"  Reconnecting in 30s...")
            for _ in range(30):
                if not running:
                    break
                time.sleep(1)

    # Final save and summary
    print("\n  Shutting down...")
    stats.print_summary()
    stats.save(args.state)
    print(f"  State saved to {os.path.abspath(args.state)}")


if __name__ == "__main__":
    main()
