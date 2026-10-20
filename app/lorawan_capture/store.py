"""Daily SQLite files: schema, naming, retention and the per-day summaries for the web UI."""

from __future__ import annotations

import bisect
import json
import math
import os
import re
import shutil
import sqlite3
import statistics
import threading
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# frames/stats and the indexes are exactly those of samples/lorawan_logger.py, so old and new
# recordings work with the same analysis scripts. `conn` (gateway online/offline) is new.
DDL = """
CREATE TABLE IF NOT EXISTS frames (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_utc TEXT NOT NULL,          -- when we recorded it
  gw_time TEXT,                  -- gateway timestamp if present
  frame_key TEXT NOT NULL,       -- sha1(phypayload): same frame across gateways
  gateway_id TEXT NOT NULL,
  is_own INTEGER NOT NULL,
  mtype TEXT, major INTEGER, is_downlink INTEGER,
  dev_addr TEXT, nwk_id INTEGER,
  join_eui TEXT, dev_eui TEXT,
  fcnt INTEGER, fport INTEGER,
  adr INTEGER, ack INTEGER, adr_ack_req INTEGER, class_b INTEGER,
  fopts_len INTEGER, fopts_hex TEXT,
  frm_len INTEGER, frm_hex TEXT, mic_hex TEXT,
  phy_len INTEGER, phy_hex TEXT,
  freq_hz INTEGER, sf INTEGER, bw_hz INTEGER, code_rate TEXT,
  rssi INTEGER, snr REAL, crc TEXT
);
CREATE INDEX IF NOT EXISTS ix_dev  ON frames(dev_addr, ts_utc);
CREATE INDEX IF NOT EXISTS ix_key  ON frames(frame_key);
CREATE INDEX IF NOT EXISTS ix_ts   ON frames(ts_utc);
CREATE TABLE IF NOT EXISTS stats (
  ts_utc TEXT, gateway_id TEXT,
  rx_received INTEGER, rx_crc_ok INTEGER, tx_received INTEGER, tx_emitted INTEGER
);
CREATE TABLE IF NOT EXISTS conn (
  ts_utc TEXT NOT NULL, gateway_id TEXT NOT NULL, state TEXT
);
-- The rest of each stats message; join to `stats` on (ts_utc, gateway_id).
CREATE TABLE IF NOT EXISTS stats_info (
  ts_utc TEXT NOT NULL, gateway_id TEXT NOT NULL,
  gw_time TEXT,                  -- the gateway's own clock when it sent the report
  config_version TEXT,
  latitude REAL, longitude REAL, altitude REAL,
  metadata TEXT                  -- JSON object, e.g. concentratord_version
);
CREATE TABLE IF NOT EXISTS stats_counts (
  ts_utc TEXT NOT NULL, gateway_id TEXT NOT NULL,
  kind TEXT NOT NULL,            -- rx_freq / tx_freq (key = Hz), rx_mod / tx_mod (key = sf7,
                                 -- sf7_bw250, fsk ...), tx_status (key = OK, TOO_LATE ...)
  key TEXT NOT NULL,
  count INTEGER NOT NULL         -- since the previous report; Concentratord's rx_* breakdowns
                                 -- count good-CRC packets only
);
CREATE TABLE IF NOT EXISTS stats_duty (
  ts_utc TEXT NOT NULL, gateway_id TEXT NOT NULL,
  window_s REAL, band TEXT, freq_min INTEGER, freq_max INTEGER,
  load_max_s REAL, load_tracked_s REAL  -- transmit time allowed / used within the window
);
"""

FRAME_COLUMNS = (
    "ts_utc", "gw_time", "frame_key", "gateway_id", "is_own", "mtype", "major", "is_downlink",
    "dev_addr", "nwk_id", "join_eui", "dev_eui", "fcnt", "fport", "adr", "ack", "adr_ack_req",
    "class_b", "fopts_len", "fopts_hex", "frm_len", "frm_hex", "mic_hex", "phy_len", "phy_hex",
    "freq_hz", "sf", "bw_hz", "code_rate", "rssi", "snr", "crc",
)
INSERT_FRAME = "INSERT INTO frames (%s) VALUES (%s)" % (
    ",".join(FRAME_COLUMNS), ",".join("?" * len(FRAME_COLUMNS)))
INSERT_STATS = "INSERT INTO stats VALUES (?,?,?,?,?,?)"
INSERT_CONN = "INSERT INTO conn VALUES (?,?,?)"
INSERT_STATS_INFO = "INSERT INTO stats_info VALUES (?,?,?,?,?,?,?,?)"
INSERT_STATS_COUNT = "INSERT INTO stats_counts VALUES (?,?,?,?,?)"
INSERT_STATS_DUTY = "INSERT INTO stats_duty VALUES (?,?,?,?,?,?,?,?)"

DAY_FILE = re.compile(r"^frames_(\d{4}-\d{2}-\d{2})\.db$")

LORAWAN_MTYPES = ("JoinRequest", "JoinAccept", "UnconfirmedDataUp", "UnconfirmedDataDown",
                  "ConfirmedDataUp", "ConfirmedDataDown")
# bad_crc: received with a bad CRC (only stored if the gateway forwards those, e.g. Concentratord
# with disable_crc_filter), so its header is noise; sf5: anything at SF5 (the 2026 flood);
# lorawan: a LoRaWAN header at a LoRaWAN data rate; other: everything else (proprietary links,
# SF6, foreign formats that happen to parse)
CLASS_SQL = ("CASE WHEN crc IS NOT NULL AND crc != 'CRC_OK' THEN 'bad_crc' "
             "WHEN sf = 5 THEN 'sf5' "
             "WHEN sf BETWEEN 7 AND 12 AND major = 0 AND mtype IN (%s) THEN 'lorawan' "
             "ELSE 'other' END") % ",".join("'%s'" % m for m in LORAWAN_MTYPES)
CLASSES = ("lorawan", "other", "sf5", "bad_crc")
SUMMARY_FORMAT = 7  # bump when summarize() changes, so cached summaries are rebuilt
SILENCE_S = 300  # no gateway stats report for this long: nothing was being recorded
REPORT_SLACK = 1.5  # reports this many intervals apart: one or more in between were missed
DOWN_AFTER = 2.5  # a gateway's reports this many intervals apart: two or more missed, it was down


def day_file(data_dir: Path, day: date) -> Path:
    return data_dir / f"frames_{day.isoformat()}.db"


def summary_file(db_path: Path) -> Path:
    return db_path.with_name(db_path.stem + ".summary.json")


def list_days(data_dir: Path) -> list[tuple[date, Path]]:
    """The daily files, newest first."""
    out = []
    for path in data_dir.glob("frames_*.db"):
        match = DAY_FILE.match(path.name)
        if match:
            try:
                out.append((date.fromisoformat(match.group(1)), path))
            except ValueError:
                continue
    return sorted(out, reverse=True)


def tune(conn: sqlite3.Connection) -> sqlite3.Connection:
    """Sort in memory: grouping a flood day's 800k rows then takes a third less time."""
    conn.execute("PRAGMA temp_store = MEMORY")
    conn.execute("PRAGMA cache_size = -32768")  # 32 MB
    return conn


def open_db(path: Path) -> sqlite3.Connection:
    conn = tune(sqlite3.connect(path, check_same_thread=False, timeout=30))
    conn.executescript(DDL)
    conn.commit()
    return conn


def open_readonly(path: Path) -> sqlite3.Connection:
    """A closed daily file, read-only."""
    return tune(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True))


def is_damaged(err: BaseException) -> bool:
    """Whether a database error means a damaged file, not a busy, locked or full one."""
    return getattr(err, "sqlite_errorname", "") in ("SQLITE_CORRUPT", "SQLITE_NOTADB") or any(
        text in str(err) for text in ("malformed", "not a database"))


def quick_check(conn: sqlite3.Connection) -> str:
    """'ok', or SQLite's first complaint about the file."""
    try:
        return conn.execute("PRAGMA quick_check(1)").fetchone()[0]
    except sqlite3.DatabaseError as err:
        return str(err)


def damaged_name(path: Path, now: datetime) -> Path:
    """Where a damaged daily file is kept: frames_<day>.damaged-<UTC time>.db, not a day of its own."""
    return path.with_name(f"{path.stem}.damaged-{now.astimezone(timezone.utc):%Y%m%dT%H%M%SZ}.db")


def repair(path: Path, now: datetime) -> dict:
    """A damaged daily file (a power cut or a hard reset in the middle of a write) made usable
    again. A copy of it is kept first, as it is (damaged_name), and the file itself is only ever
    replaced by a sound one: if rebuilding the indexes in a copy is enough, nothing is lost;
    otherwise every row that can still be read is copied into a new file that takes its place.
    {"action": "reindexed" or "salvaged", "problem", "kept", "saved": {table: rows}, "lost": rows
    in the stretches that could not be read (at most)}."""
    kept = damaged_name(path, now)
    shutil.copyfile(path, kept)  # as the cut left it; nothing below writes to it
    conn = sqlite3.connect(kept.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        problem = quick_check(conn)
    finally:
        conn.close()
    attempt = path.with_name(path.stem + ".reindex.db")  # indexes rebuilt in a copy, used only if sound
    shutil.copyfile(kept, attempt)
    conn = sqlite3.connect(attempt, timeout=30)
    try:
        try:
            conn.execute("REINDEX")
            conn.commit()
        except sqlite3.DatabaseError:
            conn.rollback()
        sound = quick_check(conn) == "ok"
        saved = {name: conn.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0] for (name,) in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'").fetchall()} if sound else {}
    finally:
        conn.close()
    if sound:
        os.replace(attempt, path)
        path.with_name(path.name + "-journal").unlink(missing_ok=True)
        return {"action": "reindexed", "problem": problem, "kept": kept.name, "saved": saved, "lost": 0}
    attempt.unlink(missing_ok=True)
    attempt.with_name(attempt.name + "-journal").unlink(missing_ok=True)
    fresh = path.with_name(path.stem + ".salvage.db")
    fresh.unlink(missing_ok=True)
    old = sqlite3.connect(kept.resolve().as_uri() + "?mode=ro", uri=True)  # read on its own: no shared transaction
    new = open_db(fresh)
    saved, lost = {}, 0
    try:
        for (table,) in new.execute("SELECT name FROM sqlite_master WHERE type = 'table' "
                                    "AND name NOT LIKE 'sqlite_%'").fetchall():
            try:
                old_columns = {row[1] for row in old.execute(f'PRAGMA table_info("{table}")')}
            except sqlite3.DatabaseError:
                old_columns = set()  # even the list of tables cannot be read
            columns = [row[1] for row in new.execute(f'PRAGMA table_info("{table}")') if row[1] in old_columns]
            if columns:
                saved[table], missing = _copy_rows(old, new, table, columns)
                lost += missing
        new.commit()
    finally:
        old.close()
        new.close()
    os.replace(fresh, path)
    path.with_name(path.name + "-journal").unlink(missing_ok=True)
    return {"action": "salvaged", "problem": problem, "kept": kept.name, "saved": saved, "lost": lost}


def _copy_rows(old: sqlite3.Connection, new: sqlite3.Connection, table: str, columns: list[str]) -> tuple[int, int]:
    """A table of a damaged file into the same table of a new one, in rowid ranges; a range that
    cannot be read is halved down to a few rows, so a damaged page costs little more than its own
    rows. Its indexes, if one of them can still be read, tell how many rows there were (a power
    cut tends to break the newest pages, and with them the table's own end). (saved, lost)."""
    top = expected = None
    try:
        top = old.execute(f'SELECT max(rowid) FROM "{table}"').fetchone()[0] or 0
    except sqlite3.DatabaseError:
        pass
    try:
        indexes = [name for (name,) in old.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ?", (table,)).fetchall()]
    except sqlite3.DatabaseError:
        indexes = []
    for index in indexes:
        try:
            expected, high = old.execute(
                f'SELECT count(*), max(r) FROM (SELECT rowid AS r FROM "{table}" INDEXED BY "{index}")').fetchone()
            top = max(top or 0, high or 0)
            break
        except sqlite3.DatabaseError:
            continue
    if top is None:
        try:
            top = old.execute("PRAGMA page_count").fetchone()[0] * 200  # more rows than its pages hold
        except sqlite3.DatabaseError:
            top = 0
    names = ",".join(f'"{c}"' for c in columns)
    select = f'SELECT {names} FROM "{table}" WHERE rowid BETWEEN ? AND ?'
    insert = f'INSERT INTO "{table}" ({names}) VALUES ({",".join("?" * len(columns))})'
    saved = unread = 0

    def copy(a: int, b: int) -> None:
        nonlocal saved, unread
        try:
            rows = old.execute(select, (a, b)).fetchall()
        except sqlite3.DatabaseError:
            if b - a < 8:
                unread += b - a + 1
                return
            middle = (a + b) // 2
            copy(a, middle)
            copy(middle + 1, b)
            return
        new.executemany(insert, rows)
        saved += len(rows)

    for a in range(1, top + 1, 1000):
        copy(a, min(a + 999, top))
    return saved, (max(expected - saved, 0) if expected is not None else unread)


def apply_retention(data_dir: Path, today: date, keep_days: int) -> list[Path]:
    """Delete daily files older than `keep_days` days before today (0 keeps everything)."""
    if keep_days <= 0:
        return []
    cutoff = today - timedelta(days=keep_days)
    removed = []
    for day, path in list_days(data_dir):
        if day < cutoff:
            for victim in (path, summary_file(path), path.with_name(path.name + "-journal")):
                victim.unlink(missing_ok=True)
            removed.append(path)
    return removed


def write_json(path: Path, data: object) -> None:
    tmp = path.with_name(f"{path.name}.{threading.get_ident()}.tmp")  # two threads may write one file
    tmp.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, path)


def read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def airtime(sf: int | None, bw: int | None, length: int, cr: int = 1) -> float:
    """Seconds on air of one packet. LoRa with explicit header, CRC and an 8-symbol preamble (what
    LoRaWAN uses; other systems may use longer ones), low data rate optimisation at SF11/SF12 on
    125 kHz; SF5 and SF6 by their own formula (SX126x / SX1302 datasheets). No spreading factor:
    FSK at 50 kbit/s (LoRaWAN's DR7) with 5 preamble, 3 sync, length and 2 CRC bytes."""
    if not sf:
        return (5 + 3 + 1 + length + 2) * 8 / 50_000
    bw = bw or 125_000
    cr = cr if 1 <= cr <= 4 else 1  # 1 = 4/5 ... 4 = 4/8
    if sf <= 6:
        blocks = math.ceil(max(8 * length + 16 - 4 * sf + 20, 0) / (4 * sf))
        symbols = 8 + 6.25 + 8 + blocks * (cr + 4)
    else:
        low = 1 if sf >= 11 and bw <= 125_000 else 0
        blocks = math.ceil(max(8 * length + 16 - 4 * sf + 8 + 20, 0) / (4 * (sf - 2 * low)))
        symbols = 8 + 4.25 + 8 + blocks * (cr + 4)
    return symbols * (1 << sf) / bw


def hour_seconds(day: date, tz: ZoneInfo) -> list[int]:
    """How long each local hour of the day really is: 3600 s, except where daylight saving time
    starts (that hour does not exist: 0) or ends (it happens twice: 7200)."""
    out = [0] * 24
    t = datetime.combine(day, datetime.min.time(), tz).astimezone(timezone.utc)
    stop = datetime.combine(day + timedelta(days=1), datetime.min.time(), tz).astimezone(timezone.utc)
    while t < stop:
        out[t.astimezone(tz).hour] += 3600
        t += timedelta(hours=1)
    return out


def summarize(conn: sqlite3.Connection, day: date, tz: ZoneInfo,
              own: frozenset[str] = frozenset()) -> dict:
    """Counts for one daily file: per gateway, local hour, channel and spreading factor, and the
    airtime per gateway, channel and hour (one grouped scan), plus how often the own devices'
    uplinks reached each gateway."""
    gateways: dict[str, dict] = {}
    hours = [dict.fromkeys(CLASSES, 0) for _ in range(24)]
    # gateway -> channel -> local hour -> seconds on air per class: the channel as heard there
    airtimes: dict = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: dict.fromkeys(CLASSES, 0.0))))
    channels: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    channels_bad: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    sfs: dict[str, int] = defaultdict(int)
    totals = dict.fromkeys(CLASSES, 0)

    def gateway(gateway_id: str) -> dict:
        return gateways.setdefault(gateway_id, {
            "frames": 0, **dict.fromkeys(CLASSES, 0), "first": None, "last": None,
            "stats": None, "conn": [], "reports": None, "uptime": None})

    local_hour: dict[str, int] = {}
    query = (f"SELECT gateway_id, substr(ts_utc, 1, 13), {CLASS_SQL}, freq_hz, sf, bw_hz, code_rate, phy_len, "
             "count(*), min(ts_utc), max(ts_utc) FROM frames GROUP BY 1, 2, 3, 4, 5, 6, 7, 8")
    for gateway_id, hour_utc, cls, freq, sf, bw, cr, length, n, first, last in conn.execute(query):
        g = gateway(gateway_id)
        g["frames"] += n
        g[cls] += n
        g["first"] = first if g["first"] is None else min(g["first"], first)
        g["last"] = last if g["last"] is None else max(g["last"], last)
        if hour_utc not in local_hour:
            local_hour[hour_utc] = datetime.fromisoformat(hour_utc + ":00:00+00:00").astimezone(tz).hour
        hours[local_hour[hour_utc]][cls] += n
        rate = int(cr) if cr and str(cr).isdigit() else 1
        airtimes[gateway_id][freq or 0][local_hour[hour_utc]][cls] += n * airtime(sf, bw, length or 0, rate)
        (channels_bad if cls == "bad_crc" else channels)[freq or 0][gateway_id] += n
        sfs["fsk" if sf is None else str(sf)] += n
        totals[cls] += n

    unique = conn.execute("SELECT count(DISTINCT frame_key) FROM frames").fetchone()[0]
    for gateway_id, reports, rx, ok, tx, emitted, last in conn.execute(
            "SELECT gateway_id, count(*), sum(rx_received), sum(rx_crc_ok), sum(tx_received), "
            "sum(tx_emitted), max(ts_utc) FROM stats GROUP BY 1"):
        gateway(gateway_id)["stats"] = {"reports": reports, "rx": rx, "rx_ok": ok, "tx": tx,
                                        "tx_emitted": emitted, "last": last}
    reports_at: dict[str, list[str]] = defaultdict(list)
    for gateway_id, ts in conn.execute("SELECT gateway_id, ts_utc FROM stats ORDER BY ts_utc"):
        reports_at[gateway_id].append(ts)
    for gateway_id, up in gateway_uptime(reports_at).items():
        gateway(gateway_id)["uptime"] = up
    try:
        for ts, gateway_id, state in conn.execute("SELECT ts_utc, gateway_id, state FROM conn ORDER BY ts_utc"):
            gateway(gateway_id)["conn"].append([ts, state])
    except sqlite3.OperationalError:
        pass  # a file from the old logger has no conn table
    try:
        for gateway_id, report in _reports(conn).items():
            gateway(gateway_id)["reports"] = report
    except sqlite3.OperationalError:
        pass  # written before the stats_* tables existed
    for gateway_id, check in _completeness(conn).items():
        gateway(gateway_id)["check"] = check

    return {
        "format": SUMMARY_FORMAT,
        "day": day.isoformat(),
        "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": sum(totals.values()),
        "unique": unique,
        "totals": totals,
        "gateways": gateways,
        "hours": hours,
        "channels": {str(f): dict(v) for f, v in sorted(channels.items())},
        "channels_bad": {str(f): dict(v) for f, v in sorted(channels_bad.items())},
        "sf": dict(sorted(sfs.items(), key=lambda kv: (kv[0] == "fsk", int(kv[0]) if kv[0].isdigit() else 0))),
        "own": own_stats(conn, own),
        "own_for": sorted(own),
        # gateway -> channel -> [[local hour, seconds on air per class (CLASSES order)], ...]
        "occupancy": {gw: {str(freq): [[h, *(round(per[c], 3) for c in CLASSES)] for h, per in sorted(by_hour.items())]
                           for freq, by_hour in sorted(per_freq.items())}
                      for gw, per_freq in airtimes.items()},
        "hour_seconds": hour_seconds(day, tz),
    }


def gateway_uptime(reports: dict[str, list[str]]) -> dict[str, dict]:
    """Per gateway, from the times of its stats reports (sent about every 30 s): the share of the
    recorded time it was reporting.

    Recorded: from the day's first report (any gateway) to its last, without the stretches in
    which no gateway at all was heard for over REPORT_SLACK of the shortest interval: the capture,
    the broker or every gateway was down then, and the reports cannot tell which. Down: two or
    more of its reports in a row missing (reports over DOWN_AFTER intervals apart), from half an
    interval after the last one that came to half an interval before the next, less the time
    nothing was recorded (and one interval around it: after a restart of the capture each gateway
    is heard again within its interval). A single missing report is not counted: a restart of the
    capture that no other gateway's reports show can cost one. up_at_end: still reporting when
    the day's reports end."""
    every = sorted(datetime.fromisoformat(t) for times in reports.values() for t in times)
    if len(every) < 2:
        return {}
    times = {gw: [datetime.fromisoformat(t) for t in ts] for gw, ts in reports.items()}
    intervals = {}
    for gw, ts in times.items():
        gaps = [(b - a).total_seconds() for a, b in zip(ts, ts[1:])]
        intervals[gw] = max(statistics.median(gaps), 1.0) if len(gaps) >= 3 else 30.0
    start, end = every[0], every[-1]
    nothing = REPORT_SLACK * min(intervals.values())
    blind = [(a, b) for a, b in zip(every, every[1:]) if (b - a).total_seconds() > nothing]
    recorded = (end - start).total_seconds() - sum((b - a).total_seconds() for a, b in blind)

    out = {}
    for gw, ts in times.items():
        interval = intervals[gw]
        pad, merged = timedelta(seconds=interval), []
        for x, y in blind:
            if merged and x - pad <= merged[-1][1]:
                merged[-1][1] = y + pad
            else:
                merged.append([x - pad, y + pad])
        down, outages = 0.0, []
        for a, b in zip([start, *ts], [*ts, end]):
            if (b - a).total_seconds() > DOWN_AFTER * interval:
                a, b = a + pad / 2, b - pad / 2
                lost = (b - a).total_seconds() - sum(max(0.0, (min(b, y) - max(a, x)).total_seconds()) for x, y in merged)
                if lost > 0:
                    down += lost
                    outages.append([a.isoformat(), b.isoformat()])
        out[gw] = {"pct": round(100 * (1 - down / recorded), 2) if recorded > 0 else None,
                   "down_s": round(down), "recorded_s": round(recorded), "interval_s": round(interval, 1),
                   "up_at_end": (end - ts[-1]).total_seconds() <= DOWN_AFTER * interval, "outages": outages[:50]}
    return out


def silences(times: list[str]) -> dict:
    """When nothing was being recorded (logger, broker or every gateway down), from the sorted
    times of the gateways' stats reports (each gateway sends one about every 30 s): the gaps of
    over SILENCE_S between two reports. first is None if there are too few reports to tell."""
    if len(times) < 10:
        return {"first": None, "last": None, "gaps": []}
    gaps, previous = [], datetime.fromisoformat(times[0])
    for a, b in zip(times, times[1:]):
        t = datetime.fromisoformat(b)
        if (t - previous).total_seconds() > SILENCE_S:
            gaps.append([a, b])
        previous = t
    return {"first": times[0], "last": times[-1], "gaps": gaps}


def file_silences(conn: sqlite3.Connection) -> dict:
    return silences([ts for (ts,) in conn.execute("SELECT ts_utc FROM stats ORDER BY ts_utc")])


def unrecorded(gaps: list, a: str, b: str) -> bool:
    """Whether nothing was recorded for over SILENCE_S between times a and b. Something was heard
    at a and at b, so only the part of a silence between them counts. (ISO times in one format
    sort like the times they stand for.)"""
    lo, hi = min(a, b), max(a, b)
    for start, end in gaps:
        if start < hi and end > lo:
            inside = datetime.fromisoformat(min(end, hi)) - datetime.fromisoformat(max(start, lo))
            if inside.total_seconds() > SILENCE_S:
                return True
    return False


def count_sent(steps, gaps: list) -> tuple[int, int]:
    """Uplinks sent, from (frame counter, time) pairs sorted by counter: a step of 1-999 means the
    uplinks in between were sent but not heard, any other step a counter reset (one uplink). Steps
    across a silence say nothing about the radio: those uplinks are counted apart, as unrecorded."""
    sent, missed = 1, 0
    for (fa, ta), (fb, tb) in zip(steps, steps[1:]):
        step = fb - fa
        if 0 < step < 1000 and unrecorded(gaps, ta, tb):
            sent, missed = sent + 1, missed + step - 1
        else:
            sent += step if 0 < step < 1000 else 1
    return sent, missed


def own_stats(conn: sqlite3.Connection, own: frozenset[str]) -> dict:
    """Per own device: uplinks sent (from the frame counter) and heard, per gateway and channel."""
    out: dict[str, dict] = {}
    if not own:
        return out
    gaps = file_silences(conn)["gaps"]
    heard: dict[str, dict[int, list]] = defaultdict(dict)
    marks = ",".join("?" * len(own))
    for dev, fcnt, gateway_id, freq, ts in conn.execute(
            f"SELECT dev_addr, fcnt, gateway_id, freq_hz, ts_utc FROM frames WHERE dev_addr IN ({marks}) "
            f"AND is_downlink = 0 AND (crc = 'CRC_OK' OR crc IS NULL)", tuple(sorted(own))):
        uplink = heard[dev].setdefault(fcnt, [freq, set(), ts])
        uplink[1].add(gateway_id)
        uplink[2] = min(uplink[2], ts)
    for dev, uplinks in heard.items():
        fcnts = sorted(uplinks)
        expected, missed = count_sent([(f, uplinks[f][2]) for f in fcnts], gaps)
        per_gateway: dict[str, int] = defaultdict(int)
        per_channel: dict[int, dict] = {}
        for fcnt in fcnts:
            freq, gws, _ = uplinks[fcnt]
            ch = per_channel.setdefault(freq, {"uplinks": 0, "gateways": defaultdict(int)})
            ch["uplinks"] += 1
            for gateway_id in gws:
                per_gateway[gateway_id] += 1
                ch["gateways"][gateway_id] += 1
        out[dev] = {
            "heard": len(fcnts), "expected": expected, "unrecorded": missed, "gateways": dict(per_gateway),
            "channels": {str(f): {"uplinks": c["uplinks"], "gateways": dict(c["gateways"])}
                         for f, c in sorted(per_channel.items())},
        }
    return out


_summary_lock = threading.Lock()


def update_summary(path: Path, changes: dict) -> dict | None:
    """Add to a closed day's cached summary. Under a lock, as web requests may add different
    parts of the same day's summary at the same time."""
    with _summary_lock:
        summary = cached_summary(path)
        if summary is None:
            return None
        summary = {**summary, **changes}
        write_json(summary_file(path), summary)
        return summary


def refresh_own(path: Path, summary: dict, own: frozenset[str]) -> dict:
    """A cached summary with the own-device part redone for the current own devices: one indexed
    query, instead of summarising the whole file again."""
    conn = sqlite3.connect(path, timeout=30)  # read-write: rolls back a journal left by a crash
    try:
        changes = {"own": own_stats(conn, own), "own_for": sorted(own)}
    finally:
        conn.close()
    return update_summary(path, changes) or {**summary, **changes}


def _completeness(conn: sqlite3.Connection) -> dict[str, dict]:
    """Per gateway: good packets stored versus good packets the gateway reported, over the
    intervals between two consecutive stats reports that both arrived. A report that never
    reached the logger (e.g. during a restart) would otherwise leave its packets stored but
    uncounted, and the share would drift over 100 %."""
    out = {}
    for (gateway_id,) in conn.execute("SELECT DISTINCT gateway_id FROM stats").fetchall():
        reports = conn.execute("SELECT ts_utc, rx_crc_ok FROM stats WHERE gateway_id = ? ORDER BY ts_utc",
                               (gateway_id,)).fetchall()
        if len(reports) < 2:
            continue
        times = [datetime.fromisoformat(ts) for ts, _ in reports]
        gaps = [(b - a).total_seconds() for a, b in zip(times, times[1:])]
        limit = 1.5 * statistics.median(gaps)
        frames = [ts for (ts,) in conn.execute(  # ISO strings in one format sort like the times
            "SELECT ts_utc FROM frames WHERE gateway_id = ? AND (crc = 'CRC_OK' OR crc IS NULL) ORDER BY ts_utc",
            (gateway_id,))]
        stored = reported = skipped = 0
        for (start, _), (end, ok), gap in zip(reports, reports[1:], gaps):
            if gap > limit:
                skipped += 1
                continue
            stored += bisect.bisect_right(frames, end) - bisect.bisect_right(frames, start)
            reported += ok or 0
        out[gateway_id] = {"stored": stored, "reported": reported, "skipped": skipped}
    return out


def _reports(conn: sqlite3.Connection) -> dict[str, dict]:
    """Per gateway, from the stats_* tables: downlink results, good packets per channel, peak
    duty-cycle use per band, and the latest config / location / clock."""
    out: dict[str, dict] = {}

    def report(gateway_id: str) -> dict:
        return out.setdefault(gateway_id, {"tx_status": {}, "rx_freq": {}, "duty_peak": {}, "info": None,
                                           "temp": None})

    for gateway_id, kind, key, n in conn.execute(
            "SELECT gateway_id, kind, key, sum(count) FROM stats_counts "
            "WHERE kind IN ('tx_status', 'rx_freq') GROUP BY 1, 2, 3"):
        report(gateway_id)[kind][key] = n
    for gateway_id, band, peak in conn.execute(
            "SELECT gateway_id, band, max(100.0 * load_tracked_s / load_max_s) FROM stats_duty "
            "WHERE load_max_s > 0 GROUP BY 1, 2"):
        report(gateway_id)["duty_peak"][band or "?"] = round(peak, 2)
    for gateway_id, ts, gw_time, version, lat, lon, alt, meta in conn.execute(
            "SELECT gateway_id, ts_utc, gw_time, config_version, latitude, longitude, altitude, metadata "
            "FROM stats_info WHERE rowid IN (SELECT max(rowid) FROM stats_info GROUP BY gateway_id)"):
        offset = None
        if gw_time:  # the gateway's clock minus ours (plus a little network delay)
            gw = datetime.fromisoformat(gw_time).replace(tzinfo=timezone.utc)
            offset = round((gw - datetime.fromisoformat(ts)).total_seconds(), 1)
        report(gateway_id)["info"] = {
            "at": ts, "config_version": version, "clock_offset_s": offset,
            "location": None if lat is None else [lat, lon, alt],
            "metadata": json.loads(meta) if meta else {},
        }
    try:  # concentrator temperature over the day (Concentratord metadata, a string)
        for gateway_id, low, high in conn.execute(
                "SELECT gateway_id, min(t), max(t) FROM (SELECT gateway_id, "
                "CAST(json_extract(metadata, '$.concentrator_temp') AS REAL) AS t FROM stats_info "
                "WHERE json_extract(metadata, '$.concentrator_temp') IS NOT NULL) GROUP BY 1"):
            report(gateway_id)["temp"] = {"min": round(low, 2), "max": round(high, 2)}
    except sqlite3.OperationalError:
        pass  # an SQLite without JSON functions
    return out


def summarize_file(path: Path, day: date, tz: ZoneInfo, own: frozenset[str]) -> dict:
    """Summarise a closed daily file and cache the result next to it."""
    conn = tune(sqlite3.connect(path, timeout=30))  # read-write: rolls back a journal left by a crash
    try:
        summary = summarize(conn, day, tz, own)
    finally:
        conn.close()
    write_json(summary_file(path), summary)
    return summary


def cached_summary(db_path: Path) -> dict | None:
    """The cached summary of a closed day, if there is one in the current format."""
    summary = read_json(summary_file(db_path))
    return summary if summary and summary.get("format") == SUMMARY_FORMAT else None


def brief(summary: dict | None) -> dict | None:
    """The few numbers shown per day in the file list."""
    if not summary:
        return None
    return {
        "rows": summary["rows"], "unique": summary["unique"], **summary["totals"],
        "gateways": len(summary["gateways"]),
        "own": {dev: {"heard": o["heard"], "expected": o["expected"]} for dev, o in summary["own"].items()},
    }
