"""Is the capture recording, and does anything need a look?

Problems (the capture itself is not recording properly) make /healthz fail, so Docker, Unraid
or Uptime Kuma can tell. Warnings (a silent gateway, an own device overdue, a channel busy with
foreign traffic) are for the UI and for /api/health: a gateway that is switched off for good
must not keep the container "unhealthy".
"""

from __future__ import annotations

import shutil
from datetime import datetime, timedelta

from . import explorer, store
from .capture import PENDING, Capture
from .config import Config

DISK_LOW = 1 << 30  # bytes: below this the capture will soon fail to write
DISK_WARN = 10 << 30
GATEWAY_RETIRED = timedelta(days=7)  # silent for longer: no warning any more, just shown
OWN_MIN_S = 15 * 60  # an own device is overdue after max(this, OWN_TIMES x its usual interval)
OWN_TIMES = 3
OWN_UNKNOWN_S = 60 * 60  # ... or after this when its interval is not known yet
CHANNEL_BUSY = 0.05  # share of an hour on air with packets that are not LoRaWAN
disk_usage = shutil.disk_usage  # replaced in tests


def ago(seconds: float) -> str:
    if seconds < 90:
        return f"{max(0, int(seconds))} s"
    if seconds < 5400:
        return f"{round(seconds / 60)} min"
    if seconds < 172_800:
        return f"{seconds / 3600:.1f} h".replace(".0 h", " h")
    return f"{round(seconds / 86400)} days"


def size(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _time(iso: str | None) -> datetime | None:
    return datetime.fromisoformat(iso) if iso else None


def check(capture: Capture, cfg: Config, full: bool = False) -> dict:
    """{"ok", "problems", "warnings", "items", "gateways", "own", "channels"}. Never waits for the
    database unless `full` (the Health tab, /api/health): that brings today's summary up to date for
    the busy channels; otherwise they come from a recent one, if there is one."""
    now = capture.clock()
    limit = cfg.health_silence_minutes * 60
    items: list[dict] = []

    def item(name: str, state: str, text: str) -> None:  # state: good / warning / critical
        items.append({"name": name, "state": state, "text": text})

    alive = capture.writer is not None and capture.writer.is_alive()
    failing = capture.db_error_at is not None and (now - capture.db_error_at).total_seconds() < 300
    if alive and failing:
        item("Writer", "critical", f"storing fails: {capture.last_error}")
    else:
        item("Writer", "good" if alive else "critical",
             "running" if alive else "stopped: nothing is being stored (see the container log)")
    for r in capture.repairs:
        if capture.path is not None and r["file"] == capture.path.name:
            lost = "" if r["lost"] is None else f", at most {r['lost']:,} lost"
            item("Today's file", "warning",
                 f"was damaged and repaired at {_time(r['at']).astimezone(cfg.tz):%H:%M} ({r['action']}): "
                 f"{sum(r['saved'].values()):,} rows kept{lost}; the damaged file is kept as {r['kept']}")

    since_start = (now - capture.started).total_seconds()
    if capture.last_message is not None:
        age = (now - capture.last_message).total_seconds()
        item("Messages", "critical" if age > limit else "good",
             f"none for {ago(age)}" if age > limit else f"last one {ago(age)} ago")
    elif since_start > limit:
        item("Messages", "critical", f"none since the start {ago(since_start)} ago")
    else:
        item("Messages", "good", "starting, none yet")

    mqtt = capture.mqtt
    broker = f"{cfg.mqtt_host}:{cfg.mqtt_port}"
    if mqtt["connected"]:
        item("Broker", "good", f"connected to {broker}")
    else:
        item("Broker", "warning", f"not connected to {broker}" + (f": {mqtt['error']}" if mqtt["error"] else ""))
    for topic, result in sorted(dict(mqtt["subscriptions"]).items()):
        if result != "ok":  # uplinks refused: stats may still arrive, yet nothing is recorded
            item("Subscription", "critical" if topic.endswith("/event/up") else "warning",
                 f"{topic} {result} by the broker (its ACL)")

    try:
        free = disk_usage(cfg.data_dir).free
        item("Disk", "critical" if free < DISK_LOW else "warning" if free < DISK_WARN else "good", f"{size(free)} free")
    except OSError as err:
        item("Disk", "critical", f"cannot check: {err}")

    dropped, queued = capture.counters.get("dropped", 0), capture.queue.qsize()
    if dropped or queued > 10_000:
        item("Queue", "warning", f"{dropped:,} messages dropped since the start" if dropped
             else f"{queued:,} messages waiting to be written")
    else:
        item("Queue", "good", "keeping up")
    errors = capture.counters.get("errors", 0)
    item("Errors", "warning" if errors else "good",
         f"{errors:,} since the start, the last: {capture.last_error}" if errors else "none since the start")

    gateways = []
    live = dict(capture.gateways)
    for gid, entry in sorted(capture.registry.snapshot().items()):
        name = entry.get("name") or gid
        times = [t for t in (_time(entry.get("last_seen")), _time((live.get(gid) or {}).get("last_up")),
                             _time((live.get(gid) or {}).get("last_stats"))) if t]
        last = max(times) if times else None
        if last is None:
            state, text = "info", "never heard; known only from its stored online/offline state"
        else:
            silent = (now - last).total_seconds()
            if silent > GATEWAY_RETIRED.total_seconds():
                state, text = "info", f"silent since {last:%Y-%m-%d} (no warning after a week)"
            elif silent > limit:
                state, text = "warning", f"silent for {ago(silent)}"
            else:
                state, text = "good", f"last heard {ago(silent)} ago"
        gateways.append({"id": gid, "name": name, "state": state, "text": text, "last": last.isoformat() if last else None})

    own = []
    for dev, entry in capture.own_devices.snapshot().items():
        name = entry.get("name") or dev
        seen = capture.own_last(dev)
        if seen is PENDING:
            own.append({"dev_addr": dev, "name": name, "state": "info", "last": None, "text": "checking"})
            continue
        if seen is None:
            overdue = since_start > OWN_UNKNOWN_S
            own.append({"dev_addr": dev, "name": name, "state": "warning" if overdue else "info", "last": None,
                        "text": "not heard today or yesterday" if overdue else "not heard yet"})
            continue
        typical = seen["typical"]
        allowed = max(OWN_MIN_S, OWN_TIMES * typical) if typical else OWN_UNKNOWN_S
        age = (now - seen["t"]).total_seconds()
        usual = f", usually every {ago(typical)}" if typical else ""
        own.append({"dev_addr": dev, "name": name, "last": seen["t"].isoformat(), "typical_s": typical,
                    "state": "warning" if age > allowed else "good",
                    "text": (f"not heard for {ago(age)}" if age > allowed else f"last heard {ago(age)} ago") + usual})

    summary = capture.live_summary() if full else capture.recent_summary()
    channels = busy_channels(capture, now, summary) if summary else []
    problems = [f"{i['name']}: {i['text']}" for i in items if i["state"] == "critical"]
    warnings = ([f"{i['name']}: {i['text']}" for i in items if i["state"] == "warning"]
                + [f"Gateway {g['name']}: {g['text']}" for g in gateways if g["state"] == "warning"]
                + [f"{o['name']}: {o['text']}" for o in own if o["state"] == "warning"]
                + [c["text"] for c in channels])
    return {"ok": not problems, "problems": problems, "warnings": warnings, "items": items,
            "gateways": gateways, "own": own, "channels": channels, "limit_minutes": cfg.health_silence_minutes,
            "checked": now.isoformat(timespec="seconds")}


def busy_channels(capture: Capture, now: datetime, summary: dict) -> list[dict]:
    """Channels where packets that are not LoRaWAN took over CHANNEL_BUSY of the last full hour or
    of this one so far (at least 10 minutes of it), as the gateway hearing most of them saw it."""
    tz = capture.cfg.tz
    local = now.astimezone(tz)
    hours = [(local.hour, (local - local.replace(minute=0, second=0, microsecond=0)).total_seconds())]
    if local.hour > 0:
        hours.insert(0, (local.hour - 1, summary["hour_seconds"][local.hour - 1]))
    names = {gid: entry.get("name") or gid for gid, entry in capture.registry.snapshot().items()}
    worst: dict[str, dict] = {}
    for gid, per_freq in summary.get("occupancy", {}).items():
        for freq, rows in per_freq.items():
            for hour, *seconds in rows:
                span = dict(hours).get(hour)
                if not span or span < 600:
                    continue
                share = dict(zip(store.CLASSES, seconds))
                foreign = (share["other"] + share["sf5"]) / span
                if foreign >= CHANNEL_BUSY and foreign > worst.get(freq, {}).get("share", 0):
                    worst[freq] = {"freq": int(freq), "hour": hour, "share": round(foreign, 4), "gateway": gid,
                                   "text": f"{int(freq) / 1e6:.3f} MHz: {round(100 * foreign)} % of the hour from {hour:02d}:00 "
                                           f"on air with packets that are not LoRaWAN (heard at {names.get(gid, gid)})"}
    return sorted(worst.values(), key=lambda c: -c["share"])


def recent_days(capture: Capture, source, days: int) -> list[dict]:
    """One row per day, newest first, for the last `days` days since the first file: whether it was
    recorded, the gaps, what each gateway's reports say was stored, the own devices."""
    files = dict(store.list_days(capture.cfg.data_dir))
    if not files:
        return []
    today = capture.day or max(files)
    first = max(min(files), today - timedelta(days=days - 1))
    silences = {}
    previous_last = None
    for day, info in source.per_day(first, today, "silences", explorer.one_query(
            explorer.STATS_TIMES, lambda rows: store.silences([r[0] for r in rows]))):
        gaps = list(info["gaps"])
        if info["first"] is not None:
            if previous_last and (_time(info["first"]) - _time(previous_last)).total_seconds() > store.SILENCE_S:
                gaps.insert(0, [previous_last, info["first"]])  # from the day before (or a missing day)
            previous_last = info["last"]
        silences[day] = (info, gaps)
    out = []
    day = today
    while day >= first:
        path = files.get(day)
        if path is None:
            out.append({"day": day.isoformat(), "file": False})
            day -= timedelta(days=1)
            continue
        summary = capture.day_summary(day, path)
        info, gaps = silences.get(day, ({"first": None}, []))
        firsts = [g["first"] for g in summary["gateways"].values() if g.get("first")]
        lasts = [g["last"] for g in summary["gateways"].values() if g.get("last")]
        out.append({
            "day": day.isoformat(), "file": True, "today": day == capture.day, "size": path.stat().st_size,
            "rows": summary["rows"], "first": min(firsts) if firsts else None, "last": max(lasts) if lasts else None,
            "reports_known": info["first"] is not None,
            "gaps": gaps, "gap_s": round(sum((_time(b) - _time(a)).total_seconds() for a, b in gaps)),
            "gateways": {gid: {"frames": g["frames"], "reports": (g.get("stats") or {}).get("reports", 0),
                               "uptime": g.get("uptime"), **(g.get("check") or {})}
                         for gid, g in summary["gateways"].items()},
            "own": {dev: {k: o.get(k) for k in ("heard", "expected", "unrecorded")} for dev, o in summary["own"].items()},
        })
        day -= timedelta(days=1)
    return out
