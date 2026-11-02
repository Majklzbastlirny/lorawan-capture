"""MQTT -> one SQLite file per local day."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import queue
import shutil
import socket
import sqlite3
import statistics
import threading
import time
from collections import Counter, deque
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from . import explorer, store
from .chirpstack import messages
from .config import Config
from .devices import OwnDevices
from .phy import parse_phy
from .registry import Registry

log = logging.getLogger(__name__)
PENDING = object()  # own_last: not looked up yet, the writer was busy


def _iso(when: datetime | None) -> str | None:
    return when.isoformat(timespec="seconds") if when else None


def _gw_time(epoch: float | None) -> str | None:
    """A gateway timestamp, in the old logger's format (naive ISO, UTC)."""
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, timezone.utc).replace(tzinfo=None).isoformat()


class Capture:
    """Owns the current day's database.

    The MQTT network thread only queues raw messages; a single writer thread parses and
    stores them, so a slow or spun-down disk never stalls the MQTT session. The web UI
    reaches the live file only through this object (under `lock`), so one process holds
    the database and no file locking between processes is involved.
    """

    def __init__(self, cfg: Config, clock: Callable[[], datetime] | None = None) -> None:
        self.cfg = cfg
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.queue: queue.Queue = queue.Queue(maxsize=200_000)
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.day: date | None = None
        self.path: Path | None = None
        self.db: sqlite3.Connection | None = None
        self.rows_today = 0
        self.dirty = False
        self.last_commit = 0.0
        self.counters: Counter = Counter()
        self.started = self.clock()
        self.last_message: datetime | None = None
        self.last_error: str | None = None
        self.db_error_at: datetime | None = None  # the last time storing something failed
        self.repairs: list[dict] = []  # today's file repaired while this process runs
        self._repaired_at = -1e9
        self._last_error_log = 0.0
        self.gateways: dict[str, dict] = {}
        self.registry = Registry(cfg.data_dir / "gateways.json", cfg.gateway_names)
        self.own_devices = OwnDevices(cfg.data_dir / "devices.json", cfg.own_devaddrs, self.clock)
        self.mqtt: dict = {"connected": False, "since": None, "error": None, "subscriptions": {}}
        self._mqtt_logged: tuple[str, float] = ("", 0.0)
        self.client = None
        self.writer: threading.Thread | None = None
        self._live: tuple[float, dict | None] = (0.0, None)
        self._sweep_lock = threading.Lock()
        self._pending_subs: dict[int, list[str]] = {}
        # own device -> {"t": last heard, "fcnt", "intervals": seconds per frame-counter step}
        self.own_seen: dict[str, dict] = {}
        # the same, ready to read without the lock: {"t", "fcnt", "typical"}, replaced whole
        self.own_snapshot: dict[str, dict] = {}
        self._own_seeded: set[str] = set()
        self._joins: dict[str, datetime] = {}  # followed DevEUI -> its latest join request

    # ------------------------------------------------------------------ MQTT
    def start_mqtt(self) -> None:
        import paho.mqtt.client as mqtt  # here, so everything else is testable without paho

        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                             client_id=self.cfg.mqtt_client_id, clean_session=True)
        if self.cfg.mqtt_username:
            client.username_pw_set(self.cfg.mqtt_username, self.cfg.mqtt_password)
        if self.cfg.mqtt_tls:
            client.tls_set()
        client.on_connect = self._on_connect
        client.on_connect_fail = self._on_connect_fail
        client.on_disconnect = self._on_disconnect
        client.on_subscribe = self._on_subscribe
        client.on_message = self.on_message
        client.reconnect_delay_set(min_delay=1, max_delay=60)
        client.connect_async(self.cfg.mqtt_host, self.cfg.mqtt_port, keepalive=60)
        client.loop_start()  # retries the first connection too
        self.client = client

    def _mqtt_problem(self, text: str) -> None:
        """Shown in the UI at once; logged when it changes, else every 5 minutes (paho retries
        with a backoff of up to a minute and would fill the log otherwise)."""
        self.mqtt.update(connected=False, error=text)
        now = time.monotonic()
        if text != self._mqtt_logged[0] or now - self._mqtt_logged[1] > 300:
            self._mqtt_logged = (text, now)
            log.error("%s, retrying", text)

    def _on_connect(self, client, userdata, flags, reason_code, properties=None) -> None:
        if reason_code.is_failure:
            self._mqtt_problem(f"broker {self.cfg.mqtt_host}:{self.cfg.mqtt_port} refused the connection: {reason_code}")
            return
        self.mqtt.update(connected=True, since=_iso(self.clock()), error=None)
        self._mqtt_logged = ("", 0.0)
        log.info("MQTT connected to %s:%s", self.cfg.mqtt_host, self.cfg.mqtt_port)
        _, mid = client.subscribe([(topic, 0) for topic in self.cfg.topics])
        self._pending_subs[mid] = list(self.cfg.topics)

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties=None) -> None:
        for topic, code in zip(self._pending_subs.pop(mid, []), reason_codes):
            if code.is_failure:
                self.mqtt["subscriptions"][topic] = f"refused ({code})"
                log.warning("subscription %s REFUSED by the broker (%s): check its ACL", topic, code)
            else:
                self.mqtt["subscriptions"][topic] = "ok"
                log.info("subscribed %s", topic)

    def _on_connect_fail(self, client, userdata) -> None:
        """No TCP session at all (paho does not say why, so find out)."""
        host, port = self.cfg.mqtt_host, self.cfg.mqtt_port
        try:
            socket.getaddrinfo(host, port)
            self._mqtt_problem(f"cannot reach the broker at {host}:{port} (no answer or connection refused)")
        except OSError:
            self._mqtt_problem(f"cannot resolve the broker name {host!r}")

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None) -> None:
        if not self.mqtt["connected"]:
            return  # the end of a refused attempt: _on_connect already said why
        self.mqtt.update(connected=False, since=_iso(self.clock()),
                         error=None if reason_code == 0 else f"connection lost: {reason_code}")
        if not self.stop_event.is_set():
            log.warning("MQTT connection lost (%s), reconnecting", reason_code)

    def on_message(self, client, userdata, msg) -> None:
        try:
            self.queue.put_nowait((self.clock(), msg.topic, bytes(msg.payload)))
        except queue.Full:
            self.counters["dropped"] += 1

    # ---------------------------------------------------------------- writer
    def start_writer(self) -> None:
        self.writer = threading.Thread(target=self.run, name="writer", daemon=True)
        self.writer.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.client is not None:
            try:
                self.client.disconnect()
                self.client.loop_stop()
            except Exception:  # noqa: BLE001 - shutting down anyway
                pass
        if self.writer is not None:
            self.writer.join(timeout=8)

    def today(self) -> date:
        return self.clock().astimezone(self.cfg.tz).date()

    def run(self) -> None:
        self.cfg.data_dir.mkdir(parents=True, exist_ok=True)
        snapshots = self.cfg.data_dir / ".snapshots"
        if snapshots.is_dir():
            for leftover in snapshots.iterdir():  # from downloads cut short by a restart
                leftover.unlink(missing_ok=True)
        notes = self.cfg.data_dir / "NOTES.md"  # site notes for the downloads, edited by hand
        if not notes.exists():
            try:
                shutil.copyfile(Path(__file__).with_name("static") / "NOTES.default.md", notes)
            except OSError as err:
                log.warning("cannot create %s: %s", notes, err)
        with self.lock:
            self._open(self.today())
            self._catch_up_joins()
        self._sweep_async()
        while True:
            try:
                batch = [self.queue.get(timeout=0.5)]
            except queue.Empty:
                batch = []
                if self.stop_event.is_set():
                    break
            while batch and len(batch) < 2000:
                try:
                    batch.append(self.queue.get_nowait())
                except queue.Empty:
                    break
            with self.lock:
                for item in batch:
                    self._handle(*item)
                today = self.today()
                if today > self.day:  # midnight, also when nothing is arriving
                    self._rotate(today)
                if self.dirty and time.monotonic() - self.last_commit >= self.cfg.commit_seconds:
                    self._commit()
            self.registry.maybe_save()
        with self.lock:
            self._close()
        self.registry.save()
        log.info("writer stopped, %s closed", self.path.name if self.path else "-")

    def _open(self, day: date) -> None:
        self.day = day
        self.path = store.day_file(self.cfg.data_dir, day)
        try:
            self.db = store.open_db(self.path)
            problem = store.quick_check(self.db)  # e.g. after a power cut in the middle of a write
            if problem != "ok":
                raise sqlite3.DatabaseError(f"database disk image is malformed: {problem}")
        except sqlite3.DatabaseError as err:
            if not store.is_damaged(err):
                raise
            self._repair(str(err), force=True)
        self.rows_today = self.db.execute("SELECT count(*) FROM frames").fetchone()[0]
        self._live = (0.0, None)
        log.info("writing %s%s", self.path.name, f" ({self.rows_today} frames already)" if self.rows_today else "")

    def _close(self) -> None:
        if self.db is not None:
            self._commit()
            self.db.close()
            self.db = None

    def _rotate(self, day: date) -> None:
        self._close()
        self._open(day)
        self._sweep_async()

    def _commit(self) -> None:
        try:
            self.db.commit()
        except sqlite3.Error as err:
            self._error(f"commit failed, last {self.cfg.commit_seconds:.0f} s lost: {err}", db=True)
            try:
                self.db.rollback()
            except sqlite3.Error:
                pass
            if store.is_damaged(err):
                self._repair(str(err))
        self.dirty = False
        self.last_commit = time.monotonic()

    def _repair(self, why: str, force: bool = False) -> bool:
        """Today's file is damaged: keep a copy, repair it (store.repair) and go on writing into it.
        Not again within 10 minutes of the last try (unless opening the file), meanwhile the errors
        are only counted."""
        if not force and time.monotonic() - self._repaired_at < 600:
            return False
        self._repaired_at = time.monotonic()
        log.error("%s is damaged (%s): repairing it", self.path.name, why)
        if self.db is not None:
            try:
                self.db.close()
            except sqlite3.Error:
                pass
            self.db = None
        try:
            result = store.repair(self.path, self.clock())
        except (OSError, sqlite3.Error) as err:  # beyond repair: set it aside and start the day's file anew
            log.exception("cannot repair %s", self.path.name)
            kept = store.damaged_name(self.path, self.clock())
            try:
                os.replace(self.path, kept)
                self.path.with_name(self.path.name + "-journal").unlink(missing_ok=True)
            except OSError:
                pass
            result = {"action": "replaced", "problem": str(err), "kept": kept.name, "saved": {}, "lost": None}
        self.db = store.open_db(self.path)
        self._live = (0.0, None)
        self.repairs.append({"at": _iso(self.clock()), "file": self.path.name, "why": why, **result})
        log.warning("%s %s: %s rows kept, %s lost; the damaged file is kept as %s", self.path.name, result["action"],
                    sum(result["saved"].values()), result["lost"], result["kept"])
        return True

    def _error(self, text: str, db: bool = False) -> None:
        self.counters["errors"] += 1
        self.last_error = f"{_iso(self.clock())} {text}"
        if db:
            self.db_error_at = self.clock()
        if time.monotonic() - self._last_error_log > 60:
            self._last_error_log = time.monotonic()
            log.warning("%s (more errors within a minute are only counted)", text)

    def _gateway(self, gateway_id: str) -> dict:
        return self.gateways.setdefault(gateway_id, {"last_up": None, "last_stats": None,
                                                     "online": None, "conn_since": None})

    def _handle(self, recv: datetime, topic: str, payload: bytes) -> None:
        parts = topic.split("/")
        if len(parts) < 4 or parts[-4] != "gateway":
            self.counters["ignored"] += 1
            return
        gateway_id, kind = parts[-3].lower(), f"{parts[-2]}/{parts[-1]}"
        handler = {"event/up": self._uplink, "event/stats": self._stats, "state/conn": self._conn}.get(kind)
        if handler is None:
            self.counters["ignored"] += 1
            return
        day = recv.astimezone(self.cfg.tz).date()
        if day > self.day:
            self._rotate(day)
        self.last_message = recv
        if self.registry.seen(gateway_id, _iso(recv), active=kind != "state/conn"):
            log.info("new gateway %s (first message: %s)", gateway_id, kind)
        try:
            handler(recv, gateway_id, payload)
        except sqlite3.Error as err:
            if store.is_damaged(err) and self._repair(str(err)):
                try:  # once more, into the repaired file
                    handler(recv, gateway_id, payload)
                    return
                except Exception as again:  # noqa: BLE001
                    err = again
            self._error(f"database error on {topic}: {err}", db=True)
        except Exception as err:  # noqa: BLE001 - one bad message must never stop the capture
            self._error(f"cannot decode {topic}: {err!r}")

    def _uplink(self, recv: datetime, gateway_id: str, payload: bytes) -> None:
        up = messages.parse_uplink(payload, gateway_id)
        phy = up.phy_payload
        if not phy:
            self.counters["empty"] += 1
            return
        o = parse_phy(phy)
        mod = up.modulation
        if mod.kind == messages.LORA:
            sf, bw, cr = mod.spreading_factor, mod.bandwidth, str(mod.code_rate or 0)
        else:
            sf = bw = None
            cr = ""
        dev = o.get("dev_addr")
        mtype = o.get("mtype")
        if mtype == "JoinRequest" and o.get("dev_eui") in self.own_devices.followed:
            self._joins[o["dev_eui"]] = recv
        elif (self._joins and dev and dev not in self.own_devices.addrs and o.get("is_downlink") == 0
              and o.get("major") == 0 and mtype in ("UnconfirmedDataUp", "ConfirmedDataUp")
              and (o.get("fcnt") or 0) <= store.REJOIN_FCNT and up.crc_status in (None, "CRC_OK")):
            self._adopt(dev, recv)  # before it is stored, so that the row counts as own
        self.db.execute(store.INSERT_FRAME, (
            recv.isoformat(), _gw_time(up.gw_time), hashlib.sha1(phy).hexdigest(), gateway_id,
            1 if dev in self.own_devices.addrs else 0,
            o.get("mtype"), o.get("major"), o.get("is_downlink"), dev, o.get("nwk_id"),
            o.get("join_eui"), o.get("dev_eui"), o.get("fcnt"), o.get("fport"),
            o.get("adr"), o.get("ack"), o.get("adr_ack_req"), o.get("class_b"),
            o.get("fopts_len"), o.get("fopts_hex"), o.get("frm_len"), o.get("frm_hex"),
            o.get("mic_hex"), len(phy), phy.hex(), up.frequency, sf, bw, cr,
            up.rssi, up.snr, up.crc_status))
        self.dirty = True
        self.rows_today += 1
        self.counters["up"] += 1
        self._gateway(gateway_id)["last_up"] = _iso(recv)
        if (o.get("is_downlink") == 0 and dev in self.own_devices.addrs and o.get("major") == 0
                and up.crc_status in (None, "CRC_OK")):
            self._own_heard(dev, o.get("fcnt"), recv)

    def _adopt(self, dev: str, recv: datetime) -> None:
        """A first uplink of a new session from an unknown address, soon after a followed
        device's join request: that device's new address (see store.sessions)."""
        window = timedelta(seconds=store.REJOIN_WINDOW_S)
        for eui in [eui for eui, at in self._joins.items() if recv - at > window]:
            del self._joins[eui]
        if not self._joins:
            return
        eui = max(self._joins, key=self._joins.__getitem__)  # the latest join
        current = self.own_devices.following().get(eui, {}).get("dev_addr")
        if current and explorer.network(current)["label"] != explorer.network(dev)["label"]:
            return  # another network's device starting a session just now
        del self._joins[eui]
        self.own_devices.adopt(eui, dev, _iso(recv))

    def _catch_up_joins(self) -> None:
        """At the start: a followed device that joined again while the capture was not running
        (or before it was followed) is found in the recordings: its newest session, if its
        current address has not been heard since that session began."""
        for eui, follow in self.own_devices.following().items():
            found = self.node_history(eui)
            if not found or found[-1][1] == follow["dev_addr"]:
                continue
            current = follow["dev_addr"]
            if current is None or (self._last_heard(current) or "") < found[-1][0]:
                self.own_devices.follow_device(eui, follow["name"], found)

    def _last_heard(self, dev: str, days: int = 31) -> str | None:
        """When an address was last heard, looking back up to `days` daily files."""
        first_day = (self.day or self.today()) - timedelta(days=days - 1)
        sql = "SELECT max(ts_utc) FROM frames WHERE dev_addr = ?"
        for day, path in store.list_days(self.cfg.data_dir):  # newest first
            if day < first_day:
                break
            try:
                if day == self.day:
                    last = self.read(lambda run: next(iter(run(sql, (dev,))), (None,))[0])
                else:
                    conn = store.open_readonly(path)
                    try:
                        last = conn.execute(sql, (dev,)).fetchone()[0]
                    finally:
                        conn.close()
            except sqlite3.Error:
                continue
            if last:
                return last
        return None

    def node_history(self, dev_eui: str, days: int = 31) -> list[tuple[str, str]]:
        """(first uplink, DevAddr) of a device's sessions in the last `days` daily files, oldest
        first; unreadable days are left out."""
        found: dict[str, str] = {}
        first_day = (self.day or self.today()) - timedelta(days=days - 1)
        for day, path in sorted(store.list_days(self.cfg.data_dir)):
            if day < first_day:
                continue
            try:
                if day == self.day:
                    rows = self.read(lambda run: store.sessions(run, dev_eui))
                else:
                    conn = store.open_readonly(path)
                    try:
                        rows = store.sessions(conn.execute, dev_eui)
                    finally:
                        conn.close()
            except sqlite3.Error as err:
                log.warning("cannot look for %s in %s: %s", dev_eui, path.name, err)
                continue
            for first, addr in rows:
                found.setdefault(addr, first)
        return sorted((first, addr) for addr, first in found.items())

    def _stats(self, recv: datetime, gateway_id: str, payload: bytes) -> None:
        st = messages.parse_stats(payload, gateway_id)
        ts = recv.isoformat()
        self.db.execute(store.INSERT_STATS, (ts, gateway_id, st.rx_received,
                                             st.rx_received_ok, st.tx_received, st.tx_emitted))
        loc = st.location or {}
        self.db.execute(store.INSERT_STATS_INFO, (
            ts, gateway_id, _gw_time(st.time), st.config_version,
            loc.get("latitude"), loc.get("longitude"), loc.get("altitude"),
            json.dumps(st.metadata, sort_keys=True) if st.metadata else None))
        self.db.executemany(store.INSERT_STATS_COUNT, [
            (ts, gateway_id, kind, str(key), count)
            for kind, counts in (("rx_freq", st.rx_per_frequency), ("tx_freq", st.tx_per_frequency),
                                 ("rx_mod", st.rx_per_modulation), ("tx_mod", st.tx_per_modulation),
                                 ("tx_status", st.tx_per_status))
            for key, count in counts.items()])
        self.db.executemany(store.INSERT_STATS_DUTY, [
            (ts, gateway_id, st.duty_cycle_window, band.name or None, band.frequency_min,
             band.frequency_max, band.load_max, band.load_tracked)
            for band in st.duty_cycle_bands or ()])
        self.dirty = True
        self.counters["stats"] += 1
        self._gateway(gateway_id)["last_stats"] = _iso(recv)

    def _conn(self, recv: datetime, gateway_id: str, payload: bytes) -> None:
        self.counters["conn"] += 1
        if not payload:  # a cleared retained message, not a state
            return
        online = messages.parse_conn_state(payload, gateway_id).online
        g = self._gateway(gateway_id)
        if g["online"] is online:  # the retained state again after a reconnect
            return
        g["online"], g["conn_since"] = online, _iso(recv)
        self.db.execute(store.INSERT_CONN, (recv.isoformat(), gateway_id, "ONLINE" if online else "OFFLINE"))
        self.dirty = True

    # ------------------------------------------------------------ for the UI
    def status(self) -> dict:
        now = self.clock()
        try:
            size = self.path.stat().st_size if self.path else None
        except OSError:
            size = None
        return {  # copies: the writer and MQTT threads keep changing these
            "now": _iso(now),
            "started": _iso(self.started),
            "mqtt": {**self.mqtt, "subscriptions": dict(self.mqtt["subscriptions"]),
                     "broker": f"{self.cfg.mqtt_host}:{self.cfg.mqtt_port}"},
            "last_message": _iso(self.last_message),
            "last_message_age": round((now - self.last_message).total_seconds(), 1) if self.last_message else None,
            "counters": dict(self.counters),
            "queue": self.queue.qsize(),
            "day": self.day.isoformat() if self.day else None,
            "db": self.path.name if self.path else None,
            "db_size": size,
            "rows_today": self.rows_today,
            "last_error": self.last_error,
            "repairs": list(self.repairs),
            "writer_alive": self.writer is not None and self.writer.is_alive(),
            "gateways": {gid: dict(g) for gid, g in list(self.gateways.items())},
            "registry": self.registry.snapshot(),
            "devices": self.own_devices.snapshot(),
            "following": self.own_devices.following(),
        }

    def own(self) -> frozenset[str]:
        return self.own_devices.addrs

    def _own_heard(self, dev: str, fcnt: int, when: datetime) -> None:
        """Note an uplink of an own device (the same one comes once per gateway: counted once)."""
        seen = self.own_seen.get(dev)
        if seen is None:
            seen = self.own_seen[dev] = {"t": when, "fcnt": fcnt, "intervals": deque(maxlen=50)}
        elif fcnt == seen["fcnt"]:
            return
        else:
            step = store.fcnt_step(seen["fcnt"], fcnt)
            if 0 < step < 1000 and when > seen["t"]:
                seen["intervals"].append((when - seen["t"]).total_seconds() / step)
            seen["t"], seen["fcnt"] = max(seen["t"], when), fcnt
        typical = statistics.median(seen["intervals"]) if len(seen["intervals"]) >= 3 else None
        self.own_snapshot[dev] = {"t": seen["t"], "fcnt": seen["fcnt"], "typical": typical}

    def own_last(self, dev: str) -> dict | None | object:
        """{"t": last heard, "fcnt", "typical": seconds per uplink lately} of an own device, None if
        not heard today or yesterday. The first time for a device it is read from today's and
        yesterday's file (a restart forgets nothing), but only if the writer is free within half a
        second: otherwise PENDING, and the next call tries again. The health check must not wait."""
        if dev not in self._own_seeded:
            if not self.lock.acquire(timeout=0.5):
                return PENDING
            try:
                if dev not in self._own_seeded:
                    self._seed_own(dev)
                    self._own_seeded.add(dev)
            finally:
                self.lock.release()
        return self.own_snapshot.get(dev)

    def _seed_own(self, dev: str) -> None:
        sql = ("SELECT ts_utc, fcnt FROM frames WHERE dev_addr = ? AND is_downlink = 0 AND major = 0 "
               "AND (crc = 'CRC_OK' OR crc IS NULL) ORDER BY ts_utc DESC LIMIT 300")
        rows = []
        if self.day is not None:
            yesterday = store.day_file(self.cfg.data_dir, self.day - timedelta(days=1))
            if yesterday.exists():
                try:
                    conn = store.open_readonly(yesterday)
                    try:
                        rows += conn.execute(sql, (dev,)).fetchall()
                    finally:
                        conn.close()
                except sqlite3.Error as err:
                    log.warning("cannot read %s: %s", yesterday.name, err)
        if self.db is not None:
            rows += self.db.execute(sql, (dev,)).fetchall()
        self.own_seen.pop(dev, None)
        self.own_snapshot.pop(dev, None)
        for ts, fcnt in sorted(rows):
            self._own_heard(dev, fcnt, datetime.fromisoformat(ts))

    def live_summary(self) -> dict:
        """Summary of today's file, at most 30 s old."""
        with self.lock:
            stamp, summary = self._live
            own = self.own()
            if (summary is not None and time.monotonic() - stamp < 30 and summary["day"] == self.day.isoformat()
                    and summary["own_for"] == sorted(own)):
                return summary
            if self.dirty:
                self._commit()
            try:
                summary = store.summarize(self.db, self.day, self.cfg.tz, own)
            except sqlite3.DatabaseError as err:
                if not (store.is_damaged(err) and self._repair(str(err))):
                    raise
                summary = store.summarize(self.db, self.day, self.cfg.tz, own)
            self._live = (time.monotonic(), summary)
            return summary

    def recent_summary(self, max_age: float = 120.0) -> dict | None:
        """Today's live summary if one was made within max_age seconds; never makes one (no lock)."""
        stamp, summary = self._live
        return summary if summary is not None and time.monotonic() - stamp < max_age else None

    def day_summary(self, day: date, path: Path, compute: bool = True) -> dict | None:
        """Today's live summary, or a closed day's cached one: made now if missing (unless not
        `compute`), its own-device part redone if the own devices changed since."""
        if day == self.day:
            return self.live_summary()
        summary = store.cached_summary(path)
        if summary is None:
            return store.summarize_file(path, day, self.cfg.tz, self.own()) if compute else None
        own = self.own()
        if summary.get("own_for") != sorted(own):
            summary = store.refresh_own(path, summary, own)
        return summary

    def query(self, sql: str, params: tuple = ()) -> list:
        """Read today's file (uncommitted rows included) for the explorer."""
        with self.lock:
            return self.db.execute(sql, params).fetchall() if self.db is not None else []

    def read(self, work: Callable[[Callable], object]) -> object:
        """work(execute) on today's file, all of it under the lock: for reads that take several
        queries from one consistent state, or stream their rows (uncommitted rows included)."""
        with self.lock:
            if self.db is None:
                return work(lambda sql, params=(): iter(()))
            return work(self.db.execute)

    def snapshot(self, dest: Path) -> date:
        """Consistent copy of today's file while capture goes on (messages queue meanwhile)."""
        with self.lock:
            if self.dirty:
                self._commit()
            self.db.execute("VACUUM INTO ?", (str(dest),))
            return self.day

    # ------------------------------------------------------------- old days
    def _sweep_async(self) -> None:
        threading.Thread(target=self._sweep, name="sweep", daemon=True).start()

    def _sweep(self) -> None:
        """Retention, then a cached summary for every closed day that has none yet."""
        if not self._sweep_lock.acquire(blocking=False):
            return
        try:
            today = self.day
            for path in store.apply_retention(self.cfg.data_dir, today, self.cfg.keep_days):
                log.info("retention: deleted %s", path.name)
            for day, path in sorted(store.list_days(self.cfg.data_dir)):
                if day >= today or store.cached_summary(path) is not None:
                    continue
                started = time.monotonic()
                try:
                    summary = store.summarize_file(path, day, self.cfg.tz, self.own())
                except sqlite3.Error as err:
                    if not store.is_damaged(err):
                        log.warning("cannot summarise %s: %s", path.name, err)
                        continue
                    try:  # a closed day damaged too (by a reset at midnight, say)
                        result = store.repair(path, self.clock())
                        log.warning("%s was damaged, %s: %d rows kept, %s lost; the damaged file is kept as %s",
                                    path.name, result["action"], sum(result["saved"].values()), result["lost"],
                                    result["kept"])
                        summary = store.summarize_file(path, day, self.cfg.tz, self.own())
                    except (OSError, sqlite3.Error) as again:
                        log.warning("cannot repair %s: %s", path.name, again)
                        continue
                log.info("summarised %s: %d frames (%.1f s)", path.name, summary["rows"], time.monotonic() - started)
        finally:
            self._sweep_lock.release()
