"""Small web UI: capture status, per-day statistics and downloads of the daily files."""

from __future__ import annotations

import base64
import gzip
import hmac
import json
import logging
import re
import shutil
import sqlite3
import uuid
import zipfile
from datetime import date, datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

from . import __version__, explorer, health, store
from .capture import Capture
from .config import Config
from .devices import normalize, normalize_eui

log = logging.getLogger(__name__)

STATIC = Path(__file__).with_name("static")
CHUNK = 1 << 20
DAY_API = re.compile(r"^/api/day/(\d{4}-\d{2}-\d{2})$")
DOWNLOAD = re.compile(r"^/download/frames_(\d{4}-\d{2}-\d{2})\.db(\.gz)?$")
GATEWAY_API = re.compile(r"^/api/gateways/([^/]+)$")
OWN_API = re.compile(r"^/api/own/([^/]+)$")
EXPLORE_API = re.compile(r"^/api/(devices|joins|other)$")
DEVICE_API = re.compile(r"^/api/device/([0-9A-Fa-f]{8})$")
DEVICE_CSV = re.compile(r"^/api/device/([0-9A-Fa-f]{8})/uplinks\.csv$")
FRAME_API = re.compile(r"^/api/frame/(\d{4}-\d{2}-\d{2})/([0-9a-f]{40})$")
ICONS = {"/favicon.svg": ("favicon.svg", "image/svg+xml"), "/favicon.ico": ("favicon.ico", "image/x-icon"),
         "/apple-touch-icon.png": ("apple-touch-icon.png", "image/png")}
MAX_BODY = 4096
MAX_SPAN_DAYS = 31  # the device views hold every uplink of the range in memory (~1.2 KB each)
COMPRESS_LEVEL = 3  # on flood days: ~3.7x smaller at ~90 MB/s; 6 is barely smaller and slower


class _Sink:
    """The response stream for a zip being built on the fly. Once something fails, writes go
    nowhere, so zipfile cannot finish a valid-looking archive that silently lacks the rest."""

    def __init__(self, out) -> None:
        self.out, self.dead = out, False

    def write(self, data) -> int:
        if not self.dead:
            self.out.write(data)
        return len(data)

    def flush(self) -> None:
        if not self.dead:
            self.out.flush()


def _day(text: str) -> date | None:
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


class Handler(BaseHTTPRequestHandler):
    capture: Capture
    cfg: Config
    server_version = f"lorawan-capture/{__version__}"

    def log_message(self, fmt: str, *args) -> None:
        log.debug("%s %s", self.address_string(), fmt % args)

    # ------------------------------------------------------------- plumbing
    def _send(self, status: int, body: bytes, ctype: str, headers: dict | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for key, value in {"Cache-Control": "no-store", **(headers or {})}.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _json(self, data: object) -> None:
        self._send(HTTPStatus.OK, json.dumps(data, separators=(",", ":")).encode(), "application/json")

    def _text(self, status: int, text: str, headers: dict | None = None) -> None:
        self._send(status, text.encode(), "text/plain; charset=utf-8", headers)

    def _authorized(self) -> bool:
        if not self.cfg.web_username:
            return True
        header = self.headers.get("Authorization", "")
        if not header.startswith("Basic "):
            return False
        try:
            user, _, password = base64.b64decode(header[6:]).decode("utf-8").partition(":")
        except (ValueError, UnicodeDecodeError):
            return False
        ok_user = hmac.compare_digest(user.encode(), self.cfg.web_username.encode())
        ok_password = hmac.compare_digest(password.encode(), (self.cfg.web_password or "").encode())
        return ok_user and ok_password

    # --------------------------------------------------------------- routes
    def do_GET(self) -> None:  # noqa: N802 - http.server naming
        path = urlsplit(self.path).path
        try:
            if path == "/healthz":  # for Docker / Unraid / Uptime Kuma: never waits for the database
                state = health.check(self.capture, self.cfg)
                return self._text(HTTPStatus.OK if state["ok"] else HTTPStatus.SERVICE_UNAVAILABLE,
                                  "ok" if state["ok"] else "; ".join(state["problems"]))
            if path in ICONS:  # no login: a phone fetches the home-screen icon without one
                name, ctype = ICONS[path]
                return self._send(HTTPStatus.OK, (STATIC / name).read_bytes(), ctype, {"Cache-Control": "max-age=86400"})
            if not self._authorized():
                return self._text(HTTPStatus.UNAUTHORIZED, "login required",
                                  {"WWW-Authenticate": 'Basic realm="lorawan-capture"'})
            if path in ("/", "/index.html"):
                return self._send(HTTPStatus.OK, (STATIC / "index.html").read_bytes(), "text/html; charset=utf-8")
            if path == "/api/status":
                return self._json(self._status())
            if path == "/api/days":
                return self._json(self._days())
            if path == "/api/health":
                return self._json(health.check(self.capture, self.cfg, full=True))
            if path == "/api/health/days":
                days = (parse_qs(urlsplit(self.path).query).get("days") or ["14"])[0]
                count = min(max(int(days), 1), 92) if days.isdigit() else 14
                return self._json({"days": health.recent_days(self.capture, explorer.Source(
                    self.capture, self.cfg.data_dir), count), "silence_s": store.SILENCE_S})
            if match := DAY_API.match(path):
                return self._day_summary(match.group(1))
            if match := EXPLORE_API.match(path):
                return self._explore(match.group(1), urlsplit(self.path).query)
            if match := DEVICE_API.match(path):
                return self._device(match.group(1).lower(), urlsplit(self.path).query)
            if match := DEVICE_CSV.match(path):
                return self._device_csv(match.group(1).lower(), urlsplit(self.path).query)
            if match := FRAME_API.match(path):
                return self._frame(match.group(1), match.group(2))
            if path == "/download/range.zip":
                return self._download_range(urlsplit(self.path).query)
            if path == "/download/README.md":
                return self._send(HTTPStatus.OK, self._readme().encode("utf-8"), "text/plain; charset=utf-8",
                                  {"Content-Disposition": 'inline; filename="README.md"'})
            if match := DOWNLOAD.match(path):
                return self._download(match.group(1), bool(match.group(2)))
            self._text(HTTPStatus.NOT_FOUND, "not found")
        except (BrokenPipeError, ConnectionResetError):
            pass  # the browser went away, e.g. a cancelled download
        except Exception:  # noqa: BLE001
            log.exception("error serving %s", path)
            try:
                self._text(HTTPStatus.INTERNAL_SERVER_ERROR, "internal error, see the container log")
            except OSError:
                pass

    def _read_body(self) -> bytes | None:
        """The request body, or None if it is missing or too large. Read it before answering,
        even a refusal: an unread body makes some systems reset the connection."""
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            return None
        if not 0 < length <= MAX_BODY:
            self.close_connection = True
            return None
        return self.rfile.read(length)

    def do_POST(self) -> None:  # noqa: N802 - http.server naming
        path = urlsplit(self.path).path
        try:
            raw = self._read_body()
            if not self._authorized():
                return self._text(HTTPStatus.UNAUTHORIZED, "login required",
                                  {"WWW-Authenticate": 'Basic realm="lorawan-capture"'})
            gateway, device = GATEWAY_API.match(path), OWN_API.match(path)
            if not (gateway or device):
                return self._text(HTTPStatus.NOT_FOUND, "not found")
            # JSON only: a form on another web site cannot send it, so no cross-site changes
            if self.headers.get("Content-Type", "").split(";")[0].strip().lower() != "application/json":
                return self._text(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, "send JSON")
            try:
                if raw is None:
                    raise ValueError("missing or too large request body")
                body = json.loads(raw)
                name = body.get("name") if isinstance(body, dict) else None
                if name is not None and not isinstance(name, str):
                    raise ValueError("the name must be text")
                dev_eui = body.get("dev_eui") if isinstance(body, dict) else None
                if dev_eui is not None and not isinstance(dev_eui, str):
                    raise ValueError("the DevEUI must be text")
                if gateway:
                    result = self.capture.registry.rename(unquote(gateway.group(1)), name)
                else:  # an own device: add, rename or link it to a DevEUI; or follow a device by its DevEUI
                    target = unquote(device.group(1))
                    try:
                        eui = normalize_eui(target)
                    except ValueError:
                        result = self.capture.own_devices.put(target, name, dev_eui)
                    else:
                        result = self.capture.own_devices.follow_device(eui, name, self.capture.node_history(eui))
            except ValueError as err:
                return self._text(HTTPStatus.BAD_REQUEST, str(err))
            self._json(result)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception:  # noqa: BLE001
            log.exception("error serving POST %s", path)
            try:
                self._text(HTTPStatus.INTERNAL_SERVER_ERROR, "internal error, see the container log")
            except OSError:
                pass

    def do_DELETE(self) -> None:  # noqa: N802 - http.server naming
        """Remove an own device. Browsers send DELETE across sites only if the server allows it
        (CORS), which this one never does."""
        path = urlsplit(self.path).path
        try:
            if self.headers.get("Content-Length", "0") not in ("", "0"):
                self._read_body()
            if not self._authorized():
                return self._text(HTTPStatus.UNAUTHORIZED, "login required",
                                  {"WWW-Authenticate": 'Basic realm="lorawan-capture"'})
            device = OWN_API.match(path)
            if not device:
                return self._text(HTTPStatus.NOT_FOUND, "not found")
            target = unquote(device.group(1))
            try:
                try:
                    key = {"dev_eui": normalize_eui(target)}
                except ValueError:
                    key = {"dev_addr": normalize(target)}
                removed = self.capture.own_devices.remove(target)
            except ValueError as err:
                return self._text(HTTPStatus.BAD_REQUEST, str(err))
            if not removed:
                return self._text(HTTPStatus.NOT_FOUND, "not an own device")
            self._json({**key, "removed": True})
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception:  # noqa: BLE001
            log.exception("error serving DELETE %s", path)
            try:
                self._text(HTTPStatus.INTERNAL_SERVER_ERROR, "internal error, see the container log")
            except OSError:
                pass

    def _status(self) -> dict:
        usage = shutil.disk_usage(self.cfg.data_dir)
        state = health.check(self.capture, self.cfg)
        return {
            **self.capture.status(),
            "health": {key: state[key] for key in ("ok", "problems", "warnings")},
            "version": __version__,
            "disk": {"total": usage.total, "free": usage.free},
            "config": {"tz": str(self.cfg.tz), "own": sorted(self.capture.own()),
                       "keep_days": self.cfg.keep_days, "topics": list(self.cfg.topics)},
        }

    def _days(self) -> list[dict]:
        out = []
        for day, path in store.list_days(self.cfg.data_dir):
            try:
                size = path.stat().st_size
            except OSError:
                continue
            try:
                summary, error = self.capture.day_summary(day, path, compute=False), None  # the sweep makes missing ones
            except sqlite3.Error as err:  # one unreadable file must not hide the others
                summary, error = None, str(err)
            out.append({"day": day.isoformat(), "file": path.name, "size": size, "today": day == self.capture.day,
                        "brief": store.brief(summary), "error": error})
        return out

    def _day_summary(self, text: str) -> None:
        day = _day(text)
        path = store.day_file(self.cfg.data_dir, day) if day else None
        if day is None or not path.exists():
            return self._text(HTTPStatus.NOT_FOUND, "no such day")
        self._json(self.capture.day_summary(day, path))

    # ------------------------------------------------------------ explorer
    def _range(self, query: str) -> tuple[date, date] | None:
        """from/to of the query string; None after answering 400 if they are missing or wrong."""
        params = parse_qs(query)
        first, last = (_day((params.get(key) or [""])[0]) for key in ("from", "to"))
        if first is None or last is None or first > last:
            self._text(HTTPStatus.BAD_REQUEST, "use ?from=YYYY-MM-DD&to=YYYY-MM-DD, from not after to")
            return None
        return first, last

    @staticmethod
    def _capped(first: date, last: date) -> tuple[date, dict]:
        """At most the last MAX_SPAN_DAYS days of a range, and what to tell the page if shortened."""
        if (last - first).days < MAX_SPAN_DAYS:
            return first, {}
        return last - timedelta(days=MAX_SPAN_DAYS - 1), {"clamped": True, "asked_from": first.isoformat(),
                                                          "max_days": MAX_SPAN_DAYS}

    def _explore(self, what: str, query: str) -> None:
        span = self._range(query)
        if span is None:
            return
        source = explorer.Source(self.capture, self.cfg.data_dir)
        if what == "devices":
            first, note = self._capped(*span)
            return self._json({**explorer.devices_report(source, first, span[1], self.capture.own()), **note})
        if what == "joins":
            return self._json(explorer.joins_report(source, *span))
        self._json(explorer.other_report(source, *span, self.cfg.tz, self.capture.clock()))

    def _device(self, dev: str, query: str) -> None:
        span = self._range(query)
        if span is None:
            return
        first, note = self._capped(*span)
        report = explorer.device_report(explorer.Source(self.capture, self.cfg.data_dir), dev, first, span[1],
                                        self.cfg.tz, self.capture.own(), self.capture.clock())
        if report is None:
            return self._text(HTTPStatus.NOT_FOUND, "nothing from or to this device in that range")
        self._json({**report, **note})

    def _gateway_columns(self) -> list[tuple[str, str]]:
        """(id, CSV column label) per known gateway, by name; the label is the name made safe."""
        out, used = [], set()
        for gid, entry in sorted(self.capture.registry.snapshot().items(),
                                 key=lambda kv: ((kv[1].get("name") or kv[0]).lower(), kv[0])):
            label = re.sub(r"[^0-9A-Za-z]+", "_", entry.get("name") or gid).strip("_") or gid
            if label in used:
                label = f"{label}_{gid[-4:]}"
            used.add(label)
            out.append((gid, label))
        return out

    def _device_csv(self, dev: str, query: str) -> None:
        """Every uplink of a device in the range as CSV, built while it is sent (no length limit)."""
        span = self._range(query)
        if span is None:
            return
        excel = (parse_qs(query).get("excel") or ["0"])[0] == "1"
        name = self.capture.own_devices.snapshot().get(dev, {}).get("name") or dev
        filename = f"lorawan_{re.sub(r'[^0-9A-Za-z_-]+', '_', name)}_{span[0]}_to_{span[1]}.csv"
        chunks = explorer.uplinks_csv(explorer.Source(self.capture, self.cfg.data_dir), dev, *span, self.cfg.tz,
                                      self._gateway_columns(), excel)
        self.send_response(HTTPStatus.OK)  # streamed: no Content-Length, HTTP/1.0 ends the body by closing
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            for chunk in chunks:
                self.wfile.write(chunk.encode("utf-8"))
        except (BrokenPipeError, ConnectionResetError):
            raise
        except Exception:  # noqa: BLE001 - headers are out: cut the file off, log why
            self.close_connection = True
            log.exception("CSV export of %s failed", dev)

    def _frame(self, text: str, key: str) -> None:
        day = _day(text)
        report = explorer.frame_report(explorer.Source(self.capture, self.cfg.data_dir), day, key) if day else None
        if report is None:
            return self._text(HTTPStatus.NOT_FOUND, "no such packet on that day")
        self._json(report)

    def _download(self, text: str, gz: bool) -> None:
        day = _day(text)
        path = store.day_file(self.cfg.data_dir, day) if day else None
        if day is None or not path.exists():
            return self._text(HTTPStatus.NOT_FOUND, "no such day")
        name, temp = path.name, None
        if day == self.capture.day:  # still being written: send a consistent snapshot
            temp, name = self._snapshot(day)
            path = temp
        try:
            with path.open("rb") as src:
                self.send_response(HTTPStatus.OK)
                if gz:  # streamed, so no Content-Length; HTTP/1.0 ends the body by closing
                    self.send_header("Content-Type", "application/gzip")
                    self.send_header("Content-Disposition", f'attachment; filename="{name}.gz"')
                    self.end_headers()
                    with gzip.GzipFile(filename=name, mode="wb", fileobj=self.wfile,
                                       compresslevel=COMPRESS_LEVEL) as out:
                        shutil.copyfileobj(src, out, CHUNK)
                else:
                    self.send_header("Content-Type", "application/vnd.sqlite3")
                    self.send_header("Content-Length", str(path.stat().st_size))
                    self.send_header("Content-Disposition", f'attachment; filename="{name}"')
                    self.end_headers()
                    shutil.copyfileobj(src, self.wfile, CHUNK)
        finally:
            if temp is not None:
                temp.unlink(missing_ok=True)

    def _snapshot(self, day: date) -> tuple[Path, str]:
        """A consistent copy of today's file (deleted by the caller) and the name to give it."""
        snapshots = self.cfg.data_dir / ".snapshots"
        snapshots.mkdir(exist_ok=True)
        temp = snapshots / f"{uuid.uuid4().hex}.db"
        self.capture.snapshot(temp)
        return temp, f"frames_{day.isoformat()}_partial-{datetime.now(self.cfg.tz):%H%M}.db"

    def _download_range(self, query: str) -> None:
        """All daily files from..to (inclusive) as one zip, built while it is sent: no temporary
        archive, so any range fits. Today's file goes in as a snapshot, the names as gateways.json."""
        span = self._range(query)
        if span is None:
            return
        first, last = span
        days = [(day, path) for day, path in sorted(store.list_days(self.cfg.data_dir)) if first <= day <= last]
        if not days:
            return self._text(HTTPStatus.NOT_FOUND, "no daily files in that range")
        temp = None
        try:
            files = []  # (file to send, name in the zip, day)
            for day, path in days:
                if day == self.capture.day:  # still being written: a consistent snapshot
                    temp, name = self._snapshot(day)
                    files.append((temp, name, day))
                else:
                    files.append((path, path.name, day))
            readme = self._readme(files, first, last)
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Disposition", f'attachment; filename="lorawan_{first}_to_{last}.zip"')
            self.end_headers()
            self._stream_zip(readme, files)
        finally:
            if temp is not None:
                temp.unlink(missing_ok=True)

    def _stream_zip(self, readme: str, files: list[tuple[Path, str, date]]) -> None:
        sink = _Sink(self.wfile)
        archive = zipfile.ZipFile(sink, "w", zipfile.ZIP_DEFLATED, compresslevel=COMPRESS_LEVEL)
        try:
            archive.writestr("README.md", readme)
            for path, name, _ in files:
                archive.write(path, arcname=name)
            for extra in ("gateways.json", "devices.json"):
                if (self.cfg.data_dir / extra).exists():
                    archive.write(self.cfg.data_dir / extra, arcname=extra)
            archive.close()
        except (BrokenPipeError, ConnectionResetError):
            sink.dead = True
            archive.close()
            raise
        except Exception:  # noqa: BLE001 - headers are out: leave the zip broken, log why
            sink.dead = True
            archive.close()
            self.close_connection = True
            log.exception("bulk download failed")

    def _readme(self, files: list[tuple[Path, str, date]] | None = None,
                first: date | None = None, last: date | None = None) -> str:
        """README.md for the downloads: what this export holds, the data guide, the site notes."""
        lines = ["# LoRaWAN capture data", ""]
        if files is None:
            lines += [f"lorawan-capture {__version__}. One file per local day ({self.cfg.tz}); "
                      "the timestamps inside are UTC.", ""]
        else:
            lines += [f"Exported {datetime.now(self.cfg.tz):%Y-%m-%d %H:%M} ({self.cfg.tz}) by lorawan-capture "
                      f"{__version__}: days {first} to {last}. Each file holds one local day ({self.cfg.tz}); "
                      "the timestamps inside are UTC.", "",
                      "| File | Bytes | Frames | Stats reports |", "|---|---|---|---|"]
            for path, name, day in files:
                frames, reports = self._counts(path, day)
                lines.append(f"| `{name}` | {path.stat().st_size:,} | {frames:,} | {reports:,} |")
            lines.append("")
        registry = self.capture.registry.snapshot()
        if registry:
            lines.append("Gateways (names from `gateways.json`):")
            lines += [f"- `{gid}`: {entry.get('name') or '(no name)'}, first seen {entry.get('first_seen') or '?'}"
                      for gid, entry in sorted(registry.items())]
            lines.append("")
        devices = self.capture.own_devices.snapshot()
        own = ", ".join(f"`{addr}`" + (f" ({entry['name']})" if entry.get("name") else "")
                        for addr, entry in devices.items()) or "none"
        lines += [f"Own devices now (names in `devices.json`): {own}. The `is_own` column says whether a "
                  "device was an own device when its row was recorded.", ""]
        text = "\n".join(lines) + "\n" + (STATIC / "DATA_README.md").read_text(encoding="utf-8")
        try:
            notes = (self.cfg.data_dir / "NOTES.md").read_text(encoding="utf-8").strip()
        except OSError:
            notes = ""
        return text + ("\n\n---\n\n" + notes + "\n" if notes else "")

    def _counts(self, path: Path, day: date) -> tuple[int, int]:
        """Frames and stats reports in a daily file, from its cached summary when there is one."""
        summary = store.cached_summary(path) if day != self.capture.day else None
        if summary is not None:
            reports = sum((g.get("stats") or {}).get("reports", 0) for g in summary["gateways"].values())
            return summary["rows"], reports
        conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
        try:
            return (conn.execute("SELECT count(*) FROM frames").fetchone()[0],
                    conn.execute("SELECT count(*) FROM stats").fetchone()[0])
        finally:
            conn.close()


def make_server(capture: Capture, cfg: Config, host: str = "") -> ThreadingHTTPServer:
    handler = type("BoundHandler", (Handler,), {"capture": capture, "cfg": cfg})
    server = ThreadingHTTPServer((host, cfg.web_port), handler)
    server.daemon_threads = True
    return server
