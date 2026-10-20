"""The own devices, added, renamed and removed in the UI (data/devices.json)."""

from __future__ import annotations

import logging
import re
import threading
from datetime import datetime, timezone
from pathlib import Path

from . import store

log = logging.getLogger(__name__)

DEV_ADDR = re.compile(r"^[0-9a-f]{8}$")
MAX_NAME = 40


def normalize(text: str) -> str:
    """'00a1b2c3', '0x00a1b2c3', '00 a1 b2 c3' or '00:a1:b2:c3' -> '00a1b2c3'. Raises ValueError."""
    addr = re.sub(r"[\s:.-]", "", str(text)).lower().removeprefix("0x")
    if not DEV_ADDR.match(addr):
        raise ValueError("a DevAddr is 8 hex digits, e.g. 00a1b2c3")
    return addr


class OwnDevices:
    """{"devices": {devaddr: {"name", "added"}}, "imported": [devaddrs taken over from OWN_DEVADDRS]}.

    Every address in OWN_DEVADDRS is taken over once. So a device removed in the UI stays
    removed after a restart, while an address added to .env later is still picked up.
    `addrs` is replaced, never changed, so the writer thread can read it without a lock.
    """

    def __init__(self, path: Path, seed: frozenset[str] = frozenset()) -> None:
        self.path = path
        self.lock = threading.Lock()
        loaded = store.read_json(path)
        loaded = loaded if isinstance(loaded, dict) else {}
        devices = loaded.get("devices") if isinstance(loaded.get("devices"), dict) else {}
        self.devices: dict[str, dict] = {
            addr: {"name": entry.get("name"), "added": entry.get("added")}
            for addr, entry in devices.items() if DEV_ADDR.match(addr) and isinstance(entry, dict)}
        self.imported = {addr for addr in loaded.get("imported") or [] if isinstance(addr, str)}
        new = sorted(set(seed) - self.imported)
        for addr in new:
            self.devices.setdefault(addr, {"name": None, "added": _now()})
            self.imported.add(addr)
            log.info("own device %s added from OWN_DEVADDRS", addr)
        self.addrs = frozenset(self.devices)
        if new:
            self.save()

    def snapshot(self) -> dict[str, dict]:
        with self.lock:
            return {addr: dict(entry) for addr, entry in sorted(self.devices.items())}

    def put(self, text: str, name: str | None) -> dict:
        """Add a device, or rename it if it is already there. Raises ValueError if invalid."""
        addr = normalize(text)
        name = (name or "").strip()
        if len(name) > MAX_NAME or any(ord(ch) < 32 for ch in name):
            raise ValueError(f"names are up to {MAX_NAME} characters, without control characters")
        with self.lock:
            entry = self.devices.get(addr)
            added = entry is None
            if added:
                entry = self.devices[addr] = {"name": None, "added": _now()}
            entry["name"] = name or None
            self.addrs = frozenset(self.devices)
            result = {"dev_addr": addr, **entry}
        self.save()
        log.info("own device %s %s (%r)", addr, "added" if added else "renamed", name or None)
        return result

    def remove(self, text: str) -> bool:
        addr = normalize(text)
        with self.lock:
            if self.devices.pop(addr, None) is None:
                return False
            self.addrs = frozenset(self.devices)
        self.save()
        log.info("own device %s removed", addr)
        return True

    def save(self) -> None:
        with self.lock:
            data = {"devices": {addr: dict(entry) for addr, entry in sorted(self.devices.items())},
                    "imported": sorted(self.imported)}
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            store.write_json(self.path, data)
        except OSError as err:
            log.warning("cannot save %s: %s", self.path.name, err)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
