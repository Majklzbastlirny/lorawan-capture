"""Every gateway ever seen, with the name given to it in the UI (data/gateways.json)."""

from __future__ import annotations

import logging
import re
import threading
import time
from pathlib import Path

from . import store

log = logging.getLogger(__name__)

GATEWAY_ID = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
MAX_NAME = 40


class Registry:
    """{gateway id: {"name", "first_seen", "last_seen"}}.

    A new gateway is added (and the file saved) the moment its first message arrives;
    last_seen is saved every few minutes. Names set in the UI win; GATEWAY_NAMES only fills
    in names that are missing.
    """

    def __init__(self, path: Path, seed_names: dict[str, str] | None = None) -> None:
        self.path = path
        self.lock = threading.Lock()
        loaded = store.read_json(path)
        self.data: dict[str, dict] = loaded if isinstance(loaded, dict) else {}
        for gateway_id, name in (seed_names or {}).items():
            entry = self._entry(gateway_id)
            if not entry["name"]:
                entry["name"] = name[:MAX_NAME]
        self.dirty = bool(seed_names)
        self.saved_at = 0.0

    def _entry(self, gateway_id: str) -> dict:
        entry = self.data.setdefault(gateway_id, {})
        for key in ("name", "first_seen", "last_seen"):
            entry.setdefault(key, None)
        return entry

    def seen(self, gateway_id: str, when: str, active: bool = True) -> bool:
        """Note a message from this gateway; True the first time it is ever seen. Only `active`
        messages (uplinks, stats) move last_seen: the broker repeats a gateway's stored
        online/offline state to every new subscriber, long after the gateway went quiet."""
        with self.lock:
            entry = self._entry(gateway_id)
            new = entry["first_seen"] is None
            if new:
                entry["first_seen"] = when
            if active:
                entry["last_seen"] = when
                self.dirty = True
        if new:
            self.save()
        return new

    def rename(self, gateway_id: str, name: str | None) -> dict:
        """Set (or with an empty name, clear) a gateway's name. Raises ValueError if invalid."""
        if not GATEWAY_ID.match(gateway_id):
            raise ValueError("invalid gateway id")
        name = (name or "").strip()
        if len(name) > MAX_NAME or any(ord(ch) < 32 for ch in name):
            raise ValueError(f"names are up to {MAX_NAME} characters, without control characters")
        with self.lock:
            entry = self._entry(gateway_id)
            entry["name"] = name or None
            self.dirty = True
            result = {"id": gateway_id, **entry}
        self.save()
        log.info("gateway %s renamed to %r", gateway_id, name or None)
        return result

    def snapshot(self) -> dict[str, dict]:
        with self.lock:
            return {gateway_id: dict(entry) for gateway_id, entry in self.data.items()}

    def save(self) -> None:
        with self.lock:
            data = {gateway_id: dict(entry) for gateway_id, entry in self.data.items()}
            self.dirty = False
            self.saved_at = time.monotonic()
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            store.write_json(self.path, data)
        except OSError as err:
            log.warning("cannot save %s: %s", self.path.name, err)

    def maybe_save(self, every: float = 300.0) -> None:
        if self.dirty and time.monotonic() - self.saved_at >= every:
            self.save()
