"""The own devices, added, renamed and removed in the UI (data/devices.json)."""

from __future__ import annotations

import logging
import re
import threading
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from . import store

log = logging.getLogger(__name__)

DEV_ADDR = re.compile(r"^[0-9a-f]{8}$")
DEV_EUI = re.compile(r"^[0-9a-f]{16}$")
MAX_NAME = 40


def _clean(text: str) -> str:
    return re.sub(r"[\s:.-]", "", str(text)).lower().removeprefix("0x")


def normalize(text: str) -> str:
    """'00a1b2c3', '0x00a1b2c3', '00 a1 b2 c3' or '00:a1:b2:c3' -> '00a1b2c3'. Raises ValueError."""
    addr = _clean(text)
    if not DEV_ADDR.match(addr):
        raise ValueError("a DevAddr is 8 hex digits, e.g. 00a1b2c3")
    return addr


def normalize_eui(text: str) -> str:
    """A DevEUI as 16 lowercase hex digits (separators and 0x allowed). Raises ValueError."""
    eui = _clean(text)
    if not DEV_EUI.match(eui):
        raise ValueError("a DevEUI is 16 hex digits, e.g. 70b3d57ed0000001")
    return eui


class OwnDevices:
    """{"devices": {devaddr: {"name", "added", "dev_eui"?, "until"?}},
        "follow": {deveui: {"name", "added"}}, "imported": [devaddrs taken over from OWN_DEVADDRS]}.

    The gateways only see a device's DevAddr, and an OTAA device gets a new one at every join.
    A device with a DevEUI is followed: when it joins again, its new address is added with
    the same name and DevEUI, and its earlier ones get an `until` (they stay own devices, so
    their history keeps counting). Every address in OWN_DEVADDRS is taken over once, so a
    device removed in the UI stays removed after a restart, while an address added to .env
    later is still picked up. `addrs` and `followed` are replaced, never changed, so the
    writer thread can read them without the lock.
    """

    def __init__(self, path: Path, seed: frozenset[str] = frozenset(),
                 clock: Callable[[], datetime] | None = None) -> None:
        self.path = path
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.lock = threading.Lock()
        loaded = store.read_json(path)
        loaded = loaded if isinstance(loaded, dict) else {}
        devices = loaded.get("devices") if isinstance(loaded.get("devices"), dict) else {}
        self.devices: dict[str, dict] = {}
        for addr, entry in devices.items():
            if DEV_ADDR.match(addr) and isinstance(entry, dict):
                kept = {"name": entry.get("name"), "added": entry.get("added")}
                if isinstance(entry.get("dev_eui"), str) and DEV_EUI.match(entry["dev_eui"]):
                    kept["dev_eui"] = entry["dev_eui"]
                    if isinstance(entry.get("until"), str):
                        kept["until"] = entry["until"]
                self.devices[addr] = kept
        follow = loaded.get("follow") if isinstance(loaded.get("follow"), dict) else {}
        self.follow: dict[str, dict] = {
            eui: {"name": entry.get("name"), "added": entry.get("added")}
            for eui, entry in follow.items() if DEV_EUI.match(eui) and isinstance(entry, dict)}
        self.imported = {addr for addr in loaded.get("imported") or [] if isinstance(addr, str)}
        new = sorted(set(seed) - self.imported)
        for addr in new:
            self.devices.setdefault(addr, {"name": None, "added": self._now()})
            self.imported.add(addr)
            log.info("own device %s added from OWN_DEVADDRS", addr)
        self._publish()
        if new:
            self.save()

    def _publish(self) -> None:  # with the lock held (or before anyone else can see the object)
        self.addrs = frozenset(self.devices)
        self.followed = frozenset(self.follow)

    def snapshot(self) -> dict[str, dict]:
        with self.lock:
            return {addr: dict(entry) for addr, entry in sorted(self.devices.items())}

    def following(self) -> dict[str, dict]:
        """Followed DevEUIs with their name and current address (None until one is known)."""
        with self.lock:
            return {eui: {**entry, "dev_addr": self._current(eui)} for eui, entry in sorted(self.follow.items())}

    def current(self) -> dict[str, dict]:
        """The addresses in use: all but those a followed device has left behind."""
        with self.lock:
            return {addr: dict(entry) for addr, entry in sorted(self.devices.items()) if "until" not in entry}

    def _current(self, eui: str) -> str | None:
        mine = [(entry.get("added") or "", addr) for addr, entry in self.devices.items()
                if entry.get("dev_eui") == eui and "until" not in entry]
        return max(mine)[1] if mine else None

    @staticmethod
    def _name(name: str | None) -> str | None:
        name = (name or "").strip()
        if len(name) > MAX_NAME or any(ord(ch) < 32 for ch in name):
            raise ValueError(f"names are up to {MAX_NAME} characters, without control characters")
        return name or None

    def put(self, text: str, name: str | None, dev_eui: str | None = None) -> dict:
        """Add a device, or rename it if it is already there. `dev_eui` links the address to a
        device to follow ("" unlinks it, None leaves it as it is); the name then goes to all
        addresses of that device. Raises ValueError if invalid."""
        addr = normalize(text)
        name = self._name(name)
        eui = normalize_eui(dev_eui) if dev_eui else dev_eui
        with self.lock:
            entry = self.devices.get(addr)
            added = entry is None
            if added:
                entry = self.devices[addr] = {"name": None, "added": self._now()}
            unlinked = entry.get("dev_eui") if eui == "" else None
            if eui == "":
                entry.pop("dev_eui", None)
                entry.pop("until", None)
                if not any(other.get("dev_eui") == unlinked for other in self.devices.values()):
                    self.follow.pop(unlinked, None)  # its last address: no longer followed
            elif eui:
                entry["dev_eui"] = eui
            linked = entry.get("dev_eui")
            if linked:
                self.follow.setdefault(linked, {"name": name, "added": self._now()})["name"] = name
                for other in self.devices.values():
                    if other.get("dev_eui") == linked:
                        other["name"] = name
            entry["name"] = name
            self._publish()
            result = {"dev_addr": addr, **entry}
        self.save()
        log.info("own device %s %s (%r%s)", addr, "added" if added else "changed", name,
                 f", follows {linked}" if linked else "")
        return result

    def follow_device(self, dev_eui: str, name: str | None, addresses: list[tuple[str, str]]) -> dict:
        """Follow a device by its DevEUI. `addresses`: (time first heard, DevAddr) of the sessions
        found in the recordings, in time order; the last one is the current address. An address
        of the device that is not among them was left behind by the first session after it."""
        eui = normalize_eui(dev_eui)
        name = self._name(name)
        with self.lock:
            entry = self.follow.setdefault(eui, {"name": name, "added": self._now()})
            entry["name"] = name = name or entry["name"]
            found = {addr for _, addr in addresses}
            for addr, device in self.devices.items():
                if device.get("dev_eui") == eui and addr not in found and "until" not in device:
                    later = [first for first, _ in addresses if first > (device.get("added") or "")]
                    if later:
                        device["until"] = later[0]
            for i, (first, addr) in enumerate(addresses):
                device = self.devices.setdefault(addr, {"name": name, "added": first})
                device.update(name=name, dev_eui=eui)
                if i + 1 < len(addresses):
                    device["until"] = addresses[i + 1][0]
                else:
                    device.pop("until", None)
            for addr, device in self.devices.items():  # earlier links of this device keep their place
                if device.get("dev_eui") == eui:
                    device["name"] = name
            self._publish()
            current = self._current(eui)
        self.save()
        log.info("following %s (%r): %s", eui, name, ", ".join(a for _, a in addresses) or "no address yet")
        return {"dev_eui": eui, "name": name, "dev_addr": current, "addresses": [a for _, a in addresses]}

    def adopt(self, dev_eui: str, addr: str, when: str) -> str | None:
        """A followed device joined again and sends from `addr` now (the writer thread)."""
        with self.lock:
            follow = self.follow.get(dev_eui)
            if follow is None:
                return None
            for device in self.devices.values():
                if device.get("dev_eui") == dev_eui and "until" not in device:
                    device["until"] = when
            self.devices[addr] = {"name": follow["name"], "added": when, "dev_eui": dev_eui}
            self._publish()
            name = follow["name"]
        self.save()
        log.info("own device %s (%s) joined again: now %s", name or dev_eui, dev_eui, addr)
        return name

    def _now(self) -> str:
        return self.clock().astimezone(timezone.utc).isoformat(timespec="seconds")

    def remove(self, text: str) -> bool:
        """Remove a device by DevAddr or DevEUI; a followed device goes with all its addresses."""
        cleaned = _clean(text)
        with self.lock:
            if DEV_EUI.match(cleaned):
                eui = cleaned
            else:
                entry = self.devices.get(normalize(text))
                if entry is None:
                    return False
                eui = entry.get("dev_eui")
                if eui is None:
                    del self.devices[normalize(text)]
            if eui is not None:
                gone = [addr for addr, entry in self.devices.items() if entry.get("dev_eui") == eui]
                if not gone and eui not in self.follow:
                    return False
                for addr in gone:
                    del self.devices[addr]
                self.follow.pop(eui, None)
            self._publish()
        self.save()
        log.info("own device %s removed", text)
        return True

    def save(self) -> None:
        with self.lock:
            data = {"devices": {addr: dict(entry) for addr, entry in sorted(self.devices.items())},
                    "follow": {eui: dict(entry) for eui, entry in sorted(self.follow.items())},
                    "imported": sorted(self.imported)}
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            store.write_json(self.path, data)
        except OSError as err:
            log.warning("cannot save %s: %s", self.path.name, err)

