"""Shared fixtures. The payloads are real gw.* messages made with chirpstack-api (copied from
the Home Assistant integration's tests/ha/payloads.json)."""

import base64
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "app"))

from lorawan_capture.capture import Capture  # noqa: E402
from lorawan_capture.config import Config  # noqa: E402

GATEWAYS = {"sensecap": "0016c001f1000001", "home": "0016c001f1000002", "lab": "b827ebfffe000003"}
UTC = timezone.utc


class Clock:
    def __init__(self, start: datetime) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs) -> None:
        self.now += timedelta(**kwargs)


def topic(gateway: str, kind: str = "event/up") -> str:
    return f"eu868/gateway/{GATEWAYS[gateway]}/{kind}"


def feed(cap: Capture, mqtt_topic: str, payload: bytes) -> None:
    """Process one message exactly as the writer thread does, synchronously."""
    with cap.lock:
        cap._handle(cap.clock(), mqtt_topic, payload)
        cap._commit()


def frame(dev: str, fcnt: int, fopts: str = "", port: int | None = 1, mhdr: int = 0x40, fctrl: int = 0x80) -> str:
    """A LoRaWAN data frame (hex): DevAddr and FCnt little-endian, FOpts length in FCtrl."""
    body = f"{port:02x}aabbccdd" if port is not None else ""
    return (f"{mhdr:02x}{bytes.fromhex(dev)[::-1].hex()}{fctrl | len(fopts) // 2:02x}"
            f"{fcnt.to_bytes(2, 'little').hex()}{fopts}{body}11223344")


def up(phy_hex: str, freq: int = 868100000, sf: int = 7, rssi: int = -100, snr: float = 5.0) -> bytes:
    return json.dumps({
        "phyPayload": base64.b64encode(bytes.fromhex(phy_hex)).decode(),
        "txInfo": {"frequency": freq, "modulation": {"lora": {"bandwidth": 125000, "spreadingFactor": sf,
                                                              "codeRate": "CR_4_5"}}},
        "rxInfo": {"rssi": rssi, "snr": snr, "crcStatus": "CRC_OK"}}).encode()


def make_config(data_dir: Path, **env: str) -> Config:
    return Config.from_env({"MQTT_HOST": "localhost", "DATA_DIR": str(data_dir), "TZ": "Europe/Prague",
                            "OWN_DEVADDRS": "00a1b2c3", "WEB_PORT": "0", **env})


@pytest.fixture
def payloads() -> dict[str, bytes]:
    data = json.loads((ROOT / "tests" / "payloads.json").read_text(encoding="utf-8"))
    return {name: bytes.fromhex(value) for name, value in data.items()}


@pytest.fixture
def make_capture(tmp_path):
    made = []

    def factory(clock=None, opened=True, **env) -> Capture:
        cap = Capture(make_config(tmp_path / "data", **env), clock=clock)
        cap.cfg.data_dir.mkdir(parents=True, exist_ok=True)
        cap._sweep_async = cap._sweep  # deterministic: no background thread in tests
        if opened:
            with cap.lock:
                cap._open(cap.today())
        made.append(cap)
        return cap

    yield factory
    for cap in made:
        cap.stop()
        with cap.lock:
            cap._close()
