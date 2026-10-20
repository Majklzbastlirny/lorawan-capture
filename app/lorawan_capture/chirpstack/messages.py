"""Normalise ChirpStack gateway MQTT payloads into small typed records.

The gateway bridge / MQTT forwarder can be configured with the protobuf
(default) or the JSON marshaler; both are accepted. JSON follows the proto3
JSON mapping (camelCase names, enums as strings, bytes as base64).
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from . import protobuf as pb

CRC_STATUS = {0: "NO_CRC", 1: "BAD_CRC", 2: "CRC_OK"}
CONN_STATE = {0: "OFFLINE", 1: "ONLINE"}
TX_ACK_STATUS = {
    0: "IGNORED",
    1: "OK",
    2: "TOO_LATE",
    3: "TOO_EARLY",
    4: "COLLISION_PACKET",
    5: "COLLISION_BEACON",
    6: "TX_FREQ",
    7: "TX_POWER",
    8: "GPS_UNLOCKED",
    9: "QUEUE_FULL",
    10: "INTERNAL_ERROR",
    11: "DUTY_CYCLE_OVERFLOW",
}
# CodeRate enum (and legacy strings) -> LoRa CR index (1 = 4/5 ... 4 = 4/8).
# Long-interleaving (2.4 GHz) rates are approximated by their nearest 4/x rate.
_CODE_RATE = {1: 1, 2: 2, 3: 3, 4: 4, 10: 1, 11: 2, 12: 4}
_CODE_RATE_NAMES = {
    "CR_4_5": 1,
    "CR_4_6": 2,
    "CR_4_7": 3,
    "CR_4_8": 4,
    "CR_LI_4_5": 1,
    "CR_LI_4_6": 2,
    "CR_LI_4_8": 4,
    "4/5": 1,
    "4/6": 2,
    "4/7": 3,
    "4/8": 4,
}

LORA = "LORA"
FSK = "FSK"
LR_FHSS = "LR_FHSS"


@dataclass(slots=True)
class Modulation:
    """Modulation parameters of a received or transmitted packet."""

    kind: str | None = None
    spreading_factor: int | None = None
    bandwidth: int | None = None  # Hz
    code_rate: int | None = None  # 1 (4/5) .. 4 (4/8)
    preamble: int | None = None
    datarate: int | None = None  # FSK bit/s

    @property
    def key(self) -> str:
        """Stable identifier used for per-modulation counters."""
        if self.kind == LORA:
            if self.bandwidth in (None, 0, 125000):
                return f"sf{self.spreading_factor}"
            return f"sf{self.spreading_factor}_bw{self.bandwidth // 1000}"
        if self.kind == FSK:
            return "fsk"
        if self.kind == LR_FHSS:
            return "lr_fhss"
        return "unknown"


def modulation_label(key: str) -> str:
    """Human readable label for a Modulation.key."""
    if key.startswith("sf"):
        sf, _, bw = key[2:].partition("_bw")
        return f"SF{sf} {bw} kHz" if bw else f"SF{sf}"
    return {"fsk": "FSK", "lr_fhss": "LR-FHSS"}.get(key, key)


@dataclass(slots=True)
class Uplink:
    """One packet as reported by one gateway (event/up)."""

    gateway_id: str
    phy_payload: bytes
    frequency: int
    modulation: Modulation
    rssi: int | None
    snr: float | None
    crc_status: str
    channel: int | None = None
    rf_chain: int | None = None
    gw_time: float | None = None


@dataclass(slots=True)
class DutyCycleBand:
    """Duty-cycle usage of one regulatory sub-band as tracked by the gateway."""

    name: str
    frequency_min: int
    frequency_max: int
    load_max: float  # seconds allowed in the window
    load_tracked: float  # seconds used in the window

    @property
    def usage_percent(self) -> float | None:
        if self.load_max <= 0:
            return None
        return 100.0 * self.load_tracked / self.load_max


@dataclass(slots=True)
class GatewayStats:
    """Periodic gateway statistics (event/stats)."""

    gateway_id: str
    time: float | None
    rx_received: int
    rx_received_ok: int
    tx_received: int
    tx_emitted: int
    config_version: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)
    location: dict[str, float] | None = None
    rx_per_frequency: dict[int, int] = field(default_factory=dict)
    tx_per_frequency: dict[int, int] = field(default_factory=dict)
    rx_per_modulation: dict[str, int] = field(default_factory=dict)
    tx_per_modulation: dict[str, int] = field(default_factory=dict)
    tx_per_status: dict[str, int] = field(default_factory=dict)
    duty_cycle_window: float | None = None
    duty_cycle_bands: list[DutyCycleBand] | None = None


@dataclass(slots=True)
class DownlinkItem:
    """One transmission option of a downlink (e.g. RX1 or RX2)."""

    frequency: int
    power: int | None
    modulation: Modulation
    phy_length: int


@dataclass(slots=True)
class Downlink:
    """Downlink scheduled by ChirpStack for a gateway (command/down)."""

    gateway_id: str
    downlink_id: int
    items: list[DownlinkItem]


@dataclass(slots=True)
class TxAck:
    """Gateway acknowledgement of a downlink (event/ack), one status per item."""

    gateway_id: str
    downlink_id: int
    statuses: list[str]


@dataclass(slots=True)
class ConnState:
    """Gateway connection state (state/conn, retained)."""

    gateway_id: str
    online: bool


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _camel(name: str) -> str:
    first, *rest = name.split("_")
    return first + "".join(part[:1].upper() + part[1:] for part in rest)


def _g(data: Any, name: str) -> Any:
    """Get a field by its proto name from a protobuf- or JSON-decoded dict."""
    if not isinstance(data, dict):
        return None
    value = data.get(name)
    if value is None:
        value = data.get(_camel(name))
    return value


def _int(value: Any, default: int | None = 0) -> int | None:
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str):
        try:
            return base64.b64decode(value)
        except (binascii.Error, ValueError):
            return b""
    return b""


def _enum(value: Any, table: dict[int, str]) -> str:
    if value is None:
        return table.get(0, "UNKNOWN")
    if isinstance(value, str):
        return value
    return table.get(int(value), str(value))


def _timestamp(value: Any) -> float | None:
    if isinstance(value, dict):
        seconds = _int(_g(value, "seconds"), 0) or 0
        nanos = _int(_g(value, "nanos"), 0) or 0
        if not seconds and not nanos:
            return None
        return seconds + nanos / 1e9
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    return None


def _duration(value: Any) -> float:
    if isinstance(value, dict):
        return (_int(_g(value, "seconds"), 0) or 0) + (_int(_g(value, "nanos"), 0) or 0) / 1e9
    if isinstance(value, str):
        try:
            return float(value.rstrip("s"))
        except ValueError:
            return 0.0
    return 0.0


def is_json(payload: bytes) -> bool:
    """True for JSON-marshaled payloads.

    A protobuf message never starts with '{' (field 15 with the group wire
    type), but it can start with b"\\n{": field 1 holding 123 bytes. That is
    also JSON whitespace followed by '{', so that one case is test-parsed.
    """
    if payload[:1] == b"{":
        return True
    if payload.lstrip()[:1] != b"{":
        return False
    if payload[:1] != b"\n":
        return True
    try:
        json.loads(payload)
    except ValueError:
        return False
    return True


def _load(payload: bytes, schema: pb.Schema) -> dict[str, Any]:
    if is_json(payload):
        try:
            data = json.loads(payload)
        except ValueError as err:
            raise pb.DecodeError(f"invalid JSON: {err}") from err
        if not isinstance(data, dict):
            raise pb.DecodeError("JSON payload is not an object")
        return data
    return pb.decode(payload, schema)


def _code_rate(data: dict[str, Any]) -> int | None:
    value = _g(data, "code_rate")
    if isinstance(value, str):
        rate = _CODE_RATE_NAMES.get(value)
    elif value:
        rate = _CODE_RATE.get(int(value))
    else:
        rate = None
    if rate is None:
        rate = _CODE_RATE_NAMES.get(_g(data, "code_rate_legacy") or "")
    return rate


def parse_modulation(data: Any) -> Modulation:
    """Parse a gw.Modulation message."""
    lora = _g(data, "lora")
    if lora is not None:
        return Modulation(
            kind=LORA,
            spreading_factor=_int(_g(lora, "spreading_factor"), None),
            bandwidth=_int(_g(lora, "bandwidth"), None),
            code_rate=_code_rate(lora),
            preamble=_int(_g(lora, "preamble"), None) or None,
        )
    fsk = _g(data, "fsk")
    if fsk is not None:
        return Modulation(kind=FSK, datarate=_int(_g(fsk, "datarate"), None))
    lr_fhss = _g(data, "lr_fhss")
    if lr_fhss is not None:
        return Modulation(kind=LR_FHSS, code_rate=_code_rate(lr_fhss))
    return Modulation()


def _legacy_modulation(tx_legacy: dict[str, Any]) -> Modulation:
    wrapped = {
        "lora": _g(tx_legacy, "lora_modulation_info"),
        "fsk": _g(tx_legacy, "fsk_modulation_info"),
        "lr_fhss": _g(tx_legacy, "lr_fhss_modulation_info"),
    }
    return parse_modulation({k: v for k, v in wrapped.items() if v is not None})


# ---------------------------------------------------------------------------
# public parsers
# ---------------------------------------------------------------------------


def parse_uplink(payload: bytes, gateway_id: str) -> Uplink:
    """Parse an event/up payload (gw.UplinkFrame)."""
    data = _load(payload, pb.UPLINK_FRAME)
    tx_info = _g(data, "tx_info")
    rx_info = _g(data, "rx_info")

    if tx_info is not None:
        frequency = _int(_g(tx_info, "frequency")) or 0
        modulation = parse_modulation(_g(tx_info, "modulation"))
    else:
        tx_legacy = _g(data, "tx_info_legacy") or {}
        frequency = _int(_g(tx_legacy, "frequency")) or 0
        modulation = _legacy_modulation(tx_legacy)

    if rx_info is not None:
        snr = _float(_g(rx_info, "snr"))
        gw_time = _timestamp(_g(rx_info, "gw_time"))
    else:
        rx_info = _g(data, "rx_info_legacy") or {}
        snr = _float(_g(rx_info, "lora_snr"))
        gw_time = _timestamp(_g(rx_info, "time"))

    if modulation.kind != LORA:
        snr = None  # only meaningful for LoRa
    elif snr is None:
        snr = 0.0  # proto3 omits a zero SNR
    return Uplink(
        gateway_id=gateway_id,
        phy_payload=_bytes(_g(data, "phy_payload")),
        frequency=frequency,
        modulation=modulation,
        rssi=_int(_g(rx_info, "rssi"), 0),
        snr=None if snr is None else round(snr, 2),
        crc_status=_enum(_g(rx_info, "crc_status"), CRC_STATUS),
        channel=_int(_g(rx_info, "channel"), None),
        rf_chain=_int(_g(rx_info, "rf_chain"), None),
        gw_time=gw_time,
    )


def _count_map(value: Any) -> dict[int, int]:
    out: dict[int, int] = {}
    if isinstance(value, dict):
        for key, count in value.items():
            freq = _int(key, None)
            if freq is not None:
                out[freq] = _int(count) or 0
    return out


def _modulation_counts(items: Any) -> dict[str, int]:
    out: dict[str, int] = {}
    for item in items or []:
        key = parse_modulation(_g(item, "modulation")).key
        out[key] = out.get(key, 0) + (_int(_g(item, "count")) or 0)
    return out


def parse_stats(payload: bytes, gateway_id: str) -> GatewayStats:
    """Parse an event/stats payload (gw.GatewayStats)."""
    data = _load(payload, pb.GATEWAY_STATS)

    location = _g(data, "location")
    loc: dict[str, float] | None = None
    if isinstance(location, dict):
        loc = {
            "latitude": _float(_g(location, "latitude")) or 0.0,
            "longitude": _float(_g(location, "longitude")) or 0.0,
            "altitude": _float(_g(location, "altitude")) or 0.0,
        }
        if not any(loc.values()):
            loc = None

    bands: list[DutyCycleBand] | None = None
    window: float | None = None
    duty = _g(data, "duty_cycle_stats")
    if isinstance(duty, dict):
        window = _duration(_g(duty, "window")) or None
        bands = [
            DutyCycleBand(
                name=str(_g(band, "name") or ""),
                frequency_min=_int(_g(band, "frequency_min")) or 0,
                frequency_max=_int(_g(band, "frequency_max")) or 0,
                load_max=_duration(_g(band, "load_max")),
                load_tracked=_duration(_g(band, "load_tracked")),
            )
            for band in _g(duty, "bands") or []
        ]

    per_status = _g(data, "tx_packets_per_status")
    return GatewayStats(
        gateway_id=gateway_id,
        time=_timestamp(_g(data, "time")),
        rx_received=_int(_g(data, "rx_packets_received")) or 0,
        rx_received_ok=_int(_g(data, "rx_packets_received_ok")) or 0,
        tx_received=_int(_g(data, "tx_packets_received")) or 0,
        tx_emitted=_int(_g(data, "tx_packets_emitted")) or 0,
        config_version=_g(data, "config_version") or None,
        metadata={str(k): str(v) for k, v in (_g(data, "metadata") or {}).items()},
        location=loc,
        rx_per_frequency=_count_map(_g(data, "rx_packets_per_frequency")),
        tx_per_frequency=_count_map(_g(data, "tx_packets_per_frequency")),
        rx_per_modulation=_modulation_counts(_g(data, "rx_packets_per_modulation")),
        tx_per_modulation=_modulation_counts(_g(data, "tx_packets_per_modulation")),
        tx_per_status={
            str(k): _int(v) or 0 for k, v in (per_status.items() if isinstance(per_status, dict) else [])
        },
        duty_cycle_window=window,
        duty_cycle_bands=bands,
    )


def parse_downlink(payload: bytes, gateway_id: str) -> Downlink:
    """Parse a command/down payload (gw.DownlinkFrame)."""
    data = _load(payload, pb.DOWNLINK_FRAME)
    items = []
    for item in _g(data, "items") or []:
        tx_info = _g(item, "tx_info") or {}
        items.append(
            DownlinkItem(
                frequency=_int(_g(tx_info, "frequency")) or 0,
                power=_int(_g(tx_info, "power"), None),
                modulation=parse_modulation(_g(tx_info, "modulation")),
                phy_length=len(_bytes(_g(item, "phy_payload"))),
            )
        )
    return Downlink(gateway_id=gateway_id, downlink_id=_int(_g(data, "downlink_id")) or 0, items=items)


def parse_tx_ack(payload: bytes, gateway_id: str) -> TxAck:
    """Parse an event/ack payload (gw.DownlinkTxAck)."""
    data = _load(payload, pb.DOWNLINK_TX_ACK)
    statuses = [_enum(_g(item, "status"), TX_ACK_STATUS) for item in _g(data, "items") or []]
    if not statuses:  # ChirpStack v3: a single error string, empty on success
        error = _g(data, "error")
        statuses = [error if error else "OK"]
    return TxAck(gateway_id=gateway_id, downlink_id=_int(_g(data, "downlink_id")) or 0, statuses=statuses)


def parse_conn_state(payload: bytes, gateway_id: str) -> ConnState:
    """Parse a state/conn payload (gw.ConnState)."""
    data = _load(payload, pb.CONN_STATE)
    return ConnState(gateway_id=gateway_id, online=_enum(_g(data, "state"), CONN_STATE) == "ONLINE")
