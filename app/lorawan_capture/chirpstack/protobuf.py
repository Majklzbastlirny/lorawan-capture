"""Minimal protobuf wire-format decoder for ChirpStack gateway messages.

Only decoding is needed, so instead of depending on chirpstack-api (which
pulls in grpcio) this implements the protobuf wire format directly, driven by
small schema tables mirroring chirpstack/api/proto/gw/gw.proto (v4).
Unknown fields are skipped, so newer ChirpStack versions keep working.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Any, Union

# Scalar kinds.
UINT = "uint"  # uint32 / uint64
INT = "int"  # int32 / int64 (negative values are 10-byte varints)
BOOL = "bool"
ENUM = "enum"  # kept as int
FLOAT = "float"
DOUBLE = "double"
STRING = "string"
BYTES = "bytes"

_VARINT_KINDS = frozenset({UINT, INT, BOOL, ENUM})
_DEFAULTS = {UINT: 0, INT: 0, BOOL: False, ENUM: 0, FLOAT: 0.0, DOUBLE: 0.0, STRING: "", BYTES: b""}


class DecodeError(ValueError):
    """Raised when a payload is not a valid protobuf message."""


@dataclass(frozen=True, slots=True)
class MapOf:
    """A protobuf map<key, value> field (encoded as repeated key/value entries)."""

    key: str
    value: str


Schema = dict[int, tuple]  # field number -> (name, kind[, repeated])
Kind = Union[str, MapOf, dict]


def _read_varint(buf: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    end = len(buf)
    while True:
        if pos >= end:
            raise DecodeError("truncated varint")
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7
        if shift >= 70:
            raise DecodeError("varint too long")


def _signed(value: int) -> int:
    return value - (1 << 64) if value >= 1 << 63 else value


def _scalar(kind: str, wire_type: int, value: Any) -> Any:
    """Convert a raw wire value; returns _SKIP on a wire type mismatch."""
    if kind in _VARINT_KINDS:
        if wire_type != 0:
            return _SKIP
        if kind == INT:
            return _signed(value)
        if kind == BOOL:
            return bool(value)
        return value
    if kind == FLOAT:
        return struct.unpack("<f", value)[0] if wire_type == 5 else _SKIP
    if kind == DOUBLE:
        return struct.unpack("<d", value)[0] if wire_type == 1 else _SKIP
    if wire_type != 2:
        return _SKIP
    if kind == STRING:
        return value.decode("utf-8", "replace")
    return bytes(value)


def _packed(kind: str, raw: bytes) -> list[Any]:
    """Decode a packed repeated scalar field."""
    out: list[Any] = []
    if kind in _VARINT_KINDS:
        pos = 0
        while pos < len(raw):
            value, pos = _read_varint(raw, pos)
            out.append(_scalar(kind, 0, value))
    elif kind in (FLOAT, DOUBLE):
        size, wire_type = (4, 5) if kind == FLOAT else (8, 1)
        if len(raw) % size:
            raise DecodeError("bad packed length")
        out.extend(_scalar(kind, wire_type, raw[i : i + size]) for i in range(0, len(raw), size))
    return out


class _Skip:
    pass


_SKIP = _Skip()


def decode(buf: bytes, schema: Schema) -> dict[str, Any]:
    """Decode ``buf`` into a dict keyed by the schema's field names.

    Absent fields are absent from the result (callers apply proto3 defaults),
    repeated fields become lists and map fields become dicts.
    """
    out: dict[str, Any] = {}
    pos = 0
    end = len(buf)
    while pos < end:
        tag, pos = _read_varint(buf, pos)
        number, wire_type = tag >> 3, tag & 0x07
        if number == 0:
            raise DecodeError("field number 0")
        if wire_type == 0:
            value, pos = _read_varint(buf, pos)
        elif wire_type == 1:
            if pos + 8 > end:
                raise DecodeError("truncated fixed64")
            value = buf[pos : pos + 8]
            pos += 8
        elif wire_type == 2:
            length, pos = _read_varint(buf, pos)
            if pos + length > end:
                raise DecodeError("truncated length-delimited field")
            value = buf[pos : pos + length]
            pos += length
        elif wire_type == 5:
            if pos + 4 > end:
                raise DecodeError("truncated fixed32")
            value = buf[pos : pos + 4]
            pos += 4
        else:
            raise DecodeError(f"unsupported wire type {wire_type}")

        field = schema.get(number)
        if field is None:
            continue
        name, kind = field[0], field[1]
        repeated = len(field) > 2 and field[2]

        if isinstance(kind, MapOf):
            if wire_type != 2:
                continue
            entry = decode(value, {1: ("key", kind.key), 2: ("value", kind.value)})
            key = entry.get("key", _DEFAULTS.get(kind.key))
            out.setdefault(name, {})[key] = entry.get("value", _DEFAULTS.get(kind.value))
            continue

        if isinstance(kind, dict):
            if wire_type != 2:
                continue
            converted: Any = decode(value, kind)
        elif repeated and wire_type == 2 and kind not in (STRING, BYTES):
            out.setdefault(name, []).extend(_packed(kind, value))
            continue
        else:
            converted = _scalar(kind, wire_type, value)
            if converted is _SKIP:
                continue

        if repeated:
            out.setdefault(name, []).append(converted)
        else:
            out[name] = converted
    return out


# ---------------------------------------------------------------------------
# Schemas (chirpstack api/proto/gw/gw.proto and common/common.proto, v4)
# ---------------------------------------------------------------------------

TIMESTAMP: Schema = {1: ("seconds", INT), 2: ("nanos", INT)}
DURATION: Schema = {1: ("seconds", INT), 2: ("nanos", INT)}

LOCATION: Schema = {
    1: ("latitude", DOUBLE),
    2: ("longitude", DOUBLE),
    3: ("altitude", DOUBLE),
    4: ("source", ENUM),
    5: ("accuracy", FLOAT),
}

LORA_MODULATION: Schema = {
    1: ("bandwidth", UINT),
    2: ("spreading_factor", UINT),
    3: ("code_rate_legacy", STRING),
    4: ("polarization_inversion", BOOL),
    5: ("code_rate", ENUM),
    6: ("preamble", UINT),
    7: ("no_crc", BOOL),
}

FSK_MODULATION: Schema = {1: ("frequency_deviation", UINT), 2: ("datarate", UINT)}

LR_FHSS_MODULATION: Schema = {
    1: ("operating_channel_width", UINT),
    2: ("code_rate_legacy", STRING),
    3: ("grid_steps", UINT),
    4: ("code_rate", ENUM),
}

MODULATION: Schema = {
    3: ("lora", LORA_MODULATION),
    4: ("fsk", FSK_MODULATION),
    5: ("lr_fhss", LR_FHSS_MODULATION),
}

UPLINK_TX_INFO: Schema = {1: ("frequency", UINT), 2: ("modulation", MODULATION)}

UPLINK_RX_INFO: Schema = {
    1: ("gateway_id", STRING),
    2: ("uplink_id", UINT),
    3: ("gw_time", TIMESTAMP),
    4: ("time_since_gps_epoch", DURATION),
    5: ("fine_time_since_gps_epoch", DURATION),
    6: ("rssi", INT),
    7: ("snr", FLOAT),
    8: ("channel", UINT),
    9: ("rf_chain", UINT),
    10: ("board", UINT),
    11: ("antenna", UINT),
    12: ("location", LOCATION),
    13: ("context", BYTES),
    15: ("metadata", MapOf(STRING, STRING)),
    16: ("crc_status", ENUM),
    17: ("ns_time", TIMESTAMP),
}

# Pre-v4 layouts, still understood so older gateway bridges keep working.
UPLINK_TX_INFO_LEGACY: Schema = {
    1: ("frequency", UINT),
    3: ("lora_modulation_info", LORA_MODULATION),
    4: ("fsk_modulation_info", FSK_MODULATION),
    5: ("lr_fhss_modulation_info", LR_FHSS_MODULATION),
}

UPLINK_RX_INFO_LEGACY: Schema = {
    1: ("gateway_id", BYTES),
    2: ("time", TIMESTAMP),
    5: ("rssi", INT),
    6: ("lora_snr", DOUBLE),
    7: ("channel", UINT),
    8: ("rf_chain", UINT),
    17: ("crc_status", ENUM),
    18: ("metadata", MapOf(STRING, STRING)),
}

UPLINK_FRAME: Schema = {
    1: ("phy_payload", BYTES),
    2: ("tx_info_legacy", UPLINK_TX_INFO_LEGACY),
    3: ("rx_info_legacy", UPLINK_RX_INFO_LEGACY),
    4: ("tx_info", UPLINK_TX_INFO),
    5: ("rx_info", UPLINK_RX_INFO),
}

PER_MODULATION_COUNT: Schema = {1: ("modulation", MODULATION), 2: ("count", UINT)}

DUTY_CYCLE_BAND: Schema = {
    1: ("name", STRING),
    2: ("frequency_min", UINT),
    3: ("frequency_max", UINT),
    4: ("load_max", DURATION),
    5: ("load_tracked", DURATION),
}

DUTY_CYCLE_STATS: Schema = {
    1: ("regulation", ENUM),
    2: ("window", DURATION),
    3: ("bands", DUTY_CYCLE_BAND, True),
}

GATEWAY_STATS: Schema = {
    1: ("gateway_id_legacy", BYTES),
    2: ("time", TIMESTAMP),
    3: ("location", LOCATION),
    4: ("config_version", STRING),
    5: ("rx_packets_received", UINT),
    6: ("rx_packets_received_ok", UINT),
    7: ("tx_packets_received", UINT),
    8: ("tx_packets_emitted", UINT),
    10: ("metadata", MapOf(STRING, STRING)),
    12: ("tx_packets_per_frequency", MapOf(UINT, UINT)),
    13: ("rx_packets_per_frequency", MapOf(UINT, UINT)),
    14: ("tx_packets_per_modulation", PER_MODULATION_COUNT, True),
    15: ("rx_packets_per_modulation", PER_MODULATION_COUNT, True),
    16: ("tx_packets_per_status", MapOf(STRING, UINT)),
    17: ("gateway_id", STRING),
    18: ("duty_cycle_stats", DUTY_CYCLE_STATS),
}

DOWNLINK_TX_INFO: Schema = {
    1: ("frequency", UINT),
    2: ("power", INT),
    3: ("modulation", MODULATION),
    4: ("board", UINT),
    5: ("antenna", UINT),
    7: ("context", BYTES),
}

DOWNLINK_FRAME_ITEM: Schema = {1: ("phy_payload", BYTES), 3: ("tx_info", DOWNLINK_TX_INFO)}

DOWNLINK_FRAME: Schema = {
    3: ("downlink_id", UINT),
    4: ("downlink_id_legacy", BYTES),
    5: ("items", DOWNLINK_FRAME_ITEM, True),
    6: ("gateway_id_legacy", BYTES),
    7: ("gateway_id", STRING),
}

DOWNLINK_TX_ACK_ITEM: Schema = {1: ("status", ENUM)}

DOWNLINK_TX_ACK: Schema = {
    1: ("gateway_id_legacy", BYTES),
    2: ("downlink_id", UINT),
    3: ("error", STRING),  # ChirpStack v3 only
    4: ("downlink_id_legacy", BYTES),
    5: ("items", DOWNLINK_TX_ACK_ITEM, True),
    6: ("gateway_id", STRING),
}

CONN_STATE: Schema = {1: ("gateway_id_legacy", BYTES), 2: ("state", ENUM), 3: ("gateway_id", STRING)}
