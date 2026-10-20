"""Device explorer: per-device statistics, MAC commands and packet breakdowns over a range of days.

Everything here comes from what the gateways receive: LoRaWAN headers, and the MAC commands that
LoRaWAN 1.0.x devices send unencrypted in FOpts (battery level, link margin, ADR answers ...).
Application payloads stay encrypted. LoRaWAN 1.1 devices encrypt FOpts too; decoded, those
give plausible-looking nonsense, which nothing in the bytes reveals.
"""

from __future__ import annotations

import csv
import io
import logging
import math
import sqlite3
import statistics
from collections import Counter, defaultdict, deque
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from . import store
from .phy import MTYPE

log = logging.getLogger(__name__)
UTC = timezone.utc

# ------------------------------------------------------------------ networks
# NetIDs whose operator is certain; the others are shown as numbers
KNOWN_NETIDS = {0x000000: "private / experimental", 0x000001: "private / experimental",
                0x000013: "The Things Network"}
NWKID_BITS = (6, 6, 9, 11, 12, 13, 15, 17)  # per DevAddr type (LoRaWAN Backend Interfaces)


def network(dev_addr: str) -> dict:
    """The operator's NetID, as far as a DevAddr shows it: the leading ones give the address
    type, the next bits the NwkID; types 0-2 carry the whole NetID, the rest only its end."""
    value = int(dev_addr, 16)
    kind = 0
    while kind < 7 and value >> (31 - kind) & 1:
        kind += 1
    bits = NWKID_BITS[kind]
    nwkid = value >> (31 - kind - bits) & ((1 << bits) - 1)
    if kind <= 2:
        netid = kind << 21 | nwkid
        return {"netid": f"{netid:06X}", "label": KNOWN_NETIDS.get(netid) or f"NetID {netid:06X}"}
    return {"netid": None, "label": f"NetID type {kind}, NwkID {nwkid:X}"}


# -------------------------------------------------------------- MAC commands
EU868_DR = {0: "SF12/125 kHz", 1: "SF11/125 kHz", 2: "SF10/125 kHz", 3: "SF9/125 kHz", 4: "SF8/125 kHz",
            5: "SF7/125 kHz", 6: "SF7/250 kHz", 7: "FSK"}
MAX_EIRP = (8, 10, 12, 13, 14, 16, 18, 20, 21, 24, 26, 27, 29, 30, 33, 36)
GPS_EPOCH = datetime(1980, 1, 6, tzinfo=UTC)
LEAP_SECONDS = 18  # GPS - UTC since 2017


def battery_text(value: int) -> str:
    if value == 0:
        return "external power"
    if value == 255:
        return "not measured"
    return f"{round((value - 1) * 100 / 253)} % ({value}/254)"


def _margin(byte: int) -> int:  # 6-bit signed, dB
    value = byte & 0x3F
    return value - 64 if value >= 32 else value


def _mhz(raw: bytes) -> str:
    return f"{int.from_bytes(raw, 'little') * 100 / 1e6:.3f} MHz"


def _acks(byte: int, names: tuple[str, ...]) -> str:
    return ", ".join(f"{name} {'ok' if byte >> (len(names) - 1 - i) & 1 else 'REFUSED'}"
                     for i, name in enumerate(names))


def _version(p: bytes) -> str:
    return f"LoRaWAN 1.{p[0] & 15}"


def _class(p: bytes) -> str:
    return {0: "class A", 2: "class C"}.get(p[0], f"mode {p[0]}")


def _link_adr_req(p: bytes) -> str:
    rate, power = p[0] >> 4, p[0] & 15
    mask, control, repeat = int.from_bytes(p[1:3], "little"), p[3] >> 4 & 7, p[3] & 15
    parts = ["data rate unchanged" if rate == 15 else f"DR{rate} ({EU868_DR.get(rate, 'not used in EU868')})"]
    if power == 15:
        parts.append("TX power unchanged")
    elif power <= 7:  # EU868: steps of 2 dB below the maximum EIRP, 16 dBm unless set otherwise
        parts.append(f"TX power {power}: {16 - 2 * power} dBm EIRP" + (" (the maximum)" if power == 0 else ""))
    else:
        parts.append(f"TX power {power} (not used in EU868)")
    if control == 6:
        parts.append("all channels on")
    elif control == 0:
        parts.append("channels " + (", ".join(str(i) for i in range(16) if mask >> i & 1) or "none") + " on")
    else:
        parts.append(f"channel mask control {control}, mask {mask:016b}")
    parts.append(f"each uplink sent {repeat or 1}x")
    return ", ".join(parts)


def _new_channel(p: bytes) -> str:
    if not int.from_bytes(p[1:4], "little"):
        return f"removes channel {p[0]}"
    return f"channel {p[0]}: {_mhz(p[1:4])}, DR{p[4] & 15}-DR{p[4] >> 4}"


def _device_time(p: bytes) -> str:
    seconds = int.from_bytes(p[0:4], "little") + p[4] / 256
    return f"network time {GPS_EPOCH + timedelta(seconds=seconds - LEAP_SECONDS):%Y-%m-%d %H:%M:%S} UTC"


UP_MAC = {  # device -> network: cid: (name, payload bytes, describe)
    0x01: ("ResetInd", 1, _version),
    0x02: ("LinkCheckReq", 0, lambda p: "asks how well the network hears it"),
    0x03: ("LinkADRAns", 1, lambda p: _acks(p[0], ("power", "data rate", "channel mask"))),
    0x04: ("DutyCycleAns", 0, lambda p: "duty-cycle limit accepted"),
    0x05: ("RXParamSetupAns", 1, lambda p: _acks(p[0], ("RX1 offset", "RX2 data rate", "channel"))),
    0x06: ("DevStatusAns", 2, lambda p: f"battery {battery_text(p[0])}, link margin {_margin(p[1]):+d} dB"),
    0x07: ("NewChannelAns", 1, lambda p: _acks(p[0], ("data rate range", "frequency"))),
    0x08: ("RXTimingSetupAns", 0, lambda p: "RX timing accepted"),
    0x09: ("TxParamSetupAns", 0, lambda p: "TX parameters accepted"),
    0x0A: ("DlChannelAns", 1, lambda p: _acks(p[0], ("uplink frequency", "frequency"))),
    0x0B: ("RekeyInd", 1, _version),
    0x0C: ("ADRParamSetupAns", 0, lambda p: "ADR parameters accepted"),
    0x0D: ("DeviceTimeReq", 0, lambda p: "asks for the network time"),
    0x0F: ("RejoinParamSetupAns", 1, lambda p: "rejoin parameters accepted"),
    0x10: ("PingSlotInfoReq", 1, lambda p: f"class B ping period 2^{p[0] & 7} s"),
    0x11: ("PingSlotChannelAns", 1, lambda p: _acks(p[0], ("data rate", "frequency"))),
    0x12: ("BeaconTimingReq", 0, lambda p: "asks for the beacon timing"),
    0x13: ("BeaconFreqAns", 1, lambda p: _acks(p[0], ("frequency",))),
    0x20: ("DeviceModeInd", 1, _class),
}
DOWN_MAC = {  # network -> device
    0x01: ("ResetConf", 1, _version),
    0x02: ("LinkCheckAns", 2, lambda p: f"link margin {p[0]} dB, heard by {p[1]} gateway(s)"),
    0x03: ("LinkADRReq", 4, _link_adr_req),
    0x04: ("DutyCycleReq", 1, lambda p: f"limit the duty cycle to 1/{2 ** (p[0] & 15)}"),
    0x05: ("RXParamSetupReq", 4, lambda p: f"RX1 offset {p[0] >> 4 & 7}, RX2 DR{p[0] & 15} on {_mhz(p[1:4])}"),
    0x06: ("DevStatusReq", 0, lambda p: "asks for the battery level and link margin"),
    0x07: ("NewChannelReq", 5, _new_channel),
    0x08: ("RXTimingSetupReq", 1, lambda p: f"RX1 delay {(p[0] & 15) or 1} s"),
    0x09: ("TxParamSetupReq", 1, lambda p: f"max EIRP {MAX_EIRP[p[0] & 15]} dBm, dwell time limit "
                                            f"up {'on' if p[0] & 16 else 'off'}, down {'on' if p[0] & 32 else 'off'}"),
    0x0A: ("DlChannelReq", 4, lambda p: f"channel {p[0]} downlinks on {_mhz(p[1:4])}"),
    0x0B: ("RekeyConf", 1, _version),
    0x0C: ("ADRParamSetupReq", 1, lambda p: f"ADR ack limit 2^{p[0] >> 4}, delay 2^{p[0] & 15}"),
    0x0D: ("DeviceTimeAns", 5, _device_time),
    0x0E: ("ForceRejoinReq", 2, lambda p: "orders a rejoin"),
    0x0F: ("RejoinParamSetupReq", 1, lambda p: "sets the rejoin intervals"),
    0x10: ("PingSlotInfoAns", 0, lambda p: "class B ping slot accepted"),
    0x11: ("PingSlotChannelReq", 4, lambda p: f"class B ping slots on {_mhz(p[0:3])}, DR{p[3] & 15}"),
    0x12: ("BeaconTimingAns", 3, lambda p: "class B beacon timing"),
    0x13: ("BeaconFreqReq", 3, lambda p: f"class B beacons on {_mhz(p[0:3])}"),
    0x20: ("DeviceModeConf", 1, _class),
}


def decode_mac(raw: bytes, uplink: bool) -> list[dict]:
    """MAC commands in FOpts. An unknown command ends the walk: its length is unknown."""
    table = UP_MAC if uplink else DOWN_MAC
    out, i = [], 0
    while i < len(raw):
        cid = raw[i]
        spec = table.get(cid)
        if spec is None or i + 1 + spec[1] > len(raw):
            out.append({"name": f"unknown 0x{cid:02X}", "hex": raw[i:].hex(),
                        "text": "cannot be read further (LoRaWAN 1.1 devices encrypt FOpts)"})
            break
        name, length, describe = spec
        payload = raw[i + 1:i + 1 + length]
        item = {"name": name, "hex": raw[i:i + 1 + length].hex(), "text": describe(payload)}
        if name == "DevStatusAns":
            item["battery"], item["margin"] = payload[0], _margin(payload[1])
        elif name == "LinkADRReq":
            if payload[0] >> 4 != 15:
                item["data_rate"] = payload[0] >> 4
            if payload[0] & 15 <= 7:
                item["tx_power"] = payload[0] & 15
        out.append(item)
        i += 1 + length
    return out


# ------------------------------------------------------------ packet breakdown
REJOIN_LENGTHS = {0: 19, 1: 24, 2: 19}


def readable_header(head: bytes) -> tuple[str, bool] | None:
    """The first 11 bytes as text, plain or after XOR 0x85 (some proprietary radios scramble
    every byte that way), when every byte is printable: (text, scrambled)."""
    if len(head) < 11:
        return None
    for scrambled in (False, True):
        text = bytes(b ^ 0x85 for b in head[:11]) if scrambled else head[:11]
        if all(32 <= c < 127 for c in text):
            return text.decode(), scrambled
    return None


def breakdown(phy: bytes, sf: int | None = None) -> dict:
    """Every byte of a packet labelled with what it means in LoRaWAN, plus notes when it does not
    look like LoRaWAN at all."""
    fields: list[dict] = []
    mac: list[dict] = []
    notes: list[str] = []

    def add(name: str, start: int, length: int, text: str) -> None:
        fields.append({"name": name, "offset": start, "hex": phy[start:start + length].hex(), "text": text})

    if not phy:
        return {"lorawan": False, "fields": fields, "mac": mac, "notes": ["empty packet"]}
    mtype, major = phy[0] >> 5 & 7, phy[0] & 3
    lorawan = major == 0 and (sf is None or sf >= 7)
    add("MHDR", 0, 1, f"message type {MTYPE[mtype]}, major version {major}")
    if mtype in (2, 3, 4, 5) and len(phy) >= 12:
        uplink = mtype in (2, 4)
        add("DevAddr", 1, 4, f"{int.from_bytes(phy[1:5], 'little'):08x} (sent least significant byte first)")
        fctrl, fol = phy[5], phy[5] & 15
        flags = [name for name, bit in (("ADR", 7), ("ADRACKReq" if uplink else "RFU", 6), ("ACK", 5),
                                        ("ClassB" if uplink else "FPending", 4)) if fctrl >> bit & 1]
        add("FCtrl", 5, 1, (", ".join(flags) or "no flags") + f"; {fol} bytes of FOpts")
        add("FCnt", 6, 2, f"{int.from_bytes(phy[6:8], 'little')} (the lower 16 bits of the frame counter)")
        if 8 + fol > len(phy) - 4:
            lorawan = False
        if fol:
            add("FOpts", 8, fol, "MAC commands, unencrypted in LoRaWAN 1.0.x")
            mac = decode_mac(phy[8:8 + fol], uplink)
        rest = phy[8 + fol:-4]
        if rest:
            add("FPort", 8 + fol, 1, str(rest[0]) + (" (MAC commands only, encrypted)" if rest[0] == 0
                                                       else " (application port)"))
            if len(rest) > 1:
                add("FRMPayload", 9 + fol, len(rest) - 1, f"{len(rest) - 1} bytes of application data, encrypted")
        add("MIC", len(phy) - 4, 4, "message integrity code")
    elif mtype == 0 and len(phy) == 23:
        add("JoinEUI", 1, 8, f"{int.from_bytes(phy[1:9], 'little'):016x}")
        add("DevEUI", 9, 8, f"{int.from_bytes(phy[9:17], 'little'):016x}")
        add("DevNonce", 17, 2, str(int.from_bytes(phy[17:19], "little")))
        add("MIC", 19, 4, "message integrity code")
    elif mtype == 1 and len(phy) in (17, 33):
        add("JoinAccept", 1, len(phy) - 1, "encrypted with the device's key")
    elif mtype == 6 and len(phy) >= 2 and REJOIN_LENGTHS.get(phy[1]) == len(phy):
        add("RejoinType", 1, 1, str(phy[1]))
        if phy[1] == 1:
            add("JoinEUI", 2, 8, f"{int.from_bytes(phy[2:10], 'little'):016x}")
            add("DevEUI", 10, 8, f"{int.from_bytes(phy[10:18], 'little'):016x}")
            add("RJcount1", 18, 2, str(int.from_bytes(phy[18:20], "little")))
            add("MIC", 20, 4, "message integrity code")
        else:
            add("NetID", 2, 3, f"{int.from_bytes(phy[2:5], 'little'):06X}")
            add("DevEUI", 5, 8, f"{int.from_bytes(phy[5:13], 'little'):016x}")
            add("RJcount0", 13, 2, str(int.from_bytes(phy[13:15], "little")))
            add("MIC", 15, 4, "message integrity code")
    else:
        lorawan = lorawan and mtype == 7  # proprietary frames are free-form by definition
        if len(phy) > 1:
            add("payload", 1, len(phy) - 1, f"{len(phy) - 1} bytes")
    if not lorawan:
        notes.append("This does not fit LoRaWAN (layout, version or spreading factor): most likely another "
                     "system sharing the channel. The field names are only what the bytes would mean "
                     "in LoRaWAN.")
    readable = readable_header(phy)
    if readable:
        text, scrambled = readable
        notes.append(f"XOR 0x85 turns the first bytes into \"{text}\": a radio system that scrambles every "
                     "byte that way." if scrambled else f"The first bytes are plain text: \"{text}\".")
    return {"lorawan": lorawan, "fields": fields, "mac": mac, "notes": notes}


airtime = store.airtime


# ------------------------------------------------------------------ reading
# `dev_addr >= ''` (rather than IS NOT NULL) makes SQLite read through the ix_dev index: on a flood
# day most rows have no DevAddr, and this skips them instead of reading the whole file
UPLINKS = ("SELECT ts_utc, gateway_id, dev_addr, fcnt, fport, frm_len, mtype, adr, ack, fopts_hex, sf, bw_hz, "
           "code_rate, freq_hz, rssi, snr, phy_len, frame_key, adr_ack_req, class_b FROM frames WHERE dev_addr >= '' "
           "AND is_downlink = 0 AND major = 0 AND sf BETWEEN 7 AND 12 "
           "AND mtype IN ('UnconfirmedDataUp', 'ConfirmedDataUp') AND (crc = 'CRC_OK' OR crc IS NULL)")
DOWNLINKS = ("SELECT ts_utc, gateway_id, fcnt, fopts_hex, freq_hz, sf, rssi, snr, mtype, frame_key, fport, frm_len, "
             "ack, class_b FROM frames WHERE dev_addr = ? AND is_downlink = 1 AND (crc = 'CRC_OK' OR crc IS NULL) "
             "ORDER BY ts_utc")
# the DevNonce has no column of its own: bytes 17-18 of the join request, little-endian
JOINS = ("SELECT dev_eui, join_eui, ts_utc, gateway_id, rssi, sf, frame_key, substr(phy_hex, 35, 4) FROM frames "
         "WHERE mtype = 'JoinRequest' "
         "AND major = 0 AND sf BETWEEN 7 AND 12 AND dev_eui IS NOT NULL AND (crc = 'CRC_OK' OR crc IS NULL)")
OTHER = ("SELECT sf, substr(phy_hex, 1, 22), gateway_id, freq_hz, count(*), min(ts_utc), max(ts_utc), "
         "min(phy_len), max(phy_len), max(rssi), max(frame_key) FROM frames "
         f"WHERE ({store.CLASS_SQL}) IN ('other', 'sf5') GROUP BY 1, 2, 3, 4")
# every reception of those in the order it arrived, to count transmissions (count_packets)
OTHER_RX = ("SELECT frame_key, julianday(ts_utc), gateway_id, sf, substr(phy_hex, 1, 22), substr(ts_utc, 1, 13) "
            f"FROM frames WHERE ({store.CLASS_SQL}) IN ('other', 'sf5') ORDER BY id")
SAME_PACKET_S = 1.5  # the same bytes at another gateway within this long: one transmission heard twice
STATS_TIMES = "SELECT ts_utc FROM stats ORDER BY ts_utc"
CACHE = 4  # version of what the explorer keeps in the summary files: bump when it changes
FRAME = ("SELECT ts_utc, gateway_id, freq_hz, sf, bw_hz, code_rate, rssi, snr, crc, phy_hex, mtype, dev_addr, "
         "is_downlink, dev_eui FROM frames WHERE frame_key = ? ORDER BY ts_utc")


class Source:
    """Rows from the daily files of a date range. Today's file is read through the capture's
    own connection (one process holds it); closed days are opened read-only. Days that cannot
    be read are skipped and listed in `skipped`."""

    def __init__(self, capture, data_dir: Path) -> None:
        self.capture, self.data_dir = capture, data_dir
        self.skipped: list[str] = []
        self.read: set[str] = set()

    def days(self, first: date, last: date) -> list[tuple[date, Path]]:
        return [(day, path) for day, path in sorted(store.list_days(self.data_dir)) if first <= day <= last]

    def _read(self, day: date, path: Path, work: Callable[[Callable], object]) -> object:
        """work(run), where run(sql, params=()) gives the rows of the day's file: today's under the
        capture's lock (the writer waits meanwhile), a closed day's read-only. None if unreadable."""
        try:
            if day == self.capture.day:
                return self.capture.read(work)
            conn = store.open_readonly(path)
            try:
                return work(conn.execute)
            finally:
                conn.close()
        except sqlite3.Error as err:
            log.warning("explorer: cannot read %s: %s", path.name, err)
            if day.isoformat() not in self.skipped:
                self.skipped.append(day.isoformat())
            return None

    def _fetch(self, day: date, path: Path, sql: str, params: tuple) -> list | None:
        return self._read(day, path, lambda run: list(run(sql, params)))

    def rows(self, first: date, last: date, sql: str, params: tuple = ()) -> Iterator[tuple[date, tuple]]:
        """Closed days row by row from the cursor, so a long range is never all in memory at once."""
        for day, path in self.days(first, last):
            if day == self.capture.day:
                rows = self._fetch(day, path, sql, params)
                if rows is not None:
                    self.read.add(day.isoformat())
                    yield from ((day, row) for row in rows)
                continue
            try:
                conn = store.open_readonly(path)
                try:
                    for row in conn.execute(sql, params):
                        yield day, row
                finally:
                    conn.close()
                self.read.add(day.isoformat())
            except sqlite3.Error as err:
                log.warning("explorer: cannot read %s: %s", path.name, err)
                if day.isoformat() not in self.skipped:
                    self.skipped.append(day.isoformat())

    def per_day(self, first: date, last: date, name: str, work: Callable[[Callable], object]) -> Iterator:
        """work(run) for each day (see _read); for a closed day, kept in its summary file once worked
        out (closed files do not change), so a flood day is scanned once, not on every visit."""
        key = f"{name}_v{CACHE}"
        for day, path in self.days(first, last):
            summary = store.cached_summary(path) if day != self.capture.day else None
            result = summary.get(key) if summary else None
            if result is None:
                result = self._read(day, path, work)
                if result is None:
                    continue
                if summary is not None:
                    store.update_summary(path, {key: result})
            self.read.add(day.isoformat())
            yield day, result

    def silences(self, first: date, last: date) -> list[list[str]]:
        """The times nothing was recorded in the range, also between days and over missing days."""
        gaps, previous = [], None
        for _, info in self.per_day(first, last, "silences",
                                    one_query(STATS_TIMES, lambda rows: store.silences([r[0] for r in rows]))):
            if info["first"] is None:
                continue
            if previous and _span(previous, info["first"]) > store.SILENCE_S:
                gaps.append([previous, info["first"]])
            gaps += info["gaps"]
            previous = info["last"]
        return gaps


def one_query(sql: str, make: Callable[[Iterable], object]) -> Callable[[Callable], object]:
    """Source.per_day work: make(the rows of one query)."""
    return lambda run: make(run(sql))


@dataclass(slots=True)
class Uplink:
    """One uplink: all receptions of one transmission (and of its repeats: same bytes)."""

    day: str
    key: str
    t: str
    fcnt: int
    port: int | None
    size: int | None
    confirmed: bool
    adr: bool
    ack: bool
    fopts: str | None
    sf: int
    bw: int
    cr: int
    freq: int
    phy_len: int
    adr_ack_req: bool  # no downlink for a long while: asks whether the network still hears it
    class_b: bool
    gateways: dict = field(default_factory=dict)  # gateway -> [receptions, best rssi, best snr]

    @property
    def sent(self) -> int:
        """Transmissions: a gateway hears each one at most once, so repeats show as more."""
        return max((g[0] for g in self.gateways.values()), default=1)


def _best(a, b):
    return b if a is None else a if b is None else max(a, b)


def _new_uplink(day: str, row: tuple) -> Uplink:
    (ts, _, _, fcnt, port, size, mtype, adr, ack, fopts, sf, bw, cr, freq, _, _, phy_len, key, adr_ack_req,
     class_b) = row
    rate = int(cr) if cr and cr.isdigit() and 1 <= int(cr) <= 4 else 1  # 1 = 4/5 ... 4 = 4/8
    return Uplink(day, key, ts, fcnt, port, size, mtype == "ConfirmedDataUp", bool(adr), bool(ack), fopts, sf, bw,
                  rate, freq, phy_len, bool(adr_ack_req), bool(class_b))


def _receive(up: Uplink, row: tuple, gw: str) -> None:
    """Add one reception (a row of UPLINKS) to its uplink."""
    ts, rssi, snr = row[0], row[14], row[15]
    if ts < up.t:
        up.t = ts
    seen = up.gateways.get(gw)
    if seen is None:
        up.gateways[gw] = [1, rssi, snr]
    else:
        seen[0] += 1
        seen[1], seen[2] = _best(seen[1], rssi), _best(seen[2], snr)


def _collect(rows) -> dict[str, dict[str, Uplink]]:
    out: dict[str, dict[str, Uplink]] = defaultdict(dict)
    shared: dict = {}  # one copy of each day and gateway id for all uplinks (a month is ~200k of them)
    for day, row in rows:
        dev, key = row[2], row[17]
        up = out[dev].get(key)
        if up is None:
            up = out[dev][key] = _new_uplink(shared.setdefault(day, day.isoformat()), row)
        _receive(up, row, shared.setdefault(row[1], row[1]))
    return out


STREAM_WINDOW_S = 60  # the receptions of one transmission (and its repeats) arrive within seconds


def stream_uplinks(source: Source, dev: str, first: date, last: date) -> Iterator[Uplink]:
    """One device's uplinks in time order, finished one by one while reading, so any range fits in
    a little memory: its rows come in time order through ix_dev, a day after the other."""
    pending: dict[str, Uplink] = {}
    for day, row in source.rows(first, last, UPLINKS + " AND dev_addr = ? ORDER BY ts_utc", (dev,)):
        ts, key = row[0], row[17]
        up = pending.get(key)
        if up is None:
            while pending:
                oldest = next(iter(pending.values()))
                if _span(oldest.t, ts) <= STREAM_WINDOW_S:
                    break
                yield pending.pop(oldest.key)
            up = pending[key] = _new_uplink(day.isoformat(), row)
        _receive(up, row, row[1])
    yield from pending.values()


def uplinks_csv(source: Source, dev: str, first: date, last: date, tz: ZoneInfo,
                gateways: list[tuple[str, str]], excel: bool) -> Iterator[str]:
    """A device's uplinks as CSV text, chunk by chunk. `gateways`: (id, column label) for the RSSI
    and SNR columns. Excel: ';' between fields, decimal commas and a byte-order mark, which is how
    Excel with Czech (and most European) settings opens a CSV; otherwise plain ',' and '.'."""
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";" if excel else ",", lineterminator="\r\n")

    def num(value, digits: int | None = None) -> str:
        if value is None:
            return ""
        text = f"{value:.{digits}f}" if digits is not None else str(value)
        return text.replace(".", ",") if excel else text

    def take() -> str:
        text = buf.getvalue()
        buf.seek(0)
        buf.truncate()
        return text

    header = ["time_utc", "time_local", "fcnt", "port", "bytes", "sf", "bandwidth_khz", "frequency_mhz",
              "confirmed", "adr", "adr_ack_req", "ack", "class_b", "transmissions", "mac_commands",
              "battery", "battery_pct", "link_margin_db", "gateways", "best_rssi_dbm", "best_snr_db"]
    for _, label in gateways:
        header += [f"rssi_{label}", f"snr_{label}"]
    writer.writerow(header + ["frame_key"])
    yield ("\ufeff" if excel else "") + take()
    for n, u in enumerate(stream_uplinks(source, dev, first, last), 1):
        cmds = decode_mac(bytes.fromhex(u.fopts), True) if u.fopts else []
        status = next((c for c in cmds if "battery" in c), None)
        level = status["battery"] if status else None
        rssi = [g[1] for g in u.gateways.values() if g[1] is not None]
        snr = [g[2] for g in u.gateways.values() if g[2] is not None]
        row = [u.t, datetime.fromisoformat(u.t).astimezone(tz).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], u.fcnt,
               num(u.port), num(u.size), u.sf, num(u.bw // 1000 if u.bw else None), num(u.freq / 1e6, 3),
               int(u.confirmed), int(u.adr), int(u.adr_ack_req), int(u.ack), int(u.class_b), u.sent,
               " ".join(c["name"] for c in cmds), num(level),
               num(round((level - 1) * 100 / 253, 1) if level and level < 255 else None),
               num(status["margin"] if status else None), len(u.gateways), num(max(rssi) if rssi else None),
               num(max(snr) if snr else None)]
        for gid, _ in gateways:
            seen = u.gateways.get(gid)
            row += [num(seen[1]) if seen else "", num(seen[2]) if seen else ""]
        writer.writerow(row + [u.key])
        if n % 500 == 0:
            yield take()
    yield take()


def _span(a: str, b: str) -> float:
    return (datetime.fromisoformat(b) - datetime.fromisoformat(a)).total_seconds()


def _summary(dev: str, ups: list[Uplink], own: frozenset[str], gaps: list) -> dict:
    """Uplinks must be sorted by time. Sent is estimated from the frame counter: steps of 1-999
    count as that many uplinks (the ones between were not heard), anything else as a new counter
    (reset). Steps across a time nothing was recorded count apart, as unrecorded."""
    sent, unrecorded, per_uplink = 1, 0, []
    for a, b in zip(ups, ups[1:]):
        step = store.fcnt_step(a.fcnt, b.fcnt)
        if not 0 < step < 1000:
            sent += 1
        elif store.unrecorded(gaps, a.t, b.t):
            sent, unrecorded = sent + 1, unrecorded + step - 1
        else:
            sent += step
            seconds = _span(a.t, b.t)
            if seconds > 0:
                per_uplink.append(seconds / step)
    signal: dict[str, list] = defaultdict(list)
    for u in ups:
        for gw, (_, rssi, snr) in u.gateways.items():
            signal[gw].append((rssi, snr))
    gateways = {}
    for gw, values in signal.items():
        rssi = [r for r, _ in values if r is not None]
        snr = [s for _, s in values if s is not None]
        gateways[gw] = {"heard": len(values),
                        "rssi_median": round(statistics.median(rssi), 1) if rssi else None,
                        "rssi_max": max(rssi) if rssi else None,
                        "snr_median": round(statistics.median(snr), 1) if snr else None}
    battery, mac = None, Counter()
    for u in ups:
        if u.fopts:
            for cmd in decode_mac(bytes.fromhex(u.fopts), uplink=True):
                mac[cmd["name"]] += 1
                if "battery" in cmd:
                    battery = {"value": cmd["battery"], "text": battery_text(cmd["battery"]),
                               "margin": cmd["margin"], "at": u.t}
    air = sum(airtime(u.sf, u.bw, u.phy_len, u.cr) * u.sent for u in ups)
    span = _span(ups[0].t, ups[-1].t)
    return {
        "dev_addr": dev, "own": dev in own, "network": network(dev),
        "heard": len(ups), "sent": sent, "lost": sent - len(ups), "unrecorded": unrecorded,
        "first": ups[0].t, "last": ups[-1].t,
        "interval_s": round(statistics.median(per_uplink), 1) if per_uplink else None,
        "gateways": gateways,
        "sf": {str(sf): n for sf, n in Counter(u.sf for u in ups).most_common()},
        "ports": Counter(u.port for u in ups).most_common(3),
        "sizes": Counter(u.size for u in ups).most_common(3),
        "confirmed": sum(u.confirmed for u in ups), "adr": sum(u.adr for u in ups),
        "acks": sum(u.ack for u in ups), "repeated": sum(u.sent > 1 for u in ups),
        "adr_ack_req": sum(u.adr_ack_req for u in ups), "class_b": sum(u.class_b for u in ups),
        "airtime_s": round(air, 3), "duty_pct": round(100 * air / span, 4) if span > 0 else None,
        "battery": battery, "mac": dict(mac),
    }


def _meta(source: Source, first: date, last: date) -> dict:
    return {"from": first.isoformat(), "to": last.isoformat(), "days": sorted(source.read),
            "skipped": source.skipped}


# ------------------------------------------------------------------ reports
def devices_report(source: Source, first: date, last: date, own: frozenset[str]) -> dict:
    """Every LoRaWAN device with uplinks in the range."""
    uplinks = _collect(source.rows(first, last, UPLINKS))
    gaps = source.silences(first, last)
    devices = [_summary(dev, sorted(ups.values(), key=lambda u: u.t), own, gaps) for dev, ups in uplinks.items()]
    devices.sort(key=lambda d: (-d["heard"], d["dev_addr"]))
    return {**_meta(source, first, last), "devices": devices}


NONCE_STEP = 256  # a DevNonce that counts moves on by about the number of attempts in between


def _nonce_step(nonces: dict, before: int, after: int) -> None:
    """Count one step between two join requests in a row: small and upwards is what a counter does,
    anything else what random numbers do."""
    nonces["small" if 0 < (after - before) % 65536 <= NONCE_STEP else "big"] += 1  # 16 bits: wraps too


def _nonce_kind(nonces: dict) -> str | None:
    if nonces["small"] >= 2 and not nonces["big"]:
        return "counter"
    return "random" if nonces["big"] >= 2 else None


def join_groups(rows) -> list[dict]:
    """One day's join requests per DevEUI."""
    joins: dict[str, dict] = {}
    for eui, join_eui, ts, gw, rssi, sf, key, nonce in rows:
        j = joins.setdefault(eui, {"dev_eui": eui, "join_eui": join_eui, "keys": set(), "first": ts, "last": ts,
                                   "gateways": {}, "sf": Counter(), "nonces": []})
        if key not in j["keys"]:
            j["keys"].add(key)
            j["sf"][sf] += 1
            if nonce and len(nonce) == 4:
                j["nonces"].append((ts, int.from_bytes(bytes.fromhex(nonce), "little")))
        j["first"], j["last"] = min(j["first"], ts), max(j["last"], ts)
        if ts == j["last"]:
            j["join_eui"] = join_eui
        j["gateways"][gw] = _best(j["gateways"].get(gw), rssi)
    out = []
    for j in joins.values():
        nonces = None
        if j["nonces"]:
            ordered = [n for _, n in sorted(j["nonces"])]
            nonces = {"first": ordered[0], "last": ordered[-1], "small": 0, "big": 0}
            for before, after in zip(ordered, ordered[1:]):
                _nonce_step(nonces, before, after)
        out.append({"dev_eui": j["dev_eui"], "join_eui": j["join_eui"], "requests": len(j["keys"]), "first": j["first"],
                    "last": j["last"], "gateways": j["gateways"], "sf": {str(sf): n for sf, n in j["sf"].items()},
                    "nonces": nonces})
    return out


def joins_report(source: Source, first: date, last: date, limit: int = 300) -> dict:
    """Devices asking to join (OTAA): their DevEUI and JoinEUI travel in clear."""
    merged: dict[str, dict] = {}
    for _, groups in source.per_day(first, last, "joins", one_query(JOINS, join_groups)):
        for j in groups:
            m = merged.get(j["dev_eui"])
            if m is None:
                merged[j["dev_eui"]] = {**j, "gateways": dict(j["gateways"]), "sf": dict(j["sf"]),
                                        "nonces": dict(j["nonces"]) if j["nonces"] else None}
                continue
            if j["nonces"]:  # days come in order: this day's first request follows the last one before
                if m["nonces"]:
                    _nonce_step(m["nonces"], m["nonces"]["last"], j["nonces"]["first"])
                    m["nonces"].update(last=j["nonces"]["last"], small=m["nonces"]["small"] + j["nonces"]["small"],
                                       big=m["nonces"]["big"] + j["nonces"]["big"])
                else:
                    m["nonces"] = dict(j["nonces"])
            m["requests"] += j["requests"]
            if j["last"] > m["last"]:
                m["last"], m["join_eui"] = j["last"], j["join_eui"]
            m["first"] = min(m["first"], j["first"])
            for gw, rssi in j["gateways"].items():
                m["gateways"][gw] = _best(m["gateways"].get(gw), rssi)
            for sf, n in j["sf"].items():
                m["sf"][sf] = m["sf"].get(sf, 0) + n
    out = sorted(merged.values(), key=lambda j: (-j["requests"], j["dev_eui"]))
    for j in out:
        nonces = j.pop("nonces")
        j["dev_nonce"] = None if nonces is None else {"first": nonces["first"], "last": nonces["last"],
                                                      "kind": _nonce_kind(nonces)}
    return {**_meta(source, first, last), "total": len(out), "joins": out[:limit]}


def _merge_group(g: dict, other: dict) -> None:
    g["frames"] += other["frames"]
    g["first"], g["last"] = min(g["first"], other["first"]), max(g["last"], other["last"])
    g["size_min"], g["size_max"] = min(g["size_min"], other["size_min"]), max(g["size_max"], other["size_max"])
    g["channels"] = sorted(set(g["channels"]) | set(other["channels"]))
    for gw, (n, rssi) in other["gateways"].items():
        mine = g["gateways"].setdefault(gw, [0, None])
        mine[0] += n
        mine[1] = _best(mine[1], rssi)
    if "packets" in other:  # a whole day (one day's per-gateway parts have no count of their own)
        g["packets"] += other["packets"]
        for hour, n in other["hours"].items():
            g["hours"][hour] = g["hours"].get(hour, 0) + n


def _group_key(sf, head: str | None) -> tuple[tuple, tuple[str, bool] | None]:
    """The system a packet belongs to: its readable header (plain or XOR 0x85) when it has one,
    else its first 3 bytes, per spreading factor. Also the header as text, if readable."""
    raw = bytes.fromhex(head or "")
    readable = readable_header(raw)
    return (sf, raw.hex() if readable else raw[:3].hex()), readable


def count_packets(rows) -> dict[tuple, Counter]:
    """Transmissions per system and UTC hour, from OTHER_RX rows (receptions in arrival order). The
    same bytes at another gateway within SAME_PACKET_S of the first reception are the same
    transmission; at the same gateway again, a new one: some systems repeat identical packets,
    a fraction of a second apart. Only the last SAME_PACKET_S of receptions are kept in memory."""
    window = SAME_PACKET_S / 86400  # in days, like julianday()
    recent: dict[str, tuple[float, set]] = {}  # frame_key -> (first reception, gateways that heard it)
    started: deque = deque()  # (first reception, frame_key) in arrival order, to forget them again
    groups: dict[tuple, tuple] = {}
    counts: dict[tuple, Counter] = defaultdict(Counter)
    for key, t, gw, sf, head, hour in rows:
        if t is not None:
            while started and t - started[0][0] >= window:
                first, old = started.popleft()
                if recent.get(old, (None,))[0] == first:
                    del recent[old]
            heard = recent.get(key)
            if heard is not None and gw not in heard[1] and t - heard[0] < window:
                heard[1].add(gw)
                continue
            recent[key] = (t, {gw})
            started.append((t, key))
        group = groups.get((sf, head))
        if group is None:
            group = groups[(sf, head)] = _group_key(sf, head)[0]
        counts[group][hour] += 1
    return counts


def other_groups(rows, receptions, limit: int = 100) -> list[dict]:
    """One day's traffic that is not LoRaWAN, grouped by system (see _group_key): from the OTHER
    rows, plus the transmissions per UTC hour counted from the OTHER_RX `receptions`. `frames`
    counts receptions (a packet two gateways heard twice), `packets` transmissions."""
    groups: dict[tuple, dict] = {}
    for sf, head, gw, freq, n, first, last, size_min, size_max, rssi, example in rows:
        key, readable = _group_key(sf, head)
        part = {"frames": n, "first": first, "last": last, "size_min": size_min, "size_max": size_max,
                "channels": [freq] if freq else [], "gateways": {gw: [n, rssi]}}
        if key in groups:
            _merge_group(groups[key], part)
        else:
            groups[key] = {"sf": sf, "head": key[1], "text": readable[0] if readable else None,
                           "xor85": bool(readable and readable[1]), "example": example, **part}
    counts = count_packets(receptions)
    for key, g in groups.items():
        hours = counts.get(key, {})
        g["packets"], g["hours"] = sum(hours.values()), dict(sorted(hours.items()))
    return sorted(groups.values(), key=lambda g: (-g["packets"], -g["frames"]))[:limit]


def _other_day(run) -> list[dict]:
    rows = list(run(OTHER))  # one cursor at a time
    return other_groups(rows, run(OTHER_RX))


def other_report(source: Source, first: date, last: date, tz: ZoneInfo, now: datetime, limit: int = 100) -> dict:
    """The other systems over a range: each day's groups (top 100 per day) merged, with their
    packets per hour (up to a week) or per day, on the same buckets for all of them."""
    merged: dict[tuple, dict] = {}
    for day, groups in source.per_day(first, last, "other", _other_day):
        for g in groups:
            key = (g["sf"], g["head"])
            if key in merged:
                _merge_group(merged[key], g)
            else:  # the example packet: the first day's, with its day to find it again
                merged[key] = {**g, "example": [day.isoformat(), g["example"]], "channels": list(g["channels"]),
                               "gateways": {gw: list(v) for gw, v in g["gateways"].items()}, "hours": dict(g["hours"])}
    out = sorted(merged.values(), key=lambda g: (-g["packets"], -g["frames"]))[:limit]
    hourly, starts, index = _buckets(first, last, tz, now)
    for g in out:
        series = [0] * len(starts)
        for hour, n in g.pop("hours").items():  # UTC hours: in whole-hour time zones, inside one bucket
            i = index(hour + ":00:00+00:00")
            if 0 <= i < len(series):
                series[i] += n
        g["series"] = series
    return {**_meta(source, first, last), "total": len(merged), "hourly": hourly,
            "buckets": [{"t": s.isoformat()} for s in starts], "other": out}


def _buckets(first: date, last: date, tz: ZoneInfo, now: datetime
             ) -> tuple[bool, list[datetime], Callable[[str], int]]:
    """Hours for up to a week, else local days; none after `now`."""
    hourly = (last - first).days < 7
    if hourly:
        start = datetime.combine(first, time(0), tz).astimezone(UTC)
        stop = min(datetime.combine(last + timedelta(days=1), time(0), tz).astimezone(UTC), now)
        count = max(math.ceil((stop - start).total_seconds() / 3600), 1)
        starts = [start + timedelta(hours=i) for i in range(count)]

        def index(ts: str) -> int:
            return int((datetime.fromisoformat(ts) - start).total_seconds() // 3600)
    else:
        days = (min(last, now.astimezone(tz).date()) - first).days + 1
        starts = [datetime.combine(first + timedelta(days=i), time(0), tz) for i in range(max(days, 1))]

        def index(ts: str) -> int:
            return (datetime.fromisoformat(ts).astimezone(tz).date() - first).days
    return hourly, starts, index


def device_report(source: Source, dev: str, first: date, last: date, tz: ZoneInfo, own: frozenset[str],
                  now: datetime) -> dict | None:
    """One device in detail: totals, a series per hour (up to a week) or per day, battery, every
    MAC command, the latest packets and the downlinks to it that a gateway overheard."""
    ups = sorted(_collect(source.rows(first, last, UPLINKS + " AND dev_addr = ?", (dev,))).get(dev, {}).values(),
                 key=lambda u: u.t)
    downlinks: dict[str, dict] = {}
    events = []
    for day, (ts, gw, fcnt, fopts, freq, sf, rssi, snr, mtype, key, port, size, ack, pending) in source.rows(
            first, last, DOWNLINKS, (dev,)):
        d = downlinks.get(key)
        if d is None:
            cmds = decode_mac(bytes.fromhex(fopts), uplink=False) if fopts else []
            d = downlinks[key] = {"t": ts, "fcnt": fcnt, "freq": freq, "sf": sf, "port": port, "size": size,
                                  "confirmed": mtype == "ConfirmedDataDown", "ack": bool(ack), "pending": bool(pending),
                                  "mac": [c["name"] for c in cmds], "day": day.isoformat(), "key": key,
                                  "gateways": []}
            events += [{"t": ts, "dir": "down", "fcnt": fcnt, "day": day.isoformat(), "key": key, **c} for c in cmds]
        d["gateways"].append([gw, rssi, snr])
    if not ups and not downlinks:
        return None

    gaps = source.silences(first, last)
    hourly, starts, index = _buckets(first, last, tz, now)
    buckets = [{"t": s.isoformat(), "heard": 0, "lost": 0, "rssi": defaultdict(list)} for s in starts]
    previous = None
    for u in ups:
        i = index(u.t)
        if 0 <= i < len(buckets):
            bucket = buckets[i]
            bucket["heard"] += 1
            for gw, (_, rssi, _snr) in u.gateways.items():
                if rssi is not None:
                    bucket["rssi"][gw].append(rssi)
            step = store.fcnt_step(previous.fcnt, u.fcnt) if previous is not None else 0
            if 0 < step < 1000 and not store.unrecorded(gaps, previous.t, u.t):
                bucket["lost"] += step - 1
        previous = u
    for bucket in buckets:
        bucket["rssi"] = {gw: round(statistics.median(v), 1) for gw, v in bucket["rssi"].items()}

    for u in ups:
        if u.fopts:
            for cmd in decode_mac(bytes.fromhex(u.fopts), uplink=True):
                events.append({"t": u.t, "dir": "up", "fcnt": u.fcnt, "day": u.day, "key": u.key, **cmd})
    events.sort(key=lambda e: e["t"], reverse=True)
    battery = [{"t": e["t"], "value": e["battery"], "text": battery_text(e["battery"]), "margin": e["margin"]}
               for e in reversed(events) if "battery" in e]
    packets = [{"t": u.t, "fcnt": u.fcnt, "port": u.port, "size": u.size, "sf": u.sf, "bw": u.bw, "freq": u.freq,
                "confirmed": u.confirmed, "adr": u.adr, "ack": u.ack, "sent": u.sent,
                "adr_ack_req": u.adr_ack_req, "class_b": u.class_b,
                "mac": [c["name"] for c in decode_mac(bytes.fromhex(u.fopts), True)] if u.fopts else [],
                "gateways": sorted(([gw, n, rssi, snr] for gw, (n, rssi, snr) in u.gateways.items()),
                                   key=lambda g: -(g[2] if g[2] is not None else -999)),
                "day": u.day, "key": u.key} for u in reversed(ups[-500:])]
    return {**_meta(source, first, last), "dev_addr": dev, "own": dev in own, "network": network(dev),
            "hourly": hourly, "summary": _summary(dev, ups, own, gaps) if ups else None, "buckets": buckets,
            "silences": gaps,
            "battery": battery, "mac": events[:1000], "mac_total": len(events), "packets": packets,
            "packets_total": len(ups), "downlinks": list(downlinks.values())[-300:][::-1],
            "downlinks_total": len(downlinks)}


def frame_report(source: Source, day: date, key: str) -> dict | None:
    """One packet: every reception of it that day and the byte-by-byte breakdown."""
    rows = [row for _, row in source.rows(day, day, FRAME, (key,))]
    if not rows:
        return None
    phy = bytes.fromhex(rows[0][9])
    receptions = [{"t": r[0], "gateway": r[1], "freq": r[2], "sf": r[3], "bw": r[4], "cr": r[5], "rssi": r[6],
                   "snr": r[7], "crc": r[8]} for r in rows]
    return {"day": day.isoformat(), "key": key, "hex": phy.hex(), "length": len(phy), "mtype": rows[0][10],
            "dev_addr": rows[0][11], "is_downlink": rows[0][12], "dev_eui": rows[0][13],
            "receptions": receptions, **breakdown(phy, rows[0][3])}
