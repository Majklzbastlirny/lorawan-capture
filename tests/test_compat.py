"""The daily files must hold exactly what samples/lorawan_logger.py stored, column by column."""

import hashlib
import importlib.util
from pathlib import Path

import pytest
from conftest import GATEWAYS, feed, topic

OLD_LOGGER = Path(__file__).resolve().parents[2] / "samples" / "lorawan_logger.py"
pytestmark = pytest.mark.skipif(
    not OLD_LOGGER.exists() or importlib.util.find_spec("chirpstack_api") is None,
    reason="needs samples/lorawan_logger.py and chirpstack-api")


def old_row(payload: bytes, gateway_id: str) -> tuple:
    """What the old logger inserted (minus id and ts_utc), using its own code and decoder."""
    spec = importlib.util.spec_from_file_location("old_logger", OLD_LOGGER)
    old = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(old)
    from chirpstack_api import gw

    uf = gw.UplinkFrame()
    uf.ParseFromString(payload)
    phy = uf.phy_payload
    o = old.parse_phy(phy)
    m = uf.tx_info.modulation
    sf = bwv = None
    cr = ""
    if m.HasField("lora"):
        sf, bwv, cr = m.lora.spreading_factor, m.lora.bandwidth, str(m.lora.code_rate)
    gwt = uf.rx_info.gw_time.ToDatetime().isoformat() if uf.rx_info.HasField("gw_time") else None
    return (gwt, hashlib.sha1(phy).hexdigest(), gateway_id, 1 if o.get("dev_addr") == "00a1b2c3" else 0,
            o.get("mtype"), o.get("major"), o.get("is_downlink"), o.get("dev_addr"), o.get("nwk_id"),
            o.get("join_eui"), o.get("dev_eui"), o.get("fcnt"), o.get("fport"), o.get("adr"), o.get("ack"),
            o.get("adr_ack_req"), o.get("class_b"), o.get("fopts_len"), o.get("fopts_hex"), o.get("frm_len"),
            o.get("frm_hex"), o.get("mic_hex"), len(phy), phy.hex(), uf.tx_info.frequency, sf, bwv, cr,
            uf.rx_info.rssi, uf.rx_info.snr, old.CRC.get(uf.rx_info.crc_status))


def with_gw_time(payload: bytes) -> bytes:
    from chirpstack_api import gw

    uf = gw.UplinkFrame()
    uf.ParseFromString(payload)
    uf.rx_info.gw_time.seconds = 1_759_303_333
    uf.rx_info.gw_time.nanos = 123_456_000
    return uf.SerializeToString()


def test_rows_match_the_old_logger(make_capture, payloads):
    cap = make_capture()
    cases = [(name, payloads[name]) for name in payloads
             if name.startswith(("own_", "flood_", "p2p_", "join_")) and not name.endswith("_json")]
    cases.append(("own_100_home_gwtime", with_gw_time(payloads["own_100_home"])))
    assert len(cases) >= 10
    for name, payload in cases:
        gateway_id = GATEWAYS[[g for g in GATEWAYS if f"_{g}" in name][0]]
        feed(cap, f"eu868/gateway/{gateway_id}/event/up", payload)
        new = cap.db.execute("SELECT * FROM frames ORDER BY id DESC LIMIT 1").fetchone()[2:]  # skip id, ts_utc
        expected = old_row(payload, gateway_id)
        assert new[:-2] == expected[:-2], name
        assert new[-2] == pytest.approx(expected[-2], abs=1e-6), name  # SNR: float32 vs rounded
        assert new[-1] == expected[-1], name


def test_schema_matches_the_old_logger(make_capture, tmp_path):
    import sqlite3

    spec = importlib.util.spec_from_file_location("old_logger", OLD_LOGGER)
    old = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(old)
    ref = sqlite3.connect(tmp_path / "old.db")
    ref.executescript(old.DDL)
    cap = make_capture()
    for table in ("frames", "stats"):
        cols_old = ref.execute(f"PRAGMA table_info({table})").fetchall()
        cols_new = cap.db.execute(f"PRAGMA table_info({table})").fetchall()
        assert cols_new == cols_old
    idx = "SELECT name, sql FROM sqlite_master WHERE type='index' ORDER BY name"
    assert cap.db.execute(idx).fetchall() == ref.execute(idx).fetchall()
    ref.close()


def test_json_payloads_are_stored_too(make_capture, payloads):
    """The old logger could only read protobuf; JSON marshaling gives the same row."""
    cap = make_capture()
    feed(cap, topic("sensecap"), payloads["own_103_sensecap_json"])
    row = cap.db.execute("SELECT dev_addr, fcnt, freq_hz, sf, code_rate, rssi, snr, crc, is_own FROM frames").fetchone()
    assert row == ("00a1b2c3", 103, 867_700_000, 7, "1", -97, 11.0, "CRC_OK", 1)
