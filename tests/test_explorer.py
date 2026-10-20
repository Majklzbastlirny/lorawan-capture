import json
import math
from datetime import datetime, timedelta

import pytest
from conftest import GATEWAYS, UTC, Clock, feed, frame, topic, up
from lorawan_capture import explorer, store
from lorawan_capture.explorer import Source

OWN, TTN = "00a1b2c3", "260b1234"
BASE = "ddb4b5b5a5b5b5b5b5b5b4" + "00" * 40  # "X100 000001" after XOR 0x85, like a scrambled proprietary radio
ROVER = "ddb4b5b5a5b5b5b5b5b5b7" + "11" * 60  # "X100 000002"
JOIN_DEV_EUI, JOIN_EUI = "1112131415161718", "0102030405060708"  # in the join_sensecap fixture


@pytest.fixture
def two_days(make_capture, payloads):
    """Yesterday (closed, summarised) and today, Prague time."""
    clock = Clock(datetime(2026, 10, 2, 10, 0, tzinfo=UTC))
    cap = make_capture(clock=clock)
    feed(cap, topic("sensecap"), up(frame(OWN, 90), rssi=-95))
    clock.advance(days=1)  # rotates: yesterday's file is closed and summarised
    for gw, rssi in (("sensecap", -99), ("home", -107), ("lab", -83)):  # battery 254, margin +5; ADR answer
        feed(cap, topic(gw), up(frame(OWN, 100, fopts="06fe05" + "0307"), rssi=rssi))
    clock.advance(seconds=90)
    # 101 and 102 lost; ADR on and ADRACKReq: no downlink for a long while
    feed(cap, topic("sensecap"), up(frame(OWN, 103, fctrl=0xC0), freq=867700000, rssi=-97))
    feed(cap, topic("home"), up(frame(OWN, 7, fopts="0353ff0001", port=None, mhdr=0x60, fctrl=0x90),  # FPending
                                freq=869525000, rssi=-60))
    for _ in range(2):  # sent twice (NbTrans 2): the same bytes reach the gateway twice; in class B
        feed(cap, topic("sensecap"), up(frame(TTN, 5, fctrl=0x90), sf=9, rssi=-120))
    feed(cap, topic("sensecap"), payloads["join_sensecap"])
    feed(cap, topic("sensecap"), up(BASE, freq=867900000, sf=5, rssi=-92))
    feed(cap, topic("home"), up(BASE, freq=867900000, sf=5, rssi=-110))
    feed(cap, topic("sensecap"), up(ROVER, freq=867900000, sf=5, rssi=-91))
    feed(cap, topic("sensecap"), payloads["p2p_sensecap"])
    return cap, clock


def test_mac_commands():
    up_cmds = explorer.decode_mac(bytes.fromhex("06fe05" + "0307" + "063e3e"), uplink=True)
    assert [(c["name"], c.get("battery"), c.get("margin")) for c in up_cmds] == [
        ("DevStatusAns", 254, 5), ("LinkADRAns", None, None), ("DevStatusAns", 62, -2)]
    assert up_cmds[0]["text"] == "battery 100 % (254/254), link margin +5 dB"
    assert up_cmds[1]["text"] == "power ok, data rate ok, channel mask ok"
    assert explorer.decode_mac(bytes.fromhex("0303"), True)[0]["text"] == "power REFUSED, data rate ok, channel mask ok"

    down = explorer.decode_mac(bytes.fromhex("0353ff0001"), uplink=False)[0]
    assert (down["name"], down["data_rate"], down["tx_power"]) == ("LinkADRReq", 5, 3)
    assert down["text"] == ("DR5 (SF7/125 kHz), TX power 3: 10 dBm EIRP, channels 0, 1, 2, 3, 4, 5, 6, 7 on, "
                            "each uplink sent 1x")
    keep = explorer.decode_mac(bytes.fromhex("03ff000061"), uplink=False)[0]  # 0xF: keep the current values
    assert "data_rate" not in keep and "tx_power" not in keep
    assert keep["text"].startswith("data rate unchanged, TX power unchanged, all channels on")
    assert explorer.decode_mac(bytes.fromhex("0702000000ff"), False)[0]["text"] == "removes channel 2"
    assert explorer.decode_mac(bytes.fromhex("0703184f8450"), False)[0]["text"] == "channel 3: 867.100 MHz, DR0-DR5"

    assert explorer.decode_mac(bytes.fromhex("0702"), True)[0]["text"] == "data rate range ok, frequency REFUSED"
    assert explorer.decode_mac(bytes.fromhex("ff0102"), True) == [
        {"name": "unknown 0xFF", "hex": "ff0102", "text": "cannot be read further (LoRaWAN 1.1 devices encrypt FOpts)"}]
    assert explorer.decode_mac(bytes.fromhex("06fe"), True)[0]["name"] == "unknown 0x06"  # too short


def test_networks_battery_airtime():
    assert explorer.network(OWN) == {"netid": "000000", "label": "private / experimental"}
    assert explorer.network(TTN) == {"netid": "000013", "label": "The Things Network"}
    assert explorer.network("27038e49")["label"] == "The Things Network"
    assert explorer.network("e0123456") == {"netid": None, "label": "NetID type 3, NwkID 9"}
    assert [explorer.battery_text(v) for v in (0, 1, 128, 254, 255)] == [
        "external power", "0 % (1/254)", "50 % (128/254)", "100 % (254/254)", "not measured"]
    # the airtime calculators' values: 13 bytes at SF7 46.3 ms, at SF12 (low data rate) 1155.1 ms
    assert round(explorer.airtime(7, 125000, 13) * 1000, 1) == 46.3
    assert round(explorer.airtime(12, 125000, 13) * 1000, 1) == 1155.1
    assert round(explorer.airtime(7, 125000, 17) * 1000, 1) == 51.5


def test_breakdown():
    b = explorer.breakdown(bytes.fromhex(frame(OWN, 100, fopts="06fe05")), sf=7)
    assert b["lorawan"] and not b["notes"]
    assert [(f["name"], f["hex"]) for f in b["fields"]] == [
        ("MHDR", "40"), ("DevAddr", "c3b2a100"), ("FCtrl", "83"), ("FCnt", "6400"), ("FOpts", "06fe05"),
        ("FPort", "01"), ("FRMPayload", "aabbccdd"), ("MIC", "11223344")]
    assert b["fields"][1]["text"].startswith("00a1b2c3") and b["fields"][2]["text"] == "ADR; 3 bytes of FOpts"
    assert b["mac"][0]["battery"] == 254

    down = explorer.breakdown(bytes.fromhex(frame(OWN, 7, fopts="0353ff0001", port=None, mhdr=0x60)), sf=7)
    assert down["mac"][0]["name"] == "LinkADRReq" and [f["name"] for f in down["fields"]][-1] == "MIC"

    join = explorer.breakdown(bytes.fromhex("0008070605040302011817161514131211abcd01020304"), sf=12)
    assert [(f["name"], f["text"]) for f in join["fields"][1:3]] == [("JoinEUI", JOIN_EUI), ("DevEUI", JOIN_DEV_EUI)]

    scrambled = explorer.breakdown(bytes.fromhex(BASE), sf=5)
    assert not scrambled["lorawan"] and len(scrambled["notes"]) == 2 and '"X100 000001"' in scrambled["notes"][1]
    assert explorer.breakdown(b"", sf=7)["notes"] == ["empty packet"]
    assert explorer.breakdown(bytes.fromhex("40"), sf=7)["fields"][0]["name"] == "MHDR"


def test_devices_over_two_days(two_days):
    cap, clock = two_days
    yesterday, today = cap.day - timedelta(days=1), cap.day
    source = Source(cap, cap.cfg.data_dir)
    report = explorer.devices_report(source, yesterday, today, cap.own())
    assert report["days"] == [yesterday.isoformat(), today.isoformat()] and report["skipped"] == []
    own, ttn = report["devices"]
    # frame counters 90 (yesterday), 100, 103: 14 sent, 3 heard
    assert (own["dev_addr"], own["own"], own["heard"], own["sent"], own["lost"]) == (OWN, True, 3, 14, 11)
    assert own["battery"]["value"] == 254 and own["battery"]["margin"] == 5
    assert own["mac"] == {"DevStatusAns": 1, "LinkADRAns": 1}
    assert own["gateways"][GATEWAYS["sensecap"]] == {"heard": 3, "rssi_median": -97, "rssi_max": -95,
                                                     "snr_median": 5.0}
    assert own["gateways"][GATEWAYS["lab"]]["heard"] == 1
    assert own["sf"] == {"7": 3} and own["ports"] == [(1, 3)]
    assert (own["adr_ack_req"], own["class_b"], ttn["adr_ack_req"], ttn["class_b"]) == (1, 0, 0, 1)
    assert (ttn["dev_addr"], ttn["network"]["label"], ttn["heard"], ttn["repeated"]) == (TTN, "The Things Network", 1, 1)
    assert ttn["airtime_s"] > 2 * explorer.airtime(9, 125000, 17) * 0.99  # both transmissions count

    joins = explorer.joins_report(Source(cap, cap.cfg.data_dir), yesterday, today)["joins"]
    assert [(j["dev_eui"], j["join_eui"], j["requests"]) for j in joins] == [(JOIN_DEV_EUI, JOIN_EUI, 1)]

    other = explorer.other_report(Source(cap, cap.cfg.data_dir), yesterday, today, cap.cfg.tz, clock())
    groups = {(g["sf"], g["text"] or g["head"]): g for g in other["other"]}
    base, rover = groups[(5, "X100 000001")], groups[(5, "X100 000002")]  # one radio system, two senders
    assert (base["frames"], base["xor85"], base["channels"], base["size_max"]) == (2, True, [867900000], 51)
    assert base["gateways"] == {GATEWAYS["sensecap"]: [1, -92], GATEWAYS["home"]: [1, -110]}
    assert base["packets"] == 1  # one transmission, two gateways heard it
    assert rover["frames"] == rover["packets"] == 1 and groups[(7, "d6a1b2")]["packets"] == 1
    # hours from yesterday 00:00 to today 12:01 (Prague); everything came in the last one
    assert other["hourly"] and len(other["buckets"]) == 37 and other["buckets"][0]["t"] == "2026-10-01T22:00:00+00:00"
    assert base["series"][-1] == sum(base["series"]) == 1


def test_other_systems_are_cached_with_a_closed_day(two_days):
    cap, _ = two_days
    yesterday = cap.day - timedelta(days=1)
    path = store.day_file(cap.cfg.data_dir, yesterday)
    conn = store.open_db(path)  # another system yesterday (11 bytes, not text either way)
    conn.execute(store.INSERT_FRAME, (
        "2026-10-02T11:00:00+00:00", None, "k" * 40, GATEWAYS["sensecap"], 0, "JoinRequest", 0, None, None, None,
        None, None, None, None, None, None, None, None, None, None, None, None, None, 11, "00" * 11,
        868300000, 6, 125000, "1", -100, 1.0, "CRC_OK"))
    conn.commit()
    conn.close()
    assert f"other_v{explorer.CACHE}" not in store.cached_summary(path)
    first = explorer.other_report(Source(cap, cap.cfg.data_dir), yesterday, yesterday, cap.cfg.tz, cap.clock())["other"]
    assert [(g["sf"], g["head"], g["example"]) for g in first] == [(6, "000000", [yesterday.isoformat(), "k" * 40])]
    assert first[0]["series"][13] == 1  # 13:00-14:00 in Prague
    cached = store.cached_summary(path)[f"other_v{explorer.CACHE}"]  # worked out once, then kept with the summary
    assert [(g["head"], g["frames"], g["packets"], g["hours"], g["example"]) for g in cached] == [
        ("000000", 1, 1, {"2026-10-02T11": 1}, "k" * 40)]
    again = explorer.other_report(Source(cap, cap.cfg.data_dir), yesterday, yesterday, cap.cfg.tz, cap.clock())["other"]
    assert again == first


def test_counting_transmissions():
    """The same bytes at another gateway: one transmission. At the same gateway again: another one."""
    s, head, hour = 1 / 86400, "d6a1b2" + "00" * 8, "2026-10-04T10"
    rows = [("k1", 0.0, "A", 7, head, hour), ("k1", 0.1 * s, "B", 7, head, hour),  # heard twice
            ("k1", 0.3 * s, "A", 7, head, hour),  # sent again, a moment later
            ("k1", 1.0 * s, "B", 7, head, hour),  # ... the other gateway hears that one too
            ("k1", 1.2 * s, "B", 7, head, hour),  # B again: a third transmission, though within 1.5 s
            ("k2", None, "A", 7, head, "2026-10-04T11")]  # no time to compare: counted as it comes
    assert explorer.count_packets(rows) == {(7, "d6a1b2"): {"2026-10-04T10": 3, "2026-10-04T11": 1}}


def test_other_packets_per_hour(make_capture):
    clock = Clock(datetime(2026, 10, 4, 10, 0, tzinfo=UTC))  # 12:00 in Prague
    cap = make_capture(clock=clock)
    pair = "d6a1b20dc3d40102"  # like the 8-byte packets of the pair on 868.1 MHz
    for gw in ("sensecap", "home"):
        feed(cap, topic(gw), up(pair, rssi=-110))
    clock.advance(milliseconds=500)
    feed(cap, topic("sensecap"), up(pair, rssi=-110))  # identical bytes again: a second transmission
    clock.advance(minutes=3)
    feed(cap, topic("sensecap"), up(pair, rssi=-110))
    clock.advance(hours=1)
    feed(cap, topic("home"), up(pair, rssi=-120))
    day = cap.day
    report = explorer.other_report(Source(cap, cap.cfg.data_dir), day, day, cap.cfg.tz, clock())
    (g,) = report["other"]
    assert (g["head"], g["frames"], g["packets"]) == ("d6a1b2", 5, 4)
    assert report["hourly"] and len(report["buckets"]) == 14  # 00:00 to 13:03 in Prague, nothing after now
    assert report["buckets"][12]["t"] == "2026-10-04T10:00:00+00:00" and g["series"][12:] == [3, 1]
    week = explorer.other_report(Source(cap, cap.cfg.data_dir), day - timedelta(days=7), day, cap.cfg.tz, clock())
    assert not week["hourly"] and len(week["buckets"]) == 8 and week["other"][0]["series"] == [0] * 7 + [4]


def test_device_detail(two_days):
    cap, clock = two_days
    yesterday, today = cap.day - timedelta(days=1), cap.day
    d = explorer.device_report(Source(cap, cap.cfg.data_dir), OWN, yesterday, today, cap.cfg.tz, cap.own(), clock())
    assert d["hourly"] and d["summary"]["sent"] == 14
    start = datetime(2026, 10, 1, 22, 0, tzinfo=UTC)  # local midnight starting 2026-10-02
    assert len(d["buckets"]) == math.ceil((clock() - start).total_seconds() / 3600)
    assert sum(b["heard"] for b in d["buckets"]) == 3 and sum(b["lost"] for b in d["buckets"]) == 11
    hour = next(b for b in d["buckets"] if b["heard"] == 2)  # 100 and 103 in the same hour
    assert hour["lost"] == 11 and hour["rssi"][GATEWAYS["lab"]] == -83

    assert [p["fcnt"] for p in d["packets"]] == [103, 100, 90]  # newest first
    assert [(p["adr_ack_req"], p["class_b"]) for p in d["packets"]] == [(True, False), (False, False), (False, False)]
    assert d["packets"][1]["gateways"][0] == [GATEWAYS["lab"], 1, -83, 5.0]  # best signal first
    assert d["packets"][1]["mac"] == ["DevStatusAns", "LinkADRAns"]
    assert [(e["dir"], e["name"]) for e in d["mac"]] == [("down", "LinkADRReq"), ("up", "DevStatusAns"),
                                                         ("up", "LinkADRAns")]
    assert d["battery"] == [{"t": d["packets"][1]["t"], "value": 254, "text": "100 % (254/254)", "margin": 5}]
    (down,) = d["downlinks"]
    assert (down["fcnt"], down["mac"], down["gateways"]) == (7, ["LinkADRReq"], [[GATEWAYS["home"], -60, 5.0]])
    assert down["pending"] and not down["ack"]

    week = explorer.device_report(Source(cap, cap.cfg.data_dir), OWN, today - timedelta(days=9), today,
                                  cap.cfg.tz, cap.own(), clock())
    assert not week["hourly"] and len(week["buckets"]) == 10  # daily from 7 days on
    assert [b["heard"] for b in week["buckets"]][-2:] == [1, 2]
    assert explorer.device_report(Source(cap, cap.cfg.data_dir), "deadbeef", yesterday, today,
                                  cap.cfg.tz, cap.own(), clock()) is None


def test_packet_inspector(two_days):
    cap, _ = two_days
    key = explorer.device_report(Source(cap, cap.cfg.data_dir), OWN, cap.day, cap.day, cap.cfg.tz, cap.own(),
                                 cap.clock())["packets"][1]["key"]
    f = explorer.frame_report(Source(cap, cap.cfg.data_dir), cap.day, key)
    assert (f["dev_addr"], f["mtype"], f["length"], f["lorawan"]) == (OWN, "UnconfirmedDataUp", 22, True)
    assert sorted(r["gateway"] for r in f["receptions"]) == sorted(GATEWAYS.values())
    assert f["mac"][0]["name"] == "DevStatusAns"
    assert explorer.frame_report(Source(cap, cap.cfg.data_dir), cap.day, "0" * 40) is None


def test_unreadable_days_are_skipped(two_days):
    cap, _ = two_days
    yesterday = cap.day - timedelta(days=1)
    store.day_file(cap.cfg.data_dir, yesterday).write_bytes(b"not a database at all, just junk" * 100)
    report = explorer.devices_report(Source(cap, cap.cfg.data_dir), yesterday, cap.day, cap.own())
    assert report["skipped"] == [yesterday.isoformat()] and report["days"] == [cap.day.isoformat()]
    assert report["devices"][0]["heard"] == 2


def test_silences_and_sent_counting():
    def t(minute):
        return f"2026-10-03T10:{minute:02d}:00+00:00"

    times = [t(m) for m in range(10)] + [t(m) for m in range(20, 30)]  # nothing from 10:09 to 10:20
    info = store.silences(times)
    assert info == {"first": t(0), "last": t(29), "gaps": [[t(9), t(20)]]}
    assert store.silences(times[:5]) == {"first": None, "last": None, "gaps": []}  # too few to tell
    # counter 1, 2, 5 (3 and 4 not heard), then 30 at 10:25, after the silence: 6-29 unrecorded
    steps = [(1, t(1)), (2, t(2)), (5, t(5)), (30, t(25))]
    assert store.count_sent(steps, info["gaps"]) == (6, 24)
    assert store.count_sent(steps, []) == (30, 0)
    # heard 30 s after the last report and 30 s before the next: only 9 s of silence in between
    assert not store.unrecorded(info["gaps"], "2026-10-03T10:09:30+00:00", "2026-10-03T10:09:39+00:00")
    assert store.unrecorded(info["gaps"], "2026-10-03T10:09:30+00:00", "2026-10-03T10:19:30+00:00")


def test_time_without_recording_is_not_lost(make_capture):
    clock = Clock(datetime(2026, 10, 3, 8, 0, tzinfo=UTC))
    cap = make_capture(clock=clock)

    def reports(minutes):  # gateway stats every 30 s
        for _ in range(minutes * 2):
            feed(cap, topic("sensecap", "event/stats"), json.dumps({"rxPacketsReceived": 1}).encode())
            clock.advance(seconds=30)

    feed(cap, topic("sensecap"), up(frame(OWN, 10)))
    reports(10)
    feed(cap, topic("sensecap"), up(frame(OWN, 15)))  # 11-14 not heard while recording: lost
    clock.advance(hours=1)                            # the logger was down for an hour
    reports(10)
    feed(cap, topic("sensecap"), up(frame(OWN, 140)))  # 16-139 sent while nothing was recorded
    own = explorer.devices_report(Source(cap, cap.cfg.data_dir), cap.day, cap.day, cap.own())["devices"][0]
    assert (own["heard"], own["sent"], own["lost"], own["unrecorded"]) == (3, 7, 4, 124)
    day = cap.live_summary()["own"][OWN]
    assert (day["heard"], day["expected"], day["unrecorded"]) == (3, 7, 124)
    detail = explorer.device_report(Source(cap, cap.cfg.data_dir), OWN, cap.day, cap.day, cap.cfg.tz, cap.own(), clock())
    assert sum(b["lost"] for b in detail["buckets"]) == 4 and detail["summary"]["unrecorded"] == 124
    assert len(detail["silences"]) == 1


def join(dev_eui: str, nonce: int) -> str:
    """A join request (hex): JoinEUI, DevEUI and DevNonce little-endian."""
    return ("00" + bytes.fromhex(JOIN_EUI)[::-1].hex() + bytes.fromhex(dev_eui)[::-1].hex()
            + nonce.to_bytes(2, "little").hex() + "01020304")


def test_join_nonces(make_capture):
    clock = Clock(datetime(2026, 10, 2, 10, 0, tzinfo=UTC))
    cap = make_capture(clock=clock)
    counting, scattered, once = "a000000000000001", "a000000000000002", "a000000000000003"

    def send(dev_eui, nonce):
        feed(cap, topic("sensecap"), up(join(dev_eui, nonce), sf=12))
        clock.advance(seconds=20)

    for nonce in (100, 101, 105):  # LoRaWAN 1.0.4 / 1.1: counts the attempts (102-104 not heard)
        send(counting, nonce)
    for nonce in (40000, 1200):  # 1.0.0-1.0.3: random
        send(scattered, nonce)
    clock.advance(days=1)  # yesterday closes
    send(counting, 106)
    send(scattered, 33000)
    send(once, 7)
    yesterday = cap.day - timedelta(days=1)
    report = explorer.joins_report(Source(cap, cap.cfg.data_dir), yesterday, cap.day)
    assert {j["dev_eui"]: (j["requests"], j["dev_nonce"]) for j in report["joins"]} == {
        counting: (4, {"first": 100, "last": 106, "kind": "counter"}),
        scattered: (3, {"first": 40000, "last": 33000, "kind": "random"}),
        once: (1, {"first": 7, "last": 7, "kind": None})}
    cached = store.cached_summary(store.day_file(cap.cfg.data_dir, yesterday))[f"joins_v{explorer.CACHE}"]
    assert sorted(j["dev_eui"] for j in cached) == [counting, scattered]  # kept with the closed day
    assert explorer.joins_report(Source(cap, cap.cfg.data_dir), yesterday, cap.day)["joins"] == report["joins"]
