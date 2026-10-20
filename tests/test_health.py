import base64
import json
import threading
import time
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from conftest import GATEWAYS, UTC, Clock, feed, frame, topic, up
from lorawan_capture import health, store
from lorawan_capture.explorer import Source

OWN = "00a1b2c3"


class Alive:
    """Stands in for a running writer thread."""

    @staticmethod
    def is_alive() -> bool:
        return True

    @staticmethod
    def join(timeout=None) -> None:
        pass


@pytest.fixture
def running(make_capture):
    """A capture whose writer counts as running, on a fake clock (12:00 in Prague)."""
    def factory(start=datetime(2026, 10, 4, 10, 0, tzinfo=UTC), **env):
        clock = Clock(start)
        cap = make_capture(clock=clock, **env)
        cap.writer = Alive()
        return cap, clock
    return factory


def state_of(result, name):
    return next((i["state"], i["text"]) for i in result["items"] if i["name"] == name)


def test_silence_after_a_grace_period(running, payloads):
    cap, clock = running()
    result = health.check(cap, cap.cfg)
    assert result["ok"] and state_of(result, "Messages") == ("good", "starting, none yet")
    assert state_of(result, "Broker")[0] == "warning"  # not connected in tests: a warning, not a problem
    clock.advance(minutes=11)
    assert health.check(cap, cap.cfg)["problems"] == ["Messages: none since the start 11 min ago"]
    feed(cap, topic("sensecap"), payloads["own_100_sensecap"])
    assert health.check(cap, cap.cfg)["ok"]
    clock.advance(minutes=10, seconds=30)
    assert health.check(cap, cap.cfg)["problems"] == ["Messages: none for 10 min"]
    cap.writer = None
    assert "Writer: stopped: nothing is being stored (see the container log)" in health.check(cap, cap.cfg)["problems"]


def test_disk_and_subscriptions(running, payloads, monkeypatch):
    cap, _ = running()
    feed(cap, topic("sensecap"), payloads["own_100_sensecap"])
    monkeypatch.setattr(health, "disk_usage", lambda path: SimpleNamespace(free=500 << 20))
    assert health.check(cap, cap.cfg)["problems"] == ["Disk: 500 MB free"]
    monkeypatch.setattr(health, "disk_usage", lambda path: SimpleNamespace(free=5 << 30))
    result = health.check(cap, cap.cfg)
    assert result["ok"] and "Disk: 5 GB free" in result["warnings"]
    cap.mqtt["subscriptions"] = {"eu868/gateway/+/event/up": "refused (Not authorized)",
                                 "eu868/gateway/+/event/stats": "ok", "eu868/gateway/+/state/conn": "refused (x)"}
    result = health.check(cap, cap.cfg)
    assert result["problems"] == [  # stats still arrive, but no uplink is ever recorded
        "Subscription: eu868/gateway/+/event/up refused (Not authorized) by the broker (its ACL)"]
    assert "Subscription: eu868/gateway/+/state/conn refused (x) by the broker (its ACL)" in result["warnings"]


def test_gateways_are_warnings_not_problems(running, payloads):
    cap, clock = running(GATEWAY_NAMES=f"{GATEWAYS['sensecap']}=SenseCAP")
    feed(cap, topic("sensecap"), payloads["own_100_sensecap"])
    cap.registry.seen(GATEWAYS["home"], (clock() - timedelta(days=8)).isoformat(timespec="seconds"))
    clock.advance(minutes=9)
    feed(cap, topic("lab", "event/stats"), payloads["stats_lab_json"])  # keeps the capture busy
    clock.advance(minutes=3)
    result = health.check(cap, cap.cfg)
    assert result["ok"]
    gateways = {g["id"]: (g["state"], g["text"]) for g in result["gateways"]}
    assert gateways[GATEWAYS["sensecap"]] == ("warning", "silent for 12 min")
    assert gateways[GATEWAYS["home"]] == ("info", "silent since 2026-09-26 (no warning after a week)")
    assert gateways[GATEWAYS["lab"]] == ("good", "last heard 3 min ago")
    assert "Gateway SenseCAP: silent for 12 min" in result["warnings"]


def test_own_device_overdue_survives_a_restart(running, make_capture):
    cap, clock = running()
    for fcnt in range(100, 106):  # every 30 s, heard by two gateways each
        for gw in ("sensecap", "home"):
            feed(cap, topic(gw), up(frame(OWN, fcnt)))
        clock.advance(seconds=30)
    cap.own_devices.put(OWN, "UART node")
    (own,) = health.check(cap, cap.cfg)["own"]
    assert own["state"] == "good" and own["typical_s"] == 30 and own["text"] == "last heard 30 s ago, usually every 30 s"
    clock.advance(minutes=16)
    feed(cap, topic("lab", "event/stats"), json.dumps({"rxPacketsReceived": 1}).encode())
    result = health.check(cap, cap.cfg)
    assert result["ok"] and "UART node: not heard for 16 min, usually every 30 s" in result["warnings"]

    again = make_capture(clock=clock)  # a restart: the new process learns it from the file
    again.writer = Alive()
    (own,) = health.check(again, again.cfg)["own"]
    assert (own["state"], own["typical_s"]) == ("warning", 30)


def test_busy_channels(running, monkeypatch):
    cap, clock = running(GATEWAY_NAMES=f"{GATEWAYS['sensecap']}=SenseCAP")
    clock.advance(minutes=30)  # 12:30 in Prague
    occupancy = {GATEWAYS["sensecap"]: {"867900000": [[11, 1.0, 0.0, 300.0, 0.0], [12, 0.0, 10.0, 0.0, 0.0]],
                                        "868100000": [[11, 900.0, 0.0, 0.0, 0.0]]},  # busy, but with LoRaWAN
                 GATEWAYS["home"]: {"867900000": [[11, 0.0, 0.0, 200.0, 0.0]]}}
    monkeypatch.setattr(cap, "live_summary", lambda: {"occupancy": occupancy, "hour_seconds": [3600] * 24})
    assert health.busy_channels(cap, clock(), cap.live_summary()) == [{
        "freq": 867900000, "hour": 11, "share": 0.0833, "gateway": GATEWAYS["sensecap"],
        "text": "867.900 MHz: 8 % of the hour from 11:00 on air with packets that are not LoRaWAN (heard at SenseCAP)"}]
    assert health.check(cap, cap.cfg)["channels"] == []  # no recent summary: the status does not make one
    assert len(health.check(cap, cap.cfg, full=True)["channels"]) == 1
    cap._live = (time.monotonic(), cap.live_summary())  # made a moment ago, e.g. for the Overview
    assert len(health.check(cap, cap.cfg)["channels"]) == 1


def test_occupancy_in_the_summary(make_capture):
    cap = make_capture(clock=Clock(datetime(2026, 10, 4, 10, 0, tzinfo=UTC)))
    for fcnt in (1, 2, 3):  # 17 bytes at SF7: 51.456 ms each
        feed(cap, topic("sensecap"), up(frame(OWN, fcnt)))
    flood = {"phyPayload": base64.b64encode(bytes.fromhex("ddb4b5b5a5b5b5b5b5b5b4" + "00" * 241)).decode(),
             "txInfo": {"frequency": 867900000, "modulation": {"lora": {"bandwidth": 125000, "spreadingFactor": 5,
                                                                         "codeRate": "CR_4_6"}}},
             "rxInfo": {"rssi": -90, "snr": 5.0, "crcStatus": "CRC_OK"}}
    feed(cap, topic("sensecap"), json.dumps(flood).encode())  # 252 bytes at SF5, 4/6: 634.25 symbols
    s = cap.live_summary()
    assert s["occupancy"][GATEWAYS["sensecap"]] == {
        "867900000": [[12, 0.0, 0.0, round(634.25 * 32 / 125000, 3), 0.0]],
        "868100000": [[12, round(3 * 0.051456, 3), 0.0, 0.0, 0.0]]}
    assert s["hour_seconds"] == [3600] * 24
    assert round(store.airtime(5, 125000, 252, 2), 6) == 0.162368


def test_hour_lengths_around_daylight_saving():
    prague = ZoneInfo("Europe/Prague")
    autumn, spring = store.hour_seconds(date(2026, 10, 25), prague), store.hour_seconds(date(2026, 3, 29), prague)
    assert autumn[2] == 7200 and sum(autumn) == 25 * 3600  # 02:00-03:00 happens twice
    assert spring[2] == 0 and sum(spring) == 23 * 3600  # and once not at all


def test_recent_days(running):
    cap, clock = running(start=datetime(2026, 10, 2, 8, 0, tzinfo=UTC))

    def reports(minutes):
        for _ in range(minutes * 2):
            feed(cap, topic("sensecap", "event/stats"), json.dumps({"rxPacketsReceived": 1}).encode())
            clock.advance(seconds=30)

    store.open_db(store.day_file(cap.cfg.data_dir, date(2026, 9, 30))).close()  # recorded, but nothing came
    reports(10)                         # 2026-10-02 10:00-10:10 local
    clock.now = datetime(2026, 10, 4, 7, 0, tzinfo=UTC)
    reports(10)                         # 2026-10-04 09:00-09:10 local; nothing at all on 10-03
    rows = health.recent_days(cap, Source(cap, cap.cfg.data_dir), 7)
    assert [(r["day"], r["file"]) for r in rows] == [("2026-10-04", True), ("2026-10-03", False),
                                                     ("2026-10-02", True), ("2026-10-01", False),
                                                     ("2026-09-30", True)]
    today, _, before, _, empty = rows
    assert today["today"] and len(today["gaps"]) == 1 and today["gaps"][0][0].startswith("2026-10-02T08:09:30")
    assert today["gateways"][GATEWAYS["sensecap"]]["uptime"]["pct"] == 100.0
    assert today["gap_s"] == round((datetime(2026, 10, 4, 7, 0) - datetime(2026, 10, 2, 8, 9, 30)).total_seconds())
    assert before["gaps"] == [] and before["gateways"][GATEWAYS["sensecap"]]["reports"] == 20
    assert not empty["reports_known"] and empty["rows"] == 0


def test_health_never_waits_for_the_writer(running):
    """/healthz with the writer busy for seconds (a flood day's summary) must still answer at once."""
    cap, _ = running()
    feed(cap, topic("sensecap"), up(frame(OWN, 1)))
    health.check(cap, cap.cfg)  # the own device is looked up once
    held, release = threading.Event(), threading.Event()

    def busy():
        with cap.lock:
            held.set()
            release.wait(5)

    worker = threading.Thread(target=busy, daemon=True)
    worker.start()
    assert held.wait(2)
    try:
        started = time.monotonic()
        result = health.check(cap, cap.cfg)
        assert time.monotonic() - started < 1 and result["ok"] and result["own"][0]["state"] == "good"
        cap.own_devices.put("aabbccdd", "new")  # its first look-up has to wait for the writer: put off
        started = time.monotonic()
        states = {o["dev_addr"]: (o["state"], o["text"]) for o in health.check(cap, cap.cfg)["own"]}
        assert time.monotonic() - started < 1.5 and states["aabbccdd"] == ("info", "checking")
    finally:
        release.set()
        worker.join(2)
    states = {o["dev_addr"]: (o["state"], o["text"]) for o in health.check(cap, cap.cfg)["own"]}
    assert states["aabbccdd"] == ("info", "not heard yet")  # looked up now; the capture just started


def test_gateway_uptime():
    """Up: sending stats reports. Time in which no gateway at all was heard counts for none of them."""
    start = datetime(2026, 10, 10, 0, 0, tzinfo=UTC)

    def every(first, last):  # a report every 30 s, minus 5 minutes in which the capture restarts
        return [(start + timedelta(seconds=s)).isoformat() for s in range(first, last, 30) if not 3600 <= s < 3900]

    up = store.gateway_uptime({"A": every(0, 14400),
                               "B": every(0, 7200) + every(7800, 14400),  # 10 minutes without reports
                               "C": every(10, 13200),  # stops 20 minutes before the others
                               "D": every(0, 9000) + every(9030, 14400)})  # one report lost: not down
    recorded = 14370 - (3900 - 3580)  # first to last report, less the time nobody was heard
    assert {g: (u["down_s"], u["recorded_s"], u["up_at_end"]) for g, u in up.items()} == {
        "A": (0, recorded, True), "B": (600, recorded, True), "C": (1160, recorded, False), "D": (0, recorded, True)}
    assert (up["A"]["pct"], up["B"]["pct"], up["C"]["pct"]) == (100.0, 95.73, 91.74)
    at = lambda s: (start + timedelta(seconds=s)).isoformat()  # noqa: E731
    assert up["B"]["outages"] == [[at(7185), at(7785)]]  # while the 20 missed reports were due
    assert store.gateway_uptime({"A": every(0, 30)}) == {}  # one report: nothing to tell
