from datetime import datetime, timedelta

import pytest
from conftest import UTC, Clock, feed, frame, topic, up
from lorawan_capture import store
from lorawan_capture.devices import OwnDevices, normalize

OWN, NEIGHBOUR = "00a1b2c3", "260b1234"


def test_env_devices_are_taken_over_once(tmp_path):
    path = tmp_path / "devices.json"
    devices = OwnDevices(path, frozenset({OWN}))
    assert devices.addrs == {OWN}
    assert store.read_json(path)["imported"] == [OWN]
    assert devices.remove(OWN)
    assert OwnDevices(path, frozenset({OWN})).addrs == frozenset()  # a restart: removed stays removed
    later = OwnDevices(path, frozenset({OWN, "11223344"}))  # an address added to .env later
    assert later.addrs == {"11223344"}


def test_add_rename_remove(tmp_path):
    path = tmp_path / "devices.json"
    devices = OwnDevices(path)
    assert not path.exists()  # nothing to keep yet
    entry = devices.put("AA BB CC DD", "  Water meter ")
    assert entry == {"dev_addr": "aabbccdd", "name": "Water meter", "added": entry["added"]}
    assert devices.put("0xAABBCCDD", "")["name"] is None  # the same device, name cleared
    assert devices.addrs == {"aabbccdd"}
    assert OwnDevices(path).snapshot() == {"aabbccdd": {"name": None, "added": entry["added"]}}
    assert not devices.remove("11223344")


@pytest.mark.parametrize("addr, name", [("123", "x"), ("gggggggg", "x"), ("00a1b2c31", "x"),
                                        (OWN, "y" * 41), (OWN, "a\nb")])
def test_invalid_devices_are_refused(tmp_path, addr, name):
    with pytest.raises(ValueError):
        OwnDevices(tmp_path / "devices.json").put(addr, name)
    assert not (tmp_path / "devices.json").exists()


def test_normalize():
    assert {normalize(text) for text in ("00a1b2c3", "0x00a1b2c3", "00 a1 b2 c3", "00:a1:b2:c3", "00a1-b2c3")} == {OWN}


def test_own_devices_apply_to_past_days(make_capture):
    clock = Clock(datetime(2026, 10, 2, 10, 0, tzinfo=UTC))
    cap = make_capture(clock=clock)
    feed(cap, topic("sensecap"), up(frame(NEIGHBOUR, 5)))
    clock.advance(days=1)  # yesterday is closed and summarised with the own devices of then
    feed(cap, topic("sensecap"), up(frame(NEIGHBOUR, 6)))
    yesterday = cap.day - timedelta(days=1)
    path = store.day_file(cap.cfg.data_dir, yesterday)
    assert store.cached_summary(path)["own"] == {} and store.cached_summary(path)["own_for"] == [OWN]

    cap.own_devices.put(NEIGHBOUR, "Neighbour")
    summary = cap.day_summary(yesterday, path)
    assert summary["own"][NEIGHBOUR]["heard"] == 1 and summary["own_for"] == [OWN, NEIGHBOUR]
    assert store.cached_summary(path)["own_for"] == [OWN, NEIGHBOUR]  # kept for next time
    assert summary["totals"] == store.cached_summary(path)["totals"]  # only the own part was redone
    assert cap.live_summary()["own"][NEIGHBOUR]["heard"] == 1

    feed(cap, topic("sensecap"), up(frame(NEIGHBOUR, 7)))
    assert cap.db.execute("SELECT fcnt, is_own FROM frames WHERE dev_addr = ? ORDER BY id",
                          (NEIGHBOUR,)).fetchall() == [(6, 0), (7, 1)]  # is_own as it was when recorded
    cap.own_devices.remove(OWN)
    assert cap.day_summary(yesterday, path)["own_for"] == [NEIGHBOUR]
