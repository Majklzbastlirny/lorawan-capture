"""Counters that wrap: the frame counter goes over the air as 16 bits (65535 -> 0), so does a
DevNonce. Neither may look like a reset of the device, nor may a reset merge uplinks."""
from datetime import datetime

from conftest import UTC, Clock, feed, frame, topic, up
from lorawan_capture import explorer, store
from lorawan_capture.explorer import Source

OWN = "00a1b2c3"


def test_fcnt_step():
    steps = [(65535, 0), (65530, 3), (41, 42), (7, 7), (30000, 0), (100, 99)]
    assert [store.fcnt_step(a, b) for a, b in steps] == [1, 9, 1, 0, 35536, 65535]


def test_the_counter_wrap_is_not_a_reset(make_capture):
    clock = Clock(datetime(2026, 10, 22, 13, 0, tzinfo=UTC))
    cap = make_capture(clock=clock)
    for fcnt in (65533, 65534, 1, 2):  # 65535 and 0 lost on the way over the wrap
        feed(cap, topic("sensecap"), up(frame(OWN, fcnt)))
        clock.advance(seconds=33)
    own = cap.live_summary()["own"][OWN]
    assert (own["heard"], own["expected"]) == (4, 6)
    source = Source(cap, cap.cfg.data_dir)
    (device,) = explorer.devices_report(source, cap.day, cap.day, cap.own())["devices"]
    assert (device["heard"], device["sent"], device["lost"]) == (4, 6, 2)
    page = explorer.device_report(Source(cap, cap.cfg.data_dir), OWN, cap.day, cap.day, cap.cfg.tz, cap.own(), clock())
    assert sum(b["lost"] for b in page["buckets"]) == 2
    assert cap.own_last(OWN)["fcnt"] == 2 and cap.own_last(OWN)["typical"] == 33


def test_a_reset_device_counts_every_uplink(make_capture):
    clock = Clock(datetime(2026, 10, 22, 13, 0, tzinfo=UTC))
    cap = make_capture(clock=clock)
    for fcnt, port in ((1, 1), (2, 1), (3, 1), (0, 2), (1, 2), (2, 2)):  # rebooted after 3: 0, 1, 2 again
        feed(cap, topic("sensecap"), up(frame(OWN, fcnt, port=port)))
        clock.advance(seconds=33)
    own = cap.live_summary()["own"][OWN]
    assert (own["heard"], own["expected"]) == (6, 6)  # not 4 uplinks: 1 and 2 came twice


def test_a_counting_devnonce_wraps_too():
    nonces = {"small": 0, "big": 0}
    explorer._nonce_step(nonces, 65535, 2)
    explorer._nonce_step(nonces, 2, 3)
    assert nonces == {"small": 2, "big": 0}
