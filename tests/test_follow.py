"""Own devices followed by their DevEUI: an OTAA device gets a new DevAddr at every join, and the
gateways see the DevEUI only in the join request. Its new address is the one whose first
uplinks (a new session's frame counter) follow that join request."""
from datetime import datetime, timedelta

from conftest import UTC, Clock, feed, frame, topic, up
from lorawan_capture import health
from lorawan_capture.devices import OwnDevices

EUI, OTHER_EUI = "70b3d57ed0000001", "70b3d57ed0000099"
OLD, NEW, TTN = "00a1b2c3", "00b2c3d4", "260b1234"
START = datetime(2026, 10, 31, 13, 0, tzinfo=UTC)


def join(dev_eui: str, nonce: int = 27) -> str:
    """A join request: MHDR, JoinEUI, DevEUI (little-endian), DevNonce, MIC."""
    return "00" + "00" * 8 + bytes.fromhex(dev_eui)[::-1].hex() + nonce.to_bytes(2, "little").hex() + "11223344"


def own_rows(cap, dev: str) -> list[int]:
    return [r[0] for r in cap.db.execute("SELECT is_own FROM frames WHERE dev_addr = ? ORDER BY id", (dev,))]


def test_a_followed_device_is_found_again_after_it_joins(make_capture):
    clock = Clock(START)
    cap = make_capture(clock=clock)
    cap.own_devices.put(OLD, "UART node", EUI)
    for fcnt in (4276, 4277):
        feed(cap, topic("sensecap"), up(frame(OLD, fcnt)))
        clock.advance(seconds=33)
    for gw in ("sensecap", "home"):  # the join request, heard twice
        feed(cap, topic(gw), up(join(EUI), sf=9))
    clock.advance(seconds=6)
    for gw in ("sensecap", "home", "lab"):  # the new session's first uplink
        feed(cap, topic(gw), up(frame(NEW, 1), sf=9))
    devices = cap.own_devices.snapshot()
    assert devices[OLD]["until"] == clock().isoformat(timespec="seconds")
    assert {k: devices[NEW][k] for k in ("name", "dev_eui")} == {"name": "UART node", "dev_eui": EUI}
    assert "until" not in devices[NEW] and cap.own_devices.following()[EUI]["dev_addr"] == NEW
    assert own_rows(cap, NEW) == [1, 1, 1]  # stored as own from the very first uplink
    assert OLD in cap.own() and NEW in cap.own()  # the old address's history still counts as own
    clock.advance(seconds=33)
    feed(cap, topic("sensecap"), up(frame(NEW, 2)))
    checked = {o["dev_addr"]: o["name"] for o in health.check(cap, cap.cfg)["own"]}
    assert checked == {NEW: "UART node"}  # the address left behind is not overdue
    again = OwnDevices(cap.cfg.data_dir / "devices.json")  # kept in devices.json
    assert again.snapshot() == cap.own_devices.snapshot() and again.followed == {EUI}


def test_only_a_new_session_soon_after_a_followed_join_is_taken_over(make_capture):
    clock = Clock(START)
    cap = make_capture(clock=clock)
    cap.own_devices.put(OLD, "UART node", EUI)
    feed(cap, topic("sensecap"), up(join(OTHER_EUI)))  # somebody else's device joins
    clock.advance(seconds=6)
    feed(cap, topic("sensecap"), up(frame(NEW, 1)))
    feed(cap, topic("sensecap"), up(join(EUI)))
    clock.advance(seconds=6)
    feed(cap, topic("sensecap"), up(frame("00c3d4e5", 50)))  # an old session, not a new one
    feed(cap, topic("sensecap"), up(frame(TTN, 1)))  # a new session, but on another network
    clock.advance(seconds=130)
    feed(cap, topic("sensecap"), up(frame("00d4e5f6", 1)))  # too late after the join
    assert cap.own() == {OLD} and cap.own_devices.following()[EUI]["dev_addr"] == OLD


def test_the_first_uplink_may_be_lost(make_capture):
    clock = Clock(START)
    cap = make_capture(clock=clock)
    cap.own_devices.put(OLD, "UART node", EUI)
    feed(cap, topic("sensecap"), up(join(EUI)))
    clock.advance(seconds=72)  # FCnt 1 and 2 were heard by no gateway
    feed(cap, topic("sensecap"), up(frame(NEW, 3)))
    assert cap.own_devices.following()[EUI]["dev_addr"] == NEW


def test_follow_by_deveui_from_the_recordings(make_capture):
    clock = Clock(START)
    cap = make_capture(clock=clock)
    feed(cap, topic("sensecap"), up(frame(OLD, 7)))  # in use before: not a session of the device
    for addr, nonce in ((NEW, 26), ("00c3d4e5", 27)):
        feed(cap, topic("sensecap"), up(join(EUI, nonce)))
        feed(cap, topic("sensecap"), up(frame(OLD, 8)))
        clock.advance(seconds=6)
        feed(cap, topic("sensecap"), up(frame(addr, 1)))
        clock.advance(hours=5)
    clock.advance(days=1)
    feed(cap, topic("sensecap"), up(frame("00c3d4e5", 900)))  # the next day: a closed file before
    history = cap.node_history(EUI)
    assert [addr for _, addr in history] == [NEW, "00c3d4e5"]
    result = cap.own_devices.follow_device(EUI, "UART node", history)
    assert (result["dev_addr"], result["addresses"]) == ("00c3d4e5", [NEW, "00c3d4e5"])
    devices = cap.own_devices.snapshot()
    assert devices[NEW]["until"] == history[1][0] and "until" not in devices["00c3d4e5"]


def test_a_join_while_the_capture_was_down_is_caught_up(make_capture):
    clock = Clock(START)
    cap = make_capture(clock=clock)
    feed(cap, topic("sensecap"), up(join(EUI)))
    clock.advance(seconds=6)
    feed(cap, topic("sensecap"), up(frame(NEW, 1)))  # not followed yet: nothing taken over
    now, clock.now = clock.now, START - timedelta(days=20)
    cap.own_devices.put(OLD, "UART node", EUI)  # the device as it was known before that join
    clock.now = now
    with cap.lock:
        cap._catch_up_joins()  # what the start of the writer does
    assert cap.own_devices.following()[EUI]["dev_addr"] == NEW
    assert cap.own_devices.snapshot()[OLD]["until"] == cap.own_devices.snapshot()[NEW]["added"]


def test_own_device_list(tmp_path):
    devices = OwnDevices(tmp_path / "devices.json")
    devices.follow_device(EUI, "Waiting", [])  # no address yet
    devices.put(OLD, "Node", "70:B3:D5:7E:D0:00:00:02")
    devices.adopt("70b3d57ed0000002", NEW, "2026-10-31T13:59:23+00:00")
    devices.put(NEW, "Renamed")  # one name for every address of the device
    assert {a: e["name"] for a, e in devices.snapshot().items()} == {OLD: "Renamed", NEW: "Renamed"}
    assert set(devices.following()) == {EUI, "70b3d57ed0000002"}  # the waiting one is still followed
    assert list(devices.current()) == [NEW]
    devices.put("00e5f6a7", None)
    assert devices.remove("70b3d57ed0000002") and devices.snapshot().keys() == {"00e5f6a7"}
    assert devices.remove(EUI) and devices.followed == frozenset()
    assert not devices.remove(EUI)
