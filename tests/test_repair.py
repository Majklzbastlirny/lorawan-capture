"""A daily file damaged by a hard reset in the middle of a write: repaired, and the capture goes on."""
import sqlite3
import struct
from datetime import datetime, timedelta

from conftest import GATEWAYS, UTC, Clock, feed, frame, topic, up
from lorawan_capture import health, store

START = datetime(2026, 10, 20, 8, 0, tzinfo=UTC)


def fill(path, n=3000):
    conn = store.open_db(path)
    rows = []
    for i in range(n):
        values = dict.fromkeys(store.FRAME_COLUMNS)
        values.update(ts_utc=(START + timedelta(seconds=i)).isoformat(), frame_key=f"{i:040x}",
                      gateway_id=GATEWAYS["sensecap"], is_own=0, mtype="UnconfirmedDataUp", major=0, is_downlink=0,
                      dev_addr=f"{i % 50:08x}", fcnt=i, phy_len=150, phy_hex="ab" * 150, freq_hz=868100000, sf=7,
                      bw_hz=125000, code_rate="CR_4_5", rssi=-100, snr=5.0, crc="CRC_OK")
        rows.append(tuple(values[c] for c in store.FRAME_COLUMNS))
    conn.executemany(store.INSERT_FRAME, rows)
    conn.executemany("INSERT INTO stats (ts_utc, gateway_id, rx_received, rx_crc_ok, tx_received, tx_emitted) "
                     "VALUES (?, ?, 1, 1, 0, 0)",
                     [((START + timedelta(seconds=30 * i)).isoformat(), GATEWAYS["sensecap"]) for i in range(100)])
    conn.commit()
    conn.close()


def layout(path, name):
    conn = sqlite3.connect(path)
    try:
        return (conn.execute("PRAGMA page_size").fetchone()[0],
                conn.execute("SELECT rootpage FROM sqlite_master WHERE name = ?", (name,)).fetchone()[0])
    finally:
        conn.close()


def leaf_pages(path, table):
    """The pages below a table's root, an interior b-tree page."""
    size, root = layout(path, table)
    data = path.read_bytes()[(root - 1) * size: root * size]
    assert data[0] == 0x05  # interior table page
    cells = struct.unpack(">H", data[3:5])[0]
    return [struct.unpack(">I", data[p:p + 4])[0] for p in struct.unpack(f">{cells}H", data[12:12 + 2 * cells])]


def damage(path, page):
    """One page overwritten with junk, as a write cut short can leave it."""
    size, _ = layout(path, "frames")
    with open(path, "r+b") as f:
        f.seek((page - 1) * size)
        f.write(b"\xff" * size)


def test_a_damaged_index_costs_nothing(tmp_path):
    path = tmp_path / "frames_2026-10-20.db"
    fill(path)
    damage(path, layout(path, "ix_ts")[1])
    conn = sqlite3.connect(path)
    assert store.quick_check(conn) != "ok"
    conn.close()
    result = store.repair(path, START)
    assert result["action"] in ("reindexed", "salvaged") and result["lost"] == 0
    assert result["saved"]["frames"] == 3000 and result["saved"]["stats"] == 100
    conn = sqlite3.connect(path)
    assert store.quick_check(conn) == "ok"
    assert conn.execute("SELECT count(*) FROM frames WHERE ts_utc >= ?", (START.isoformat(),)).fetchone()[0] == 3000
    conn.close()
    assert (tmp_path / result["kept"]).exists() and result["kept"].startswith("frames_2026-10-20.damaged-")
    assert [p.name for _, p in store.list_days(tmp_path)] == [path.name]  # the damaged copy is no day


def test_a_damaged_page_of_rows_costs_only_those_rows(tmp_path):
    path = tmp_path / "frames_2026-10-20.db"
    fill(path)
    pages = leaf_pages(path, "frames")
    damage(path, pages[len(pages) // 2])
    before = path.read_bytes()
    result = store.repair(path, START)
    assert (tmp_path / result["kept"]).read_bytes() == before  # kept exactly as the cut left it
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted([path.name, result["kept"]])  # no leftovers
    assert result["action"] == "salvaged" and result["saved"]["stats"] == 100
    assert 3000 - 40 <= result["saved"]["frames"] < 3000 and 0 < result["lost"] <= 40
    conn = sqlite3.connect(path)
    assert store.quick_check(conn) == "ok"
    assert conn.execute("SELECT max(fcnt) FROM frames").fetchone()[0] == 2999  # the rows after it are there
    conn.close()


def test_the_capture_repairs_todays_file_and_goes_on(make_capture):
    clock = Clock(START)
    cap = make_capture(clock=clock)
    path = cap.path
    with cap.lock:
        cap._close()
    fill(path)
    damage(path, leaf_pages(path, "frames")[3])
    again = make_capture(clock=clock)  # the restart after the reset: opening the file finds the damage
    (repair,) = again.repairs
    assert repair["action"] == "salvaged" and again.rows_today == repair["saved"]["frames"] > 2900
    feed(again, topic("sensecap"), up(frame("00a1b2c3", 1)))
    assert again.counters["errors"] == 0 and again.live_summary()["rows"] == again.rows_today == repair["saved"]["frames"] + 1
    (note,) = [i for i in health.check(again, again.cfg)["items"] if i["name"] == "Today's file"]
    assert note["state"] == "warning" and note["text"].startswith("was damaged and repaired at 10:00 (salvaged)")
    assert again.status()["repairs"] == again.repairs


def test_failing_writes_make_healthz_fail(make_capture):
    clock = Clock(START)
    cap = make_capture(clock=clock)
    cap.writer = type("Running", (), {"is_alive": staticmethod(lambda: True), "join": staticmethod(lambda timeout=None: None)})()
    feed(cap, topic("sensecap"), up(frame("00a1b2c3", 1)))
    cap._error("database error on eu868/gateway/x/event/up: disk I/O error", db=True)
    problems = health.check(cap, cap.cfg)["problems"]
    assert problems == ["Writer: storing fails: 2026-10-20T08:00:00+00:00 database error on eu868/gateway/x/event/up: "
                        "disk I/O error"]
    clock.advance(minutes=6)
    feed(cap, topic("sensecap"), up(frame("00a1b2c3", 2)))
    assert health.check(cap, cap.cfg)["ok"]
