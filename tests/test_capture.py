import base64
import json
import sqlite3
import time
from datetime import date, datetime, timedelta

from conftest import UTC, Clock, feed, topic
from lorawan_capture import store


def rows(path, sql="SELECT count(*) FROM frames"):
    conn = sqlite3.connect(path)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def test_one_file_per_local_day(make_capture, payloads):
    # 21:59:50 UTC = 23:59:50 in Prague (CEST): the file changes at local, not UTC, midnight
    clock = Clock(datetime(2026, 10, 3, 21, 59, 50, tzinfo=UTC))
    cap = make_capture(clock=clock)
    feed(cap, topic("sensecap"), payloads["own_100_sensecap"])
    feed(cap, topic("sensecap"), payloads["flood_0_sensecap"])
    clock.advance(seconds=20)
    feed(cap, topic("home"), payloads["own_100_home"])
    feed(cap, topic("sensecap", "event/stats"), payloads["stats_sensecap"])

    first, second = (store.day_file(cap.cfg.data_dir, date(2026, 10, d)) for d in (3, 4))
    assert cap.path == second
    assert rows(first) == [(2,)]
    assert rows(second) == [(1,)]
    assert rows(second, "SELECT gateway_id, rx_received, rx_crc_ok, tx_received, tx_emitted FROM stats") == [
        ("0016c001f1000001", 134, 120, 1, 1)]

    summary = store.read_json(store.summary_file(first))  # written when the day closed
    assert summary["day"] == "2026-10-03"
    assert summary["totals"] == {"lorawan": 1, "other": 0, "sf5": 1, "bad_crc": 0}
    assert summary["hours"][23] == {"lorawan": 1, "other": 0, "sf5": 1, "bad_crc": 0}
    assert summary["gateways"]["0016c001f1000001"]["frames"] == 2
    assert not store.summary_file(second).exists()  # today's stays live


def test_writer_thread_rotates_at_midnight_without_traffic(make_capture, payloads):
    clock = Clock(datetime(2026, 10, 3, 21, 59, 58, tzinfo=UTC))
    cap = make_capture(clock=clock, opened=False, COMMIT_SECONDS="0")
    cap.start_writer()
    cap.queue.put((clock(), topic("sensecap"), payloads["own_100_sensecap"]))
    deadline = time.monotonic() + 5
    while cap.rows_today < 1 and time.monotonic() < deadline:
        time.sleep(0.05)
    clock.advance(seconds=5)  # 00:00:03 local, nothing arrives
    while cap.day != date(2026, 10, 4) and time.monotonic() < deadline:
        time.sleep(0.05)
    cap.stop()
    assert cap.day == date(2026, 10, 4)
    assert rows(store.day_file(cap.cfg.data_dir, date(2026, 10, 3))) == [(1,)]
    assert store.day_file(cap.cfg.data_dir, date(2026, 10, 4)).exists()


def test_conn_states_are_logged_once_per_change(make_capture, payloads):
    cap = make_capture()
    for _ in range(3):  # the retained state comes again after every reconnect
        feed(cap, topic("sensecap", "state/conn"), payloads["conn_online"])
    feed(cap, topic("home", "state/conn"), payloads["conn_offline"])
    feed(cap, topic("home", "state/conn"), b"")  # cleared retained message
    assert cap.db.execute("SELECT gateway_id, state FROM conn ORDER BY rowid").fetchall() == [
        ("0016c001f1000001", "ONLINE"), ("0016c001f1000002", "OFFLINE")]
    assert cap.gateways["0016c001f1000001"]["online"] is True


def test_bad_messages_are_counted_not_fatal(make_capture, payloads):
    cap = make_capture()
    feed(cap, topic("sensecap"), b"\xff\xff\xff not protobuf")
    feed(cap, "something/else", b"x")
    feed(cap, topic("sensecap", "event/ack"), payloads["ack_42"])
    feed(cap, topic("lab", "event/stats"), payloads["stats_lab_json"])
    feed(cap, topic("sensecap"), payloads["own_100_sensecap"])
    assert cap.counters["errors"] == 1
    assert cap.counters["ignored"] == 2
    assert "event/up" in cap.last_error
    assert cap.db.execute("SELECT count(*) FROM frames").fetchone() == (1,)
    assert cap.db.execute("SELECT rx_received, rx_crc_ok FROM stats").fetchall() == [(5, 4)]


def test_own_device_reception(make_capture, payloads):
    cap = make_capture()
    for gw in ("sensecap", "home", "lab"):
        feed(cap, topic(gw), payloads[f"own_100_{gw}"])
    feed(cap, topic("sensecap"), payloads["own_103_sensecap_json"])  # 101 and 102 were lost
    feed(cap, topic("sensecap"), payloads["p2p_sensecap"])
    feed(cap, topic("sensecap"), payloads["join_sensecap"])
    summary = cap.live_summary()
    own = summary["own"]["00a1b2c3"]
    assert (own["heard"], own["expected"]) == (2, 4)
    assert own["gateways"] == {"0016c001f1000001": 2, "0016c001f1000002": 1, "b827ebfffe000003": 1}
    assert own["channels"]["868100000"]["uplinks"] == 1
    assert summary["totals"] == {"lorawan": 5, "other": 1, "sf5": 0, "bad_crc": 0}  # the join is LoRaWAN
    assert summary["unique"] == 4
    assert summary["sf"] == {"7": 5, "12": 1}


def test_stats_details_are_stored(make_capture, payloads):
    clock = Clock(datetime(2026, 10, 3, 10, 0, 0, tzinfo=UTC))
    cap = make_capture(clock=clock)
    feed(cap, topic("sensecap", "event/stats"), payloads["stats_sensecap"])  # protobuf
    feed(cap, topic("lab", "event/stats"), payloads["stats_lab_json"])  # JSON
    rich = {"time": "2026-10-03T10:00:05Z", "configVersion": "1.2.3", "metadata": {"model": "RAK2246"},
            "location": {"latitude": 50.1, "longitude": 15.2, "altitude": 300},
            "rxPacketsReceived": 10, "rxPacketsReceivedOk": 7, "txPacketsReceived": 3, "txPacketsEmitted": 2,
            "rxPacketsPerFrequency": {"868100000": 7}, "txPacketsPerFrequency": {"869525000": 2},
            "rxPacketsPerModulation": [{"modulation": {"lora": {"bandwidth": 125000, "spreadingFactor": 7}}, "count": 7}],
            "txPacketsPerStatus": {"OK": 2, "TOO_LATE": 1},
            "dutyCycleStats": {"window": "3600s", "bands": [{"name": "P", "frequencyMin": 869400000,
                               "frequencyMax": 869650000, "loadMax": "360s", "loadTracked": "3.6s"}]}}
    feed(cap, topic("home", "event/stats"), json.dumps(rich).encode())

    db = cap.db
    assert db.execute("SELECT count(*) FROM stats").fetchone() == (3,)  # the old table is unchanged
    info = db.execute("SELECT gateway_id, gw_time, config_version, latitude, longitude, altitude, metadata "
                      "FROM stats_info ORDER BY rowid").fetchall()
    assert info[0] == ("0016c001f1000001", None, None, None, None, None, '{"concentratord_version": "4.4.0"}')
    assert info[2] == ("0016c001f1000002", "2026-10-03T10:00:05", "1.2.3", 50.1, 15.2, 300.0, '{"model": "RAK2246"}')
    counts = db.execute("SELECT gateway_id, kind, key, count FROM stats_counts ORDER BY rowid").fetchall()
    assert ("0016c001f1000001", "rx_freq", "867900000", 120) in counts
    assert ("b827ebfffe000003", "rx_freq", "867900000", 4) in counts
    assert {c[1:] for c in counts if c[0] == "0016c001f1000002"} == {
        ("rx_freq", "868100000", 7), ("tx_freq", "869525000", 2), ("rx_mod", "sf7", 7),
        ("tx_status", "OK", 2), ("tx_status", "TOO_LATE", 1)}
    assert db.execute("SELECT * FROM stats_duty ORDER BY rowid").fetchall() == [
        (info_ts := db.execute("SELECT ts_utc FROM stats ORDER BY rowid").fetchone()[0], "0016c001f1000001",
         3600.0, "g1", 868_000_000, 868_600_000, 36.0, 9.0),
        (db.execute("SELECT ts_utc FROM stats ORDER BY rowid DESC").fetchone()[0], "0016c001f1000002",
         3600.0, "P", 869_400_000, 869_650_000, 360.0, 3.6)]
    assert info_ts == "2026-10-03T10:00:00+00:00"

    reports = {gid: g["reports"] for gid, g in cap.live_summary()["gateways"].items()}
    home = reports["0016c001f1000002"]
    assert home["tx_status"] == {"OK": 2, "TOO_LATE": 1}
    assert home["duty_peak"] == {"P": 1.0}
    assert home["info"]["clock_offset_s"] == 5.0 and home["info"]["config_version"] == "1.2.3"
    assert reports["0016c001f1000001"]["duty_peak"] == {"g1": 25.0}


def test_stored_share_skips_missing_reports(make_capture, payloads):
    clock = Clock(datetime(2026, 10, 3, 10, 0, 0, tzinfo=UTC))
    cap = make_capture(clock=clock)

    def stats(ok):
        feed(cap, topic("sensecap", "event/stats"),
             json.dumps({"rxPacketsReceived": ok, "rxPacketsReceivedOk": ok}).encode())

    stats(0)                                             # 10:00:00
    for i in range(2):                                   # two packets, reported at 10:00:30
        clock.advance(seconds=10)
        feed(cap, topic("sensecap"), payloads[f"flood_{i}_sensecap"])
    clock.advance(seconds=10)
    stats(2)
    clock.advance(seconds=10)                            # stored, but its report (10:01:00)
    feed(cap, topic("sensecap"), payloads["flood_2_sensecap"])  # never arrives
    clock.advance(seconds=30)
    feed(cap, topic("sensecap"), payloads["flood_3_sensecap"])
    clock.advance(seconds=20)
    stats(1)                                             # 10:01:30 covers 10:01:00-10:01:30 only
    clock.advance(seconds=30)
    stats(0)                                             # 10:02:00
    assert cap.live_summary()["gateways"]["0016c001f1000001"]["check"] == {"stored": 2, "reported": 2, "skipped": 1}


def test_concentrator_temperature_over_the_day(make_capture):
    cap = make_capture()
    for temp in ("31.8125", "35.5", "33.0"):
        feed(cap, topic("sensecap", "event/stats"), json.dumps(
            {"rxPacketsReceived": 1, "metadata": {"concentrator_temp": temp, "model": "seeed_wm1302"}}).encode())
    report = cap.live_summary()["gateways"]["0016c001f1000001"]["reports"]
    assert report["temp"] == {"min": 31.81, "max": 35.5}
    assert report["info"]["metadata"] == {"concentrator_temp": "33.0", "model": "seeed_wm1302"}  # the latest


def test_bad_crc_frames_are_kept_apart(make_capture, payloads):
    """With Concentratord's disable_crc_filter the gateway forwards packets with a bad CRC.
    Their bytes are noise even when they look like a LoRaWAN frame from the own node."""
    cap = make_capture()
    phy = bytes.fromhex("40c3b2a1008066000162aabbccdd11223344")  # parses as the own node, fcnt 102
    bad = {"phyPayload": base64.b64encode(phy).decode(),
           "txInfo": {"frequency": 867100000, "modulation": {"lora": {"bandwidth": 125000, "spreadingFactor": 7}}},
           "rxInfo": {"rssi": -120, "snr": -9.5, "crcStatus": "BAD_CRC"}}
    feed(cap, topic("sensecap"), json.dumps(bad).encode())
    feed(cap, topic("sensecap"), payloads["own_100_sensecap"])
    s = cap.live_summary()
    assert s["totals"] == {"lorawan": 1, "other": 0, "sf5": 0, "bad_crc": 1}
    assert s["channels_bad"] == {"867100000": {"0016c001f1000001": 1}}
    assert "867100000" not in s["channels"]
    assert s["own"]["00a1b2c3"]["heard"] == 1  # the noise did not count as an uplink


def test_site_notes_are_created_once(make_capture):
    cap = make_capture(opened=False)
    notes = cap.cfg.data_dir / "NOTES.md"
    cap.start_writer()
    deadline = time.monotonic() + 5
    while not notes.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    cap.stop()
    assert notes.read_text(encoding="utf-8").startswith("# Site notes")
    notes.write_text("edited by hand", encoding="utf-8")
    again = make_capture(opened=False)  # a restart must not overwrite the edits
    again.start_writer()
    time.sleep(0.5)
    again.stop()
    assert notes.read_text(encoding="utf-8") == "edited by hand"


def test_snapshot_is_a_complete_copy(make_capture, payloads, tmp_path):
    cap = make_capture()
    for i in range(5):
        feed(cap, topic("sensecap"), payloads[f"flood_{i}_sensecap"])
    with cap.lock:  # leave the last insert uncommitted: the snapshot must still include it
        cap._handle(cap.clock(), topic("home"), payloads["flood_0_home"])
    dest = tmp_path / "snap.db"
    cap.snapshot(dest)
    assert rows(dest) == [(6,)]
    assert rows(dest, "SELECT count(*) FROM sqlite_master WHERE type='index'") == [(3,)]


def test_retention(tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    today = date(2026, 10, 10)
    for back in range(6):
        store.open_db(store.day_file(data, today - timedelta(days=back))).close()
    removed = store.apply_retention(data, today, keep_days=2)
    assert sorted(p.name for p in removed) == ["frames_2026-10-05.db", "frames_2026-10-06.db",
                                               "frames_2026-10-07.db"]
    assert [d.isoformat() for d, _ in store.list_days(data)] == ["2026-10-10", "2026-10-09", "2026-10-08"]
    assert store.apply_retention(data, today, keep_days=0) == []
