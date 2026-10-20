import base64
import gzip
import json
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from datetime import timedelta

import pytest
from conftest import feed, topic
from lorawan_capture import store
from lorawan_capture.web import make_server


@pytest.fixture
def web(make_capture, payloads):
    started = []

    def factory(**env):
        cap = make_capture(opened=False, **env)
        cap.start_writer()
        for name in ("own_100_sensecap", "own_100_home", "flood_0_sensecap", "flood_1_sensecap"):
            gw = name.rsplit("_", 1)[1]
            cap.queue.put((cap.clock(), topic(gw), payloads[name]))
        cap.queue.put((cap.clock(), topic("sensecap", "event/stats"), payloads["stats_sensecap"]))
        deadline = time.monotonic() + 5
        while (cap.rows_today < 4 or cap.dirty) and time.monotonic() < deadline:
            time.sleep(0.05)
        server = make_server(cap, cap.cfg, host="127.0.0.1")
        threading.Thread(target=server.serve_forever, daemon=True).start()
        started.append(server)
        return cap, f"http://127.0.0.1:{server.server_address[1]}"

    yield factory
    for server in started:
        server.shutdown()
        server.server_close()


def get(url, auth=None):
    req = urllib.request.Request(url)
    if auth:
        req.add_header("Authorization", "Basic " + base64.b64encode(auth.encode()).decode())
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status, dict(resp.headers), resp.read()


def status_of(url, auth=None):
    try:
        return get(url, auth)[0]
    except urllib.error.HTTPError as err:
        return err.code


def test_api_and_downloads(web, tmp_path):
    cap, base = web()
    assert get(base + "/healthz")[2] == b"ok"
    assert b"Packets per hour" in get(base + "/")[2]

    status = json.loads(get(base + "/api/status")[2])
    assert status["writer_alive"] and status["rows_today"] == 4
    assert status["config"]["own"] == ["00a1b2c3"]
    assert sorted(status["registry"]) == ["0016c001f1000001", "0016c001f1000002"]  # seen, not yet named

    today = cap.day.isoformat()
    days = json.loads(get(base + "/api/days")[2])
    assert days[0]["day"] == today and days[0]["today"]
    assert days[0]["brief"]["sf5"] == 2 and days[0]["brief"]["lorawan"] == 2

    summary = json.loads(get(base + f"/api/day/{today}")[2])
    assert summary["own"]["00a1b2c3"]["heard"] == 1
    assert summary["gateways"]["0016c001f1000001"]["stats"]["reports"] == 1

    _, headers, body = get(base + f"/download/frames_{today}.db")
    assert "_partial-" in headers["Content-Disposition"]
    snap = tmp_path / "snap.db"
    snap.write_bytes(body)
    conn = sqlite3.connect(snap)
    assert conn.execute("SELECT count(*) FROM frames").fetchone() == (4,)
    conn.close()

    _, headers, body = get(base + f"/download/frames_{today}.db.gz")
    assert headers["Content-Type"] == "application/gzip"
    snap.write_bytes(gzip.decompress(body))
    conn = sqlite3.connect(snap)
    assert conn.execute("SELECT count(*) FROM stats").fetchone() == (1,)
    conn.close()
    snapshots = cap.cfg.data_dir / ".snapshots"  # the server deletes its temporary copy just
    deadline = time.monotonic() + 3              # after the client has the last byte
    while any(snapshots.iterdir()) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not any(snapshots.iterdir())


def test_closed_day_is_served_as_is(web, payloads):
    cap, base = web()
    yesterday = cap.day - timedelta(days=1)
    path = store.day_file(cap.cfg.data_dir, yesterday)
    conn = store.open_db(path)
    conn.execute(store.INSERT_STATS, ("2026-01-01T00:00:00+00:00", "aa", 1, 1, 0, 0))
    conn.commit()
    conn.close()
    summary = json.loads(get(base + f"/api/day/{yesterday}")[2])  # computed on demand, then cached
    assert summary["gateways"]["aa"]["stats"]["rx"] == 1
    assert store.summary_file(path).exists()
    _, headers, body = get(base + f"/download/frames_{yesterday}.db")
    assert body == path.read_bytes() and "_partial" not in headers["Content-Disposition"]


def test_range_download(web, tmp_path):
    import io
    import zipfile

    cap, base = web()
    today = cap.day
    for back in (1, 3, 10):  # closed days; 10 days back falls outside the range
        conn = store.open_db(store.day_file(cap.cfg.data_dir, today - timedelta(days=back)))
        conn.execute(store.INSERT_STATS, ("2026-01-01T00:00:00+00:00", "aa", back, back, 0, 0))
        conn.commit()
        conn.close()
    first = today - timedelta(days=6)
    _, headers, body = get(base + f"/download/range.zip?from={first}&to={today}")
    assert headers["Content-Type"] == "application/zip"
    assert f'filename="lorawan_{first}_to_{today}.zip"' in headers["Content-Disposition"]
    archive = zipfile.ZipFile(io.BytesIO(body))
    assert archive.testzip() is None
    names = archive.namelist()
    assert names[:3] == ["README.md", f"frames_{today - timedelta(days=3)}.db", f"frames_{today - timedelta(days=1)}.db"]
    assert names[3].startswith(f"frames_{today}_partial-") and names[4:] == ["gateways.json", "devices.json"]
    readme = archive.read("README.md").decode()
    assert f"days {first} to {today}" in readme
    assert f"| `{names[3]}` |" in readme and readme.split(f"| `{names[3]}` |")[1].split("\n")[0].endswith("| 4 | 1 |")
    assert "### Table `frames`" in readme and "Own devices now (names in `devices.json`): `00a1b2c3`." in readme
    assert "- `0016c001f1000001`: (no name)" in readme
    assert readme.rstrip().endswith("on which channels, since when.")  # the site notes come last
    for name, sql, expected in ((names[1], "SELECT rx_received FROM stats", [(3,)]),
                                (names[3], "SELECT count(*) FROM frames", [(4,)])):
        (tmp_path / name).write_bytes(archive.read(name))
        conn = sqlite3.connect(tmp_path / name)
        assert conn.execute(sql).fetchall() == expected
        conn.close()
    assert "0016c001f1000001" in json.loads(archive.read("gateways.json"))

    assert status_of(base + f"/download/range.zip?from={today}&to={first}") == 400  # reversed
    assert status_of(base + "/download/range.zip?from=2026-02-30&to=2026-03-01") == 400
    assert status_of(base + "/download/range.zip") == 400
    assert status_of(base + "/download/range.zip?from=2020-01-01&to=2020-01-31") == 404  # nothing there
    snapshots = cap.cfg.data_dir / ".snapshots"
    deadline = time.monotonic() + 3
    while any(snapshots.iterdir()) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not any(snapshots.iterdir())


def test_readme_page(web):
    cap, base = web()
    _, headers, body = get(base + "/download/README.md")
    text = body.decode()
    assert headers["Content-Type"].startswith("text/plain")
    assert text.startswith("# LoRaWAN capture data") and "## How to read these files" in text
    assert "# Site notes" in text and "| File |" not in text  # no file table outside a bulk download

    (cap.cfg.data_dir / "NOTES.md").write_text("# Site notes\n\nMy own notes.\n", encoding="utf-8")
    assert get(base + "/download/README.md")[2].decode().rstrip().endswith("My own notes.")


@pytest.mark.parametrize("path", [
    "/download/frames_2026-13-45.db", "/download/frames_..%2F..%2Fsecret.db", "/download/../store.py",
    "/api/day/2026-02-30", "/api/day/..%2F..", "/download/frames_2020-01-01.db", "/nope"])
def test_bad_paths_are_404(web, path):
    _, base = web()
    assert status_of(base + path) == 404


def post(url, body, ctype="application/json", auth=None):
    req = urllib.request.Request(url, data=body if isinstance(body, bytes) else json.dumps(body).encode(),
                                 headers={"Content-Type": ctype}, method="POST")
    if auth:
        req.add_header("Authorization", "Basic " + base64.b64encode(auth.encode()).decode())
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as err:
        return err.code, err.read().decode()


def test_rename_gateway(web):
    cap, base = web()
    url = base + "/api/gateways/0016c001f1000001"
    assert post(url, {"name": "Roof"}) == (200, {"id": "0016c001f1000001", "name": "Roof",
                                                 "first_seen": cap.registry.snapshot()["0016c001f1000001"]["first_seen"],
                                                 "last_seen": cap.registry.snapshot()["0016c001f1000001"]["last_seen"]})
    status = json.loads(get(base + "/api/status")[2])
    assert status["registry"]["0016c001f1000001"]["name"] == "Roof"
    assert store.read_json(cap.cfg.data_dir / "gateways.json")["0016c001f1000001"]["name"] == "Roof"

    assert post(url, b"name=Evil", ctype="application/x-www-form-urlencoded")[0] == 415  # no cross-site forms
    assert post(url, {"name": "x" * 41})[0] == 400
    assert post(url, {"name": 5})[0] == 400
    assert post(url, b"not json")[0] == 400
    assert post(base + "/api/gateways/bad%20id", {"name": "x"})[0] == 400
    assert post(base + "/api/elsewhere", {"name": "x"})[0] == 404
    assert post(url, {"name": ""})[1]["name"] is None  # cleared


def test_login(web):
    _, base = web(WEB_USERNAME="admin", WEB_PASSWORD="s3cret")
    assert status_of(base + "/") == 401
    assert status_of(base + "/api/status", auth="admin:wrong") == 401
    assert status_of(base + "/api/status", auth="admin:s3cret") == 200
    assert status_of(base + "/healthz") == 200  # the Docker healthcheck needs no login
    for path, ctype, magic in (("/favicon.svg", "image/svg+xml", b"<svg"), ("/favicon.ico", "image/x-icon", b"\0\0\1\0"),
                               ("/apple-touch-icon.png", "image/png", b"\x89PNG")):  # nor do the icons
        _, headers, body = get(base + path)
        assert headers["Content-Type"] == ctype and body.startswith(magic) and headers["Cache-Control"] == "max-age=86400"
    assert post(base + "/api/gateways/0016c001f1000001", {"name": "x"})[0] == 401
    assert post(base + "/api/gateways/0016c001f1000001", {"name": "x"}, auth="admin:s3cret")[0] == 200


def delete(url, auth=None):
    req = urllib.request.Request(url, method="DELETE")
    if auth:
        req.add_header("Authorization", "Basic " + base64.b64encode(auth.encode()).decode())
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as err:
        return err.code, err.read().decode()


def test_explorer_api(web):
    cap, base = web()
    today = cap.day.isoformat()
    q = f"?from={today}&to={today}"
    devices = json.loads(get(base + "/api/devices" + q)[2])
    assert [(d["dev_addr"], d["own"], d["heard"]) for d in devices["devices"]] == [("00a1b2c3", True, 1)]
    assert devices["days"] == [today] and devices["skipped"] == []
    other = json.loads(get(base + "/api/other" + q)[2])
    assert [g["sf"] for g in other["other"]] == [5, 5] and other["total"] == 2
    assert other["hourly"] and all(len(g["series"]) == len(other["buckets"]) for g in other["other"])
    assert json.loads(get(base + "/api/joins" + q)[2])["joins"] == []

    device = json.loads(get(base + "/api/device/00a1b2c3" + q)[2])
    assert device["summary"]["heard"] == 1 and len(device["packets"][0]["gateways"]) == 2
    packet = json.loads(get(base + f"/api/frame/{today}/{device['packets'][0]['key']}")[2])
    assert len(packet["receptions"]) == 2 and [f["name"] for f in packet["fields"]][:2] == ["MHDR", "DevAddr"]

    first = cap.day - timedelta(days=200)  # a long range: the device views show its last 31 days
    longer = json.loads(get(base + f"/api/devices?from={first}&to={today}")[2])
    assert (longer["clamped"], longer["asked_from"], longer["from"]) == (True, first.isoformat(),
                                                                         (cap.day - timedelta(days=30)).isoformat())
    assert json.loads(get(base + f"/api/device/00a1b2c3?from={first}&to={today}")[2])["clamped"]
    assert "clamped" not in json.loads(get(base + f"/api/joins?from={first}&to={today}")[2])
    assert "clamped" not in devices

    assert status_of(base + "/api/devices") == 400
    assert status_of(base + f"/api/devices?from={today}&to=2020-01-01") == 400
    assert status_of(base + "/api/device/12345678" + q) == 404
    assert status_of(base + "/api/device/xyz" + q) == 404
    assert status_of(base + f"/api/frame/{today}/{'0' * 40}") == 404
    assert status_of(base + f"/api/frame/{today}/../secret") == 404


def test_own_devices_api(web):
    cap, base = web()
    url = base + "/api/own/AABBCCDD"
    code, body = post(url, {"name": "Meter"})
    assert code == 200 and (body["dev_addr"], body["name"]) == ("aabbccdd", "Meter")
    status = json.loads(get(base + "/api/status")[2])
    assert status["devices"]["aabbccdd"]["name"] == "Meter" and status["config"]["own"] == ["00a1b2c3", "aabbccdd"]
    assert store.read_json(cap.cfg.data_dir / "devices.json")["devices"]["aabbccdd"]["name"] == "Meter"
    assert post(url, {"name": "Water meter"})[1]["name"] == "Water meter"  # renamed

    assert post(url, b"name=x", ctype="application/x-www-form-urlencoded")[0] == 415
    assert post(base + "/api/own/123", {"name": "x"})[0] == 400
    assert delete(url) == (200, {"dev_addr": "aabbccdd", "removed": True})
    assert delete(url)[0] == 404
    assert delete(base + "/api/own/zz")[0] == 400
    assert delete(base + "/api/gateways/0016c001f1000001")[0] == 404
    assert "aabbccdd" not in json.loads(get(base + "/api/status")[2])["devices"]


def test_own_devices_need_the_login(web):
    _, base = web(WEB_USERNAME="admin", WEB_PASSWORD="s3cret")
    assert delete(base + "/api/own/00a1b2c3")[0] == 401
    assert post(base + "/api/own/aabbccdd", {"name": "x"})[0] == 401
    assert status_of(base + "/api/devices?from=2026-01-01&to=2026-01-02") == 401
    assert delete(base + "/api/own/00a1b2c3", auth="admin:s3cret")[0] == 200


def test_health_endpoints(web):
    cap, base = web()
    assert json.loads(get(base + "/api/status")[2])["health"]["ok"]
    full = json.loads(get(base + "/api/health")[2])
    assert full["ok"] and {"Writer", "Messages", "Disk", "Broker"} <= {i["name"] for i in full["items"]}
    days = json.loads(get(base + "/api/health/days?days=3")[2])
    assert days["days"][0]["day"] == cap.day.isoformat() and days["days"][0]["today"]
    assert json.loads(get(base + "/api/health/days?days=x")[2])["days"]  # falls back to 14 days


def test_healthz_fails_when_nothing_arrives(web):
    _, base = web(HEALTH_SILENCE_MINUTES="0.001")
    time.sleep(0.3)
    with pytest.raises(urllib.error.HTTPError) as err:
        get(base + "/healthz")
    assert err.value.code == 503 and err.value.read().decode().startswith("Messages: none for")


def test_device_csv(web):
    import csv
    import io

    cap, base = web()
    today = cap.day.isoformat()
    _, headers, body = get(base + f"/api/device/00a1b2c3/uplinks.csv?from={today}&to={today}")
    assert headers["Content-Type"].startswith("text/csv")
    assert f'filename="lorawan_00a1b2c3_{today}_to_{today}.csv"' in headers["Content-Disposition"]
    rows = list(csv.reader(io.StringIO(body.decode("utf-8"))))
    assert len(rows) == 2  # the header and the one uplink, heard by two gateways
    record = dict(zip(*rows))
    assert (record["fcnt"], record["gateways"], record["best_rssi_dbm"], record["best_snr_db"]) == ("100", "2", "-99", "12.5")
    assert (record["rssi_0016c001f1000001"], record["rssi_0016c001f1000002"]) == ("-99", "-107")
    assert record["frequency_mhz"] == "868.100" and record["time_local"].startswith(today)

    excel = get(base + f"/api/device/00a1b2c3/uplinks.csv?from={today}&to={today}&excel=1")[2].decode("utf-8")
    assert excel.startswith("﻿")  # so Excel reads it as UTF-8
    record = dict(zip(*csv.reader(io.StringIO(excel[1:]), delimiter=";")))
    assert (record["best_snr_db"], record["frequency_mhz"]) == ("12,5", "868,100")
    assert status_of(base + "/api/device/00a1b2c3/uplinks.csv") == 400


def test_one_unreadable_day_does_not_hide_the_others(web, monkeypatch):
    cap, base = web()
    summary = cap.day_summary

    def damaged(day, path, compute=True):
        if day == cap.day:
            raise sqlite3.DatabaseError("database disk image is malformed")
        return summary(day, path, compute)

    monkeypatch.setattr(cap, "day_summary", damaged)
    days = json.loads(get(base + "/api/days")[2])
    assert [(d["today"], d["error"]) for d in days][:1] == [(True, "database disk image is malformed")]
    assert all(d["error"] is None for d in days[1:])
