import pytest
from conftest import GATEWAYS, feed, topic
from lorawan_capture import store
from lorawan_capture.registry import Registry


def test_a_new_gateway_is_recorded_at_once(make_capture, payloads):
    cap = make_capture()
    feed(cap, topic("sensecap"), payloads["own_100_sensecap"])
    path = cap.cfg.data_dir / "gateways.json"
    saved = store.read_json(path)  # written immediately, not at the next periodic save
    entry = saved[GATEWAYS["sensecap"]]
    assert entry["name"] is None and entry["first_seen"] == entry["last_seen"]

    cap.clock = lambda: cap.started.replace(year=cap.started.year + 1)  # much later
    feed(cap, topic("sensecap", "event/stats"), payloads["stats_sensecap"])
    feed(cap, "eu868/gateway/aa555a0000000001/state/conn", payloads["conn_online"])  # a brand-new one
    snap = cap.registry.snapshot()
    assert snap[GATEWAYS["sensecap"]]["first_seen"] == entry["first_seen"]
    assert snap[GATEWAYS["sensecap"]]["last_seen"] != entry["last_seen"]
    assert "aa555a0000000001" in store.read_json(path)


def test_a_stored_online_state_is_not_activity(make_capture, payloads):
    """EMQX hands the retained online/offline state to every new subscriber, so it says
    nothing about when the gateway was last heard."""
    cap = make_capture()
    feed(cap, topic("home", "state/conn"), payloads["conn_offline"])  # a gateway known only this way
    entry = cap.registry.snapshot()[GATEWAYS["home"]]
    assert entry["first_seen"] and entry["last_seen"] is None
    feed(cap, topic("sensecap"), payloads["own_100_sensecap"])
    heard = cap.registry.snapshot()[GATEWAYS["sensecap"]]["last_seen"]
    cap.clock = lambda: cap.started.replace(year=cap.started.year + 1)
    feed(cap, topic("sensecap", "state/conn"), payloads["conn_online"])
    assert cap.registry.snapshot()[GATEWAYS["sensecap"]]["last_seen"] == heard


def test_names_from_the_ui_win_over_the_env_file(make_capture, tmp_path):
    cap = make_capture(GATEWAY_NAMES=f"{GATEWAYS['sensecap']}=SenseCAP,{GATEWAYS['home']}=Home")
    assert cap.registry.snapshot()[GATEWAYS["sensecap"]]["name"] == "SenseCAP"  # before any message
    cap.registry.rename(GATEWAYS["sensecap"], "  Roof  ")
    cap.registry.rename(GATEWAYS["home"], "")  # cleared: falls back to the id in the UI

    again = Registry(cap.cfg.data_dir / "gateways.json", {GATEWAYS["sensecap"]: "SenseCAP"})  # a restart
    names = {gid: entry["name"] for gid, entry in again.snapshot().items()}
    assert names == {GATEWAYS["sensecap"]: "Roof", GATEWAYS["home"]: None}


@pytest.mark.parametrize("gateway_id, name", [("bad id", "x"), ("../x", "x"), ("a" * 65, "x"),
                                              ("0016c001f1000001", "y" * 41), ("0016c001f1000001", "a\nb")])
def test_invalid_renames_are_refused(tmp_path, gateway_id, name):
    with pytest.raises(ValueError):
        Registry(tmp_path / "gateways.json").rename(gateway_id, name)
    assert not (tmp_path / "gateways.json").exists()
