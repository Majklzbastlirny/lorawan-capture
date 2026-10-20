"""What the UI and the log say when the broker cannot be used (paho callbacks, no network)."""


class Code:
    """Stand-in for paho's ReasonCode."""

    def __init__(self, value: int, text: str) -> None:
        self.value, self.text = value, text

    @property
    def is_failure(self) -> bool:
        return self.value >= 0x80

    def __eq__(self, other) -> bool:
        return self.value == (other.value if isinstance(other, Code) else other)

    def __str__(self) -> str:
        return self.text


class Client:
    def subscribe(self, topics):
        self.topics = topics
        return 0, 7


def test_refused_login_stays_visible(make_capture, caplog):
    cap = make_capture()
    cap._on_connect(Client(), None, None, Code(0x87, "Not authorized"))
    cap._on_disconnect(Client(), None, None, Code(0x80, "Unspecified error"))  # paho's follow-up
    assert cap.mqtt["connected"] is False
    assert cap.mqtt["error"] == "broker localhost:1883 refused the connection: Not authorized"
    for _ in range(5):  # retries do not repeat the same line
        cap._on_connect(Client(), None, None, Code(0x87, "Not authorized"))
    assert sum("refused the connection" in r.message for r in caplog.records) == 1
    assert not any("connection lost" in r.message for r in caplog.records)


def test_unresolvable_broker_is_named(make_capture):
    cap = make_capture(MQTT_HOST="nowhere.invalid")
    cap._on_connect_fail(None, None)
    assert cap.mqtt["error"] == "cannot resolve the broker name 'nowhere.invalid'"


def test_connect_subscribe_and_lose(make_capture, caplog):
    cap = make_capture()
    client = Client()
    cap._on_connect(client, None, None, Code(0, "Success"))
    assert cap.mqtt["connected"] and cap.mqtt["error"] is None
    assert [t for t, _ in client.topics] == list(cap.cfg.topics)
    cap._on_subscribe(client, 7, 7, [Code(0, "Granted QoS 0"), Code(0x87, "Not authorized"), Code(0, "Granted QoS 0")])
    assert list(cap.mqtt["subscriptions"].values()) == ["ok", "refused (Not authorized)", "ok"]
    cap._on_disconnect(client, None, None, Code(0x8E, "Session taken over"))
    assert cap.mqtt["error"] == "connection lost: Session taken over"
    assert any("connection lost" in r.message for r in caplog.records)
