"""Settings, all from environment variables (see .env.example)."""

from __future__ import annotations

import logging
import os
import socket
from dataclasses import dataclass, field
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

log = logging.getLogger(__name__)


def _bool(value: str | None) -> bool:
    return (value or "").strip().lower() in ("1", "true", "yes", "on")


def _list(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _zone(name: str | None) -> ZoneInfo:
    try:
        return ZoneInfo((name or "UTC").lstrip(":"))
    except (ZoneInfoNotFoundError, ValueError):
        log.warning("unknown TZ %r, using UTC", name)
        return ZoneInfo("UTC")


@dataclass(frozen=True)
class Config:
    mqtt_host: str
    mqtt_port: int = 1883
    mqtt_username: str | None = None
    mqtt_password: str | None = None
    mqtt_tls: bool = False
    mqtt_client_id: str = "lorawan-capture"
    topics: tuple[str, ...] = ()
    own_devaddrs: frozenset[str] = frozenset()
    gateway_names: dict[str, str] = field(default_factory=dict)
    data_dir: Path = Path("/data")
    tz: ZoneInfo = field(default_factory=lambda: ZoneInfo("UTC"))
    keep_days: int = 0
    health_silence_minutes: float = 10.0  # no message for this long: the capture is unhealthy
    commit_seconds: float = 2.0
    web_port: int = 8080
    web_username: str | None = None
    web_password: str | None = None

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Config:
        env = dict(os.environ if env is None else env)
        host = env.get("MQTT_HOST", "").strip()
        if not host:
            raise SystemExit("MQTT_HOST is not set (see .env.example)")
        prefix = env.get("TOPIC_PREFIX", "eu868").strip().strip("/")
        topics = _list(env.get("MQTT_TOPICS")) or [
            f"{prefix}/gateway/+/event/up",
            f"{prefix}/gateway/+/event/stats",
            f"{prefix}/gateway/+/state/conn",
        ]
        names = {}
        for item in _list(env.get("GATEWAY_NAMES")):
            gateway_id, _, name = item.partition("=")
            if gateway_id.strip() and name.strip():
                names[gateway_id.strip().lower()] = name.strip()
        return cls(
            mqtt_host=host,
            mqtt_port=int(env.get("MQTT_PORT") or 1883),
            mqtt_username=env.get("MQTT_USERNAME") or None,
            mqtt_password=env.get("MQTT_PASSWORD") or None,
            mqtt_tls=_bool(env.get("MQTT_TLS")),
            mqtt_client_id=env.get("MQTT_CLIENT_ID") or f"lorawan-capture-{socket.gethostname()}",
            topics=tuple(topics),
            own_devaddrs=frozenset(addr.lower() for addr in _list(env.get("OWN_DEVADDRS"))),
            gateway_names=names,
            data_dir=Path(env.get("DATA_DIR") or "/data"),
            tz=_zone(env.get("TZ")),
            keep_days=int(env.get("KEEP_DAYS") or 0),
            health_silence_minutes=float(env.get("HEALTH_SILENCE_MINUTES") or 10),
            commit_seconds=float(env.get("COMMIT_SECONDS") or 2),
            web_port=int(env.get("WEB_PORT") or 8080),
            web_username=env.get("WEB_USERNAME") or None,
            web_password=env.get("WEB_PASSWORD") or None,
        )
