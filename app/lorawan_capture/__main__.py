"""python -m lorawan_capture: capture + web UI in one process."""

from __future__ import annotations

import logging
import os
import signal
import threading

from . import __version__
from .capture import Capture
from .config import Config
from .web import make_server


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper(),
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    log = logging.getLogger("lorawan_capture")
    cfg = Config.from_env()
    log.info("lorawan-capture %s: broker %s:%s%s as %s, data %s, TZ %s, own %s, keep %s",
             __version__, cfg.mqtt_host, cfg.mqtt_port, " (TLS)" if cfg.mqtt_tls else "",
             cfg.mqtt_username or "anonymous", cfg.data_dir, cfg.tz,
             ",".join(sorted(cfg.own_devaddrs)) or "-",
             f"{cfg.keep_days} days" if cfg.keep_days else "everything")
    cfg.data_dir.mkdir(parents=True, exist_ok=True)

    capture = Capture(cfg)
    capture.start_writer()
    capture.start_mqtt()
    server = make_server(capture, cfg)

    def stop(signum, _frame) -> None:
        log.info("signal %d, stopping", signum)
        threading.Thread(target=server.shutdown, daemon=True).start()  # not from this thread

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    log.info("web UI on port %d", cfg.web_port)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        capture.stop()


if __name__ == "__main__":
    main()
