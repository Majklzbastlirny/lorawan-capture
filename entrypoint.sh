#!/bin/sh
# Run as PUID:PGID (Unraid's nobody:users = 99:100 by default) with group-writable files,
# so the daily files on the share can be read and deleted over SMB like any other.
set -e
PUID="${PUID:-99}"
PGID="${PGID:-100}"
DATA_DIR="${DATA_DIR:-/data}"
umask 0002
mkdir -p "$DATA_DIR"
if [ "$(id -u)" = "0" ]; then
    chown "$PUID:$PGID" "$DATA_DIR" || echo "warning: cannot chown $DATA_DIR" >&2
    exec setpriv --reuid="$PUID" --regid="$PGID" --clear-groups python -m lorawan_capture
fi
exec python -m lorawan_capture
