# LoRaWAN capture

Records everything ChirpStack v4 gateways publish over MQTT (uplinks, gateway stats, connection
state) into **one SQLite file per day**, with a small web UI: statistics per day, channel
occupancy, a device explorer, gateway health and uptime, and downloads. It only listens: it
needs read access to the broker's gateway topics and nothing else.

Made for a home site on Unraid (the instructions below), but it is one Docker container and
runs wherever Docker does.

## Install on Unraid (Compose Manager plugin)

1. Copy this folder to `/mnt/user/appdata/lorawan-capture`
   (over SMB: `\\<server>\appdata\lorawan-capture`).
2. In that folder copy `.env.example` to `.env`, fill in `MQTT_USERNAME` /
   `MQTT_PASSWORD` and check `MQTT_HOST`.
3. Docker tab, **Compose**, *Add New Stack*, name it `lorawan-capture`. *Edit Stack*,
   paste `docker-compose.yml` as the compose file, save, then **Compose Up**.
   The first start builds the image (about a minute). (The plugin's button names may
   differ slightly between versions.)
4. Check the container log: `MQTT connected` and three `subscribed …` lines. A
   `REFUSED` line means the broker's ACL blocks that topic (uplinks and stats are what
   matter, `state/conn` is optional); a wrong address or password is reported there and
   in the UI header, and the container keeps retrying.
5. Open `http://<server>:8088`; the header should read "Capturing" within a minute.

**Updating:** copy the new files over the folder (your `.env` and `data/` are not part of it, so
they stay) and *Compose Up* again: the image is rebuilt from the folder each time
(`pull_policy: build`). If you changed the port in the stack, keep your own `docker-compose.yml`. From a terminal instead:
`docker compose -f /mnt/user/appdata/lorawan-capture/docker-compose.yml up -d --build`.

Put the folder somewhere else? Change the three `/mnt/user/appdata/lorawan-capture` paths
in `docker-compose.yml`.

## What it stores

| File | Content |
|---|---|
| `data/frames_YYYY-MM-DD.db` | One per local day (`TZ` in the compose file); a new file starts at midnight. Tables below. |
| `data/frames_YYYY-MM-DD.summary.json` | Cached statistics for the UI, written when the day closes (rebuilt automatically after an update that changes them). |
| `data/gateways.json` | Every gateway ever seen (first seen; last heard = last uplink or stats) and the names given to them. |
| `data/devices.json` | Your own devices (DevAddr and name), managed in the UI; see [Own devices](#own-devices). |
| `data/NOTES.md` | Site notes (hardware, interference, known foreign traffic) for anyone reading the data later. Created from a default on first start if missing; edit it freely, it goes into every bulk download. |

Tables in each daily file:

| Table | One row per | Content |
|---|---|---|
| `frames` | received packet and gateway | as the old logger: time, gateway, decoded LoRaWAN header, raw bytes, frequency, SF, bandwidth, code rate, RSSI, SNR, CRC status |
| `stats` | gateway stats report | as the old logger: received, good CRC, downlinks asked and sent, since the previous report |
| `stats_info` | stats report | the gateway's clock, config version, location, metadata (JSON) |
| `stats_counts` | stats report and item | `kind` = `rx_freq` / `tx_freq` (key: Hz), `rx_mod` / `tx_mod` (key: `sf7`, `sf7_bw250`, `fsk` …), `tx_status` (key: `OK`, `TOO_LATE` …) with its count |
| `stats_duty` | stats report and band | duty-cycle window, band, transmit seconds allowed and used |
| `conn` | gateway state change | online / offline |

The `stats*` tables of one report share `ts_utc` and `gateway_id`. Note that Concentratord's
per-channel and per-SF counts include good-CRC packets only.

- Subscribes to `<prefix>/gateway/+/event/up`, `…/event/stats` and `…/state/conn`
  (`TOPIC_PREFIX`, default `eu868`).
- Writes are committed every 2 s, so a crash or power cut loses at most that much.
- Size: an ordinary day is a few MB; a day flooded with foreign traffic can be several hundred MB.
  `KEEP_DAYS` deletes old files automatically.

## Gateways and names

A new gateway needs no configuration: the subscriptions use a wildcard for the gateway
ID, so its traffic is stored from its first packet. It is also added to
`data/gateways.json`, the log says `new gateway …`, and the UI shows a banner until it
has a name.

Every gateway has a **Rename** button in the UI's gateway table (Enter saves, Escape
cancels; an empty name goes back to showing the ID). Names apply at once everywhere and
survive restarts. `GATEWAY_NAMES` in `.env` only fills in names that are missing: a name
set in the UI wins. The daily files store gateway IDs, not names.

## Own devices

Add, rename and remove your devices in the UI: the **Own devices** card on the Overview
(*Add device*, *Rename*, *Remove*), or the buttons on any device's page in the Devices tab.
The list lives in `data/devices.json`. `OWN_DEVADDRS` in `.env` only adds addresses: each one
is taken over once, so a device removed in the UI stays removed after a restart, and an address
added to `.env` later is still picked up.

- A device is known by its DevAddr, the only identity the gateways see. An OTAA device gets a
  new DevAddr each time it joins; add the new one then.
- Rows recorded from now on get `is_own = 1` for it. Rows already in the files keep the value
  they were recorded with, but the UI's numbers (Overview and Devices) always use the current
  list, for past days too.

## Web UI

Three tabs. The capture state (broker, last message, counting up every second, today's file,
free disk) stays at the top, with a second badge when something needs a look (it links to the
Health tab).

**Overview**, per day:
  totals, packets per hour (LoRaWAN / other / SF5, each on its own scale), gateways (with
  their **uptime**, below), gateway reports (bad CRC, how much of what a gateway reported was
  stored, downlink results, duty cycle, clock offset, config version), own devices' reception
  per gateway, channels, and **channel occupancy**: how much of each hour every channel was on air, per
  gateway (the airtime of the packets it decoded, from their length, spreading factor and
  bandwidth; signals it cannot decode, like a nearby RFID reader, do not show). A banner warns when a
  channel spends 5 % of an hour or more on packets that are not LoRaWAN: a flood of SF5 packets
  once took about 40 % of 867.9 MHz here, while normal traffic stays well under 1 %.

Gateway uptime and state come from the stats report each gateway sends about every 30 s:

- *Uptime* is the share of the recorded time a gateway was reporting. It counts as down when two
  or more reports in a row are missing (over a minute at 30 s), not for a single one. Times when
  no gateway at all was heard are left out: the capture, the broker or every gateway was down
  then, and the reports cannot tell which (so a restart of the capture costs no gateway any
  uptime). Hover it for the times without reports and the disconnects from the broker.
- *State*: today, what the gateway reports now; for a past day, whether it was still reporting
  when the day ended.

**Devices**, over any range of days (today, 7 or 30 days, everything, or from–to):

- Every LoRaWAN device heard: network (from the DevAddr: TTN, private, or the NetID), uplinks
  heard and sent, the share lost, interval, spreading factors, median RSSI and share heard per
  gateway, battery, the share of uplinks with ADRACKReq (no downlink for a long while, so the
  device asks whether the network still hears it), last heard. Sortable and searchable; click a
  device for its page.
- A device's page: the totals, uplinks heard and lost per hour (up to a week) or per day, signal
  per gateway, battery over time, every MAC command it sent or was sent (overheard downlinks),
  its latest uplinks, and the downlinks to it that a gateway overheard.
- **CSV:** *Download CSV* on a device's page gives every uplink of the range (no 31-day limit,
  it is written while it downloads), one row each with the radio data and RSSI/SNR per gateway;
  the device list has its own *Download CSV*. When the browser uses decimal commas (Czech
  settings) the file is made for Excel there: `;` between fields, decimal commas; the other link
  gives a plain `,` / `.` CSV for scripts.
- *Inspect* on any packet: every byte labelled (LoRaWAN fields, or, for other systems, the
  bytes as text and after XOR 0x85), and every gateway that received it with RSSI, SNR, channel.
- Join requests (DevEUI and JoinEUI are sent unencrypted, and so is the DevNonce: random in
  LoRaWAN 1.0.0–1.0.3, counting every join attempt from 1.0.4 on, which tells the versions apart).
- The other radio systems sharing the channels, grouped by their header, with an example packet
  each, and a chart per system of its packets per hour (up to a week) or per day. The charts
  share one time axis, so systems that wake up together line up, and each has its own scale
  (the six biggest systems; *Show all* for the rest). There, *Packets* are transmissions: the
  same bytes heard by two gateways within 1.5 s count once, but heard again by the same gateway
  they count again (some systems repeat identical packets a fraction of a second apart).

What the numbers mean:

- *Sent* comes from the frame counter: a jump from 100 to 103 means 101 and 102 were sent
  but no gateway heard them (*lost*). The counter goes over the air as 16 bits, so after 65535 comes 0:
  that counts as the next uplink, not as a reset (a device that sends every 30 s wraps about
  every 23 days). Jumps across a time when nothing was being recorded
  (no gateway stats report for 5 minutes: logger, broker or gateways down) are not counted
  as lost but shown apart, as sent while nothing was recorded.
- The payloads are encrypted; what is shown comes from the headers. MAC commands (battery and
  link margin from DevStatusAns, ADR settings, channels …) are unencrypted in LoRaWAN 1.0.x;
  LoRaWAN 1.1 devices encrypt them, and their decodes are then meaningless.
- The first look at a day full of foreign traffic (a flood) reads its whole file, a few seconds
  per such day; what was worked out is kept with the day's summary, so later looks are quick.
- The device list and the device pages cover at most 31 days at once (the last 31 of a longer
  range; the page says so): they hold every uplink of the range in memory, about 250 MB for a
  month here. Join requests and other systems have no limit.

**Health**: what is checked right now, and the last 14 days (see [Health and alerts](#health-and-alerts)).

Downloads (on the Overview):

- **Downloads:** past days download as stored. Today's file can be downloaded any time;
  you get a consistent snapshot named `…_partial-HHMM.db` while capture goes on.
  `.db.gz` is about four times smaller.
- **Bulk downloads:** last 7 days, last 30 days, everything, or any from–to range, as one
  `.zip` (built while it downloads, so any size works). It holds the daily files, `gateways.json`,
  `devices.json` and a `README.md`: what the export contains, how to read every table and column, and the site
  notes from `data/NOTES.md`. The same guide is at `/download/README.md`.
- No login unless `WEB_USERNAME` / `WEB_PASSWORD` are set: keep it on the LAN.

## Health and alerts

The Health tab shows:

- **Problems**: the capture is not recording properly. The writer stopped, no message has
  arrived for `HEALTH_SILENCE_MINUTES` (10; after a start it waits that long first), less than
  1 GB of disk is left, or the broker refuses the uplink topic.
- **Warnings**: worth a look, recording goes on.
  - The broker connection is down.
  - A subscription other than uplinks was refused.
  - Messages were dropped, there were errors, or less than 10 GB of disk is left.
  - A gateway has been silent for `HEALTH_SILENCE_MINUTES`. After a week this stops, so a
    gateway switched off for good does not warn forever.
  - An own device is overdue: not heard for 3 times its usual interval, and at least
    15 minutes.
  - A channel spent 5 % of the last hour or more on packets that are not LoRaWAN.
- **Recent days**:
  - when the first and last packet of each day came;
  - the times nothing was recorded at all (no gateway report for 5 minutes), including whole
    missing days;
  - each gateway's uptime and stored share (flagged under 99 %);
  - the own devices' uplinks heard of those sent.

Alerts:

- `/healthz` answers 503 with the problems while there are any, and stays open without a
  login. Docker's healthcheck uses it, so Unraid's Docker tab shows the container as
  *unhealthy* after about 3 minutes. Uptime Kuma (an HTTP monitor on
  `http://<server>:8098/healthz`) or similar can notify you. Warnings never make it fail.
- `/api/health` has everything as JSON: `ok`, `problems`, `warnings` and the details. The
  Health tab shows a ready Home Assistant REST sensor for it (add `username` / `password`
  there if you set a login).

## After a power cut

A hard reset or a power cut in the middle of a write can damage today's file ("database disk
image is malformed"). The capture checks the file whenever it opens it, and on such an error
while running. A damaged file is repaired by itself:

- a copy is kept first, as `frames_<day>.damaged-<time>.db` beside it (not listed as a day:
  delete it once you are happy);
- if only its indexes are hurt, they are rebuilt and nothing is lost;
- otherwise every row that can still be read goes into a new file in its place. Usually only the
  last few rows written before the cut are lost.

The Health tab says what was done until the day ends. While storing fails, `/healthz` fails too,
so Unraid shows the container as unhealthy.

The usual advice on Unraid is to give databases the pool's own path rather than the user share
(`/mnt/user/...`): if appdata lives on the cache pool, mount
`/mnt/cache/appdata/lorawan-capture/data` (with your pool's name) as `/data` in the compose file.

## Settings (`.env`)

| Variable | Default | |
|---|---|---|
| `MQTT_HOST` | – | broker address (required) |
| `MQTT_PORT` | `1883` | |
| `MQTT_USERNAME`, `MQTT_PASSWORD` | – | a user allowed to subscribe to `eu868/gateway/#` is enough |
| `MQTT_TLS` | `false` | |
| `TOPIC_PREFIX` | `eu868` | region prefix of the gateway bridge |
| `MQTT_TOPICS` | – | explicit comma-separated topic list instead of the three defaults |
| `OWN_DEVADDRS` | – | your DevAddrs (hex, comma separated), each taken over once into `data/devices.json`; then manage them in the UI |
| `GATEWAY_NAMES` | – | initial names, `id=name,id=name`; names set in the UI win |
| `KEEP_DAYS` | `0` | delete daily files older than this many days; 0 keeps everything |
| `HEALTH_SILENCE_MINUTES` | `10` | no message for this long is a problem (`/healthz` fails); a gateway silent this long is a warning |
| `WEB_USERNAME`, `WEB_PASSWORD` | – | HTTP basic auth for the UI (`/healthz` and the page icon stay open) |
| `LOG_LEVEL` | `INFO` | |

In `docker-compose.yml`: `TZ` (day boundary), `PUID`/`PGID` (file owner, default
nobody:users so the files can be deleted over SMB), and the port (8088).

## Bad-CRC packets per channel

Gateways normally drop packets with a bad CRC and only report how many there were in total.
To see them per channel and hour, set this in the gateway's `concentratord.toml` (ChirpStack
Concentratord) and restart it:

```toml
[concentratord]
  disable_crc_filter=true
```

The logger then stores them in `frames` with `crc = 'BAD_CRC'`, keeps them out of the
LoRaWAN / other / SF5 counts and the own-device numbers, and the UI shows them separately
("+N bad" per channel). ChirpStack and the Home Assistant integration receive them too:
ChirpStack cannot use them, and the integration counts them per channel only. So it is best
turned on for a while to diagnose, then off again.

## Notes

- If the broker's ACL refuses a subscription, the container log says which, and so does
  the footer of the UI. Uplinks and stats are what matter; `state/conn` is optional.
- Only this one process opens the daily files (the UI goes through it), so SQLite on the
  appdata share is fine. To keep months of data, map `/data` to an array share instead.
- `/healthz` backs the Docker healthcheck: it fails while the capture is not recording (see
  [Health and alerts](#health-and-alerts)), not only when the process is stuck.

## Development

```
python -m pytest tests
```

Needs `pytest` (and `paho-mqtt`, see `requirements.txt`). `tests/test_compat.py` compares rows
column by column with an earlier logger script that is not part of this repository, so it is
skipped unless `chirpstack-api` is installed and `samples/lorawan_logger.py` sits next to this
folder.

Code in `app/lorawan_capture`: `capture.py` (MQTT, writer thread, day rotation), `store.py`
(schema, retention, summaries), `registry.py` (known gateways and their names), `devices.py`
(own devices), `explorer.py` (the Devices tab: per-device statistics, MAC commands, packet
breakdown, CSV), `health.py` (the Health tab, `/healthz`, `/api/health`), `web.py` + `static/index.html` (UI), `phy.py` (LoRaWAN header
decode, same as the old logger), `chirpstack/` (message decoder copied from the Home
Assistant integration).

## License

MIT, see [LICENSE](LICENSE).
