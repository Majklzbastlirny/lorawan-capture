## How to read these files

Each `frames_YYYY-MM-DD.db` is an SQLite 3 database holding one local day of what the
LoRaWAN gateways reported over MQTT (ChirpStack MQTT Forwarder / Gateway Bridge topics
`<prefix>/gateway/<id>/event/up`, `…/event/stats`, `…/state/conn`). A name ending in
`_partial-HHMM.db` is a consistent snapshot of a day that was still being recorded at that
local time. `gateways.json` maps gateway IDs to the names given in the web UI:
`{"<gateway id>": {"name": …, "first_seen": …, "last_seen": …}}`.
`devices.json` lists the own devices as set in the web UI, `{"devices": {"<dev_addr>": {"name": …, "added": …}}, …}`.

Open them with any SQLite tool (`sqlite3 frames_2026-10-03.db`, DB Browser for SQLite) or:

```python
import sqlite3
con = sqlite3.connect("frames_2026-10-03.db")
for row in con.execute("SELECT gateway_id, count(*) FROM frames GROUP BY 1"):
    print(row)
```

Several days at once (pandas):

```python
import glob, sqlite3
import pandas as pd
frames = pd.concat(pd.read_sql("SELECT * FROM frames", sqlite3.connect(f))
                   for f in sorted(glob.glob("frames_*.db")))
```

### Table `frames`: one row per packet and gateway that received it

The same transmission heard by three gateways is three rows with the same `frame_key`.

| Column | Meaning |
|---|---|
| `id` | row number within the file |
| `ts_utc` | when the logger received it, ISO 8601 UTC (`2026-10-03T08:10:33.589613+00:00`); about 0.1 s after the radio reception |
| `gw_time` | the gateway's own reception time if it sends one (GPS gateways), naive ISO UTC; usually empty |
| `frame_key` | SHA-1 of the raw payload: equal for every reception of one transmission |
| `gateway_id` | gateway EUI-64, hex (names in `gateways.json`) |
| `is_own` | 1 if `dev_addr` was one of the own devices when the row was recorded (the list can change later; `devices.json` holds the current one) |
| `phy_len`, `phy_hex` | the complete raw LoRa payload (LoRaWAN PHYPayload); every column below except radio metadata is derived from it |
| `mtype`, `major` | message type and major version from the first byte (MHDR): `JoinRequest`, `JoinAccept`, `UnconfirmedDataUp`, `UnconfirmedDataDown`, `ConfirmedDataUp`, `ConfirmedDataDown`, `RejoinRequest`, `Proprietary`. For packets that are not LoRaWAN these are just whatever the first byte happens to say |
| `is_downlink` | 1 = an overheard downlink (another gateway's or network's transmission), 0 = uplink, empty = not a data frame (join, rejoin, proprietary, foreign) |
| `dev_addr` | device address, 8 hex digits as ChirpStack shows it (sent little-endian on air) |
| `nwk_id` | top 7 bits of `dev_addr` (the NwkID of NetID types 0/1) |
| `join_eui`, `dev_eui` | from join requests, 16 hex digits. The DevNonce has no column: it is bytes 17–18 of `phy_hex` (hex characters 35–38), little-endian; random in LoRaWAN 1.0.0–1.0.3, a count of the join attempts from 1.0.4 on |
| `fcnt` | frame counter, the 16 bits sent on air |
| `fport` | application port; empty when there is no payload |
| `adr`, `ack`, `adr_ack_req`, `class_b` | FCtrl bits (`class_b` means FPending on downlinks). `adr_ack_req`: the device has had no downlink for a long while and asks whether the network still hears it |
| `fopts_len`, `fopts_hex` | MAC commands carried in the header |
| `frm_len`, `frm_hex` | the application payload, **encrypted** (AES-128 with the device's AppSKey, or NwkSKey on port 0); readable only with the device keys (ChirpStack shows it decrypted for its own devices) |
| `mic_hex` | the last 4 bytes (message integrity code) |
| `freq_hz` | channel frequency, Hz |
| `sf`, `bw_hz` | LoRa spreading factor and bandwidth (Hz); empty for FSK |
| `code_rate` | ChirpStack code-rate number: `1` = 4/5, `2` = 4/6, `3` = 4/7, `4` = 4/8; empty for FSK |
| `rssi` | dBm (some gateways report the signal, others the whole channel; see the site notes) |
| `snr` | dB |
| `crc` | `CRC_OK`, `BAD_CRC` (only stored if the gateway forwards bad packets), `NO_CRC` |

Indexes: `ix_dev (dev_addr, ts_utc)`, `ix_key (frame_key)`, `ix_ts (ts_utc)`.

Layout of a LoRaWAN data frame in `phy_hex` (LoRaWAN 1.0.x):
`MHDR (1) | DevAddr (4, LE) | FCtrl (1) | FCnt (2, LE) | FOpts (0-15) | FPort (1) | FRMPayload | MIC (4)`.

### Table `stats`: one row per gateway stats report (typically every 30 s)

Counts since the gateway's previous report.

| Column | Meaning |
|---|---|
| `ts_utc`, `gateway_id` | when it arrived, which gateway |
| `rx_received` | packets the radio received, **including** bad CRC |
| `rx_crc_ok` | of those, with a good CRC (`rx_received - rx_crc_ok` = bad CRC) |
| `tx_received` | downlinks the gateway was asked to send, from every network server it serves |
| `tx_emitted` | downlinks actually transmitted |

### Tables `stats_info`, `stats_counts`, `stats_duty`: the rest of each report

All rows of one report share `ts_utc` and `gateway_id` with its `stats` row.

- `stats_info`: `gw_time` (the gateway's clock when it sent the report), `config_version`,
  `latitude` / `longitude` / `altitude`, `metadata` (JSON text, e.g. model, software versions,
  `concentrator_temp` in °C as a string).
- `stats_counts`: `kind`, `key`, `count`. Kinds: `rx_freq` / `tx_freq` (key = frequency in Hz),
  `rx_mod` / `tx_mod` (key = `sf7`, `sf7_bw250`, `fsk`, …), `tx_status` (key = `OK`,
  `TOO_LATE`, `TOO_EARLY`, `COLLISION_PACKET`, …). ChirpStack Concentratord counts only
  good-CRC packets in its `rx_*` breakdowns.
- `stats_duty`: per regulatory band: `window_s`, `band`, `freq_min`, `freq_max`, `load_max_s`
  (transmit seconds allowed in the window), `load_tracked_s` (used).

### Table `conn`: gateway online/offline

`ts_utc`, `gateway_id`, `state` (`ONLINE` / `OFFLINE`), recorded on a change, and once per
gateway each time the logger starts.

### Useful queries

```sql
-- transmissions (not receptions) per hour
SELECT substr(ts_utc, 1, 13) AS hour_utc, count(DISTINCT frame_key) FROM frames GROUP BY 1;

-- which gateways heard each uplink of the own devices
SELECT dev_addr, fcnt, group_concat(gateway_id), max(rssi) FROM frames
WHERE is_own = 1 AND is_downlink = 0 GROUP BY dev_addr, fcnt;

-- traffic per channel, spreading factor and gateway
SELECT freq_hz, sf, gateway_id, count(*) FROM frames GROUP BY 1, 2, 3;

-- bad-CRC share per report
SELECT ts_utc, gateway_id, 1.0 * (rx_received - rx_crc_ok) / rx_received FROM stats WHERE rx_received > 0;

-- duty-cycle use (%) per band
SELECT ts_utc, gateway_id, band, 100.0 * load_tracked_s / load_max_s FROM stats_duty WHERE load_max_s > 0;
```

### Things to know

- Times inside the files are UTC; the files split at local midnight. A row is filed by the
  time it was received, so within a second of midnight one can land in the neighbouring file.
- Gateways forward only good-CRC packets unless set otherwise (Concentratord:
  `disable_crc_filter`), so `frames` normally has no bad packets; `stats` counts them.
- Packets that are not LoRaWAN (other systems on the same channels) are stored too. Spreading
  factors 5 and 6 are never LoRaWAN in EU868.
- No rows for a while means nothing was received or the logger was not running; the
  ~30 s rhythm of `stats` shows which.
- `frames` and `stats` are identical to those of the older recordings made with
  `lorawan_logger.py`, so the same code reads both.
