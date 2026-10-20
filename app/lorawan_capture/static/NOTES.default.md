# Site notes

What the data alone does not tell. Kept in `data/NOTES.md` on the server: edit it freely
(it is created from this default only when missing), and every bulk download includes it.

## Gateways (names in `gateways.json`)

- `<gateway id>` "<name>": hardware, antenna, where it stands, since when.
- RSSI: whether each gateway reports the signal's RSSI (SX1302) or the whole channel's (SX1301,
  SX1308: includes the noise), and its noise floor.
- Whether a gateway also forwards to another network (its transmit counts then include those
  downlinks).

## Own devices

- DevAddr `<devaddr>`: what it is, spreading factor, how often it sends.

## Interference and foreign traffic

- Anything that is not LoRaWAN and shows in the data (or costs uplinks without showing): what,
  on which channels, since when.
