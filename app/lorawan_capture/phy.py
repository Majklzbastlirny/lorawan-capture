"""LoRaWAN PHYPayload header decode.

Kept identical to parse_phy() in samples/lorawan_logger.py, so the daily files have the
same column values as the older recordings and the same analysis scripts work on both.
"""

from __future__ import annotations

MTYPE = {0: "JoinRequest", 1: "JoinAccept", 2: "UnconfirmedDataUp",
         3: "UnconfirmedDataDown", 4: "ConfirmedDataUp", 5: "ConfirmedDataDown",
         6: "RejoinRequest", 7: "Proprietary"}


def parse_phy(phy: bytes) -> dict:
    """Decode LoRaWAN PHYPayload header. Returns dict; never raises on short frames."""
    o: dict = {}
    if not phy:
        return o
    mhdr = phy[0]
    mt = (mhdr >> 5) & 0x07
    o["mtype"] = MTYPE.get(mt, "Unknown(%d)" % mt)
    o["major"] = mhdr & 0x03
    o["mic_hex"] = phy[-4:].hex() if len(phy) >= 5 else None
    # MType 2/4 = data uplink, 3/5 = data downlink. Both share the FHDR layout,
    # so parse them identically. (Downlinks appear here because RX1 in EU868
    # reuses the uplink channel, so one gateway overhears another transmitting.)
    if mt in (2, 3, 4, 5) and len(phy) >= 12:
        o["is_downlink"] = 1 if mt in (3, 5) else 0
        o["dev_addr"] = "%08x" % int.from_bytes(phy[1:5], "little")
        o["nwk_id"] = int.from_bytes(phy[1:5], "little") >> 25
        fctrl = phy[5]
        o["adr"] = (fctrl >> 7) & 1
        o["adr_ack_req"] = (fctrl >> 6) & 1
        o["ack"] = (fctrl >> 5) & 1
        o["class_b"] = (fctrl >> 4) & 1   # uplink: ClassB; downlink: FPending
        fol = fctrl & 0x0F
        o["fopts_len"] = fol
        o["fcnt"] = int.from_bytes(phy[6:8], "little")
        o["fopts_hex"] = phy[8:8 + fol].hex() or None
        rest = phy[8 + fol:-4]
        if rest:
            o["fport"] = rest[0]
            o["frm_hex"] = rest[1:].hex() or None
            o["frm_len"] = len(rest) - 1
        else:
            o["frm_len"] = 0
    elif mt == 0 and len(phy) >= 23:                          # join request
        o["join_eui"] = "%016x" % int.from_bytes(phy[1:9], "little")
        o["dev_eui"] = "%016x" % int.from_bytes(phy[9:17], "little")
    return o
