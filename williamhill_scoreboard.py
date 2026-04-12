import asyncio
import base64
import io
import os
import re
import ssl
import argparse
from datetime import datetime

import cbor2
import websockets

# ─────────────────────────────────────────────────────────────────
# KONFIGURACJA
# ─────────────────────────────────────────────────────────────────

WS_URL = (
    "wss://scoreboards-push.williamhill.com/diffusion"
    "?ty=WB&v=18&ca=8&r=300000"
    "&sp=%7B%22src%22%3A%22traf_sb_football%22"
    "%2C%22origin%22%3A%22https%3A%2F%2Fsports.whcdn.net%22%7D"
)
HEADERS = {
    "Origin":     "https://sports.whcdn.net",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}
HANDSHAKE_B64 = (
    "AHUBFT5zY29yZWJvYXJkcy92MS9lcG9jaAAAgIAQAQD"
    "/////B/////8H/////wf/////BwA="
)

PERIOD_LABELS = {
    "FIRST_HALF":  "1st Half",
    "SECOND_HALF": "2nd Half",
    "HALF_TIME":   "Half Time",
    "FULL_TIME":   "Full Time",
    "PRE_MATCH":   "Pre Match",
    "EXTRA_TIME":  "Extra Time",
    "PENALTIES":   "Penalties",
}

EVENT_TYPES_LIST = sorted([
    "GOAL", "YELLOW", "RED_CARD",
    "FREE_KICK", "DANGEROUS_FREE_KICK",
    "CORNER", "GOAL_KICK", "THROW_IN", "PENALTY", "KICK_OFF",
    "SHOT_ON_TARGET", "SHOT_OFF_TARGET", "SHOT_BLOCKED",
    "DANGEROUS_ATTACK", "ATTACK", "SAFE", "BALL_SAFE", "CLEARANCE",
    "FDANGER", "DANGER",
    "OFFSIDE", "SUBSTITUTION",
    "GAME_FINISHED", "STOP_GAME",
    "HALF_TIME", "FULL_TIME",
    "STOP_1_ST_HALF", "STOP_2_ND_HALF",
    "FIRST_HALF", "SECOND_HALF",
], key=len, reverse=True)

NO_TEAM_EVENTS = {
    "HALF_TIME", "FULL_TIME", "GAME_FINISHED", "STOP_GAME",
    "STOP_1_ST_HALF", "STOP_2_ND_HALF",
    "FIRST_HALF", "SECOND_HALF",
    "BALL_SAFE",
}

GAME_OVER_EVENTS = {"GAME_FINISHED", "STOP_GAME", "FULL_TIME"}

# ─────────────────────────────────────────────────────────────────
# SUBSCRIBE / KEEPALIVE
# ─────────────────────────────────────────────────────────────────

def make_subscribe(topic: str) -> bytes:
    topic_b  = topic.encode("utf-8")
    selector = bytes([0x3E]) + topic_b
    return bytes([0x00, 0x03, 0x02, len(selector)]) + selector


def make_keepalive() -> bytes:
    return bytes([0x06]) + os.urandom(3) + bytes([0x2b])

# ─────────────────────────────────────────────────────────────────
# CBOR
# ─────────────────────────────────────────────────────────────────

def cbor_decode_all(data: bytes) -> list:
    results = []
    stream  = io.BytesIO(data)
    decoder = cbor2.CBORDecoder(stream)
    size    = len(data)
    while stream.tell() < size:
        try:
            results.append(decoder.decode())
        except Exception:
            pos = stream.tell()
            if pos >= size:
                break
            stream.seek(pos + 1)
    return results


def collect_strings(obj) -> list:
    out = []
    if isinstance(obj, str):
        out.append(obj)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            out += collect_strings(k)
            out += collect_strings(v)
    elif isinstance(obj, (list, tuple)):
        for item in obj:
            out += collect_strings(item)
    return out


def deep_find(obj, key: str):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = deep_find(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for item in obj:
            r = deep_find(item, key)
            if r is not None:
                return r
    return None

# ─────────────────────────────────────────────────────────────────
# PARSOWANIE EVENTU
# ─────────────────────────────────────────────────────────────────

def find_in_bytes(data: bytes, targets: list[str]) -> str | None:
    for target in targets:
        if target.encode("ascii") in data:
            return target
        if len(target) > 5:
            fragment = target.encode("ascii")[:5]
            if fragment in data:
                return target
    return None


def ascii_runs(payload: bytes, min_len: int = 3) -> list[str]:
    runs, i = [], 0
    while i < len(payload):
        j = i
        while j < len(payload) and 0x20 <= payload[j] <= 0x7E:
            j += 1
        if j - i >= min_len:
            runs.append(payload[i:j].decode("ascii"))
        i = max(j + 1, i + 1)
    return runs


def parse_event(raw: bytes) -> dict:
    payload = raw[6:]

    cbor_team  = None
    cbor_etype = None
    try:
        values = cbor_decode_all(payload)
        for v in values:
            t = deep_find(v, "teamType")
            if isinstance(t, str) and t not in ("", "MATCH"):
                cbor_team = t
            e = deep_find(v, "type")
            if isinstance(e, str) and e.upper() in [x.upper() for x in EVENT_TYPES_LIST]:
                cbor_etype = e.upper()
            if cbor_team and cbor_etype:
                break
        if not cbor_etype:
            for v in values:
                e = deep_find(v, "type")
                if isinstance(e, str):
                    cbor_etype = e.upper()
                    break
    except Exception:
        pass

    byte_etype = find_in_bytes(payload, EVENT_TYPES_LIST)
    byte_team  = None
    byte_tdbg  = "-"

    if not cbor_team:
        for marker, team in [
            (b"HOME",   "HOME"), (b"AWAY",   "AWAY"),
            (b"DHOME",  "HOME"), (b"DAWAY",  "AWAY"),
            (b"dHOME",  "HOME"), (b"dAWAY",  "AWAY"),
            (b"kDHOME", "HOME"), (b"kDAWAY", "AWAY"),
            (b"HDAWAY", "AWAY"), (b"HDHOME", "HOME"),
            (b"ODHOME", "HOME"), (b"ODAWAY", "AWAY"),
            (b"edAWAY", "AWAY"), (b"edHOME", "HOME"),
        ]:
            if marker in payload:
                byte_team = team
                byte_tdbg = marker.decode()
                break

        if not byte_team:
            for etype in EVENT_TYPES_LIST:
                fragment  = etype.encode("ascii")[:max(4, len(etype)-2)]
                idx       = payload.find(fragment)
                if idx < 2:
                    continue
                team_byte = payload[idx - 2]
                if team_byte == 0x48:
                    byte_team = "HOME"; byte_tdbg = "pre-H"; break
                pre = payload[idx - 3] if idx >= 3 else 0
                if team_byte == 0x41 and 0x40 <= pre <= 0x7F:
                    byte_team = "AWAY"; byte_tdbg = "pre-A"; break

        if not byte_team:
            if b"HOM" in payload:   byte_team = "HOME"; byte_tdbg = "HOM"
            elif b"AWA" in payload: byte_team = "AWAY"; byte_tdbg = "AWA"

    final_team  = cbor_team  or byte_team
    final_etype = cbor_etype or byte_etype
    team_source = "cbor" if cbor_team else (byte_tdbg if byte_team else "-")

    return {
        "event_type":    final_etype,
        "team":          final_team,
        "team_source":   team_source,
        "cbor_team":     cbor_team,
        "cbor_etype":    cbor_etype,
        "byte_team":     byte_team,
        "byte_tdbg":     byte_tdbg,
        "debug_strings": ascii_runs(payload),
    }

# ─────────────────────────────────────────────────────────────────
# PARSOWANIE STANU MECZU
# ─────────────────────────────────────────────────────────────────

def parse_match_state(raw: bytes) -> dict:
    state  = {}
    values = cbor_decode_all(raw[2:])
    strings = []
    for v in values:
        strings += collect_strings(v)

    for s in strings:
        if " vs " in s and len(s) < 100:
            state["match"] = s.strip(); break
    for s in strings:
        if s in ("FOOTBALL", "BASKETBALL", "TENNIS"):
            state["sport"] = s; break
    for s in strings:
        if s in PERIOD_LABELS:
            state["period"] = PERIOD_LABELS[s]; break

    def find_score(obj):
        if isinstance(obj, dict):
            h = obj.get("home", obj.get("dhome"))
            a = obj.get("away", obj.get("daway"))
            if isinstance(h, int) and isinstance(a, int) and h <= 20 and a <= 20:
                return h, a
            for v in obj.values():
                r = find_score(v)
                if r: return r
        elif isinstance(obj, list):
            for item in obj:
                r = find_score(item)
                if r: return r
        return None

    for v in values:
        r = find_score(v)
        if r:
            state["score"] = {"home": r[0], "away": r[1]}; break

    for v in values:
        t = deep_find(v, "ticking")
        if t is not None:
            state["clock_ticking"] = bool(t); break

    stat_keys = {
        "shotsOnTarget":  "shots_on",
        "shotsOffTarget": "shots_off",
        "corners":        "corners",
        "attacks":        "attacks",
        "goalKicks":      "goal_kicks",
    }

    def find_stats(obj):
        found = {}
        if isinstance(obj, dict):
            for raw_key, clean_key in stat_keys.items():
                if raw_key in obj:
                    sub = obj[raw_key]
                    if isinstance(sub, dict):
                        h = sub.get("home", sub.get("dhome"))
                        a = sub.get("away", sub.get("daway"))
                        if isinstance(h, int) and isinstance(a, int):
                            found[clean_key] = {"home": h, "away": a}
            for v in obj.values():
                found.update(find_stats(v))
        elif isinstance(obj, list):
            for item in obj:
                found.update(find_stats(item))
        return found

    stats = {}
    for v in values:
        stats.update(find_stats(v))
    if stats:
        state["stats"] = stats

    return state

# ─────────────────────────────────────────────────────────────────
# HEXDUMP
# ─────────────────────────────────────────────────────────────────

def hexdump(raw: bytes, width: int = 16) -> str:
    lines = []
    for i in range(0, len(raw), width):
        chunk   = raw[i:i + width]
        hex_p   = " ".join(f"{b:02x}" for b in chunk).ljust(width * 3 - 1)
        ascii_p = "".join(chr(b) if 0x20 <= b <= 0x7E else "." for b in chunk)
        lines.append(f"    {i:04x}  {hex_p}  {ascii_p}")
    return "\n".join(lines)

# ─────────────────────────────────────────────────────────────────
# WYŚWIETLANIE
# ─────────────────────────────────────────────────────────────────

def print_match_state(state: dict):
    print(f"  Match  : {state.get('match', '?')}")
    print(f"  Sport  : {state.get('sport', '?')}")
    print(f"  Period : {state.get('period', '?')}")
    score = state.get("score", {})
    if score:
        print(f"  Score  : {score.get('home','?')} - {score.get('away','?')}")
    ticking = state.get("clock_ticking")
    if ticking is not None:
        print(f"  Clock  : {'running' if ticking else 'stopped'}")
    stats = state.get("stats", {})
    if stats:
        print("  Stats  :")
        for key, lbl in [
            ("shots_on",   "Shots on target "),
            ("shots_off",  "Shots off target"),
            ("corners",    "Corners         "),
            ("attacks",    "Attacks         "),
            ("goal_kicks", "Goal kicks      "),
        ]:
            if key in stats:
                h = stats[key].get("home", "?")
                a = stats[key].get("away", "?")
                print(f"    {lbl}: {h} - {a}")


def print_event_line(
    now_s: str,
    etype: str | None,
    team: str | None,
    team_source: str,
    info: dict,
    raw: bytes,
):
    no_team    = etype in NO_TEAM_EVENTS
    team_disp  = team  if team  else "???"
    etype_disp = etype if etype else "UNKNOWN"

    if no_team:
        print(f"  {now_s}        {etype_disp}")
    else:
        print(f"  {now_s}  {team_disp:<4}  {etype_disp}")

    missing_team = not no_team and not team
    unknown_evt  = not etype

    if unknown_evt or missing_team:
        dbg = info.get("debug_strings", [])
        print(f"    ! cbor_team={info.get('cbor_team')}  "
              f"cbor_type={info.get('cbor_etype')}  "
              f"byte_team={info.get('byte_tdbg')}  "
              f"ascii={dbg}")
        print(hexdump(raw))

# ─────────────────────────────────────────────────────────────────
# KEEPALIVE
# ─────────────────────────────────────────────────────────────────

async def keepalive(ws):
    try:
        while True:
            await asyncio.sleep(20)
            await ws.send(make_keepalive())
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────────
# GŁÓWNA PĘTLA Z AUTO-RECONNECT
# ─────────────────────────────────────────────────────────────────

async def listen(event_id: str):
    ssl_ctx   = ssl.create_default_context()
    game_over = False

    last_team:    str | None      = None
    last_team_ts: datetime | None = None

    topic_main      = f"scoreboards/v1/OB_EV{event_id}"
    topic_incidents = f"scoreboards/v1/OB_EV{event_id}/incidents"

    print("=" * 55)
    print(f"  William Hill — Live Scoreboard")
    print(f"  Match : OB_EV{event_id}")
    print(f"  Start : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    while True:
        try:
            async with websockets.connect(
                WS_URL,
                additional_headers=HEADERS,
                ssl=ssl_ctx,
                ping_interval=None,
                ping_timeout=None,
                max_size=10 * 1024 * 1024,
                close_timeout=5,
            ) as ws:
                print("[✓] Connected\n")
                await ws.send(base64.b64decode(HANDSHAKE_B64))
                await ws.recv()
                print("[✓] Handshake OK\n")

                # Subskrybuj oba tematy
                await ws.send(make_subscribe(topic_main))
                await ws.send(make_subscribe(topic_incidents))
                print(f"[✓] Subscribed: {topic_main}")
                print(f"[✓] Subscribed: {topic_incidents}\n")
                print("═" * 55)
                print(f"  {'TIME':8}  {'TEAM':<4}  EVENT")
                print("─" * 45)

                ping_task = asyncio.create_task(keepalive(ws))
                try:
                    async for message in ws:
                        raw   = base64.b64decode(message) if isinstance(message, str) else message
                        ftype = raw[0] if raw else 0xFF
                        now   = datetime.now()
                        now_s = now.strftime("%H:%M:%S")

                        if ftype == 0x01:
                            await ws.send(raw)
                            continue
                        if ftype in (0x02, 0x03):
                            continue
                        if ftype == 0x06 and (b"epoch" in raw or len(raw) <= 6):
                            continue

                        if ftype == 0x04:
                            # Ignoruj MATCH STATE z /incidents (za dużo szumu)
                            if topic_incidents.encode() not in raw:
                                print()
                                print_match_state(parse_match_state(raw))
                                print("─" * 45)
                                print(f"  {'TIME':8}  {'TEAM':<4}  EVENT")
                                print("─" * 45)

                        elif ftype == 0x05:
                            info  = parse_event(raw)
                            etype = info.get("event_type")

                            if not etype and len(raw) <= 45:
                                continue

                            if etype in GAME_OVER_EVENTS:
                                game_over = True

                            if info.get("team"):
                                last_team    = info["team"]
                                last_team_ts = now

                            team        = info.get("team")
                            team_source = info.get("team_source", "-")
                            if not team and etype and etype not in NO_TEAM_EVENTS:
                                age = (now - last_team_ts).total_seconds() if last_team_ts else 999
                                if last_team and age < 30:
                                    team        = last_team
                                    team_source = f"ctx,{age:.1f}s"

                            print_event_line(now_s, etype, team, team_source, info, raw)

                        elif ftype == 0x06:
                            info  = parse_event(raw)
                            etype = info.get("event_type")
                            if etype:
                                team        = info.get("team")
                                team_source = info.get("team_source", "-")
                                if not team and etype not in NO_TEAM_EVENTS:
                                    age = (now - last_team_ts).total_seconds() if last_team_ts else 999
                                    if last_team and age < 30:
                                        team        = last_team
                                        team_source = f"ctx,{age:.1f}s"
                                print_event_line(now_s, etype, team, team_source, info, raw)

                        elif ftype == 0x00 and len(raw) > 10:
                            text  = raw.decode("utf-8", errors="replace")
                            clean = re.sub(r'[^\x20-\x7E]', ' ', text)
                            print(f"\n  [{re.sub(' +', ' ', clean).strip()[:80]}]")
                            print("─" * 45)

                finally:
                    ping_task.cancel()
                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass

        except websockets.exceptions.ConnectionClosedError as e:
            if game_over:
                print(f"\n[✓] Match finished.")
                return
            print(f"\n[!] Reconnecting in 3s... ({e})")
            await asyncio.sleep(3)
        except websockets.exceptions.WebSocketException as e:
            if game_over:
                print(f"\n[✓] Match finished.")
                return
            print(f"\n[!] Reconnecting in 3s... ({e})")
            await asyncio.sleep(3)
        except Exception as e:
            print(f"\n[!] Error: {type(e).__name__}: {e}")
            print("[~] Reconnecting in 5s...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="William Hill Live Scoreboard")
    parser.add_argument(
        "event_id",
        help="Event ID, np. 39319952",
    )
    args = parser.parse_args()

    try:
        asyncio.run(listen(args.event_id))
    except KeyboardInterrupt:
        print("\n[✓] Stopped.")