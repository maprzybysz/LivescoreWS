"""
williamhill_scoreboard_v2.py
────────────────────────────
Refaktoryzacja v1 z czystą architekturą opartą na streamach.

Dlaczego NIE używamy oficjalnej biblioteki `diffusion` (Push Technology SDK)?
──────────────────────────────────────────────────────────────────────────────
William Hill używa własnej, zmodyfikowanej implementacji serwera Diffusion,
która jest NIEKOMPATYBILNA z oficjalnym Python SDK w kilku miejscach:

  1. SDK wysyła parametry połączenia (ty, v, ca, r) jako HTTP nagłówki —
     serwer WH oczekuje ich w QUERY STRINGU URL-a.
  2. SDK używa ty=PY, v=25 — WH wymaga ty=WB, v=18.
  3. SDK weryfikuje bajt protokołu '#' (0x23) w odpowiedzi handshake —
     WH odpowiada innym formatem binarnym.
  4. Brak nagłówka Origin → CloudFront zwraca 403 Forbidden.

Wszystkie powyższe punkty to twarde sprawdzenia w SDK nieobejście bez
głębokiego monkey-patchingu prywatnych klas SDK — co byłoby kruche i
trudne w utrzymaniu.

Architektura v2
───────────────
Zamiast ręcznej pętli if/elif na bajtach ramek (jak w v1), logika jest
podzielona na klasy z wyraźnymi obowiązkami:

  DiffusionClient   – niskopoziomowe połączenie WS + handshake + ping/pong
  MatchSession      – stan meczu + routing wiadomości do odpowiednich handlerów
  MatchStateHandler – obsługa snapshotu (nazwy drużyn, nagłówek tabeli)
  IncidentsHandler  – obsługa incydentów (eventy meczowe, koniec meczu)
  ScoreboardPrinter – formatowanie i wypisywanie tabeli (oddzielone od logiki)

Różnice względem v1
───────────────────
  - Brak if/elif ftype w głównej pętli — routing w MatchSession.dispatch()
  - Stan meczu w MatchSession (nie luźne zmienne w listen())
  - Wypisywanie w ScoreboardPrinter (nie funkcje globalne)
  - Łatwiejsze testowanie każdej klasy osobno

Instalacja
──────────
  pip install websockets cbor2
"""

from __future__ import annotations

import asyncio
import argparse
import base64
import io
import ssl
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Set

import cbor2
import websockets

# ─────────────────────────────────────────────────────────────────
# TRYB DEBUG
# ─────────────────────────────────────────────────────────────────

DEBUG: bool = False

# ─────────────────────────────────────────────────────────────────
# ZAPIS RAMEK DO PLIKU — DWA OSOBNE PLIKI
# ─────────────────────────────────────────────────────────────────

# --frames-log-known   → ramki z rozpoznanym typem/drużyną
# --frames-log-unknown → ramki bez rozpoznanego typu i drużyny
FRAMES_LOG_KNOWN:   str = ""
FRAMES_LOG_UNKNOWN: str = ""


def _write_frame_to(path: str, ftype: int, raw: bytes, label: str = ""):
    """Dopisuje ramkę do pliku w formacie hex dump."""
    if not path:
        return
    try:
        now_s = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        width = 16
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"[{now_s}] ftype=0x{ftype:02x} len={len(raw)}{' ' + label if label else ''}\n")
            for i in range(0, len(raw), width):
                chunk   = raw[i:i + width]
                hex_p   = " ".join(f"{b:02x}" for b in chunk).ljust(width * 3 - 1)
                ascii_p = "".join(chr(b) if 0x20 <= b <= 0x7E else "." for b in chunk)
                f.write(f"  {i:04x}  {hex_p}  {ascii_p}\n")
            f.write("\n")
    except Exception:
        pass


def log_frame_known(ftype: int, raw: bytes, etype: str, team: str):
    label = f"etype={etype} team={team}"
    _write_frame_to(FRAMES_LOG_KNOWN, ftype, raw, label)


def log_frame_unknown(ftype: int, raw: bytes):
    _write_frame_to(FRAMES_LOG_UNKNOWN, ftype, raw)


# ─────────────────────────────────────────────────────────────────
# KONFIGURACJA
# ─────────────────────────────────────────────────────────────────

WS_URL = (
    "wss://scoreboards-push.williamhill.com/diffusion"
    "?ty=WB&v=18&ca=8&r=300000"
    "&sp=%7B%22src%22%3A%22traf_sb_football%22"
    "%2C%22origin%22%3A%22https%3A%2F%2Fsports.whcdn.net%22%7D"
)

WS_HEADERS = {
    "Origin":     "https://sports.whcdn.net",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

HANDSHAKE_B64 = (
    "AHUBFT5zY29yZWJvYXJkcy92MS9lcG9jaAAAgIAQAQD"
    "/////B/////8H/////wf/////BwA="
)

PERIOD_LABELS = {
    "FIRST_HALF":              "1H",
    "SECOND_HALF":             "2H",
    "HALF_TIME":               "HT",
    "FULL_TIME":               "FT",
    "PRE_MATCH":               "PRE",
    "EXTRA_TIME_FIRST_HALF":   "ET1",
    "EXTRA_TIME_SECOND_HALF":  "ET2",
    "EXTRA_TIME_HALF_TIME":    "ETH",
    "PENALTIES":               "PEN",
}

# Czas (sekundy) po którym zaczyna się "nadgodzina" w danej połowie — z scoreboard.js
PERIOD_OVERTIME: dict[str, int] = {
    "FIRST_HALF":             45 * 60,
    "SECOND_HALF":            90 * 60,
    "EXTRA_TIME_FIRST_HALF":  105 * 60,
    "EXTRA_TIME_SECOND_HALF": 120 * 60,
}

# Typy eventów które JS ignoruje (dontShow z scoreboard.js) — pomijamy je też
DONT_SHOW: set = {
    "BROADCAST_DELAYED", "COMMENTARY", "OTHER", "REMOVE_MATCH",
    "SOURCE_TRANSMISSION_ONLINE", "SOURCE_TRANSMISSION_OFFLINE",
    "START_PENALTY_SHOOTOUT_FIRST_PENALTY", "START_TIMER", "STOP_TIMER", "UPDATE_TIMER",
}

# Pełna lista z animacji w scoreboard.js + dodatkowe znane
EVENT_TYPES_LIST = sorted([
    # Z animacji scoreboard.js
    "ATTACK", "BALL_SAFE", "BLOCKED_SHOT", "CORNER",
    "DANGER", "DANGEROUS_ATTACK",
    "EXTRA_TIME_FIRST_HALF", "EXTRA_TIME_HALF_TIME",
    "EXTRA_TIME_KICK_OFF", "EXTRA_TIME_SECOND_HALF",
    "FIRST_HALF", "FIRST_HALF_KICK_OFF",
    "FOUL", "FREE_KICK", "GOAL", "GOAL_KICK",
    "HALF_TIME", "KICK_OFF",
    "PENALTY", "PENALTY_MISSED", "PENALTY_RETAKEN",
    "RED_CARD", "SAFE",
    "SECOND_HALF", "SECOND_HALF_KICK_OFF",
    "SHOT_OFF_TARGET", "SHOT_ON_TARGET", "SHOT_WOODWORK",
    "START_1_ST_HALF", "START_1_ST_HALF_OF_EXTRA_TIME",
    "START_2_ND_HALF", "START_2_ND_HALF_OF_EXTRA_TIME",
    "START_PENALTY_SHOOTOUT",
    "STOP_1_ST_HALF", "STOP_1_ST_HALF_OF_EXTRA_TIME",
    "STOP_2_ND_HALF", "STOP_2_ND_HALF_OF_EXTRA_TIME",
    "STOP_GAME", "SUBSTITUTION", "THROW_IN",
    "YELLOW_CARD",
    # Dodatkowe
    "DANGEROUS_FREE_KICK",
    "YELLOW", "YELLOW_RED_CARD",
    "OFFSIDE",
    "END_OF_EXTRA_TIME", "END_OF_MATCH",
    "FULL_TIME", "GAME_FINISHED",
    "POSSIBLE_RED_CARD", "POSSIBLE_GOAL",
    "VAR_REVIEW",
    "BALL_SAFE",
    # Ze scoreboard.js dontDisplayTime
    "EXTRA_TIME_NOT_STARTED", "PENALTIES_NOT_STARTED",
], key=len, reverse=True)

# Eventy bez drużyny — z scoreboard.js (dontDisplayTime + whistleIcons)
NO_TEAM_EVENTS: set = {
    "HALF_TIME", "FULL_TIME", "GAME_FINISHED", "STOP_GAME",
    "BALL_SAFE",
    "STOP_1_ST_HALF", "STOP_2_ND_HALF",
    "STOP_1_ST_HALF_OF_EXTRA_TIME", "STOP_2_ND_HALF_OF_EXTRA_TIME",
    "EXTRA_TIME_HALF_TIME", "EXTRA_TIME_NOT_STARTED",
    "END_OF_EXTRA_TIME", "END_OF_MATCH",
    "FIRST_HALF", "SECOND_HALF",
    "FIRST_HALF_KICK_OFF", "SECOND_HALF_KICK_OFF",
    "EXTRA_TIME_KICK_OFF",
    "START_1_ST_HALF", "START_2_ND_HALF",
    "START_1_ST_HALF_OF_EXTRA_TIME", "START_2_ND_HALF_OF_EXTRA_TIME",
    "START_PENALTY_SHOOTOUT", "PENALTIES_NOT_STARTED",
}

GAME_OVER_EVENTS: set = {
    "GAME_FINISHED", "STOP_GAME", "FULL_TIME",
    "STOP_2_ND_HALF", "END_OF_MATCH",
}

CLOCK_EVENTS: set = {
    "FIRST_HALF", "SECOND_HALF", "HALF_TIME", "FULL_TIME",
    "EXTRA_TIME_FIRST_HALF", "EXTRA_TIME_SECOND_HALF",
    "FIRST_HALF_KICK_OFF", "SECOND_HALF_KICK_OFF",
    "EXTRA_TIME_KICK_OFF", "START_PENALTY_SHOOTOUT",
    "START_1_ST_HALF", "START_2_ND_HALF",
    "START_1_ST_HALF_OF_EXTRA_TIME", "START_2_ND_HALF_OF_EXTRA_TIME",
}

COL_TEAM_WIDTH = 22

# ─────────────────────────────────────────────────────────────────
# CBOR — DEKODOWANIE
# ─────────────────────────────────────────────────────────────────

def cbor_decode_all(data: bytes) -> list:
    """Dekoduje wszystkie wartości CBOR z ciągu bajtów."""
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
            out += collect_strings(k) + collect_strings(v)
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
# PARSOWANIE
# ─────────────────────────────────────────────────────────────────

def extract_team_names(raw: bytes) -> tuple[str, str]:
    """Wyciąga nazwy drużyn z ramki snapshot (0x04)."""
    for offset in (2, 4, 6, 1, 0):
        if offset >= len(raw):
            continue
        try:
            values = cbor_decode_all(raw[offset:])
        except Exception:
            continue
        for v in values:
            name = deep_find(v, "name")
            if isinstance(name, str) and " vs " in name:
                parts = name.split(" vs ", 1)
                return parts[0].strip(), parts[1].strip()
            home_obj = deep_find(v, "homeTeam")
            away_obj = deep_find(v, "awayTeam")
            if home_obj and away_obj:
                h = deep_find(home_obj, "name") if isinstance(home_obj, dict) else None
                a = deep_find(away_obj, "name") if isinstance(away_obj, dict) else None
                if isinstance(h, str) and isinstance(a, str) and h and a:
                    return h, a
            h = deep_find(v, "homeName")
            a = deep_find(v, "awayName")
            if isinstance(h, str) and isinstance(a, str) and h and a:
                return h, a
    for offset in (2, 4, 6):
        try:
            strings = []
            for v in cbor_decode_all(raw[offset:]):
                strings += collect_strings(v)
            for s in strings:
                if " vs " in s and 5 < len(s) < 100:
                    parts = s.split(" vs ", 1)
                    return parts[0].strip(), parts[1].strip()
        except Exception:
            pass
    return "", ""


def find_in_bytes(data: bytes, targets: list[str]) -> str | None:
    for target in targets:
        if target.encode("ascii") in data:
            return target
    for target in targets:
        if len(target) > 5 and target.encode("ascii")[:5] in data:
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


def parse_incident(raw: bytes) -> dict:
    """
    Parsuje incydent meczowy z ramki delta (0x05/0x06).
    Zwraca dict z kluczami: event_type, team, duration_secs, period,
    incident_idx, cbor_etype, cbor_team, debug_strings.
    """
    cbor_team = cbor_etype = cbor_period = cbor_incident_idx = None
    duration_secs = None

    for offset in (5, 6, 7, 9, 10, 4, 3, 2):
        if offset >= len(raw):
            continue
        try:
            values = cbor_decode_all(raw[offset:])
        except Exception:
            continue

        # Obsługa bytes-wrapped CBOR
        expanded = list(values)
        for v in values:
            if isinstance(v, (bytes, bytearray)) and len(v) > 2:
                try:
                    expanded += cbor_decode_all(bytes(v))
                except Exception:
                    pass

        for v in expanded:
            if not cbor_team:
                t = deep_find(v, "teamType")
                if isinstance(t, str) and t not in ("", "MATCH"):
                    cbor_team = t

            if not cbor_etype:
                e = deep_find(v, "type")
                if isinstance(e, str) and e.upper() not in DONT_SHOW:
                    cbor_etype = e.upper()

            if not cbor_period:
                # Szukaj period w time.period lub bezpośrednio
                time_obj = deep_find(v, "time")
                if isinstance(time_obj, dict):
                    p = time_obj.get("period")
                    if isinstance(p, str) and p:
                        cbor_period = p
                    d = time_obj.get("duration")
                    if isinstance(d, int) and 0 <= d < 18000 and duration_secs is None:
                        duration_secs = d
                if not cbor_period:
                    p = deep_find(v, "period")
                    if isinstance(p, str) and p:
                        cbor_period = p

            if duration_secs is None:
                d = deep_find(v, "duration")
                if isinstance(d, int) and 0 <= d < 18000:
                    duration_secs = d

            # Wyciągnij incidentIdx do deduplikacji
            if not cbor_incident_idx:
                idx = deep_find(v, "incidentIdx")
                if isinstance(idx, str) and idx:
                    cbor_incident_idx = idx

        if cbor_etype or cbor_team:
            break

    # Fallback skanowania bajtów
    payload    = raw[5:] if len(raw) > 5 else raw
    byte_etype = find_in_bytes(payload, EVENT_TYPES_LIST)
    byte_team  = None
    byte_tdbg  = "-"

    if not cbor_team:
        for marker, team in [
            (b"dHOME", "HOME"), (b"dAWAY", "AWAY"),
            (b"HOME", "HOME"),  (b"AWAY", "AWAY"),
        ]:
            if marker in payload:
                byte_team = team
                byte_tdbg = marker.decode()
                break

    return {
        "event_type":   cbor_etype or byte_etype,
        "team":         cbor_team  or byte_team,
        "duration_secs": duration_secs,
        "period":       cbor_period,
        "incident_idx": cbor_incident_idx,
        "cbor_team":    cbor_team,
        "cbor_etype":   cbor_etype,
        "byte_team":    byte_team,
        "byte_tdbg":    byte_tdbg,
        "debug_strings": ascii_runs(payload),
    }


def duration_to_minute(secs: int | None, period: str | None) -> str:
    """
    Oblicza minutę meczu z sekundy i okresu — dokładnie jak scoreboard.js:
      - normalna minuta: floor(secs/60) + 1
      - nadgodzina: np. "45' +2" gdy secs > 45*60 w FIRST_HALF
    """
    if secs is None:
        return ""
    base_secs = PERIOD_OVERTIME.get(period or "", 0)
    if base_secs and secs > base_secs:
        overtime = (secs - base_secs) // 60 + 1
        return f"{base_secs // 60}' +{overtime}"
    minute = secs // 60 + 1
    return f"{minute}'"


# ─────────────────────────────────────────────────────────────────
# NISKOPOZIOMOWY KLIENT DIFFUSION
# ─────────────────────────────────────────────────────────────────

class DiffusionClient:
    """
    Niskopoziomowe połączenie z serwerem Diffusion przez WebSocket.
    """

    def __init__(self, on_frame):
        self._on_frame = on_frame
        self._ssl_ctx  = ssl.create_default_context()

    def _make_subscribe(self, topic: str) -> bytes:
        topic_b  = topic.encode("utf-8")
        selector = bytes([0x3E]) + topic_b
        return bytes([0x00, 0x03, 0x02, len(selector)]) + selector

    def _make_pong(self, ping_raw: bytes) -> bytes:
        return bytes([0x06]) + ping_raw[1:]

    async def connect_and_run(self, topics: list[str]) -> None:
        async with websockets.connect(
            WS_URL,
            additional_headers=WS_HEADERS,
            ssl=self._ssl_ctx,
            ping_interval=None,
            ping_timeout=None,
            max_size=10 * 1024 * 1024,
            close_timeout=5,
        ) as ws:
            await ws.send(base64.b64decode(HANDSHAKE_B64))
            await ws.recv()

            for topic in topics:
                await ws.send(self._make_subscribe(topic))

            try:
                async for message in ws:
                    raw   = (base64.b64decode(message)
                             if isinstance(message, str) else message)
                    ftype = raw[0] if raw else 0xFF

                    # SERVICE_REQUEST / SUBSCRIBE_REQUEST → obowiązkowa odpowiedź
                    if ftype == 0x00:
                        if DEBUG and len(raw) >= 3:
                            svc = raw[2]
                            label = {55: "SYSTEM_PING", 56: "USER_PING"}.get(
                                svc, f"SUBSCRIBE_REQ svc={svc} len={len(raw)}")
                            print(f"    [DBG] {label} conv={raw[1]} → pong")
                        await ws.send(self._make_pong(raw))
                        continue

                    if ftype == 0x01:
                        if DEBUG:
                            print(f"    [DBG] Handshake ACK (0x01) len={len(raw)}")
                        await ws.send(raw)
                        continue

                    if ftype in (0x02, 0x03):
                        if DEBUG:
                            label = "SUBSCRIBE_ACK" if ftype == 0x02 else "UNSUBSCRIBE_ACK"
                            print(f"    [DBG] {label} (0x{ftype:02x}) len={len(raw)}")
                        continue

                    if ftype == 0x06 and (b"epoch" in raw or len(raw) <= 6):
                        if DEBUG:
                            print(f"    [DBG] Epoch keepalive (0x06) len={len(raw)}")
                        continue

                    await self._on_frame(ftype, raw)
            finally:
                pass


# ─────────────────────────────────────────────────────────────────
# WYŚWIETLANIE
# ─────────────────────────────────────────────────────────────────

class ScoreboardPrinter:
    """Formatuje i drukuje tabelę eventów meczowych."""

    def __init__(self, event_id: str):
        self.event_id       = event_id
        self.header_printed = False

    def print_header(self, home: str, away: str):
        if self.header_printed:
            return
        h = home or "Home"
        a = away or "Away"
        print("─" * 72)
        print(f"  OB_EV{self.event_id}  |  {h} vs {a}")
        print("─" * 72)
        print(f"  {'TIME':8}  {'TEAM':<{COL_TEAM_WIDTH}}  {'MIN':<8}  {'PER':<4}  EVENT")
        print("─" * 72)
        self.header_printed = True

    def print_system(self, msg: str):
        now_s = datetime.now().strftime("%H:%M:%S")
        print(f"  {now_s}  {'~':<{COL_TEAM_WIDTH}}  {'':8}  {'':4}  {msg}")

    def print_event(
        self,
        etype: str | None,
        team: str | None,
        home: str,
        away: str,
        info: dict,
        raw: bytes,
    ):
        now_s    = datetime.now().strftime("%H:%M:%S")
        no_team  = etype in NO_TEAM_EVENTS
        team_disp = "" if no_team else self._resolve_team(team, home, away)
        period   = info.get("period") or ""
        per_disp = PERIOD_LABELS.get(period, period[:4] if period else "")
        min_str  = duration_to_minute(info.get("duration_secs"), period)
        etype_str = etype or "UNKNOWN"
        print(f"  {now_s}  {team_disp:<{COL_TEAM_WIDTH}}  {min_str:<8}  {per_disp:<4}  {etype_str}")

        if (not no_team and not team) or not etype:
            dbg = info.get("debug_strings", [])
            print(f"    ! cbor_team={info.get('cbor_team')}  "
                  f"cbor_type={info.get('cbor_etype')}  "
                  f"byte_team={info.get('byte_tdbg')}  ascii={dbg}")
            print(self._hexdump(raw))

    @staticmethod
    def _resolve_team(team: str | None, home: str, away: str) -> str:
        if team == "HOME" and home:
            return home
        if team == "AWAY" and away:
            return away
        return team or "???"

    @staticmethod
    def _hexdump(raw: bytes, width: int = 16) -> str:
        lines = []
        for i in range(0, len(raw), width):
            chunk   = raw[i:i + width]
            hex_p   = " ".join(f"{b:02x}" for b in chunk).ljust(width * 3 - 1)
            ascii_p = "".join(chr(b) if 0x20 <= b <= 0x7E else "." for b in chunk)
            lines.append(f"    {i:04x}  {hex_p}  {ascii_p}")
        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────
# HANDLERY STRUMIENI TEMATÓW
# ─────────────────────────────────────────────────────────────────

class MatchStateHandler:
    def __init__(self, printer: ScoreboardPrinter, session: "MatchSession"):
        self._printer = printer
        self._session = session

    def on_snapshot(self, raw: bytes):
        if not self._session.home_name:
            h, a = extract_team_names(raw)
            if h and a:
                self._session.home_name = h
                self._session.away_name = a
                if DEBUG:
                    print(f"    [DBG] Snapshot → {h} vs {a}")
        self._printer.print_header(
            self._session.home_name, self._session.away_name
        )


class IncidentsHandler:
    def __init__(self, printer: ScoreboardPrinter, session: "MatchSession"):
        self._printer = printer
        self._session = session

    def on_delta(self, raw: bytes):
        if self._session.game_over:
            return

        info  = parse_incident(raw)
        etype = info.get("event_type")
        team  = info.get("team")

        # Pomiń typy z listy DONT_SHOW
        if etype and etype in DONT_SHOW:
            return

        # Deduplikacja po incidentIdx — każdy event przychodzi na 2 tematy
        incident_idx = info.get("incident_idx")
        if incident_idx:
            if incident_idx in self._session.seen_incidents:
                if DEBUG:
                    print(f"    [DBG] DEDUP incidentIdx={incident_idx} etype={etype}")
                return
            self._session.seen_incidents.add(incident_idx)

        if not etype and not team:
            if DEBUG:
                ftype   = raw[0] if raw else 0xFF
                strings = info.get("debug_strings", [])
                print(f"  {datetime.now().strftime('%H:%M:%S')}  "
                      f"{'?':<{COL_TEAM_WIDTH}}  {'':8}  {'':4}  "
                      f"UNKNOWN_DELTA ftype=0x{ftype:02x} len={len(raw)}")
                print(f"    ascii={strings}")
                print(self._printer._hexdump(raw))
            log_frame_unknown(raw[0] if raw else 0xFF, raw)
            return

        if not etype and team:
            etype = "STAT_UPDATE"

        if etype in CLOCK_EVENTS:
            if etype == self._session.last_clock_event:
                return
            self._session.last_clock_event = etype

        if etype in GAME_OVER_EVENTS:
            self._session.game_over = True

        if team:
            self._session.last_team    = team
            self._session.last_team_ts = datetime.now()

        if not team and etype and etype not in NO_TEAM_EVENTS:
            if self._session.last_team and self._session.last_team_ts:
                age = (datetime.now() - self._session.last_team_ts).total_seconds()
                if age < 30:
                    team = self._session.last_team

        # Zapisz do właściwego pliku logu
        if etype and etype != "UNKNOWN":
            log_frame_known(raw[0] if raw else 0xFF, raw,
                            etype or "", team or "")
        else:
            log_frame_unknown(raw[0] if raw else 0xFF, raw)

        self._printer.print_event(
            etype, team,
            self._session.home_name, self._session.away_name,
            info, raw,
        )

        if self._session.game_over:
            self._printer.print_system(
                f"KONIEC MECZU  {self._session.home_name} vs {self._session.away_name}"
            )
            self._session.done.set()


# ─────────────────────────────────────────────────────────────────
# SESJA MECZU — STAN I ROUTING
# ─────────────────────────────────────────────────────────────────

@dataclass
class MatchSession:
    event_id: str

    home_name:        str           = ""
    away_name:        str           = ""
    last_clock_event: str           = ""
    last_team:        Optional[str] = None
    last_team_ts:     Optional[datetime] = None
    game_over:        bool          = False
    done:             asyncio.Event = field(default_factory=asyncio.Event)

    # Deduplikacja eventów po incidentIdx
    seen_incidents:   Set[str]      = field(default_factory=set)

    topic_main:      str = ""
    topic_incidents: str = ""

    state_handler:     Optional[MatchStateHandler]  = field(default=None, repr=False)
    incidents_handler: Optional[IncidentsHandler]   = field(default=None, repr=False)

    async def dispatch(self, ftype: int, raw: bytes):
        if ftype == 0x04:
            if not self.game_over and self.state_handler:
                self.state_handler.on_snapshot(raw)
            return

        if ftype in (0x05, 0x06):
            if self.incidents_handler:
                self.incidents_handler.on_delta(raw)
            return

        # Nieznany typ ramki — zawsze na ekran + do pliku unknown
        now_s   = datetime.now().strftime("%H:%M:%S")
        strings = ascii_runs(raw)
        width   = 16
        lines   = []
        for i in range(0, len(raw), width):
            chunk   = raw[i:i + width]
            hex_p   = " ".join(f"{b:02x}" for b in chunk).ljust(width * 3 - 1)
            ascii_p = "".join(chr(b) if 0x20 <= b <= 0x7E else "." for b in chunk)
            lines.append(f"    {i:04x}  {hex_p}  {ascii_p}")
        hexdump = "\n".join(lines)
        print(f"  {now_s}  {'?':<{COL_TEAM_WIDTH}}  {'':8}  {'':4}  "
              f"UNKNOWN_FRAME ftype=0x{ftype:02x} len={len(raw)}")
        print(f"    ascii={strings}")
        print(hexdump)
        log_frame_unknown(ftype, raw)


# ─────────────────────────────────────────────────────────────────
# GŁÓWNA PĘTLA Z AUTO-RECONNECT
# ─────────────────────────────────────────────────────────────────

async def listen(event_id: str):
    topic_main      = f"scoreboards/v1/OB_EV{event_id}"
    topic_incidents = f"scoreboards/v1/OB_EV{event_id}/incidents"

    printer = ScoreboardPrinter(event_id)
    session = MatchSession(event_id=event_id)
    session.topic_main        = topic_main
    session.topic_incidents   = topic_incidents
    session.state_handler     = MatchStateHandler(printer, session)
    session.incidents_handler = IncidentsHandler(printer, session)

    client = DiffusionClient(on_frame=session.dispatch)

    while not session.game_over:
        try:
            await client.connect_and_run([topic_main, topic_incidents])

        except websockets.exceptions.ConnectionClosedError:
            if session.game_over:
                return
            printer.print_system("RECONNECTING...")
            await asyncio.sleep(3)
            printer.print_system("RECONNECTED")

        except websockets.exceptions.WebSocketException as e:
            if session.game_over:
                return
            printer.print_system(f"RECONNECTING...  ({type(e).__name__})")
            await asyncio.sleep(3)

        except Exception as e:
            printer.print_system(f"BŁĄD  {type(e).__name__}: {e}")
            await asyncio.sleep(5)

        if session.done.is_set():
            return

    await session.done.wait()


# ─────────────────────────────────────────────────────────────────
# PUNKT WEJŚCIA
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="William Hill Live Scoreboard v2",
        epilog="Przykład: python williamhill_scoreboard_v2.py 39319952",
    )
    parser.add_argument("event_id",
        help="Identyfikator meczu z URL William Hill")
    parser.add_argument("--debug", action="store_true",
        help="Tryb debug: pingi, ACK, nierozpoznane delty")
    parser.add_argument("--frames-log-known", metavar="PLIK",
        help="Plik logu dla ROZPOZNANYCH ramek (hex dump)")
    parser.add_argument("--frames-log-unknown", metavar="PLIK",
        help="Plik logu dla NIEROZPOZNANYCH ramek (hex dump)")

    args = parser.parse_args()
    DEBUG              = args.debug
    FRAMES_LOG_KNOWN   = args.frames_log_known   or ""
    FRAMES_LOG_UNKNOWN = args.frames_log_unknown or ""

    try:
        asyncio.run(listen(args.event_id))
    except KeyboardInterrupt:
        print("\nPrzerwano przez użytkownika.")

