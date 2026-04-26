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
import os
import ssl
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import cbor2
import websockets

# ─────────────────────────────────────────────────────────────────
# TRYB DEBUG
# ─────────────────────────────────────────────────────────────────

DEBUG: bool = False

# ─────────────────────────────────────────────────────────────────
# ZAPIS RAMEK DO PLIKU
# ─────────────────────────────────────────────────────────────────

# Gdy ustawione, każda odebrana ramka (przed filtrowaniem) jest dopisywana
# do tego pliku w formacie hex. Ustawiane przez argparse (--frames-log).
FRAMES_LOG_PATH: str = ""

def log_frame(ftype: int, raw: bytes):
    """Dopisuje ramkę do pliku logów w formacie czytelnym szesnastkowo."""
    if not FRAMES_LOG_PATH:
        return
    try:
        now_s = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        hex_str = raw.hex(" ")
        with open(FRAMES_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"[{now_s}] ftype=0x{ftype:02x} len={len(raw)}\n")
            # Hex dump z offsetami i ASCII
            width = 16
            for i in range(0, len(raw), width):
                chunk   = raw[i:i + width]
                hex_p   = " ".join(f"{b:02x}" for b in chunk).ljust(width * 3 - 1)
                ascii_p = "".join(chr(b) if 0x20 <= b <= 0x7E else "." for b in chunk)
                f.write(f"  {i:04x}  {hex_p}  {ascii_p}\n")
            f.write("\n")
    except Exception:
        pass

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
    "FIRST_HALF":  "1st Half",
    "SECOND_HALF": "2nd Half",
    "HALF_TIME":   "Half Time",
    "FULL_TIME":   "Full Time",
    "PRE_MATCH":   "Pre Match",
    "EXTRA_TIME":  "Extra Time",
    "PENALTIES":   "Penalties",
}

EVENT_TYPES_LIST = sorted([
    "GOAL", "YELLOW", "YELLOW_CARD", "RED_CARD",
    "FREE_KICK", "DANGEROUS_FREE_KICK",
    "CORNER", "GOAL_KICK", "THROW_IN", "PENALTY", "KICK_OFF",
    "SHOT_ON_TARGET", "SHOT_OFF_TARGET", "SHOT_BLOCKED",
    "BLOCKED_SHOT", "SHOT_WOODWORK",
    "DANGEROUS_ATTACK", "ATTACK", "SAFE", "BALL_SAFE", "CLEARANCE",
    "FDANGER", "DANGER",
    "OFFSIDE", "SUBSTITUTION", "FOUL",
    "PENALTY_MISSED", "PENALTY_RETAKEN",
    "GAME_FINISHED", "STOP_GAME",
    "HALF_TIME", "FULL_TIME",
    "STOP_1_ST_HALF", "STOP_2_ND_HALF",
    "FIRST_HALF", "SECOND_HALF",
    "EXTRA_TIME_FIRST_HALF", "EXTRA_TIME_SECOND_HALF",
    "EXTRA_TIME_HALF_TIME", "EXTRA_TIME_KICK_OFF",
    "STOP_1_ST_HALF_OF_EXTRA_TIME", "STOP_2_ND_HALF_OF_EXTRA_TIME",
    "END_OF_EXTRA_TIME",
    "FIRST_HALF_KICK_OFF", "SECOND_HALF_KICK_OFF",
    "START_1_ST_HALF", "START_2_ND_HALF",
    "START_PENALTY_SHOOTOUT", "END_OF_MATCH",
], key=len, reverse=True)

NO_TEAM_EVENTS = {
    "HALF_TIME", "FULL_TIME", "GAME_FINISHED", "STOP_GAME",
    "STOP_1_ST_HALF", "STOP_2_ND_HALF", "FIRST_HALF", "SECOND_HALF",
    "BALL_SAFE",
    "EXTRA_TIME_FIRST_HALF", "EXTRA_TIME_SECOND_HALF",
    "EXTRA_TIME_HALF_TIME", "EXTRA_TIME_KICK_OFF",
    "FIRST_HALF_KICK_OFF", "SECOND_HALF_KICK_OFF",
    "START_1_ST_HALF", "START_2_ND_HALF", "START_PENALTY_SHOOTOUT",
    "STOP_1_ST_HALF_OF_EXTRA_TIME", "STOP_2_ND_HALF_OF_EXTRA_TIME",
    "END_OF_EXTRA_TIME", "END_OF_MATCH",
}

GAME_OVER_EVENTS = {
    "GAME_FINISHED", "STOP_GAME", "FULL_TIME", "STOP_2_ND_HALF", "END_OF_MATCH",
}

CLOCK_EVENTS = {
    "FIRST_HALF", "SECOND_HALF", "HALF_TIME", "FULL_TIME",
    "EXTRA_TIME_FIRST_HALF", "EXTRA_TIME_SECOND_HALF",
    "FIRST_HALF_KICK_OFF", "SECOND_HALF_KICK_OFF",
    "EXTRA_TIME_KICK_OFF", "START_PENALTY_SHOOTOUT",
    "START_1_ST_HALF", "START_2_ND_HALF",
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
    # Fallback
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
    Parsuje incydent meczowy z ramki delta.
    Payload zaczyna się od bajtu 6 (nagłówek ramki Diffusion).
    """
    payload = raw[6:]

    cbor_team = cbor_etype = None
    duration_secs = None

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
        for v in values:
            time_obj = deep_find(v, "time")
            if isinstance(time_obj, dict):
                d = time_obj.get("duration")
                if isinstance(d, int) and d >= 0:
                    duration_secs = d
                    break
            d = deep_find(v, "duration")
            if isinstance(d, int) and 0 <= d < 18000:
                duration_secs = d
                break
            for key in ("clockTime", "matchTime", "elapsedTime"):
                d = deep_find(v, key)
                if isinstance(d, int) and 0 <= d < 18000:
                    duration_secs = d
                    break
            if duration_secs is not None:
                break
    except Exception:
        pass

    # Fallback skanowania bajtów
    byte_etype = find_in_bytes(payload, EVENT_TYPES_LIST)
    byte_team  = None
    byte_tdbg  = "-"

    if not cbor_team:
        for marker, team in [
            (b"HOME", "HOME"), (b"AWAY", "AWAY"),
            (b"DHOME", "HOME"), (b"DAWAY", "AWAY"),
            (b"dHOME", "HOME"), (b"dAWAY", "AWAY"),
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
                fragment  = etype.encode("ascii")[:max(4, len(etype) - 2)]
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
            if b"HOM" in payload:    byte_team = "HOME"; byte_tdbg = "HOM"
            elif b"AWA" in payload:  byte_team = "AWAY"; byte_tdbg = "AWA"

    return {
        "event_type":    cbor_etype or byte_etype,
        "team":          cbor_team  or byte_team,
        "team_source":   "cbor" if cbor_team else (byte_tdbg if byte_team else "-"),
        "cbor_team":     cbor_team,
        "cbor_etype":    cbor_etype,
        "byte_team":     byte_team,
        "byte_tdbg":     byte_tdbg,
        "duration_secs": duration_secs,
        "debug_strings": ascii_runs(payload),
    }


def duration_to_minute(secs: int | None) -> str:
    if secs is None:
        return ""
    return f"{secs // 60 + 1}'"


# ─────────────────────────────────────────────────────────────────
# NISKOPOZIOMOWY KLIENT DIFFUSION
# ─────────────────────────────────────────────────────────────────

class DiffusionClient:
    """
    Niskopoziomowe połączenie z serwerem Diffusion przez WebSocket.
    Odpowiada za:
      - Handshake (jednorazowy przy połączeniu)
      - Odpowiadanie na SERVICE_REQUEST (SYSTEM_PING 0x00 → SERVICE_RESPONSE 0x06)
      - Wysyłanie keepalive co 20 s
      - Subskrypcje tematów
      - Dostarczanie surowych ramek 0x04/0x05/0x06 do callbacka on_frame()
    """

    def __init__(self, on_frame):
        """
        Args:
            on_frame: async callable(ftype: int, raw: bytes) wywoływany
                      dla każdej ramki z danymi (0x04 snapshot, 0x05/0x06 delta).
        """
        self._on_frame = on_frame
        self._ssl_ctx  = ssl.create_default_context()

    def _make_subscribe(self, topic: str) -> bytes:
        topic_b  = topic.encode("utf-8")
        selector = bytes([0x3E]) + topic_b
        return bytes([0x00, 0x03, 0x02, len(selector)]) + selector

    def _make_keepalive(self) -> bytes:
        return bytes([0x06]) + os.urandom(3) + bytes([0x2B])

    def _make_pong(self, ping_raw: bytes) -> bytes:
        return bytes([0x06]) + ping_raw[1:]

    async def _keepalive_loop(self, ws):
        try:
            while True:
                await asyncio.sleep(20)
                await ws.send(self._make_keepalive())
        except Exception:
            pass

    async def connect_and_run(self, topics: list[str]) -> None:
        """Nawiązuje połączenie, subskrybuje tematy i uruchamia pętlę odczytu."""
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

            ka_task = asyncio.create_task(self._keepalive_loop(ws))
            try:
                async for message in ws:
                    raw   = (base64.b64decode(message)
                             if isinstance(message, str) else message)
                    ftype = raw[0] if raw else 0xFF

                    # Zapisz każdą ramkę do pliku logu (przed filtrowaniem)
                    log_frame(ftype, raw)

                    # SERVICE_REQUEST → pong (obowiązkowe, inaczej serwer rozłączy)
                    if ftype == 0x00:
                        if DEBUG and len(raw) >= 3:
                            svc = raw[2]
                            label = {55: "SYSTEM_PING", 56: "USER_PING"}.get(svc, f"svc={svc}")
                            print(f"    [DBG] {label} conv={raw[1]} → pong")
                        await ws.send(self._make_pong(raw))
                        continue

                    # Handshake ACK → echo
                    if ftype == 0x01:
                        if DEBUG:
                            print(f"    [DBG] Handshake ACK (0x01) len={len(raw)}")
                        await ws.send(raw)
                        continue

                    # Subscribe/unsubscribe ACK → ignoruj
                    if ftype in (0x02, 0x03):
                        if DEBUG:
                            label = "SUBSCRIBE_ACK" if ftype == 0x02 else "UNSUBSCRIBE_ACK"
                            print(f"    [DBG] {label} (0x{ftype:02x}) len={len(raw)}")
                        continue

                    # Epoch keepalive → ignoruj
                    if ftype == 0x06 and (b"epoch" in raw or len(raw) <= 6):
                        if DEBUG:
                            print(f"    [DBG] Epoch keepalive (0x06) len={len(raw)}")
                        continue

                    # Ramki z danymi → przekaż do handlera
                    await self._on_frame(ftype, raw)
            finally:
                ka_task.cancel()
                try:
                    await ka_task
                except asyncio.CancelledError:
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
        print("─" * 60)
        print(f"  OB_EV{self.event_id}  |  {h} vs {a}")
        print("─" * 60)
        print(f"  {'TIME':8}  {'TEAM':<{COL_TEAM_WIDTH}}{'MIN':<6}EVENT")
        print("─" * 60)
        self.header_printed = True

    def print_system(self, msg: str):
        now_s = datetime.now().strftime("%H:%M:%S")
        print(f"  {now_s}  {'~':<{COL_TEAM_WIDTH}}{'':6}{msg}")

    def print_event(
        self,
        etype: str | None,
        team: str | None,
        home: str,
        away: str,
        info: dict,
        raw: bytes,
    ):
        now_s     = datetime.now().strftime("%H:%M:%S")
        no_team   = etype in NO_TEAM_EVENTS
        team_disp = "" if no_team else self._resolve_team(team, home, away)
        min_str   = duration_to_minute(info.get("duration_secs"))
        etype_str = etype or "UNKNOWN"
        print(f"  {now_s}  {team_disp:<{COL_TEAM_WIDTH}}{min_str:<6}{etype_str}")

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
    """
    Obsługuje temat główny: scoreboards/v1/OB_EV{id}
    Ramka 0x04 (snapshot) → wyciąga nazwy drużyn.
    """

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
    """
    Obsługuje temat incydentów: scoreboards/v1/OB_EV{id}/incidents
    Ramki 0x05/0x06 (delta) → parsuje i wyświetla eventy meczowe.
    """

    def __init__(self, printer: ScoreboardPrinter, session: "MatchSession"):
        self._printer = printer
        self._session = session

    def on_delta(self, raw: bytes):
        if self._session.game_over:
            return

        info  = parse_incident(raw)
        etype = info.get("event_type")
        team  = info.get("team")

        if not etype and not team:
            # Ramka bez etype i bez team — delta pozycji/pędu bez wartości wyświetleniowej.
            # W trybie --debug pokazujemy żeby nic nie umknęło, w normalnym trybie pomijamy.
            if DEBUG:
                now_s   = datetime.now().strftime("%H:%M:%S")
                ftype   = raw[0] if raw else 0xFF
                strings = info.get("debug_strings", [])
                print(f"  {now_s}  {'?':<{COL_TEAM_WIDTH}}{'':6}"
                      f"UNKNOWN_DELTA ftype=0x{ftype:02x} len={len(raw)}")
                print(f"    ascii={strings}")
                print(self._printer._hexdump(raw))
            return

        if not etype and team:
            etype = "STAT_UPDATE"

        # Deduplikacja eventów zegarowych
        if etype in CLOCK_EVENTS:
            if etype == self._session.last_clock_event:
                return
            self._session.last_clock_event = etype

        if etype in GAME_OVER_EVENTS:
            self._session.game_over = True

        # Aktualizuj kontekst drużyny
        if team:
            self._session.last_team    = team
            self._session.last_team_ts = datetime.now()

        # Fallback drużyny z kontekstu (okno 30 s)
        if not team and etype and etype not in NO_TEAM_EVENTS:
            if self._session.last_team and self._session.last_team_ts:
                age = (datetime.now() - self._session.last_team_ts).total_seconds()
                if age < 30:
                    team = self._session.last_team

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
    """
    Przechowuje stan meczu i routuje ramki do odpowiednich handlerów.
    Jeden obiekt na czas życia połączenia (resetowany przy reconnect).
    """
    event_id: str

    home_name:        str           = ""
    away_name:        str           = ""
    last_clock_event: str           = ""
    last_team:        Optional[str] = None
    last_team_ts:     Optional[datetime] = None
    game_over:        bool          = False
    done:             asyncio.Event = field(default_factory=asyncio.Event)

    # Tematy rozróżniane po ścieżce — ustawiane przez listen()
    topic_main:      str = ""
    topic_incidents: str = ""

    # Handlery (ustawiane po inicjalizacji)
    state_handler:     Optional[MatchStateHandler]  = field(default=None, repr=False)
    incidents_handler: Optional[IncidentsHandler]   = field(default=None, repr=False)

    async def dispatch(self, ftype: int, raw: bytes):
        """Routuje ramkę do odpowiedniego handlera na podstawie jej typu."""

        # Snapshot (pełny stan) — może dotyczyć dowolnego tematu
        if ftype == 0x04:
            if not self.game_over and self.state_handler:
                self.state_handler.on_snapshot(raw)
            return

        # Delta incydentu (0x05 / 0x06)
        if ftype in (0x05, 0x06):
            if self.incidents_handler:
                self.incidents_handler.on_delta(raw)
            return

        # Nieznany typ ramki — wyświetl zawsze na osi czasu z hex dumpem
        now_s   = datetime.now().strftime("%H:%M:%S")
        payload = raw[6:] if len(raw) > 6 else raw
        strings = ascii_runs(raw)
        width   = 16
        lines   = []
        for i in range(0, len(raw), width):
            chunk   = raw[i:i + width]
            hex_p   = " ".join(f"{b:02x}" for b in chunk).ljust(width * 3 - 1)
            ascii_p = "".join(chr(b) if 0x20 <= b <= 0x7E else "." for b in chunk)
            lines.append(f"    {i:04x}  {hex_p}  {ascii_p}")
        hexdump = "\n".join(lines)

        print(f"  {now_s}  {'?':<{COL_TEAM_WIDTH}}{'':6}UNKNOWN_FRAME ftype=0x{ftype:02x} len={len(raw)}")
        print(f"    ascii={strings}")
        print(hexdump)


# ─────────────────────────────────────────────────────────────────
# GŁÓWNA PĘTLA Z AUTO-RECONNECT
# ─────────────────────────────────────────────────────────────────

async def listen(event_id: str):
    """
    Łączy się z serwerem, subskrybuje tematy i nasłuchuje eventów.
    Przy rozłączeniu automatycznie reconnektuje (chyba że mecz się skończył).
    """
    topic_main      = f"scoreboards/v1/OB_EV{event_id}"
    topic_incidents = f"scoreboards/v1/OB_EV{event_id}/incidents"

    printer = ScoreboardPrinter(event_id)
    session = MatchSession(event_id=event_id)
    session.topic_main      = topic_main
    session.topic_incidents = topic_incidents
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

        # Sprawdź czy mecz skończył się podczas sesji
        if session.done.is_set():
            return

    # Czekaj na sygnał końca meczu (może przyjść z on_delta przez done.set())
    await session.done.wait()


# ─────────────────────────────────────────────────────────────────
# PUNKT WEJŚCIA
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="William Hill Live Scoreboard v2 — architektura strumieniowa.",
        epilog="Przykład: python williamhill_scoreboard_v2.py 39319952",
    )
    parser.add_argument(
        "event_id",
        help="Identyfikator meczu z URL William Hill, np. 39319952",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Włącz tryb debug: loguje SYSTEM_PING, User_PING i nierozpoznane ramki",
    )
    parser.add_argument(
        "--frames-log",
        help="Ścieżka do pliku logu ramek (hex dump wszystkich odebranych ramek)",
    )
    args = parser.parse_args()
    DEBUG = args.debug
    FRAMES_LOG_PATH = args.frames_log or ""

    try:
        asyncio.run(listen(args.event_id))
    except KeyboardInterrupt:
        print("\nPrzerwano przez użytkownika.")

