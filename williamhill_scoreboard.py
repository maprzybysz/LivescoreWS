"""
williamhill_scoreboard.py
─────────────────────────
Skrobak live scoreboard dla William Hill (sports.whcdn.net).

Przegląd protokołu
──────────────────
Strona używa Diffusion — własnościowego protokołu pub/sub po WebSocket.
Wszystkie wiadomości to ramki binarne z 1-bajtowym prefiksem typu:

  0x00  SERVICE_REQUEST od serwera → musimy odpowiedzieć SERVICE_RESPONSE
                            (0x06) z tym samym payloadem.  Brak odpowiedzi
                            powoduje zamknięcie połączenia przez serwer po
                            ~30 s (dokładnie: systemPingPeriod × 2 ms) —
                            to jest główna przyczyna reconnectów.

                            Szczególny przypadek: service_id == 55 to
                            SYSTEM_PING (service_id == 56 to USER_PING).
                            Format ramki SERVICE_REQUEST:
                              byte[0] = 0x00  (typ SERVICE_REQUEST)
                              byte[1] = conversation_id (varint)
                              byte[2] = service_id (varint, 55 = SYSTEM_PING)
                            Odpowiedź SERVICE_RESPONSE:
                              byte[0] = 0x06, reszta payloadu bez zmian.
  0x01  Topic load ACK    → odsyłamy bez zmian (handshake protokołu)
  0x02  Subscribe ACK     → ignorujemy
  0x03  Unsubscribe ACK   → ignorujemy
  0x04  Topic snapshot    → pełny payload CBOR ze stanem meczu i nazwami
                            drużyn; przychodzi raz po każdej subskrypcji
  0x05  Topic delta       → przyrostowa aktualizacja CBOR (eventy, statsy,
                            zegar)
  0x06  Keepalive / pong  → wysyłamy co 20 s i jako odpowiedź na ping

Subskrybowane tematy
────────────────────
  scoreboards/v1/OB_EV{id}            – stan meczu (wynik, statsy, zegar)
  scoreboards/v1/OB_EV{id}/incidents  – indywidualne eventy meczowe z
                                        polami teamType (HOME/AWAY) i type

Nazwy drużyn
────────────
Nazwy drużyn NIE ma w ramkach delta (0x05). Przychodzą w pierwszym
snapshocie (0x04) pod kluczem "name" jako "Gospodarz vs Gość", albo
zagnieżdżone pod homeTeam.name / awayTeam.name. Parsujemy je raz
i cachujemy na cały czas życia procesu.

Parsowanie eventów
──────────────────
Eventy przychodzą w dwóch wariantach:
  a) Pełne CBOR  – dekodowane przez cbor2; pola "type" i "teamType"
                   wyciągane przez deep_find().
  b) Binarne     – brak poprawnej struktury CBOR; fallback do skanowania
                   surowych bajtów w poszukiwaniu znanych ciągów ASCII
                   (typy eventów) i markerów HOME/AWAY.

Ramki bez typu eventu i bez drużyny są ciche odrzucane — to delta
pozycji/pędu bez wartości wyświetleniowej.
Ramki z drużyną ale bez typu eventu pokazywane są jako STAT_UPDATE.

Minuta meczu
────────────
Każdy incydent zawiera pole incident.time.duration (sekundy od początku
meczu).  Przeliczamy na minutę meczu: minuta = sekundy // 60 + 1.
Minutę wyświetlamy w kolumnie MIN między TEAM a EVENT.

Deduplikacja
────────────
Eventy zegarowe (FIRST_HALF, SECOND_HALF itp.) przychodzą jako
ciągły strumień identycznych delt podczas gdy zegar tyka. Wyświetlamy
tylko pierwsze wystąpienie przy każdej zmianie okresu.

Reconnect
─────────
Serwer zamyka połączenie co kilka minut. Automatycznie się
łączymy ponownie i pokazujemy linię RECONNECTING / RECONNECTED
w tabeli eventów. Po reconneccie serwer wysyła ponownie pełny
snapshot, więc nazwy drużyn i aktualny stan meczu są zawsze świeże.

Tryb debug
──────────
Uruchom z flagą --debug żeby zobaczyć dodatkowe informacje: odebrane
SYSTEM_PING, identyfikatory conversation/service oraz ramki których
nie udało się rozpoznać.
"""

import asyncio
import base64
import io
import os
import ssl
import argparse
from datetime import datetime

import cbor2
import websockets

# ─────────────────────────────────────────────────────────────────
# TRYB DEBUG
# ─────────────────────────────────────────────────────────────────

# Ustawiane przez argparse przy starcie — nie zmieniaj ręcznie.
DEBUG: bool = False

# ─────────────────────────────────────────────────────────────────
# ZAPIS RAMEK DO PLIKU
# ─────────────────────────────────────────────────────────────────

# Gdy ustawione, każda odebrana ramka (przed filtrowaniem) jest dopisywana
# do tego pliku w formacie hex dump. Ustawiane przez argparse (--frames-log).
FRAMES_LOG_PATH: str = ""


def log_frame(ftype: int, raw: bytes):
    """Dopisuje ramkę do pliku logów w formacie czytelnym szesnastkowo."""
    if not FRAMES_LOG_PATH:
        return
    try:
        now_s = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        width = 16
        with open(FRAMES_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"[{now_s}] ftype=0x{ftype:02x} len={len(raw)}\n")
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

# Endpoint WebSocket Diffusion — parametry query identyfikują typ klienta
# i ustawiają timeout reconnectu po stronie serwera (r=300000 ms = 5 min).
WS_URL = (
    "wss://scoreboards-push.williamhill.com/diffusion"
    "?ty=WB&v=18&ca=8&r=300000"
    "&sp=%7B%22src%22%3A%22traf_sb_football%22"
    "%2C%22origin%22%3A%22https%3A%2F%2Fsports.whcdn.net%22%7D"
)

# Nagłówki udające prawdziwą przeglądarkę — serwer sprawdza Origin.
HEADERS = {
    "Origin":     "https://sports.whcdn.net",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}

# Początkowa ramka handshake Diffusion (zakodowana base64).
# Po zdekodowaniu zawiera stały nagłówek protokołu, którego serwer
# oczekuje przed akceptacją jakichkolwiek subskrypcji.
HANDSHAKE_B64 = (
    "AHUBFT5zY29yZWJvYXJkcy92MS9lcG9jaAAAgIAQAQD"
    "/////B/////8H/////wf/////BwA="
)

# Czytelne etykiety dla identyfikatorów okresów używanych w payloadach CBOR.
PERIOD_LABELS = {
    "FIRST_HALF":  "1st Half",
    "SECOND_HALF": "2nd Half",
    "HALF_TIME":   "Half Time",
    "FULL_TIME":   "Full Time",
    "PRE_MATCH":   "Pre Match",
    "EXTRA_TIME":  "Extra Time",
    "PENALTIES":   "Penalties",
}

# Wszystkie znane typy eventów, posortowane od najdłuższego — skaner
# bajtów dopasowuje wtedy najbardziej szczegółowy ciąg jako pierwszy
# (np. DANGEROUS_FREE_KICK przed FREE_KICK).
EVENT_TYPES_LIST = sorted([
    "GOAL", "YELLOW", "YELLOW_CARD", "RED_CARD",
    "FREE_KICK", "DANGEROUS_FREE_KICK",
    "CORNER", "GOAL_KICK", "THROW_IN", "PENALTY", "KICK_OFF",
    "SHOT_ON_TARGET", "SHOT_OFF_TARGET", "SHOT_BLOCKED",
    "BLOCKED_SHOT", "SHOT_WOODWORK",
    "DANGEROUS_ATTACK", "ATTACK", "SAFE", "BALL_SAFE", "CLEARANCE",
    "FDANGER", "DANGER",
    "OFFSIDE", "SUBSTITUTION",
    "FOUL",
    "PENALTY_MISSED", "PENALTY_RETAKEN",
    "GAME_FINISHED", "STOP_GAME",
    "HALF_TIME", "FULL_TIME",
    "STOP_1_ST_HALF", "STOP_2_ND_HALF",
    "FIRST_HALF", "SECOND_HALF",
    # Eventy dogrywki
    "EXTRA_TIME_FIRST_HALF", "EXTRA_TIME_SECOND_HALF",
    "EXTRA_TIME_HALF_TIME", "EXTRA_TIME_KICK_OFF",
    "STOP_1_ST_HALF_OF_EXTRA_TIME", "STOP_2_ND_HALF_OF_EXTRA_TIME",
    "END_OF_EXTRA_TIME",
    # Strzały startowe poszczególnych połówek
    "FIRST_HALF_KICK_OFF", "SECOND_HALF_KICK_OFF",
    # Sygnały startu/stopu okresu
    "START_1_ST_HALF", "START_2_ND_HALF",
    "START_PENALTY_SHOOTOUT",
    # Koniec meczu
    "END_OF_MATCH",
], key=len, reverse=True)

# Eventy dotyczące całego meczu, nie konkretnej drużyny.
# Wyświetlane bez kolumny drużyny.
NO_TEAM_EVENTS = {
    "HALF_TIME", "FULL_TIME", "GAME_FINISHED", "STOP_GAME",
    "STOP_1_ST_HALF", "STOP_2_ND_HALF",
    "FIRST_HALF", "SECOND_HALF",
    "BALL_SAFE",
    # Eventy dogrywki i strzały startowe
    "EXTRA_TIME_FIRST_HALF", "EXTRA_TIME_SECOND_HALF",
    "EXTRA_TIME_HALF_TIME", "EXTRA_TIME_KICK_OFF",
    "FIRST_HALF_KICK_OFF", "SECOND_HALF_KICK_OFF",
    "START_1_ST_HALF", "START_2_ND_HALF",
    "START_PENALTY_SHOOTOUT",
    "STOP_1_ST_HALF_OF_EXTRA_TIME", "STOP_2_ND_HALF_OF_EXTRA_TIME",
    "END_OF_EXTRA_TIME", "END_OF_MATCH",
}

# Otrzymanie któregokolwiek z tych eventów oznacza koniec meczu.
# STOP_2_ND_HALF to sygnał końca drugiej połowy używany w niektórych
# ligach zamiast FULL_TIME / GAME_FINISHED.
GAME_OVER_EVENTS = {
    "GAME_FINISHED", "STOP_GAME", "FULL_TIME", "STOP_2_ND_HALF",
    "END_OF_MATCH",
}

# Eventy zegarowe przychodzą jako ciągły strumień identycznych delt
# podczas gdy zegar tyka. Deduplikujemy — wyświetlamy tylko raz
# przy każdej zmianie okresu.
CLOCK_EVENTS = {
    "FIRST_HALF", "SECOND_HALF", "HALF_TIME", "FULL_TIME",
    "EXTRA_TIME_FIRST_HALF", "EXTRA_TIME_SECOND_HALF",
    "FIRST_HALF_KICK_OFF", "SECOND_HALF_KICK_OFF",
    "EXTRA_TIME_KICK_OFF", "START_PENALTY_SHOOTOUT",
    "START_1_ST_HALF", "START_2_ND_HALF",
}

# Szerokość kolumny TEAM w tabeli wyjściowej.
COL_TEAM_WIDTH = 22

# ─────────────────────────────────────────────────────────────────
# PROTOKÓŁ — BUDOWANIE RAMEK
# ─────────────────────────────────────────────────────────────────

def make_subscribe(topic: str) -> bytes:
    """
    Buduje ramkę SUBSCRIBE protokołu Diffusion dla podanej ścieżki tematu.
    Układ ramki:  00 03 02 <len> 3E <topic_bytes>
      00 03 02  = nagłówek komendy subscribe
      3E        = '>' prefix selektora (dopasowanie ścieżki tematu)
    """
    topic_b  = topic.encode("utf-8")
    selector = bytes([0x3E]) + topic_b
    return bytes([0x00, 0x03, 0x02, len(selector)]) + selector


def make_keepalive() -> bytes:
    """
    Ramka keepalive wysyłana przez klienta co 20 s.
    Format: 06 <3 losowe bajty> 2B
    Losowe bajty zapobiegają traktowaniu powtarzających się ramek
    jako duplikatów i ich odrzucaniu przez serwer.
    """
    return bytes([0x06]) + os.urandom(3) + bytes([0x2b])


def make_pong(ping_raw: bytes) -> bytes:
    """
    Buduje odpowiedź SERVICE_RESPONSE (0x06) na SERVICE_REQUEST (0x00) serwera.

    Format SERVICE_REQUEST:
      byte[0] = 0x00  (typ SERVICE_REQUEST)
      byte[1] = conversation_id (varint)
      byte[2] = service_id (varint) — 55 dla SYSTEM_PING, 56 dla USER_PING

    Odpowiedź SERVICE_RESPONSE ma identyczny payload, z pierwszym bajtem
    zmienionym z 0x00 na 0x06 — conversation_id i service_id pozostają
    bez zmian, dzięki czemu serwer może dopasować odpowiedź do żądania.

    Brak odpowiedzi na SYSTEM_PING powoduje rozłączenie przez serwer po
    systemPingPeriod × 2 ms (zwykle kilka minut) — to jest główna
    przyczyna reconnectów widocznych w logach.
    """
    return bytes([0x06]) + ping_raw[1:]


# ─────────────────────────────────────────────────────────────────
# CZAS MECZU
# ─────────────────────────────────────────────────────────────────

def duration_to_minute(secs: int | None) -> str:
    """
    Przelicza czas trwania meczu (sekundy od początku okresu) na minutę.
    Formuła: minuta = sekundy // 60 + 1 (minuty liczymy od 1, nie od 0).
    Zwraca pusty string gdy brak danych.
    """
    if secs is None:
        return ""
    return f"{secs // 60 + 1}'"

# ─────────────────────────────────────────────────────────────────
# CBOR — DEKODOWANIE
# ─────────────────────────────────────────────────────────────────

def cbor_decode_all(data: bytes) -> list:
    """
    Dekoduje wszystkie wartości CBOR z ciągu bajtów.
    Jeden payload Diffusion może zawierać wiele połączonych wartości CBOR.
    Ręcznie przesuwamy pozycję strumienia i pomijamy jeden bajt przy
    błędach dekodowania, żeby utrzymać synchronizację.
    """
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
    """Rekurencyjnie zbiera wszystkie wartości stringowe z obiektu CBOR."""
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
    """
    Rekurencyjnie przeszukuje zdekodowany obiekt CBOR w poszukiwaniu
    pierwszego wystąpienia podanego klucza. Zwraca wartość lub None.
    """
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
# PARSOWANIE NAZW DRUŻYN
# ─────────────────────────────────────────────────────────────────

def extract_team_names(raw: bytes) -> tuple[str, str]:
    """
    Wyciąga nazwy drużyny gospodarza i gości z ramki snapshot (0x04).

    Payload CBOR może zaczynać się na różnych offsetach bajtowych
    w zależności od liczby bajtów nagłówka Diffusion — próbujemy
    offsetów 0–6.

    Priorytety źródeł nazw:
      1. Pole "name" zawierające separator " vs "  →  "Gospodarz vs Gość"
      2. homeTeam.name / awayTeam.name
      3. homeName / awayName
      4. Fallback: dowolny string zawierający " vs "
    """
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
    # Fallback — skanuj wszystkie stringi
    try:
        for offset in (2, 4, 6):
            values = cbor_decode_all(raw[offset:])
            strings = []
            for v in values:
                strings += collect_strings(v)
            for s in strings:
                if " vs " in s and 5 < len(s) < 100:
                    parts = s.split(" vs ", 1)
                    return parts[0].strip(), parts[1].strip()
    except Exception:
        pass
    return "", ""

# ─────────────────────────────────────────────────────────────────
# PARSOWANIE EVENTU (ramka 0x05 / 0x06)
# ─────────────────────────────────────────────────────────────────

def find_in_bytes(data: bytes, targets: list[str]) -> str | None:
    """
    Skanuje surowe bajty w poszukiwaniu pierwszego pasującego ciągu ASCII
    z listy targets. Próbuje też dopasowania 5-bajtowego prefiksu dla
    dłuższych stringów, żeby obsłużyć payloady z obciętymi ciągami.
    """
    # Najpierw tylko pełne dopasowania
    for target in targets:
        if target.encode("ascii") in data:
            return target
    # Dopiero potem prefiksy (fallback)
    for target in targets:
        if len(target) > 5:
            fragment = target.encode("ascii")[:5]
            if fragment in data:
                return target
    return None


def ascii_runs(payload: bytes, min_len: int = 3) -> list[str]:
    """
    Wyciąga wszystkie ciągi drukowalnych znaków ASCII o długości
    co najmniej min_len. Używane do debugowania nierozpoznanych ramek.
    """
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
    """
    Parsuje event meczowy z ramki delta (0x05 lub 0x06).

    Payload zaczyna się od bajtu 6 (po nagłówku ramki Diffusion).

    Strategia:
      1. Próba dekodowania CBOR — szukamy kluczy "teamType" i "type".
         Wartości teamType: HOME, AWAY, MATCH (MATCH = event całego meczu)
      2. Fallback do skanowania surowych bajtów:
         a. Szukamy znanych wielobajtowych sekwencji markerów HOME/AWAY.
         b. Sprawdzamy bajt przed stringiem typu eventu: 0x48 (H) → HOME,
            0x41 (A) → AWAY — kodowanie jednobajtowego stringu CBOR.
         c. Ostateczność: obecność b"HOM" lub b"AWA" gdziekolwiek.

    Zwraca słownik z:
      event_type    – dopasowany typ eventu lub None
      team          – "HOME", "AWAY" lub None
      team_source   – jak określono drużynę (do debugowania)
      cbor_*        – surowe wyniki parsowania CBOR
      byte_*        – surowe wyniki skanowania bajtów
      debug_strings – drukowalne ciągi ASCII z payloadu
    """
    payload = raw[6:]
    cbor_team = None
    cbor_etype = None
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

        # Szukaj czasu meczu — kilka możliwych lokalizacji w CBOR
        for v in values:
            # Wariant 1: incident.time.duration
            time_obj = deep_find(v, "time")
            if isinstance(time_obj, dict):
                d = time_obj.get("duration")
                if isinstance(d, int) and d >= 0:
                    duration_secs = d
                    break
            # Wariant 2: klucz "duration" bezpośrednio na poziomie incydentu
            d = deep_find(v, "duration")
            if isinstance(d, int) and 0 <= d < 18000:  # max 5h w sekundach
                duration_secs = d
                break
            # Wariant 3: clockTime lub matchTime
            for key in ("clockTime", "matchTime", "elapsedTime"):
                d = deep_find(v, key)
                if isinstance(d, int) and 0 <= d < 18000:
                    duration_secs = d
                    break
            if duration_secs is not None:
                break
    except Exception:
        pass

    # ── Fallback skanowania bajtów ───────────────────────────────
    byte_etype = find_in_bytes(payload, EVENT_TYPES_LIST)
    byte_team  = None
    byte_tdbg  = "-"

    if not cbor_team:
        # Znane wielobajtowe markery drużyn pojawiające się dosłownie w payloadach
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

        # Sprawdź bajt przed stringiem typu eventu
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

        # Ostateczność
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
        "duration_secs": duration_secs,
        "debug_strings": ascii_runs(payload),
    }

# ─────────────────────────────────────────────────────────────────
# PARSOWANIE STANU MECZU (ramka 0x04)
# ─────────────────────────────────────────────────────────────────

def parse_match_state(raw: bytes) -> dict:
    """
    Parsuje snapshot stanu meczu (ramka 0x04).
    Wyciąga: nazwę meczu, sport, okres, wynik, flagę tykania zegara
    oraz statystyki per drużyna (strzały, rzuty rożne, ataki, auty).
    """
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
# HEXDUMP (debug)
# ─────────────────────────────────────────────────────────────────

def hexdump(raw: bytes, width: int = 16) -> str:
    """Klasyczny dump hex+ASCII, używany dla nierozpoznanych ramek."""
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

def print_header(event_id: str, home_name: str, away_name: str):
    """Drukuje nagłówek tabeli — raz na sesję (lub po reconneccie)."""
    h = home_name if home_name else "Home"
    a = away_name if away_name else "Away"
    print("─" * 60)
    print(f"  OB_EV{event_id}  |  {h} vs {a}")
    print("─" * 60)
    print(f"  {'TIME':8}  {'TEAM':<{COL_TEAM_WIDTH}}{'MIN':<6}EVENT")
    print("─" * 60)


def print_system_line(now_s: str, msg: str):
    """
    Linia systemowa (reconnect, błąd, koniec meczu) wyrównana
    do kolumny EVENT — identyczny format jak zwykłe eventy,
    ale z '~' w kolumnie drużyny żeby łatwo odróżnić.
    Kolumna MIN jest pusta dla linii systemowych.
    """
    print(f"  {now_s}  {'~':<{COL_TEAM_WIDTH}}{'':6}{msg}")


def resolve_team(team: str | None, home_name: str, away_name: str) -> str:
    """Zamienia HOME/AWAY na nazwę drużyny, jeśli jest znana."""
    if team == "HOME" and home_name:
        return home_name
    if team == "AWAY" and away_name:
        return away_name
    return team if team else "???"


def print_event_line(
    now_s: str,
    etype: str | None,
    team: str | None,
    home_name: str,
    away_name: str,
    info: dict,
    raw: bytes,
):
    """
    Drukuje jedną linię eventu w tabeli.
    Eventy bez drużyny (NO_TEAM_EVENTS) mają pustą kolumnę TEAM.
    Kolumna MIN pokazuje minutę meczu obliczoną z pola time.duration
    incydentu (pusta jeśli brak danych).
    Nierozpoznane eventy lub eventy bez drużyny dostają blok debug
    z hexdumpem payloadu.
    """
    no_team    = etype in NO_TEAM_EVENTS
    team_disp  = "" if no_team else resolve_team(team, home_name, away_name)
    etype_disp = etype if etype else "UNKNOWN"
    min_str    = duration_to_minute(info.get("duration_secs"))

    print(f"  {now_s}  {team_disp:<{COL_TEAM_WIDTH}}{min_str:<6}{etype_disp}")

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
    """
    Wysyła ramkę keepalive co 20 s żeby podtrzymać połączenie.
    Uruchamiane jako osobny task asyncio.
    """
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
    """
    Główna pętla nasłuchiwania eventów meczowych.

    Przepływ:
      1. Połącz się z WebSocket i wyślij handshake Diffusion.
      2. Zasubskrybuj dwa tematy: główny stan meczu + incydenty.
      3. Odbieraj ramki i dispatch według bajtu typu:
           0x00 → SERVICE_RESPONSE (pong) — obowiązkowy, inaczej serwer
                  rozłączy po ~30 s.  Gdy service_id == 55: SYSTEM_PING
                  (logowane w trybie debug).
           0x04 → snapshot: wyciągnij nazwy drużyn, wydrukuj nagłówek
           0x05 → delta: parsuj event, deduplikuj, wydrukuj
           0x06 → keepalive/delta: jak 0x05 ale krótsza forma
      4. Przy rozłączeniu: czekaj 3 s i połącz ponownie.
      5. Przy evencie kończącym mecz: wydrukuj MATCH FINISHED i zakończ.

    Zmienne stanu (poza pętlą reconnect):
      home_name / away_name   – nazwy drużyn (z pierwszego snapshotu)
      last_team / last_team_ts – ostatnio widziana drużyna + timestamp,
                                 używane do przypisania drużyny eventom
                                 bez teamType w oknie 30 s
      last_clock_event        – do deduplikacji eventów zegarowych
      header_printed          – flaga czy nagłówek tabeli już wydrukowany
      game_over               – flaga końca meczu
    """
    ssl_ctx   = ssl.create_default_context()
    game_over = False

    last_team:        str | None      = None
    last_team_ts:     datetime | None = None
    last_clock_event: str             = ""

    home_name:      str  = ""
    away_name:      str  = ""
    header_printed: bool = False

    topic_main      = f"scoreboards/v1/OB_EV{event_id}"
    topic_incidents = f"scoreboards/v1/OB_EV{event_id}/incidents"

    while True:
        try:
            async with websockets.connect(
                WS_URL,
                additional_headers=HEADERS,
                ssl=ssl_ctx,
                ping_interval=None,   # wyłączamy wbudowany ping websockets —
                ping_timeout=None,    # obsługujemy pingi Diffusion sami (0x00→0x06)
                max_size=10 * 1024 * 1024,
                close_timeout=5,
            ) as ws:
                # Handshake Diffusion — musi być przed subskrypcjami
                await ws.send(base64.b64decode(HANDSHAKE_B64))
                await ws.recv()

                # Subskrybuj oba tematy jednocześnie
                await ws.send(make_subscribe(topic_main))
                await ws.send(make_subscribe(topic_incidents))

                ping_task = asyncio.create_task(keepalive(ws))
                try:
                    async for message in ws:
                        raw   = base64.b64decode(message) if isinstance(message, str) else message
                        ftype = raw[0] if raw else 0xFF
                        now   = datetime.now()
                        now_s = now.strftime("%H:%M:%S")

                        # Zapisz każdą ramkę do pliku logu (przed filtrowaniem)
                        log_frame(ftype, raw)

                        # ── SERVICE_REQUEST serwera → obowiązkowa odpowiedź SERVICE_RESPONSE ──
                        if ftype == 0x00:
                            # Wykryj SYSTEM_PING (service_id == 55) dla logowania debug.
                            # Niezależnie od rodzaju pingu odpowiedź jest taka sama:
                            # bajt[0] zmieniony z 0x00 na 0x06, reszta payloadu bez zmian.
                            if DEBUG and len(raw) >= 3:
                                conv_id    = raw[1]
                                service_id = raw[2]
                                if service_id == 55:
                                    print(f"    [DBG] SYSTEM_PING conv={conv_id} svc={service_id} → wysyłam pong")
                                elif service_id == 56:
                                    print(f"    [DBG] USER_PING conv={conv_id} svc={service_id} → wysyłam pong")
                                else:
                                    print(f"    [DBG] SERVICE_REQUEST conv={conv_id} svc={service_id} → wysyłam pong")
                            await ws.send(make_pong(raw))
                            continue

                        # ── Handshake ACK → echo ─────────────────────────
                        if ftype == 0x01:
                            if DEBUG:
                                print(f"    [DBG] Handshake ACK (0x01) len={len(raw)}")
                            await ws.send(raw)
                            continue

                        # ── Subscribe / unsubscribe ACK → ignoruj ────────
                        if ftype in (0x02, 0x03):
                            if DEBUG:
                                label = "SUBSCRIBE_ACK" if ftype == 0x02 else "UNSUBSCRIBE_ACK"
                                print(f"    [DBG] {label} (0x{ftype:02x}) len={len(raw)}")
                            continue

                        # ── Ramka epoch keepalive → ignoruj ──────────────
                        if ftype == 0x06 and (b"epoch" in raw or len(raw) <= 6):
                            if DEBUG:
                                print(f"    [DBG] Epoch keepalive (0x06) len={len(raw)}")
                            continue

                        # ── Snapshot (pełny stan meczu) ───────────────────
                        if ftype == 0x04:
                            # Wyciągnij nazwy drużyn z pierwszego snapshotu
                            if not home_name:
                                h, a = extract_team_names(raw)
                                if h and a:
                                    home_name = h
                                    away_name = a

                            # Drukuj nagłówek gdy tylko poznamy nazwy drużyn
                            if not header_printed and home_name:
                                print_header(event_id, home_name, away_name)
                                header_printed = True

                        # ── Delta (event meczowy) ─────────────────────────
                        elif ftype == 0x05:
                            # Drukuj nagłówek jeśli jeszcze nie (edge case)
                            if not header_printed:
                                print_header(event_id, home_name, away_name)
                                header_printed = True

                            info  = parse_event(raw)
                            etype = info.get("event_type")

                            # Pomiń ramki bez etype i bez drużyny —
                            # to delta pozycji/pędu bez wartości wyświetleniowej
                            if not etype and not info.get("team"):
                                continue

                            # Ramka z drużyną ale bez etype → aktualizacja statystyk
                            if not etype and info.get("team"):
                                etype = "STAT_UPDATE"

                            # Deduplikacja eventów zegarowych
                            if etype in CLOCK_EVENTS:
                                if etype == last_clock_event:
                                    continue
                                last_clock_event = etype

                            if etype in GAME_OVER_EVENTS:
                                game_over = True

                            # Zaktualizuj ostatnio widzianą drużynę
                            if info.get("team"):
                                last_team    = info["team"]
                                last_team_ts = now

                            # Jeśli brak drużyny — próbuj z kontekstu (okno 30 s)
                            team = info.get("team")
                            if not team and etype and etype not in NO_TEAM_EVENTS:
                                age = (now - last_team_ts).total_seconds() if last_team_ts else 999
                                if last_team and age < 30:
                                    team = last_team

                            print_event_line(now_s, etype, team, home_name, away_name, info, raw)

                            if game_over:
                                print_system_line(now_s, f"KONIEC MECZU  {home_name} vs {away_name}")
                                return

                        # ── Delta w skróconej formie ──────────────────────
                        elif ftype == 0x06:
                            if not header_printed:
                                continue  # poczekaj na snapshot

                            info  = parse_event(raw)
                            etype = info.get("event_type")
                            if not etype:
                                continue

                            if etype in CLOCK_EVENTS:
                                if etype == last_clock_event:
                                    continue
                                last_clock_event = etype

                            if etype in GAME_OVER_EVENTS:
                                game_over = True

                            team = info.get("team")
                            if not team and etype not in NO_TEAM_EVENTS:
                                age = (now - last_team_ts).total_seconds() if last_team_ts else 999
                                if last_team and age < 30:
                                    team = last_team

                            print_event_line(now_s, etype, team, home_name, away_name, info, raw)

                            if game_over:
                                print_system_line(now_s, f"KONIEC MECZU  {home_name} vs {away_name}")
                                return

                        # ── Nieznany typ ramki — wyświetl zawsze ─────────
                        else:
                            strings = ascii_runs(raw)
                            print(f"  {now_s}  {'?':<{COL_TEAM_WIDTH}}{'':6}"
                                  f"UNKNOWN_FRAME ftype=0x{ftype:02x} len={len(raw)}")
                            print(f"    ascii={strings}")
                            print(hexdump(raw))

                finally:
                    ping_task.cancel()
                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass

        except websockets.exceptions.ConnectionClosedError:
            if game_over:
                return
            now_s = datetime.now().strftime("%H:%M:%S")
            if header_printed:
                print_system_line(now_s, "RECONNECTING...")
            await asyncio.sleep(3)
            if header_printed:
                print_system_line(datetime.now().strftime("%H:%M:%S"), "RECONNECTED")

        except websockets.exceptions.WebSocketException as e:
            if game_over:
                return
            now_s = datetime.now().strftime("%H:%M:%S")
            if header_printed:
                print_system_line(now_s, f"RECONNECTING...  ({type(e).__name__})")
            await asyncio.sleep(3)

        except Exception as e:
            now_s = datetime.now().strftime("%H:%M:%S")
            if header_printed:
                print_system_line(now_s, f"BŁĄD  {type(e).__name__}: {e}")
            await asyncio.sleep(5)


# ──────────────────────────────────────────────────────���──────────
# PUNKT WEJŚCIA
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="William Hill Live Scoreboard — odbiera eventy meczowe w czasie rzeczywistym.",
        epilog="Przykład: python williamhill_scoreboard.py 39319952",
    )
    parser.add_argument(
        "event_id",
        help="Identyfikator meczu z URL William Hill, np. 39319952",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Włącz tryb debug: loguje SYSTEM_PING, USER_PING i nierozpoznane ramki",
    )
    parser.add_argument(
        "--frames-log",
        metavar="PLIK",
        help=(
            "Zapisuj wszystkie odebrane ramki (hex dump) do podanego pliku. "
            "Przykład: --frames-log ramki_39319952.txt"
        ),
    )
    args = parser.parse_args()

    # Ustaw globalne flagi przed uruchomieniem pętli
    DEBUG = args.debug
    FRAMES_LOG_PATH = args.frames_log or ""

    try:
        asyncio.run(listen(args.event_id))
    except KeyboardInterrupt:
        print("\n[✓] Zatrzymano.")