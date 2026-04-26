# William Hill Live Scoreboard

Skrypt w Pythonie odbierający eventy meczowe w czasie rzeczywistym
z platformy William Hill przez WebSocket (protokół Diffusion).

Dostępne są dwie wersje:

| Plik | Opis |
|------|------|
| `williamhill_scoreboard.py` | v1 — ręczna implementacja protokołu Diffusion przez `websockets` |
| `williamhill_scoreboard_v2.py` | v2 — oficjalny Python SDK `diffusion` (zalecana) |

## Wymagania

- Python 3.11+

### v1 (ręczna)
```bash
pip install websockets cbor2
```

### v2 (SDK Diffusion — zalecana)
```bash
pip install diffusion cbor2
```

## Uruchomienie

### v1
```bash
python williamhill_scoreboard.py <event_id>
```

### v2 (SDK)
```bash
python williamhill_scoreboard_v2.py <event_id>
```

### Opcje

| Opcja | Opis |
|-------|------|
| `event_id` | Identyfikator meczu z URL William Hill (wymagany) |
| `--debug` | Włącz tryb debug: loguje subskrypcje i nierozpoznane ramki |

### Przykład

```bash
python williamhill_scoreboard_v2.py 39319952
python williamhill_scoreboard_v2.py 39319952 --debug
```

### Przykładowy output

```
────────────────────────────────────────────────────────────
  OB_EV39319952  |  Crystal Palace vs Newcastle
────────────────────────────────────────────────────────────
  TIME      TEAM                  MIN    EVENT
────────────────────────────────────────────────────────────
  15:45:45                        1'     FIRST_HALF
  15:45:53  Newcastle             2'     DANGEROUS_FREE_KICK
  15:46:09  Newcastle             3'     SAFE
  15:46:25  Newcastle             3'     SHOT_OFF_TARGET
  15:47:01  Crystal Palace        4'     GOAL
  16:02:42  ~                            KONIEC MECZU  Crystal Palace vs Newcastle
```

## Co robi SDK Diffusion zamiast v1?

| v1 (ręczna) | v2 (SDK) |
|---|---|
| `HANDSHAKE_B64` — ręczny Base64 handshake | ✅ SDK robi to automatycznie |
| `make_subscribe()` — ręczne budowanie ramki | ✅ `session.topics.subscribe(path)` |
| `make_pong()` — odpowiedź na SYSTEM_PING 0x00 | ✅ SDK odpowiada na pingi automatycznie |
| `make_keepalive()` — task asyncio co 20s | ✅ SDK utrzymuje keepalive wewnętrznie |
| Parsowanie bajtów ramki (0x04/0x05/0x06) | ✅ SDK dostarcza czyste bajty CBOR |
| Ręczny auto-reconnect po `ConnectionClosedError` | ✅ SDK reconnektuje automatycznie |

## Kolumny tabeli

| Kolumna | Opis |
|---------|------|
| `TIME`  | Czas lokalny odebrania eventu |
| `TEAM`  | Drużyna powiązana z eventem (`~` = linia systemowa) |
| `MIN`   | Minuta meczu obliczona z pola `time.duration` incydentu (pusta gdy brak danych) |
| `EVENT` | Typ eventu |

## Uwagi

- Po odebraniu eventu kończącego mecz (`FULL_TIME`, `GAME_FINISHED`,
  `STOP_2_ND_HALF`, `END_OF_MATCH`) skrypt kończy działanie automatycznie.
- Skrypt nie zapisuje danych — jeśli chcesz logować, przekieruj output:
  ```bash
  python williamhill_scoreboard_v2.py 39319952 | tee mecz.log
  ```