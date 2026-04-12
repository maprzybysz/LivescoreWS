# William Hill Live Scoreboard

Skrypt w Pythonie odbierający eventy meczowe w czasie rzeczywistym
z platformy William Hill przez WebSocket (protokół Diffusion).

## Wymagania

- Python 3.11+
- Biblioteki:

```bash
pip install websockets cbor2
```

## Uruchomienie

```bash
python williamhill_scoreboard.py <event_id>
```

### Opcje

| Opcja | Opis |
|-------|------|
| `event_id` | Identyfikator meczu z URL William Hill (wymagany) |
| `--debug` | Włącz tryb debug: loguje SYSTEM_PING i nierozpoznane ramki |

### Przykład

```bash
python williamhill_scoreboard.py 39319952
python williamhill_scoreboard.py 39319952 --debug
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
  16:01:02  ~                            RECONNECTING...
  16:01:05  ~                            RECONNECTED
  16:02:42  ~                            KONIEC MECZU  Crystal Palace vs Newcastle
```

## Kolumny tabeli

| Kolumna | Opis |
|---------|------|
| `TIME`  | Czas lokalny odebrania eventu |
| `TEAM`  | Drużyna powiązana z eventem (`~` = linia systemowa) |
| `MIN`   | Minuta meczu obliczona z pola `time.duration` incydentu (pusta gdy brak danych) |
| `EVENT` | Typ eventu |

## Uwagi

- Skrypt **automatycznie łączy się ponownie** po zerwaniu połączenia —
  serwer rozłącza klientów co kilka minut, to normalne zachowanie.
- Po odebraniu eventu kończącego mecz (`FULL_TIME`, `GAME_FINISHED`,
  `STOP_2_ND_HALF`, `END_OF_MATCH`) skrypt kończy działanie automatycznie.
- Skrypt nie zapisuje danych — jeśli chcesz logować, przekieruj output:
  ```bash
  python williamhill_scoreboard.py 39319952 | tee mecz.log
  ```