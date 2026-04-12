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
### Przykład

```bash
python williamhill_scoreboard.py 39319952
```

### Przykładowy output

```
────────────────────────────────────────────────────────────
  OB_EV39319952  |  Crystal Palace vs Newcastle
────────────────────────────────────────────────────────────
  TIME      TEAM                  EVENT
────────────────────────────────────────────────────────────
  15:45:45                        FIRST_HALF
  15:45:53  Newcastle             DANGEROUS_FREE_KICK
  15:46:09  Newcastle             SAFE
  15:46:25  Newcastle             SHOT_OFF_TARGET
  15:47:01  Crystal Palace        GOAL
  16:01:02  ~                     RECONNECTING...
  16:01:05  ~                     RECONNECTED
  16:02:42  ~                     KONIEC MECZU  Crystal Palace vs Newcastle
```

## Kolumny tabeli

| Kolumna | Opis |
|---------|------|
| `TIME`  | Czas lokalny odebrania eventu |
| `TEAM`  | Drużyna powiązana z eventem (`~` = linia systemowa) |
| `EVENT` | Typ eventu |

## Uwagi

- Skrypt **automatycznie łączy się ponownie** po zerwaniu połączenia —
  serwer rozłącza klientów co kilka minut, to normalne zachowanie.
- Po odebraniu eventu kończącego mecz (`FULL_TIME`, `GAME_FINISHED`,
  `STOP_2_ND_HALF`) skrypt kończy działanie automatycznie.
- Skrypt nie zapisuje danych — jeśli chcesz logować, przekieruj output:
  ```bash
  python williamhill_scoreboard.py 39319952 | tee mecz.log
  ```