# Save NBP exchange rates — skrypt i workflow

Poniżej znajdziesz dwa pliki: skrypt Pythona `scripts/save_nbp_rates.py` który:

* Codziennie pobiera bieżące kursy (tabela A) z API NBP i zapisuje je do katalogu `docs/exc`.
* Wykonuje jednorazowe (idempotentne) uzupełnienie archiwum od 2021-01-01 do dnia dzisiejszego, pobierając dane w partiach (maks. 93 dni na żądanie) zgodnie z ograniczeniami API NBP.
* Dla każdej daty tworzy plik `docs/exc/<DD_MM_YYYY>.json` zawierający listę kursów z polami `currency`, `code`, `mid` (wartość średniego kursu). Jeśli plik dla danej daty już istnieje, jest pomijany.

Drugi plik to zmodyfikowany workflow GitHub Actions `.github/workflows/save-nbp-rates.yml`, który uruchamia skrypt codziennie i zatwierdza nowe pliki do repozytorium.

---

## Plik: `scripts/save_nbp_rates.py`

```python
#!/usr/bin/env python3
# scripts/save_nbp_rates.py

"""
Pobiera kursy z API NBP (tabela A). Tworzy pliki JSON w katalogu docs/exc.
Każdy plik ma nazwę zgodną z datą publikacji kursu: DD_MM_YYYY.json

Zasady działania:
- Backfill (od 2021-01-01) wykonany jest w partiach maks. 93 dni (limit API).
- Dla każdej daty zapisujemy lista kursów zawierającą pola: currency, code, mid.
- Pliki istniejące są pomijane (idempotentne).

Uwaga: skrypt używa tylko bibliotek standardowych (urllib), więc nie trzeba instalować dodatkowych zależności.
"""

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import urllib.request
import json
import os
import sys
import tempfile

# Konfiguracja
TZ = "Europe/Warsaw"
OUT_DIR = os.path.join("docs", "exc")
START_DATE = date(2021, 1, 1)  # data początkowa do backfill
CHUNK_DAYS = 93  # maksymalny zakres jednego zapytania do API NBP
BASE_TABLE_URL = "https://api.nbp.pl/api/exchangerates/tables/A/{start}/{end}/?format=json"
SINGLE_DAY_URL = "https://api.nbp.pl/api/exchangerates/tables/A/{date}/?format=json"

HEADERS = {"User-Agent": "save-nbp-rates-script/1.0 (+https://github.com/)"}


def ensure_out_dir():
    os.makedirs(OUT_DIR, exist_ok=True)


def path_for_date(d: date):
    return os.path.join(OUT_DIR, d.strftime("%d_%m_%Y.json"))


def write_json_atomic(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(suffix=".json", dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "wb") as tmpf:
            tmpf.write(json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        os.replace(tmp_path, path)
        print("✅ Zapisano:", path)
        return True
    except Exception as e:
        print("❌ Błąd zapisu:", e)
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        return False


def http_get(url, timeout=60):
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
            return raw.decode(charset)
    except urllib.error.HTTPError as e:
        # zwracamy obiekt wyjątku, by caller mógł rozpoznać 404
        return e
    except Exception as e:
        print("❌ Błąd HTTP:", e)
        return e


def fetch_tables_range(start_dt: date, end_dt: date):
    url = BASE_TABLE_URL.format(start=start_dt.isoformat(), end=end_dt.isoformat())
    print(f"Pobieram zakres: {start_dt} -> {end_dt}")
    resp = http_get(url)
    if isinstance(resp, urllib.error.HTTPError):
        print(f"❌ HTTP error {resp.code} dla {url}")
        return None
    if isinstance(resp, Exception):
        return None
    try:
        parsed = json.loads(resp)
        return parsed
    except Exception as e:
        print("❌ Niepoprawny JSON podczas parsowania zakresu:", e)
        return None


def process_table_entry(table_entry):
    # table_entry ma strukturę zwróconą przez API NBP dla tabeli (contains 'effectiveDate' i 'rates')
    eff_date = table_entry.get("effectiveDate")
    rates = table_entry.get("rates", [])
    try:
        d = datetime.fromisoformat(eff_date).date()
    except Exception:
        d = datetime.strptime(eff_date, "%Y-%m-%d").date()

    out_path = path_for_date(d)
    if os.path.exists(out_path):
        print(f"✔ Istnieje: {out_path}")
        return True

    # przekształcamy rates, zachowując wymagane pola
    simplified = []
    for r in rates:
        simplified.append({
            "currency": r.get("currency"),
            "code": r.get("code"),
            "mid": r.get("mid")
        })

    return write_json_atomic(out_path, {"date": eff_date, "rates": simplified})


def backfill(start_date: date, end_date: date):
    success = True
    cur = start_date
    while cur <= end_date:
        chunk_end = min(cur + timedelta(days=CHUNK_DAYS - 1), end_date)
        parsed = fetch_tables_range(cur, chunk_end)
        if parsed is None:
            # jeśli błąd, spróbuj przeskoczyć ten chunk, ale kontynuujemy
            print(f"⚠ Pusty/niepoprawny wynik dla zakresu {cur} - {chunk_end}, kontynuuję dalej.")
            cur = chunk_end + timedelta(days=1)
            success = False
            continue

        # parsed jest listą obiektów — jeden na każdy dzień publikacji
        for table_entry in parsed:
            ok = process_table_entry(table_entry)
            if not ok:
                success = False

        cur = chunk_end + timedelta(days=1)
    return success


def fetch_today_and_save(today_local_date: date):
    # Najpierw sprawdzamy single day (przydatne jeśli chcemy mieć pewność co do 'dzisiaj')
    url = SINGLE_DAY_URL.format(date=today_local_date.isoformat())
    print(f"Pobieram dane dla dnia: {today_local_date}")
    resp = http_get(url)
    if isinstance(resp, urllib.error.HTTPError):
        if resp.code == 404:
            print(f"ℹ Brak publikacji dla {today_local_date} (np. weekend/święto).")
            return True
        else:
            print(f"❌ Błąd HTTP {resp.code} dla dnia {today_local_date}")
            return False
    if isinstance(resp, Exception):
        return False

    try:
        parsed = json.loads(resp)
    except Exception as e:
        print("❌ Niepoprawny JSON dla dnia dzisiejszego:", e)
        return False

    # API zwraca listę (jedna tabela). Przetwarzamy każdy element (zwykle jeden)
    success = True
    for table_entry in parsed:
        if not process_table_entry(table_entry):
            success = False
    return success


def main():
    now_local = datetime.now(ZoneInfo(TZ))
    today_local_date = now_local.date()

    ensure_out_dir()

    overall_success = True

    # 1) Backfill od START_DATE do wczoraj (aby nie dublować pracy przy pobieraniu 'dzisiaj')
    if START_DATE <= today_local_date:
        end_backfill = today_local_date
        print(f"Rozpoczynam backfill od {START_DATE} do {end_backfill}")
        if not backfill(START_DATE, end_backfill):
            overall_success = False

    # 2) Dodatkowo upewniamy się, że dla dzisiaj też mamy plik (jeśli opublikowano)
    if not fetch_today_and_save(today_local_date):
        overall_success = False

    sys.exit(0 if overall_success else 1)


if __name__ == "__main__":
    main()
```

---

## Plik: `.github/workflows/save-nbp-rates.yml`

```yaml
name: Save NBP exchange rates daily (backfill + daily)

permissions:
  contents: write
on:
  schedule:
    - cron: '0 5 * * *'    # codziennie o 05:00 UTC
  workflow_dispatch: {}

jobs:
  save:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4
        with:
          persist-credentials: true

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.x'

      - name: Run save script
        run: python3 scripts/save_nbp_rates.py

      - name: Commit & push (only if new files)
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git add docs/exc || true
          git diff --staged --quiet || (git commit -m "Add NBP exchange rates: $(date -u +'%Y-%m-%d %H:%M UTC')" && git push)
```

---

### Uwagi i wskazówki

* Skrypt jest idempotentny — bezpieczny do ponownego uruchamiania (nie nadpisuje istniejących plików).
* Backfill używa endpointu zakresowego API NBP z limitem 93 dni, dzięki czemu liczba żądań jest ograniczona.
* Pliki tworzone są w `docs/exc` i mają format JSON z kluczem `date` oraz listą `rates` z polami `currency`, `code`, `mid`.
* Jeśli chcesz inny format plików (np. pliki per-waluta zamiast per-daty) napisz, dostosuję skrypt.

Jeśli chcesz, mogę od razu wstawić te pliki do Twojego repo (np. wygenerować patch), lub zmienić format nazewnictwa plików — daj znać która konwencja nazewnictwa (np. `DD_MM_YYYY.json` lub `YYYY-MM-DD.json`) jest dla Ciebie preferowana.
