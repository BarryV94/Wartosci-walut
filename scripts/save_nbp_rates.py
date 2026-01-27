#!/usr/bin/env python3
# scripts/save_nbp_rates.py

"""
Pobiera tabele kursów z API NBP i zapisuje je do plików JSON w katalogu docs/exc.

Zmiany względem oryginału:
- Domyślny START_YEAR = 2002 (można nadpisać zmienną środowiskową START_YEAR)
- Bezpieczniejsze przetwarzanie wpisów (obsługa brakujących/pustych pól w `rates`)
- Zapis problematycznych wpisów do docs/exc/bad_entries zamiast przerywania backfilla
- http_get z retry/backoff (dla transient errors)

Uruchomienie przykładowe:

    START_YEAR=2002 python scripts/save_nbp_rates.py

"""

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import urllib.request
import urllib.error
import json
import os
import sys
import tempfile
import time

TZ = "Europe/Warsaw"

BASE_OUT_DIR = os.path.join("docs", "exc")
MAX_FILES_PER_DIR = 999

# Domyślny rok startowy: 2002. Nadpisz przez START_YEAR w env, np. START_YEAR=2010
START_YEAR = int(os.getenv("START_YEAR", "2002"))
START_DATE = date(START_YEAR, 1, 1)

CHUNK_DAYS = 93

BACKFILL_MARKER = os.path.join(BASE_OUT_DIR, ".backfill_done")
LAST_MARKER = os.path.join(BASE_OUT_DIR, ".last")

BASE_TABLE_URL = (
    "https://api.nbp.pl/api/exchangerates/tables/A/"
    "{start}/{end}/?format=json"
)
SINGLE_DAY_URL = (
    "https://api.nbp.pl/api/exchangerates/tables/A/"
    "{date}/?format=json"
)

HEADERS = {
    "User-Agent": "nbp-exchange-rates-fetcher/1.0"
}

# --- util

def ensure_base_dir():
    os.makedirs(BASE_OUT_DIR, exist_ok=True)


def existing_subdirs():
    result = []
    if not os.path.isdir(BASE_OUT_DIR):
        return result
    for name in os.listdir(BASE_OUT_DIR):
        path = os.path.join(BASE_OUT_DIR, name)
        if os.path.isdir(path) and name.isdigit():
            try:
                result.append(int(name))
            except Exception:
                continue
    return sorted(result)


def pick_target_dir():
    subs = existing_subdirs()
    if not subs:
        target = 1
    else:
        last = subs[-1]
        last_path = os.path.join(BASE_OUT_DIR, str(last))
        try:
            count = len([f for f in os.listdir(last_path) if f.endswith(".json")])
        except Exception:
            count = 0
        target = last if count < MAX_FILES_PER_DIR else last + 1
    path = os.path.join(BASE_OUT_DIR, str(target))
    os.makedirs(path, exist_ok=True)
    return path


def path_for_date(d: date):
    base = pick_target_dir()
    return os.path.join(base, d.strftime("%d_%m_%Y.json"))


def write_json_atomic(path, data):
    fd, tmp_path = tempfile.mkstemp(
        suffix=".json",
        dir=os.path.dirname(path)
    )
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(json.dumps(
                data,
                ensure_ascii=False,
                separators=(",", ":")
            ).encode("utf-8"))
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


# http_get z retry/backoff. Zwraca treść (string) lub obiekt urllib.error.HTTPError lub inny Exception
def http_get(url, retries=3, backoff_base=0.5, timeout=60):
    attempt = 0
    while True:
        attempt += 1
        req = urllib.request.Request(url, headers=HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                charset = resp.headers.get_content_charset() or "utf-8"
                return raw.decode(charset)
        except urllib.error.HTTPError as e:
            # 404 chcemy zwrócić natychmiast (ktoś sprawdza resp.code == 404)
            if e.code == 404:
                return e
            # dla błędów serwera możemy spróbować retry
            if 500 <= e.code < 600 and attempt <= retries:
                wait = backoff_base * (2 ** (attempt - 1))
                print(f"⚠ HTTPError {e.code}, próba {attempt}/{retries}. Czekam {wait}s i retry...")
                time.sleep(wait)
                continue
            return e
        except Exception as e:
            # transient network error - retry limited times
            if attempt <= retries:
                wait = backoff_base * (2 ** (attempt - 1))
                print(f"⚠ Błąd sieci ({e}), próba {attempt}/{retries}. Czekam {wait}s i retry...")
                time.sleep(wait)
                continue
            print("❌ HTTP:", e)
            return e


def append_last_marker(path):
    try:
        os.makedirs(os.path.dirname(LAST_MARKER), exist_ok=True)
        with open(LAST_MARKER, "a", encoding="utf-8") as f:
            now_str = datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"{now_str}: {path}\n")
    except Exception as e:
        print("❌ Błąd zapisu .last:", e)


# defensywna funkcja przetwarzajaca pojedyńczy wpis
def process_table_entry(entry):
    # defensywne pobranie pól
    eff_date = None
    if isinstance(entry, dict):
        eff_date = entry.get("effectiveDate") or entry.get("effective_date")
        rates = entry.get("rates", []) if isinstance(entry, dict) else []
    else:
        print("⚠ Nieoczekiwany entry (nie dict) — pomijam:", entry)
        return False

    if not eff_date:
        print("⚠ Brak pola effectiveDate w entry, pomijam:", entry)
        return False

    try:
        d = datetime.strptime(eff_date, "%Y-%m-%d").date()
    except Exception as e:
        print("❌ Nieprawidłowy format daty:", eff_date, e)
        return False

    out_path = path_for_date(d)

    if os.path.exists(out_path):
        append_last_marker(out_path)
        return True

    rates_list = []
    for r in rates:
        if not isinstance(r, dict):
            print("⚠ Nieoczekiwany element w rates (nie dict) — pomijam:", r)
            continue

        # zbieramy dostępne pola (obsługa mid oraz bid/ask)
        code = r.get("code")
        currency = r.get("currency") or r.get("name") or None

        rate_entry = {}
        if currency is not None:
            rate_entry["currency"] = currency
        if code is not None:
            rate_entry["code"] = code
        if "mid" in r:
            rate_entry["mid"] = r["mid"]
        if "bid" in r:
            rate_entry["bid"] = r["bid"]
        if "ask" in r:
            rate_entry["ask"] = r["ask"]

        # jeśli nie znaleziono nic sensownego — logujemy i pomijamy
        if not rate_entry:
            print("⚠ Pusty/nieużyteczny rate_entry — pomijam:", r)
            continue

        rates_list.append(rate_entry)

    payload = {
        "date": eff_date,
        "rates": rates_list,
    }

    if write_json_atomic(out_path, payload):
        append_last_marker(out_path)
        return True
    return False


def fetch_range(start_d: date, end_d: date):
    url = BASE_TABLE_URL.format(
        start=start_d.isoformat(),
        end=end_d.isoformat()
    )
    resp = http_get(url)
    if isinstance(resp, Exception):
        return None
    try:
        return json.loads(resp)
    except Exception:
        return None


def backfill():
    print("🔁 BACKFILL od", START_DATE.isoformat())
    cur = START_DATE
    today = date.today()
    bad_dir = os.path.join(BASE_OUT_DIR, "bad_entries")
    os.makedirs(bad_dir, exist_ok=True)

    while cur <= today:
        chunk_end = min(cur + timedelta(days=CHUNK_DAYS - 1), today)
        print(f"📥 Pobieram zakres: {cur.isoformat()} — {chunk_end.isoformat()}")
        data = fetch_range(cur, chunk_end)
        if data:
            for entry in data:
                try:
                    process_table_entry(entry)
                except Exception as e:
                    # nie przerywamy backfilla — zapisujemy problematyczny wpis do folderu bad_entries
                    print("❌ Błąd przetwarzania wpisu (zapisuję do bad_entries):", e)
                    bad_path = os.path.join(
                        bad_dir,
                        "bad_" + datetime.utcnow().isoformat().replace(":", "_") + ".json"
                    )
                    try:
                        with open(bad_path, "w", encoding="utf-8") as bf:
                            json.dump(entry, bf, ensure_ascii=False, indent=2)
                        print("ℹ Zapisano problematyczny wpis:", bad_path)
                    except Exception as e2:
                        print("❌ Nie udało się zapisać problematycznego wpisu:", e2)
        else:
            print(f"⚠ Brak danych dla zakresu {cur.isoformat()} — {chunk_end.isoformat()}")

        cur = chunk_end + timedelta(days=1)

    try:
        with open(BACKFILL_MARKER, "w", encoding="utf-8") as f:
            f.write(datetime.utcnow().isoformat())
    except Exception as e:
        print("❌ Nie udało się zapisać BACKFILL_MARKER:", e)
    print("✅ BACKFILL ZAKOŃCZONY")


def fetch_recent_and_today(today: date, lookback_days: int = 7):
    """
    Najpierw spróbuj pobrać tabelę dla zakresu (today-lookback_days .. today).
    Jeśli to nic nie zwróci (np. API odpowiedziało 404), spróbuj pojedyncze dni
    cofając się od today do today-lookback_days (zachowując obsługę 404).
    Zwraca True jeśli wykonał się poprawnie (nawet jeśli nic nie było do zapisania).
    """
    start = today - timedelta(days=lookback_days - 1)
    print(f"🔎 Próba pobrania zakresu {start.isoformat()} — {today.isoformat()}")
    data = fetch_range(start, today)
    if data:
        print(f"ℹ Znalazłem {len(data)} wpisów w zakresie, przetwarzam...")
        for entry in data:
            process_table_entry(entry)
        return True

    # jeśli zakres nic nie zwrócił, spróbuj po kolei — od dziś wstecz
    print("ℹ Zakres nic nie zwrócił — próbuję pojedynczych dni wstecz")
    for i in range(0, lookback_days):
        d = today - timedelta(days=i)
        url = SINGLE_DAY_URL.format(date=d.isoformat())
        resp = http_get(url)
        if isinstance(resp, urllib.error.HTTPError):
            # 404 -> brak tabeli w tym dniu (weekend/święto)
            if resp.code == 404:
                print(f"ℹ {d.isoformat()}: brak (404)")
                continue
            print(f"❌ Błąd HTTP dla {d.isoformat()}: {resp}")
            return False
        if isinstance(resp, Exception):
            print("❌ Błąd przy pobieraniu:", resp)
            return False
        try:
            data = json.loads(resp)
        except Exception as e:
            print("❌ Nie udało się zdekodować JSON:", e)
            return False
        if data:
            print(f"ℹ {d.isoformat()}: znaleziono dane, przetwarzam...")
            for entry in data:
                process_table_entry(entry)
            return True

    print(f"ℹ Brak kursów w ostatnich {lookback_days} dniach (weekend/święta).")
    return True


def main():
    ensure_base_dir()
    today = datetime.now(ZoneInfo(TZ)).date()
    if not os.path.exists(BACKFILL_MARKER):
        backfill()
    else:
        print("✔ Backfill już wykonany")
    # spróbuj pobrać dane dla ostatnich dni (zwykle złapie też dzisiejsze, jeśli istnieją)
    fetch_recent_and_today(today, lookback_days=7)
    sys.exit(0)


if __name__ == "__main__":
    main()
