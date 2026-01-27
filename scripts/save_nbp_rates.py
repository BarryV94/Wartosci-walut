#!/usr/bin/env python3
# scripts/save_nbp_rates.py

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import urllib.request
import urllib.error
import json
import os
import sys
import tempfile

TZ = "Europe/Warsaw"

BASE_OUT_DIR = os.path.join("docs", "exc")
MAX_FILES_PER_DIR = 999

START_DATE = date(2002, 1, 1)
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

def ensure_base_dir():
    os.makedirs(BASE_OUT_DIR, exist_ok=True)

def existing_subdirs():
    result = []
    for name in os.listdir(BASE_OUT_DIR):
        path = os.path.join(BASE_OUT_DIR, name)
        if os.path.isdir(path) and name.isdigit():
            result.append(int(name))
    return sorted(result)

def pick_target_dir():
    subs = existing_subdirs()
    if not subs:
        target = 1
    else:
        last = subs[-1]
        last_path = os.path.join(BASE_OUT_DIR, str(last))
        count = len([f for f in os.listdir(last_path) if f.endswith(".json")])
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

def http_get(url):
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
            return raw.decode(charset)
    except urllib.error.HTTPError as e:
        # zwracamy obiekt HTTPError, caller sprawdza kod
        return e
    except Exception as e:
        print("❌ HTTP:", e)
        return e

def append_last_marker(path):
    try:
        with open(LAST_MARKER, "a", encoding="utf-8") as f:
            now_str = datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"{now_str}: {path}\n")
    except Exception as e:
        print("❌ Błąd zapisu .last:", e)

def process_table_entry(entry):
    eff_date = entry["effectiveDate"]
    rates = entry["rates"]

    d = datetime.strptime(eff_date, "%Y-%m-%d").date()
    out_path = path_for_date(d)

    if os.path.exists(out_path):
        append_last_marker(out_path)
        return True

    payload = {
        "date": eff_date,
        "rates": [
            {
                "currency": r["currency"],
                "code": r["code"],
                "mid": r["mid"],
            }
            for r in rates
        ],
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
    while cur <= today:
        chunk_end = min(cur + timedelta(days=CHUNK_DAYS - 1), today)
        data = fetch_range(cur, chunk_end)
        if data:
            for entry in data:
                process_table_entry(entry)
        cur = chunk_end + timedelta(days=1)
    with open(BACKFILL_MARKER, "w") as f:
        f.write(datetime.utcnow().isoformat())
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
