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

# =========================
# KONFIGURACJA
# =========================

TZ = "Europe/Warsaw"

BASE_OUT_DIR = os.path.join("docs", "exc")
MAX_FILES_PER_DIR = 999

START_DATE = date(2021, 1, 1)
CHUNK_DAYS = 93

BACKFILL_MARKER = os.path.join(BASE_OUT_DIR, ".backfill_done")

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

# =========================
# FOLDER LOGIC
# =========================

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

# =========================
# IO
# =========================

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
        return e
    except Exception as e:
        print("❌ HTTP:", e)
        return e

# =========================
# NBP
# =========================

def process_table_entry(entry):
    eff_date = entry["effectiveDate"]
    rates = entry["rates"]

    d = datetime.strptime(eff_date, "%Y-%m-%d").date()
    out_path = path_for_date(d)

    if os.path.exists(out_path):
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

    return write_json_atomic(out_path, payload)


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
    print("🔁 BACKFILL od 2021")
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


def fetch_today(today: date):
    url = SINGLE_DAY_URL.format(date=today.isoformat())
    resp = http_get(url)

    if isinstance(resp, urllib.error.HTTPError):
        if resp.code == 404:
            print("ℹ Brak kursu (weekend/święto)")
            return True
        return False

    if isinstance(resp, Exception):
        return False

    data = json.loads(resp)
    for entry in data:
        process_table_entry(entry)

    return True

# =========================
# MAIN
# =========================

def main():
    ensure_base_dir()
    today = datetime.now(ZoneInfo(TZ)).date()

    if not os.path.exists(BACKFILL_MARKER):
        backfill()
    else:
        print("✔ Backfill już wykonany")

    fetch_today(today)
    sys.exit(0)


if __name__ == "__main__":
    main()
