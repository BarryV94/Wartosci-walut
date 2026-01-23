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
# POMOCNICZE
# =========================

def ensure_base_dir():
    os.makedirs(BASE_OUT_DIR, exist_ok=True)


def existing_subdirs():
    result = []
    if not os.path.exists(BASE_OUT_DIR):
        return result

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
        json_count = len([
            f for f in os.listdir(last_path)
            if f.endswith(".json")
        ])
        target = last if json_count < MAX_FILES_PER_DIR else last + 1

    target_path = os.path.join(BASE_OUT_DIR, str(target))
    os.makedirs(target_path, exist_ok=True)
    return target_path


def path_for_date(d: date):
    base = pick_target_dir()
    filename = d.strftime("%d_%m_%Y.json")
    return os.path.join(base, filename)


def write_json_atomic(path, data):
    fd, tmp_path = tempfile.mkstemp(
        suffix=".json",
        dir=os.path.dirname(path)
    )
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(
                json.dumps(
                    data,
                    ensure_ascii=False,
                    separators=(",", ":")
                ).encode("utf-8")
            )
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
        print("❌ Błąd HTTP:", e)
        return e

# =========================
# LOGIKA NBP
# =========================

def process_table_entry(entry):
    eff_date = entry.get("effectiveDate")
    rates = entry.get("rates", [])

    d = datetime.strptime(eff_date, "%Y-%m-%d").date()
    out_path = path_for_date(d)

    if os.path.exists(out_path):
        print("✔ Istnieje:", out_path)
        return True

    simplified = []
    for r in rates:
        simplified.append({
            "currency": r.get("currency"),
            "code": r.get("code"),
            "mid": r.get("mid"),
        })

    payload = {
        "date": eff_date,
        "rates": simplified,
    }

    return write_json_atomic(out_path, payload)


def fetch_range(start_d: date, end_d: date):
    url = BASE_TABLE_URL.format(
        start=start_d.isoformat(),
        end=end_d.isoformat()
    )
    print(f"⬇ Zakres: {start_d} → {end_d}")

    resp = http_get(url)

    if isinstance(resp, urllib.error.HTTPError):
        print(f"❌ HTTP {resp.code} dla zakresu")
        return None

    if isinstance(resp, Exception):
        return None

    try:
        return json.loads(resp)
    except Exception as e:
        print("❌ Błąd JSON:", e)
        return None


def backfill(start_d: date, end_d: date):
    cur = start_d
    ok = True

    while cur <= end_d:
        chunk_end = min(cur + timedelta(days=CHUNK_DAYS - 1), end_d)
        data = fetch_range(cur, chunk_end)

        if data is None:
            ok = False
            cur = chunk_end + timedelta(days=1)
            continue

        for entry in data:
            if not process_table_entry(entry):
                ok = False

        cur = chunk_end + timedelta(days=1)

    return ok


def fetch_today(today: date):
    url = SINGLE_DAY_URL.format(date=today.isoformat())
    print("⬇ Dzień:", today)

    resp = http_get(url)

    if isinstance(resp, urllib.error.HTTPError):
        if resp.code == 404:
            print("ℹ Brak publikacji (weekend/święto)")
            return True
        print("❌ HTTP", resp.code)
        return False

    if isinstance(resp, Exception):
        return False

    try:
        data = json.loads(resp)
    except Exception as e:
        print("❌ Błąd JSON:", e)
        return False

    ok = True
    for entry in data:
        if not process_table_entry(entry):
            ok = False

    return ok

# =========================
# MAIN
# =========================

def main():
    ensure_base_dir()

    now = datetime.now(ZoneInfo(TZ))
    today = now.date()

    success = True

    if START_DATE <= today:
        if not backfill(START_DATE, today):
            success = False

    if not fetch_today(today):
        success = False

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
