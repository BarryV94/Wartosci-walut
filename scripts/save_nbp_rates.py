#!/usr/bin/env python3
# scripts/save_nbp_rates.py
# Rozszerzona wersja z migracją plików umieszczonych w katalogach typu docs/exc/1, docs/exc/2, ...

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import urllib.request
import urllib.error
import json
import os
import sys
import tempfile
import time
import gzip
import shutil
import re

TZ = "Europe/Warsaw"

BASE_OUT_DIR = os.path.join("docs", "exc")

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

DATE_FILENAME_RE = re.compile(r"^(\d{2})_(\d{2})_(\d{4})\.json\.gz$")


# --- util

def ensure_base_dir():
    os.makedirs(BASE_OUT_DIR, exist_ok=True)


def append_last_marker(path):
    try:
        os.makedirs(os.path.dirname(LAST_MARKER), exist_ok=True)
        with open(LAST_MARKER, "a", encoding="utf-8") as f:
            now_str = datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"{now_str}: {path}\n")
    except Exception as e:
        print("❌ Błąd zapisu .last:", e)


def path_for_date(d: date):
    """
    Zwraca ścieżkę docs/exc/<YEAR>/<dd_mm_YYYY>.json.gz i tworzy katalog YEAR jeśli potrzeba.
    """
    year_dir = os.path.join(BASE_OUT_DIR, str(d.year))
    os.makedirs(year_dir, exist_ok=True)
    filename = d.strftime("%d_%m_%Y.json.gz")
    return os.path.join(year_dir, filename)


def write_json_gz_atomic(path, data):
    """
    Zapisuje JSON skompresowany gzip atomowo (tmp -> os.replace).
    Zwraca True jeśli OK.
    """
    dirn = os.path.dirname(path)
    os.makedirs(dirn, exist_ok=True)
    # przygotuj zawartość
    try:
        payload_bytes = json.dumps(
            data,
            ensure_ascii=False,
            separators=(",", ":")
        ).encode("utf-8")
    except Exception as e:
        print("❌ Błąd serializacji JSON:", e)
        return False

    fd, tmp_path = tempfile.mkstemp(suffix=".json.gz", dir=dirn)
    os.close(fd)
    try:
        with gzip.open(tmp_path, "wb") as gz:
            gz.write(payload_bytes)
        os.replace(tmp_path, path)
        print("✅ Zapisano:", path)
        return True
    except Exception as e:
        print("❌ Błąd zapisu (gzip):", e)
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

        if not rate_entry:
            print("⚠ Pusty/nieużyteczny rate_entry — pomijam:", r)
            continue

        rates_list.append(rate_entry)

    payload = {
        "date": eff_date,
        "rates": rates_list,
    }

    if write_json_gz_atomic(out_path, payload):
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
    start = today - timedelta(days=lookback_days - 1)
    print(f"🔎 Próba pobrania zakresu {start.isoformat()} — {today.isoformat()}")
    data = fetch_range(start, today)
    if data:
        print(f"ℹ Znalazłem {len(data)} wpisów w zakresie, przetwarzam...")
        for entry in data:
            process_table_entry(entry)
        return True

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


# --- Nowa funkcja migracji

def migrate_misplaced_files():
    """
    Przeszukuje BASE_OUT_DIR i przenosi pliki o nazwie dd_mm_YYYY.json.gz,
    które leżą w katalogach nie-będących katalogami roku (np. '1', '2', '3', ...),
    do katalogu docs/exc/<YYYY>/<dd_mm_YYYY>.json.gz

    Zachowanie przy konflikcie:
      - jeśli plik docelowy nie istnieje -> move
      - jeśli istnieje i ma tę samą wielkość -> usuń źródło (uznajemy za duplikat)
      - jeśli istnieje i różna wielkość -> przenieś źródło, dodając suffix "-conflict-<timestamp>"
    """
    print("🔧 Sprawdzam i migruję pliki z błędnych katalogów (jeśli występują)...")
    if not os.path.isdir(BASE_OUT_DIR):
        print("ℹ Brak katalogu bazowego, pomijam migrację.")
        return

    for entry in os.listdir(BASE_OUT_DIR):
        entry_path = os.path.join(BASE_OUT_DIR, entry)
        # Jeśli to katalog-rok (4 cyfry), pomiń
        if not os.path.isdir(entry_path):
            continue
        if re.fullmatch(r"\d{4}", entry):
            # katalog prawdopodobnie prawidłowy: "2023", "2024"
            continue

        # Przeszukujemy katalog entry_path w poszukiwaniu plików pasujących do nazwy daty
        moved_any = False
        for root, dirs, files in os.walk(entry_path):
            for fname in files:
                m = DATE_FILENAME_RE.match(fname)
                if not m:
                    continue
                day, month, year = m.groups()
                try:
                    # validacja daty
                    _ = date(int(year), int(month), int(day))
                except Exception:
                    print(f"⚠ Nieprawidłowa data w nazwie pliku {fname} — pomijam.")
                    continue

                src = os.path.join(root, fname)
                dest_dir = os.path.join(BASE_OUT_DIR, year)
                os.makedirs(dest_dir, exist_ok=True)
                dest = os.path.join(dest_dir, fname)

                if not os.path.exists(dest):
                    try:
                        shutil.move(src, dest)
                        print(f"➡ Przeniesiono: {src} -> {dest}")
                        moved_any = True
                    except Exception as e:
                        print(f"❌ Nie udało się przenieść {src} -> {dest}: {e}")
                else:
                    try:
                        src_sz = os.path.getsize(src)
                        dest_sz = os.path.getsize(dest)
                        if src_sz == dest_sz:
                            # duplikat -> usuń źródło
                            os.remove(src)
                            print(f"ℹ Duplikat (ten sam rozmiar) — usunięto źródło: {src}")
                            moved_any = True
                        else:
                            ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
                            new_name = f"{fname[:-7]}-conflict-{ts}.json.gz"  # fname[:-7] to dd_mm_YYYY
                            new_dest = os.path.join(dest_dir, new_name)
                            shutil.move(src, new_dest)
                            print(f"⚠ Konflikt rozmiaru — przeniesiono jako: {new_dest}")
                            moved_any = True
                    except Exception as e:
                        print(f"❌ Błąd przy obsłudze konfliktu dla {src}: {e}")

        # po przejściu po katalogu spróbuj usunąć pusty katalogy
        # (tylko jeśli emptiness; nie usuwamy katalogu jeśli coś pozostało)
        for root, dirs, files in os.walk(entry_path, topdown=False):
            # usuń pliki tymczasowe (opcjonalnie) — tu pomijamy
            if not os.listdir(root):
                try:
                    os.rmdir(root)
                    print(f"🧹 Usunięto pusty katalog: {root}")
                except Exception:
                    pass

        if moved_any:
            print(f"✅ Migracja z katalogu {entry_path} zakończona.")
        else:
            # brak plików do migracji
            # (możemy usunąć puste katalogi powyżej, już zrobione)
            pass

    print("🔧 Migracja zakończona.")


def main():
    ensure_base_dir()
    # wykonaj migrację starych plików (jeśli jakieś są w niewłaściwych folderach)
    try:
        migrate_misplaced_files()
    except Exception as e:
        print("❌ Błąd podczas migracji plików:", e)

    today = datetime.now(ZoneInfo(TZ)).date()
    if not os.path.exists(BACKFILL_MARKER):
        backfill()
    else:
        print("✔ Backfill już wykonany")
    fetch_recent_and_today(today, lookback_days=7)
    sys.exit(0)


if __name__ == "__main__":
    main()
