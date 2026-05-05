#!/usr/bin/env python3
# scripts/save_nbp_rates.py
# Wersja z migracją legacy + wymuszeniem formatu .json.gz
#
# Zachowuje oryginalną funkcjonalność:
# - backfill
# - fetch ostatnich dni
# - atomic gzip write
# - migrację legacy katalogów
# - usuwanie / konwersję starych .json tak, aby zostały tylko .json.gz

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
import hashlib
import shutil
import re
from typing import Optional, Any

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

# -------------------
# Helper / I/O
# -------------------

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


def plain_json_equivalent_path(gz_path: str) -> Optional[str]:
    """
    Dla pliku ...json.gz zwraca odpowiadający plik ...json.
    """
    if gz_path.endswith(".json.gz"):
        return gz_path[:-3]  # usuwa tylko ".gz"
    return None


def remove_plain_json_equivalent(gz_path: str):
    """
    Jeśli istnieje odpowiadający plik .json dla danego .json.gz, usuwa go.
    """
    plain = plain_json_equivalent_path(gz_path)
    if plain and os.path.exists(plain):
        try:
            os.remove(plain)
            print("🧹 Usunięto legacy .json:", plain)
        except Exception as e:
            print("⚠ Nie udało się usunąć legacy .json:", plain, e)


def write_json_gz_atomic(path, data):
    """
    Zapisuje JSON skompresowany gzip atomowo (tmp -> os.replace).
    Zwraca True jeśli OK.
    """
    dirn = os.path.dirname(path)
    os.makedirs(dirn, exist_ok=True)

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


def file_sha256(path):
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def read_json_from_file(path) -> Optional[Any]:
    """
    Odczytuje JSON z pliku .json lub .json.gz i zwraca obiekt (lub None).
    """
    try:
        if path.endswith(".gz"):
            with gzip.open(path, "rt", encoding="utf-8") as f:
                return json.load(f)
        else:
            with open(path, "rt", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print("⚠ Nie udało się odczytać JSON z", path, ":", e)
        return None


def normalized_json_signature(path: str) -> Optional[str]:
    """
    Zwraca logiczny podpis JSON niezależny od formatu (.json / .json.gz).
    Przydatne przy porównywaniu plików, bo gzip daje różne bajty mimo tego samego JSON-a.
    """
    data = read_json_from_file(path)
    if data is None:
        return None
    try:
        normalized = json.dumps(
            data,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":")
        ).encode("utf-8")
        return hashlib.sha256(normalized).hexdigest()
    except Exception:
        return None


def write_json_gz_from_file_atomic(src_json_path: str, dst_gz_path: str) -> bool:
    """
    Czyta JSON z pliku .json i zapisuje go jako .json.gz atomowo.
    """
    data = read_json_from_file(src_json_path)
    if data is None:
        return False
    ok = write_json_gz_atomic(dst_gz_path, data)
    if ok:
        try:
            os.remove(src_json_path)
            print("🗑 Usunięto źródłowy .json:", src_json_path)
        except Exception as e:
            print("⚠ Zapisano .json.gz, ale nie udało się usunąć .json:", src_json_path, e)
    return ok

# -------------------
# Legacy migration
# -------------------

FNAME_REGEX = re.compile(r"^(\d{2})_(\d{2})_(\d{4})(?:\.json|\.json\.gz)$")


def make_conflict_name(fname: str, ts: str) -> str:
    """
    Zwraca nazwę konfliktową zawsze w formacie .json.gz.
    """
    if fname.endswith(".json.gz"):
        base = fname[:-8]  # usuwa ".json.gz"
    elif fname.endswith(".json"):
        base = fname[:-5]  # usuwa ".json"
    else:
        base = fname
    return f"{base}_conflict_{ts}.json.gz"


def migrate_single_file_to_target(src_path: str, target_path: str, fname: str):
    """
    Przenosi pojedynczy plik legacy do target_path.
    Zawsze końcowy format ma być .json.gz.
    """
    if fname.endswith(".json.gz"):
        # Jeśli już gzip, można przenieść atomowo
        try:
            os.replace(src_path, target_path)
            print("→ Przeniesiono:", src_path, "=>", target_path)
            return
        except Exception as e:
            print("❌ Błąd przenoszenia .json.gz:", src_path, e)
            return

    if fname.endswith(".json"):
        # Jeśli zwykły JSON, kompresujemy do .json.gz
        if write_json_gz_from_file_atomic(src_path, target_path):
            print("→ Skonwertowano:", src_path, "=>", target_path)
        else:
            print("❌ Nie udało się skonwertować:", src_path)
        return

    print("ℹ Pomijam nierelewantny plik:", src_path)


def migrate_legacy_structure():
    """
    Przenosi pliki z katalogów legacy (np. docs/exc/1, docs/exc/4, itp.)
    do katalogów z rokiem.

    Zasady:
      - Jeśli nazwa pliku zawiera dd_mm_YYYY -> używa tego YYYY.
      - W przeciwnym razie próbuje odczytać JSON i wyciągnąć pole "date" (YYYY-MM-DD).
      - Wszystkie końcowe pliki mają być w formacie .json.gz.
      - Konflikty rozwiązywane są przez porównanie logicznej zawartości JSON.
    """
    print("🔧 Sprawdzam strukturę legacy w", BASE_OUT_DIR)
    try:
        entries = os.listdir(BASE_OUT_DIR)
    except FileNotFoundError:
        print("ℹ Brak katalogu", BASE_OUT_DIR)
        return

    for name in entries:
        sub = os.path.join(BASE_OUT_DIR, name)

        # pomijamy pliki markerów i katalogi-roki (czterocyfrowe)
        if not os.path.isdir(sub):
            continue
        if re.fullmatch(r"\d{4}", name):
            continue

        print(f"📂 Przetwarzam legacy katalog: {sub}")
        try:
            files = os.listdir(sub)
        except Exception as e:
            print("⚠ Nie mogę wymienić plików w", sub, ":", e)
            continue

        for fname in files:
            src_path = os.path.join(sub, fname)
            if not os.path.isfile(src_path):
                continue

            if not (fname.endswith(".json") or fname.endswith(".json.gz")):
                print("ℹ Pomijam nierelewantny plik:", src_path)
                continue

            year = None
            m = FNAME_REGEX.match(fname)
            if m:
                year = m.group(3)
            else:
                data = read_json_from_file(src_path)
                if data:
                    dstr = None
                    if isinstance(data, dict):
                        dstr = data.get("date") or data.get("effectiveDate") or data.get("effective_date")
                    if not dstr and isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                        dstr = data[0].get("date") or data[0].get("effectiveDate") or data[0].get("effective_date")
                    if dstr:
                        try:
                            parsed = datetime.strptime(dstr[:10], "%Y-%m-%d").date()
                            year = str(parsed.year)
                        except Exception:
                            year = None

            if not year:
                bad_dir = os.path.join(BASE_OUT_DIR, "bad_legacy")
                os.makedirs(bad_dir, exist_ok=True)
                target = os.path.join(bad_dir, fname)
                if fname.endswith(".json"):
                    target += ".gz"

                if os.path.exists(target):
                    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
                    target = os.path.join(bad_dir, make_conflict_name(fname, ts))

                try:
                    if fname.endswith(".json.gz"):
                        os.replace(src_path, target)
                    else:
                        if write_json_gz_from_file_atomic(src_path, target):
                            pass
                        else:
                            continue
                    print("⚠ Nieznany rok dla", src_path, "=> przeniesiono do", target)
                except Exception as e:
                    print("❌ Nie udało się przenieść", src_path, ":", e)
                continue

            target_dir = os.path.join(BASE_OUT_DIR, year)
            os.makedirs(target_dir, exist_ok=True)

            # Wszystkie pliki końcowe mają być .json.gz
            if fname.endswith(".json.gz"):
                target_name = fname
            else:
                target_name = fname + ".gz"

            target_path = os.path.join(target_dir, target_name)

            # Jeśli target nie istnieje -> proste przeniesienie / konwersja
            if not os.path.exists(target_path):
                migrate_single_file_to_target(src_path, target_path, fname)
                continue

            # target istnieje -> porównaj logiczną zawartość JSON
            src_sig = normalized_json_signature(src_path)
            tgt_sig = normalized_json_signature(target_path)

            try:
                src_mtime = os.path.getmtime(src_path)
                tgt_mtime = os.path.getmtime(target_path)
            except Exception:
                src_mtime = None
                tgt_mtime = None

            if src_sig and tgt_sig and src_sig == tgt_sig:
                # identyczne logicznie -> usuń źródło
                try:
                    os.remove(src_path)
                    print("✔ Plik identyczny logicznie, usunięto źródło:", src_path)
                except Exception as e:
                    print("⚠ Nie udało się usunąć identycznego źródła:", src_path, e)
                continue

            # różne pliki -> jeśli źródło jest nowsze, zrób backup starego targeta i podmień
            if src_mtime and tgt_mtime and src_mtime > tgt_mtime:
                bak_name = os.path.basename(target_path) + ".bak." + datetime.utcnow().strftime("%Y%m%dT%H%M%S")
                bak_path = os.path.join(target_dir, bak_name)
                try:
                    os.replace(target_path, bak_path)
                    if fname.endswith(".json.gz"):
                        os.replace(src_path, target_path)
                    else:
                        if not write_json_gz_from_file_atomic(src_path, target_path):
                            raise RuntimeError("Nie udało się skompresować źródłowego .json")
                    print("⚠ Konflikt: stary target zbackupowany jako", bak_path, "— nowy plik ustawiony jako", target_path)
                except Exception as e:
                    print("❌ Błąd przy zamianie plików (backup + replace):", e)
                    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
                    conflict_name = make_conflict_name(fname, ts)
                    conflict_path = os.path.join(target_dir, conflict_name)
                    try:
                        if fname.endswith(".json.gz"):
                            os.replace(src_path, conflict_path)
                        else:
                            if write_json_gz_from_file_atomic(src_path, conflict_path):
                                pass
                            else:
                                raise RuntimeError("Nie udało się zapisać konfliktowego gzip")
                        print("⚠ Przeniesiono źródło jako konflikt:", conflict_path)
                    except Exception as e2:
                        print("❌ Nie udało się przenieść źródła konfliktowego:", e2)
                continue
            else:
                # target jest nowszy lub nie mamy mtime -> przenieś źródło z suffixem konfliktowym
                ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
                conflict_name = make_conflict_name(fname, ts)
                conflict_path = os.path.join(target_dir, conflict_name)
                try:
                    if fname.endswith(".json.gz"):
                        os.replace(src_path, conflict_path)
                    else:
                        if not write_json_gz_from_file_atomic(src_path, conflict_path):
                            raise RuntimeError("Nie udało się skompresować źródłowego .json")
                    print("⚠ Target nowszy — przeniesiono źródło jako:", conflict_path)
                except Exception as e:
                    print("❌ Nie udało się przenieść konfliktowego źródła:", e)
                    try:
                        if fname.endswith(".json.gz"):
                            shutil.copy2(src_path, conflict_path)
                            os.remove(src_path)
                            print("⚠ Skopiowano źródło jako konflikt:", conflict_path)
                        else:
                            if write_json_gz_from_file_atomic(src_path, conflict_path):
                                print("⚠ Skopiowano źródło jako konflikt:", conflict_path)
                    except Exception as e2:
                        print("❌ Ostateczny błąd przenoszenia/kopii:", e2)

        # po przeniesieniu plików spróbuj usunąć pusty katalog legacy
        try:
            remaining = os.listdir(sub)
            if len(remaining) == 0:
                os.rmdir(sub)
                print("🗑 Usunięto pusty legacy katalog:", sub)
            else:
                print("ℹ Po migracji katalog zawiera nadal pliki (pozostawiam):", sub)
        except Exception as e:
            print("⚠ Nie udało się usunąć katalogu", sub, ":", e)

    print("🔧 Migracja legacy zakończona.")


def convert_plain_json_to_gz():
    """
    Przechodzi po całym BASE_OUT_DIR i konwertuje każdy zwykły .json do .json.gz.
    Dzięki temu w katalogu zostają tylko pliki .json.gz.
    """
    print("🔁 Konwersja pozostałych .json -> .json.gz")

    for root, _, files in os.walk(BASE_OUT_DIR):
        for fname in files:
            if not fname.endswith(".json") or fname.endswith(".json.gz"):
                continue

            src_path = os.path.join(root, fname)
            gz_path = src_path + ".gz"

            # Jeśli gzip już istnieje
            if os.path.exists(gz_path):
                src_sig = normalized_json_signature(src_path)
                gz_sig = normalized_json_signature(gz_path)

                if src_sig and gz_sig and src_sig == gz_sig:
                    try:
                        os.remove(src_path)
                        print("🧹 Usunięto duplikat .json, bo istnieje już .json.gz:", src_path)
                    except Exception as e:
                        print("❌ Nie udało się usunąć duplikatu .json:", src_path, e)
                else:
                    # Konflikt: zapisujemy jeszcze raz jako konfliktowy .json.gz i usuwamy źródło
                    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
                    conflict_path = os.path.join(root, f"{os.path.splitext(fname)[0]}_conflict_{ts}.json.gz")
                    if os.path.exists(conflict_path):
                        # awaryjnie nadpisz inną nazwą
                        conflict_path = os.path.join(root, f"{os.path.splitext(fname)[0]}_conflict_{ts}_{int(time.time())}.json.gz")
                    if write_json_gz_from_file_atomic(src_path, conflict_path):
                        print("⚠ Konfliktowy .json zapisany jako:", conflict_path)
                    else:
                        print("⚠ Nie udało się przekonwertować konfliktowego .json:", src_path)
                continue

            # zwykła konwersja .json -> .json.gz
            if write_json_gz_from_file_atomic(src_path, gz_path):
                print("✅ Zostawiono tylko:", gz_path)
            else:
                print("⚠ Nie udało się przekonwertować:", src_path)

# -------------------
# HTTP + processing
# -------------------

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
            if e.code == 404:
                return e
            if 500 <= e.code < 600 and attempt <= retries:
                wait = backoff_base * (2 ** (attempt - 1))
                print(f"⚠ HTTPError {e.code}, próba {attempt}/{retries}. Czekam {wait}s i retry...")
                time.sleep(wait)
                continue
            return e
        except Exception as e:
            if attempt <= retries:
                wait = backoff_base * (2 ** (attempt - 1))
                print(f"⚠ Błąd sieci ({e}), próba {attempt}/{retries}. Czekam {wait}s i retry...")
                time.sleep(wait)
                continue
            print("❌ HTTP:", e)
            return e


def process_table_entry(entry):
    # defensywne pobranie pól
    eff_date = None
    if isinstance(entry, dict):
        eff_date = entry.get("effectiveDate") or entry.get("effective_date") or entry.get("date")
        rates = entry.get("rates", []) if isinstance(entry, dict) else []
    else:
        print("⚠ Nieoczekiwany entry (nie dict) — pomijam:", entry)
        return False

    if not eff_date:
        print("⚠ Brak pola effectiveDate/date w entry, pomijam:", entry)
        return False

    try:
        d = datetime.strptime(eff_date[:10], "%Y-%m-%d").date()
    except Exception as e:
        print("❌ Nieprawidłowy format daty:", eff_date, e)
        return False

    out_path = path_for_date(d)

    # jeśli istnieje legacy .json dla tej samej daty, usuń go
    remove_plain_json_equivalent(out_path)

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


def backfill(today: Optional[date] = None):
    if today is None:
        today = datetime.now(ZoneInfo(TZ)).date()

    print("🔁 BACKFILL od", START_DATE.isoformat())
    cur = START_DATE
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

    # 1) migracja legacy
    try:
        migrate_legacy_structure()
    except Exception as e:
        print("❌ Błąd podczas migracji legacy (kontynuuję):", e)

    # 2) konwersja wszystkich pozostałych .json -> .json.gz
    try:
        convert_plain_json_to_gz()
    except Exception as e:
        print("❌ Błąd podczas konwersji .json -> .json.gz (kontynuuję):", e)

    # 3) normalny przebieg: backfill jeśli potrzeba + pobranie ostatnich dni
    today = datetime.now(ZoneInfo(TZ)).date()
    if not os.path.exists(BACKFILL_MARKER):
        backfill(today=today)
    else:
        print("✔ Backfill już wykonany")

    fetch_recent_and_today(today, lookback_days=7)

    # 4) końcowe sprzątanie: jeszcze raz usuń / skonwertuj ewentualne pliki .json
    try:
        convert_plain_json_to_gz()
    except Exception as e:
        print("❌ Błąd końcowego sprzątania .json -> .json.gz:", e)

    sys.exit(0)


if __name__ == "__main__":
    main()
