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
START_DATE = date(2021, 1, 1) # data początkowa do backfill
CHUNK_DAYS = 93 # maksymalny zakres jednego zapytania do API NBP
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
main()
