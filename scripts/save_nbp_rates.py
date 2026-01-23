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
BASE_OUT_DIR = os.path.join("docs", "exc")
MAX_FILES_PER_DIR = 999
START_DATE = date(2021, 1, 1) # data początkowa do backfill
CHUNK_DAYS = 93 # maksymalny zakres jednego zapytania do API NBP
BASE_TABLE_URL = "https://api.nbp.pl/api/exchangerates/tables/A/{start}/{end}/?format=json"
SINGLE_DAY_URL = "https://api.nbp.pl/api/exchangerates/tables/A/{date}/?format=json"


HEADERS = {"User-Agent": "save-nbp-rates-script/1.0 (+https://github.com/)"}




def ensure_out_dir():
os.makedirs(BASE_OUT_DIR, exist_ok=True)




def _existing_subdirs():
subs = []
if not os.path.exists(BASE_OUT_DIR):
return subs
for name in os.listdir(BASE_OUT_DIR):
p = os.path.join(BASE_OUT_DIR, name)
if os.path.isdir(p) and name.isdigit():
subs.append(int(name))
return sorted(subs)




def _pick_target_dir():
subs = _existing_subdirs()
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
    base = _pick_target_dir()
    filename = d.strftime("%d_%m_%Y.json")
    return os.path.join(base, filename)




def write_json_atomic(path, data):
os.makedirs(os.path.dirname(path), exist_ok=True)
fd, tmp_path = tempfile.mkstemp(suffix=".json", dir=os.path.dirname(path))
main()
