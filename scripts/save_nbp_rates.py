#!/usr/bin/env python3
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
