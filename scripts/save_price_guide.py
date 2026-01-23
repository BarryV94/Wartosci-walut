#!/usr/bin/env python3
# scripts/save_price_guide.py

from datetime import datetime
from zoneinfo import ZoneInfo
import urllib.request
import json
import os
import sys
import tempfile

TZ = "Europe/Warsaw"
BASE_OUT_DIR = os.path.join("docs")

PRICE_GUIDES = {
    "price_guide_21": "price_guides_starwarsunlimited",
}

BASE_URL = "https://downloads.s3.cardmarket.com/productCatalog/priceGuide/{}.json"


def file_for_date_path(out_dir, guide_name, date_obj):
    date_str = date_obj.strftime("%d_%m_%Y")
    return os.path.join(
        BASE_OUT_DIR,
        out_dir,
        f"{guide_name}_{date_str}.json"
    )


def download_to_path(url, path):
    print(f"Pobieram: {url}")
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            raw = resp.read()
    except Exception as e:
        print("❌ Błąd pobierania:", e)
        return False

    try:
        parsed = json.loads(raw)
    except Exception as e:
        print("❌ Niepoprawny JSON:", e)
        return False

    os.makedirs(os.path.dirname(path), exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(
        suffix=".json",
        dir=os.path.dirname(path)
    )
    try:
        with os.fdopen(fd, "wb") as tmpf:
            tmpf.write(
                json.dumps(
                    parsed,
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


def main():
    now_local = datetime.now(ZoneInfo(TZ))
    success = True

    for guide_name, folder in PRICE_GUIDES.items():
        url = BASE_URL.format(guide_name)
        target_path = file_for_date_path(folder, guide_name, now_local)

        if os.path.exists(target_path):
            print(f"✔ Istnieje: {target_path}")
            continue

        print(f"⬇ Brak pliku dla dziś: {guide_name}")
        if not download_to_path(url, target_path):
            success = False

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
