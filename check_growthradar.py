import os
import requests
import pandas as pd
import re
import random
import json
from datetime import datetime

# =========================
# CONFIG
# =========================
SCAN_SIZE = 2000
STATE_UNIVERSE_FILE = "universe_cache.json"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# =========================
class UniverseManager:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # -------------------------
    def load_cache(self):
        if os.path.exists(STATE_UNIVERSE_FILE):
            try:
                with open(STATE_UNIVERSE_FILE, "r") as f:
                    data = json.load(f)
                    print(f"[Universe] cache loaded: {len(data)}")
                    return data
            except:
                return []
        return []

    # -------------------------
    def save_cache(self, universe):
        try:
            with open(STATE_UNIVERSE_FILE, "w") as f:
                json.dump(universe, f)
        except:
            pass

    # -------------------------
    def fetch_from_github_txt(self):
        url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt"
        r = self.session.get(url, timeout=10)

        if r.status_code != 200:
            print(f"[GitHub TXT FAIL] {r.status_code}")
            return []

        symbols = r.text.split("\n")
        return symbols

    # -------------------------
    def fetch_from_nasdaq_csv(self):
        url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
        r = self.session.get(url, timeout=10)

        if r.status_code != 200:
            print(f"[NASDAQ CSV FAIL] {r.status_code}")
            return []

        try:
            df = pd.read_csv(url)
            return df["Symbol"].tolist()
        except:
            return []

    # -------------------------
    def fetch_from_stooq_backup(self):
        # 超重要 fallback（軽量・安定）
        url = "https://stooq.com/q/l/?s=aapl.us&f=sd2t2ohlcv&h&e=csv"
        try:
            r = self.session.get(url, timeout=10)
            if r.status_code == 200:
                # ここはダミーシグナルだが「死なない保証」用
                return ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN"]
        except:
            pass
        return []

    # -------------------------
    def normalize(self, symbols):
        clean = []
        for s in symbols:
            if isinstance(s, str):
                s = s.strip().upper()
                if re.match(r"^[A-Z]{1,5}$", s):
                    clean.append(s)
        return list(set(clean))

    # -------------------------
    def load_universe(self):
        print("\n🚀 Loading Universe (multi-source fallback)...")

        # 1. GitHub
        try:
            data = self.fetch_from_github_txt()
            uni = self.normalize(data)
            if len(uni) > 1000:
                print(f"[Universe OK] GitHub TXT: {len(uni)}")
                self.save_cache(uni)
                return uni[:SCAN_SIZE]
        except Exception as e:
            print(f"[GitHub TXT ERROR] {e}")

        # 2. NASDAQ CSV
        try:
            data = self.fetch_from_nasdaq_csv()
            uni = self.normalize(data)
            if len(uni) > 1000:
                print(f"[Universe OK] NASDAQ CSV: {len(uni)}")
                self.save_cache(uni)
                return uni[:SCAN_SIZE]
        except Exception as e:
            print(f"[NASDAQ ERROR] {e}")

        # 3. CACHE
        cache = self.load_cache()
        if cache:
            print(f"[Universe OK] CACHE fallback: {len(cache)}")
            return cache[:SCAN_SIZE]

        # 4. LAST RESORT
        print("[Universe WARNING] fallback to mini universe")
        fallback = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "AMD", "META"]
        return fallback


# =========================
# MAIN ENGINE (stub integration)
# =========================
class GrowthRadarV31_1:
    def __init__(self):
        self.uni = UniverseManager()

    def run(self):
        universe = self.uni.load_universe()

        print(f"\n📊 FINAL UNIVERSE SIZE: {len(universe)}")

        if len(universe) == 0:
            print("❌ CRITICAL FAILURE: universe empty")
            return

        print("\n🔥 SAMPLE:")
        print(universe[:20])


if __name__ == "__main__":
    GrowthRadarV31_1().run()
