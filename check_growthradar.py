import os
import requests
import pandas as pd
import numpy as np
import random
import re
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
SCAN_SIZE = 2000
MAX_WORKERS = 10
MIN_PRICE = 2.0

HEADERS = {"User-Agent": "Mozilla/5.0"}

# =========================
class GrowthRadarV31Debug:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

        # DEBUG COUNTERS
        self.stats = {
            "universe": 0,
            "fetched_ok": 0,
            "fetch_fail": 0,
            "parse_fail": 0,
            "filtered_price": 0,
            "filtered_data": 0,
        }

    # =========================
    def load_universe(self):
        try:
            url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt"
            r = self.session.get(url, timeout=10)

            symbols = r.text.split("\n")
            clean = [
                s.strip().upper()
                for s in symbols
                if re.match(r"^[A-Z]{1,5}$", s)
            ]

            random.shuffle(clean)
            self.stats["universe"] = len(clean)

            print(f"[Universe] {len(clean)} loaded")

            return clean[:SCAN_SIZE]

        except Exception as e:
            print(f"[Universe ERROR] {e}")
            return []

    # =========================
    def fetch(self, ticker):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(url, timeout=6).json()

            if "chart" not in r or not r["chart"]["result"]:
                self.stats["parse_fail"] += 1
                return None

            res = r["chart"]["result"][0]

            close = res["indicators"]["quote"][0]["close"]

            if not close:
                self.stats["parse_fail"] += 1
                return None

            close = [c for c in close if c is not None]

            if len(close) < 120:
                self.stats["filtered_data"] += 1
                return None

            price = close[-1]

            if price < MIN_PRICE:
                self.stats["filtered_price"] += 1
                return None

            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1
            m6 = price / close[-120] - 1

            trend = np.mean(close[-10:]) / np.mean(close[-30:-10]) - 1
            accel = m1 - m3

            self.stats["fetched_ok"] += 1

            return {
                "ticker": ticker,
                "price": price,
                "m6": m6,
                "trend": trend,
                "accel": accel
            }

        except Exception as e:
            self.stats["fetch_fail"] += 1
            return None

    # =========================
    def run(self):
        universe = self.load_universe()

        print(f"\n🚀 v31-debug scanning {len(universe)} tickers...\n")

        raw = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            for r in ex.map(self.fetch, universe):
                if r:
                    raw.append(r)

        print("\n📊 DEBUG SUMMARY")
        print(f"universe: {self.stats['universe']}")
        print(f"fetched_ok: {self.stats['fetched_ok']}")
        print(f"fetch_fail: {self.stats['fetch_fail']}")
        print(f"parse_fail: {self.stats['parse_fail']}")
        print(f"filtered_price: {self.stats['filtered_price']}")
        print(f"filtered_data: {self.stats['filtered_data']}")
        print(f"passed: {len(raw)}")

        if not raw:
            print("\n❌ NO VALID DATA (ALL FILTERED OR FAILED)")
            return

        df = pd.DataFrame(raw)

        print("\n🔥 SAMPLE DATA")
        print(df.head(10))


if __name__ == "__main__":
    GrowthRadarV31Debug().run()
