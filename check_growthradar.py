import os
import requests
import pandas as pd
import numpy as np
import random
import re
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

SCAN_SIZE = 2000
MAX_WORKERS = 10
MIN_PRICE = 2.0

HEADERS = {"User-Agent": "Mozilla/5.0"}

# =========================
# DEBUG METRICS
# =========================
debug_log = {
    "fetched": 0,
    "failed_api": 0,
    "failed_parse": 0,
    "filtered_price": 0,
    "filtered_data": 0,
    "passed": 0
}

# =========================
def log(reason, ticker):
    print(f"[{reason}] {ticker}")

# =========================
class GrowthRadarV29Debug:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # =========================
    def load_universe(self):
        url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt"
        r = self.session.get(url, timeout=10).text.split("\n")
        clean = list(set([x.strip().upper() for x in r if re.match(r"^[A-Z]{1,5}$", x)]))
        random.shuffle(clean)
        return clean[:SCAN_SIZE]

    # =========================
    def fetch(self, t):

        debug_log["fetched"] += 1

        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?range=1y&interval=1d"
            r = self.session.get(url, timeout=6).json()

            res_list = r.get("chart", {}).get("result")

            if not res_list:
                debug_log["failed_api"] += 1
                log("API_EMPTY", t)
                return None

            res = res_list[0]

            close = res["indicators"]["quote"][0].get("close", [])
            volume = res["indicators"]["quote"][0].get("volume", [])

            if not close or len(close) < 60:
                debug_log["failed_parse"] += 1
                log("TOO_SHORT", t)
                return None

            price = close[-1]

            if price is None:
                debug_log["failed_parse"] += 1
                log("NO_PRICE", t)
                return None

            if price < MIN_PRICE:
                debug_log["filtered_price"] += 1
                log("LOW_PRICE", t)
                return None

            # NaN除去
            close = [c for c in close if c is not None]
            volume = [v if v is not None else 0 for v in volume]

            if len(close) < 60:
                debug_log["failed_parse"] += 1
                log("AFTER_CLEAN_TOO_SHORT", t)
                return None

            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1 if len(close) > 63 else m1
            m6 = price / close[-120] - 1 if len(close) > 120 else m1

            trend = np.mean(close[-10:]) / (np.mean(close[-30:-10]) + 1e-9) - 1
            accel = m1 - m3

            # フィルタ
            if abs(accel) < 0.02:
                debug_log["filtered_data"] += 1
                log("NO_ACCEL", t)
                return None

            debug_log["passed"] += 1

            return {
                "ticker": t,
                "price": price,
                "m1": m1,
                "m3": m3,
                "m6": m6,
                "trend": trend,
                "accel": accel
            }

        except Exception as e:
            debug_log["failed_api"] += 1
            log(f"EXCEPTION {e}", t)
            return None

    # =========================
    def score(self, df):

        df["momentum"] = (
            df["m6"].rank(pct=True) * 0.4 +
            df["accel"].rank(pct=True) * 0.3 +
            df["trend"].rank(pct=True) * 0.3
        )

        return df.sort_values("momentum", ascending=False)

    # =========================
    def report_debug(self):

        print("\n📊 DEBUG SUMMARY")
        for k, v in debug_log.items():
            print(f"{k}: {v}")

        total = debug_log["fetched"]
        if total > 0:
            print("\nSUCCESS RATE:", round(debug_log["passed"] / total * 100, 2), "%")

    # =========================
    def run(self):

        universe = self.load_universe()

        print(f"\n🚀 v29-debug scanning {len(universe)} tickers...\n")

        raw = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch, t): t for t in universe}
            for f in as_completed(futures):
                r = f.result()
                if r:
                    raw.append(r)

        self.report_debug()

        if not raw:
            print("\n❌ NO VALID DATA (ALL FILTERED)")
            return

        df = self.score(pd.DataFrame(raw))

        tier1 = df[df["momentum"] > 0.85]
        tier2 = df[(df["momentum"] <= 0.85) & (df["momentum"] > 0.7)]

        now = datetime.now().strftime("%Y-%m-%d %H:%M")

        msg = [
            "🚀 GrowthRadar v29-debug",
            f"Scanned:{len(universe)} Valid:{len(df)} Tier1:{len(tier1)} Tier2:{len(tier2)} {now}",
            "\n🔥 TOP Tier1"
        ]

        for r in tier1.head(10).to_dict("records"):
            msg.append(f"{r['ticker']} S:{r['momentum']:.2f}")

        msg.append("\n👀 Tier2")
        for r in tier2.head(10).to_dict("records"):
            msg.append(f"{r['ticker']} S:{r['momentum']:.2f}")

        text = "\n".join(msg)

        print("\n" + text)

        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": text})


if __name__ == "__main__":
    GrowthRadarV29Debug().run()
