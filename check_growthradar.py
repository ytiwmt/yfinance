import os
import requests
import pandas as pd
import numpy as np
import random
import re
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
class UniverseProvider:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # -------------------------
    def github_universe(self):
        try:
            url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt"
            r = self.session.get(url, timeout=10)

            if r.status_code != 200:
                return []

            lines = r.text.splitlines()

            clean = []
            for x in lines:
                x = x.strip().upper()

                # BRK.B / BF.B対応
                if re.match(r"^[A-Z0-9\.\-]{1,6}$", x):
                    clean.append(x)

            return clean

        except:
            return []

    # -------------------------
    def nasdaq_universe(self):
        try:
            url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
            df = pd.read_csv(url)
            return df["Symbol"].dropna().astype(str).str.upper().tolist()
        except:
            return []

    # -------------------------
    def fallback_universe(self):
        return [
            "AAPL","MSFT","NVDA","AMZN","META",
            "TSLA","GOOGL","AMD","PLTR","INTC",
            "NFLX","AVGO","QCOM","ADBE","COST"
        ]

    # -------------------------
    def get(self):
        universe = []

        # ① GitHub
        u1 = self.github_universe()
        print(f"[Universe] GitHub: {len(u1)}")
        universe.extend(u1)

        # ② NASDAQ
        u2 = self.nasdaq_universe()
        print(f"[Universe] NASDAQ: {len(u2)}")
        universe.extend(u2)

        # ③ fallback
        if len(universe) < 500:
            u3 = self.fallback_universe()
            print(f"[Universe] Fallback used: {len(u3)}")
            universe.extend(u3)

        # 重複排除
        universe = list(set(universe))

        random.shuffle(universe)

        return universe[:SCAN_SIZE]

# =========================
class GrowthRadarV29_1:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.uni = UniverseProvider()

    # -------------------------
    def fetch(self, t):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{t}?range=1y&interval=1d"
            r = self.session.get(url, timeout=6).json()

            res_list = r.get("chart", {}).get("result")
            if not res_list:
                return None

            res = res_list[0]

            close = res["indicators"]["quote"][0].get("close", [])
            volume = res["indicators"]["quote"][0].get("volume", [])

            close = [c for c in close if c is not None]
            volume = [v if v else 0 for v in volume]

            if len(close) < 60:
                return None

            price = close[-1]
            if price < MIN_PRICE:
                return None

            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1 if len(close) > 63 else m1
            m6 = price / close[-120] - 1 if len(close) > 120 else m1

            trend = np.mean(close[-10:]) / (np.mean(close[-30:-10]) + 1e-9) - 1
            accel = m1 - m3

            return {
                "ticker": t,
                "price": price,
                "m1": m1,
                "m3": m3,
                "m6": m6,
                "trend": trend,
                "accel": accel
            }

        except:
            return None

    # -------------------------
    def score(self, df):
        df["momentum"] = (
            df["m6"].rank(pct=True) * 0.4 +
            df["accel"].rank(pct=True) * 0.3 +
            df["trend"].rank(pct=True) * 0.3
        )
        return df.sort_values("momentum", ascending=False)

    # -------------------------
    def run(self):

        universe = self.uni.get()

        print(f"\n🚀 Universe size: {len(universe)}\n")

        if len(universe) == 0:
            print("CRITICAL: EMPTY UNIVERSE")
            return

        raw = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch, t): t for t in universe}
            for f in as_completed(futures):
                r = f.result()
                if r:
                    raw.append(r)

        print(f"\nValid fetched: {len(raw)}")

        if len(raw) == 0:
            print("NO VALID DATA (fetch issue or API block)")
            return

        df = self.score(pd.DataFrame(raw))

        tier1 = df[df["momentum"] > 0.85]
        tier2 = df[(df["momentum"] <= 0.85) & (df["momentum"] > 0.7)]

        now = datetime.now().strftime("%Y-%m-%d %H:%M")

        print(f"""
🚀 GrowthRadar v29.1
Universe:{len(universe)} Valid:{len(df)}
Tier1:{len(tier1)} Tier2:{len(tier2)} {now}

🔥 TOP Tier1
""")

        for r in tier1.head(10).to_dict("records"):
            print(r["ticker"], r["momentum"])

        print("\n👀 Tier2")
        for r in tier2.head(10).to_dict("records"):
            print(r["ticker"], r["momentum"])


if __name__ == "__main__":
    GrowthRadarV29_1().run()
