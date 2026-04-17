import os
import requests
import pandas as pd
import numpy as np
import random
import re
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIG
# =========================
SCAN_SIZE = 2000
MAX_WORKERS = 10
STATE_FILE = "growthradar_v31_timeseries.json"
MIN_PRICE = 2.0

HEADERS = {"User-Agent": "Mozilla/5.0"}

# =========================
# STATE (完全時系列)
# =========================
class TimeSeriesStore:
    def __init__(self, path):
        self.path = path
        self.state = self.load()

    def load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r") as f:
                    return json.load(f)
            except:
                return {}
        return {}

    def update(self, df):
        today = datetime.now().strftime("%Y-%m-%d")

        for r in df.to_dict("records"):
            t = r["ticker"]

            if t not in self.state:
                self.state[t] = []

            self.state[t].append({
                "date": today,
                "price": float(r["price"]),
                "score": float(r["score"]),
                "m6": float(r["m6"]),
                "trend": float(r["trend"])
            })

            # 過去データ保持制限（30〜90日想定）
            self.state[t] = self.state[t][-60:]

        with open(self.path, "w") as f:
            json.dump(self.state, f)

    def get_series(self, ticker):
        return self.state.get(ticker, [])


# =========================
class GrowthRadarV31:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.state = TimeSeriesStore(STATE_FILE)

    # =========================
    def load_universe(self):
        url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt"
        r = self.session.get(url, timeout=10).text.split("\n")

        symbols = [
            s.strip().upper()
            for s in r
            if re.match(r"^[A-Z]{1,5}$", s)
        ]

        random.shuffle(symbols)
        return symbols[:SCAN_SIZE]

    # =========================
    def fetch(self, ticker):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(url, timeout=6).json()
            res = r["chart"]["result"][0]

            close = [c for c in res["indicators"]["quote"][0]["close"] if c]

            if len(close) < 120:
                return None

            price = close[-1]
            if price < MIN_PRICE:
                return None

            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1
            m6 = price / close[-120] - 1

            trend = np.mean(close[-10:]) / np.mean(close[-30:-10]) - 1
            accel = m1 - m3

            # ===== raw score（絶対値禁止）=====
            score = (
                m6 * 0.4 +
                accel * 0.3 +
                trend * 0.3
            )

            return {
                "ticker": ticker,
                "price": price,
                "m6": m6,
                "trend": trend,
                "accel": accel,
                "score": score
            }

        except:
            return None

    # =========================
    def compute_trajectory(self, ticker):
        series = self.state.get_series(ticker)

        if len(series) < 5:
            return None

        scores = [x["score"] for x in series]

        # slope（一次トレンド）
        x = np.arange(len(scores))
        slope = np.polyfit(x, scores, 1)[0]

        # acceleration（変化の変化）
        if len(scores) >= 10:
            mid = len(scores) // 2
            slope1 = np.polyfit(x[:mid], scores[:mid], 1)[0]
            slope2 = np.polyfit(x[mid:], scores[mid:], 1)[0]
            accel = slope2 - slope1
        else:
            accel = 0

        # persistence（上昇維持）
        up_days = sum(1 for i in range(1, len(scores)) if scores[i] > scores[i-1])

        return {
            "ticker": ticker,
            "slope": slope,
            "accel": accel,
            "persistence": up_days / len(scores)
        }

    # =========================
    def run(self):
        universe = self.load_universe()

        raw = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            for r in ex.map(self.fetch, universe):
                if r:
                    raw.append(r)

        if not raw:
            print("NO DATA")
            return

        df = pd.DataFrame(raw)

        # ===== 保存（ここが本体）=====
        self.state.update(df)

        # ===== trajectory評価 =====
        traj = []
        for t in df["ticker"]:
            tr = self.compute_trajectory(t)
            if tr:
                traj.append(tr)

        traj_df = pd.DataFrame(traj)

        # ===== Tier設計（絶対値禁止）=====
        tier1 = traj_df[(traj_df["slope"] > 0) & (traj_df["accel"] > 0.001)]
        tier2 = traj_df[(traj_df["slope"] > 0)]

        tier1 = tier1.sort_values("slope", ascending=False)
        tier2 = tier2.sort_values("slope", ascending=False)

        print(f"""
🚀 GrowthRadar v31 (Trajectory Memory Engine)

Universe: {len(universe)}
Tracked: {len(self.state.state)}
Active: {len(traj_df)}

🔥 Tier1 (Strong Uptrend)
""")

        for r in tier1.head(10).to_dict("records"):
            print(f"{r['ticker']} slope:{r['slope']:.4f} accel:{r['accel']:.4f}")

        print("\n👀 Tier2 (Positive Drift)\n")

        for r in tier2.head(10).to_dict("records"):
            print(f"{r['ticker']} slope:{r['slope']:.4f} persistence:{r['persistence']:.2f}")


if __name__ == "__main__":
    GrowthRadarV31().run()
