import os
import requests
import pandas as pd
import numpy as np
import random
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

UNIVERSE_FILE = "universe.json"
HISTORY_FILE = "history.json"

SCAN_SPLIT = 3
MAX_WORKERS = 8

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

# =========================
# ENGINE
# =========================
class GrowthRadarV19:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # =========================
    # UNIVERSE
    # =========================
    def build_universe(self):
        base = ["PLTR","NVDA","RKLB","ASTS","OKLO","HIMS","CELH","UPST","COIN","HOOD","SMCI","RDDT","LUNR","IONQ","APP","SOUN","DUOL","MSTR","TSLA","MARA"]

        try:
            url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
            df = pd.read_csv(url)
            df = df.dropna(subset=["Symbol"])
            df = df[~df["Symbol"].str.contains(r"[\$\.\-\=]", na=False)]

            symbols = df["Symbol"].tolist()
            random.shuffle(symbols)

            universe = list(dict.fromkeys(base + symbols))
            universe = universe[:3000]

            with open(UNIVERSE_FILE, "w") as f:
                json.dump(universe, f)

            return universe

        except:
            return base

    def load_universe(self):
        if os.path.exists(UNIVERSE_FILE):
            with open(UNIVERSE_FILE) as f:
                return json.load(f)
        return self.build_universe()

    # =========================
    # ROTATION
    # =========================
    def get_today_batch(self, universe):
        day_index = datetime.utcnow().day % SCAN_SPLIT
        size = len(universe) // SCAN_SPLIT

        start = day_index * size
        end = start + size

        return universe[start:end]

    # =========================
    # DATA
    # =========================
    def fetch(self, ticker):
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"

        try:
            r = self.session.get(url, timeout=10)
            j = r.json()["chart"]["result"][0]

            close = [c for c in j["indicators"]["quote"][0]["close"] if c]
            volume = [v for v in j["indicators"]["quote"][0]["volume"] if v]

            if len(close) < 120:
                return None

            price = close[-1]
            if price < 1:
                return None

            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1
            m12 = price / close[0] - 1

            accel = m1 - m3
            vol = (sum(volume[-5:]) / 5) / (sum(volume[-30:]) / 30 + 1e-9)

            return {
                "ticker": ticker,
                "price": price,
                "m1": m1,
                "m12": m12,
                "accel": accel,
                "vol": vol
            }

        except:
            return None

    # =========================
    # HISTORY
    # =========================
    def load_history(self):
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE) as f:
                return json.load(f)
        return {}

    def save_history(self, data):
        with open(HISTORY_FILE, "w") as f:
            json.dump(data, f)

    # =========================
    # RUN
    # =========================
    def run(self):
        universe = self.load_universe()
        batch = self.get_today_batch(universe)

        results = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch, t): t for t in batch}

            for f in as_completed(futures):
                res = f.result()
                if res:
                    results.append(res)

        df = pd.DataFrame(results)
        if df.empty:
            print("No data")
            return

        # =========================
        # Z-SCORE
        # =========================
        for col in ["accel", "m1", "vol"]:
            df[f"z_{col}"] = (df[col] - df[col].mean()) / (df[col].std() + 1e-9)

        df["score"] = (
            0.5 * df["z_accel"] +
            0.3 * df["z_vol"] +
            0.2 * df["z_m1"]
        )

        # =========================
        # HISTORY COMPARISON
        # =========================
        history = self.load_history()
        new_history = {}

        anomalies = []

        for _, r in df.iterrows():
            t = r["ticker"]
            score = r["score"]

            prev = history.get(t, 0)

            # 新規異常のみ
            if score > 1.5 and (score - prev) > 0.8:
                anomalies.append(r)

            new_history[t] = score

        self.save_history(new_history)

        # =========================
        # OUTPUT
        # =========================
        anomalies = sorted(anomalies, key=lambda x: x["score"], reverse=True)[:15]

        msg = [
            f"🚀 GrowthRadar v19",
            f"Batch: {len(batch)} | Hits: {len(anomalies)}\n"
        ]

        for r in anomalies:
            msg.append(
                f"{r['ticker']} | Score:{r['score']:.2f}\n"
                f"Price:{r['price']:.2f} | Accel:{r['accel']:.2f} | M1:{r['m1']:.1%}"
            )

        text = "\n".join(msg)

        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": text})
        else:
            print(text)


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    GrowthRadarV19().run()
