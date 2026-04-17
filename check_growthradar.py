import os
import requests
import pandas as pd
import numpy as np
import random
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# =========================
# CONFIG (v25.1 Noise Filter)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
MAX_WORKERS = 8
SCAN_SIZE = 1500

MIN_PRICE = 1.0
MIN_MCAP = 5e7
MAX_MCAP = 1.5e12

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

class GrowthRadarV25_1:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # =========================
    # UNIVERSE
    # =========================
    def load_universe(self):
        symbols = []

        sources = [
            "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt",
            "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv",
        ]

        print("Fetching universe...")
        for url in sources:
            try:
                r = self.session.get(url, timeout=10)
                if r.status_code == 200:
                    if url.endswith(".txt"):
                        found = r.text.split("\n")
                    else:
                        df = pd.read_csv(url)
                        found = df["Symbol"].tolist()
                    symbols.extend(found)
            except:
                pass

        clean = list(set([
            s.strip().upper()
            for s in symbols
            if isinstance(s, str) and re.match(r"^[A-Z]{1,5}$", s.strip())
        ]))

        random.shuffle(clean)
        print(f"Universe size: {len(clean)}")
        return clean

    # =========================
    # FETCH BASIC
    # =========================
    def fetch(self, ticker):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(url, timeout=6)
            data = r.json()["chart"]["result"][0]

            close = [c for c in data["indicators"]["quote"][0]["close"] if c]
            volume = [v for v in data["indicators"]["quote"][0]["volume"] if v]

            if len(close) < 60:
                return None

            price = close[-1]
            if price < MIN_PRICE:
                return None

            # ===== 指標 =====
            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1 if len(close) > 63 else m1

            accel = m1 - m3

            vol_ratio = (sum(volume[-5:]) / 5) / (sum(volume[-21:]) / 21 + 1e-9)

            # =========================
            # ノイズ除去（ここが本体）
            # =========================

            # ① 異常爆発（ほぼゴミ）
            if m1 > 3.0:
                return None

            # ② 出来高死んでる
            if vol_ratio < 0.8:
                return None

            # ③ 加速してない
            if accel < 0.05:
                return None

            return {
                "ticker": ticker,
                "price": price,
                "m1": m1,
                "accel": accel,
                "vol": vol_ratio
            }

        except:
            return None

    # =========================
    # RUN
    # =========================
    def run(self):
        universe = self.load_universe()
        batch = universe[:SCAN_SIZE]

        print(f"Scanning {len(batch)} symbols...")

        results = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch, t): t for t in batch}

            for f in as_completed(futures):
                r = f.result()
                if r:
                    results.append(r)

        print(f"Valid results: {len(results)}")

        if not results:
            print("No candidates.")
            return

        df = pd.DataFrame(results)

        # ===== Zスコア =====
        for col in ["accel", "m1", "vol"]:
            df[col] = df[col].astype(float)
            df[f"z_{col}"] = (df[col] - df[col].mean()) / (df[col].std() + 1e-9)

        df["score"] = (
            df["z_accel"] * 0.5 +
            df["z_m1"] * 0.3 +
            df["z_vol"] * 0.2
        )

        top = df.sort_values("score", ascending=False).head(15)

        self.report(top, len(batch), len(results))

    # =========================
    # REPORT
    # =========================
    def report(self, df, scanned, valid):
        now = datetime.now().strftime("%Y-%m-%d %H:%M")

        msg = [
            f"🚀 GrowthRadar v25.1 (Noise Filter)",
            f"Scanned: {scanned} | Valid: {valid} | {now}\n"
        ]

        for r in df.to_dict("records"):
            msg.append(
                f"{r['ticker']} | Score:{r['score']:.2f}\n"
                f"Price:{r['price']:.2f} | "
                f"M1:{r['m1']:+.1%} | "
                f"Accel:{r['accel']:.2f} | "
                f"Vol:{r['vol']:.1f}x\n"
            )

        text = "\n".join(msg)

        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": text})

        print(text)


if __name__ == "__main__":
    GrowthRadarV25_1().run()
