import os
import requests
import pandas as pd
import numpy as np
import random
import time
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# =========================
# CONFIG (v25 Tenbagger Core)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
MAX_WORKERS = 10
BATCH_SIZE = 1500  # 1回で広く拾う

# テンバガー条件（厳格）
MIN_PRICE = 2.0
MIN_MCAP = 1e8       # 100M
MAX_MCAP = 5e10      # 50B（ここ重要）

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

# =========================
# ENGINE
# =========================
class GrowthRadarV25:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # =========================
    # UNIVERSE（広く＋現実株のみ）
    # =========================
    def load_universe(self):
        symbols = []

        sources = [
            "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt",
            "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
        ]

        for url in sources:
            try:
                res = self.session.get(url, timeout=10)
                if res.status_code == 200:
                    if url.endswith(".txt"):
                        found = res.text.split("\n")
                    else:
                        df = pd.read_csv(url)
                        found = df["Symbol"].tolist()

                    symbols.extend(found)
            except:
                pass

        # clean
        symbols = list(set([
            s.strip().upper() for s in symbols
            if re.match(r"^[A-Z]{1,5}$", str(s))
        ]))

        random.shuffle(symbols)
        return symbols[:BATCH_SIZE]

    # =========================
    # BASIC INFO（ここが超重要）
    # =========================
    def fetch_details(self, ticker):
        try:
            url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={ticker}"
            r = self.session.get(url, timeout=5).json()
            data = r["quoteResponse"]["result"][0]

            return {
                "mcap": data.get("marketCap", 0),
                "type": data.get("quoteType", ""),
                "name": data.get("longName", "")
            }
        except:
            return None

    def is_noise(self, name):
        noise_keywords = ["WARRANT", "UNIT", "RIGHT", "ACQUISITION"]
        return any(k in name.upper() for k in noise_keywords)

    # =========================
    # CORE ANALYSIS
    # =========================
    def analyze(self, ticker):
        try:
            # --- details first (重要：ここで9割カット) ---
            d = self.fetch_details(ticker)
            if not d:
                return None

            # フィルタ①：実在株のみ
            if d["type"] != "EQUITY":
                return None

            # フィルタ②：ノイズ除去
            if self.is_noise(d["name"]):
                return None

            mcap = d["mcap"]
            if not mcap or not (MIN_MCAP <= mcap <= MAX_MCAP):
                return None

            # --- price data ---
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(url, timeout=5)
            j = r.json()["chart"]["result"][0]

            close = [c for c in j["indicators"]["quote"][0]["close"] if c is not None]
            vol   = [v for v in j["indicators"]["quote"][0]["volume"] if v is not None]

            if len(close) < 120:
                return None

            price = close[-1]
            if price < MIN_PRICE:
                return None

            # --- metrics ---
            m1  = price / close[-21] - 1
            m3  = price / close[-63] - 1
            m6  = price / close[-120] - 1
            m12 = price / close[0] - 1

            accel = m1 - m3
            vol_ratio = (sum(vol[-5:]) / 5) / (sum(vol[-30:]) / 30 + 1e-9)

            # =========================
            # TENBAGGER FILTER CORE
            # =========================

            # ① すでに終わった銘柄除外
            if m12 > 3.0:
                return None

            # ② 中期トレンド必須
            if not (0.2 < m3 < 1.5):
                return None

            # ③ 初動条件（過熱排除）
            if m1 > 0.5:
                return None

            # ④ 加速
            if accel < 0.1:
                return None

            # ⑤ 出来高確認
            if vol_ratio < 1.2:
                return None

            # =========================
            # SCORE
            # =========================
            score = (
                (m3 * 10) +
                (accel * 20) +
                (vol_ratio * 5)
            )

            return {
                "ticker": ticker,
                "price": price,
                "mcap": mcap,
                "m1": m1,
                "m3": m3,
                "accel": accel,
                "vol": vol_ratio,
                "score": score
            }

        except:
            return None

    # =========================
    # RUN
    # =========================
    def run(self):
        universe = self.load_universe()
        print(f"Scanning {len(universe)} symbols...")

        results = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.analyze, t): t for t in universe}

            for f in as_completed(futures):
                r = f.result()
                if r:
                    results.append(r)

        if not results:
            self.output("No real tenbagger candidates today.")
            return

        df = pd.DataFrame(results)
        df = df.sort_values("score", ascending=False).head(15)

        self.report(df, len(universe), len(results))

    # =========================
    # OUTPUT
    # =========================
    def report(self, df, total, hits):
        now = datetime.now().strftime("%Y-%m-%d %H:%M")

        msg = [
            f"🚀 **GrowthRadar v25 (True Tenbagger Core)**",
            f"Universe: {total} | Candidates: {hits} | {now}\n"
        ]

        for r in df.to_dict("records"):
            msg.append(
                f"**{r['ticker']}** | Score:{r['score']:.2f}\n"
                f"Price:${r['price']:.2f} | MC:{r['mcap']/1e9:.2f}B\n"
                f"M3:{r['m3']:+.1%} | M1:{r['m1']:+.1%}\n"
                f"Accel:{r['accel']:.2f} | Vol:{r['vol']:.2f}x\n"
            )

        self.output("\n".join(msg))

    def output(self, text):
        if WEBHOOK_URL:
            try:
                requests.post(WEBHOOK_URL, json={"content": text}, timeout=10)
            except:
                pass
        print(text)


# =========================
# ENTRY
# =========================
if __name__ == "__main__":
    GrowthRadarV25().run()
