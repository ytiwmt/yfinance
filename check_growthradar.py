import os
import requests
import pandas as pd
import numpy as np
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# =========================
# CONFIG (v26.4+)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

MAX_WORKERS = 10
SCAN_SIZE = 1500

MIN_PRICE = 2.0
MIN_MCAP = 5e7
MIN_AVG_VOL_VAL = 5e5  # $500k/day

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

class GrowthRadarV26_4_Plus:
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
            str(s).strip().upper()
            for s in symbols
            if isinstance(s, str) and re.match(r"^[A-Z]{1,5}$", str(s).strip())
        ]))

        random.shuffle(clean)
        return clean

    # =========================
    # FETCH
    # =========================
    def fetch(self, ticker):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(url, timeout=6).json()
            res = r["chart"]["result"][0]

            # ゾンビ排除
            if (time.time() - res["meta"].get("regularMarketTime", 0)) > 86400 * 5:
                return None

            close = [c for c in res["indicators"]["quote"][0]["close"] if c]
            volume = [v for v in res["indicators"]["quote"][0]["volume"] if v]

            if len(close) < 126:
                return None

            price = close[-1]
            if price < MIN_PRICE:
                return None

            # 流動性
            avg_vol_val = np.mean(close[-21:]) * np.mean(volume[-21:])
            if avg_vol_val < MIN_AVG_VOL_VAL:
                return None

            # リターン
            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1
            m6 = price / close[-126] - 1

            if m6 < 0.3 or m1 > 1.5:
                return None

            volat = np.std(close[-21:]) / np.mean(close[-21:])
            if volat > 0.25:
                return None

            return {
                "ticker": ticker,
                "price": price,
                "m6": m6,
                "accel": m1 - m3,
                "trend": np.mean(close[-10:]) / np.mean(close[-30:-10]) - 1,
                "vol_short": np.mean(volume[-5:]),
                "vol_mid": np.mean(volume[-21:]),
                "vol_long": np.mean(volume[-63:])
            }

        except:
            return None

    # =========================
    # META
    # =========================
    def fetch_meta(self, tickers):
        meta = {}
        try:
            for i in range(0, len(tickers), 100):
                chunk = tickers[i:i+100]
                url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={','.join(chunk)}"
                r = self.session.get(url, timeout=10).json()

                for res in r.get("quoteResponse", {}).get("result", []):
                    meta[res["symbol"]] = {
                        "name": res.get("longName", res.get("shortName", res["symbol"])),
                        "mcap": res.get("marketCap", 0)
                    }
        except:
            pass
        return meta

    # =========================
    # RUN
    # =========================
    def run(self):
        universe = self.load_universe()
        batch = universe[:SCAN_SIZE]

        print(f"Scanning {len(batch)} symbols...")

        raw = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch, t): t for t in batch}
            for f in as_completed(futures):
                r = f.result()
                if r:
                    raw.append(r)

        if not raw:
            print("No candidates.")
            return

        meta = self.fetch_meta([r["ticker"] for r in raw])

        data = []
        for r in raw:
            m = meta.get(r["ticker"], {"name": r["ticker"], "mcap": 0})
            if 0 < m["mcap"] < MIN_MCAP:
                continue
            r.update(m)
            data.append(r)

        df = pd.DataFrame(data)
        if df.empty:
            print("No valid after meta.")
            return

        # =========================
        # Tier1（厳格）
        # =========================
        df_strict = df[
            (df["accel"] >= 0.20) &
            (df["trend"] >= 0.20) &
            (df["vol_mid"] >= df["vol_long"] * 1.3) &
            (df["vol_short"] >= df["vol_mid"] * 0.9)
        ].copy()

        # =========================
        # Tier2（最適化版）
        # =========================
        df_loose = df[
            (df["accel"] >= 0.18) &          # ← 微強化
            (df["trend"] >= 0.15) &          # ← 本質修正
            (df["vol_mid"] >= df["vol_long"] * 1.1)
        ].copy()

        def score(d):
            if d.empty:
                return d
            d["vol_ratio"] = d["vol_short"] / (d["vol_mid"] + 1e-9)

            d["score"] = (
                d["m6"].rank(pct=True) * 0.40 +
                d["accel"].rank(pct=True) * 0.20 +
                d["trend"].rank(pct=True) * 0.25 +
                d["vol_ratio"].rank(pct=True) * 0.15
            )
            return d.sort_values("score", ascending=False)

        t1 = score(df_strict)
        t2 = score(df_loose)

        self.report(t1, t2, len(batch), len(df))

    # =========================
    # REPORT
    # =========================
    def report(self, t1, t2, scanned, base):
        now = datetime.now().strftime("%Y-%m-%d %H:%M")

        msg = [
            f"🚀 GrowthRadar v26.4+",
            f"Scanned:{scanned} | Base:{base} | Tier1:{len(t1)} | Tier2:{len(t2)} | {now}\n"
        ]

        msg.append("🏆 Tier1 (High Conviction)\n")
        for r in t1.head(10).to_dict("records"):
            msg.append(
                f"{r['ticker']} | S:{r['score']:.2f} | "
                f"P:${r['price']:.2f} | M6:{r['m6']:+.0%} | T:{r['trend']:.2f}"
            )

        msg.append("\n👀 Tier2 (Watchlist)\n")
        for r in t2.head(10).to_dict("records"):
            msg.append(
                f"{r['ticker']} | S:{r['score']:.2f} | "
                f"P:${r['price']:.2f} | M6:{r['m6']:+.0%} | T:{r['trend']:.2f}"
            )

        text = "\n".join(msg)

        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": text})

        print(text)


if __name__ == "__main__":
    GrowthRadarV26_4_Plus().run()
