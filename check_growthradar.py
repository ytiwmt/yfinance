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
# CONFIG (v26.3)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

MAX_WORKERS = 10
SCAN_SIZE = 1500

MIN_PRICE = 2.0
MIN_MCAP = 5e7  # $50M

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

class GrowthRadarV26_3:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

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

    def fetch_technical(self, ticker):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(url, timeout=6).json()
            data = r["chart"]["result"][0]

            # ゾンビ排除
            last_trade_ts = data["meta"].get("regularMarketTime", 0)
            if (time.time() - last_trade_ts) > 86400 * 5:
                return None

            close = [c for c in data["indicators"]["quote"][0]["close"] if c]
            volume = [v for v in data["indicators"]["quote"][0]["volume"] if v]

            if len(close) < 126:
                return None

            price = close[-1]
            if price < MIN_PRICE:
                return None

            # ===== リターン =====
            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1
            m6 = price / close[-126] - 1

            # ===== コア =====
            if m6 < 0.3 or m1 > 1.5:
                return None

            accel = m1 - m3
            if accel < 0.2:   # ← 強化
                return None

            # ===== 安定性 =====
            volat = np.std(close[-21:]) / np.mean(close[-21:])
            if volat > 0.22:  # ← 少し厳格化
                return None

            # ===== 出来高（厳格化） =====
            vol_short = np.mean(volume[-5:])
            vol_mid = np.mean(volume[-21:])
            vol_long = np.mean(volume[-63:])

            # 明確な資金流入
            if vol_mid < vol_long * 1.3:
                return None

            # 短期も維持
            if vol_short < vol_mid * 0.9:
                return None

            vol_ratio = vol_short / (vol_mid + 1e-9)

            # ===== トレンド質（強化） =====
            trend_smooth = np.mean(close[-10:]) / np.mean(close[-30:-10]) - 1

            if trend_smooth < 0.2:
                return None

            return {
                "ticker": ticker,
                "price": price,
                "m6": m6,
                "accel": accel,
                "trend": trend_smooth,
                "vol": vol_ratio
            }

        except:
            return None

    def fetch_bulk_meta(self, tickers):
        meta = {}
        if not tickers:
            return meta

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

    def run(self):
        universe = self.load_universe()
        batch = universe[:SCAN_SIZE]

        print(f"Scanning {len(batch)} symbols...")

        tech_results = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch_technical, t): t for t in batch}
            for f in as_completed(futures):
                r = f.result()
                if r:
                    tech_results.append(r)

        print(f"Tech valid: {len(tech_results)}")

        valid_tickers = [r["ticker"] for r in tech_results]
        meta_data = self.fetch_bulk_meta(valid_tickers)

        final_list = []
        for r in tech_results:
            m = meta_data.get(r["ticker"], {"name": r["ticker"], "mcap": 0})

            if m["mcap"] > 0 and m["mcap"] < MIN_MCAP:
                continue

            r.update(m)
            final_list.append(r)

        if not final_list:
            print("No candidates.")
            return

        df = pd.DataFrame(final_list)

        # ===== rankスコア =====
        for col in ["m6", "accel", "trend", "vol"]:
            df[col] = df[col].astype(float)

        df["score"] = (
            df["m6"].rank(pct=True) * 0.4 +
            df["accel"].rank(pct=True) * 0.2 +
            df["trend"].rank(pct=True) * 0.25 +
            df["vol"].rank(pct=True) * 0.15
        )

        top = df.sort_values("score", ascending=False).head(10)

        self.report(top, len(batch), len(df))

    def report(self, df, scanned, valid):
        now = datetime.now().strftime("%Y-%m-%d %H:%M")

        msg = [
            f"🚀 GrowthRadar v26.3 (High Precision)",
            f"Scanned: {scanned} | Valid: {valid} | {now}\n"
        ]

        for r in df.to_dict("records"):
            mcap_str = f"${r['mcap']/1e9:.2f}B" if r['mcap'] > 0 else "N/A"

            msg.append(
                f"{r['ticker']} | Score:{r['score']:.2f}\n"
                f"Price:${r['price']:.2f} | MC:{mcap_str}\n"
                f"M6:{r['m6']:+.1%} | Accel:{r['accel']:.2f} | "
                f"Trend:{r['trend']:.2f} | Vol:{r['vol']:.1f}x\n"
            )

        text = "\n".join(msg)

        if WEBHOOK_URL:
            requests.post(WEBHOOK_URL, json={"content": text})

        print(text)


if __name__ == "__main__":
    GrowthRadarV26_3().run()
