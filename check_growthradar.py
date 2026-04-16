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
# CONFIG (v21.1 Notification Guard)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
UNIVERSE_FILE = "universe.json"
SCAN_SPLIT = 2
MAX_WORKERS = 10

MIN_PRICE = 1.0
MIN_MCAP = 5e7
MAX_MCAP = 1e11 # 大型株も許容

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json"
}

class GrowthRadarV21_1:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def load_universe(self):
        symbols = []
        if os.path.exists(UNIVERSE_FILE):
            try:
                with open(UNIVERSE_FILE) as f:
                    symbols = json.load(f)
            except: pass

        hot_list = [
            "IONQ","RKLB","ASTS","OKLO","LUNR","QUBT","RGTI","QBTS","EOSE","MAAS","BULL","UPST","TEM","MLYS","AUR","LUMN","HOOD","COIN","MARA","MSTR",
            "PLTR","SOUN","BBAI","NNE","SMR","GGE","HITI","CGC","PLUG","RUN","ENPH","TSLA","RIVN","LCID","AFRM","SOFI","SQ","PYPL","SHOP","SE"
        ]
        
        if not symbols or len(symbols) < 500:
            try:
                url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
                df = pd.read_csv(url)
                symbols = df["Symbol"].dropna().tolist()
                symbols = [s for s in symbols if not re.search(r"[\$\.\-\=]", s)]
            except:
                symbols = hot_list

        combined = list(dict.fromkeys(hot_list + symbols))
        random.shuffle(combined)
        
        with open(UNIVERSE_FILE, "w") as f:
            json.dump(combined, f)
        return combined

    def get_batch(self, universe):
        idx = datetime.utcnow().day % SCAN_SPLIT
        size = len(universe) // SCAN_SPLIT
        return universe[idx*size:(idx+1)*size]

    def fetch(self, ticker):
        try:
            p_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(p_url, timeout=7)
            j = r.json()["chart"]["result"][0]
            close = [c for c in j["indicators"]["quote"][0]["close"] if c]
            vol = [v for v in j["indicators"]["quote"][0]["volume"] if v]

            if len(close) < 60: return None
            price = close[-1]
            if price < MIN_PRICE: return None

            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1
            accel = m1 - m3
            vol_ratio = (sum(vol[-5:])/5) / (sum(vol[-30:])/30 + 1e-9)

            rev = 0.0
            mcap = 0
            try:
                f_url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=financialData,defaultKeyStatistics"
                fr = self.session.get(f_url, timeout=5).json()
                res = fr["quoteSummary"]["result"][0]
                rev = res.get("financialData", {}).get("revenueGrowth", {}).get("raw", 0)
                mcap = res.get("defaultKeyStatistics", {}).get("marketCap", {}).get("raw", 0)
            except:
                pass 

            if mcap > 0 and (mcap < MIN_MCAP or mcap > MAX_MCAP): return None

            return {
                "ticker": ticker, "price": price, "m1": m1, "accel": accel,
                "vol": vol_ratio, "rev": rev, "mcap": mcap
            }
        except:
            return None

    def run(self):
        universe = self.load_universe()
        batch = self.get_batch(universe)
        results = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch, t): t for t in batch}
            for f in as_completed(futures):
                r = f.result()
                if r: results.append(r)

        # ---------------------------------------------------------
        # 通知ガード: 結果が空でも状況を通知する
        # ---------------------------------------------------------
        if not results:
            self.send_webhook(f"⚠️ **GrowthRadar v21.1 Status**\nNo data could be fetched from {len(batch)} symbols. Yahoo API may be blocking requests or universe is invalid.")
            return

        df = pd.DataFrame(results)
        
        for col in ["accel", "m1", "vol"]:
            df[f"z_{col}"] = (df[col] - df[col].mean()) / (df[col].std() + 1e-9)

        df["score"] = (
            df["z_accel"] * 0.4 +
            df["z_m1"] * 0.3 +
            df["z_vol"] * 0.3 +
            (df["rev"].clip(0, 1) * 0.5)
        )

        top_df = df.sort_values("score", ascending=False).head(15)
        
        if top_df.empty:
            self.send_webhook(f"ℹ️ **GrowthRadar v21.1 Status**\nAnalyzed {len(df)} symbols, but none met the ranking criteria.")
        else:
            self.report(top_df, len(batch), len(df))

    def report(self, df, scanned, valid):
        msg = [
            f"🛰️ **GrowthRadar v21.1 (Aggressive)**",
            f"Universe: {scanned} | Analyzed: {valid}\n"
        ]

        for r in df.to_dict("records"):
            rev_str = f"{r['rev']:.1%}" if r['rev'] != 0 else "N/A"
            mcap_str = f"{r['mcap']/1e9:.2f}B" if r['mcap'] != 0 else "N/A"
            
            msg.append(
                f"**{r['ticker']}** | Score: {r['score']:.2f}\n"
                f"└ Price: ${r['price']:.2f} | MC: {mcap_str} | Rev: {rev_str}\n"
                f"└ M1: {r['m1']:+.1%} | Accel: {r['accel']:.2f} | Vol: {r['vol']:.1f}x"
            )

        self.send_webhook("\n".join(msg))

    def send_webhook(self, text):
        if WEBHOOK_URL:
            try:
                requests.post(WEBHOOK_URL, json={"content": text}, timeout=10)
            except Exception as e:
                print(f"Webhook failed: {e}")
        else:
            print(text)

if __name__ == "__main__":
    GrowthRadarV21_1().run()
