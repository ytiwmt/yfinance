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
# CONFIG (v23 Mass Scan Mode)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
UNIVERSE_FILE = "universe_v23.json"
# 1回でスキャンする割合（1なら全件、2なら半分）
SCAN_SPLIT = 1 
MAX_WORKERS = 15

# フィルタ基準
MIN_PRICE = 1.0
MIN_MCAP = 5e7  # 50Mドル

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
}

class GrowthRadarV23:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def load_universe(self):
        """数千件の銘柄リストを確実に構築する"""
        symbols = []
        if os.path.exists(UNIVERSE_FILE):
            try:
                with open(UNIVERSE_FILE) as f:
                    symbols = json.load(f)
            except: pass

        if not symbols or len(symbols) < 1000:
            print("Fetching fresh universe...")
            try:
                # 複数のソースからティッカーを取得
                url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt"
                res = self.session.get(url, timeout=10)
                symbols = [s.strip().upper() for s in res.text.split('\n') 
                           if s.strip().isalpha() and 1 <= len(s.strip()) <= 5]
            except:
                symbols = ["IONQ", "PLTR", "TSLA", "NVDA", "RKLB"] # Fallback

        # 重複削除とシャッフル
        symbols = sorted(list(set(symbols)))
        random.shuffle(symbols)
        
        with open(UNIVERSE_FILE, "w") as f:
            json.dump(symbols, f)
        return symbols

    def get_batch(self, universe):
        if SCAN_SPLIT <= 1: return universe
        idx = datetime.utcnow().minute % SCAN_SPLIT # 分単位で回す
        size = len(universe) // SCAN_SPLIT
        return universe[idx*size:(idx+1)*size]

    def fetch_mcap_safe(self, ticker):
        """時価総額を意地でも取得する"""
        # Method 1: v7 quote (High reliability)
        try:
            url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={ticker}"
            r = self.session.get(url, timeout=3).json()
            data = r["quoteResponse"]["result"][0]
            return data.get("marketCap", 0), data.get("longName", "")
        except:
            return 0, ""

    def fetch(self, ticker):
        try:
            # --- Technicals (1y daily) ---
            p_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(p_url, timeout=5)
            j = r.json()["chart"]["result"][0]
            
            meta = j.get("meta", {})
            close = [c for c in j["indicators"]["quote"][0]["close"] if c is not None]
            vol = [v for v in j["indicators"]["quote"][0]["volume"] if v is not None]

            if len(close) < 60: return None
            price = close[-1]
            
            if price < MIN_PRICE: return None

            # 指標計算
            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1
            accel = m1 - m3
            
            vol_avg_short = sum(vol[-5:]) / 5
            vol_avg_long = sum(vol[-20:]) / 20
            vol_ratio = vol_avg_short / (vol_avg_long + 1e-9)

            # --- Fundamentals & Noise Filter ---
            mcap, name = self.fetch_mcap_safe(ticker)
            
            # SPAC/Warrant フィルタ (簡易化して誤爆を防ぐ)
            if any(k in name.upper() for k in ["WARRANT", "UNIT", "ACQUISITION CORP"]):
                return None
            
            if mcap > 0 and mcap < MIN_MCAP: return None

            return {
                "ticker": ticker, "name": name, "price": price, "m1": m1, 
                "accel": accel, "vol": vol_ratio, "mcap": mcap
            }
        except:
            return None

    def run(self):
        universe = self.load_universe()
        batch = self.get_batch(universe)
        print(f"Scanning {len(batch)} symbols...")
        
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch, t): t for t in batch}
            for f in as_completed(futures):
                r = f.result()
                if r: results.append(r)

        if not results:
            self.send_webhook("⚠️ 有効データがありませんでした。")
            return

        df = pd.DataFrame(results)
        
        # Z-Scoreでスコアリング
        for col in ["accel", "m1", "vol"]:
            df[f"z_{col}"] = (df[col] - df[col].mean()) / (df[col].std() + 1e-9)

        df["score"] = (df["z_accel"] * 0.5 + df["z_vol"] * 0.3 + df["z_m1"] * 0.2)
        top_df = df.sort_values("score", ascending=False).head(15)
        
        self.report(top_df, len(batch), len(df))

    def report(self, df, scanned, valid):
        msg = [
            f"🚀 **GrowthRadar v23 (Mass Scan)**",
            f"Universe: {scanned} | Valid: {valid}\n"
        ]

        for r in df.to_dict("records"):
            mcap_str = f"${r['mcap']/1e9:.2f}B" if r['mcap'] > 0 else "N/A"
            msg.append(
                f"**{r['ticker']}** ({r['name'][:20]}) | **Score: {r['score']:.2f}**\n"
                f"└ Price: ${r['price']:.2f} | MC: {mcap_str}\n"
                f"└ M1: {r['m1']:+.1%} | Accel: {r['accel']:+.1%} | Vol: {r['vol']:.1f}x"
            )

        self.send_webhook("\n".join(msg))

    def send_webhook(self, text):
        if WEBHOOK_URL:
            try: requests.post(WEBHOOK_URL, json={"content": text}, timeout=10)
            except: pass
        print(text)

if __name__ == "__main__":
    GrowthRadarV23().run()
