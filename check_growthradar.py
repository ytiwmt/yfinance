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
# CONFIG (v22 Noise Filtered)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")
UNIVERSE_FILE = "universe.json"
SCAN_SPLIT = 2
MAX_WORKERS = 12 

# フィルタ基準
MIN_PRICE = 1.5  # 低価格すぎる仕手株を少し警戒
MIN_MCAP = 7e7   # 70Mドル以下はボラティリティが危険すぎるため切り上げ
MAX_MCAP = 2e11  # 超大型（NVDA等）は異常値が出にくいため上限設定

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

class GrowthRadarV22:
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

        # トレンド・成長株のベース
        hot_list = [
            "IONQ","RKLB","ASTS","OKLO","LUNR","QUBT","RGTI","QBTS","EOSE","MAAS","BULL","UPST","TEM","MLYS","AUR","LUMN","HOOD","COIN","MARA","MSTR",
            "PLTR","SOUN","BBAI","NNE","SMR","GGE","HITI","CGC","PLUG","RUN","ENPH","TSLA","RIVN","LCID","AFRM","SOFI","SQ","PYPL","SHOP","SE","MGRT"
        ]
        
        if not symbols or len(symbols) < 1000:
            try:
                # 取得元をより安定した場所に切り替え
                url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt"
                res = self.session.get(url, timeout=10)
                symbols = [s.strip() for s in res.text.split('\n') if s.strip().isalpha() and len(s.strip()) <= 5]
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

    def is_noise(self, ticker, name=""):
        """SPAC、ユニット、ワラント、合併待ち銘柄を排除"""
        noise_keywords = ["Warrant", "Unit", "Acquisition", "Rights", "Redemption", "Merge"]
        if any(k.lower() in name.lower() for k in noise_keywords):
            return True
        # ティッカー自体にW(ワラント)やU(ユニット)がつくケースを排除
        if len(ticker) > 4 and (ticker.endswith("W") or ticker.endswith("U")):
            return True
        return False

    def fetch(self, ticker):
        try:
            # --- Technicals ---
            p_url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(p_url, timeout=7)
            j = r.json()["chart"]["result"][0]
            
            meta = j.get("meta", {})
            name = meta.get("symbol", "")
            
            # 時価総額をmetaから優先取得（N/A対策）
            mcap = meta.get("marketCap", 0)
            
            close = [c for c in j["indicators"]["quote"][0]["close"] if c]
            vol = [v for v in j["indicators"]["quote"][0]["volume"] if v]

            if len(close) < 100: return None
            price = close[-1]
            
            # SPACや低価格すぎる銘柄を排除
            if price < MIN_PRICE: return None
            if self.is_noise(ticker): return None

            # 指標計算
            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1
            accel = m1 - m3
            
            # 出来高の質（直近5日平均 / 過去20日平均）
            vol_avg_short = sum(vol[-5:]) / 5
            vol_avg_long = sum(vol[-20:]) / 20
            vol_ratio = vol_avg_short / (vol_avg_long + 1e-9)

            # --- Fundamentals (Secondary) ---
            rev = 0.0
            if mcap == 0:
                try:
                    f_url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=financialData,defaultKeyStatistics"
                    fr = self.session.get(f_url, timeout=3).json()
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
        try:
            universe = self.load_universe()
            batch = self.get_batch(universe)
            results = []

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futures = {ex.submit(self.fetch, t): t for t in batch}
                for f in as_completed(futures):
                    r = f.result()
                    if r: results.append(r)

            if not results:
                self.send_webhook(f"⚠️ **GrowthRadar v22 Status**\n母集団 {len(batch)} 件中、有効データなし。")
                return

            df = pd.DataFrame(results)
            
            # 異常すぎる値をクリップ（数値の歪み対策）
            df["m1"] = df["m1"].clip(-1, 5) 
            df["accel"] = df["accel"].clip(-5, 5)
            df["vol"] = df["vol"].clip(0, 10)

            # Z-Score計算
            for col in ["accel", "m1", "vol"]:
                df[f"z_{col}"] = (df[col] - df[col].mean()) / (df[col].std() + 1e-9)

            # スコアリング（出来高と加速に重み）
            df["score"] = (
                df["z_accel"] * 0.45 +
                df["z_vol"] * 0.35 +
                df["z_m1"] * 0.20
            )

            # 有効な成長率があれば加点
            df.loc[df["rev"] > 0.1, "score"] += 0.5

            top_df = df.sort_values("score", ascending=False).head(15)
            self.report(top_df, len(batch), len(df))

        except Exception as e:
            self.send_webhook(f"🚨 **GrowthRadar Fatal Error**\n`{str(e)}`")

    def report(self, df, scanned, valid):
        msg = [
            f"🛰️ **GrowthRadar v22 (Noise Filtered)**",
            f"Universe: {scanned} | Analyzed: {valid}\n"
        ]

        for r in df.to_dict("records"):
            rev_str = f"{r['rev']:.1%}" if r['rev'] != 0 else "N/A"
            mcap_str = f"{r['mcap']/1e9:.2f}B" if r['mcap'] != 0 else "N/A"
            
            msg.append(
                f"**{r['ticker']}** | Score: {r['score']:.2f}\n"
                f"└ Price: ${r['price']:.2f} | MC: {mcap_str} | Rev: {rev_str}\n"
                f"└ M1: {r['m1']:+.1%} | Vol: {r['vol']:.1f}x"
            )

        self.send_webhook("\n".join(msg))

    def send_webhook(self, text):
        if WEBHOOK_URL:
            try: requests.post(WEBHOOK_URL, json={"content": text}, timeout=10)
            except: pass
        print(text)

if __name__ == "__main__":
    GrowthRadarV22().run()
