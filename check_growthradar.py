import os
import requests
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import time
from io import StringIO

# =========================
# 構成設定 (GitHub Actions 最適化)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

# GitHub環境での安定性を考慮
SCAN_LIMIT = 600   
MAX_WORKERS = 2    

# スクリーニング基準を「相対的」に調整
MIN_MCAP = 50_000_000
MAX_MCAP = 8_000_000_000
MIN_PRICE = 1.0
# 成長率のハードルを少し下げ、テクニカル評価を重視する
MIN_YOY_THRESHOLD = 0.10 

class GrowthRadarAgile:
    def __init__(self):
        self.session = requests.Session()
        self.agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        ]

    def update_headers(self):
        self.session.headers.update({
            'User-Agent': random.choice(self.agents),
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://finance.yahoo.com/'
        })

    def get_tickers(self):
        """監視対象銘柄の取得（コア銘柄を強化）"""
        core_growth = [
            "PLTR", "CELH", "RKLB", "IONQ", "HIMS", "UPST", "DUOL", "APP", 
            "SOUN", "BROS", "MSTR", "HOOD", "NVDA", "VRT", "SMCI", "RDDT", 
            "LUNR", "OKLO", "ASTS", "SERV", "NNE"
        ]
        try:
            url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
            res = self.session.get(url, timeout=10)
            if res.status_code == 200:
                df = pd.read_csv(StringIO(res.text))
                nasdaq = df["Symbol"].dropna().tolist()
                return list(set(core_growth + nasdaq))
            return core_growth
        except:
            return core_growth

    def analyze(self, ticker):
        """財務データが取れない場合でもテクニカルで加点する柔軟な解析"""
        try:
            self.update_headers()
            time.sleep(random.uniform(0.8, 1.5))
            
            t = yf.Ticker(ticker, session=self.session)
            f = t.fast_info
            
            # 1. 基本チェック
            mcap = getattr(f, 'market_cap', 0)
            if mcap < MIN_MCAP or mcap > MAX_MCAP:
                return None

            hist = t.history(period="1y")
            if hist.empty or len(hist) < 50: return None
            
            p_now = hist["Close"].iloc[-1]
            high_52w = hist["Close"].max()
            # 新高値からの距離
            dist_high = (high_52w - p_now) / (high_52w + 1e-9)
            # 50日移動平均との乖離（トレンド強度）
            ma50 = hist["Close"].tail(50).mean()
            relative_strength = p_now / ma50 if ma50 > 0 else 1.0

            # 2. 財務解析 (失敗しても続行)
            yoy_growth = 0
            accel = 0
            has_financials = False
            
            try:
                fin = t.quarterly_financials
                if fin is not None and not fin.empty:
                    fin.index = fin.index.str.replace(" ", "").str.upper()
                    rev_key = next((k for k in ["TOTALREVENUE", "REVENUE"] if k in fin.index), None)
                    if rev_key:
                        rev = fin.loc[rev_key].dropna().values
                        if len(rev) >= 2:
                            yoy_growth = (rev[0] - rev[1]) / rev[1] if rev[1] > 0 else 0
                            has_financials = True
                            if len(rev) >= 3:
                                g1 = (rev[1] - rev[2]) / rev[2] if rev[2] > 0 else 0
                                accel = yoy_growth - g1
            except:
                pass

            # 3. スコアリング (テクニカル + 財務)
            score = 0
            # テクニカル評価 (最大15点)
            if dist_high < 0.05: score += 10 # ブレイクアウト直前
            elif dist_high < 0.15: score += 5
            if relative_strength > 1.2: score += 5 # 強い上昇トレンド

            # 財務評価 (最大15点)
            if has_financials:
                if yoy_growth > 0.3: score += 10
                elif yoy_growth > 0.1: score += 5
                if accel > 0: score += 5
            else:
                # 財務データが取れない場合はテクニカルが非常に強い場合のみ通す
                if score < 10: return None

            return {
                "ticker": ticker,
                "price": p_now,
                "mcap": mcap,
                "yoy": yoy_growth,
                "score": score,
                "dist": dist_high
            }
        except:
            return None

    def run(self):
        start_time = time.time()
        tickers = self.get_tickers()
        random.shuffle(tickers)
        
        targets = tickers[:SCAN_LIMIT]
        print(f"[*] スキャン開始: {len(targets)} 銘柄")
        
        final_list = []
        # 直接解析フェーズへ（prefilterを統合して効率化）
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.analyze, t): t for t in targets}
            for f in as_completed(futures):
                res = f.result()
                if res and res["score"] >= 10:
                    final_list.append(res)

        df = pd.DataFrame(final_list)
        if not df.empty:
            # スコア順、次いで新高値に近い順
            df = df.sort_values(["score", "dist"], ascending=[False, True]).head(12)
        
        self.notify(df, len(targets), len(final_list))
        print(f"[*] 完了: {time.time() - start_time:.1f}秒")

    def notify(self, df, total, hits):
        msg = []
        msg.append("🏹 **GrowthRadar v10.7 Agile Hunter**")
        msg.append("----------------------------")
        
        if df.empty:
            msg.append("⚠️ 現在の基準（テクニカル/財務）で合致する銘柄なし")
        else:
            for _, r in df.iterrows():
                # 財務データがある場合のみYoYを表示
                yoy_str = "{:.0%}".format(r['yoy']) if r['yoy'] != 0 else "N/A"
                line = "**{}** | Score: {} | YoY: {} | High: -{:.0%}".format(
                    r['ticker'], r['score'], yoy_str, r['dist']
                )
                msg.append(line)

        msg.append("----------------------------")
        msg.append("Stats: Scanned={} | Hits={}".format(total, hits))
        
        full_msg = "\n".join(msg)
        if WEBHOOK_URL:
            try: requests.post(WEBHOOK_URL, json={"content": full_msg}, timeout=15)
            except: print("Webhook Error")
        else: print(full_msg)

if __name__ == "__main__":
    GrowthRadarAgile().run()
