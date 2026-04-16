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

# GitHub環境での安定性を考慮した制限
SCAN_LIMIT = 500   # 欲張らずに確実に完走させる
MAX_WORKERS = 2    # 並列数を絞って検知を回避

# スクリーニング基準
MIN_MCAP = 50_000_000
MAX_MCAP = 4_000_000_000
MIN_PRICE = 1.0
MIN_YOY = 0.15

class GrowthRadarGhost:
    def __init__(self):
        self.session = requests.Session()
        # ブラウザを装うための複数のUser-Agent
        self.agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'
        ]

    def update_headers(self):
        self.session.headers.update({
            'User-Agent': random.choice(self.agents),
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://finance.yahoo.com',
            'Referer': 'https://finance.yahoo.com/'
        })

    def get_tickers(self):
        """監視対象銘柄の取得"""
        # 成長株の精鋭リスト（データ取得失敗時の保険）
        core_growth = ["PLTR", "CELH", "RKLB", "IONQ", "HIMS", "UPST", "DUOL", "APP", "SOUN", "BROS", "MSTR", "HOOD", "NVDA", "VRT", "SMCI"]
        try:
            url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
            res = self.session.get(url, timeout=10)
            if res.status_code == 200:
                df = pd.read_csv(StringIO(res.text))
                nasdaq = df["Symbol"].dropna().tolist()
                # 重複を排除して結合
                return list(set(core_growth + nasdaq))
            return core_growth
        except:
            return core_growth

    def prefilter(self, ticker):
        """フェーズ1: 生存確認と価格チェック"""
        for i in range(2): # 2回試行
            try:
                self.update_headers()
                time.sleep(random.uniform(0.5, 1.5)) # 検知回避のウェイト
                
                t = yf.Ticker(ticker, session=self.session)
                hist = t.history(period="5d") # 直近データのみ
                
                if hist.empty or len(hist) < 2:
                    continue

                p_now = hist["Close"].iloc[-1]
                vol_avg = hist["Volume"].mean()
                
                if p_now < MIN_PRICE or vol_avg < 30_000:
                    return None
                
                return {"ticker": ticker, "price": p_now}
            except:
                time.sleep(2)
        return None

    def analyze(self, ticker):
        """フェーズ2: 成長性と時価総額の深層解析"""
        try:
            self.update_headers()
            time.sleep(random.uniform(0.8, 2.0))
            
            t = yf.Ticker(ticker, session=self.session)
            f = t.fast_info
            
            # 時価総額チェック
            mcap = getattr(f, 'market_cap', None)
            if not mcap or mcap < MIN_MCAP or mcap > MAX_MCAP:
                return None

            # 財務諸表 (四半期)
            fin = t.quarterly_financials
            if fin is None or fin.empty:
                return None
            
            # 行名の正規化
            fin.index = fin.index.str.replace(" ", "").str.upper()
            
            # 売上の取得
            rev_labels = ["TOTALREVENUE", "REVENUE"]
            rev_key = next((k for k in rev_labels if k in fin.index), None)
            if not rev_key: return None
            
            rev = fin.loc[rev_key].dropna().values
            if len(rev) < 2: return None

            # 成長率 (YoY近似)
            g0 = (rev[0] - rev[1]) / rev[1] if rev[1] > 0 else 0
            if g0 < MIN_YOY: return None

            # 成長の加速
            accel = 0
            if len(rev) >= 3:
                g1 = (rev[1] - rev[2]) / rev[2] if rev[2] > 0 else 0
                accel = g0 - g1

            # 営業利益の改善
            margin_boost = 0
            if "OPERATINGINCOME" in fin.index:
                op_inc = fin.loc["OPERATINGINCOME"].dropna().values
                if len(op_inc) >= 2 and rev[0] > 0 and rev[1] > 0:
                    margin_boost = (op_inc[0] / rev[0]) - (op_inc[1] / rev[1])

            return {
                "ticker": ticker,
                "price": getattr(f, 'last_price', 0),
                "mcap": mcap,
                "yoy": g0,
                "accel": accel,
                "margin": margin_boost
            }
        except:
            return None

    def run(self):
        start_time = time.time()
        tickers = self.get_tickers()
        random.shuffle(tickers)
        
        # ステージ1
        targets = tickers[:SCAN_LIMIT]
        p1_results = []
        print(f"[*] ステージ1 スキャン開始 ({len(targets)} 銘柄)")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.prefilter, t): t for t in targets}
            for f in as_completed(futures):
                res = f.result()
                if res: p1_results.append(res)

        # 救済措置：S1が全滅した場合、コア銘柄を強制追加
        if not p1_results:
            print("[!] S1通過ゼロのため、コア銘柄を直接スキャンします")
            core_growth = ["PLTR", "CELH", "RKLB", "IONQ", "HIMS", "NVDA", "MSTR"]
            p1_results = [{"ticker": t} for t in core_growth]

        print(f"[*] ステージ1 完了: {len(p1_results)} 銘柄通過")
        
        # ステージ2
        final_list = []
        candidates = [x["ticker"] for x in p1_results]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.analyze, t): t for t in candidates}
            for f in as_completed(futures):
                res = f.result()
                if res:
                    # スコアリング
                    score = 0
                    if res["yoy"] > 0.35: score += 10
                    if res["accel"] > 0: score += 5
                    if res["margin"] > 0: score += 5
                    if res["mcap"] < 1_000_000_000: score += 5 # 小型株プレミアム
                    
                    res["score"] = score
                    final_list.append(res)

        # 結果の出力
        df = pd.DataFrame(final_list)
        if not df.empty:
            df = df.sort_values("score", ascending=False).head(10)
        
        self.notify(df, len(p1_results), len(final_list))
        print(f"[*] 全工程完了: {time.time() - start_time:.1f}秒")

    def notify(self, df, s1, s2):
        msg = []
        msg.append("🚀 **GrowthRadar v10.6 Ghost Protocol**")
        msg.append("----------------------------")
        
        if df.empty:
            msg.append("⚠️ 条件に合致する爆発銘柄は見つかりませんでした。")
        else:
            for _, r in df.iterrows():
                line = "**{}** | Score: {} | YoY: {:.0%} | MC: {:.1f}B".format(
                    r['ticker'], r['score'], r['yoy'], r['mcap']/1e9
                )
                msg.append(line)

        msg.append("----------------------------")
        msg.append("Stats: S1_Pass={} | Final_Hits={}".format(s1, s2))
        
        full_msg = "\n".join(msg)
        
        if WEBHOOK_URL:
            try:
                requests.post(WEBHOOK_URL, json={"content": full_msg}, timeout=15)
            except:
                print("[!] Webhook送信失敗")
        else:
            print(full_msg)

if __name__ == "__main__":
    GrowthRadarGhost().run()
