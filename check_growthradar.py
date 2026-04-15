import os
import requests
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import time

# =========================
# ENV
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

# =========================
# CONFIG (Aggressive Settings)
# =========================
MAX_WORKERS = 10  # セッション共有により高速化
SCAN_LIMIT = 1500
FINAL_LIMIT = 300

MIN_MCAP = 50_000_000
MAX_MCAP = 3_000_000_000  # 小型株の定義をより厳格に
MIN_SCORE = 10            # 妥協を許さない基準値

# =========================
# CORE LOGIC
# =========================

class ButcherScanner:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0'})

    def get_tickers(self):
        """NASDAQ上場銘柄を取得（バックアップ付き）"""
        try:
            url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
            df = pd.read_csv(url)
            return df["Symbol"].dropna().tolist()
        except:
            return ["AAPL", "NVDA", "TSLA", "AMD", "META", "UPST", "PLTR", "IONQ", "RKLB"]

    def pre_filter(self, ticker):
        """Phase1: 価格、出来高、短期トレンドの最低条件"""
        try:
            t = yf.Ticker(ticker, session=self.session)
            hist = t.history(period="10d")
            if hist.empty or len(hist) < 5: return None

            price = hist["Close"].iloc[-1]
            avg_vol = hist["Volume"].mean()
            
            # ペニーストックと不人気株を即排除
            if price < 2 or avg_vol < 100_000: return None
            
            # 短期モメンタム (5日移動平均との乖離)
            ma5 = hist["Close"].tail(5).mean()
            if price < ma5: return None # 下降局面は無視

            return {"ticker": ticker, "price": price}
        except:
            return None

    def fetch_deep_data(self, ticker):
        """Phase3: 財務の質と真のモメンタムを解剖"""
        try:
            t = yf.Ticker(ticker, session=self.session)
            
            # 1. 財務 (売上成長 + キャッシュフロー)
            yoy, ocf_ratio = 0, 0
            try:
                # 四半期財務
                fin = t.quarterly_financials
                cash = t.quarterly_cashflow
                
                if fin is not None and not fin.empty:
                    fin.index = fin.index.str.replace(' ', '').str.upper()
                    if "TOTALREVENUE" in fin.index:
                        rev = fin.loc["TOTALREVENUE"].dropna().values
                        if len(rev) >= 2 and rev[1] > 0:
                            yoy = (rev[0] - rev[1]) / rev[1]

                if cash is not None and not cash.empty:
                    cash.index = cash.index.str.replace(' ', '').str.upper()
                    if "OPERATINGCASHFLOW" in cash.index:
                        ocf = cash.loc["OPERATINGCASHFLOW"].dropna().values
                        # 売上高に対する営業CFの割合（健全性指標）
                        if len(ocf) > 0 and len(rev) > 0 and rev[0] > 0:
                            ocf_ratio = ocf[0] / rev[0]
            except: pass

            if yoy <= 0: return None # 成長してないなら話にならない

            # 2. 時価総額の厳密チェック
            mcap = 0
            try:
                mcap = t.fast_info.market_cap
                if mcap < MIN_MCAP or mcap > MAX_MCAP: return None
            except: return None # Mcap不明はリスクのため排除

            # 3. テクニカル分析 (3ヶ月 & 1ヶ月)
            hist = t.history(period="4mo")
            if len(hist) < 60: return None
            
            p_now = hist["Close"].iloc[-1]
            p_1m = hist["Close"].iloc[-20]
            p_3m = hist["Close"].iloc[-60]

            mom_3m = (p_now - p_3m) / p_3m
            mom_1m = (p_now - p_1m) / p_1m
            
            # 「落ちてくるナイフ」排除：1ヶ月で15%以上掘ってるなら除外
            if mom_1m < -0.15: return None

            # 出来高の質：直近3日の平均 / 20日平均
            vol_spike = hist["Volume"].tail(3).mean() / (hist["Volume"].tail(20).mean() + 1)

            return {
                "ticker": ticker,
                "yoy": yoy,
                "ocf_ratio": ocf_ratio,
                "mom_3m": mom_3m,
                "mom_1m": mom_1m,
                "vol_spike": vol_spike,
                "mcap": mcap,
                "price": p_now
            }
        except:
            return None

    def calculate_score(self, d):
        """妥協なきスコアリング"""
        s = 0
        
        # 成長性 (YoY)
        if d["yoy"] > 1.0: s += 8   # 爆速
        elif d["yoy"] > 0.4: s += 6
        elif d["yoy"] > 0.15: s += 3

        # 現金創出力 (OCF Ratio)
        if d["ocf_ratio"] > 0.2: s += 4 # 超健全
        elif d["ocf_ratio"] > 0: s += 1
        else: s -= 5 # 現金が流出している赤字垂れ流しは減点

        # モメンタムの質
        if d["mom_1m"] > 0 and d["mom_3m"] > 0:
            s += 3 # 上昇トレンド継続
        if d["mom_1m"] > d["mom_3m"]:
            s += 3 # 上昇が加速している

        # 出来高の異常
        if d["vol_spike"] > 2.0: s += 5 # 機関投資家の買いの可能性
        elif d["vol_spike"] > 1.3: s += 2

        # 時価総額ボーナス
        if d["mcap"] < 300_000_000: s += 2 # 超小型プレミアム

        return s

    def run(self):
        start_time = time.time()
        stats = {"p1": 0, "p3_in": 0, "valid": 0}
        
        print("--- Execution Started ---")
        tickers = self.get_tickers()
        random.shuffle(tickers)
        
        # Phase 1
        p1_list = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.pre_filter, t): t for t in tickers[:SCAN_LIMIT]}
            for f in as_completed(futures):
                res = f.result()
                if res: p1_list.append(res)
        
        stats["p1"] = len(p1_list)
        
        # Phase 2: Sort
        # 単純なMomではなく直近の強さで足切り
        p2_list = sorted(p1_list, key=lambda x: x.get('price', 0), reverse=True)[:FINAL_LIMIT]
        final_tickers = [x["ticker"] for x in p2_list]
        stats["p3_in"] = len(final_tickers)

        # Phase 3
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch_deep_data, t): t for t in final_tickers}
            for f in as_completed(futures):
                res = f.result()
                if res:
                    res["score"] = self.calculate_score(res)
                    if res["score"] >= MIN_SCORE:
                        results.append(res)
                        stats["valid"] += 1

        # Display / Notify
        df = pd.DataFrame(results)
        if not df.empty:
            df = df.sort_values("score", ascending=False).head(10)
            self.notify(df, stats)
        else:
            self.notify(pd.DataFrame(), stats)
            
        print(f"--- Finished in {time.time() - start_time:.1f}s ---")

    def notify(self, df, stats):
        msg = "🔪 **GrowthRadar v9.0 [The Butcher]**\n\n"
        if df.empty:
            msg += "❌ No high-quality survivors today."
        else:
            for _, r in df.iterrows():
                health = "✅" if r['ocf_ratio'] > 0 else "🚨"
                msg += (
                    f"**{r['ticker']}** | Score: **{r['score']}**\n"
                    f"RevYoY: {r['yoy']:.1%} {health} OCF/Rev: {r['ocf_ratio']:.1%}\n"
                    f"Mom1M: {r['mom_1m']:.1%} | Vol: x{r['vol_spike']:.1f}\n\n"
                )
        
        msg += f"
