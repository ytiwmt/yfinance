import os
import requests
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import time

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

SCAN_LIMIT = 1500
MAX_WORKERS = 8

MIN_MCAP = 150_000_000
MAX_MCAP = 2_500_000_000
MIN_PRICE = 4.0
MIN_YOY = 0.25

class GrowthRadarZeroError:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0'
        })

    def get_tickers(self):
        try:
            url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
            return pd.read_csv(url)["Symbol"].dropna().tolist()
        except:
            return ["NVDA", "TSLA", "CELH", "RKLB", "IONQ", "HIMS", "PLTR", "UPST"]

    def prefilter(self, ticker):
        try:
            t = yf.Ticker(ticker, session=self.session)
            hist = t.history(period="1mo")
            if len(hist) < 15: return None
            p_now = hist["Close"].iloc[-1]
            vol_avg = hist["Volume"].mean()
            if p_now < MIN_PRICE or vol_avg < 100_000: return None
            if p_now < hist["Close"].iloc[0]: return None
            return {"ticker": ticker, "price": p_now}
        except: return None

    def analyze(self, ticker):
        try:
            t = yf.Ticker(ticker, session=self.session)
            fast = t.fast_info
            mcap = fast.market_cap
            if not mcap or mcap < MIN_MCAP or mcap > MAX_MCAP: return None

            hist = t.history(period="1y")
            if len(hist) < 150: return None
            p_now = hist["Close"].iloc[-1]
            high_1y = hist["Close"].max()
            dist_high = (high_1y - p_now) / high_1y
            
            recent = hist.tail(20)
            up_vol = recent[recent['Close'] > recent['Open']]['Volume'].sum()
            down_vol = recent[recent['Close'] <= recent['Open']]['Volume'].sum()
            acc_dist = up_vol / (down_vol + 1)

            fin = t.quarterly_financials
            if fin is None or fin.empty: return None
            fin.index = fin.index.str.replace(" ", "").str.upper()
            if "TOTALREVENUE" not in fin.index: return None
            rev = fin.loc["TOTALREVENUE"].dropna().values
            if len(rev) < 3: return None

            g0 = (rev[0] - rev[1]) / rev[1] if rev[1] > 0 else 0
            g1 = (rev[1] - rev[2]) / rev[2] if rev[2] > 0 else 0
            accel = g0 - g1
            if g0 < MIN_YOY: return None

            margin_boost = 0
            if "OPERATINGINCOME" in fin.index:
                op_inc = fin.loc["OPERATINGINCOME"].dropna().values
                if len(op_inc) >= 2:
                    margin_boost = (op_inc[0] / rev[0]) - (op_inc[1] / rev[1])

            return {
                "ticker": ticker, "price": p_now, "mcap": mcap,
                "yoy": g0, "accel": accel, "margin_boost": margin_boost,
                "dist_high": dist_high, "acc_dist": acc_dist
            }
        except: return None

    def calculate_score(self, d):
        s = 0
        if d["accel"] > 0.1: s += 8
        elif d["accel"] > 0: s += 4
        if d["margin_boost"] > 0.05: s += 6
        elif d["margin_boost"] > 0: s += 2
        if d["dist_high"] < 0.03: s += 7
        elif d["dist_high"] < 0.1: s += 3
        if d["acc_dist"] > 1.5: s += 5
        if d["mcap"] < 1_000_000_000: s += 3
        return s

    def run(self):
        start_time = time.time()
        tickers = self.get_tickers()
        random.shuffle(tickers)
        
        p1_results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.prefilter, t): t for t in tickers[:SCAN_LIMIT]}
            for f in as_completed(futures):
                res = f.result()
                if res: p1_results.append(res)

        final_list = []
        targets = [x["ticker"] for x in p1_results]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.analyze, t): t for t in targets}
            for f in as_completed(futures):
                res = f.result()
                if res:
                    res["score"] = self.calculate_score(res)
                    if res["score"] >= 15: final_list.append(res)

        df = pd.DataFrame(final_list)
        if not df.empty:
            df = df.sort_values("score", ascending=False).head(12)
        
        self.notify(df, len(p1_results), len(final_list))
        print("Done. Time:", round(time.time() - start_time, 1))

    def notify(self, df, s1, s2):
        # 構文エラーを避けるため、format関数を使用し、文字列内で改行を明示的に記述
        msg_list = []
        msg_list.append("🚀 **GrowthRadar v10.3 Zero-Error**")
        msg_list.append("--- 利益加速 & ブレイクアウト捕捉 ---\n")
        
        if df.empty:
            msg_list.append("❌ 対象銘柄なし")
        else:
            for _, r in df.iterrows():
                line1 = "**{}** | Score: **{}**".format(r['ticker'], r['score'])
                line2 = "売上増: {:.1%} (加速: {:.1%})".format(r['yoy'], r['accel'])
                line3 = "利幅改善: {:.1%} | 出来高質: {:.1f}".format(r['margin_boost'], r['acc_dist'])
                line4 = "高値乖離: -{:.1%} | MCap: {:.1f}B".format(r['dist_high'], r['mcap']/1e9)
                msg_list.append(line1 + "\n" + line2 + "\n" + line3 + "\n" + line4 + "\n")

        msg_list.append("
