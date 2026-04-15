import os
import requests
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import random
import time

# =========================
# 設定 (テンバガー・ハント・ルール)
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

SCAN_LIMIT = 1500  # スキャンする銘柄数
MAX_WORKERS = 8     # 並列実行数

# スクリーニング基準
MIN_MCAP = 150_000_000    # 時価総額下限 (1.5億ドル)
MAX_MCAP = 2_500_000_000  # 時価総額上限 (25億ドル)
MIN_PRICE = 4.0           # 株価下限 (機関投資家が買える基準)
MIN_YOY = 0.25            # 売上成長率下限 (25%以上)

# =========================
# スキャナー・エンジン
# =========================
class GrowthRadarFinal:
    def __init__(self):
        # API制限を回避するためのセッション設定
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def get_tickers(self):
        """ナスダック上場銘柄リストを取得"""
        try:
            url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
            return pd.read_csv(url)["Symbol"].dropna().tolist()
        except Exception:
            # 取得失敗時のフォールバック銘柄
            return ["NVDA", "TSLA", "CELH", "RKLB", "IONQ", "HIMS", "PLTR", "UPST", "SMCI", "AMD"]

    def prefilter(self, ticker):
        """フェーズ1: 出来高と直近トレンドによる足切り (高速処理)"""
        try:
            t = yf.Ticker(ticker, session=self.session)
            hist = t.history(period="1mo")
            if len(hist) < 15:
                return None

            p_now = hist["Close"].iloc[-1]
            vol_avg = hist["Volume"].mean()

            # 最低価格と最低限の流動性チェック
            if p_now < MIN_PRICE or vol_avg < 100_000:
                return None
            
            # 直近1ヶ月で下降トレンドのものは除外
            if p_now < hist["Close"].iloc[0]:
                return None

            return {"ticker": ticker, "price": p_now}
        except Exception:
            return None

    def analyze(self, ticker):
        """フェーズ2: 財務の爆発力（加速）とテクニカル（新高値）の深層解析"""
        try:
            t = yf.Ticker(ticker, session=self.session)
            fast = t.fast_info
            mcap = fast.market_cap
            
            # 時価総額のフィルタリング
            if not mcap or mcap < MIN_MCAP or mcap > MAX_MCAP:
                return None

            # 1. テクニカル分析 (1年間のデータ)
            hist = t.history(period="1y")
            if len(hist) < 150:
                return None
            
            p_now = hist["Close"].iloc[-1]
            high_1y = hist["Close"].max()
            dist_high = (high_1y - p_now) / high_1y # 新高値からの乖離率
            
            # 買い集めの質 (上昇日の出来高合計 / 下落日の出来高合計)
            recent = hist.tail(20)
            up_vol = recent[recent['Close'] > recent['Open']]['Volume'].sum()
            down_vol = recent[recent['Close'] <= recent['Open']]['Volume'].sum()
            acc_dist = up_vol / (down_vol + 1)

            # 2. 財務分析 (四半期ごとの売上・利益の加速)
            fin = t.quarterly_financials
            if fin is None or fin.empty:
                return None
            
            # インデックスの正規化 (スペース削除・大文字化)
            fin.index = fin.index.str.replace(" ", "").str.upper()
            
            if "TOTALREVENUE" not in fin.index:
                return None
            
            rev = fin.loc["TOTALREVENUE"].dropna().values
            if len(rev) < 3:
                return None

            # 売上成長率と『加速』の計算
            g0 = (rev[0] - rev[1]) / rev[1] if rev[1] > 0 else 0
            g1 = (rev[1] - rev[2]) / rev[2] if rev[2] > 0 else 0
            accel = g0 - g1
            
            # 最低成長率フィルタ
            if g0 < MIN_YOY:
                return None

            # 利益率の改善 (営業レバレッジの確認)
            margin_boost = 0
            if "OPERATINGINCOME" in fin.index:
                op_inc = fin.loc["OPERATINGINCOME"].dropna().values
                if len(op_inc) >= 2:
                    m0 = op_inc[0] / rev[0]
                    m1 = op_inc[1] / rev[1]
                    margin_boost = m0 - m1

            return {
                "ticker": ticker,
                "price": p_now,
                "mcap": mcap,
                "yoy": g0,
                "accel": accel,
                "margin_boost": margin_boost,
                "dist_high": dist_high,
                "acc_dist": acc_dist
            }
        except Exception:
            return None

    def calculate_score(self, d):
        """銘柄のポテンシャルを点数化"""
        s = 0
        
        # 売上成長の加速 (最重要)
        if d["accel"] > 0.1: s += 8
        elif d["accel"] > 0: s += 4
        
        # 利益率の劇的な改善
        if d["margin_boost"] > 0.05: s += 6
        elif d["margin_boost"] > 0: s += 2

        # 新高値ブレイクアウト直前
        if d["dist_high"] < 0.03: s += 7
        elif d["dist_high"] < 0.1: s += 3
        
        # 出来高の質 (機関投資家の買い集め)
        if d["acc_dist"] > 1.5: s += 5
        
        # テンバガーになりやすい小型株サイズ
        if d["mcap"] < 1_000_000_000: s += 3

        return s

    def run(self):
        start_time = time.time()
        tickers = self.get_tickers()
        random.shuffle(tickers)
        
        print(f"[*] スキャン開始: {SCAN_LIMIT} 銘柄対象")

        # ステップ 1: プレフィルタリング (高速)
        p1_results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.prefilter, t): t for t in tickers[:SCAN_LIMIT]}
            for f in as_completed(futures):
                res = f.result()
                if res:
                    p1_results.append(res)

        # ステップ 2: 深層解析
        final_list = []
        targets = [x["ticker"] for x in p1_results]
        print(f"[*] 解析候補: {len(targets)} 銘柄")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.analyze, t): t for t in targets}
            for f in as_completed(futures):
                res = f.result()
                if res:
                    res["score"] = self.calculate_score(res)
                    # スコア15以上を合格とする
                    if res["score"] >= 15:
                        final_list.append(res)

        # 結果のソート
        df = pd.DataFrame(final_list)
        if not df.empty:
            df = df.sort_values("score", ascending=False).head(12)
        
        self.send_notification(df, len(p1_results), len(final_list))
        print(f"[*] 完了: 所要時間 {time.time() - start_time:.1f} 秒")

    def send_notification(self, df, s1_count, final_count):
        """結果をDiscord/Slackに通知、または標準出力"""
        header = "🔥 **GrowthRadar v10.2 Final**\n--- 利益率改善と加速を伴うテンバガー候補 ---\n\n"
        
        body = ""
        if df.empty:
            body = "❌ 条件を満たす爆発候補は見つかりませんでした。\n"
        else:
            for _, r in df.iterrows():
                boost = "⚡" if r['margin_boost'] > 0 else "➖"
                body += f"**{r['ticker']}** | Score: **{r['score']}**\n"
                body += f"売上増: {r['yoy']:.1%} (加速: {r['accel']:.1%})\n"
                body += f"利益率改善: {r['margin_boost']:.1%} {boost} | 出来高質: {r['acc_dist']:.1f}\n"
                body += f"高値乖離: -{r['dist_high']:.1%} | 時価総額: {r['mcap']/1e8:.1f}億ドル\n\n"

        footer = f"
