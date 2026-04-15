import os
import requests
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# =========================
# ENV
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

# =========================
# CONFIG
# =========================
MAX_WORKERS = 3  # yfinanceのブロックを避けるため少し下げる
PHASE2_LIMIT = 500
FINAL_LIMIT = 150 # 確実性を高めるため、一度に追う数を絞る

MIN_REVENUE = 1_000_000 # 10Mから1Mへ緩和（データの単位ミス対策）
MIN_MCAP = 50_000_000  # 緩和
MAX_MCAP = 5_000_000_000
MIN_YOY = -0.1 # 一旦マイナスも許容して「動くか」を確認

# =========================
# TICKERS (NASDAQリスト取得)
# =========================
def get_tickers():
    try:
        url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
        df = pd.read_csv(url)
        return df["Symbol"].dropna().tolist()
    except:
        return ["AAPL", "TSLA", "NVDA", "MSFT", "AMD", "GOOGL", "META"] # 失敗時のバックアップ

# =========================
# Phase1（軽い価格チェック）
# =========================
def pre_filter(ticker):
    try:
        t = yf.Ticker(ticker)
        # 5日分取得
        hist = t.history(period="7d")
        if hist.empty or len(hist) < 2: return None

        price = hist["Close"].iloc[-1]
        vol = hist["Volume"].mean()

        if price < 1 or vol < 50_000: return None # 出来高条件を少し緩和

        mom = (price - hist["Close"].iloc[0]) / hist["Close"].iloc[0]
        return {"ticker": ticker, "mom": mom, "price": price}
    except:
        return None

# =========================
# Phase3（財務データ取得：ここが鬼門）
# =========================
def fetch_data(ticker):
    try:
        t = yf.Ticker(ticker)
        
        # 1. 財務諸表の取得
        fin = t.quarterly_financials
        if fin is None or fin.empty:
            return None

        # インデックス名を正規化（大文字小文字スペースを無視）
        fin.index = fin.index.str.replace(' ', '').str.upper()
        target_label = "TOTALREVENUE"

        if target_label not in fin.index:
            # 別のラベルを探す（Operating Revenueなど）
            alt_labels = ["OPERATINGREVENUE", "TOTALOPERATINGREVENUE"]
            for alt in alt_labels:
                if alt in fin.index:
                    target_label = alt
                    break
            else:
                return None

        rev = fin.loc[target_label].dropna().values
        if len(rev) < 2: return None # 2四半期あればYoY計算可能とする

        r0 = rev[0] # 直近
        r1 = rev[1] if len(rev) > 1 else r0
        r2 = rev[2] if len(rev) > 2 else r1

        yoy = (r0 - r2) / r2 if len(rev) > 2 and r2 > 0 else (r0 - r1) / r1

        # 2. 基本情報（時価総額）
        # t.info は重いので、fast_info を使用（yfの最新版で推奨）
        try:
            mcap = t.fast_info.market_cap
        except:
            mcap = 0 # 取れない場合は後でフィルタ

        # 3. モメンタム再計算（3ヶ月）
        hist = t.history(period="3mo")
        if hist.empty:
            momentum = 0
            vol_trend = 1
        else:
            momentum = (hist["Close"].iloc[-1] - hist["Close"].iloc[0]) / hist["Close"].iloc[0]
            vol_now = hist["Volume"].tail(5).mean()
            vol_prev = hist["Volume"].head(5).mean()
            vol_trend = vol_now / (vol_prev + 1)

        return {
            "ticker": ticker,
            "yoy": yoy,
            "accel": (r0/r1) - (r1/r2) if r1>0 and r2>0 else 0,
            "momentum": momentum,
            "vol_trend": vol_trend,
            "mcap": mcap
        }
    except Exception as e:
        # print(f"Error {ticker}: {e}") # デバッグ用
        return None

# =========================
# SCORE
# =========================
def score(d):
    s = 0
    if d["yoy"] > 0.3: s += 5
    elif d["yoy"] > 0.1: s += 3
    
    if d["momentum"] > 0.2: s += 4
    elif d["momentum"] > 0: s += 1
    
    if d["vol_trend"] > 1.2: s += 2
    return s

# =========================
# NOTIFY
# =========================
def notify(df, stats):
    header = "🚀 **GrowthRadar v8.5 (Robust)**\n"
    if df.empty:
        msg = header + "⚠️ No candidates found. Financial data might be restricted by Yahoo."
    else:
        msg = header
        for _, r in df.iterrows():
            msg += (
                f"**{r['ticker']}** | Score:{r['score']}\n"
                f"YoY:{r['yoy']:.1%}, Mom:{r['momentum']:.1%}, Vol:{r['vol_trend']:.1%}\n\n"
            )

    msg += (
        "```"
        f"Phase1: {stats['phase1']}\n"
        f"Checked: {stats['checked']}\n"
        f"Valid: {stats['valid']}\n"
        "```"
    )

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": msg})
    else:
        print(msg)

# =========================
# MAIN
# =========================
def main():
    start_time = time.time()
    stats = {"phase1": 0, "checked": 0, "valid": 0}
    
    all_tickers = get_tickers()
    # 処理時間を考慮し、最初は全件ではなくシャッフルして一部を狙う
    import random
    random.shuffle(all_tickers)
    tickers_to_scan = all_tickers[:1000] 

    # Phase 1: Price/Vol Filter
    phase1_results = []
    print("Phase 1 Scanning...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(pre_filter, t): t for t in tickers_to_scan}
        for f in as_completed(futures):
            res = f.result()
            if res: phase1_results.append(res)
    
    stats["phase1"] = len(phase1_results)
    
    # Phase 2: Sort by Momentum
    phase1_sorted = sorted(phase1_results, key=lambda x: x["mom"], reverse=True)
    tickers_final = [x["ticker"] for x in phase1_sorted[:FINAL_LIMIT]]

    # Phase 3: Deep Fetch
    results = []
    print(f"Phase 3 Checking {len(tickers_final)} tickers...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_data, t): t for t in tickers_final}
        for f in as_completed(futures):
            stats["checked"] += 1
            res = f.result()
            if res:
                stats["valid"] += 1
                res["score"] = score(res)
                results.append(res)

    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values("score", ascending=False).head(10)

    notify(df, stats)
    print(f"Total time: {time.time() - start_time:.1f}s")

if __name__ == "__main__":
    main()
