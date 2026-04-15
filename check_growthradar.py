import os
import requests
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# ENV
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

# =========================
# CONFIG
# =========================
MAX_WORKERS = 5

PHASE2_LIMIT = 2500
FINAL_LIMIT = 1000

MIN_REVENUE = 10_000_000
MIN_MCAP = 100_000_000
MAX_MCAP = 5_000_000_000

MIN_YOY = 0.03   # さらに緩和

# =========================
# TICKERS
# =========================
def get_tickers():
    url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
    df = pd.read_csv(url)
    return df["Symbol"].dropna().tolist()

# =========================
# Phase1（軽い）
# =========================
def pre_filter(ticker):
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="5d")

        if hist is None or hist.empty:
            return None

        price = hist["Close"].iloc[-1]
        vol = hist["Volume"].mean()

        if price < 1:
            return None

        if vol < 100_000:
            return None

        price_5d = hist["Close"].iloc[0]
        mom = (price - price_5d) / price_5d

        return {
            "ticker": ticker,
            "mom": mom
        }

    except:
        return None

# =========================
# Phase3（現実対応）
# =========================
def fetch_data(ticker):
    try:
        t = yf.Ticker(ticker)

        fin = t.quarterly_financials
        if fin is None or fin.empty or "Total Revenue" not in fin.index:
            return None

        rev = fin.loc["Total Revenue"].dropna().values
        if len(rev) < 3:
            return None

        r0, r1, r2 = rev[:3]

        if min(r0, r1, r2) <= 0:
            return None

        if r0 < MIN_REVENUE:
            return None

        yoy = (r0 - r2) / r2

        # 成長だけは最低限維持
        if yoy < MIN_YOY:
            return None

        # accelは取得だけ
        qoq_now = (r0 - r1) / r1
        qoq_prev = (r1 - r2) / r2
        accel = qoq_now - qoq_prev

        info = t.info
        mcap = info.get("marketCap", 0)

        if not mcap or mcap < MIN_MCAP or mcap > MAX_MCAP:
            return None

        hist = t.history(period="3mo")

        if hist is None or hist.empty or len(hist) < 20:
            momentum = 0
            vol_trend = 1
        else:
            price_now = hist["Close"].iloc[-1]
            price_3m = hist["Close"].iloc[0]
            momentum = (price_now - price_3m) / price_3m

            vol_now = hist["Volume"].tail(5).mean()
            vol_prev = hist["Volume"].head(5).mean()
            vol_trend = vol_now / vol_prev if vol_prev else 1

        return {
            "ticker": ticker,
            "yoy": yoy,
            "accel": accel,
            "momentum": momentum,
            "vol_trend": vol_trend,
            "mcap": mcap
        }

    except:
        return None

# =========================
# SCORE（現実版）
# =========================
def score(d):
    s = 0

    # 成長（重要度高）
    if d["yoy"] > 0.5: s += 5
    elif d["yoy"] > 0.3: s += 4
    elif d["yoy"] > 0.1: s += 3
    else: s += 2

    # accel（ボーナス扱い）
    if d["accel"] > 0.3: s += 3
    elif d["accel"] > 0: s += 1

    # モメンタム
    if d["momentum"] > 0.5: s += 4
    elif d["momentum"] > 0.2: s += 3
    elif d["momentum"] > 0: s += 1

    # 出来高
    if d["vol_trend"] > 1.5: s += 2
    elif d["vol_trend"] > 1.2: s += 1

    return s

# =========================
# NOTIFY
# =========================
def notify(df, stats):
    msg = "🚀 GrowthRadar v8.4 (Stable)\n\n"

    if df.empty:
        msg += "⚠️ No candidates even after fallback\n\n"

    for _, r in df.iterrows():
        msg += (
            f"{r['ticker']} | Score:{r['score']}\n"
            f"YoY:{r['yoy']:.2f} Accel:{r['accel']:.2f}\n"
            f"Mom:{r['momentum']:.2f} Vol:{r['vol_trend']:.2f}\n\n"
        )

    msg += (
        "--- Stats ---\n"
        f"Phase1: {stats['phase1']}\n"
        f"Phase2: {stats['phase2']}\n"
        f"Checked: {stats['checked']}\n"
        f"Valid: {stats['valid']}\n"
    )

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"content": msg}, timeout=10)
    else:
        print(msg)

# =========================
# MAIN
# =========================
def main():
    stats = {"phase1": 0, "phase2": 0, "checked": 0, "valid": 0}

    tickers = get_tickers()

    # ---------- Phase1 ----------
    phase1 = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(pre_filter, t): t for t in tickers}
        for f in as_completed(futures):
            res = f.result()
            if res:
                phase1.append(res)

    stats["phase1"] = len(phase1)

    # ---------- Phase2 ----------
    phase1_sorted = sorted(phase1, key=lambda x: x["mom"], reverse=True)
    phase2 = phase1_sorted[:PHASE2_LIMIT]

    stats["phase2"] = len(phase2)

    # ---------- Phase3 ----------
    tickers_final = [x["ticker"] for x in phase2[:FINAL_LIMIT]]

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_data, t): t for t in tickers_final}

        for f in as_completed(futures):
            stats["checked"] += 1

            res = f.result()
            if not res:
                continue

            stats["valid"] += 1
            res["score"] = score(res)
            results.append(res)

    df = pd.DataFrame(results)

    # fallback完全保証
    if df.empty and results:
        df = pd.DataFrame(results)

    if not df.empty:
        df = df.sort_values("score", ascending=False).head(15)

    notify(df, stats)

# =========================
# ENTRY
# =========================
if __name__ == "__main__":
    main()
