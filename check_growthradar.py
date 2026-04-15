import os
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# 環境変数
# =========================
FMP_API_KEY = os.environ.get("FMP_API_KEY")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

if not FMP_API_KEY:
    raise ValueError("FMP_API_KEY is missing")

# =========================
# 設定
# =========================
MIN_MARKET_CAP = 300_000_000
MIN_GROWTH = 0.25
MIN_GROSS_MARGIN = 0.40

TOP_N = 20
MAX_WORKERS = 5  # 無料プラン対策

# =========================
# 母集団取得（安全版）
# =========================
def get_tickers():
    url = f"https://financialmodelingprep.com/api/v3/stock/list?apikey={FMP_API_KEY}"
    
    res = requests.get(url, timeout=10)
    print("STATUS:", res.status_code)

    try:
        data = res.json()
    except Exception as e:
        print("JSON decode error:", e)
        print("RAW:", res.text[:200])
        return []

    # 型チェック（最重要）
    if not isinstance(data, list):
        print("API ERROR RESPONSE:", data)
        return []

    tickers = []
    for d in data:
        if not isinstance(d, dict):
            continue
        if d.get("exchangeShortName") == "NASDAQ":
            symbol = d.get("symbol")
            if symbol:
                tickers.append(symbol)

    print(f"Tickers fetched: {len(tickers)}")
    return tickers

# =========================
# 個別データ取得（耐障害）
# =========================
def fetch_data(ticker):
    try:
        url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=3&apikey={FMP_API_KEY}"
        res = requests.get(url, timeout=10)
        fin = res.json()

        if not isinstance(fin, list) or len(fin) < 3:
            return None

        rev_latest = fin[0].get("revenue")
        rev_prev = fin[1].get("revenue")
        rev_3y = fin[2].get("revenue")

        if not all([rev_latest, rev_prev, rev_3y]):
            return None

        yoy = (rev_latest - rev_prev) / rev_prev
        cagr = (rev_latest / rev_3y) ** (1/2) - 1

        url2 = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
        res2 = requests.get(url2, timeout=10)
        prof = res2.json()

        if not isinstance(prof, list) or not prof:
            return None

        gross_margin = prof[0].get("grossProfitMargin")
        market_cap = prof[0].get("mktCap")

        if gross_margin is None or market_cap is None:
            return None

        return {
            "ticker": ticker,
            "yoy": yoy,
            "cagr": cagr,
            "gross_margin": gross_margin,
            "market_cap": market_cap
        }

    except Exception:
        return None

# =========================
# フィルタ
# =========================
def pass_filter(d):
    if d["market_cap"] < MIN_MARKET_CAP:
        return False
    if d["yoy"] < MIN_GROWTH:
        return False
    if d["gross_margin"] < MIN_GROSS_MARGIN:
        return False
    return True

# =========================
# スコア
# =========================
def score(d):
    s = 0

    if d["yoy"] > 0.60:
        s += 5
    elif d["yoy"] > 0.40:
        s += 4
    elif d["yoy"] > 0.25:
        s += 3

    if d["cagr"] > 0.40:
        s += 2
    elif d["cagr"] > 0.25:
        s += 1

    if d["gross_margin"] > 0.70:
        s += 3
    elif d["gross_margin"] > 0.50:
        s += 2
    elif d["gross_margin"] > 0.40:
        s += 1

    return s

# =========================
# Discord通知
# =========================
def send_discord(df):
    if not WEBHOOK_URL:
        print("No webhook set")
        return

    if df.empty:
        msg = "No candidates found"
    else:
        msg = "🚀 GrowthRadar TOP\n\n"
        for _, row in df.iterrows():
            msg += (
                f"{row['Ticker']} | Score:{row['Score']}\n"
                f"YoY:{row['YoY%']}% CAGR:{row['CAGR%']}%\n\n"
            )

    requests.post(WEBHOOK_URL, json={"content": msg})

# =========================
# メイン
# =========================
def main():
    tickers = get_tickers()

    if not tickers:
        print("No tickers fetched. Exiting.")
        return

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_data, t) for t in tickers]

        for f in as_completed(futures):
            d = f.result()
            if d is None:
                continue

            if not pass_filter(d):
                continue

            s = score(d)

            results.append({
                "Ticker": d["ticker"],
                "YoY%": round(d["yoy"] * 100, 1),
                "CAGR%": round(d["cagr"] * 100, 1),
                "GrossMargin%": round(d["gross_margin"] * 100, 1),
                "Score": s,
                "MarketCap(B)": round(d["market_cap"] / 1e9, 2)
            })

    df = pd.DataFrame(results)

    if df.empty:
        print("No candidates")
        send_discord(df)
        return

    df = df.sort_values(by="Score", ascending=False).head(TOP_N)

    print(df)
    df.to_csv("growthradar_top.csv", index=False)

    send_discord(df)

# =========================
if __name__ == "__main__":
    main()
