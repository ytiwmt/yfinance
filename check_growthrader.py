import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# 設定
# =========================
FMP_API_KEY = "YOUR_API_KEY"
DISCORD_WEBHOOK_URL = "WEBHOOK_URL_GrowthRader"

MIN_MARKET_CAP = 300_000_000
MIN_GROWTH = 0.25
MIN_GROSS_MARGIN = 0.40

TOP_N = 20
MAX_WORKERS = 10  # 並列数（10〜20推奨）

# =========================
# 母集団取得（NASDAQ）
# =========================
def get_tickers():
    url = f"https://financialmodelingprep.com/api/v3/stock/list?apikey={FMP_API_KEY}"
    data = requests.get(url).json()

    # NASDAQだけに絞る
    tickers = [d["symbol"] for d in data if d["exchangeShortName"] == "NASDAQ"]
    return tickers

# =========================
# 個別データ取得
# =========================
def fetch_data(ticker):
    try:
        # financials
        url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=3&apikey={FMP_API_KEY}"
        fin = requests.get(url).json()

        if len(fin) < 3:
            return None

        rev_latest = fin[0]["revenue"]
        rev_prev = fin[1]["revenue"]
        rev_3y = fin[2]["revenue"]

        if rev_prev == 0 or rev_3y == 0:
            return None

        yoy = (rev_latest - rev_prev) / rev_prev
        cagr = (rev_latest / rev_3y) ** (1/2) - 1

        # profile
        url2 = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
        prof = requests.get(url2).json()

        if not prof:
            return None

        gross_margin = prof[0].get("grossProfitMargin")
        market_cap = prof[0].get("mktCap")

        return {
            "ticker": ticker,
            "yoy": yoy,
            "cagr": cagr,
            "gross_margin": gross_margin,
            "market_cap": market_cap
        }

    except:
        return None

# =========================
# フィルタ
# =========================
def pass_filter(d):
    if d["market_cap"] is None or d["market_cap"] < MIN_MARKET_CAP:
        return False
    if d["yoy"] < MIN_GROWTH:
        return False
    if d["gross_margin"] is None or d["gross_margin"] < MIN_GROSS_MARGIN:
        return False
    return True

# =========================
# スコア
# =========================
def score(d):
    s = 0

    # Growth
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

    # Profitability
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
    if df.empty:
        return

    msg = "🚀 テンバガー候補 TOP\n\n"

    for _, row in df.iterrows():
        msg += (
            f"{row['Ticker']} | Score:{row['Score']}\n"
            f"YoY:{row['YoY%']}% CAGR:{row['CAGR%']}%\n\n"
        )

    requests.post(DISCORD_WEBHOOK_URL, json={"content": msg})

# =========================
# メイン
# =========================
def main():
    tickers = get_tickers()
    print(f"Tickers: {len(tickers)}")

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
        return

    df = df.sort_values(by="Score", ascending=False).head(TOP_N)

    print(df)

    # 保存
    df.to_csv("tenbagger_top.csv", index=False)

    # Discord
    send_discord(df)

if __name__ == "__main__":
    main()
