import os
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# ENV
# =========================
FMP_API_KEY = os.environ.get("FMP_API_KEY")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

if not FMP_API_KEY:
    raise ValueError("FMP_API_KEY missing")


# =========================
# RUSSELL 3000
# =========================
def get_tickers():
    url = "https://www.stockmarketmba.com/databases/Russell3000.csv"
    df = pd.read_csv(url)

    col = [c for c in df.columns if "ticker" in c.lower()][0]
    tickers = df[col].dropna().tolist()

    print(f"Tickers loaded: {len(tickers)}")
    return tickers


# =========================
# FETCH (small-cap tolerant)
# =========================
def fetch(ticker):
    try:
        url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
        r = requests.get(url, timeout=10).json()

        if not isinstance(r, list) or not r:
            return None

        mcap = r[0].get("mktCap")
        price = r[0].get("price")
        sector = r[0].get("sector")

        if not mcap or not price:
            return None

        return {
            "ticker": ticker,
            "mcap": mcap,
            "price": price,
            "sector": sector
        }

    except:
        return None


# =========================
# SCORE (テンバガー寄り）
# =========================
def score(d):
    s = 0

    # 小型ほど加点
    if d["mcap"] < 500_000_000:
        s += 5
    elif d["mcap"] < 1_000_000_000:
        s += 4
    elif d["mcap"] < 5_000_000_000:
        s += 2
    else:
        s += 1

    # セクター加点
    if d["sector"] in ["Technology", "Healthcare"]:
        s += 1

    return s


# =========================
# DISCORD
# =========================
def notify(df):
    if not WEBHOOK_URL:
        print("NO WEBHOOK")
        return

    if df.empty:
        msg = "⚠️ GrowthRadar v2: No candidates"
    else:
        msg = "🚀 GrowthRadar v2 (Russell 3000)\n\n"
        for _, r in df.iterrows():
            msg += (
                f"{r['ticker']} | Score:{r['score']}\n"
                f"MCap:{r['mcap_b']}B | Sector:{r['sector']}\n\n"
            )

    try:
        requests.post(WEBHOOK_URL, json={"content": msg}, timeout=10)
    except Exception as e:
        print("Webhook failed:", e)


# =========================
# MAIN
# =========================
def main():
    tickers = get_tickers()

    results = []

    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = [ex.submit(fetch, t) for t in tickers]

        for f in as_completed(futures):
            d = f.result()
            if not d:
                continue

            d["score"] = score(d)
            d["mcap_b"] = round(d["mcap"] / 1e9, 2)

            results.append(d)

    df = pd.DataFrame(results)

    if df.empty:
        notify(df)
        return

    df = df.sort_values("score", ascending=False).head(20)

    print(df)
    notify(df)


if __name__ == "__main__":
    main()
