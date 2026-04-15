import os
import requests
import pandas as pd

FMP_API_KEY = os.environ.get("FMP_API_KEY")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")


# =========================
# NASDAQリスト取得（安定）
# =========================
def get_tickers():
    url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
    df = pd.read_csv(url)

    tickers = df["Symbol"].dropna().tolist()
    print(f"Tickers loaded: {len(tickers)}")

    return tickers


# =========================
# BASE（yfinance代替）
# =========================
def fetch_base(ticker):
    try:
        url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
        r = requests.get(url, timeout=5).json()

        if not isinstance(r, list) or not r:
            return None

        d = r[0]

        mcap = d.get("mktCap")
        price = d.get("price")
        sector = d.get("sector")

        if not mcap or not price or not sector:
            return None

        if mcap > 3_000_000_000:
            return None

        if price < 3:
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
# GROWTH（FMP）
# =========================
def fetch_growth(ticker):
    try:
        url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=2&apikey={FMP_API_KEY}"
        r = requests.get(url, timeout=5).json()

        if not isinstance(r, list) or len(r) < 2:
            return None

        rev1 = r[0].get("revenue", 0)
        rev0 = r[1].get("revenue", 0)

        if rev0 <= 0:
            return None

        return (rev1 - rev0) / rev0

    except:
        return None


# =========================
# SCORE
# =========================
def score(d):
    s = 0

    if d["mcap"] < 200_000_000:
        s += 7
    elif d["mcap"] < 500_000_000:
        s += 5
    elif d["mcap"] < 1_000_000_000:
        s += 3

    rev = d.get("revenue_growth")
    if rev is not None:
        if rev > 0.5:
            s += 8
        elif rev > 0.3:
            s += 6
        elif rev > 0.15:
            s += 4
        elif rev > 0.05:
            s += 2

    if d["price"] > 20:
        s += 1

    if d["sector"] in ["Technology", "Healthcare"]:
        s += 2

    return s


# =========================
# MAIN
# =========================
def main():
    tickers = get_tickers()

    results = []
    count = 0

    for t in tickers[:300]:  # ★制限
        base = fetch_base(t)
        if not base:
            continue

        g = fetch_growth(t)
        if g is not None:
            base["revenue_growth"] = g

        base["score"] = score(base)
        results.append(base)

        count += 1

        if count % 50 == 0:
            print(f"Processed: {count}")

    df = pd.DataFrame(results)

    if not df.empty:
        df = df[df["score"] >= 6] \
            .sort_values("score", ascending=False) \
            .head(15)

    print(df)


if __name__ == "__main__":
    main()
