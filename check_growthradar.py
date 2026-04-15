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
# UNIVERSE
# =========================
def get_tickers():
    url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
    df = pd.read_csv(url)

    tickers = df["Symbol"].dropna().tolist()

    print(f"Tickers loaded: {len(tickers)}")
    return tickers


# =========================
# FETCH (FMP fundamentals)
# =========================
def fetch(ticker):
    try:
        url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}"
        r = requests.get(url, timeout=10).json()

        if not isinstance(r, list) or not r:
            return None

        p = r[0]

        return {
            "ticker": ticker,
            "mcap": p.get("mktCap"),
            "price": p.get("price"),
            "sector": p.get("sector")
        }

    except:
        return None


# =========================
# EXTRA FUNDAMENTALS
# =========================
def fetch_growth(ticker):
    try:
        url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=2&apikey={FMP_API_KEY}"
        r = requests.get(url, timeout=10).json()

        if not isinstance(r, list) or len(r) < 2:
            return {}

        latest = r[0]
        prev = r[1]

        rev_growth = None
        if prev.get("revenue") and latest.get("revenue"):
            rev_growth = (latest["revenue"] - prev["revenue"]) / abs(prev["revenue"])

        return {
            "revenue_growth": rev_growth
        }

    except:
        return {}


# =========================
# SCORE v3（テンバガー型）
# =========================
def score(d):
    s = 0

    mcap = d.get("mcap", 0)

    # SIZE
    if mcap < 300_000_000:
        s += 6
    elif mcap < 1_000_000_000:
        s += 5
    elif mcap < 5_000_000_000:
        s += 2
    else:
        s += 0

    # GROWTH
    rev = d.get("revenue_growth")

    if rev is not None:
        if rev > 0.5:
            s += 6
        elif rev > 0.3:
            s += 5
        elif rev > 0.2:
            s += 3
        elif rev > 0.1:
            s += 1

    # MOMENTUM
    price = d.get("price", 0)
    if price > 50:
        s += 2
    elif price > 20:
        s += 1

    # SECTOR
    if d.get("sector") in ["Technology", "Healthcare", "Communication Services"]:
        s += 1

    return s


# =========================
# DISCORD
# =========================
def notify(df, total, processed, valid):
    if not WEBHOOK_URL:
        print(df)
        return

    if df.empty:
        msg = "⚠️ GrowthRadar v3: No candidates\n\n"
    else:
        msg = "🚀 GrowthRadar v3 (Tenbagger Mode)\n\n"
        for _, r in df.iterrows():
            msg += (
                f"{r['ticker']} | Score:{r['score']}\n"
                f"MCap:{r['mcap_b']}B | Sector:{r['sector']}\n"
                f"RevGrowth:{r.get('revenue_growth', None)}\n\n"
            )

    msg += (
        "--------------------\n"
        f"Total: {total}\n"
        f"Processed: {processed}\n"
        f"Valid: {valid}\n"
    )

    requests.post(WEBHOOK_URL, json={"content": msg}, timeout=10)


# =========================
# MAIN
# =========================
def main():
    tickers = get_tickers()

    total = len(tickers)
    processed = 0
    valid = 0

    results = []

    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = [ex.submit(fetch, t) for t in tickers]

        for f in as_completed(futures):
            processed += 1

            if processed % 500 == 0:
                print(f"Processed: {processed}/{total}")

            base = f.result()
            if not base:
                continue

            growth = fetch_growth(base["ticker"])
            base.update(growth)

            valid += 1

            base["score"] = score(base)
            base["mcap_b"] = round(base.get("mcap", 0) / 1e9, 2)

            results.append(base)

    df = pd.DataFrame(results)

    if not df.empty:
        df = df.sort_values("score", ascending=False).head(20)

    notify(df, total, processed, valid)


if __name__ == "__main__":
    main()
