import os
import requests
import pandas as pd

# =========================
# ENV
# =========================
FMP_API_KEY = os.environ.get("FMP_API_KEY")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

if not FMP_API_KEY:
    raise ValueError("FMP_API_KEY missing")


# =========================
# SAFE REQUEST
# =========================
def safe_get(url, params=None):
    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()

        if not isinstance(data, list):
            print("API ERROR:", data)
            return None

        return data
    except Exception as e:
        print("REQUEST ERROR:", e)
        return None


# =========================
# TICKERS
# =========================
def get_tickers():
    url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
    df = pd.read_csv(url)

    tickers = df["Symbol"].dropna().tolist()
    print(f"Tickers loaded: {len(tickers)}")

    return tickers


# =========================
# BASE
# =========================
def fetch_base(ticker):
    try:
        url = f"https://financialmodelingprep.com/api/v3/profile/{ticker}"
        params = {"apikey": FMP_API_KEY}

        data = safe_get(url, params)
        if not data:
            return None

        d = data[0]

        mcap = d.get("mktCap")
        price = d.get("price")
        sector = d.get("sector")

        if not mcap or not price:
            return None

        if mcap > 10_000_000_000:
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
# GROWTH
# =========================
def fetch_growth(ticker):
    try:
        url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}"
        params = {"limit": 2, "apikey": FMP_API_KEY}

        data = safe_get(url, params)

        if not data or len(data) < 2:
            return None

        rev1 = data[0].get("revenue", 0)
        rev0 = data[1].get("revenue", 0)

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

    if d["mcap"] < 300_000_000:
        s += 5
    elif d["mcap"] < 1_000_000_000:
        s += 3
    else:
        s += 1

    rev = d.get("revenue_growth")
    if rev is not None:
        if rev > 0.4:
            s += 5
        elif rev > 0.2:
            s += 3
        elif rev > 0.05:
            s += 1

    if d["price"] > 10:
        s += 1

    if d.get("sector") in ["Technology", "Healthcare"]:
        s += 1

    return s


# =========================
# SUCCESS NOTIFY
# =========================
def notify(df, stats):
    if not WEBHOOK_URL:
        print(df)
        return

    msg = "🚀 GrowthRadar v5.7\n\n"

    if df.empty:
        msg += "No candidates\n\n"
    else:
        for _, r in df.iterrows():
            msg += (
                f"{r['ticker']} | Score:{r['score']}\n"
                f"MCap:{round(r['mcap']/1e9,2)}B "
                f"| Rev:{r.get('revenue_growth',0):.2f}\n\n"
            )

    msg += (
        "--- Stats ---\n"
        f"Checked: {stats['checked']}\n"
        f"Base OK: {stats['base_ok']}\n"
        f"Growth OK: {stats['growth_ok']}\n"
    )

    requests.post(WEBHOOK_URL, json={"content": msg}, timeout=10)


# =========================
# ERROR NOTIFY
# =========================
def notify_error(e, stats):
    if not WEBHOOK_URL:
        print("ERROR:", e)
        return

    msg = (
        "🔥 GrowthRadar ERROR\n\n"
        f"{str(e)}\n\n"
        "--- Stats ---\n"
        f"Checked: {stats['checked']}\n"
        f"Base OK: {stats['base_ok']}\n"
        f"Growth OK: {stats['growth_ok']}\n"
    )

    try:
        requests.post(WEBHOOK_URL, json={"content": msg}, timeout=10)
    except:
        print("Discord送信すら失敗")


# =========================
# MAIN
# =========================
def main(stats):
    tickers = get_tickers()

    results = []

    for t in tickers[:300]:
        stats["checked"] += 1

        base = fetch_base(t)
        if not base:
            continue

        stats["base_ok"] += 1

        g = fetch_growth(t)
        if g is not None:
            base["revenue_growth"] = g
            stats["growth_ok"] += 1

        base["score"] = score(base)
        results.append(base)

        if stats["checked"] % 50 == 0:
            print(f"Processed: {stats['checked']}")

    print(f"Base通過: {stats['base_ok']}")
    print(f"Growth取得: {stats['growth_ok']}")

    df = pd.DataFrame(results)

    if not df.empty:
        df = df.sort_values("score", ascending=False).head(20)

    notify(df, stats)


# =========================
# WRAPPER（最重要）
# =========================
def main_wrapper():
    stats = {"checked": 0, "base_ok": 0, "growth_ok": 0}

    try:
        main(stats)
    except Exception as e:
        print("FATAL:", e)
        notify_error(e, stats)


# =========================
# ENTRY
# =========================
if __name__ == "__main__":
    main_wrapper()
