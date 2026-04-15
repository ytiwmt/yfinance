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
# STEP1: SCREENER（1 call）
# =========================
def get_candidates():
    url = "https://financialmodelingprep.com/api/v3/stock-screener"

    params = {
        "marketCapLowerThan": 3_000_000_000,
        "priceMoreThan": 3,
        "exchange": "NASDAQ",
        "limit": 1000,
        "apikey": FMP_API_KEY
    }

    r = requests.get(url, params=params, timeout=10).json()

    print(f"Screener fetched: {len(r)}")

    return r


# =========================
# STEP2: 上位抽出（構造圧縮）
# =========================
def select_top(candidates):
    df = pd.DataFrame(candidates)

    # 安全処理
    df = df.dropna(subset=["marketCap", "price"])

    # 小型優先でソート
    df = df.sort_values("marketCap")

    # 上位200だけ
    df = df.head(200)

    print(f"Selected for growth scan: {len(df)}")

    return df.to_dict("records")


# =========================
# STEP3: GROWTH取得（最大200 calls）
# =========================
def fetch_growth(ticker):
    try:
        url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}"
        params = {
            "limit": 2,
            "apikey": FMP_API_KEY
        }

        r = requests.get(url, params=params, timeout=5).json()

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
# SCORE v5（実戦）
# =========================
def score(d):
    s = 0

    mcap = d["marketCap"]
    price = d["price"]

    # SIZE
    if mcap < 200_000_000:
        s += 7
    elif mcap < 500_000_000:
        s += 6
    elif mcap < 1_000_000_000:
        s += 4
    else:
        s += 2

    # GROWTH（主軸）
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

    # MOMENTUM proxy
    if price > 20:
        s += 2

    # SECTOR
    if d.get("sector") in ["Technology", "Healthcare", "Communication Services"]:
        s += 2

    return s


# =========================
# DISCORD
# =========================
def notify(df, stats):
    if not WEBHOOK_URL:
        print(df)
        return

    if df.empty:
        msg = "⚠️ GrowthRadar v5: No candidates"
    else:
        msg = "🚀 GrowthRadar v5 (Rate-Limit Safe)\n\n"

        for _, r in df.iterrows():
            msg += (
                f"{r['symbol']} | Score:{r['score']}\n"
                f"MCap:{round(r['marketCap']/1e9,2)}B | Rev:{r.get('revenue_growth',0):.2f}\n\n"
            )

    msg += (
        "\n--- Stats ---\n"
        f"Screener: {stats['screener']}\n"
        f"Selected: {stats['selected']}\n"
        f"Growth: {stats['growth']}\n"
    )

    requests.post(WEBHOOK_URL, json={"content": msg}, timeout=10)


# =========================
# MAIN
# =========================
def main():
    stats = {"screener": 0, "selected": 0, "growth": 0}

    # ① Screener
    candidates = get_candidates()
    stats["screener"] = len(candidates)

    # ② Select
    selected = select_top(candidates)
    stats["selected"] = len(selected)

    results = []

    # ③ Growth
    for d in selected:
        g = fetch_growth(d["symbol"])
        if g is not None:
            d["revenue_growth"] = g
            stats["growth"] += 1

        d["score"] = score(d)
        results.append(d)

    df = pd.DataFrame(results)

    if not df.empty:
        df = df[df["score"] >= 6].sort_values("score", ascending=False).head(15)

    notify(df, stats)


if __name__ == "__main__":
    main()
