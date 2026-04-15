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
    return df["Symbol"].dropna().tolist()


# =========================
# GROWTH + ACCELERATION
# =========================
def fetch_growth_accel(ticker):
    try:
        url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}"
        params = {"limit": 4, "apikey": FMP_API_KEY}

        data = safe_get(url, params)
        if not data or len(data) < 4:
            return None

        r0 = data[0].get("revenue", 0)
        r1 = data[1].get("revenue", 0)
        r2 = data[2].get("revenue", 0)
        r3 = data[3].get("revenue", 0)

        if min(r0, r1, r2, r3) <= 0:
            return None

        # YoY
        yoy = (r0 - r2) / r2

        # QoQ acceleration
        qoq_now = (r0 - r1) / r1
        qoq_prev = (r1 - r2) / r2
        accel = qoq_now - qoq_prev

        return {
            "yoy": yoy,
            "accel": accel
        }

    except:
        return None


# =========================
# SCORE v6（核心）
# =========================
def score_v6(d):
    s = 0

    yoy = d["yoy"]
    accel = d["accel"]

    # ① 成長
    if yoy > 0.5:
        s += 5
    elif yoy > 0.3:
        s += 4
    elif yoy > 0.15:
        s += 3
    elif yoy > 0.05:
        s += 1

    # ② 加速（最重要）
    if accel > 0.2:
        s += 6
    elif accel > 0.1:
        s += 4
    elif accel > 0.05:
        s += 2

    # ③ 最低ライン（ゴミ除去）
    if yoy < 0.05:
        s -= 3

    return s


# =========================
# NOTIFY
# =========================
def notify(df, stats):
    if not WEBHOOK_URL:
        print(df)
        return

    msg = "🚀 GrowthRadar v6 (Acceleration)\n\n"

    if df.empty:
        msg += "No candidates\n\n"
    else:
        for _, r in df.iterrows():
            msg += (
                f"{r['ticker']} | Score:{r['score']}\n"
                f"YoY:{r['yoy']:.2f} | Accel:{r['accel']:.2f}\n\n"
            )

    msg += (
        "--- Stats ---\n"
        f"Checked: {stats['checked']}\n"
        f"Valid: {stats['valid']}\n"
    )

    requests.post(WEBHOOK_URL, json={"content": msg}, timeout=10)


# =========================
# MAIN
# =========================
def main(stats):
    tickers = get_tickers()

    results = []

    for t in tickers[:300]:
        stats["checked"] += 1

        g = fetch_growth_accel(t)
        if not g:
            continue

        stats["valid"] += 1

        d = {
            "ticker": t,
            "yoy": g["yoy"],
            "accel": g["accel"]
        }

        d["score"] = score_v6(d)
        results.append(d)

        if stats["checked"] % 50 == 0:
            print(f"Processed: {stats['checked']}")

    df = pd.DataFrame(results)

    if not df.empty:
        df = df.sort_values("score", ascending=False).head(15)

    notify(df, stats)


# =========================
# ERROR NOTIFY
# =========================
def notify_error(e, stats):
    if not WEBHOOK_URL:
        print(e)
        return

    msg = (
        "🔥 GrowthRadar v6 ERROR\n\n"
        f"{str(e)}\n\n"
        f"Checked: {stats['checked']}\n"
    )

    try:
        requests.post(WEBHOOK_URL, json={"content": msg}, timeout=10)
    except:
        print("Discord failure")


# =========================
# WRAPPER
# =========================
def main_wrapper():
    stats = {"checked": 0, "valid": 0}

    try:
        main(stats)
    except Exception as e:
        notify_error(e, stats)


# =========================
# ENTRY
# =========================
if __name__ == "__main__":
    main_wrapper()
