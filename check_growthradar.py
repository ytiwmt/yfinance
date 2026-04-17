import os
import requests
import pandas as pd
import numpy as np
import random
import re
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# =========================
# CONFIG
# =========================
WEBHOOK_URL = os.environ.get("WEBHOOK_URL_GROWTHRADAR")

MAX_WORKERS = 10
SCAN_SIZE = 2000

STATE_FILE = "state_v2671.json"

MIN_PRICE = 2.0
MIN_MCAP = 5e7
MIN_AVG_VOL_VAL = 5e5

HEADERS = {"User-Agent": "Mozilla/5.0"}

# =========================
# STATE
# =========================
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

# =========================
class GrowthRadarV26_7_1:

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # =========================
    def load_universe(self):
        symbols = []
        sources = [
            "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all_tickers.txt",
            "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv",
        ]

        for url in sources:
            try:
                r = self.session.get(url, timeout=10)
                if r.status_code == 200:
                    found = r.text.split("\n") if url.endswith(".txt") else pd.read_csv(url)["Symbol"].tolist()
                    symbols.extend(found)
            except:
                pass

        clean = list(set([
            str(s).strip().upper()
            for s in symbols
            if isinstance(s, str) and re.match(r"^[A-Z]{1,5}$", str(s).strip())
        ]))

        random.shuffle(clean)
        return clean[:SCAN_SIZE]

    # =========================
    def fetch(self, ticker):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1y&interval=1d"
            r = self.session.get(url, timeout=6).json()
            res = r["chart"]["result"][0]

            close = [c for c in res["indicators"]["quote"][0]["close"] if c]
            volume = [v for v in res["indicators"]["quote"][0]["volume"] if v]

            if len(close) < 126:
                return None

            price = close[-1]
            if price < MIN_PRICE:
                return None

            avg_vol_val = np.mean(close[-21:]) * np.mean(volume[-21:])
            if np.isnan(avg_vol_val) or avg_vol_val < MIN_AVG_VOL_VAL:
                return None

            m1 = price / close[-21] - 1
            m3 = price / close[-63] - 1
            m6 = price / close[-126] - 1

            if np.isnan(m6):
                return None

            if m6 < 0.3:
                return None

            trend = np.mean(close[-10:]) / (np.mean(close[-30:-10]) + 1e-9) - 1

            return {
                "ticker": ticker,
                "price": price,
                "m6": m6,
                "accel": m1 - m3,
                "trend": trend,
                "vol_short": np.mean(volume[-5:]),
                "vol_mid": np.mean(volume[-21:]),
                "vol_long": np.mean(volume[-63:])
            }

        except:
            return None

    # =========================
    def score(self, df):
        df["vol_ratio"] = df["vol_short"] / (df["vol_mid"] + 1e-9)

        df["score"] = (
            df["m6"].rank(pct=True) * 0.40 +
            df["accel"].rank(pct=True) * 0.20 +
            df["trend"].rank(pct=True) * 0.25 +
            df["vol_ratio"].rank(pct=True) * 0.15
        )

        return df

    # =========================
    def update_state(self, df, state):
        now = datetime.now().strftime("%Y-%m-%d")

        for _, r in df.iterrows():
            t = r["ticker"]

            if t not in state:
                state[t] = {"history": []}

            state[t]["history"].append({
                "date": now,
                "score": float(r["score"]),
                "trend": float(r["trend"]),
                "m6": float(r["m6"])
            })

            state[t]["history"] = state[t]["history"][-10:]

        return state

    # =========================
    def build_state_view(self, state):
        rows = []

        for t, s in state.items():
            hist = s.get("history", [])

            if len(hist) == 0:
                continue

            scores = [h.get("score", 0) for h in hist]

            if len(scores) == 0:
                continue

            state_score = np.mean(scores) * 0.7 + np.max(scores) * 0.3

            momentum = scores[-1] - scores[0] if len(scores) > 1 else 0
            stability = 1 - (np.std(scores) if len(scores) > 1 else 0)

            rows.append({
                "ticker": t,
                "state_score": state_score,
                "momentum": momentum,
                "stability": stability
            })

        if len(rows) == 0:
            return pd.DataFrame(columns=["ticker", "state_score", "momentum", "stability"])

        return pd.DataFrame(rows)

    # =========================
    def run(self):
        universe = self.load_universe()
        batch = universe[:SCAN_SIZE]

        raw = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(self.fetch, t): t for t in batch}
            for f in as_completed(futures):
                r = f.result()
                if r:
                    raw.append(r)

        if len(raw) == 0:
            print("NO DATA")
            return

        df = self.score(pd.DataFrame(raw))

        # =========================
        # STATE
        # =========================
        state = load_state()
        state = self.update_state(df, state)
        save_state(state)

        state_df = self.build_state_view(state)

        # =========================
        # LIVE
        # =========================
        live = df.sort_values("score", ascending=False)

        live_t1 = live[live["score"] > 0.80]
        live_t2 = live[(live["score"] <= 0.80) & (live["score"] > 0.60)]

        # =========================
        # STATE
        # =========================
        state_top = state_df.sort_values("state_score", ascending=False)

        # =========================
        # MOMENTUM
        # =========================
        mom_top = state_df.sort_values("momentum", ascending=False)

        # =========================
        # REPORT
        # =========================
        now = datetime.now().strftime("%Y-%m-%d %H:%M")

        msg = [
            "🚀 GrowthRadar v26.7.1 (Stable)",
            f"Live:{len(live)} State:{len(state_df)} {now}\n"
        ]

        msg.append("🔥 LIVE Tier1")
        for r in live_t1.head(8).to_dict("records"):
            msg.append(f"{r['ticker']} S:{r['score']:.2f}")

        msg.append("\n⚡ STATE")
        for r in state_top.head(8).to_dict("records"):
            msg.append(f"{r['ticker']} S:{r['state_score']:.2f}")

        msg.append("\n🚀 MOMENTUM")
        for r in mom_top.head(8).to_dict("records"):
            msg.append(f"{r['ticker']} Δ:{r['momentum']:.2f}")

        print("\n".join(msg))


if __name__ == "__main__":
    GrowthRadarV26_7_1().run()
