import yfinance as yf
import pandas as pd
import requests
import json
import os
import numpy as np

webhook_url_yfinance = os.getenv("WEBHOOK_URL_YFINANCE")

def get_sp500_tickers():
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers)
        return [t.replace('.', '-') for t in pd.read_html(response.text)[0]['Symbol'].tolist()]
    except:
        return []

def calc_yield_zscore(stock, div_rate):
    hist = stock.history(period="2y")
    if hist.empty:
        return None, None, None

    prices = hist['Close']
    yields = (div_rate / prices) * 100

    mean = yields.mean()
    std = yields.std()

    current_price = prices.iloc[-1]
    current_yield = (div_rate / current_price) * 100

    if std == 0:
        return None, None, None

    zscore = (current_yield - mean) / std
    return current_yield, mean, zscore

def get_fcf(stock):
    try:
        cf = stock.cashflow
        if cf is None or cf.empty:
            return None

        op_cf = cf.loc['Total Cash From Operating Activities'].iloc[0]
        capex = cf.loc['Capital Expenditures'].iloc[0]

        return op_cf + capex  # capexはマイナスなので加算
    except:
        return None

def analyze_market():
    if not webhook_url_yfinance:
        return

    tickers = get_sp500_tickers()
    found = []

    for symbol in tickers:
        try:
            stock = yf.Ticker(symbol)
            info = stock.info

            price = info.get('currentPrice') or info.get('regularMarketPrice')
            div_rate = info.get('trailingAnnualDividendRate') or info.get('dividendRate')

            if not price or not div_rate or div_rate <= 0:
                continue

            # ---------- Step1: 異常検知 ----------
            cur_yield, avg_yield, z = calc_yield_zscore(stock, div_rate)
            if z is None or z < 1.5:
                continue

            # ---------- Step2: 生存判定 ----------
            payout = info.get('payoutRatio')
            if payout and payout > 0.8:
                continue

            fcf = get_fcf(stock)
            shares = info.get('sharesOutstanding')

            if not fcf or not shares:
                continue

            total_div = div_rate * shares

            if fcf < total_div:
                continue

            # ---------- Step3: 財務健全性 ----------
            debt = info.get('totalDebt')
            ebitda = info.get('ebitda')

            if debt and ebitda and ebitda > 0:
                if debt / ebitda > 3:
                    continue

            found.append({
                "Symbol": symbol,
                "Yield": f"{cur_yield:.2f}%",
                "Avg": f"{avg_yield:.2f}%",
                "Z": f"{z:.2f}",
                "RawYield": cur_yield
            })

        except:
            continue

    top = sorted(found, key=lambda x: x['RawYield'], reverse=True)[:5]
    send_notification(top)

def send_notification(deals):
    if not deals:
        payload = {
            "content": "📡 バグ検知なし（条件を満たす銘柄なし）"
        }
    else:
        embeds = []
        for d in deals:
            embeds.append({
                "title": f"💎 バグ候補: {d['Symbol']}",
                "color": 3447003,
                "fields": [
                    {"name": "利回り", "value": d['Yield'], "inline": True},
                    {"name": "平均利回り", "value": d['Avg'], "inline": True},
                    {"name": "Zスコア", "value": d['Z'], "inline": True}
                ]
            })

        payload = {
            "content": "✅ 実務フィルタ通過銘柄",
            "embeds": embeds
        }

    requests.post(webhook_url_yfinance, data=json.dumps(payload),
                  headers={"Content-Type": "application/json"})

if __name__ == "__main__":
    analyze_market()
