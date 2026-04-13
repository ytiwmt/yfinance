import yfinance as yf
import pandas as pd
import requests
import json
import os

webhook_url_yfinance = os.getenv("WEBHOOK_URL_YFINANCE")

def get_sp500_tickers():
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers)
        tables = pd.read_html(response.text)
        return [t.replace('.', '-') for t in tables[0]['Symbol'].tolist()]
    except:
        return []

def analyze_market():
    if not webhook_url_yfinance: return

    tickers = get_sp500_tickers()
    found_opportunities = []

    for symbol in tickers:
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            price = info.get('currentPrice') or info.get('regularMarketPrice') or info.get('previousClose')
            div_rate = info.get('trailingAnnualDividendRate') or info.get('dividendRate')
            
            if not price or not div_rate or div_rate <= 0: continue
                
            cur_yield = (div_rate / price) * 100
            payout = info.get('payoutRatio', 0) * 100
            
            if cur_yield < 3.5 or payout > 80 or payout <= 0: continue

            hist = stock.history(period="2y")
            avg_yield_2y = (div_rate / hist['Close'].mean()) * 100
            
            if cur_yield > (avg_yield_2y * 1.2):
                found_opportunities.append({
                    "Symbol": symbol,
                    "Yield": f"{cur_yield:.2f}%",
                    "Avg": f"{avg_yield_2y:.2f}%",
                    "Payout": f"{payout:.1f}%",
                    "RawYield": cur_yield
                })
        except: continue

    top_deals = sorted(found_opportunities, key=lambda x: x['RawYield'], reverse=True)[:10]
    send_rich_notification(top_deals)

def send_rich_notification(deals):
    if not deals:
        payload = {"content": "📡 **市場パトロール完了**: 異常なし。"}
    else:
        embeds = []
        for d in deals:
            embeds.append({
                "title": f"🚀 {d['Symbol']} がバグ水準です",
                "color": 3066993, # 緑色
                "fields": [
                    {"name": "現在利回り", "value": d['Yield'], "inline": True},
                    {"name": "過去2年平均", "value": d['Avg'], "inline": True},
                    {"name": "配当性向", "value": d['Payout'], "inline": True}
                ],
            })
        
        # 1回に最大10個のEmbedを送れる
        payload = {
            "content": "⚠️ **【米国株・流動性バグ検知】** 以下の銘柄が歴史的割安水準にあります。",
            "embeds": embeds
        }
    
    requests.post(webhook_url_yfinance, data=json.dumps(payload), headers={"Content-Type": "application/json"})

if __name__ == "__main__":
    analyze_market()
