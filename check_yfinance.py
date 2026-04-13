import yfinance as yf
import pandas as pd
import requests
import json
import os

# GitHub Secretsから取得
webhook_url_yfinance = os.getenv("WEBHOOK_URL_YFINANCE")

def get_sp500_tickers():
    """WikipediaからS&P500銘柄を403回避しつつ取得"""
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        tables = pd.read_html(response.text)
        return [t.replace('.', '-') for t in tables[0]['Symbol'].tolist()]
    except Exception as e:
        print(f"List acquisition error: {e}")
        return []

def analyze_market():
    if not webhook_url_yfinance:
        print("Error: WEBHOOK_URL_YFINANCE is not set.")
        return

    tickers = get_sp500_tickers()
    found_opportunities = []
    print(f"Scanning {len(tickers)} stocks...")

    for symbol in tickers:
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            
            # --- 確実な利回り計算ロジック ---
            # 1. 現在の株価
            current_price = info.get('currentPrice') or info.get('regularMarketPrice') or info.get('previousClose')
            # 2. 年間配当額（ドル）
            div_rate = info.get('trailingAnnualDividendRate') or info.get('dividendRate')
            
            if not current_price or not div_rate or div_rate <= 0:
                continue
                
            # 自前で利回りを計算（確実）
            cur_yield = (div_rate / current_price) * 100
            
            # 配当性向
            payout = info.get('payoutRatio', 0) * 100
            
            # フィルタ：利回り3.5%以上、配当性向80%以下
            if cur_yield < 3.5 or payout > 80 or payout <= 0:
                continue

            # 過去2年の平均利回り（ヒストリカル・データから算出）
            hist = stock.history(period="2y")
            if hist.empty:
                continue
            
            avg_price_2y = hist['Close'].mean()
            # 過去2年の平均的な価格に対する今の配当額の利回り
            avg_yield_2y = (div_rate / avg_price_2y) * 100
            
            # 現在の利回りが過去平均より20%以上高い（＝歴史的に割安）
            if cur_yield > (avg_yield_2y * 1.2):
                found_opportunities.append({
                    "Symbol": symbol,
                    "Yield": cur_yield,
                    "AvgYield": avg_yield_2y,
                    "Payout": payout
                })
        except:
            continue

    # 通知処理
    top_deals = sorted(found_opportunities, key=lambda x: x['Yield'], reverse=True)[:10]
    
    if top_deals:
        msg = "【米国株・流動性バグ検知レポート】\n"
        for d in top_deals:
            msg += f"✅ **{d['Symbol']}**: 利回り{d['Yield']:.2f}% (平均{d['AvgYield']:.2f}%) / 配当性向{d['Payout']:.1f}%\n"
        send_discord_message(msg)
    else:
        send_discord_message("本日、条件に合致する「バグ」銘柄は見つかりませんでした。")

def send_discord_message(content):
    payload = {"content": content}
    headers = {"Content-Type": "application/json"}
    requests.post(webhook_url_yfinance, data=json.dumps(payload), headers=headers)

if __name__ == "__main__":
    analyze_market()
