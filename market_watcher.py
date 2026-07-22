# market_watcher.py (রিয়েল-টাইম লাইভ প্রাইস ও ট্রেন্ড ট্র্যাকার)
# BANDWIDTH OPTIMIZATION:
#   - btc_ticker_loop() COMMENTED OUT (BTC live ticker removed, not needed)
#   - btc_ema_update_loop() reads from DB, no API calls (BTC 15m → resample 1h → EMA20)
#   - sol_live_ticker_loop() kept for tick-level SOL price (chart display)
import asyncio
import pandas as pd
import ta
from datetime import datetime
from config import SYMBOL, LEVERAGE, state_manager
from database import safe_update_live_state, engine, DB_ENABLED
from exchange_helper import exchange_helper

# মডিউল-লেভেল ট্র্যাকার গ্লোবাল ভেরিয়েবলসমূহ
global_btc_bullish = True
global_btc_bearish = False
global_btc_price = 0.0
global_btc_e20 = 0.0

async def sol_live_ticker_loop():
    """লাইভ সোল প্রাইস ট্র্যাকার লুপ — tick-level for chart display"""
    while True:
        try:
            ticker = await exchange_helper.watch_ticker(SYMBOL)
            p = float(ticker['last'])
            
            cur = state_manager.get()
            updates = {"price": round(p, 2)}
            
            if cur.get("in_position", False):
                entry_p = cur.get("entry_price", 0.0)
                pos_size_usd = cur.get("pos_size", 0.0)
                position_type = cur.get("position_type", "NONE")
                
                if entry_p > 0:
                    if position_type == "LONG":
                        l_pnl = ((p / entry_p) - 1) * 100 * LEVERAGE
                        l_val = pos_size_usd * ((p / entry_p) - 1)
                    else: 
                        l_pnl = (1 - (p / entry_p)) * 100 * LEVERAGE
                        l_val = pos_size_usd * (1 - (p / entry_p))
                else:
                    l_pnl = 0.0
                    l_val = 0.0
                    
                updates["live_pnl_pct"] = round(l_pnl, 2)
                updates["live_pnl_val"] = round(l_val, 2)
            
            await safe_update_live_state(updates)
        except Exception as e:
            print(f"Live Ticker Loop Warning: {e}", flush=True)
            await asyncio.sleep(1)

async def btc_ema_update_loop():
    """BANDWIDTH OPTIMIZED: BTC 1h EMA from DB (no API calls)"""
    global global_btc_e20, global_btc_bullish, global_btc_bearish, global_btc_price
    
    while True:
        try:
            if not DB_ENABLED or engine is None:
                # Fallback if no DB
                global_btc_bullish = True
                global_btc_bearish = False
                await asyncio.sleep(300)
                continue
            
            # BANDWIDTH OPT: Read BTC 15m from DB, resample to 1h, calculate EMA20
            df_btc = pd.read_sql("SELECT * FROM btc_15m_history ORDER BY t ASC", engine)
            
            if df_btc.empty or len(df_btc) < 80:  # Need at least ~80 15m candles for 20 1h candles
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Not enough BTC 15m data for 1h EMA, waiting...", flush=True)
                await asyncio.sleep(60)
                continue
            
            # Convert timestamp to datetime for resampling
            df_btc['dt'] = pd.to_datetime(df_btc['t'], unit='ms')
            df_btc.set_index('dt', inplace=True)
            
            # Resample 15m to 1h
            df_1h = df_btc.resample('1h').agg({
                't': 'first', 'o': 'first', 'h': 'max', 'l': 'min', 'c': 'last', 'v': 'sum'
            }).dropna().reset_index(drop=True)
            
            if len(df_1h) < 20:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Not enough 1h BTC candles, waiting...", flush=True)
                await asyncio.sleep(60)
                continue
            
            # Calculate EMA20 on 1h
            ema_series = ta.trend.ema_indicator(df_1h['c'], 20)
            if not ema_series.empty:
                global_btc_e20 = float(ema_series.iloc[-1])
                # Use last 15m close as approximate live price for trend check
                global_btc_price = float(df_btc['c'].iloc[-1])
                global_btc_bullish = global_btc_price > global_btc_e20
                global_btc_bearish = global_btc_price < global_btc_e20
                
                print(f"[{datetime.now().strftime('%H:%M:%S')}] BTC 1h EMA 20 updated from DB: ${global_btc_e20:.2f}, Price: ${global_btc_price:.2f}, Bullish: {global_btc_bullish}", flush=True)
            
            # Update every 15 minutes (same as candle close) instead of 5 minutes
            await asyncio.sleep(900)
            
        except Exception as e:
            print(f"BTC EMA Update Loop Warning: {e}", flush=True)
            await asyncio.sleep(60)


# BANDWIDTH OPTIMIZATION: btc_ticker_loop() COMMENTED OUT
# BTC live ticker removed — not needed for dashboard
# BTC trend comes from DB-based btc_ema_update_loop() above
# 
# async def btc_ticker_loop():
#     """লাইভ বিটিসি প্রাইস ও ট্রেন্ড ট্র্যাকার লুপ — REMOVED (bandwidth save)"""
#     global global_btc_bullish, global_btc_bearish, global_btc_price, global_btc_e20
#     while True:
#         try:
#             ticker_btc = await exchange_helper.watch_ticker("BTC/USDT:USDT")
#             btc_p = float(ticker_btc['last'])
#             global_btc_price = btc_p
#             
#             if global_btc_e20 > 0.0:
#                 global_btc_bullish = btc_p > global_btc_e20
#                 global_btc_bearish = btc_p < global_btc_e20
#             else:
#                 global_btc_bullish = True
#                 global_btc_bearish = False
#         except Exception as e:
#             print(f"BTC Ticker Loop Warning: {e}", flush=True)
#             await asyncio.sleep(2)
