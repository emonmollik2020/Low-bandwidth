# bot.py (চূড়ান্ত সংস্করণ - এআই-চালিত স্মার্ট এক্সিট ও লাইভ থটস যুক্ত)
# BANDWIDTH OPTIMIZATION:
#   - 0.5s polling loop → event-driven (WebSocket-based, 15m candle trigger)
#   - DB write: every tick → only on 15m candle close
#   - File write (to_csv): every tick → only on 15m candle close
#   - State save: frequent → key events only (new candle, position change, SL/TP update)
#   - BTC 15m sync: SOL-এর সাথে একসাথে fetch + save
#   - asyncio.sleep(0.5) → removed, event-driven loop
import asyncio
import pandas as pd
import numpy as np
import ta
import time
import traceback
from datetime import datetime, timezone
from config import (
    SYMBOL, INITIAL_FUND, LEVERAGE, RISK_FRACTION, DEF_TP, DEF_SL,
    MAX_CANDLES_TO_KEEP, COOLDOWN_SECONDS, HISTORY_FILE, DB_ENABLED,
    state_manager, live_bus
)
from database import init_db_tables, load_state, safe_save_state, engine
from indicators import (
    clean_float, resample_tf, analyze_tf, format_seconds_to_bengali, get_advanced_pats
)
from exchange_helper import exchange_helper

# এআই ইঞ্জিন, ডাটা লোডার এবং মার্কেট ওয়াচার মডিউলগুলো ইমপোর্ট করা
import ai_engine
import data_loader
import market_watcher
import features

# গ্লোবাল ভেরিয়েবলসমূহ
MODEL_FILE = 'trading_model.pkl'

# BANDWIDTH OPT: Track pending state updates to batch them
_pending_state_updates = {}
_last_state_save_time = 0
_STATE_SAVE_INTERVAL = 5  # Minimum seconds between non-critical state saves

async def _batch_save_state(updates, force=False):
    """BANDWIDTH OPT: Batch state updates, save only on key events or interval"""
    global _pending_state_updates, _last_state_save_time
    
    _pending_state_updates.update(updates)
    
    now = time.time()
    should_save = (
        force or  # Key event (position open/close, SL/TP hit)
        (now - _last_state_save_time) >= _STATE_SAVE_INTERVAL  # Interval batch
    )
    
    if should_save:
        await safe_save_state(_pending_state_updates)
        _pending_state_updates = {}
        _last_state_save_time = now


# BANDWIDTH OPT: New function to sync BTC 15m with SOL (module level)
async def _sync_btc_15m(sol_closed_ts):
    """Fetch and save BTC 15m candle matching SOL closed timestamp"""
    if not DB_ENABLED:
        return
    try:
        from sqlalchemy import text
        # Fetch BTC 15m candle for the same period
        btc_candles = await exchange_helper.fetch_ohlcv_strict(
            "BTC/USDT:USDT", '15m', 
            since=sol_closed_ts - 15*60*1000, 
            limit=2
        )
        if btc_candles:
            df_btc_new = pd.DataFrame(btc_candles, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            # Save to DB
            with engine.connect() as conn:
                timestamps = tuple(df_btc_new['t'].tolist())
                if len(timestamps) == 1:
                    conn.execute(text(f"DELETE FROM btc_15m_history WHERE t = {timestamps[0]}"))
                else:
                    conn.execute(text("DELETE FROM btc_15m_history WHERE t IN :timestamps"), {"timestamps": timestamps})
                conn.commit()
            df_btc_new.to_sql('btc_15m_history', engine, if_exists='append', index=False)
    except Exception as e:
        print(f"BTC sync warning: {e}", flush=True)


# ওএইচএলসিভি এবং লাইভ সিদ্ধান্তের লুপ
async def sol_ohlcv_loop():
    df_sol = state_manager.global_df.copy()
    
    cur_init = load_state(force_reload=True)
    total = cur_init.get("trades", 0)
    win_rate = cur_init.get("win_rate", 0)
    wins = int((win_rate / 100.0) * total) if total > 0 else 0
    net_pnl = cur_init.get("total_pnl", 0.0)
    last_trade_time = cur_init.get("last_trade_time", 0.0)

    last_calc_time = 0.0
    last_closed_candle_ts = 0  # ক্যান্ডেল ক্লোজ ট্র্যাকার
    last_db_save_ts = 0  # BANDWIDTH OPT: Track last DB save

    while True:
        try:
            # BANDWIDTH OPT: Event-driven — wait for new WebSocket data instead of polling
            raw_sol = await exchange_helper.watch_ohlcv(SYMBOL, '15m', limit=2)
            df_new = pd.DataFrame(raw_sol, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            
            # BANDWIDTH OPT: Only save to DB on new candle close, not every tick
            new_candle_closed = False
            current_closed_ts = 0
            
            if len(df_sol) >= 2 and len(df_new) >= 2:
                current_closed_ts = int(df_new['t'].iloc[-2])
                if last_closed_candle_ts == 0:
                    last_closed_candle_ts = current_closed_ts
                    new_candle_closed = True
                elif current_closed_ts > last_closed_candle_ts:
                    last_closed_candle_ts = current_closed_ts
                    new_candle_closed = True
            
            # BANDWIDTH OPT: DB write only on new candle close
            if new_candle_closed and DB_ENABLED:
                try:
                    from sqlalchemy import text
                    with engine.connect() as conn:
                        timestamps = tuple(df_new['t'].tolist())
                        if len(timestamps) == 1:
                            conn.execute(text(f"DELETE FROM sol_15m_history WHERE t = {timestamps[0]}"))
                        else:
                            conn.execute(text("DELETE FROM sol_15m_history WHERE t IN :timestamps"), {"timestamps": timestamps})
                        conn.commit()
                    df_new.to_sql('sol_15m_history', engine, if_exists='append', index=False)
                    
                    # BANDWIDTH OPT: Sync BTC 15m together with SOL
                    await _sync_btc_15m(current_closed_ts)
                    
                except Exception:
                    pass
            
            df_sol = pd.concat([df_sol, df_new]).drop_duplicates(subset=['t'], keep='last').sort_values('t').reset_index(drop=True)
            if len(df_sol) > MAX_CANDLES_TO_KEEP:
                df_sol = df_sol.iloc[-MAX_CANDLES_TO_KEEP:]
            
            # BANDWIDTH OPT: File write only on new candle close
            if new_candle_closed:
                df_sol.to_csv(HISTORY_FILE, index=False)
            
            state_manager.global_df = df_sol.copy()

            # --- ক্যান্ডেল ক্লোজ ডিটেকশন ও এআই ট্রেনিং ট্রিগার লজিক ---
            if new_candle_closed:
                print(f"🔔 [Event] নতুন ক্যান্ডেল ক্লোজ সনাক্ত হয়েছে (TS: {current_closed_ts})। এআই রি-ট্রেনিং শুরু হচ্ছে...", flush=True)
                asyncio.create_task(asyncio.to_thread(
                    ai_engine.train_model_in_background, df_sol.copy(), market_watcher.global_btc_price
                ))
            # ------------------------------------------------------
            
            df15 = df_sol.copy()
            if df15.empty or len(df15) < 200:
                # BANDWIDTH OPT: Longer sleep when not enough data
                await asyncio.sleep(5)
                continue

            current_time = time.time()
            # BANDWIDTH OPT: Less frequent calculation (not every 0.5s)
            if current_time - last_calc_time < 2.0:
                await asyncio.sleep(0.5)
                continue
            last_calc_time = current_time

            df15_temp = df15.copy()
            df15_temp['dt'] = pd.to_datetime(df15_temp['t'], unit='ms')
            df15_temp.set_index('dt', inplace=True)
            
            df30m = resample_tf(df15_temp, '30min')
            df45m = resample_tf(df15_temp, '45min')
            df1h = resample_tf(df15_temp, '1h')
            df2h = resample_tf(df15_temp, '2h')
            df3h = resample_tf(df15_temp, '3h')
            df4h = resample_tf(df15_temp, '4h')
            df1d = resample_tf(df15_temp, '1D')

            state_manager.resampled_dfs = {
                '15m': df15.copy(), '30m': df30m.copy(), '45m': df45m.copy(), '1h': df1h.copy(),
                '2h': df2h.copy(), '3h': df3h.copy(), '4h': df4h.copy(), '1d': df1d.copy()
            }
            
            pdh = clean_float(df1d['h'].iloc[-2]) if len(df1d) >= 2 else 0.0
            pdl = clean_float(df1d['l'].iloc[-2]) if len(df1d) >= 2 else 0.0
            h4_res = clean_float(df4h['h'].iloc[-20:].max()) if len(df4h) >= 20 else 0.0
            h4_sup = clean_float(df4h['l'].iloc[-20:].min()) if len(df4h) >= 20 else 0.0
            
            p = df15['c'].iloc[-1]
            high_p = df15['h'].iloc[-1] 
            low_p = df15['l'].iloc[-1]
            
            # ফিক্সড: ভলিউম কনফার্মেশন আগে চলমান (আংশিক) ক্যান্ডেলের ভলিউম দিয়ে হতো, যা
            # স্বাভাবিকভাবেই ক্যান্ডেলের মাঝপথে কম থাকে এবং শেষের দিকে হঠাৎ বেড়ে যায় — তাই এটা
            # অনির্ভরযোগ্য ছিল। এখন শুধু সম্পূর্ণ ক্লোজড ক্যান্ডেলের ভলিউম ব্যবহার করা হচ্ছে।
            sol_vol_ma = df15['v'].iloc[-16:-1].mean() if len(df15) >= 16 else df15['v'].mean()
            sol_current_vol_closed = df15['v'].iloc[-2]
            volume_confirmed = sol_current_vol_closed > (1.2 * sol_vol_ma)
            
            vwap_series = ta.volume.volume_weighted_average_price(high=df15['h'], low=df15['l'], close=df15['c'], volume=df15['v'], window=14)
            vwap = vwap_series.fillna(0).iloc[-1]
            vwap_closed = vwap_series.fillna(0).iloc[-2]
            
            # ফিক্সড: ATR আগে চলমান (আংশিক) ক্যান্ডেল থেকে হিসাব হতো, তাই মাঝেমধ্যে
            # ভোলাটিলিটিকে কম দেখাত এবং SL অতিরিক্ত টাইট হয়ে যেত। এখন ক্লোজড ক্যান্ডেল ব্যবহার হচ্ছে।
            atr_series = ta.volatility.average_true_range(high=df15['h'], low=df15['l'], close=df15['c'], window=14).fillna(0)
            atr_closed = atr_series.iloc[-2]
            
            atr_pct = atr_closed / p if p > 0.0 else 0.0
            # ফিক্সড: SL ফ্লোর ১.০% থেকে বাড়িয়ে ১.৮% করা হলো — লাইভ ট্রেড হিস্ট্রি বিশ্লেষণে দেখা
            # গেছে SOL-এর স্বাভাবিক ১৫মি ক্যান্ডেল-নয়েজেই ১.০% ফ্লোরের SL প্রায়ই ভেঙে যাচ্ছিল
            # (কয়েকটা ট্রেড এন্ট্রির ১ মিনিটের মধ্যেই স্টপ-আউট হয়েছিল)।
            dynamic_tp_pct = max(0.028, min(0.070, 3.0 * atr_pct))  
            dynamic_sl_pct = max(0.018, min(0.040, 2.0 * atr_pct))  
            
            r15_series = ta.momentum.rsi(df15['c'], window=14).fillna(0)
            e20_series = ta.trend.ema_indicator(df15['c'], 20).fillna(0)
            e50_series = ta.trend.ema_indicator(df15['c'], 50).fillna(0)
            
            r15_live = r15_series.iloc[-1]
            e20_live = e20_series.iloc[-1]
            e50_live = e50_series.iloc[-1]
            
            p_closed = df15['c'].iloc[-2]
            r15_closed = r15_series.iloc[-2]
            e20_closed = e20_series.iloc[-2]
            e50_closed = e50_series.iloc[-2]
            
            # ফিক্সড: এন্ট্রি-সংক্রান্ত VWAP/EMA কনফার্মেশন এখন ক্লোজড ক্যান্ডেলের স্থিতিশীল দাম দিয়ে,
            # লাইভ/চলমান ক্যান্ডেলের ক্ষণস্থায়ী স্পাইক দিয়ে নয়
            vwap_long_confirmed = p_closed > vwap_closed
            vwap_short_confirmed = p_closed < vwap_closed
            
            e200_series = ta.trend.ema_indicator(df1h['c'], 200)
            e200 = e200_series.iloc[-1] if (len(df1h) >= 200 and e200_series is not None) else 0.0
            ema_200_available = e200 > 0.0
            
            m_obj = ta.trend.MACD(df1h['c'])
            mv = m_obj.macd().iloc[-1] if len(df1h) > 0 else 0.0
            ms = m_obj.macd_signal().iloc[-1] if len(df1h) > 0 else 0.0
            
            pats15 = get_advanced_pats(df15)
            pats1h = get_advanced_pats(df1h)
            
            cur = state_manager.get()
            in_pos = cur.get("in_position", False)
            entry_p = cur.get("entry_price", 0.0)
            position_type = cur.get("position_type", "NONE")
            peak_p = cur.get("peak_p", 0.0)
            valley_p = cur.get("valley_p", 0.0)

            pos_size_usd = cur.get("pos_size", 0.0)

            time_since_last_trade = time.time() - last_trade_time
            cooldown_over = time_since_last_trade >= COOLDOWN_SECONDS

            bull_signal = any(pt['t'] == 'bull' for pt in pats15) or any(pt['t'] == 'bull' for pt in pats1h)
            bear_signal = any(pt['t'] == 'bear' for pt in pats15) or any(pt['t'] == 'bear' for pt in pats1h)
            
            macro_bullish = (p_closed > e200) if ema_200_available else False
            macro_bearish = (p_closed < e200) if ema_200_available else False
            
            # ফিক্সড: EMA অ্যালাইনমেন্টও এখন ক্লোজড ক্যান্ডেলের দাম দিয়ে চেক হচ্ছে
            ema_long_alignment = p_closed > e20_closed and p_closed > e50_closed
            ema_short_alignment = p_closed < e20_closed and p_closed < e50_closed

            # এআই প্রেডিকশন লজিক
            ai_prediction = 0
            ai_loaded = False
            
            # ৪টি নতুন সাপোর্ট-রেজিস্ট্যান্স দূরত্বের গাণিতিক মান প্রস্তুত করা (ক্লোজড ক্যান্ডেলের দাম দিয়ে)
            dist_to_pdh = float((pdh - p_closed) / p_closed) if p_closed > 0 else 0.0
            dist_to_pdl = float((pdl - p_closed) / p_closed) if p_closed > 0 else 0.0
            dist_to_h4_res = float((h4_res - p_closed) / p_closed) if p_closed > 0 else 0.0
            dist_to_h4_sup = float((h4_sup - p_closed) / p_closed) if p_closed > 0 else 0.0

            # ফিক্সড: সাপোর্ট/রেজিস্ট্যান্স লেভেল আগে শুধু তথ্য হিসেবে দেখানো হতো, এন্ট্রি আটকাত না।
            # লাইভ ট্রেড বিশ্লেষণে দেখা গেছে বেশিরভাগ LONG এন্ট্রি রেজিস্ট্যান্সের গায়ে আর একমাত্র
            # SHORT এন্ট্রিটা সাপোর্টের গায়ে হয়েছিল — উভয় ক্ষেত্রেই তারপর দাম উল্টো দিকে গেছে।
            # এখন গুরুত্বপূর্ণ লেভেলের ০.৮%-এর মধ্যে থাকলে সেই দিকের এন্ট্রি ব্লক করা হবে।
            SR_PROXIMITY_THRESHOLD = 0.008
            near_resistance = (abs(dist_to_h4_res) < SR_PROXIMITY_THRESHOLD) or (abs(dist_to_pdh) < SR_PROXIMITY_THRESHOLD)
            near_support = (abs(dist_to_h4_sup) < SR_PROXIMITY_THRESHOLD) or (abs(dist_to_pdl) < SR_PROXIMITY_THRESHOLD)

            if ai_engine.ai_model is not None:
                try:
                    # ফিক্সড: লোকাল ভ্যারিয়েবলের নাম 'features' রাখা হতো, যা 'features' মডিউলকে শ্যাডো করে ফেলত।
                    # এখন সব ফিচার ক্লোজড ক্যান্ডেলের মান দিয়ে গণনা হচ্ছে (btc_c বাদে, যেটা সত্যিই লাইভ),
                    # যাতে ট্রেনিং পাইপলাইনের (features.py) সাথে ঠিক মেলে
                    feature_values = [
                        float(r15_closed),
                        float(p_closed - e20_closed),
                        float(p_closed - e50_closed),
                        float(p_closed - vwap_closed),
                        float(atr_pct),
                        float(sol_current_vol_closed / (sol_vol_ma if sol_vol_ma > 0 else 1)),
                        float(mv - ms),
                        float(p_closed - e200) if ema_200_available else 0.0,
                        float(market_watcher.global_btc_price),
                        dist_to_pdh,
                        dist_to_pdl,
                        dist_to_h4_res,
                        dist_to_h4_sup
                    ]

                    # ফিক্সড: হার্ডকোডেড কলাম লিস্টের বদলে শেয়ারড features.py থেকে আনা তালিকা,
                    # যাতে ট্রেনিং আর লাইভ প্রেডিকশনের ফিচার-অর্ডার কখনো আলাদা না হয়
                    features_df = pd.DataFrame([feature_values], columns=features.FEATURE_COLS)

                    # ফিক্সড: এআই এখন একটা রিগ্রেশন মডেল (bundle: model + threshold_long/short)।
                    # মডেল সরাসরি পরবর্তী ১ ঘণ্টার প্রত্যাশিত % রিটার্ন প্রেডিক্ট করে, তারপর সেই
                    # রিটার্ন ট্রেনিং-ডাটা-থেকে-পাওয়া থ্রেশহোল্ডের সাথে তুলনা করে ১/-১/০-তে রূপান্তরিত হয়।
                    ai_bundle = ai_engine.ai_model
                    predicted_return = float(ai_bundle["model"].predict(features_df)[0])

                    if predicted_return >= ai_bundle["threshold_long"]:
                        ai_prediction = 1
                    elif predicted_return <= ai_bundle["threshold_short"]:
                        ai_prediction = -1
                    else:
                        ai_prediction = 0

                    ai_loaded = True
                except Exception as e:
                    print(f"AI Prediction Error (Falling back to Rules): {e}", flush=True)
                    ai_loaded = False

            # রুল-বেজড কনফ্লুয়েন্স ফিল্টার — সবসময় গণনা করা হয়, AI চালু থাকলেও।
            # ফিক্সড: এখন (ক) শুধু নতুন ক্যান্ডেল ক্লোজ হলেই নতুন এন্ট্রি বিবেচনা করা হয় (লাইভ-টিক
            # নয়েজে এন্ট্রি ঠেকাতে), এবং (খ) গুরুত্বপূর্ণ সাপোর্ট/রেজিস্ট্যান্সের কাছাকাছি থাকলে
            # সংশ্লিষ্ট দিকের এন্ট্রি ব্লক করা হয়।
            rule_can_buy_long = (new_candle_closed and not near_resistance and
                                  macro_bullish and ema_long_alignment and market_watcher.global_btc_bullish and 
                                  volume_confirmed and vwap_long_confirmed and 
                                  (40 < r15_closed < 65) and (mv > ms) and bull_signal and cooldown_over)

            rule_can_buy_short = (new_candle_closed and not near_support and
                                   macro_bearish and ema_short_alignment and market_watcher.global_btc_bearish and 
                                   volume_confirmed and vwap_short_confirmed and 
                                   (35 < r15_closed < 60) and (mv < ms) and bear_signal and cooldown_over)

            # ফিক্সড: AI চালু থাকলে আগে শুধু AI প্রেডিকশনেই এন্ট্রি হতো (OR-লজিক), যা সব
            # টেকনিক্যাল সেফটি ফিল্টার বাইপাস করে দিত। এখন AI + রুল-বেজড ফিল্টার — দুটোই
            # একমত হলেই (AND-লজিক) এন্ট্রি হবে, যাতে AI ভুল করলেও ফিল্টার ট্রেড আটকায়।
            if ai_loaded:
                can_buy_long = (ai_prediction == 1) and cooldown_over and ema_200_available and rule_can_buy_long
                can_buy_short = (ai_prediction == -1) and cooldown_over and ema_200_available and rule_can_buy_short
            else:
                can_buy_long = rule_can_buy_long
                can_buy_short = rule_can_buy_short

            # ফিক্সড: আগে স্মার্ট-এক্সিট লসেও ট্রিগার হতে পারত (TP পাওয়ার আগেই লাভজনক ট্রেড কেটে
            # যেত, কিন্তু SL-কে পুরো নির্ধারিত ক্ষতি সহ্য করতে হতো — win/loss সাইজে অসামঞ্জস্য তৈরি করত)।
            # এখন স্মার্ট-এক্সিট শুধু তখনই ট্রিগার হবে যখন ট্রেড ইতিমধ্যে লাভে আছে (লাভ লক করার জন্য),
            # ক্ষতির সিদ্ধান্ত সবসময় স্পষ্ট SL লেভেলের হাতে থাকবে।
            long_smart_sell = (p_closed > entry_p) and ((p_closed < e50_closed) or (r15_closed > 78) or (ai_loaded and ai_prediction == -1))
            short_smart_sell = (p_closed < entry_p) and ((p_closed > e50_closed) or (r15_closed < 22) or (ai_loaded and ai_prediction == 1))

            if in_pos:
                initial_sl_dist_pct = cur.get("entry_sl_pct", DEF_SL)
                if initial_sl_dist_pct <= 0:
                    initial_sl_dist_pct = DEF_SL
                
                if position_type == "LONG":
                    breakeven_trigger = entry_p * (1 + (0.6 * initial_sl_dist_pct))
                    if p >= breakeven_trigger and cur["sl_level"] < round(entry_p, 2):
                        cur["log"].insert(0, {"t": datetime.now().strftime("%H:%M"), "m": "🛡️ SL Breakeven-এ উন্নীত [🟢 LONG]"})
                        # BANDWIDTH OPT: Batch save
                        await _batch_save_state({"sl_level": round(entry_p, 2), "log": cur["log"]}, force=True)

                    if p > peak_p:
                        peak_p = p
                        new_sl = round(p * (1 - initial_sl_dist_pct), 2)
                        if new_sl > cur["sl_level"]:
                            await _batch_save_state({"sl_level": new_sl, "peak_p": peak_p}, force=True)

                    if high_p >= cur["tp_level"] or low_p <= cur["sl_level"] or long_smart_sell:
                        in_pos = False
                        position_type = "NONE"
                        
                        # লগে প্রদর্শনের জন্য এক্সিট রিজন বা কারণ সুনির্দিষ্ট করা
                        if low_p <= cur["sl_level"]:
                            exit_p = cur["sl_level"]
                            exit_reason = "Stop Loss"
                        elif high_p >= cur["tp_level"]:
                            exit_p = cur["tp_level"]
                            exit_reason = "Take Profit"
                        elif ai_loaded and ai_prediction == -1:
                            exit_p = p
                            exit_reason = "AI Reversal Exit"
                        else:
                            exit_p = p
                            exit_reason = "Technical Smart Exit"
                        
                        final_pnl_val = pos_size_usd * ((exit_p / entry_p) - 1) - (pos_size_usd * 0.0012)
                        net_pnl += final_pnl_val
                        # ফিক্সড: আগে raw price দিয়ে win গোনা হতো, ফি কাটার পর নেগেটিভ PnL-ও win কাউন্ট হতো
                        if final_pnl_val > 0: wins += 1
                        
                        total += 1
                        best_val = max(cur.get("best", 0.0), final_pnl_val)
                        worst_val = min(cur.get("worst", 0.0), final_pnl_val)
                        
                        last_trade_time = time.time()
                        cur["history"].insert(0, {
                            "ts": int(time.time()), "t": datetime.now().strftime("%H:%M"), "a": "SELL", "p": round(exit_p, 2), 
                            "r": f"{round((final_pnl_val / (pos_size_usd / LEVERAGE)) * 100, 2)}%"
                        })
                        cur["log"].insert(0, {"t": datetime.now().strftime("%H:%M"), "m": f"🔴 Position Close Long ([{exit_reason}] fee $ {pos_size_usd * 0.0012:.2f})"})
                        
                        # BANDWIDTH OPT: Force save on position close
                        await _batch_save_state({
                            "trades": total,
                            "balance": round(INITIAL_FUND + net_pnl, 2), "total_pnl": round(net_pnl, 2),
                            "win_rate": round((wins / total) * 100, 1) if total > 0 else 0,
                            "best": round(best_val, 2), "worst": round(worst_val, 2), "last_action": "SELL",
                            "in_position": False, "position_type": "NONE", "entry_price": 0.0, "pos_size": 0.0, "margin": 0.0, "peak_p": 0.0,
                            "entry_sl_pct": 0.0, "last_trade_time": last_trade_time, "history": cur["history"], "log": cur["log"]
                        }, force=True)

                elif position_type == "SHORT":
                    breakeven_trigger = entry_p * (1 - (0.6 * initial_sl_dist_pct))
                    if p <= breakeven_trigger and cur["sl_level"] > round(entry_p, 2):
                        cur["log"].insert(0, {"t": datetime.now().strftime("%H:%M"), "m": "🛡️ SL Breakeven-এ উন্নীত [🔴 SHORT]"})
                        await _batch_save_state({"sl_level": round(entry_p, 2), "log": cur["log"]}, force=True)

                    if valley_p == 0.0 or p < valley_p:
                        valley_p = p
                        new_sl = round(p * (1 + initial_sl_dist_pct), 2)
                        if cur["sl_level"] == 0.0 or new_sl < cur["sl_level"]:
                            await _batch_save_state({"sl_level": new_sl, "valley_p": valley_p}, force=True)

                    if low_p <= cur["tp_level"] or high_p >= cur["sl_level"] or short_smart_sell:
                        in_pos = False
                        position_type = "NONE"
                        
                        # লগে প্রদর্শনের জন্য এক্সিট রিজন বা কারণ সুনির্দিষ্ট করা
                        if high_p >= cur["sl_level"]:
                            exit_p = cur["sl_level"]
                            exit_reason = "Stop Loss"
                        elif low_p <= cur["tp_level"]:
                            exit_p = cur["tp_level"]
                            exit_reason = "Take Profit"
                        elif ai_loaded and ai_prediction == 1:
                            exit_p = p
                            exit_reason = "AI Reversal Exit"
                        else:
                            exit_p = p
                            exit_reason = "Technical Smart Exit"
                        
                        final_pnl_val = pos_size_usd * (1 - (exit_p / entry_p)) - (pos_size_usd * 0.0012)
                        net_pnl += final_pnl_val
                        # ফিক্সড: আগে raw price দিয়ে win গোনা হতো, ফি কাটার পর নেগেটিভ PnL-ও win কাউন্ট হতো
                        if final_pnl_val > 0: wins += 1
                        
                        total += 1
                        best_val = max(cur.get("best", 0.0), final_pnl_val)
                        worst_val = min(cur.get("worst", 0.0), final_pnl_val)
                        
                        last_trade_time = time.time()
                        cur["history"].insert(0, {
                            "ts": int(time.time()), "t": datetime.now().strftime("%H:%M"), "a": "SELL", "p": round(exit_p, 2), 
                            "r": f"{round((final_pnl_val / (pos_size_usd / LEVERAGE)) * 100, 2)}%"
                        })
                        cur["log"].insert(0, {"t": datetime.now().strftime("%H:%M"), "m": f"🔴 Position Close Short ([{exit_reason}] fee $ {pos_size_usd * 0.0012:.2f})"})
                        
                        # BANDWIDTH OPT: Force save on position close
                        await _batch_save_state({
                            "trades": total,
                            "balance": round(INITIAL_FUND + net_pnl, 2), "total_pnl": round(net_pnl, 2),
                            "win_rate": round((wins / total) * 100, 1) if total > 0 else 0,
                            "best": round(best_val, 2), "worst": round(worst_val, 2), "last_action": "SELL",
                            "in_position": False, "position_type": "NONE", "entry_price": 0.0, "pos_size": 0.0, "margin": 0.0, "valley_p": 0.0,
                            "entry_sl_pct": 0.0, "last_trade_time": last_trade_time, "history": cur["history"], "log": cur["log"]
                        }, force=True)
            else:
                if can_buy_long:
                    # ফিক্সড: এন্ট্রি প্রাইস আগে লাইভ/চলমান ক্যান্ডেলের দাম (p) থেকে নেওয়া হতো, যা
                    # একটা ক্ষণস্থায়ী স্পাইক হতে পারত। এখন স্থিতিশীল ক্লোজড ক্যান্ডেলের দাম ব্যবহার হচ্ছে।
                    entry_p = p_closed
                    peak_p = entry_p
                    in_pos = True
                    position_type = "LONG"
                    
                    account_balance = cur.get("balance", INITIAL_FUND)
                    risk_amount = account_balance * RISK_FRACTION
                    pos_size_usd = risk_amount / dynamic_sl_pct
                    pos_size_usd = max(10.0, min(account_balance * LEVERAGE, pos_size_usd)) 
                    margin_usd = pos_size_usd / LEVERAGE  
                    
                    # এন্ট্রি কনফ্লুয়েন্স বা কারণ ডাইনামিকালি সাজানো
                    reasons = []
                    if ema_long_alignment: reasons.append("১৫মি EMA আপট্রেন্ড")
                    if vwap_long_confirmed: reasons.append("VWAP বুলিশ")
                    if mv > ms: reasons.append("১ঘ MACD বুলিশ")
                    if bull_signal: reasons.append("মোমবাতি প্যাটার্ন")
                    if abs(dist_to_h4_sup) < 0.015: reasons.append("H4 সাপোর্ট বাউন্স")
                    if volume_confirmed: reasons.append("ভলিউম স্পাইক")
                    if market_watcher.global_btc_bullish: reasons.append("BTC আপট্রেন্ড")
                    
                    reason_str = ", ".join(reasons) if reasons else "এআই ট্রেন্ড প্যাটার্ন"
                    
                    cur["history"].insert(0, {"ts": int(time.time()), "t": datetime.now().strftime("%H:%M"), "a": "BUY", "p": round(entry_p, 2), "r": "---"})
                    cur["log"].insert(0, {"t": datetime.now().strftime("%H:%M"), "m": f"🟢 Position Open Long (size $ {pos_size_usd:.2f}) [কারণ: {reason_str}]"})
                    
                    # BANDWIDTH OPT: Force save on position open
                    await _batch_save_state({
                        "balance": round(account_balance, 2), "in_position": True, "position_type": "LONG",
                        "entry_price": round(entry_p, 2),
                        "sl_level": round(entry_p * (1 - dynamic_sl_pct), 2), "tp_level": round(entry_p * (1 + dynamic_tp_pct), 2),
                        "last_action": "BUY", "pos_size": round(pos_size_usd, 2), "margin": round(margin_usd, 2),
                        "peak_p": peak_p, "entry_sl_pct": dynamic_sl_pct, "history": cur["history"], "log": cur["log"]
                    }, force=True)
                
                elif can_buy_short:
                    # ফিক্সড: এখানেও স্থিতিশীল ক্লোজড ক্যান্ডেলের দাম ব্যবহার হচ্ছে
                    entry_p = p_closed
                    valley_p = entry_p
                    in_pos = True
                    position_type = "SHORT"
                    
                    account_balance = cur.get("balance", INITIAL_FUND)
                    risk_amount = account_balance * RISK_FRACTION
                    pos_size_usd = max(10.0, min(account_balance * LEVERAGE, risk_amount / dynamic_sl_pct)) 
                    margin_usd = pos_size_usd / LEVERAGE  
                    
                    # এন্ট্রি কনফ্লুয়েন্স বা কারণ ডাইনামিকালি সাজানো
                    reasons = []
                    if ema_short_alignment: reasons.append("১৫মি EMA ডাউনট্রেন্ড")
                    if vwap_short_confirmed: reasons.append("VWAP বেয়ারিশ")
                    if mv < ms: reasons.append("১ঘ MACD বেয়ারিশ")
                    if bear_signal: reasons.append("মোমবাতি প্যাটার্ন")
                    if abs(dist_to_h4_res) < 0.015: reasons.append("H4 রেজিস্ট্যান্স রিটেস্ট")
                    if volume_confirmed: reasons.append("ভলিউম স্পাইক")
                    if market_watcher.global_btc_bearish: reasons.append("BTC ডাউনট্রেন্ড")
                    
                    reason_str = ", ".join(reasons) if reasons else "এআই ট্রেন্ড প্যাটার্ন"
                    
                    # ফিক্সড: আগে SHORT পজিশন খোলার লগেও ভুলভাবে "BUY" লেখা হতো, যা বিভ্রান্তিকর ছিল
                    cur["history"].insert(0, {"ts": int(time.time()), "t": datetime.now().strftime("%H:%M"), "a": "SHORT", "p": round(entry_p, 2), "r": "---"})
                    cur["log"].insert(0, {"t": datetime.now().strftime("%H:%M"), "m": f"🔻 Position Open Short (size $ {pos_size_usd:.2f}) [কারণ: {reason_str}]"})
                    
                    # BANDWIDTH OPT: Force save on position open
                    await _batch_save_state({
                        "balance": round(account_balance, 2), "in_position": True, "position_type": "SHORT",
                        "entry_price": round(entry_p, 2),
                        "sl_level": round(entry_p * (1 + dynamic_sl_pct), 2), "tp_level": round(entry_p * (1 - dynamic_tp_pct), 2),
                        "last_action": "SHORT", "pos_size": round(pos_size_usd, 2), "margin": round(margin_usd, 2),
                        "valley_p": valley_p, "entry_sl_pct": dynamic_sl_pct, "history": cur["history"], "log": cur["log"]
                    }, force=True)
            
            confluences = {
                "macro_bullish": bool(macro_bullish), "btc_bullish": bool(market_watcher.global_btc_bullish), "vwap_long": bool(vwap_long_confirmed),
                "volume_confirmed": bool(volume_confirmed), "ema_long": bool(ema_long_alignment), "macd_long": bool(mv > ms), "bull_signal": bool(bull_signal),
                "macro_bearish": bool(macro_bearish), "btc_bearish": bool(market_watcher.global_btc_bearish), "vwap_short": bool(vwap_short_confirmed),
                "ema_short": bool(ema_short_alignment), "macd_short": bool(mv < ms), "bear_signal": bool(bear_signal)
            }
            
            exit_conditions = {
                "sl_safe": bool(low_p > cur.get("sl_level", 0.0)) if position_type == "LONG" else (bool(high_p < cur.get("sl_level", 99999.0)) if position_type == "SHORT" else True),
                "tp_safe": bool(high_p < cur.get("tp_level", 99999.0)) if position_type == "LONG" else (bool(low_p > cur.get("tp_level", 0.0)) if position_type == "SHORT" else True),
                "ema50_safe": bool(p_closed >= e50_closed) if position_type == "LONG" else (bool(p_closed <= e50_closed) if position_type == "SHORT" else True),
                "rsi_safe": bool(r15_closed <= 78) if position_type == "LONG" else (bool(r15_closed >= 22) if position_type == "SHORT" else True),
                "is_breakeven": bool(cur.get("sl_level", 0.0) >= entry_p) if position_type == "LONG" else (bool(cur.get("sl_level", 99999.0) <= entry_p) if position_type == "SHORT" else False)
            }

            analysis_15m = {"rsi": round(r15_live, 1), "ema20": round(e20_live, 2), "ema50": round(e50_live, 2), "vwap": round(vwap, 2), "sig": "বুলিশ ✅" if p > e20_live else "বেয়ারিশ ❌", "pats": pats15}
            analysis_30m = analyze_tf(df30m, p)
            analysis_45m = analyze_tf(df45m, p)
            analysis_1h = analyze_tf(df1h, p)
            analysis_1h.update({"ema200": round(e200, 2), "btc_price": round(market_watcher.global_btc_price, 1), "sig": "বুলিশ ✅" if p > e200 else ("বেয়ারিশ ❌" if ema_200_available else "ডাটা নেই 🛑")})
            analysis_2h = analyze_tf(df2h, p)
            analysis_3h = analyze_tf(df3h, p)
            analysis_4h = analyze_tf(df4h, p)
            analysis_1d = analyze_tf(df1d, p)

            now = datetime.now(timezone.utc)
            minutes_to_next_15m = 15 - (now.minute % 15)
            seconds_to_next_15m = (minutes_to_next_15m * 60) - now.second
            est_str = "পজিশন সক্রিয়" if in_pos else format_seconds_to_bengali(seconds_to_next_15m)

            # 🧠 এআই লাইভ থট প্রসেস জেনারেটর (AI Thoughts Generator)
            ai_thought_reasons = []
            if p > e200:
                ai_thought_reasons.append("সোলানার মূল্য ১ ঘণ্টার ২০০ EMA-এর ওপরে থাকায় আমি সামগ্রিক ট্রেন্ডকে বুলিশ দেখছি।")
            else:
                ai_thought_reasons.append("সোলানার মূল্য ১ ঘণ্টার ২০০ EMA-এর নিচে থাকায় বাজার দীর্ঘমেয়াদে বেয়ারিশ অবস্থানে আছে।")
                
            if abs(dist_to_h4_res) < 0.015:
                ai_thought_reasons.append("মূল্য বর্তমানে ৪ ঘণ্টার প্রধান রেজিস্ট্যান্স জোনের কাছাকাছি রয়েছে, তাই LONG পজিশন এড়ানোই ট্রেন্ডের স্বার্থে বুদ্ধিমানের কাজ হবে।")
            elif abs(dist_to_h4_sup) < 0.015:
                ai_thought_reasons.append("আমরা ৪ ঘণ্টার একটি শক্তিশালী সাপোর্ট জোনে প্রবেশ করেছি। এখান থেকে প্রাইস বাউন্স ব্যাক হওয়ার চমৎকার সম্ভাবনা রয়েছে।")
                
            if r15_live > 70:
                ai_thought_reasons.append("১৫মি RSI বর্তমানে অতিরিক্ত কেনা (Overbought) জোনে আছে, তাই একটি সাময়িক প্রাইস কারেকশন আশা করা যায়।")
            elif r15_live < 30:
                ai_thought_reasons.append("১৫মি RSI বর্তমানে অতিরিক্ত বিক্রি (Oversold) জোন স্পর্শ করেছে, মার্কেট এখান থেকে রিবাউন্ড করতে পারে।")
                
            if not volume_confirmed:
                ai_thought_reasons.append("তবে বাজারে ট্রেডিং ভলিউমের গতি অত্যন্ত মন্থর, ব্রেকআউট ছাড়া আমি হুট করে এন্ট্রি নিয়ে ফাঁদে পা দিতে চাই না।")
            else:
                ai_thought_reasons.append("ভলিউম বেশ ইতিবাচক এবং এন্ট্রি নেওয়ার জন্য শক্তিশালী মোমেন্টাম কনফার্মড।")
                
            if ai_prediction == 1:
                ai_thought_reasons.append("আমার ঐতিহাসিক প্যাটার্ন ম্যাচিং অনুযায়ী এখানে একটি আকর্ষণীয় LONG সুযোগ তৈরি হয়েছে!")
            elif ai_prediction == -1:
                ai_thought_reasons.append("আমার গাণিতিক বিশ্লেষণ বলছে এখানে একটি শক্তিশালী SHORT সুযোগ বা পতনের সম্ভাবনা রয়েছে!")
            else:
                ai_thought_reasons.append("মার্কেটে কোনো স্পষ্ট জয়ের প্যাটার্ন কনফার্মেশন না পাওয়ায় আমি শান্তভাবে ট্রেন্ড পর্যবেক্ষণ করছি।")
                
            ai_thoughts_str = " ".join(ai_thought_reasons)

            # BANDWIDTH OPT: Batch save analysis (not critical, interval batch OK)
            await _batch_save_state({
                "confluences": confluences, "exit_conditions": exit_conditions, "estimated_time": est_str, "pdh": clean_float(pdh), "pdl": clean_float(pdl), "h4_res": clean_float(h4_res), "h4_sup": clean_float(h4_sup),
                "ai_thoughts": ai_thoughts_str,
                "analysis_15m": analysis_15m, "analysis_30m": analysis_30m, "analysis_45m": analysis_45m, "analysis_1h": analysis_1h, "analysis_2h": analysis_2h, "analysis_3h": analysis_3h, "analysis_4h": analysis_4h, "analysis_1d": analysis_1d
            })

            cur_reason = state_manager.get()
            if cur_reason.get("in_position", False):
                reason_str = f"পজিশন সক্রিয় [{position_type}]"
            elif not ema_200_available:
                reason_str = "পর্যাপ্ত ডাটা নেই (EMA 200 লোড হচ্ছে, ট্রেড বন্ধ 🛑)"
            elif not cooldown_over:
                reason_str = f"কুলডাউন ({int((COOLDOWN_SECONDS - time_since_last_trade)/60)} মিনিট বাকি)"
            elif ai_loaded:
                reason_str = "এআই সোলানা এন্ট্রি সিগন্যাল স্ক্যান করছে... 🤖"
            else:
                if not market_watcher.global_btc_bullish and p > e200:
                    reason_str = "বিটকয়েন ট্রেন্ড ডাউন (BTC Bearish)"
                elif market_watcher.global_btc_bullish and p < e200:
                    reason_str = "বিটকয়েন ট্রেন্ড আপ (SOL SHORT এর উপযুক্ত নয়)"
                elif not vwap_long_confirmed and p > e200:
                    reason_str = "মূল্য VWAP লাইনের নিচে (Bearish Zone)"
                elif not vwap_short_confirmed and p < e200:
                    reason_str = "মূল্য VWAP লাইনের ওপরে (Bullish Zone)"
                elif not volume_confirmed:
                    reason_str = "ভারী ভলিউম (ভলিউম ব্রেকআউটের অপেক্ষা)"
                elif not ema_long_alignment and p > e200:
                    reason_str = "১৫-মিনিট চার্টে ল্যাপ বা রিট্রেসমেন্ট চলছে"
                elif not ema_short_alignment and p < e200:
                    reason_str = "১৫-মিনিট চার্টে বাউন্স ব্যাক বা কারেকশন চলছে"
                else:
                    reason_str = "সুইং এন্ট্রি প্যাটার্ন খুঁজছে..."
                
            # BANDWIDTH OPT: Batch save wait reason
            await _batch_save_state({"wait_reason": reason_str})
            
        except Exception as e:
            print(f"OHLCV Loop Warning: {e}", flush=True)
            await asyncio.sleep(2)
        
        # BANDWIDTH OPT: No fixed sleep — event-driven from watch_ohlcv
        # await asyncio.sleep(0.5)  # REMOVED


async def bot_engine():
    try:
        print("Starting Bot Engine...", flush=True)
        init_db_tables()
        
        print("Loading SOL 15m Database...", flush=True)
        await data_loader.bootstrap_or_backfill_sol()
        
        # BANDWIDTH OPT: Load BTC history too
        print("Loading BTC 15m Database...", flush=True)
        await data_loader.bootstrap_or_backfill_btc()
        
        print("Spawning concurrent ticker tasks...", flush=True)
        asyncio.create_task(market_watcher.sol_live_ticker_loop())
        # BANDWIDTH OPT: btc_ticker_loop removed
        asyncio.create_task(market_watcher.btc_ema_update_loop())
        
        print("Waiting for tickers to update price...", flush=True)
        await asyncio.sleep(3)
        
        # এআই ইঞ্জিন থেকে মডেল লোড বা প্রথমবার অটো-ট্রেনিং
        # BANDWIDTH OPT: Fresh train only, no load
        await ai_engine.load_or_train_ai_model(market_watcher.global_btc_price)
        
        print("Starting main decision loop...", flush=True)
        asyncio.create_task(sol_ohlcv_loop())
        
        print("Bot Engine successfully initialized and running!", flush=True)
        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        print(f"CRITICAL ERROR IN BOT ENGINE STARTUP: {e}", flush=True)
        traceback.print_exc()
