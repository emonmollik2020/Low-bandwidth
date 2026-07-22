# data_loader.py (হিস্টোরিক্যাল ডেটা লোডার ও সিঙ্ক ইঞ্জিন)
# BANDWIDTH OPTIMIZATION:
#   - Gap-only fetch (DB last timestamp vs current time)
#   - btc_15m_history sync with SOL (new table)
#   - bootstrap_or_backfill_sol() — DB check first, gap detect
#   - bootstrap_or_backfill_btc() — new function for BTC data sync
import asyncio
import os
import time
import pandas as pd
from datetime import datetime
from config import SYMBOL, HISTORY_FILE, DB_ENABLED, MAX_CANDLES_TO_KEEP, state_manager
from database import engine, load_state, safe_save_state
from exchange_helper import exchange_helper

async def bootstrap_or_backfill_sol():
    """BANDWIDTH OPTIMIZED: DB first, gap-only fetch"""
    now_ms = int(time.time() * 1000)
    df = None
    last_db_ts = 0
    
    # Step 1: Try loading from Cloud DB first
    if DB_ENABLED:
        try:
            df = pd.read_sql("SELECT * FROM sol_15m_history ORDER BY t ASC", engine)
            if not df.empty:
                last_db_ts = int(df['t'].iloc[-1])
                print(f"Loaded existing SOL history from Supabase. Rows: {len(df)}, Last: {pd.to_datetime(last_db_ts, unit='ms')}", flush=True)
        except Exception as e:
            print(f"Database read error: {e}", flush=True)
            df = None

    # Step 2: Fallback to local CSV
    if (df is None or df.empty) and os.path.exists(HISTORY_FILE):
        try:
            df = pd.read_csv(HISTORY_FILE)
            if not df.empty:
                last_db_ts = int(df['t'].iloc[-1])
                print(f"Loaded SOL history from local file. Rows: {len(df)}", flush=True)
                if DB_ENABLED and not df.empty:
                    df.to_sql('sol_15m_history', engine, if_exists='append', index=False)
        except Exception:
            df = None

    # Step 3: Check gap and fetch only missing data
    gap_ms = now_ms - last_db_ts if last_db_ts > 0 else now_ms
    
    # If no data at all or significant gap, fetch missing portion
    if df is None or df.empty or len(df) < 2500 or gap_ms > 15 * 60 * 1000:
        print(f"Syncing SOL gaps. Last DB timestamp: {pd.to_datetime(last_db_ts, unit='ms') if last_db_ts else 'None'}, Gap: {gap_ms/60000:.1f} minutes", flush=True)
        
        all_candles = []
        # BANDWIDTH OPT: Fetch only from last known timestamp, not full 90 days
        fetch_since = last_db_ts + 1 if last_db_ts > 0 else (now_ms - 90 * 24 * 60 * 60 * 1000)
        end_time_ms = now_ms
        
        while end_time_ms > fetch_since:
            elapsed_ms = now_ms - end_time_ms
            total_gap = now_ms - fetch_since
            progress_pct = min(100, max(1, int((elapsed_ms / total_gap) * 100))) if total_gap > 0 else 100
            progress_msg = f"SOL gap fill: {progress_pct}% complete"
            
            cur = load_state(force_reload=True)
            cur["wait_reason"] = progress_msg
            est_sec = max(5, int((end_time_ms - fetch_since) / (24 * 60 * 60 * 1000) * 0.8))
            cur["estimated_time"] = f"প্রায় {est_sec} সেকেন্ড বাকি"
            await safe_save_state(cur)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {progress_msg}", flush=True)
            
            try:
                params = {'endTime': end_time_ms}
                candles = await exchange_helper.fetch_ohlcv_strict(SYMBOL, '15m', limit=200, params=params)
                if not candles:
                    break
                all_candles.extend(candles)
                
                oldest_ts = candles[0][0]
                if oldest_ts >= end_time_ms or oldest_ts <= fetch_since:
                    # Filter out candles before fetch_since
                    all_candles = [c for c in all_candles if c[0] >= fetch_since]
                    break
                    
                end_time_ms = oldest_ts - 1
            except Exception as e:
                print(f"SOL Bootstrap Fetch Warning: {e}", flush=True)
                await asyncio.sleep(2)
                
            await asyncio.sleep(0.15)
                
        if all_candles:
            df_new = pd.DataFrame(all_candles, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            df_new = df_new.drop_duplicates(subset=['t'], keep='last').sort_values('t').reset_index(drop=True)
            
            if df is not None and not df.empty:
                df = pd.concat([df, df_new]).drop_duplicates(subset=['t'], keep='last').sort_values('t').reset_index(drop=True)
            else:
                df = df_new
                
            df.to_csv(HISTORY_FILE, index=False)
            
            if DB_ENABLED:
                try:
                    from sqlalchemy import text
                    with engine.begin() as conn:
                        conn.execute(text("TRUNCATE TABLE sol_15m_history;"))
                    df.to_sql('sol_15m_history', engine, if_exists='append', index=False)
                    print("Successfully saved SOL historical data to Supabase.", flush=True)
                except Exception as e:
                    print(f"SOL DB Insert Warning: {e}", flush=True)
        else:
            if df is None:
                df = pd.DataFrame(columns=['t', 'o', 'h', 'l', 'c', 'v'])
    else:
        # Small gap or no gap — just sync recent missing candles
        last_ts = int(df['t'].iloc[-1])
        if now_ms - last_ts > 15 * 60 * 1000:
            print("Syncing small SOL gaps...", flush=True)
            missing_candles = []
            since = last_ts + 1
            while since < now_ms:
                try:
                    candles = await exchange_helper.fetch_ohlcv_strict(SYMBOL, '15m', since=since, limit=1000)
                    if not candles:
                        break
                    missing_candles.extend(candles)
                    since = candles[-1][0] + 1
                    await asyncio.sleep(0.15)
                except Exception:
                    await asyncio.sleep(1)
                    break
            
            if missing_candles:
                df_missing = pd.DataFrame(missing_candles, columns=['t', 'o', 'h', 'l', 'c', 'v'])
                df = pd.concat([df, df_missing]).drop_duplicates(subset=['t'], keep='last').sort_values('t').reset_index(drop=True)
                
                if len(df) > MAX_CANDLES_TO_KEEP:
                    df_csv = df.iloc[-MAX_CANDLES_TO_KEEP:]
                else:
                    df_csv = df.copy()
                df_csv.to_csv(HISTORY_FILE, index=False)
                
                if DB_ENABLED:
                    try:
                        df_missing.to_sql('sol_15m_history', engine, if_exists='append', index=False)
                    except Exception:
                        pass
                        
    state_manager.global_df = df.copy()
    return df


async def bootstrap_or_backfill_btc():
    """BANDWIDTH OPTIMIZED: New function for BTC 15m data sync"""
    if not DB_ENABLED:
        print("DB not enabled, skipping BTC backfill", flush=True)
        return None
        
    now_ms = int(time.time() * 1000)
    df_btc = None
    last_db_ts = 0
    
    # Step 1: Load from Cloud DB
    try:
        df_btc = pd.read_sql("SELECT * FROM btc_15m_history ORDER BY t ASC", engine)
        if not df_btc.empty:
            last_db_ts = int(df_btc['t'].iloc[-1])
            print(f"Loaded existing BTC history from Supabase. Rows: {len(df_btc)}, Last: {pd.to_datetime(last_db_ts, unit='ms')}", flush=True)
    except Exception as e:
        print(f"BTC Database read error: {e}", flush=True)
        df_btc = None
    
    # Step 2: Check gap and fetch only missing data
    gap_ms = now_ms - last_db_ts if last_db_ts > 0 else now_ms
    
    if df_btc is None or df_btc.empty or gap_ms > 15 * 60 * 1000:
        print(f"Syncing BTC gaps. Last DB timestamp: {pd.to_datetime(last_db_ts, unit='ms') if last_db_ts else 'None'}, Gap: {gap_ms/60000:.1f} minutes", flush=True)
        
        all_candles = []
        fetch_since = last_db_ts + 1 if last_db_ts > 0 else (now_ms - 90 * 24 * 60 * 60 * 1000)
        end_time_ms = now_ms
        
        while end_time_ms > fetch_since:
            try:
                params = {'endTime': end_time_ms}
                candles = await exchange_helper.fetch_ohlcv_strict("BTC/USDT:USDT", '15m', limit=200, params=params)
                if not candles:
                    break
                all_candles.extend(candles)
                
                oldest_ts = candles[0][0]
                if oldest_ts >= end_time_ms or oldest_ts <= fetch_since:
                    all_candles = [c for c in all_candles if c[0] >= fetch_since]
                    break
                    
                end_time_ms = oldest_ts - 1
            except Exception as e:
                print(f"BTC Bootstrap Fetch Warning: {e}", flush=True)
                await asyncio.sleep(2)
                
            await asyncio.sleep(0.15)
        
        if all_candles:
            df_new = pd.DataFrame(all_candles, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            df_new = df_new.drop_duplicates(subset=['t'], keep='last').sort_values('t').reset_index(drop=True)
            
            if df_btc is not None and not df_btc.empty:
                df_btc = pd.concat([df_btc, df_new]).drop_duplicates(subset=['t'], keep='last').sort_values('t').reset_index(drop=True)
            else:
                df_btc = df_new
            
            # Save to DB
            try:
                from sqlalchemy import text
                with engine.begin() as conn:
                    conn.execute(text("TRUNCATE TABLE btc_15m_history;"))
                df_btc.to_sql('btc_15m_history', engine, if_exists='append', index=False)
                print("Successfully saved BTC historical data to Supabase.", flush=True)
            except Exception as e:
                print(f"BTC DB Insert Warning: {e}", flush=True)
        else:
            if df_btc is None:
                df_btc = pd.DataFrame(columns=['t', 'o', 'h', 'l', 'c', 'v'])
    else:
        # Small gap sync
        last_ts = int(df_btc['t'].iloc[-1])
        if now_ms - last_ts > 15 * 60 * 1000:
            missing_candles = []
            since = last_ts + 1
            while since < now_ms:
                try:
                    candles = await exchange_helper.fetch_ohlcv_strict("BTC/USDT:USDT", '15m', since=since, limit=1000)
                    if not candles:
                        break
                    missing_candles.extend(candles)
                    since = candles[-1][0] + 1
                    await asyncio.sleep(0.15)
                except Exception:
                    await asyncio.sleep(1)
                    break
            
            if missing_candles:
                df_missing = pd.DataFrame(missing_candles, columns=['t', 'o', 'h', 'l', 'c', 'v'])
                df_btc = pd.concat([df_btc, df_missing]).drop_duplicates(subset=['t'], keep='last').sort_values('t').reset_index(drop=True)
                
                try:
                    df_missing.to_sql('btc_15m_history', engine, if_exists='append', index=False)
                except Exception:
                    pass
    
    return df_btc
