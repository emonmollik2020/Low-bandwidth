# features.py (শেয়ারড ফিচার ইঞ্জিনিয়ারিং মডিউল)
# BANDWIDTH OPTIMIZATION:
#   - _fetch_btc_history_sync() COMMENTED OUT (API fetch removed)
#   - _btc_cache populated from DB (pd.read_sql("btc_15m_history"))
#   - get_aligned_btc_series() reads from DB, not API
#
# এই ফাইলটাই একমাত্র জায়গা যেখানে ফিচার তৈরি ও লেবেলিং লজিক থাকবে।
# লাইভ বট (ai_engine.py) এবং অফলাইন ট্রেনিং স্ক্রিপ্ট (train_model.py) — দুটোই
# এই একই ফাংশন ব্যবহার করে, যাতে ট্রেনিং আর লাইভ প্রেডিকশনের ফিচার কখনো আলাদা না হয়।
import time
import numpy as np
import pandas as pd
import ta

# BANDWIDTH OPTIMIZATION: Removed ccxt import (no longer fetching BTC from API)
# import ccxt

from config import DB_ENABLED
from database import engine

# মডেলে ব্যবহৃত ফিচার কলামের চূড়ান্ত ও একমাত্র তালিকা (ক্রম গুরুত্বপূর্ণ)
FEATURE_COLS = [
    'rsi', 'diff_ema20', 'diff_ema50', 'diff_vwap', 'atr_pct', 'vol_ratio',
    'macd_diff', 'diff_ema200', 'btc_c',
    'dist_to_pdh', 'dist_to_pdl', 'dist_to_h4_res', 'dist_to_h4_sup'
]

LOOK_AHEAD = 4            # পরবর্তী ৪টি ক্যান্ডেল (১৫মি x ৪ = ১ ঘণ্টা)
PROFIT_THRESHOLD = 0.015  # ১.৫% অনুকূল মুভ হলে সিগন্যাল
LOSS_THRESHOLD = 0.010    # ১.০%-এর বেশি প্রতিকূল মুভ হলে সিগন্যাল বাতিল

# মডিউল-লেভেল BTC হিস্টোরি ক্যাশ (বারবার DB read এড়ানোর জন্য)
_btc_cache = {"df": None, "last_fetch_ms": 0}


# BANDWIDTH OPTIMIZATION: COMMENTED OUT — no longer fetching BTC from API
# def _fetch_btc_history_sync(since_ms, until_ms=None):
#     """Bitget থেকে BTC/USDT:USDT 15m ক্লোজ প্রাইস হিস্ট্রি সিঙ্ক্রোনাসভাবে টেনে আনে (SOL ডাটার সাথে টাইমস্ট্যাম্প অ্যালাইন করার জন্য)"""
#     exchange = ccxt.bitget()
#     now_ms = until_ms or int(time.time() * 1000)
#     all_candles = []
#     current_since = since_ms
#     while current_since < now_ms:
#         try:
#             batch = exchange.fetch_ohlcv("BTC/USDT:USDT", "15m", since=current_since, limit=1000)
#             if not batch:
#                 break
#             all_candles.extend(batch)
#             new_since = batch[-1][0] + 1
#             if new_since <= current_since:
#                 break
#             current_since = new_since
#         except Exception as e:
#             print(f"⚠️ [Features] BTC হিস্ট্রি ফেচ ওয়ার্নিং: {e}", flush=True)
#             break
#     if not all_candles:
#         return pd.DataFrame(columns=['t', 'btc_c'])
#     df_btc = pd.DataFrame(all_candles, columns=['t', 'o', 'h', 'l', 'btc_c', 'v'])
#     return df_btc[['t', 'btc_c']].drop_duplicates(subset=['t']).sort_values('t').reset_index(drop=True)


# BANDWIDTH OPTIMIZATION: Read BTC from DB instead of API
def _fetch_btc_from_db():
    """Read BTC 15m history from Cloud DB (zero API calls)"""
    if not DB_ENABLED or engine is None:
        return pd.DataFrame(columns=['t', 'btc_c'])
    try:
        df_btc = pd.read_sql("SELECT t, c as btc_c FROM btc_15m_history ORDER BY t ASC", engine)
        if not df_btc.empty:
            return df_btc.drop_duplicates(subset=['t']).sort_values('t').reset_index(drop=True)
    except Exception as e:
        print(f"⚠️ [Features] BTC DB read warning: {e}", flush=True)
    return pd.DataFrame(columns=['t', 'btc_c'])


def get_aligned_btc_series(sol_timestamps):
    """
    BANDWIDTH OPTIMIZED: প্রতিটা SOL ক্যান্ডেলের নিজস্ব সময়কার BTC ক্লোজ প্রাইস রিটার্ন করে।
    এখন Cloud DB থেকে পড়ে, API call নেই।
    """
    global _btc_cache
    now_ms = int(time.time() * 1000)

    # Cache miss or stale (older than 15 minutes)
    if _btc_cache["df"] is None or _btc_cache["df"].empty or (now_ms - _btc_cache["last_fetch_ms"] > 15 * 60 * 1000):
        _btc_cache["df"] = _fetch_btc_from_db()
        _btc_cache["last_fetch_ms"] = now_ms

    df_btc = _btc_cache["df"]
    if df_btc is None or df_btc.empty:
        return pd.Series(0.0, index=sol_timestamps.index)

    merged = pd.merge_asof(
        pd.DataFrame({'t': sol_timestamps}).sort_values('t'),
        df_btc.sort_values('t'),
        on='t', direction='backward'
    )
    return merged['btc_c'].ffill().bfill().fillna(0.0).reset_index(drop=True)


def build_features(df_raw):
    """df_raw: কলাম ['t','o','h','l','c','v'] সম্বলিত OHLCV ডাটাফ্রেম। রিটার্ন করে এনরিচড ডাটাফ্রেম (FEATURE_COLS সহ)।"""
    df = df_raw.copy().sort_values('t').reset_index(drop=True)

    df['rsi'] = ta.momentum.rsi(df['c'], window=14).fillna(50)
    df['ema20'] = ta.trend.ema_indicator(df['c'], window=20).fillna(df['c'])
    df['ema50'] = ta.trend.ema_indicator(df['c'], window=50).fillna(df['c'])
    df['ema200'] = ta.trend.ema_indicator(df['c'], window=200).fillna(df['c'])
    df['vwap'] = ta.volume.volume_weighted_average_price(
        high=df['h'], low=df['l'], close=df['c'], volume=df['v'], window=14
    ).fillna(df['c'])

    atr = ta.volatility.average_true_range(high=df['h'], low=df['l'], close=df['c'], window=14).fillna(0)
    df['atr_pct'] = (atr / df['c']).replace([np.inf, -np.inf], 0).fillna(0)

    vol_ma = df['v'].rolling(window=15).mean().fillna(1).replace(0, 1)
    df['vol_ratio'] = (df['v'] / vol_ma).fillna(1)

    macd_obj = ta.trend.MACD(df['c'])
    df['macd_diff'] = (macd_obj.macd() - macd_obj.macd_signal()).fillna(0)

    df['diff_ema20'] = df['c'] - df['ema20']
    df['diff_ema50'] = df['c'] - df['ema50']
    df['diff_vwap'] = df['c'] - df['vwap']
    df['diff_ema200'] = df['c'] - df['ema200']

    df['pdh'] = df['h'].rolling(window=96).max().shift(1).fillna(df['h'])
    df['pdl'] = df['l'].rolling(window=96).min().shift(1).fillna(df['l'])
    df['h4_res'] = df['h'].rolling(window=320).max().shift(1).fillna(df['h'])
    df['h4_sup'] = df['l'].rolling(window=320).min().shift(1).fillna(df['l'])

    df['dist_to_pdh'] = (df['pdh'] - df['c']) / df['c']
    df['dist_to_pdl'] = (df['pdl'] - df['c']) / df['c']
    df['dist_to_h4_res'] = (df['h4_res'] - df['c']) / df['c']
    df['dist_to_h4_sup'] = (df['h4_sup'] - df['c']) / df['c']

    # BANDWIDTH OPTIMIZED: DB থেকে BTC প্রাইস (zero API call)
    df['btc_c'] = get_aligned_btc_series(df['t'])

    return df


def make_labels(df):
    """৩-শ্রেণির লেবেল তৈরি করে: 1 = LONG সুযোগ, -1 = SHORT সুযোগ, 0 = নিউট্রাল (রেফারেন্সের জন্য রাখা হয়েছে)"""
    close_prices = df['c'].values
    high_prices = df['h'].values
    low_prices = df['l'].values
    n = len(df)
    labels = np.zeros(n, dtype=int)

    for i in range(n - LOOK_AHEAD):
        current_price = close_prices[i]
        future_highs = high_prices[i + 1: i + 1 + LOOK_AHEAD]
        future_lows = low_prices[i + 1: i + 1 + LOOK_AHEAD]

        max_future_return = (np.max(future_highs) - current_price) / current_price
        min_future_return = (np.min(future_lows) - current_price) / current_price

        if max_future_return >= PROFIT_THRESHOLD and min_future_return > -LOSS_THRESHOLD:
            labels[i] = 1
        elif min_future_return <= -PROFIT_THRESHOLD and max_future_return < LOSS_THRESHOLD:
            labels[i] = -1

    return labels


def make_regression_target(df, look_ahead=LOOK_AHEAD):
    """
    প্রতিটা ক্যান্ডেলের জন্য পরবর্তী look_ahead ক্যান্ডেল পরের প্রকৃত % রিটার্ন রিটার্ন করে
    (close[i+look_ahead] - close[i]) / close[i]। এটা make_labels()-এর মতো বিরল থ্রেশহোল্ড-ভিত্তিক
    নয় — প্রতিটা রো-ই একটা বৈধ (non-rare) ট্রেনিং টার্গেট পায়, তাই মডেল সম্পূর্ণ ডাটাসেট থেকে শিখতে পারে।
    শেষ look_ahead সারিতে NaN থাকবে (পর্যাপ্ত ভবিষ্যৎ ডাটা না থাকায়)।
    """
    close_prices = df['c'].values
    n = len(df)
    target = np.full(n, np.nan, dtype=float)
    for i in range(n - look_ahead):
        current_price = close_prices[i]
        future_price = close_prices[i + look_ahead]
        target[i] = (future_price - current_price) / current_price
    return target
