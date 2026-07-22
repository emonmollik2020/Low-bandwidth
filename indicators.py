# indicators.py
import math
import pandas as pd
import ta

def clean_float(val):
    if val is None or pd.isna(val) or math.isnan(val) or math.isinf(val):
        return 0.0
    return float(val)

def get_advanced_pats(df):
    p = []
    if len(df) < 5:
        return p
        
    c1 = df.iloc[-1]
    c2 = df.iloc[-2]
    c3 = df.iloc[-3]
    c4 = df.iloc[-4]
    c5 = df.iloc[-5]
    
    def info(c):
        o, h, l, cl = c['o'], c['h'], c['l'], c['c']
        body = abs(cl - o)
        tot = max(0.001, h - l)
        u_wick = h - max(cl, o)
        l_wick = min(cl, o) - l
        is_green = cl >= o
        is_red = cl < o
        return o, h, l, cl, body, tot, u_wick, l_wick, is_green, is_red
        
    o1, h1, l1, cl1, b1, r1, u1, lw1, g1, rd1 = info(c1)
    o2, h2, l2, cl2, b2, r2, u2, lw2, g2, rd2 = info(c2)
    o3, h3, l3, cl3, b3, r3, u3, lw3, g3, rd3 = info(c3)
    o4, h4, l4, cl4, b4, r4, u4, lw4, g4, rd4 = info(c4)
    o5, h5, l5, cl5, b5, r5, u5, lw5, g5, rd5 = info(c5)

    # ১. এক ক্যান্ডেলস্টিক প্যাটার্নস (Single Candlestick)
    if b1 / r1 < 0.10 and r1 > 0:
        p.append({"n": "ডোজি ✝️", "t": "neutral"})
    if b1 / r1 < 0.10 and lw1 >= r1 * 0.70 and u1 <= r1 * 0.10:
        p.append({"n": "ড্রাগনফ্লাই ডোজি 🦎", "t": "bull"})  # বাগ ফিক্স: ফন্ট এরর প্রতিরোধী বাংলা 'ফ' ব্যবহার করা হয়েছে
    if b1 / r1 < 0.10 and u1 >= r1 * 0.70 and lw1 <= r1 * 0.10:
        p.append({"n": "গ্রেভস্টোন ডোজি 🪦", "t": "bear"})
    if lw1 >= 2.0 * b1 and u1 <= 0.15 * r1 and b1 > 0: 
        p.append({"n": "হ্যামার 🔨", "t": "bull"})
    if u1 >= 2.0 * b1 and lw1 <= 0.15 * r1 and b1 > 0: 
        p.append({"n": "ইনভার্টেড হ্যামার 🪓", "t": "bull"})
    if lw1 >= 2.0 * b1 and u1 <= 0.15 * r1 and b1 > 0 and cl1 > cl2:
        p.append({"n": "হ্যাংগিং ম্যান 👤", "t": "bear"})
    if u1 >= 2.0 * b1 and lw1 <= 0.15 * r1 and b1 > 0 and cl1 > cl2: 
        p.append({"n": "শুটিং স্টার ☄️", "t": "bear"})
    if g1 and b1 / r1 >= 0.90 and r1 > 0:
        p.append({"n": "বুলিশ মারুবোজু 🟩", "t": "bull"})
    if rd1 and b1 / r1 >= 0.90 and r1 > 0:
        p.append({"n": "বেয়ারিশ মারুবোজু 🟥", "t": "bear"})
    if g1 and 0.10 <= b1 / r1 <= 0.40 and u1 >= 0.30 * r1 and lw1 >= 0.30 * r1:
        p.append({"n": "বুলিশ স্পিনিং টপ 🟢", "t": "bull"})
    if rd1 and 0.10 <= b1 / r1 <= 0.40 and u1 >= 0.30 * r1 and lw1 >= 0.30 * r1:
        p.append({"n": "বেয়ারিশ স্পিনিং টপ 🔴", "t": "bear"})
    if b1 / r1 < 0.10 and u1 >= 0.40 * r1 and lw1 >= 0.40 * r1:
        p.append({"n": "লং-লেগড ডোজি ✝️", "t": "neutral"})
    if 0.10 <= b1 / r1 <= 0.30 and u1 >= 0.35 * r1 and lw1 >= 0.35 * r1:
        p.append({"n": "হাই ওয়েভ 🌊", "t": "neutral"})

    # ২. দুই ক্যান্ডেলস্টিক প্যাটার্নস (Double Candlestick)
    if g1 and rd2 and cl1 >= o2 and o1 <= cl2:
        p.append({"n": "বুলিশ এনগালফিং 📈", "t": "bull"})
    if rd1 and g2 and cl1 <= o2 and o1 >= cl2:
        p.append({"n": "বেয়ারিশ এনগালফিং 📉", "t": "bear"})
    if rd2 and g1 and b2 > r2 * 0.60 and cl1 <= o2 and o1 >= cl2:
        p.append({"n": "বুলিশ হারামি 🤰", "t": "bull"})
    if g2 and rd1 and b2 > r2 * 0.60 and cl1 >= o2 and o1 <= cl2:
        p.append({"n": "বেয়ারিশ হারামি 🤰", "t": "bear"})
    if rd2 and g1 and o1 < l2 and cl1 > (cl2 + b2 * 0.50) and cl1 <= o2:
        p.append({"n": "পিয়ার্সিং লাইন ⚡", "t": "bull"})
    if g2 and rd1 and o1 > h2 and cl1 < (cl2 - b2 * 0.50) and cl1 >= o2:
        p.append({"n": "ডার্ক ক্লাউড কাভার ☁️", "t": "bear"})
    if abs(l1 - l2) / max(0.01, l1) < 0.001:
        p.append({"n": "টুইজার বটম 🧲", "t": "bull"})
    if abs(h1 - h2) / max(0.01, h1) < 0.001:
        p.append({"n": "টুইজার টপ 🧲", "t": "bear"})
    if rd2 and g1 and b2 / r2 >= 0.85 and b1 / r1 >= 0.85 and o1 >= o2:
        p.append({"n": "বুলিশ কিকার 🦵", "t": "bull"})
    if g2 and rd1 and b2 / r2 >= 0.85 and b1 / r1 >= 0.85 and o1 <= o2:
        p.append({"n": "বেয়ারিশ কিকার 🦵", "t": "bear"})

    # ৩. তিন বা ততোধিক ক্যান্ডেলস্টিক প্যাটার্নস (Multi-Candlestick)
    if rd3 and g1 and b3 > r3 * 0.50 and b2 / r2 < 0.30 and cl1 > (cl3 + b3 * 0.50):
        p.append({"n": "মর্নিং স্টার 🌅", "t": "bull"})
    if g3 and rd1 and b3 > r3 * 0.50 and b2 / r2 < 0.30 and cl1 < (cl3 - b3 * 0.50):
        p.append({"n": "ইভনিং স্টার 🌇", "t": "bear"})
    if g1 and g2 and g3 and b1 > r1 * 0.60 and b2 > r2 * 0.60 and b3 > r3 * 0.60 and cl1 > cl2 > cl3:
        p.append({"n": "থ্রি হোয়াইট সোলজার্স 💂‍♂️", "t": "bull"})
    if rd1 and rd2 and rd3 and b1 > r1 * 0.60 and b2 > r2 * 0.60 and b3 > r3 * 0.60 and cl1 < cl2 < cl3:
        p.append({"n": "থ্রি ব্ল্যাক ক্রোস 🐦", "t": "bear"})
    if rd3 and g2 and g1 and cl2 <= o3 and o2 >= cl3 and cl1 > h2:
        p.append({"n": "থ্রি ইনসাইড আপ 📈", "t": "bull"})
    if g3 and rd2 and rd1 and cl2 >= o3 and o2 <= cl3 and cl1 < l2:
        p.append({"n": "থ্রি ইনসাইড ডাউন 📉", "t": "bear"})
    if rd3 and g2 and g1 and cl2 >= o3 and o2 <= cl3 and cl1 > cl2:
        p.append({"n": "থ্রি ইনসাইড আপ 📈", "t": "bull"})
    if g3 and rd2 and rd1 and cl2 <= o3 and o2 >= cl3 and cl1 < cl2:
        p.append({"n": "থ্রি আউটসাইড ডাউন 📉", "t": "bear"})
    if g5 and g1 and b5 > r5 * 0.60 and b1 > r1 * 0.60 and cl1 > h5 and max(h2, h3, h4) < h5 and min(l2, l3, l4) > l5:
        p.append({"n": "রাইজিং থ্রি মেথডস ⚡", "t": "bull"})
    if rd5 and rd1 and b5 > r5 * 0.60 and b1 > r1 * 0.60 and cl1 < l5 and max(h2, h3, h4) < h5 and min(l2, l3, l4) > l5:
        p.append({"n": "ফলিং থ্রি মেথডস ⚡", "t": "bear"})

    return p

def resample_tf(df_temp, tf_str):
    df_res = df_temp.resample(tf_str).agg({
        't': 'first', 'o': 'first', 'h': 'max', 'l': 'min', 'c': 'last', 'v': 'sum'
    }).dropna().reset_index(drop=True)
    return df_res

def analyze_tf(df_res, original_price):
    if len(df_res) < 2:
        return {"rsi": 0, "ema20": 0, "ema50": 0, "sig": "로드 হচ্ছে...", "pats": []}
    
    rsi_window = max(2, min(14, len(df_res) - 1))
    ema20_win = max(2, min(20, len(df_res) - 1))
    ema50_win = max(2, min(50, len(df_res) - 1))
    
    rsi_s = ta.momentum.rsi(df_res['c'], window=rsi_window).fillna(0)
    e20_s = ta.trend.ema_indicator(df_res['c'], window=ema20_win).fillna(0)
    e50_s = ta.trend.ema_indicator(df_res['c'], window=ema50_win).fillna(0)
    
    rsi_val = rsi_s.iloc[-1] if not rsi_s.empty else 0.0
    e20_val = e20_s.iloc[-1] if not e20_s.empty else 0.0
    e50_val = e50_s.iloc[-1] if not e50_s.empty else 0.0
    
    sig = "বুলিশ ✅" if original_price > e20_val else "বেয়ারিশ ❌"
    pats = get_advanced_pats(df_res)
    
    return {
        "rsi": round(clean_float(rsi_val), 1),
        "ema20": round(clean_float(e20_val), 2),
        "ema50": round(clean_float(e50_val), 2),
        "sig": sig,
        "pats": pats
    }

def format_seconds_to_bengali(sec):
    sec = int(sec)
    if sec <= 0:
        return "খুব কাছাকাছি"
    weeks = sec // (7 * 24 * 3600)
    sec %= (7 * 24 * 3600)
    days = sec // (24 * 3600)
    sec %= (24 * 3600)
    hours = sec // 3600
    sec %= 3600
    minutes = sec // 60
    seconds = sec % 60
    
    parts = []
    if weeks > 0:
        parts.append(f"{weeks} সপ্তাহ")
    if days > 0:
        parts.append(f"{days} দিন")
    if hours > 0:
        parts.append(f"{hours} ঘণ্টা")
    if minutes > 0:
        parts.append(f"{minutes} মিনিট")
    if seconds > 0 and len(parts) < 2:
        parts.append(f"{seconds} সেকেন্ড")
        
    return " ".join(parts) if parts else "খুব কাছাকাছি"
