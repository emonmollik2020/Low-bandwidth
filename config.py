# config.py (বটের গ্লোবাল কনফিগারেশন)
# BANDWIDTH OPTIMIZATION:
#   - Minor: Added cache variables for batch state save timing
#   - All other settings UNCHANGED
import os
import asyncio
import pandas as pd

SYMBOL = "SOL/USDT:USDT"
STATE_FILE = "bot_state.json"
HISTORY_FILE = "sol_15m_history.csv"
MODEL_FILE = "trading_model.pkl"  # এআই ইঞ্জিনের জন্য মডেল ফাইলের নাম (ব্যান্ডউইথ অপ্ট: এখন ব্যবহার হচ্ছে না)
INITIAL_FUND = 100.0

# ১ বছরের ডাটা ধারণ করার জন্য লিমিট বাড়িয়ে ৪০,০০০ করা হলো
MAX_CANDLES_TO_KEEP = 40000  

LEVERAGE = 10
RISK_FRACTION = 0.02 # প্রতিটি ট্রেডে সর্বোচ্চ ২% রিস্ক
DEF_TP = 0.035  
DEF_SL = 0.020  
COOLDOWN_SECONDS = 900

DATABASE_URL = os.environ.get("DATABASE_URL")
DB_ENABLED = bool(DATABASE_URL)

# BANDWIDTH OPTIMIZATION: Batch state save configuration
STATE_SAVE_INTERVAL = 5  # Minimum seconds between non-critical state saves
BTC_SYNC_INTERVAL = 15 * 60  # 15 minutes (candle close interval)

# ড্যাশবোর্ডের লাইভ কানেকশন আপডেট ব্রডকাস্টার (ওয়েবসোকেট)
class LiveEventBus:
    def __init__(self):
        self.connections = set()

    def register(self, websocket):
        self.connections.add(websocket)

    def unregister(self, websocket):
        self.connections.discard(websocket)

    async def broadcast(self, data):
        for connection in list(self.connections):
            try:
                await connection.send_json(data)
            except Exception:
                self.connections.discard(connection)

live_bus = LiveEventBus()

# থ্রেড এবং অ্যাসিনক্রোনাস নিরাপদ স্টেট ম্যানেজার
class StateManager:
    def __init__(self):
        self._state = {
            "price": 0.0,
            "balance": INITIAL_FUND,
            "total_pnl": 0.0,
            "last_update": "...",
            "trades": 0,
            "win_rate": 0,
            "best": 0.0,
            "worst": 0.0,
            "last_action": "---",
            "in_position": False,
            "position_type": "NONE", 
            "peak_p": 0.0,            
            "valley_p": 0.0,          
            "live_pnl_pct": 0.0,
            "live_pnl_val": 0.0,
            "entry_price": 0.0,
            "sl_level": 0.0,
            "tp_level": 0.0,
            "pos_size": 0.0,  
            "margin": 0.0,    
            "entry_sl_pct": 0.0, 
            "last_trade_time": 0.0,
            "estimated_time": "লোড হচ্ছে...",
            "pdh": 0.0,
            "pdl": 0.0,
            "h4_res": 0.0,
            "h4_sup": 0.0,
            "analysis_15m": {"rsi": 0, "ema20": 0, "ema50": 0, "vwap": 0, "sig": "লোড হচ্ছে...", "pats": []},
            "analysis_30m": {"rsi": 0, "ema20": 0, "ema50": 0, "sig": "লোড হচ্ছে...", "pats": []},
            "analysis_45m": {"rsi": 0, "ema20": 0, "ema50": 0, "sig": "লোড হচ্ছে...", "pats": []},
            "analysis_1h": {"rsi": 0, "ema20": 0, "ema50": 0, "ema200": 0, "btc_price": 0, "sig": "লোড হচ্ছে...", "pats": []},
            "analysis_2h": {"rsi": 0, "ema20": 0, "ema50": 0, "sig": "লোড হচ্ছে...", "pats": []},
            "analysis_3h": {"rsi": 0, "ema20": 0, "ema50": 0, "sig": "লোড হচ্ছে...", "pats": []},
            "analysis_4h": {"rsi": 0, "ema20": 0, "ema50": 0, "sig": "লোড হচ্ছে...", "pats": []},
            "analysis_1d": {"rsi": 0, "ema20": 0, "ema50": 0, "sig": "লোড হচ্ছে...", "pats": []},
            "confluences": {
                "macro_bullish": False, "btc_bullish": False, "vwap_long": False, "volume_confirmed": False,
                "ema_long": False, "macd_long": False, "bull_signal": False,
                "macro_bearish": False, "btc_bearish": False, "vwap_short": False,
                "ema_short": False, "macd_short": False, "bear_signal": False
            },
            "exit_conditions": { 
                "sl_safe": True, "tp_safe": True, "ema50_safe": True, "rsi_safe": True, "is_breakeven": False
            },
            "wait_reason": "লোড হচ্ছে...",
            "ai_thoughts": "এআই মার্কেট স্ট্রাকচার এবং ঐতিহাসিক ক্যান্ডেলস্টিক প্যাটার্ন বিশ্লেষণ করছে... 🧠",  # নতুন যুক্ত করা হলো
            "log": [],
            "history": []
        }
        self.candle_cache_btc = []
        self.resampled_dfs = {}  # প্রি-রিস্যাম্পলড ডাটাফ্রেমগুলো র্যামে রাখার জন্য
        self.global_df = pd.DataFrame()

    def get(self):
        return self._state.copy()

    def update(self, data):
        self._state.update(data)

state_manager = StateManager()
