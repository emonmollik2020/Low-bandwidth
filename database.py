# database.py (স্টেট ও ডাটাবেজ হ্যান্ডেলার - মডেল স্টোর আলাদা করা সংস্করণ)
# BANDWIDTH OPTIMIZATION: 
#   - btc_15m_history table added for BTC data sync
#   - ai_model_store table creation commented out (can re-enable later)
#   - save_model_to_db() and load_model_from_db() commented out
#   - safe_save_state() called less frequently from bot.py
import json
import os
import base64
import pickle
import asyncio
from sqlalchemy import create_engine, text
from config import DATABASE_URL, DB_ENABLED, STATE_FILE, state_manager, live_bus

engine = create_engine(DATABASE_URL) if DB_ENABLED else None

# গ্লোবাল ফ্ল্যাগ (ক্লাউড কানেকশন ট্র্যাকার)
_state_loaded_from_db = False  

def init_db_tables():
    if not DB_ENABLED:
        return
    try:
        with engine.connect() as conn:
            # ১৫ মিনিটের হিস্টোরি টেবিল (SOL)
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS sol_15m_history (
                t BIGINT PRIMARY KEY,
                o DOUBLE PRECISION,
                h DOUBLE PRECISION,
                l DOUBLE PRECISION,
                c DOUBLE PRECISION,
                v DOUBLE PRECISION
            );
            """))
            # ১৫ মিনিটের হিস্টোরি টেবিল (BTC) — নতুন যুক্ত
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS btc_15m_history (
                t BIGINT PRIMARY KEY,
                o DOUBLE PRECISION,
                h DOUBLE PRECISION,
                l DOUBLE PRECISION,
                c DOUBLE PRECISION,
                v DOUBLE PRECISION
            );
            """))
            # বটের লাইভ স্টেট টেবিল
            conn.execute(text("""
            CREATE TABLE IF NOT EXISTS bot_state (
                key TEXT PRIMARY KEY,
                state_data JSONB
            );
            """))
            # সম্পূর্ণ আলাদা এআই মডেল স্টোর টেবিল — BANDWIDTH OPTIMIZATION: বন্ধ (পরবর্তীতে চালু করা যাবে)
            # conn.execute(text("""
            # CREATE TABLE IF NOT EXISTS ai_model_store (
            #     key TEXT PRIMARY KEY,
            #     state_data JSONB
            # );
            # """))
            conn.commit()
        print("Database tables initialized successfully.")
    except Exception as e:
        print(f"Error initializing database tables: {e}")

def save_state(d):
    state_manager.update(d)
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(d, f)
    except Exception as e:
        print(f"Local file write error: {e}")
        
    if DB_ENABLED:
        try:
            state_json = json.dumps(d)
            with engine.connect() as conn:
                query = """
                INSERT INTO bot_state (key, state_data) 
                VALUES ('current_state', :state_data) 
                ON CONFLICT (key) 
                DO UPDATE SET state_data = EXCLUDED.state_data;
                """
                conn.execute(text(query), {"state_data": state_json})
                conn.commit()
        except Exception as e:
            print(f"Cloud state backup error: {e}")

def load_state(force_reload=False):
    global _state_loaded_from_db
    cur_state = state_manager.get()
    
    if _state_loaded_from_db and not force_reload:
        return cur_state
        
    loaded_state = cur_state.copy()
    if DB_ENABLED:
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT state_data FROM bot_state WHERE key = 'current_state'")).fetchone()
                if result:
                    db_state = result[0]
                    if isinstance(db_state, str):
                        db_state = json.loads(db_state)
                    
                    if isinstance(db_state, dict):
                        for k, v in cur_state.items():
                            loaded_state[k] = db_state.get(k, v)
                        state_manager.update(loaded_state)
                        _state_loaded_from_db = True
                        print("Successfully recovered last bot state from Supabase Cloud.")
                        return loaded_state
        except Exception as e:
            print(f"Could not load state from Cloud Database: {e}")
        
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                local_state = json.load(f)
                for k, v in cur_state.items():
                    loaded_state[k] = local_state.get(k, v)
            state_manager.update(loaded_state)
            _state_loaded_from_db = True
            return loaded_state
        except Exception:
            pass
            
    _state_loaded_from_db = True
    return loaded_state

# BANDWIDTH OPTIMIZATION: AI মডেল সেভ করা বন্ধ — পরবর্তীতে চালু করতে হলে কমেন্ট সরিয়ে ফেলুন
# def save_model_to_db(model):
#     """এআই মডেলটিকে বাইনারি থেকে Base64 টেক্সটে রূপান্তর করে 'ai_model_store' আলাদা টেবিলে সেভ করা"""
#     if not DB_ENABLED:
#         return
#     try:
#         model_bytes = pickle.dumps(model)
#         model_b64 = base64.b64encode(model_bytes).decode('utf-8')
#         
#         with engine.connect() as conn:
#             query = """
#             INSERT INTO ai_model_store (key, state_data) 
#             VALUES ('ai_model_base64', :state_data) 
#             ON CONFLICT (key) 
#             DO UPDATE SET state_data = EXCLUDED.state_data;
#             """
#             state_json = json.dumps({"b64": model_b64})
#             conn.execute(text(query), {"state_data": state_json})
#             conn.commit()
#         print("🤖 [Database] এআই মডেল সফলভাবে ক্লাউড ডাটাবেজের 'ai_model_store' টেবিলে সেভ হয়েছে।", flush=True)
#     except Exception as e:
#         print(f"❌ [Database] ক্লাউডে মডেল সেভ করতে সমস্যা হয়েছে: {e}", flush=True)

# BANDWIDTH OPTIMIZATION: AI মডেল লোড করা বন্ধ — পরবর্তীতে চালু করতে হলে কমেন্ট সরিয়ে ফেলুন
# def load_model_from_db():
#     """'ai_model_store' আলাদা টেবিল থেকে এআই মডেল ডাউনলোড করে পাইথন অবজেক্টে রূপান্তর করা"""
#     if not DB_ENABLED:
#         return None
#     try:
#         with engine.connect() as conn:
#             result = conn.execute(text("SELECT state_data FROM ai_model_store WHERE key = 'ai_model_base64'")).fetchone()
#             if result:
#                 db_data = result[0]
#                 if isinstance(db_data, str):
#                     db_data = json.loads(db_data)
#                 
#                 model_b64 = db_data.get("b64")
#                 if model_b64:
#                     model_bytes = base64.b64decode(model_b64.encode('utf-8'))
#                     return pickle.loads(model_bytes)
#     except Exception as e:
#         print(f"⚠️ [Database] ক্লাউড থেকে মডেল লোড করা যায়নি: {e}", flush=True)
#     return None

# ক্লাউড ও ফাইলে ডাটা সেভ করার স্টেট আপডেট ফাংশন
async def safe_save_state(updates):
    state_manager.update(updates)
    full_state = state_manager.get()
    await asyncio.to_thread(save_state, full_state)
    await live_bus.broadcast(full_state)
    return full_state

# মেমোরি-অনলি লাইভ স্টেট আপডেট এবং ব্রডকাস্ট (ব্লকিং-মুক্ত)
async def safe_update_live_state(updates):
    state_manager.update(updates)
    full_state = state_manager.get()
    await live_bus.broadcast(full_state)
    return full_state
