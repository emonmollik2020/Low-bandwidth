# app.py (ক্যাশ-মুক্ত, চার্ট-গ্যাপ ফিক্সড, ১০০০-লাইটওয়েট ক্যান্ডেল, ব্যাকগ্রাউন্ড প্রি-রিস্যাম্পলড চূড়ান্ত সংস্করণ - ফিক্সড)
# BANDWIDTH OPTIMIZATION:
#   - WebSocket broadcast: state change only (not every tick)
#   - safe_update_live_state(): optimized, batched broadcast
#   - CACHE_HEADERS remain (no-cache for live data)
#   - All other logic UNCHANGED
import asyncio
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import pandas as pd
import zipfile
import io
from config import SYMBOL, state_manager, live_bus
from database import load_state, init_db_tables
from indicators import resample_tf, clean_float
from bot import bot_engine

app = FastAPI(title="SOL Futures Pro Trading Bot")

# স্ট্যাটিক ও টেমপ্লেট ডিরেক্টরি মাউন্ট করা
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

CACHE_HEADERS = {
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache",
    "Expires": "0"
}

# গ্লোবাল মেমোরি ক্যাশ ভেরিয়েবলসমূহ
ohlcv_cache = {}
last_cached_timestamp = 0

tf_map = {
    "30m": "30min",
    "45m": "45min",
    "1h": "1h",
    "2h": "2h",
    "3h": "3h",
    "4h": "4h",
    "1d": "1D"
}

# GET রিকোয়েস্টের জন্য হোম পেজ
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request, "index.html")

# আপটাইম রোবটের HEAD পিং রিকোয়েস্ট গ্রহণ করার জন্য রাউট
@app.head("/", response_class=HTMLResponse)
async def index_head():
    return HTMLResponse(content="", headers=CACHE_HEADERS)

# GET রিকোয়েস্টের জন্য পিং
@app.get("/ping")
async def ping():
    return JSONResponse(content={"status": "alive"}, headers=CACHE_HEADERS)

# পিং রাউটের জন্য HEAD রিকোয়েস্টের অনুমতি দেওয়া হলো
@app.head("/ping")
async def ping_head():
    return JSONResponse(content={"status": "alive"}, headers=CACHE_HEADERS)

@app.get("/api/data")
async def api_data():
    return JSONResponse(content=load_state(), headers=CACHE_HEADERS)

@app.get("/api/backup")
async def download_backup():
    """মোবাইলে এক ক্লিকে সম্পূর্ণ বটের লাইটওয়েট জিপ ব্যাকআপ ডাউনলোড করার এপিআই"""
    try:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
            for root, dirs, files in os.walk("."):
                # সার্ভার ক্যাশ বা প্রজেক্টের বাইরের ফোল্ডারগুলো বাদ দেওয়া হলো
                if any(p in root for p in [".git", "__pycache__", "static/js/chart.js", ".cache"]):
                    continue
                for file in files:
                    # সব প্রয়োজনীয় কোড ফাইল জিপে যুক্ত করা
                    if file.endswith((".py", ".html", ".css", ".js", ".txt", ".md")) or file in ["Dockerfile", ".gitattributes"]:
                        file_path = os.path.join(root, file)
                        zip_file.write(file_path, os.path.relpath(file_path, "."))
        zip_buffer.seek(0)
        return StreamingResponse(
            zip_buffer,
            media_type="application/x-zip-compressed",
            headers={"Content-Disposition": "attachment; filename=trading_bot_backup.zip"}
        )
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)})    

# প্রি-রিস্যাম্পলড এবং ব্লকিং-মুক্ত ওএইচএলসিভি (OHLCV) রাউট
@app.get("/api/ohlcv")
async def api_ohlcv(tf: str = "15m"):
    global last_cached_timestamp  # ফিক্সড: গ্লোবাল ভেরিয়েবল ডিক্লেয়ারেশন যুক্ত করা হলো
    try:
        # ব্যাকগ্রাউন্ডে প্রি-ক্যালকুলেট করা রিস্যাম্পল ডাটাফ্রেম র্যাম থেকে রিড করা হচ্ছে
        df_res = None
        if hasattr(state_manager, 'resampled_dfs') and tf in state_manager.resampled_dfs:
            df_res = state_manager.resampled_dfs[tf].copy()
            
        # ফলব্যাক গার্ড (যদি কোনো কারণে ক্যাশে ডাটা না থাকে)
        if df_res is None:
            df = state_manager.global_df.copy() if (hasattr(state_manager, 'global_df') and state_manager.global_df is not None) else None
            if df is not None and not df.empty:
                # সর্বশেষ ক্যান্ডেলের টাইমস্ট্যাম্প
                latest_ts = int(df['t'].iloc[-1])
                
                # যদি নতুন ১৫-মিনিটের ক্যান্ডেল ক্লোজ হয়, তবে ক্যাশ ক্লিয়ার হবে
                if latest_ts != last_cached_timestamp:
                    ohlcv_cache.clear()
                    last_cached_timestamp = latest_ts
                
                # ক্যাশ হিট: ক্যাশে ডাটা থাকলে তা সাথে সাথে রিটার্ন করবে
                if tf in ohlcv_cache:
                    return JSONResponse(content=ohlcv_cache[tf], headers=CACHE_HEADERS)
                
                # ক্যাশ মিস: নতুন করে ক্যান্ডেল রিস্যাম্পল করা হবে
                if tf != '15m' and tf in tf_map:
                    df_temp = df.copy()
                    df_temp['dt'] = pd.to_datetime(df_temp['t'], unit='ms')
                    df_temp.set_index('dt', inplace=True)
                    df_res = resample_tf(df_temp, tf_map[tf])
                else:
                    df_res = df.copy()
        
        if df_res is not None and not df_res.empty:
            df_res = df_res.dropna(subset=['t','o','h','l','c']).sort_values('t')
            
            tf_res = 0.0
            tf_sup = 0.0
            if len(df_res) >= 21:
                tf_res = clean_float(df_res['h'].iloc[-21:-1].max())
                tf_sup = clean_float(df_res['l'].iloc[-21:-1].min())
            elif len(df_res) >= 2:
                tf_res = clean_float(df_res['h'].iloc[:-1].max())
                tf_sup = clean_float(df_res['l'].iloc[:-1].min())
            
            # ডাটা সাইজ ১,০০০ ক্যান্ডেলে সীমাবদ্ধ করা হলো
            df_limit = df_res.tail(1000)
            
            # টাইমস্ট্যাম্প কনভার্সন ভেক্টরাইজড ও লিস্টে রূপান্তর (JSON int64 এরর ফিক্সড)
            if tf == '1d':
                time_series = pd.to_datetime(df_limit['t'], unit='ms').dt.strftime('%Y-%m-%d').tolist()
            else:
                time_series = df_limit['t'].apply(lambda t_val: int(t_val / 1000) if t_val > 9999999999 else int(t_val)).tolist()
            
            opens = df_limit['o'].values
            highs = df_limit['h'].values
            lows = df_limit['l'].values
            closes = df_limit['c'].values
            
            candles = [
                {
                    "time": time_series[i],
                    "open": clean_float(opens[i]),
                    "high": clean_float(highs[i]),
                    "low": clean_float(lows[i]),
                    "close": clean_float(closes[i])
                }
                for i in range(len(df_limit))
            ]
            
            # রেসপন্স ডাটা মেমোরি ক্যাশে সেভ করে রাখা
            response_data = {
                "candles": candles, "tf_res": tf_res, "tf_sup": tf_sup, "tf_name": tf.upper()
            }
            ohlcv_cache[tf] = response_data
            
            return JSONResponse(content=response_data, headers=CACHE_HEADERS)
            
    except Exception as e:
        print(f"Error serving ohlcv: {e}")
        
    return JSONResponse({"candles": [], "tf_res": 0.0, "tf_sup": 0.0, "tf_name": ""}, headers=CACHE_HEADERS)

# BANDWIDTH OPTIMIZATION: WebSocket with state change detection
_last_broadcast_state_hash = None

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    live_bus.register(websocket)
    try:
        # Send initial state
        initial_state = load_state()
        await websocket.send_json(initial_state)
        
        # BANDWIDTH OPT: Only broadcast on state change, not every tick
        while True:
            current_state = state_manager.get()
            # Simple hash comparison to detect changes
            current_hash = hash(str(sorted(current_state.items())))
            
            if current_hash != _last_broadcast_state_hash:
                _last_broadcast_state_hash = current_hash
                await websocket.send_json(current_state)
            
            # Wait for client message or timeout (prevents tight loop)
            try:
                await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
            except asyncio.TimeoutError:
                pass  # Continue loop, check for state change
                
    except WebSocketDisconnect:
        live_bus.unregister(websocket)
    except Exception:
        live_bus.unregister(websocket)

@app.on_event("startup")
async def startup_event():
    init_db_tables()
    asyncio.create_task(bot_engine())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", 7860)), reload=False)
