# exchange_helper.py
# BANDWIDTH OPTIMIZATION:
#   - fetch_historical_ohlcv_padded() supports gap-only fetch (since param)
#   - watch_ticker() kept for SOL tick-level price (chart display)
#   - BTC ticker removed (not needed, BTC data from DB)
import asyncio
import ccxt.pro as ccxtpro
import pandas as pd
import time
from config import SYMBOL

class ExchangeHelper:
    def __init__(self):
        self.exchange = ccxtpro.bitget({'enableRateLimit': True})

    async def fetch_ohlcv_strict(self, symbol, timeframe, since=None, limit=None, params=None):
        if params is None:
            params = {}
        return await self.exchange.fetch_ohlcv(symbol, timeframe, since, limit, params)

    async def fetch_historical_ohlcv_padded(self, symbol, timeframe, since_ms=None, until_ms=None):
        """
        BANDWIDTH OPTIMIZATION: Gap-only fetch support.
        If since_ms provided, fetches only from that point (gap fill).
        If not provided, falls back to full historical fetch (first bootstrap).
        """
        if until_ms is None:
            until_ms = int(time.time() * 1000)
        if since_ms is None:
            # Full bootstrap (first time only) — 90 days
            since_ms = int((time.time() - (90 * 24 * 60 * 60)) * 1000)
            
        all_candles = []
        end_time_ms = until_ms
        
        print(f"Fetching historical OHLCV for {symbol} on {timeframe} from {pd.to_datetime(since_ms, unit='ms')} to {pd.to_datetime(until_ms, unit='ms')}...")
        
        while end_time_ms > since_ms:
            try:
                params = {'endTime': end_time_ms}
                batch = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=200, params=params)
                
                if not batch:
                    break
                    
                all_candles.extend(batch)
                
                oldest_ts = batch[0][0]
                
                # Stop if we've reached since_ms
                if oldest_ts <= since_ms:
                    # Filter out candles before since_ms
                    all_candles = [c for c in all_candles if c[0] >= since_ms]
                    break
                    
                if oldest_ts >= end_time_ms:
                    break
                    
                end_time_ms = oldest_ts - 1
                await asyncio.sleep(0.1) # Rate limit protection
                
            except Exception as e:
                print(f"Error in historical fetch pagination: {e}")
                await asyncio.sleep(2)
                
        if all_candles:
            df_all = pd.DataFrame(all_candles, columns=['t', 'o', 'h', 'l', 'c', 'v'])
            df_all = df_all.drop_duplicates(subset=['t']).sort_values('t').reset_index(drop=True)
            return df_all.values.tolist()
        return []

    async def watch_ohlcv(self, symbol, timeframe, limit=None):
        try:
            ohlcv = await self.exchange.watch_ohlcv(symbol, timeframe, limit=limit)
            return ohlcv
        except Exception as e:
            print(f"Error watching WebSocket OHLCV: {e}")
            await asyncio.sleep(2)
            return await self.fetch_ohlcv_strict(symbol, timeframe, limit=limit)

    async def watch_ticker(self, symbol):
        """BANDWIDTH: Kept for SOL tick-level price (chart display). Not used for BTC."""
        try:
            ticker = await self.exchange.watch_ticker(symbol)
            return ticker
        except Exception as e:
            print(f"Error watching WebSocket Ticker: {e}")
            await asyncio.sleep(2)
            return await self.exchange.fetch_ticker(symbol)

    async def close(self):
        await self.exchange.close()

# Indentation ফিক্সড: মডিউল লেভেলে অবজেক্ট ডিক্লেয়ারেশন
exchange_helper = ExchangeHelper()
