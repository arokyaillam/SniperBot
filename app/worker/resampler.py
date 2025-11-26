import asyncio
import json
import time
from datetime import datetime
import asyncpg
from app.core.config import settings

class Resampler:
    def __init__(self):
        self.current_candles = {}  # {symbol: {data_points}}
        self.db_pool = None

    async def start(self):
        """Initialize DB pool."""
        self.db_pool = await asyncpg.create_pool(
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            database=settings.POSTGRES_DB,
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT
        )
        print("DEBUG: Resampler DB pool created.")

    async def stop(self):
        """Close DB pool."""
        if self.db_pool:
            await self.db_pool.close()

    def parse_full_data(self, data):
        """
        Parses the 'fullFeed' structure from Upstox WebSocket.
        Extracts OHLC, Volume, OI, Greeks, and Market Depth Walls.
        """
        try:
            # Structure: feeds[token]['fullFeed']['marketFF']
            # We assume 'data' passed here is the inner dictionary for a specific token
            # e.g. data = feeds[token]
            
            if "fullFeed" not in data or "marketFF" not in data["fullFeed"]:
                return None

            market_ff = data["fullFeed"]["marketFF"]
            
            # 1. Basic Data
            ltp = float(market_ff.get("ltpc", {}).get("ltp", 0))
            vtt = int(market_ff.get("ltpc", {}).get("volume", 0)) # Volume Traded Today
            oi = int(market_ff.get("marketOHLC", {}).get("oi", 0))
            
            # 2. Greeks
            greeks = data["fullFeed"].get("optionGreeks", {})
            iv = float(greeks.get("iv", 0))
            delta = float(greeks.get("delta", 0))
            theta = float(greeks.get("theta", 0))
            gamma = float(greeks.get("gamma", 0))
            vega = float(greeks.get("vega", 0))
            
            # 3. Depth Analysis (Walls)
            market_level = market_ff.get("marketLevel", {})
            bid_ask_quote = market_level.get("bidAskQuote", [])
            
            total_buy_qty = int(market_level.get("totalBuyQty", 0))
            total_sell_qty = int(market_level.get("totalSellQty", 0))
            
            best_bid = 0.0
            best_ask = 0.0
            max_buy_wall_price = 0.0
            max_buy_wall_qty = -1
            max_sell_wall_price = 0.0
            max_sell_wall_qty = -1
            
            if bid_ask_quote:
                # Best Bid/Ask are at index 0
                best_bid = float(bid_ask_quote[0].get("bidP", 0))
                best_ask = float(bid_ask_quote[0].get("askP", 0))
                
                # Loop for Walls
                for level in bid_ask_quote:
                    # Buy Side
                    bid_qty = int(level.get("bidQ", 0))
                    bid_price = float(level.get("bidP", 0))
                    if bid_qty > max_buy_wall_qty:
                        max_buy_wall_qty = bid_qty
                        max_buy_wall_price = bid_price
                        
                    # Sell Side
                    ask_qty = int(level.get("askQ", 0))
                    ask_price = float(level.get("askP", 0))
                    if ask_qty > max_sell_wall_qty:
                        max_sell_wall_qty = ask_qty
                        max_sell_wall_price = ask_price

            return {
                "ltp": ltp,
                "vtt": vtt,
                "oi": oi,
                "total_buy_qty": total_buy_qty,
                "total_sell_qty": total_sell_qty,
                "iv": iv,
                "delta": delta,
                "theta": theta,
                "gamma": gamma,
                "vega": vega,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "max_buy_wall_price": max_buy_wall_price,
                "max_buy_wall_qty": max_buy_wall_qty,
                "max_sell_wall_price": max_sell_wall_price,
                "max_sell_wall_qty": max_sell_wall_qty,
                "timestamp": int(time.time()) # Current processing time
            }
            
        except Exception as e:
            print(f"Error parsing full data: {e}")
            return None

    async def process_tick(self, symbol, raw_data):
        """
        Processes a single tick. Aggregates into 1-minute candles.
        """
        parsed = self.parse_full_data(raw_data)
        if not parsed:
            return

        # Determine current minute bucket
        ts = parsed["timestamp"]
        minute_ts = (ts // 60) * 60
        
        if symbol not in self.current_candles:
            self.current_candles[symbol] = {
                "minute_ts": minute_ts,
                "open": parsed["ltp"],
                "high": parsed["ltp"],
                "low": parsed["ltp"],
                "close": parsed["ltp"],
                "volume": parsed["vtt"], # Will be updated to max(vtt)
                "last_tick": parsed # Store full last tick for snapshot values
            }
        else:
            candle = self.current_candles[symbol]
            
            # Check if new minute
            if minute_ts > candle["minute_ts"]:
                # Finalize and Store previous candle
                await self.store_candle(symbol, candle)
                
                # Start new candle
                self.current_candles[symbol] = {
                    "minute_ts": minute_ts,
                    "open": parsed["ltp"],
                    "high": parsed["ltp"],
                    "low": parsed["ltp"],
                    "close": parsed["ltp"],
                    "volume": parsed["vtt"],
                    "last_tick": parsed
                }
            else:
                # Update existing candle
                candle["high"] = max(candle["high"], parsed["ltp"])
                candle["low"] = min(candle["low"], parsed["ltp"])
                candle["close"] = parsed["ltp"]
                candle["volume"] = max(candle["volume"], parsed["vtt"]) # Max VTT
                candle["last_tick"] = parsed # Always update to latest for snapshot

    async def store_candle(self, symbol, candle):
        """
        Inserts the aggregated candle into the database.
        """
        if not self.db_pool:
            return

        last_tick = candle["last_tick"]
        
        # Convert timestamp to datetime
        dt = datetime.fromtimestamp(candle["minute_ts"])
        
        query = """
        INSERT INTO market_candles (
            timestamp, symbol,
            open, high, low, close, volume,
            open_interest, total_buy_qty, total_sell_qty,
            iv, delta, theta, gamma, vega,
            best_bid, best_ask,
            max_buy_wall_price, max_buy_wall_qty,
            max_sell_wall_price, max_sell_wall_qty
        ) VALUES (
            $1, $2,
            $3, $4, $5, $6, $7,
            $8, $9, $10,
            $11, $12, $13, $14, $15,
            $16, $17,
            $18, $19,
            $20, $21
        )
        ON CONFLICT (timestamp, symbol) DO NOTHING;
        """
        
        try:
            await self.db_pool.execute(
                query,
                dt, symbol,
                candle["open"], candle["high"], candle["low"], candle["close"], candle["volume"],
                last_tick["oi"], last_tick["total_buy_qty"], last_tick["total_sell_qty"],
                last_tick["iv"], last_tick["delta"], last_tick["theta"], last_tick["gamma"], last_tick["vega"],
                last_tick["best_bid"], last_tick["best_ask"],
                last_tick["max_buy_wall_price"], last_tick["max_buy_wall_qty"],
                last_tick["max_sell_wall_price"], last_tick["max_sell_wall_qty"]
            )
            # print(f"DEBUG: Stored candle for {symbol} at {dt}")
        except Exception as e:
            print(f"Error storing candle: {e}")

resampler = Resampler()
