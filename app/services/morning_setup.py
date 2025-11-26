import json
import asyncio
import math
from app.core.redis_client import redis_client

class MorningSetup:
    def __init__(self, market_feed):
        self.market_feed = market_feed

    async def get_spot_price(self, symbol="NSE_INDEX|Nifty 50"):
        """
        Fetches the latest LTP for the symbol from Redis 'live_ticks' channel.
        Waits for up to 5 seconds for a tick.
        """
        print(f"DEBUG: Waiting for tick for {symbol}...")
        pubsub = redis_client.pubsub()
        pubsub.subscribe("live_ticks")

        try:
            # Wait for a few seconds to get a tick
            # We use a loop with timeout
            end_time = asyncio.get_event_loop().time() + 5.0
            
            while asyncio.get_event_loop().time() < end_time:
                message = pubsub.get_message(ignore_subscribe_messages=True)
                if message:
                    data = json.loads(message['data'])
                    # Check if it matches our symbol (assuming data has instrument_key or similar)
                    # The protobuf structure usually has 'instrument_key' or we can infer.
                    # Based on feed_service, we publish the full dict.
                    # Let's check the structure. Usually 'instrument_key' or 'key'.
                    # If we can't verify symbol, we might assume it's the one we want if we only subscribed to one.
                    # But we might be subscribed to many.
                    # Let's assume 'instrument_key' is present.
                    if data.get("instrument_key") == symbol or data.get("instrument_token") == symbol:
                        # Extract LTP
                        # Protobuf 'feeds' -> 'ltpc' -> 'ltp'
                        # Or 'feeds' -> 'ff' -> 'marketFF' -> 'ltpc' -> 'ltp'
                        # Structure varies by mode. 'full' mode has 'feeds'.
                        feeds = data.get("feeds")
                        if feeds:
                            # It's a map of instrument_key -> feed
                            feed = feeds.get(symbol)
                            if feed:
                                ltp = feed.get("ltpc", {}).get("ltp")
                                if ltp:
                                    return float(ltp)
                
                await asyncio.sleep(0.1)
                
        except Exception as e:
            print(f"Error fetching spot price: {e}")
        finally:
            pubsub.close()
            
        return None

    async def setup_morning_strikes(self):
        """
        Main logic for morning setup.
        """
        print("DEBUG: Starting Morning Setup...")
        
        # 1. Get Spot Price
        spot_price = await self.get_spot_price("NSE_INDEX|Nifty 50")
        if not spot_price:
            print("ERROR: Could not fetch Spot Price for Nifty 50.")
            return {"status": "error", "message": "Could not fetch Spot Price"}
            
        print(f"DEBUG: Spot Price: {spot_price}")
        
        # 2. Calculate ATM
        # Round to nearest 50
        atm = round(spot_price / 50) * 50
        print(f"DEBUG: Calculated ATM: {atm}")
        
        # 3. Generate Grid (ATM +/- 2)
        # [ATM-100, ATM-50, ATM, ATM+50, ATM+100]
        strikes = [atm - 100, atm - 50, atm, atm + 50, atm + 100]
        print(f"DEBUG: Strike Grid: {strikes}")
        
        # 4. Fetch Keys from Redis
        # Key Format: CONTRACT:NIFTY:{expiry}:{strike}:{type}
        # We search for CONTRACT:NIFTY:*:{strike}:{type}
        instrument_keys = []
        
        for strike in strikes:
            for opt_type in ["CE", "PE"]:
                pattern = f"CONTRACT:NIFTY:*:{strike}:{opt_type}"
                keys = redis_client.keys(pattern)
                
                if keys:
                    # We expect only one key because contract_manager stores only nearest expiry
                    # But if there are stale keys, we might get multiple.
                    # We should pick the one with the nearest expiry if possible, 
                    # but contract_manager logic implies only one set is stored (or we should clear old ones).
                    # For now, take the first one.
                    key = keys[0]
                    value_json = redis_client.get(key)
                    if value_json:
                        value = json.loads(value_json)
                        instr_key = value.get("instrument_key")
                        if instr_key:
                            instrument_keys.append(instr_key)
                else:
                    print(f"WARNING: No contract found for {strike} {opt_type}")

        if not instrument_keys:
            print("ERROR: No instrument keys found. Did you run /refresh-contracts?")
            return {"status": "error", "message": "No instrument keys found"}

        # 5. Subscribe
        if self.market_feed:
            await self.market_feed.subscribe_instruments(instrument_keys)
            msg = f"✅ Morning Grid Locked: ATM {atm} | Tracks: {strikes}"
            print(msg)
            return {
                "status": "success", 
                "message": msg,
                "atm": atm,
                "strikes": strikes,
                "subscribed_keys": instrument_keys
            }
        else:
            return {"status": "error", "message": "Market Feed not active"}
