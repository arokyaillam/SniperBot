import redis
import json
import time
import threading
import httpx
import sys

# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6380
API_URL = "http://127.0.0.1:8000"
SPOT_SYMBOL = "NSE_INDEX|Nifty 50"
SPOT_PRICE = 24210.0  # Expected ATM: 24200
EXPIRY = "2025-11-28" # Adjust as needed or make dynamic

def setup_mock_redis():
    """
    Sets up mock contract data in Redis.
    """
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        print(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        
        # Mock Strikes: ATM +/- 2 (24100 to 24300)
        strikes = [24100, 24150, 24200, 24250, 24300]
        count = 0
        
        print("Setting mock contract keys...")
        for strike in strikes:
            for opt_type in ["CE", "PE"]:
                # Key format from ContractManager
                key = f"CONTRACT:NIFTY:{EXPIRY}:{strike}:{opt_type}"
                value = json.dumps({
                    "instrument_key": f"NSE_FO|{strike}{opt_type}",
                    "lot_size": 50
                })
                r.set(key, value)
                count += 1
                
        print(f"Set {count} mock contract keys.")
        return r
    except Exception as e:
        print(f"Error connecting to Redis: {e}")
        sys.exit(1)

def publish_ticks(r):
    """
    Publishes mock spot price ticks to Redis.
    """
    print(f"Publishing mock ticks for {SPOT_SYMBOL} at {SPOT_PRICE}...")
    tick = {
        "instrument_key": SPOT_SYMBOL,
        "feeds": {
            SPOT_SYMBOL: {
                "ltpc": {
                    "ltp": SPOT_PRICE
                }
            }
        }
    }
    
    # Keep publishing for 10 seconds
    for _ in range(20):
        r.publish("live_ticks", json.dumps(tick))
        time.sleep(0.5)

def trigger_setup():
    """
    Calls the API endpoint.
    """
    # 0. Start Feed (to initialize MARKET_FEED)
    print("Starting Feed...")
    try:
        # Using a dummy symbol to initialize the feed object
        httpx.get(f"{API_URL}/start-feed?symbols={SPOT_SYMBOL}")
    except Exception as e:
        print(f"Warning starting feed: {e}")

    # Wait a bit for ticks to start flowing
    time.sleep(2)
    
    print("\nTriggering Morning Setup via API...")
    try:
        response = httpx.post(f"{API_URL}/run-morning-setup")
        print(f"Status Code: {response.status_code}")
        print("Response:")
        print(json.dumps(response.json(), indent=2))
    except Exception as e:
        print(f"Error calling API: {e}")

def main():
    # 1. Setup Redis Data
    r = setup_mock_redis()
    
    # 2. Start Tick Publisher in Background
    tick_thread = threading.Thread(target=publish_ticks, args=(r,))
    tick_thread.daemon = True
    tick_thread.start()
    
    # 3. Trigger Setup
    trigger_setup()
    
    print("\nTest Complete.")

if __name__ == "__main__":
    main()
