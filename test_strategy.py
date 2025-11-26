import redis
import json
import time
import threading

# Configuration
REDIS_HOST = 'localhost'
REDIS_PORT = 6380
SYMBOL = "NSE_FO|24200CE"

def listen_for_signals(r):
    """
    Listens for trade signals.
    """
    pubsub = r.pubsub()
    pubsub.subscribe("trade_signals")
    print("Listening for Trade Signals...")
    
    for message in pubsub.listen():
        if message['type'] == 'message':
            data = json.loads(message['data'])
            print("\n[SIGNAL RECEIVED]")
            print(json.dumps(data, indent=2))
            # Exit after receiving the signal
            return

def run_test():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    # Start Listener
    listener_thread = threading.Thread(target=listen_for_signals, args=(r,))
    listener_thread.daemon = True
    listener_thread.start()
    
    time.sleep(1)
    
    # 1. Publish Previous Candle (Baseline)
    print("\nPublishing Previous Candle (Baseline)...")
    prev_candle = {
        "symbol": SYMBOL,
        "candle": {
            "timestamp": int(time.time()) - 60,
            "open": 100, "high": 110, "low": 90, "close": 100, "volume": 1000,
            "open_interest": 1000, # Baseline OI
            "total_buy_qty": 1000, "total_sell_qty": 1000,
            "delta": 0.3, "gamma": 0.0005,
            "max_sell_wall_price": 120
        }
    }
    r.publish("candle_closed", json.dumps(prev_candle))
    time.sleep(1)
    
    # 2. Publish Current Candle (Strong Buy Scenario)
    print("Publishing Current Candle (Strong Buy)...")
    # Scenario:
    # - Wall Break: Close 150 > Wall 140 (+30)
    # - OI Unwinding: 900 < 1000 (+20)
    # - Pressure: 2000 > 1000 (+20)
    # - Greeks: Delta 0.5 > 0.4 (+10), Gamma 0.002 > 0.001 (+5)
    # - Trend: Close 150 > VWAP ~145 (+15)
    # Total: 100
    
    current_candle = {
        "symbol": SYMBOL,
        "candle": {
            "timestamp": int(time.time()),
            "open": 140, "high": 155, "low": 140, "close": 150, "volume": 5000,
            "open_interest": 900, # Decreased
            "total_buy_qty": 2000, "total_sell_qty": 1000, # Buying Pressure
            "delta": 0.5, "gamma": 0.002,
            "max_sell_wall_price": 140 # Broken
        }
    }
    r.publish("candle_closed", json.dumps(current_candle))
    
    # Wait for signal
    time.sleep(2)
    print("\nTest Finished.")

if __name__ == "__main__":
    run_test()
