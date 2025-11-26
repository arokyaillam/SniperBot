import asyncio
import time
from app.worker.resampler import Resampler
from app.core.database import init_db

async def test_resampler():
    print("Initializing Database...")
    await init_db()
    
    resampler = Resampler()
    await resampler.start()
    
    symbol = "NSE_INDEX|Nifty 50"
    
    # Simulate Ticks for a single minute
    # Tick 1: Open
    tick1 = {
        "fullFeed": {
            "marketFF": {
                "ltpc": {"ltp": 24100.0, "volume": 1000000},
                "marketOHLC": {"oi": 50000},
                "marketLevel": {
                    "totalBuyQty": 1000, "totalSellQty": 2000,
                    "bidAskQuote": [{"bidP": 24099, "askP": 24101, "bidQ": 100, "askQ": 100}]
                }
            },
            "optionGreeks": {"iv": 12.5, "delta": 0.5, "theta": -10, "gamma": 0.02, "vega": 5}
        }
    }
    
    # Tick 2: High
    tick2 = {
        "fullFeed": {
            "marketFF": {
                "ltpc": {"ltp": 24150.0, "volume": 1000500},
                "marketOHLC": {"oi": 50100},
                "marketLevel": {
                    "totalBuyQty": 1100, "totalSellQty": 2100,
                    "bidAskQuote": [{"bidP": 24149, "askP": 24151, "bidQ": 150, "askQ": 150}]
                }
            },
            "optionGreeks": {"iv": 12.6, "delta": 0.55, "theta": -11, "gamma": 0.02, "vega": 5.1}
        }
    }
    
    # Tick 3: Low
    tick3 = {
        "fullFeed": {
            "marketFF": {
                "ltpc": {"ltp": 24090.0, "volume": 1001000},
                "marketOHLC": {"oi": 50200},
                "marketLevel": {
                    "totalBuyQty": 1200, "totalSellQty": 2200,
                    "bidAskQuote": [{"bidP": 24089, "askP": 24091, "bidQ": 200, "askQ": 200}]
                }
            },
            "optionGreeks": {"iv": 12.4, "delta": 0.45, "theta": -9, "gamma": 0.02, "vega": 4.9}
        }
    }

    # Tick 4: Close (Next Minute trigger)
    # To trigger the storage of the previous minute, we need to send a tick from the NEXT minute.
    # We'll artificially set the timestamp in the resampler logic or just wait.
    # But since Resampler uses system time, we can't easily mock time without patching.
    # Instead, we will manually inspect the `current_candles` state after ticks.
    
    print("Processing Tick 1 (Open 24100)...")
    await resampler.process_tick(symbol, tick1)
    
    print("Processing Tick 2 (High 24150)...")
    await resampler.process_tick(symbol, tick2)
    
    print("Processing Tick 3 (Low 24090)...")
    await resampler.process_tick(symbol, tick3)
    
    # Verify internal state
    candle = resampler.current_candles.get(symbol)
    if candle:
        print("\nCurrent Candle State:")
        print(f"Open: {candle['open']} (Expected 24100.0)")
        print(f"High: {candle['high']} (Expected 24150.0)")
        print(f"Low: {candle['low']} (Expected 24090.0)")
        print(f"Close: {candle['close']} (Expected 24090.0)")
        print(f"Volume: {candle['volume']} (Expected 1001000)")
    else:
        print("Error: No candle found in resampler.")

    # Force store by simulating a tick from the future
    print("\nSimulating tick from next minute to force DB insert...")
    # We need to hack the timestamp logic in process_tick to force a new minute
    # Since process_tick uses parsed['timestamp'] which comes from time.time(),
    # we can't easily force it unless we wait 60s.
    # ALTERNATIVE: We call store_candle directly for testing.
    
    if candle:
        print("Manually triggering store_candle...")
        await resampler.store_candle(symbol, candle)
        
        # Verify DB
        print("\nVerifying Database...")
        conn = await resampler.db_pool.acquire()
        row = await conn.fetchrow("SELECT * FROM market_candles WHERE symbol = $1 ORDER BY timestamp DESC LIMIT 1", symbol)
        await resampler.db_pool.release(conn)
        
        if row:
            print("SUCCESS: Candle found in DB!")
            print(f"Timestamp: {row['timestamp']}")
            print(f"OHLC: {row['open']}, {row['high']}, {row['low']}, {row['close']}")
            print(f"Volume: {row['volume']}")
        else:
            print("FAILURE: No candle found in DB.")

    await resampler.stop()

if __name__ == "__main__":
    asyncio.run(test_resampler())
