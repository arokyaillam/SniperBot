import json
import asyncio
import redis
from app.core.redis_client import redis_client
from app.core.config import settings

class SniperStrategy:
    def __init__(self):
        self.latest_candles = {}  # {instrument_token: candle_data}
        self.redis = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            decode_responses=True
        )

    def calculate_vwap(self, candle):
        """
        Calculates approximate VWAP for the candle.
        """
        return (candle['high'] + candle['low'] + candle['close']) / 3

    def get_strike_grade(self, delta):
        """
        Determines the strike grade based on Delta.
        """
        if 0.4 <= abs(delta) <= 0.6:
            return "ATM/Near-OTM (Prime Target)"
        elif abs(delta) > 0.6:
            return "ITM (High Delta)"
        else:
            return "OTM (Low Delta)"

    def calculate_trade_score(self, candle, prev_candle):
        """
        Calculates the trade score based on multiple factors.
        Total Score: 100
        """
        score = 0
        breakdown = []

        # 1. Wall Break (30 pts)
        # If close > max_sell_wall_price (and wall exists)
        max_sell_wall = candle.get('max_sell_wall_price', 0)
        if max_sell_wall > 0 and candle['close'] > max_sell_wall:
            score += 30
            breakdown.append("Wall Break (+30)")

        # 2. OI Unwinding (20 pts)
        # If OI decreased (Sellers leaving)
        if prev_candle and candle['open_interest'] < prev_candle['open_interest']:
            score += 20
            breakdown.append("OI Unwinding (+20)")

        # 3. Pressure Check (20 pts)
        # If Demand > Supply
        if candle['total_buy_qty'] > candle['total_sell_qty']:
            score += 20
            breakdown.append("Buying Pressure (+20)")

        # 4. Greeks Confirmation (15 pts)
        # Split: Delta (10), Gamma (5)
        delta = candle.get('delta', 0)
        gamma = candle.get('gamma', 0)
        
        if abs(delta) > 0.40:
            score += 10
            breakdown.append("Good Delta (+10)")
        
        if gamma > 0.001:
            score += 5
            breakdown.append("Gamma Accel (+5)")

        # 5. Trend Check (15 pts)
        # If Close > VWAP
        vwap = self.calculate_vwap(candle)
        if candle['close'] > vwap:
            score += 15
            breakdown.append("Above VWAP (+15)")

        return score, breakdown

    def process_candle(self, symbol, candle_data):
        """
        Processes a new candle: calculates score and publishes signal.
        """
        prev_candle = self.latest_candles.get(symbol)
        
        # Calculate Score
        score, breakdown = self.calculate_trade_score(candle_data, prev_candle)
        
        # Determine Signal
        signal_type = "NEUTRAL"
        if score >= 80:
            signal_type = "STRONG BUY"
        elif score >= 60:
            signal_type = "WATCHLIST"
            
        # Strike Grade
        delta = candle_data.get('delta', 0)
        strike_grade = self.get_strike_grade(delta)

        # Log Analysis
        print(f"[{symbol}] Score: {score}/100 | Signal: {signal_type} | Grade: {strike_grade}")
        if breakdown:
            print(f"  -> Factors: {', '.join(breakdown)}")

        # Publish Signal if significant
        if signal_type in ["STRONG BUY", "WATCHLIST"]:
            signal_payload = {
                "symbol": symbol,
                "signal": signal_type,
                "score": score,
                "breakdown": breakdown,
                "strike_grade": strike_grade,
                "price": candle_data['close'],
                "timestamp": candle_data['timestamp']
            }
            self.redis.publish("trade_signals", json.dumps(signal_payload))
            print(f"  -> Signal Published: {signal_type}")

        # Update Memory
        self.latest_candles[symbol] = candle_data

    def run(self):
        """
        Subscribes to Redis 'candle_closed' channel and processes messages.
        """
        pubsub = self.redis.pubsub()
        pubsub.subscribe("candle_closed")
        print("SniperStrategy Engine Running... Listening for candles.")

        for message in pubsub.listen():
            if message['type'] == 'message':
                try:
                    data = json.loads(message['data'])
                    symbol = data.get('symbol')
                    candle = data.get('candle')
                    
                    if symbol and candle:
                        self.process_candle(symbol, candle)
                except Exception as e:
                    print(f"Error processing message: {e}")

if __name__ == "__main__":
    strategy = SniperStrategy()
    strategy.run()
