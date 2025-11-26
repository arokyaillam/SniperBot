import redis
import json
import sys

def check_feed():
    try:
        # Connect to Redis
        r = redis.Redis(host='localhost', port=6380, decode_responses=True)
        pubsub = r.pubsub()
        pubsub.subscribe('live_ticks')
        
        print("Listening for ticks on channel 'live_ticks'...")
        print("Press Ctrl+C to stop.")

        for message in pubsub.listen():
            if message['type'] == 'message':
                data = message['data']
                try:
                    # Pretty print JSON
                    parsed = json.loads(data)
                    print(json.dumps(parsed, indent=2))
                except:
                    print(data)
    except KeyboardInterrupt:
        print("\nStopped.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_feed()
