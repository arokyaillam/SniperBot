import redis
import json

def check_contracts():
    try:
        # Connect to Redis (Port 6380 as per docker-compose)
        r = redis.Redis(host='localhost', port=6380, decode_responses=True)
        
        # Scan for contract keys
        print("Scanning for contracts in Redis...")
        keys = []
        cursor = '0'
        while cursor != 0:
            cursor, batch = r.scan(cursor=cursor, match='CONTRACT:*', count=1000)
            keys.extend(batch)
            
        print(f"Total contracts found: {len(keys)}")
        
        if keys:
            print("\nSample contracts (first 5):")
            for key in keys[:5]:
                value = r.get(key)
                print(f"Key: {key}")
                print(f"Value: {value}")
                
            # Check for specific NIFTY contract if available
            print("\nChecking for a NIFTY CE contract...")
            nifty_ce = [k for k in keys if "NIFTY" in k and ":CE" in k]
            if nifty_ce:
                print(f"Found {len(nifty_ce)} NIFTY CE contracts.")
                example_key = nifty_ce[0]
                print(f"Example Key: {example_key}")
                print(f"Value: {r.get(example_key)}")
                
                # Extract expiry from key
                # Key format: CONTRACT:{SYMBOL}:{EXPIRY}:{STRIKE}:{OPT_TYPE}
                parts = example_key.split(":")
                if len(parts) >= 3:
                    print(f"Expiry Date in Key: {parts[2]}")
            else:
                print("No NIFTY CE contracts found.")
                
    except Exception as e:
        print(f"Error connecting to Redis: {e}")

if __name__ == "__main__":
    check_contracts()
