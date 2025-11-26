import redis
import httpx
import asyncio
import json

async def test_api():
    try:
        # Connect to Redis
        r = redis.Redis(host='localhost', port=6380, decode_responses=True)
        token = r.get("access_token")
        if not token:
            print("Error: No access token in Redis. Please login at http://127.0.0.1:8000/login")
            return

        print(f"Token found: {token[:10]}...")
        
        url = "https://api.upstox.com/v2/option/contract"
        params = {"instrument_key": "NSE_INDEX|Nifty 50"}
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}"
        }
        
        print(f"Fetching {url} with params {params}...")
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, params=params, headers=headers)
            print(f"Status: {resp.status_code}")
            
            if resp.status_code != 200:
                print("Error Response:")
                print(resp.text)
                return
                
            data = resp.json()
            if "data" in data:
                contracts = data["data"]
                print(f"Got {len(contracts)} contracts.")
                
                if contracts:
                    print("\nSample contract (first item):")
                    print(json.dumps(contracts[0], indent=2))
                    
                    expiries = set(c.get('expiry') for c in contracts if c.get('expiry'))
                    print(f"\nExpiries found: {sorted(list(expiries))}")
                else:
                    print("Contracts list is empty.")
            else:
                print("No 'data' field in response.")
                print(json.dumps(data, indent=2))
                
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(test_api())
