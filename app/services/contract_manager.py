import httpx
import json
from datetime import datetime
from app.core.redis_client import redis_client

async def fetch_and_store_contracts(instrument_key: str = "NSE_INDEX|Nifty 50"):
    """
    Fetches option contracts for the given instrument, filters for the nearest expiry,
    and stores them in Redis.
    """
    print(f"DEBUG: Fetching contracts for {instrument_key}")
    
    # 1. Get Access Token
    access_token = redis_client.get("access_token")
    if not access_token:
        raise Exception("Access token not found in Redis. Please authenticate first.")
    
    # 2. Call Upstox API
    url = "https://api.upstox.com/v2/option/contract"
    params = {"instrument_key": instrument_key}
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
    if data.get("status") != "success" or not data.get("data"):
        raise Exception(f"Failed to fetch contracts: {data}")
        
    contracts = data["data"]
    
    # 3. Identify Nearest Expiry
    # Extract all unique expiry dates
    expiry_dates = set()
    for contract in contracts:
        if contract.get("expiry"):
            expiry_dates.add(contract["expiry"])
            
    if not expiry_dates:
        raise Exception("No expiry dates found in contracts.")
        
    # Sort dates and find the nearest one >= today
    sorted_expiries = sorted(expiry_dates)
    today = datetime.now().date()
    nearest_expiry = None
    
    for expiry_str in sorted_expiries:
        expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d").date()
        if expiry_date >= today:
            nearest_expiry = expiry_str
            break
            
    if not nearest_expiry:
        raise Exception("No future expiry found.")
        
    print(f"DEBUG: Nearest Expiry identified: {nearest_expiry}")
    
    # 4. Filter and Store
    # Symbol Normalization
    symbol_map = {
        "NSE_INDEX|Nifty 50": "NIFTY",
        "NSE_INDEX|Nifty Bank": "BANKNIFTY"
    }
    symbol_name = symbol_map.get(instrument_key, instrument_key.split("|")[-1].replace(" ", "").upper())
    
    redis_data = {}
    count = 0
    
    for contract in contracts:
        if contract.get("expiry") == nearest_expiry:
            strike = contract.get("strike_price")
            opt_type = contract.get("instrument_type") # CE or PE
            instr_key = contract.get("instrument_key")
            lot_size = contract.get("lot_size")
            
            if strike and opt_type and instr_key:
                # Key: CONTRACT:{SYMBOL}:{EXPIRY}:{STRIKE}:{OPT_TYPE}
                # Example: CONTRACT:NIFTY:2025-11-28:24200:CE
                key = f"CONTRACT:{symbol_name}:{nearest_expiry}:{strike}:{opt_type}"
                
                # Value: JSON with instrument_key and lot_size
                value = json.dumps({
                    "instrument_key": instr_key,
                    "lot_size": lot_size
                })
                
                redis_data[key] = value
                count += 1
                
    if redis_data:
        redis_client.mset(redis_data)
        print(f"DEBUG: Stored {count} contracts in Redis.")
    else:
        print("DEBUG: No contracts matched the criteria.")
        
    return count
