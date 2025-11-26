from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import RedirectResponse
from app.core.utils import convert_unix_to_ist
from app.core.config import settings
from app.services.feed_service import MarketFeed
import time
import httpx

app = FastAPI(title="SniperBot", version="1.0.0")

# Global variable to store access token (Temporary)
from app.core.redis_client import redis_client
ACCESS_TOKEN = redis_client.get("access_token")
MARKET_FEED = None

@app.get("/")
def read_root():
    current_time = int(time.time())
    return {
        "message": "Welcome to SniperBot",
        "server_time_ist": convert_unix_to_ist(current_time),
        "authenticated": ACCESS_TOKEN is not None,
        "feed_active": MARKET_FEED is not None
    }

@app.get("/login")
def login():
    """
    Redirects user to Upstox login page.
    """
    login_url = (
        f"https://api.upstox.com/v2/login/authorization/dialog"
        f"?response_type=code"
        f"&client_id={settings.UPSTOX_API_KEY}"
        f"&redirect_uri={settings.UPSTOX_REDIRECT_URI}"
    )
    print(f"DEBUG: Login URL: {login_url}")
    print(f"DEBUG: Redirect URI from settings: {settings.UPSTOX_REDIRECT_URI}")
    return RedirectResponse(login_url)

@app.get("/callback")
async def callback(code: str):
    """
    Callback URL for Upstox OAuth. Exchanges code for access token.
    """
    global ACCESS_TOKEN
    
    url = "https://api.upstox.com/v2/login/authorization/token"
    headers = {
        "accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {
        "code": code,
        "client_id": settings.UPSTOX_API_KEY,
        "client_secret": settings.UPSTOX_API_SECRET,
        "redirect_uri": settings.UPSTOX_REDIRECT_URI,
        "grant_type": "authorization_code"
    }

    from app.core.redis_client import redis_client
    
    async with httpx.AsyncClient() as client:
        response = await client.post(url, headers=headers, data=data)
        
    if response.status_code == 200:
        token_data = response.json()
        ACCESS_TOKEN = token_data.get("access_token")
        
        # Store in Redis for other services
        if ACCESS_TOKEN:
            redis_client.set("access_token", ACCESS_TOKEN)
            
        return {
            "message": "Authentication Successful",
            "access_token_preview": f"{ACCESS_TOKEN[:10]}..." if ACCESS_TOKEN else None
        }
    else:
        return {
            "message": "Authentication Failed",
            "error": response.text
        }

@app.get("/start-feed")
def start_feed(background_tasks: BackgroundTasks, symbols: str):
    """
    Starts the WebSocket feed for the given symbols.
    Symbols should be comma-separated, e.g., "NSE_INDEX|Nifty 50,NSE_INDEX|Nifty Bank"
    """
    global ACCESS_TOKEN, MARKET_FEED
    
    if not ACCESS_TOKEN:
        return {"error": "Authentication required. Please login first."}
    
    instrument_keys = symbols.split(",")
    feed = MarketFeed(ACCESS_TOKEN, instrument_keys)
    MARKET_FEED = feed
    
    # Run the streamer in the background
    background_tasks.add_task(feed.start_stream)
    
    return {
        "message": "Feed started in background",
        "instruments": instrument_keys
    }

@app.post("/run-morning-setup")
async def run_morning_setup():
    """
    Triggers the morning setup logic:
    1. Get Spot Price
    2. Calculate ATM
    3. Generate Grid
    4. Subscribe to Options
    """
    global MARKET_FEED
    
    if not MARKET_FEED:
        return {"error": "Market Feed is not active. Please start the feed first."}
        
    from app.services.morning_setup import MorningSetup
    setup = MorningSetup(MARKET_FEED)
    result = await setup.setup_morning_strikes()
    return result

@app.get("/refresh-contracts")
async def refresh_contracts(instrument: str = "NSE_INDEX|Nifty 50"):
    """
    Fetches and stores option contracts for the given instrument.
    Default: NSE_INDEX|Nifty 50
    """
    from app.services.contract_manager import fetch_and_store_contracts
    
    try:
        count = await fetch_and_store_contracts(instrument)
        return {
            "message": "Contracts refreshed successfully",
            "instrument": instrument,
            "contracts_stored": count
        }
    except Exception as e:
        return {
            "message": "Failed to refresh contracts",
            "error": str(e)
        }
