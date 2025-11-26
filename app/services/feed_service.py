import asyncio
import json
import ssl
import websockets
import httpx
from google.protobuf.json_format import MessageToDict
import app.core.MarketDataFeedV3_pb2 as pb
from app.core.redis_client import redis_client

class MarketFeed:
    def __init__(self, access_token: str, instrument_keys: list):
        self.access_token = access_token
        self.instrument_keys = instrument_keys

    async def get_market_data_feed_authorize_v3(self):
        """Get authorization for market data feed."""
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }
        url = 'https://api.upstox.com/v3/feed/market-data-feed/authorize'
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=headers)
            return response.json()

    def decode_protobuf(self, buffer):
        """Decode protobuf message."""
        feed_response = pb.FeedResponse()
        feed_response.ParseFromString(buffer)
        return feed_response

    async def start_stream(self):
        """Fetch market data using WebSocket and publish to Redis."""
        print("DEBUG: Starting WebSocket Stream (Direct)")
        
        # Create default SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

        try:
            # Get market data feed authorization
            response = await self.get_market_data_feed_authorize_v3()
            ws_url = response["data"]["authorized_redirect_uri"]
            
            async with websockets.connect(ws_url, ssl=ssl_context) as websocket:
                print('DEBUG: Connection established')

                await asyncio.sleep(1)  # Wait for 1 second

                # Data to be sent over the WebSocket
                data = {
                    "guid": "someguid",
                    "method": "sub",
                    "data": {
                        "mode": "full_d30",
                        "instrumentKeys": self.instrument_keys
                    }
                }

                # Convert data to binary and send over WebSocket
                binary_data = json.dumps(data).encode('utf-8')
                await websocket.send(binary_data)

                # Continuously receive and decode data from WebSocket
                while True:
                    message = await websocket.recv()
                    decoded_data = self.decode_protobuf(message)

                    # Convert the decoded data to a dictionary
                    data_dict = MessageToDict(decoded_data)

                    # Publish to Redis
                    redis_client.publish("live_ticks", json.dumps(data_dict))
                    
        except asyncio.CancelledError:
            print("DEBUG: WebSocket stream cancelled.")
        except Exception as e:
            print(f"Error in WebSocket stream: {e}")

