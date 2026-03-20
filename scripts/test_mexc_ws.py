import asyncio
import aiohttp
import json
import time

async def test_mexc_ws():
    uri = "wss://wbs.mexc.com/ws"
    symbols = ["AMIUSDT", "APTUSDT"]
    
    print(f"Connecting to {uri}...")
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(uri) as ws:
            print("Connected!")
            
            params = [f"spot@public.bookTicker.v3.api@{s}" for s in symbols]
            subscribe_msg = {
                "method": "SUBSCRIPTION",
                "params": params
            }
            await ws.send_json(subscribe_msg)
            print(f"Subscribed to {params}")
            
            start_time = time.time()
            count = 0
            while time.time() - start_time < 30: # Test for 30 seconds
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=5.0)
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        print(f"Received: {data}")
                        count += 1
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        print("Closed")
                        break
                except asyncio.TimeoutError:
                    print("Timeout waiting for message, sending PING...")
                    # MEXC heartbeat: Client can send PING
                    await ws.send_str('{"method":"PING"}')
            
            print(f"Test finished. Received {count} messages.")

if __name__ == "__main__":
    asyncio.run(test_mexc_ws())
