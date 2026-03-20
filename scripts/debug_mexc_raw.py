
import asyncio
import sys
import os
import json
import hmac
import hashlib
import time
import urllib.parse
import aiohttp

# Add project root to path
sys.path.append(os.getcwd())

from config.settings import settings
from utils.logger import get_logger

logger = get_logger()

BASE_URL = "https://api.mexc.com"

async def get_raw_mexc_balance(api_key, api_secret):
    timestamp = str(int(time.time() * 1000))
    params = {"timestamp": timestamp, "recvWindow": "10000"}
    query_string = urllib.parse.urlencode(params)
    signature = hmac.new(
        api_secret.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    params["signature"] = signature
    
    headers = {"X-MEXC-APIKEY": api_key}
    
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"{BASE_URL}/api/v3/account",
            params=params,
            headers=headers,
        ) as resp:
            return await resp.json()

async def main():
    api_key = settings.mexc_api_key
    api_secret = settings.mexc_api_secret
    
    print(f"\n--- Checking Raw MEXC Balances ---")
    
    try:
        data = await get_raw_mexc_balance(api_key, api_secret)
        print(json.dumps(data, indent=2))
        
        # Check specific coins
        print("\nParsed Balances for specific coins (USDT, AMI, APT):")
        results = {}
        for bal in data.get("balances", []):
            asset = bal.get("asset", "")
            free = bal.get("free")
            locked = bal.get("locked")
            if asset in ["USDT", "AMI", "APT"]:
                print(f"  {asset:6}: free={free}, locked={locked}")
                
    except Exception as e:
        print(f"Error fetching MEXC: {e}")

if __name__ == "__main__":
    asyncio.run(main())
