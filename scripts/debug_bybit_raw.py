
import asyncio
import sys
import os
import json
import hmac
import hashlib
import time
import aiohttp

# Add project root to path
sys.path.append(os.getcwd())

from config.settings import settings
from utils.logger import get_logger

logger = get_logger()

BASE_URL = "https://api.bybit.com"
RECV_WINDOW = "10000"

async def get_raw_balance(api_key, api_secret, account_type):
    timestamp = str(int(time.time() * 1000))
    recv_window = RECV_WINDOW
    qs = f"accountType={account_type}"
    sign_body = timestamp + api_key + recv_window + qs
    signature = hmac.new(
        api_secret.encode("utf-8"),
        sign_body.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": recv_window,
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"{BASE_URL}/v5/account/wallet-balance",
            params={"accountType": account_type},
            headers=headers,
        ) as resp:
            return await resp.json()

async def main():
    api_key = settings.bybit_api_key
    api_secret = settings.bybit_api_secret
    
    print(f"\n--- Checking Raw Bybit Balances ---")
    
    for acc in ["UNIFIED", "FUND"]:
        print(f"\nAccount Type: {acc}")
        try:
            data = await get_raw_balance(api_key, api_secret, acc)
            print(json.dumps(data, indent=2))
        except Exception as e:
            print(f"Error fetching {acc}: {e}")

if __name__ == "__main__":
    asyncio.run(main())
