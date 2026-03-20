
import asyncio
import sys
import os
import aiohttp
import json

# Add project root to path
sys.path.append(os.getcwd())

from config.settings import settings

async def get_balance_view(address, metadata_addr, node_url):
    print(f"--- Testing Balance View for {address} ---")
    print(f"Metadata: {metadata_addr}")
    
    # We use the view function 0x1::fungible_asset::balance
    # Signature: (owner: address, metadata: Object<Metadata>): u64
    payload = {
        "function": "0x1::primary_fungible_store::balance", # This is easier than finding the store object
        "type_arguments": [],
        "arguments": [address, metadata_addr]
    }
    
    url = f"{node_url.rstrip('/')}/view"
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=5) as resp:
            status = resp.status
            data = await resp.json()
            print(f"Status: {status}")
            print(f"Response: {data}")
            if status == 200 and isinstance(data, list) and len(data) > 0:
                return int(data[0])
    return 0

async def main():
    addr = settings.aptos_wallet_address
    node = settings.aptos_node_url
    
    # 1. Native APT (Fungible Asset metadata for APT is 0xa)
    apt_bal = await get_balance_view(addr, "0xA", node)
    print(f"Native APT (FA): {apt_bal/10**8} APT")

    # 2. USDT
    usdt_metadata = settings.usdt_token_address
    usdt_bal = await get_balance_view(addr, usdt_metadata, node)
    print(f"USDT (FA): {usdt_bal/10**6} USDT")

if __name__ == "__main__":
    asyncio.run(main())
