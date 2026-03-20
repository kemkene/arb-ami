
import asyncio
import sys
import os
import aiohttp
import json

# Add project root to path
sys.path.append(os.getcwd())

from config.settings import settings

async def view_call(node_url, function, type_args, args):
    payload = {
        "function": function,
        "type_arguments": type_args,
        "arguments": args
    }
    url = f"{node_url.rstrip('/')}/view"
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=5) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                text = await resp.text()
                return f"Error {resp.status}: {text}"

async def main():
    addr = settings.aptos_wallet_address
    node = settings.aptos_node_url
    
    print(f"--- Aptos View Diagnostics for {addr} ---")
    
    # 1. Legacy Coin APT
    print("\n[1. Checking Legacy Coin (0x1::coin::balance<APT>)]")
    res = await view_call(node, "0x1::coin::balance", ["0x1::aptos_coin::AptosCoin"], [addr])
    print(f"Result: {res}")

    # 2. Fungible Asset APT (0xA)
    print("\n[2. Checking Fungible Asset (0x1::primary_fungible_store::balance<Metadata>(addr, 0xA))]")
    res = await view_call(node, "0x1::primary_fungible_store::balance", ["0x1::fungible_asset::Metadata"], [addr, "0xA"])
    print(f"Result: {res}")

    # 3. USDT (Fungible Asset)
    usdt_metadata = settings.usdt_token_address
    print(f"\n[3. Checking USDT FA ({usdt_metadata})]")
    res = await view_call(node, "0x1::primary_fungible_store::balance", ["0x1::fungible_asset::Metadata"], [addr, usdt_metadata])
    print(f"Result: {res}")

if __name__ == "__main__":
    asyncio.run(main())
