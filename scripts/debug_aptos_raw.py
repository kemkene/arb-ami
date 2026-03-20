
import asyncio
import sys
import os
import aiohttp
from aptos_sdk.async_client import RestClient

# Add project root to path
sys.path.append(os.getcwd())

async def check_aptos_diagnostics(address, node_url):
    print(f"--- Aptos Diagnostics ---")
    print(f"Target Address: {address}")
    print(f"Node URL: {node_url}")
    
    # 1. Manual check via aiohttp (Raw REST)
    print("\n[1. Testing Raw REST API via aiohttp]")
    try:
        # Construct the URL properly (avoid double /v1 if node_url already has it)
        base_node = node_url.rstrip("/")
        resource_path = f"/accounts/{address}/resource/0x1::coin::CoinStore%3C0x1::aptos_coin::AptosCoin%3E"
        full_url = base_node + resource_path
        print(f"URL: {full_url}")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(full_url, timeout=10) as resp:
                status = resp.status
                data = await resp.json()
                print(f"Status Code: {status}")
                if status == 200:
                    val = data.get("data", {}).get("coin", {}).get("value")
                    print(f"Success! APT (OCTAs): {val}")
                    print(f"Balance: {float(val)/10**8} APT")
                else:
                    print(f"Error Response: {data}")
    except Exception as e:
        print(f"aiohttp test failed: {e}")

    # 2. Testing aptos-sdk RestClient
    print("\n[2. Testing aptos-sdk RestClient]")
    try:
        # Important: the SDK might add its own /v1. Let's try both.
        urls_to_try = [node_url, node_url.replace("/v1", "")]
        for u in urls_to_try:
            print(f"Trying SDK with URL: {u}")
            try:
                client = RestClient(u)
                bal = await client.account_balance(address)
                print(f"SDK Success with {u}! Balance: {bal/10**8} APT")
                break
            except Exception as ex:
                print(f"SDK failed with {u}: {ex}")
    except Exception as e:
        print(f"SDK overall test failed: {e}")

if __name__ == "__main__":
    from config.settings import settings
    addr = settings.aptos_wallet_address
    node = settings.aptos_node_url
    asyncio.run(check_aptos_diagnostics(addr, node))
