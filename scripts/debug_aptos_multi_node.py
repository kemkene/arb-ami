
import asyncio
import sys
import os
import aiohttp
import json

NODES = [
    "https://fullnode.mainnet.aptoslabs.com/v1",
    "https://aptos-mainnet.nodereal.io/v1/public",
    "https://mainnet.aptos.nodes.guru",
    "https://aptos-mainnet.blastapi.io/public"
]

TARGET_ADDR = "0xf8901f2e8a6b2728e8b363b17c8f3c1f4fb0bf52a2ffd22d2d18b2be065f4859"

async def test_node(node_url, address):
    print(f"\n--- Testing Node: {node_url} ---")
    res_url = f"{node_url.rstrip('/')}/accounts/{address}/resources"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(res_url, timeout=5) as resp:
                print(f"Status: {resp.status}")
                if resp.status == 200:
                    resources = await resp.json()
                    print(f"Resources found: {len(resources)}")
                    apt_res = [r for r in resources if "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>" in r["type"]]
                    if apt_res:
                        val = apt_res[0]["data"]["coin"]["value"]
                        print(f"✅ APT Found! Val: {val} ({(float(val)/10**8):.4f} APT)")
                    else:
                        print("❌ APT CoinStore NOT FOUND in resources list.")
                else:
                    print(f"Failed: {await resp.text()}")
    except Exception as e:
        print(f"Error: {e}")

async def main():
    for node in NODES:
        await test_node(node, TARGET_ADDR)

if __name__ == "__main__":
    asyncio.run(main())
