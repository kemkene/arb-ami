
import asyncio
import aiohttp
import json

async def test_0x1():
    url = "https://fullnode.mainnet.aptoslabs.com/v1/accounts/0x1/resources"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=10) as resp:
            data = await resp.json()
            print(f"Node: {url}")
            print(f"Status: {resp.status}")
            print(f"Resources found for 0x1: {len(data)}")
            if len(data) > 0:
                print(f"First few types: {[r['type'] for r in data[:3]]}")

if __name__ == "__main__":
    asyncio.run(test_0x1())
