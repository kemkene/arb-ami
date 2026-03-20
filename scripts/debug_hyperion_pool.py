import asyncio
from aptos_sdk.async_client import RestClient
from aptos_sdk.account_address import AccountAddress

NODE_URL = "https://fullnode.mainnet.aptoslabs.com/v1"
AMI_ADDR = "0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451"
APT_ADDR = "0xa"
HYPERION_POOL_V3 = "0x8b4a2c4bb53857c718a04c020b98f8c2e1f99a68b0f57389a8bf5434cd22e05c::pool_v3"

async def main():
    client = RestClient(NODE_URL)
    target_pool = "0x617a777d6a19da5bf346af49a7f648acce66db9dd3f98c78bd10ed556708a7da"
    print(f"--- Searching for correct Tier for Pool: {target_pool} ---")
    
    # Try all common tiers
    tiers = [1, 5, 10, 30, 60, 100, 200]
    import aiohttp
    async with aiohttp.ClientSession() as session:
        for tier in tiers:
            try:
                # Use pool_v3::liquidity_pool_address_safe
                payload = {
                    "function": f"{HYPERION_POOL_V3}::liquidity_pool_address_safe",
                    "type_arguments": [],
                    "arguments": [AMI_ADDR, APT_ADDR, str(tier)]
                }
                async with session.post(f"{NODE_URL}/view", json=payload) as resp:
                    res = await resp.json()
                    if isinstance(res, list) and len(res) > 0:
                        pool_addr = res[0]
                        print(f"Tier {tier}: Returns Pool Address = {pool_addr}")
                        if pool_addr.lower() == target_pool.lower():
                            print(f"🌟 MATCH FOUND! Tier {tier} is for this pool.")
                    else:
                        print(f"Tier {tier}: No pool returned or Error - {res}")
            except Exception as e:
                print(f"Tier {tier}: Error - {e}")

    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
