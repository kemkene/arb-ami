import asyncio
import sys
import os
import aiohttp
from aptos_sdk.account import Account

# Add project root to path
sys.path.append(os.getcwd())
try:
    from config.settings import settings
except ImportError:
    # If running from scripts/ directory
    sys.path.append(os.path.dirname(os.getcwd()))
    from config.settings import settings

async def simulate_hyperion_swap():
    print("--- Hyperion Swap Simulation (Tier 100) ---")
    
    private_key = settings.aptos_private_key
    account = Account.load_key(private_key)
    node_url = settings.aptos_node_url
    
    # Constants
    HYPERION_ROUTER = "0x8b4a2c4bb53857c718a04c020b98f8c2e1f99a68b0f57389a8bf5434cd22e05c::router_v3"
    AMI_METADATA = "0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451"
    APT_METADATA = "0xa"
    
    # We test exact_input_swap_entry(token_in, token_out, fee_tier, amount_in, min_amount_out, recipient, deadline)
    # Tier 100 = 1%
    
    url = f"{node_url}/transactions/simulate"
    import time
    deadline = int(time.time()) + 3600
    
    payload = {
        "sender": str(account.address()),
        "sequence_number": "0", # Should ideally fetch real seq, but 0 is usually fine for sim
        "max_gas_amount": "100000",
        "gas_unit_price": "100",
        "expiration_timestamp_secs": str(deadline),
        "payload": {
            "type": "entry_function_payload",
            "function": f"{HYPERION_ROUTER}::exact_input_swap_entry",
            "type_arguments": [],
            "arguments": [
                "100",           # fee_tier: u8 (Tier 100 = 1%)
                "10000000",      # amount_in: u64 (0.1 APT)
                "0",             # min_amount_out: u64
                "0",             # sqrt_price_limit: u128 (0 = no limit)
                APT_METADATA,    # token_in: Metadata Object
                AMI_METADATA,    # token_out: Metadata Object
                str(account.address()), # recipient: address
                str(deadline)    # deadline: u64
            ]
        },
        "signature": {
            "type": "ed25519_signature",
            "public_key": str(account.public_key()),
            "signature": "0x" + ("00" * 64)
        }
    }
    
    print(f"Testing APT -> AMI with Tier 100...")
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            results = await resp.json()
            if resp.status == 200 and isinstance(results, list):
                tx = results[0]
                if tx.get("success"):
                    print(f"✅ Simulation SUCCESS for Tier 100!")
                    print(f"   Gas Used: {tx.get('gas_used')}")
                else:
                    print(f"❌ Simulation FAILED: {tx.get('vm_status')}")
            else:
                print(f"❌ API Error {resp.status}: {results}")

if __name__ == "__main__":
    asyncio.run(simulate_hyperion_swap())
