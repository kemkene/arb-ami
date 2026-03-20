
import asyncio
import sys
import os
import aiohttp
from aptos_sdk.account import Account
from aptos_sdk.async_client import RestClient
from aptos_sdk.transactions import EntryFunction, TransactionPayload, TransactionArgument, SignedTransaction
from aptos_sdk.authenticator import Authenticator, Ed25519Authenticator
from aptos_sdk.ed25519 import Signature
from aptos_sdk.bcs import Serializer
from aptos_sdk.account_address import AccountAddress
from aptos_sdk.type_tag import TypeTag, StructTag

# Add project root to path
sys.path.append(os.getcwd())
from config.settings import settings

async def simulate_swap():
    print("--- Aptos Swap Simulation (Safe Test) ---")
    
    # Setup
    private_key = settings.aptos_private_key
    account = Account.load_key(private_key)
    node_url = settings.aptos_node_url
    client = RestClient(node_url)
    
    # Constants
    CELLANA_ROUTER = "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::router"
    APTOS_COIN_TYPE = "0x1::aptos_coin::AptosCoin"
    # AMI FA Metadata from user
    AMI_METADATA = "0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451"
    
    async def run_real_sim_json(label, function_name, type_args, args_json):
        print(f"\n--- Real Transaction Simulation (JSON): {label} ---")
        url = f"{node_url}/transactions/simulate"
        
        # Get current account sequence number for a more realistic simulation
        try:
            acc_info = await client.account(account.address())
            seq = int(acc_info.get("sequence_number", 0))
        except:
            seq = 0
            
        # Use a very large expiration for simulation
        import time
        expiration = int(time.time()) + 10000 
        
        # Aptos JSON simulation payload format
        payload = {
            "sender": str(account.address()),
            "sequence_number": str(seq),
            "max_gas_amount": "100000",
            "gas_unit_price": "100",
            "expiration_timestamp_secs": str(expiration),
            "payload": {
                "type": "entry_function_payload",
                "function": f"{CELLANA_ROUTER}::{function_name}",
                "type_arguments": type_args,
                "arguments": args_json
            },
            "signature": {
                "type": "ed25519_signature",
                "public_key": str(account.public_key()),
                "signature": "0x" + ("00" * 64) # Zero signature for simulation
            }
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    body = await resp.json()
                    if resp.status == 200 and isinstance(body, list) and len(body) > 0:
                        tx_res = body[0]
                        if tx_res.get("success"):
                            print(f"✅ Simulation SUCCESS!")
                            print(f"   Gas Used: {tx_res.get('gas_used')}")
                            # Look for Swap/Deposit events
                            for ev in tx_res.get("events", []):
                                if "SwapEvent" in ev.get("type", ""):
                                    print(f"   🔥 SwapEvent: {ev.get('data')}")
                        else:
                            print(f"❌ Simulation FAILED: {tx_res.get('vm_status')}")
                    else:
                        print(f"❌ API Error {resp.status}: {body}")
        except Exception as e:
            print(f"❌ Error: {e}")

    APT_FA_METADATA = "0xedc2704f2cef417a06d1756a04a16a9fa6faaed13af469be9cdfcac5a21a8e2e"
    
    # 1. APT -> AMI (0.1 APT)
    await run_real_sim_json("APT -> AMI", "swap_coin_for_asset_entry", 
        [APTOS_COIN_TYPE],
        [
            "10000000", # 0.1 APT
            "0",        # min_out
            AMI_METADATA,
            False,
            str(account.address())
        ]
    )

    # 2. AMI -> APT (1000 AMI)
    await run_real_sim_json("AMI -> APT", "swap_asset_for_coin_entry",
        [APTOS_COIN_TYPE],
        [
            "100000000000", # 1000 AMI
            "0",            # min_out
            AMI_METADATA,
            False,
            str(account.address())
        ]
    )

    await client.close()

if __name__ == "__main__":
    asyncio.run(simulate_swap())
