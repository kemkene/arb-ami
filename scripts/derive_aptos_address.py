
import sys
import os
from aptos_sdk.account import Account

# Add project root to path
sys.path.append(os.getcwd())

from config.settings import settings

def derive():
    priv_key = settings.aptos_private_key
    if not priv_key:
        print("No private key in .env")
        return
    
    try:
        account = Account.load_key(priv_key)
        derived_addr = str(account.address())
        config_addr = settings.aptos_wallet_address
        
        print(f"Private Key: {priv_key[:10]}...")
        print(f"Derived Address: {derived_addr}")
        print(f"Config Address:  {config_addr}")
        
        if derived_addr.lower() == config_addr.lower():
            print("✅ SUCCESS: Addresses match!")
        else:
            print("❌ FAILURE: Addresses DO NOT match!")
            print("This is why you see 0 balance in the bot but not on the explorer (if you used the wrong address).")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    derive()
