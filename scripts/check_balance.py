
import asyncio
import sys
import os
import time
import aiohttp
import json

# Add project root to path
sys.path.append(os.getcwd())

from exchanges.bybit_trader import BybitTrader
from exchanges.mexc_trader import MexcTrader
from config.settings import settings
from utils.logger import get_logger

logger = get_logger()

# Constants
APT_METADATA = "0xa"
USDT_METADATA = "0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b"
AMI_METADATA = "0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451"

async def get_aptos_asset_balance(session, address, metadata_addr, decimals, node_url):
    """Get balance of a fungible asset via view function."""
    url = f"{node_url}/view"
    payload = {
        "function": "0x1::primary_fungible_store::balance",
        "type_arguments": ["0x1::fungible_asset::Metadata"],
        "arguments": [address, metadata_addr]
    }
    try:
        async with session.post(url, json=payload, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data and len(data) > 0:
                    return int(data[0]) / (10 ** decimals)
            else:
                text = await resp.text()
                print(f"DEBUG: View failed for {metadata_addr} status={resp.status} body={text}")
    except Exception as e:
        print(f"DEBUG: Error fetching asset {metadata_addr}: {e}")
    return 0.0

async def get_aptos_coin_balance(session, address, coin_type, decimals, node_url):
    """Get balance of a legacy Coin (v1) via view function."""
    url = f"{node_url}/view"
    payload = {
        "function": "0x1::coin::balance",
        "type_arguments": [coin_type],
        "arguments": [address]
    }
    try:
        async with session.post(url, json=payload, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data and len(data) > 0:
                    return int(data[0]) / (10 ** decimals)
    except Exception:
        pass
    return 0.0

async def get_aptos_balances(address):
    node_url = settings.aptos_node_url
    results = {"APT": 0.0, "USDT": 0.0}
    
    async with aiohttp.ClientSession() as session:
        # 1. APT (Fungible Asset, 8 decimals)
        t_apt = get_aptos_asset_balance(session, address, APT_METADATA, 8, node_url)
        
        # 2. USDT (Fungible Asset, 6 decimals)
        t_usdt = get_aptos_asset_balance(session, address, USDT_METADATA, 6, node_url)
        
        # 3. AMI (Check both FA and Coin)
        # AMI FA
        t_ami_fa = get_aptos_asset_balance(session, address, AMI_METADATA, 8, node_url)
        # AMI Coin (v1)
        ami_coin_type = f"{AMI_METADATA[:66]}::liquidity_pool::AmiCoin"
        t_ami_coin = get_aptos_coin_balance(session, address, ami_coin_type, 8, node_url)
        
        apt, usdt, ami_fa, ami_coin = await asyncio.gather(t_apt, t_usdt, t_ami_fa, t_ami_coin)
        
        results["APT"] = apt
        results["USDT"] = usdt
        results["AMI"] = max(ami_fa, ami_coin)
    
    return results

async def main():
    print(f"\n{'='*50}")
    print(f"   UNIFIED BALANCE REPORT - {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}\n")

    # 1. Bybit
    print("Checking Bybit...")
    try:
        bybit = BybitTrader()
        bybit_bal = await bybit.get_balance()
        print(f"Bybit Wallet: USDT={bybit_bal.get('USDT', 0):.2f}, AMI={bybit_bal.get('AMI', 0):.2f}, APT={bybit_bal.get('APT', 0):.2f}")
    except Exception as e:
        print(f"Bybit Error: {e}")

    # 2. MEXC
    print("\nChecking MEXC...")
    try:
        mexc = MexcTrader()
        mexc_bal = await mexc.get_balance()
        print(f"MEXC Wallet:  USDT={mexc_bal.get('USDT', 0):.2f}, AMI={mexc_bal.get('AMI', 0):.2f}, APT={mexc_bal.get('APT', 0):.2f}")
    except Exception as e:
        print(f"MEXC Error: {e}")

    # 3. Aptos Wallet
    aptos_addr = settings.aptos_wallet_address
    if not aptos_addr and settings.aptos_private_key:
        try:
            from aptos_sdk.account import Account
            acc = Account.load_key(settings.aptos_private_key)
            aptos_addr = str(acc.address())
        except ImportError:
            pass

    if aptos_addr:
        print(f"\nChecking Aptos Wallet ({aptos_addr}):")
        aptos_bal = await get_aptos_balances(aptos_addr)
        print(f"Aptos Wallet: APT={aptos_bal['APT']:.6f}, USDT={aptos_bal['USDT']:.2f}, AMI={aptos_bal['AMI']:.0f}")
    else:
        print("\nChecking Aptos Wallet: No address found in .env")

    print(f"\n{'='*50}")

if __name__ == "__main__":
    asyncio.run(main())
