import asyncio
import sys
import os
import argparse

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

print("--- SCRIPT STARTED (PRE-IMPORT) ---", flush=True)

from exchanges.bybit_trader import BybitTrader
from exchanges.mexc_trader import MexcTrader
from config.settings import settings
from utils.logger import get_logger

logger = get_logger()
print("--- IMPORTS COMPLETED ---", flush=True)

async def test_inquiry():
    print("\n" + "="*50, flush=True)
    print("      EXCHANGE TRANSFER OPERATIONS TEST", flush=True)
    print("="*50 + "\n", flush=True)

    bybit = BybitTrader()
    mexc = MexcTrader()

    # 1. Sync info
    print("Step 1: Syncing instrument info...", flush=True)
    await bybit.sync_instrument_info("APTUSDT")
    await mexc.sync_instrument_info("APTUSDT")
    print("Done.\n", flush=True)

    # 2. Check Balances
    print("Step 2: Checking Balances...", flush=True)
    bybit_bal = await bybit.get_balance(["APT", "USDT"])
    mexc_bal = await mexc.get_balance(["APT", "USDT"])
    
    print(f"BYBIT Balance: {bybit_bal}", flush=True)
    print(f"MEXC  Balance: {mexc_bal}", flush=True)
    print("-" * 30 + "\n", flush=True)

    # 3. Check Deposit Addresses
    print("Step 3: Fetching Deposit Addresses...", flush=True)
    
    # Bybit
    print("Checking Bybit...", flush=True)
    for coin, chain in [("APT", "APT"), ("USDT", "ETH"), ("USDT", "TRX"), ("USDT", "Aptos")]:
        addr = await bybit.get_deposit_address(coin, chain)
        print(f"  [Bybit] {coin} on {chain}: {addr}", flush=True)

    # MEXC
    print("\nChecking MEXC...", flush=True)
    # MEXC networks are often named differently: 'APT', 'ETH', 'TRX', 'Aptos'
    for coin, net in [("APT", "Aptos"), ("USDT", "Ethereum"), ("USDT", "TRX"), ("USDT", "Aptos")]:
        addr = await mexc.get_deposit_address(coin, net)
        print(f"  [MEXC]  {coin} on {net}: {addr}", flush=True)
    print("-" * 30 + "\n", flush=True)

    return bybit, mexc

async def test_withdraw_menu(bybit, mexc):
    while True:
        print("\n--- WITHDRAWAL TEST MENU ---", flush=True)
        print("1. Withdraw 0.1 APT from BYBIT to Your Wallet", flush=True)
        print("2. Withdraw 0.1 APT from MEXC to Your Wallet", flush=True)
        print("3. Check Balances Again", flush=True)
        print("Q. Quit", flush=True)
        
        choice = input("\nSelect an option: ").strip().upper()
        
        if choice == "Q":
            break
        elif choice == "3":
            bybit_bal = await bybit.get_balance(["APT"])
            mexc_bal = await mexc.get_balance(["APT"])
            print(f"BYBIT APT: {bybit_bal.get('APT')}", flush=True)
            print(f"MEXC  APT: {mexc_bal.get('APT')}", flush=True)
            continue

        target_wallet = settings.aptos_wallet_address
        if not target_wallet:
            print("❌ ERROR: APTOS_WALLET_ADDRESS not found in settings!", flush=True)
            continue

        amount = 0.1
        
        if choice == "1":
            print(f"\n⚠️  Action: Withdraw {amount} APT from BYBIT to {target_wallet}", flush=True)
            print(f"⚠️  Network: {settings.bybit_withdraw_chain}", flush=True)
            print("⚠️  This will cost exchange withdrawal fees (~0.01 APT).", flush=True)
            confirm = input("Are you sure? Type 'YES' to proceed: ").strip()
            if confirm == "YES":
                wid = await bybit.withdraw("APT", amount, target_wallet, settings.bybit_withdraw_chain)
                if wid:
                    print(f"✅ SUCCESS! Bybit Withdrawal ID: {wid}", flush=True)
                else:
                    print("❌ FAILED! Check logs for details.", flush=True)
            else:
                print("Aborted.", flush=True)
        
        elif choice == "2":
            print(f"\n⚠️  Action: Withdraw {amount} APT from MEXC to {target_wallet}", flush=True)
            print(f"⚠️  Network: {settings.mexc_withdraw_network}", flush=True)
            print("⚠️  This will cost exchange withdrawal fees.", flush=True)
            confirm = input("Are you sure? Type 'YES' to proceed: ").strip()
            if confirm == "YES":
                wid = await mexc.withdraw("APT", amount, target_wallet, settings.mexc_withdraw_network)
                if wid:
                    print(f"✅ SUCCESS! MEXC Withdrawal ID: {wid}", flush=True)
                else:
                    print("❌ FAILED! Check logs for details.", flush=True)
            else:
                print("Aborted.", flush=True)

async def main():
    parser = argparse.ArgumentParser(description="Transfer Operations Test")
    parser.add_argument("--withdraw-bybit", type=float, help="Amount of APT to withdraw from Bybit")
    parser.add_argument("--withdraw-mexc", type=float, help="Amount of APT to withdraw from MEXC")
    parser.add_argument("--mexc-deposit", type=str, help="Coin to query MEXC deposit address for")
    parser.add_argument("--bybit-deposit", type=str, help="Coin to query Bybit deposit address for")
    parser.add_argument("--confirm", action="store_true", help="Confirm execution")
    args = parser.parse_args()

    try:
        print("--- STARTING TEST ---", flush=True)
        bybit = BybitTrader()
        mexc = MexcTrader()
        
        target_wallet = settings.aptos_wallet_address

        if args.withdraw_bybit:
            amount = args.withdraw_bybit
            print(f"TESTING WITHDRAW FROM BYBIT: {amount} APT to {target_wallet}", flush=True)
            if args.confirm:
                wid = await bybit.withdraw("APT", amount, target_wallet, settings.bybit_withdraw_chain)
                if wid:
                    print(f"✅ SUCCESS! Bybit Withdrawal ID: {wid}", flush=True)
                else:
                    print("❌ FAILED! Check logs.", flush=True)
            else:
                print("⚠️  Withdrawal skipped. Use --confirm to proceed.", flush=True)
        
        elif args.withdraw_mexc:
            amount = args.withdraw_mexc
            print(f"TESTING WITHDRAW FROM MEXC: {amount} APT to {target_wallet}", flush=True)
            if args.confirm:
                wid = await mexc.withdraw("APT", amount, target_wallet, settings.mexc_withdraw_network)
                if wid:
                    print(f"✅ SUCCESS! MEXC Withdrawal ID: {wid}", flush=True)
                else:
                    print("❌ FAILED! Check logs.", flush=True)
            else:
                print("⚠️  Withdrawal skipped. Use --confirm to proceed.", flush=True)
        
        elif args.mexc_deposit == "LIST_ALL":
            print("FETCHING ALL MEXC ASSET CONFIG...", flush=True)
            data = await mexc.get_all_assets_info()
            if isinstance(data, list):
                apt_info = [d for d in data if d.get("coin") == "APT"]
                print(f"APT CONFIG: {apt_info}", flush=True)
            else:
                print(f"FAILED: {data}", flush=True)

        elif args.mexc_deposit:
            print(f"QUERYING MEXC DEPOSIT ADDRESS FOR {args.mexc_deposit}...", flush=True)
            # Try 'Aptos' as default for MEXC
            await mexc.get_deposit_address(args.mexc_deposit, "Aptos")
            
        elif args.bybit_deposit == "INFO":
            print("FETCHING BYBIT API KEY INFO...", flush=True)
            data = await bybit.get_api_key_info()
            print(f"BYBIT API INFO: {data}", flush=True)

        elif args.bybit_deposit:
            print(f"QUERYING BYBIT DEPOSIT ADDRESS FOR {args.bybit_deposit}...", flush=True)
            # Try 'APT' as default for Bybit
            await bybit.get_deposit_address(args.bybit_deposit, "APT")
            
        else:
            # Default behavior: run initial balance check
            print("Running balance check...", flush=True)
            b = await bybit.get_balance("APT")
            m = await mexc.get_balance("APT")
            print(f"Bybit APT: {b}", flush=True)
            print(f"MEXC APT: {m}", flush=True)
            print("\nUse --withdraw-bybit, --withdraw-mexc, --mexc-deposit, or --bybit-deposit for more actions.", flush=True)
        
        print("\nTest completed.", flush=True)
    except KeyboardInterrupt:
        print("\nInterrupted.", flush=True)
    except Exception as e:
        print(f"\nFATAL ERROR: {e}", flush=True)
        import traceback
        traceback.print_exc()
    finally:
        print("Exiting.", flush=True)

if __name__ == "__main__":
    asyncio.run(main())
