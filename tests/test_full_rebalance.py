"""
Full Rebalance Manager Test Script
Tests both shortage (top-up) and excess (deposit) logic.
"""
import asyncio
import argparse
from core.balance_manager import BalanceManager
from core.rebalance_manager import RebalanceManager
from exchanges.bybit_trader import BybitTrader
from exchanges.mexc_trader import MexcTrader
from config.settings import settings
from utils.logger import get_logger

logger = get_logger()

async def main():
    parser = argparse.ArgumentParser(description="Full Rebalance Manager Tester")
    parser.add_argument("--mode", choices=["status", "run"], default="status", help="Mode: status check or run rebalance check")
    args = parser.parse_args()

    # Initialize components
    bybit_trader = BybitTrader()
    mexc_trader = MexcTrader()
    balance_manager = BalanceManager(bybit_trader=bybit_trader, mexc_trader=mexc_trader)
    
    rebalancer = RebalanceManager(balance_manager, bybit_trader, mexc_trader)

    print("\n--- Rebalance Manager Status Check ---")
    await balance_manager.refresh()
    
    # Print current balances
    print(f"\n[DEX BALANCES]")
    print(f"  APT: {balance_manager.get_free('dex', 'APT'):.2f} (Min Threshold: {settings.min_apt_threshold})")
    print(f"  AMI: {balance_manager.get_free('dex', 'AMI'):.0f} (Min Threshold: {settings.min_ami_threshold})")
    
    print(f"\n[CEX BALANCES - TOTAL USDT]")
    total_usdt = balance_manager.get_free('bybit', 'USDT') + balance_manager.get_free('mexc', 'USDT')
    print(f"  USDT: ${total_usdt:.2f} (Min Threshold: ${settings.min_usdt_threshold})")
    
    print(f"\n[REBALANCE LIMITS (DEX EXCESS)]")
    print(f"  Max APT on DEX: {rebalancer.max_apt_dex:.2f}")
    print(f"  Max AMI on DEX: {rebalancer.max_ami_dex:.0f}")

    if args.mode == "run":
        print("\n--- Executing Rebalance Check ---")
        await rebalancer.check_and_rebalance()
    
    # Cleanup
    await rebalancer.stop()
    await bybit_trader.close()
    await mexc_trader.close()

if __name__ == "__main__":
    asyncio.run(main())
