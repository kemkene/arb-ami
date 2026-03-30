import asyncio
from core.balance_manager import BalanceManager
from core.rebalance_manager import RebalanceManager
from core.trade_executor import TradeExecutor
from config.settings import settings
from utils.logger import get_logger

logger = get_logger()

async def test_rebalance():
    print("🧪 [TEST] Starting Rebalance Simulation...")
    trade_executor = TradeExecutor()
    balance_manager = BalanceManager(
        bybit_trader=trade_executor.bybit,
        mexc_trader=trade_executor.mexc
    )
    
    # Init RebalanceManager
    rebalancer = RebalanceManager(
        balance_manager=balance_manager,
        bybit_trader=trade_executor.bybit,
        mexc_trader=trade_executor.mexc
    )
    
    # Override settings for testing
    object.__setattr__(settings, 'rebalance_enabled', True)
    object.__setattr__(settings, 'rebalance_interval_min', 0.01) # Run almost immediately

    
    print(f"Current Settings:")
    print(f"- APT Threshold: {settings.min_apt_threshold}")
    print(f"- AMI Threshold: {settings.min_ami_threshold}")
    print(f"- CEX Address: {settings.aptos_address}")
    
    # 1. Force a refresh to see current state
    await balance_manager.refresh()

    
    # 2. Run one check cycle
    print("\n🔍 Checking balances and triggering rebalance logic...")
    await rebalancer.check_and_rebalance()
    
    print("\n✅ Test finished. Check logs above for any 'Withdrawing' or 'Warning' messages.")

if __name__ == "__main__":
    asyncio.run(test_rebalance())
