import asyncio
import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.rebalance_manager import RebalanceManager

class TestCexRebalance(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.balance_manager = MagicMock()
        self.bybit_trader = MagicMock()
        self.mexc_trader = MagicMock()
        
        # Mock trader methods
        self.bybit_trader.get_deposit_address = AsyncMock(return_value="BYBIT_ADDR")
        self.mexc_trader.get_deposit_address = AsyncMock(return_value="MEXC_ADDR")
        self.bybit_trader.withdraw = AsyncMock(return_value="WID_BYBIT")
        self.mexc_trader.withdraw = AsyncMock(return_value="WID_MEXC")
        
        # Setup mock settings
        self.mock_settings_patcher = patch('core.rebalance_manager.settings')
        self.mock_settings = self.mock_settings_patcher.start()
        self.mock_settings.rebalance_enabled = True
        self.mock_settings.min_ami_threshold = 10000.0
        self.mock_settings.min_apt_threshold = 90.0
        self.mock_settings.min_usdt_threshold = 20.0
        self.mock_settings.cex_rebalance_threshold_factor = 2.0
        self.mock_settings.bybit_withdraw_chain = "APTOS"
        self.mock_settings.mexc_withdraw_network = "APT"

        self.rebalancer = RebalanceManager(
            self.balance_manager,
            self.bybit_trader,
            self.mexc_trader
        )

    async def asyncTearDown(self):
        self.mock_settings_patcher.stop()

    async def test_ami_imbalance_bybit_to_mexc(self):
        """Test Bybit(22k) -> MEXC(2k) triggers rebalance."""
        # Mock balances
        def get_free(exch, coin):
            if exch == "bybit" and coin == "AMI": return 22000.0
            if exch == "mexc" and coin == "AMI": return 2000.0
            return 100.0
            
        self.balance_manager.get_free.side_effect = get_free
        self.balance_manager.refresh = AsyncMock()
        
        await self.rebalancer._check_cex_to_cex_imbalance()
        
        # Verify
        self.bybit_trader.withdraw.assert_called_once()
        args, kwargs = self.bybit_trader.withdraw.call_args
        self.assertEqual(kwargs['coin'], "AMI")
        self.assertEqual(kwargs['amount'], 10000.0)
        self.assertEqual(kwargs['address'], "MEXC_ADDR")

    async def test_usdt_imbalance_mexc_to_bybit(self):
        """Test MEXC(100) -> Bybit(5) triggers rebalance for USDT."""
        def get_free(exch, coin):
            if exch == "bybit" and coin == "USDT": return 5.0
            if exch == "mexc" and coin == "USDT": return 100.0
            return 1000.0
            
        self.balance_manager.get_free.side_effect = get_free
        
        await self.rebalancer._check_cex_to_cex_imbalance()
        
        # Verify
        self.mexc_trader.withdraw.assert_called_once()
        args, kwargs = self.mexc_trader.withdraw.call_args
        self.assertEqual(kwargs['coin'], "USDT")
        # Amount = 20 - 5 + 4 = 19
        self.assertEqual(kwargs['amount'], 19.0)
        self.assertEqual(kwargs['address'], "BYBIT_ADDR")

if __name__ == '__main__':
    unittest.main()
