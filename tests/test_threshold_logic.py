import unittest
from unittest.mock import MagicMock, patch
import os
import sys
import asyncio

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.dex_cex_arbitrage import DexCexArbitrage
from core.arbitrage_engine import ArbitrageEngine, Opportunity, TradeLeg, LegSide

def run_async(coro):
    return asyncio.get_event_loop().run_until_complete(coro)

class TestThresholdLogic(unittest.TestCase):

    def setUp(self):
        # Mock settings (Updated to 0.2 as per new request)
        self.mock_settings = MagicMock()
        self.mock_settings.min_profit_threshold = 0.2
        self.mock_settings.min_profit_dex_to_cex = 0.2
        self.mock_settings.min_profit_pct_dex_to_cex = 0.2
        self.mock_settings.min_profit_ami_cycle = 0.2
        self.mock_settings.min_profit_pct_ami_cycle = 0.2
        self.mock_settings.min_profit_apt_cycle = 0.2
        self.mock_settings.min_profit_pct_apt_cycle = 0.2
        self.mock_settings.min_profit_cross_cex = 0.2
        self.mock_settings.min_profit_cex_to_cex = 0.2
        self.mock_settings.execution_risk_buffer_pct = 0.5
        self.mock_settings.gas_cost_usd = 0.005
        self.mock_settings.reserve_poll_interval_s = 15.0
        self.mock_settings.trade_amount_usdt = 100.0
        self.mock_settings.min_profit_dex_dex = 0.2
        self.mock_settings.min_profit_apt_start = 0.2
        self.mock_settings.min_profit_pct_apt_start = 0.2
        self.mock_settings.min_profit_ami_start = 0.2
        self.mock_settings.min_profit_pct_ami_start = 0.2
        self.mock_settings.min_dynamic_trade_size_usdt = 5.0
        self.mock_settings.trade_cooldown_s = 2.0
        self.mock_settings.cex_symbol = "AMIUSDT"
        self.mock_settings.apt_cex_symbol = "APTUSDT"
        self.mock_settings.hyperion_fee_rate = 0.001

        # Patch settings globally for core modules
        self.patcher = patch('core.dex_cex_arbitrage.settings', self.mock_settings)
        self.patcher.start()
        self.patcher_engine = patch('core.arbitrage_engine.settings', self.mock_settings)
        self.patcher_engine.start()

    def tearDown(self):
        self.patcher.stop()
        self.patcher_engine.stop()

    def test_dex_cex_risk_buffer(self):
        """Test risk buffer calculation in DexCexArbitrage."""
        mock_collector = MagicMock()
        mock_executor = MagicMock()
        
        with patch('core.dex_cex_arbitrage.get_logger'):
            engine = DexCexArbitrage(mock_collector, mock_executor)
            
            # Case 1: 100 USDT trade, 0.5% buffer = 0.5 USD buffer
            buffer = engine._get_execution_risk_buffer(100.0)
            self.assertEqual(buffer, 0.5)

    def test_dex_cex_min_profit_direction(self):
        """Test direction-specific min profit in DexCexArbitrage."""
        mock_collector = MagicMock()
        mock_executor = MagicMock()
        
        with patch('core.dex_cex_arbitrage.get_logger'):
            engine = DexCexArbitrage(mock_collector, mock_executor)
            
            # Case 1: dex_to_cex, small trade (100 USDT)
            # min_profit_dex_to_cex = 0.2, 0.2% of 100 = 0.2 -> 0.2
            min_p = engine._get_min_profit_for_direction("DEX_TO_CEX_BYBIT", 100.0)
            self.assertEqual(min_p, 0.2)
            
            # Case 2: dex_to_cex, large trade (500 USDT)
            # min_profit_dex_to_cex = 0.2, 0.2% of 500 = 1.0 -> 1.0
            min_p = engine._get_min_profit_for_direction("DEX_TO_CEX_BYBIT", 500.0)
            self.assertEqual(min_p, 1.0)

    def test_arbitrage_engine_filter_logic(self):
        """Test that ArbitrageEngine correctly filters low-profit trades."""
        mock_collector = MagicMock()
        mock_executor = MagicMock()
        
        async def mock_check_balances(*args, **kwargs):
            return True, {}
        mock_executor._check_balances = mock_check_balances
        
        with patch('core.arbitrage_engine.get_logger'):
            engine = ArbitrageEngine(mock_collector, cex_symbol="AMIUSDT", trade_executor=mock_executor)
            
            # Borderline opportunity
            # Profit: 0.6 USD
            # Trade size: 100 USDT
            # Risk buffer (0.5%): 0.5 USD
            # Net profit: 0.1 USD
            # Min required (0.2 USD or 0.2% of 100=0.2): 0.2 USD
            # Should be SHADOW (is_shadow = True)
            
            legs = [TradeLeg("bybit", "AMIUSDT", LegSide.BUY, 0.01, 10000)]
            opp = Opportunity(
                direction="DEX_TO_CEX_BYBIT",
                profit_usdt=0.6,
                legs=legs,
                buy_price=0.01,
                sell_price=0.011,
                log_msg="Test",
                trade_usdt=100.0
            )
            
            engine._check_all_routes = MagicMock(return_value=[opp])
            engine._log_and_execute = MagicMock()
            
            # Run the trigger logic
            run_async(engine._trigger_dex_involved_checks())
            
            # Check results
            self.assertTrue(engine._log_and_execute.called)
            kwargs = engine._log_and_execute.call_args.kwargs
            self.assertTrue(kwargs.get('is_shadow'))
            self.assertIn("Net Profit $0.1000", kwargs.get('skip_reason'))
            self.assertIn("< Min Required $0.2000", kwargs.get('skip_reason'))

    def test_high_quality_trade_passes(self):
        """Test that a high-quality trade passes the filter."""
        mock_collector = MagicMock()
        mock_executor = MagicMock()
        async def mock_check_balances(*args, **kwargs):
            return True, {}
        mock_executor._check_balances = mock_check_balances
        
        with patch('core.arbitrage_engine.get_logger'):
            engine = ArbitrageEngine(mock_collector, cex_symbol="AMIUSDT", trade_executor=mock_executor)
            
            # High quality opportunity
            # Profit: 1.0 USD, Size: 100 USDT, Buffer: 0.5 USD, Net: 0.5 USD, Min: 0.2 USD
            opp = Opportunity(
                direction="DEX_TO_CEX_BYBIT",
                profit_usdt=1.0,
                legs=[],
                buy_price=0.01,
                sell_price=0.015,
                log_msg="Test High Quality",
                trade_usdt=100.0
            )
            
            engine._check_all_routes = MagicMock(return_value=[opp])
            engine._log_and_execute = MagicMock()
            
            run_async(engine._trigger_dex_involved_checks())
            
            self.assertTrue(engine._log_and_execute.called)
            args = engine._log_and_execute.call_args.args
            kwargs = engine._log_and_execute.call_args.kwargs
            
            self.assertFalse(kwargs.get('is_shadow'))
            self.assertEqual(args[5], 0.5) 

if __name__ == '__main__':
    unittest.main()
