import unittest
import time
from unittest.mock import patch

from core.arbitrage_engine import ArbitrageEngine
from core.price_collector import PriceCollector


class TestArbitrageEngine(unittest.IsolatedAsyncioTestCase):
    def _engine_with_zero_fees(self, collector: PriceCollector) -> ArbitrageEngine:
        engine = ArbitrageEngine(collector)
        engine.bybit_fee = 0.0
        engine.mexc_fee = 0.0
        engine.min_profit = 0.0
        return engine

    async def test_initialization(self):
        collector = PriceCollector()
        engine = ArbitrageEngine(collector)
        
        # Verify required attributes are initialized
        self.assertTrue(hasattr(engine, "_execution_lock"))
        self.assertTrue(hasattr(engine, "_trade_cooldown_s"))
        self.assertTrue(hasattr(engine, "_last_trade_ts"))
        self.assertTrue(hasattr(engine, "_is_running"))
        self.assertTrue(hasattr(engine, "gas_cost_usd"))
        self.assertTrue(hasattr(engine, "_PRICE_LOG_INTERVAL_S"))
        self.assertTrue(hasattr(engine, "_last_price_log"))
        
        from asyncio import Lock
        self.assertIsInstance(engine._execution_lock, Lock)

    async def test_balance_aware_sizing(self):
        collector = PriceCollector()
        # Price: 1 AMI = 0.0075 APT. 1 APT = 10 USDT.
        # So 1 AMI = 0.075 USDT.
        collector.update("mexc", "AMIUSDT", bid=0.08, ask=0.08)
        collector.update("mexc", "APTUSDT", bid=10.0, ask=10.0)
        collector.update("bybit", "APTUSDT", bid=10.0, ask=10.0)
        collector.update("bybit", "AMIUSDT", bid=0.08, ask=0.08)
        
        engine = ArbitrageEngine(collector)
        engine.cellana_reserves_ami = 100000000 * 10**8 # Large reserves to make it profitable
        engine.cellana_reserves_apt = 750000 * 10**8 # Spot = 0.0075 APT/AMI
        engine.cellana_last_spot = 0.0075
        engine.cellana_last_update_ts = time.time()
        engine.cellana_fee = 0.002
        engine.mexc_fee = 0.001
        engine.min_profit = 0.01

        # Mock BalanceManager
        from unittest.mock import MagicMock
        bm = MagicMock()
        engine.balance_manager = bm
        
        # Scenario: We have very little APT (only 1 APT = 10 USDT)
        # But the optimal size might be 100 USDT.
        bm.get_free.side_effect = lambda ex, asset: 1.0 if asset == "APT" else 1000.0
        
        ami_quote = collector.get_exchange("AMIUSDT", "mexc")
        apt_quote = collector.get_exchange("APTUSDT", "mexc")
        
        opp = engine._check_dex_cex_for_exchange("mexc", ami_quote, apt_quote, engine.mexc_fee)
        
        self.assertIsNotNone(opp)
        # The qty of the first leg (sell APT on DEX/Swap APT) should be <= 0.95 (safe balance)
        apt_leg = opp.legs[0]
        self.assertLessEqual(apt_leg.qty, 0.95)
        self.assertLessEqual(apt_leg.qty * 10, 10.0) # Size in USDT should be around 9.5 USDT

    async def test_check_cex_cex_logs_when_profitable(self):
        collector = PriceCollector()
        collector.update("bybit", "AMIUSDT", bid=99.0, ask=100.0, bid_qty=2.0, ask_qty=2.0)
        collector.update("mexc", "AMIUSDT", bid=102.0, ask=101.0, bid_qty=3.0, ask_qty=3.0)
        bybit = collector.get_exchange("AMIUSDT", "bybit")
        mexc = collector.get_exchange("AMIUSDT", "mexc")
        engine = self._engine_with_zero_fees(collector)

        opp = engine._check_cex_cex(bybit, mexc)
        self.assertIsNotNone(opp)
        self.assertGreater(opp.profit_usdt, 0)

    async def test_check_cex_cex_no_log_when_unprofitable(self):
        collector = PriceCollector()
        collector.update("bybit", "AMIUSDT", bid=101.0, ask=102.0, bid_qty=2.0, ask_qty=2.0)
        collector.update("mexc", "AMIUSDT", bid=100.0, ask=101.0, bid_qty=3.0, ask_qty=3.0)
        bybit = collector.get_exchange("AMIUSDT", "bybit")
        mexc = collector.get_exchange("AMIUSDT", "mexc")
        engine = self._engine_with_zero_fees(collector)

        with patch("core.arbitrage_engine.logger.success") as mock_success:
            engine._check_cex_cex(bybit, mexc)

        mock_success.assert_not_called()

if __name__ == "__main__":
    unittest.main()
