import unittest
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

    async def test_check_cex_cex_logs_when_profitable(self):
        collector = PriceCollector()
        collector.update("bybit", "AMIUSDT", bid=99.0, ask=100.0, bid_qty=2.0, ask_qty=2.0)
        collector.update("mexc", "AMIUSDT", bid=102.0, ask=101.0, bid_qty=3.0, ask_qty=3.0)
        bybit = collector.get_exchange("AMIUSDT", "bybit")
        mexc = collector.get_exchange("AMIUSDT", "mexc")
        engine = self._engine_with_zero_fees(collector)

        with patch("core.arbitrage_engine.logger.success") as mock_success:
            engine._check_cex_cex(bybit, mexc)

        self.assertGreaterEqual(mock_success.call_count, 1)

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
