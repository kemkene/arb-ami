"""
Test scenarios for arbitrage opportunities involving Panora DEX.
Simulates DEX-CEX arbitrage between Panora and Bybit/MEXC.
"""

import asyncio
import unittest
from unittest.mock import patch, MagicMock

from core.price_collector import PriceCollector
from core.arbitrage_engine import ArbitrageEngine
from config.settings import settings


def _mock_panora_client(panora_price):
    """Create a mock PanoraClient that verifies quotes at the given price."""
    client = MagicMock()

    async def mock_quote(from_amount, from_token_address=None, to_token_address=None):
        if to_token_address == settings.usdt_token_address:
            # Selling AMI → USDC
            return {"toTokenAmount": str(from_amount * panora_price)}
        else:
            # Buying AMI with USDC
            return {"toTokenAmount": str(from_amount / panora_price)}

    client.get_swap_quote = mock_quote
    client.parse_to_token_amount = lambda data: float(data.get("toTokenAmount", 0))

    return client


class TestPanoraArbitrage(unittest.IsolatedAsyncioTestCase):
    """Test arbitrage detection between Panora DEX and CEX exchanges."""

    PANORA_SYMBOL = f"{settings.ami_token_address[:4]}_{settings.usdt_token_address[:4]}"

    def _make_engine(self, collector: PriceCollector, panora_price=None) -> ArbitrageEngine:
        pc = _mock_panora_client(panora_price) if panora_price else None
        engine = ArbitrageEngine(collector, panora_client=pc)
        engine.bybit_fee = 0.001   # 0.1%
        engine.mexc_fee = 0.001    # 0.1%
        engine.panora_fee = 0.003  # 0.3%
        engine.min_profit = 0.0
        return engine

    def _make_engine_zero_fees(self, collector: PriceCollector, panora_price=None) -> ArbitrageEngine:
        pc = _mock_panora_client(panora_price) if panora_price else None
        engine = ArbitrageEngine(collector, panora_client=pc)
        engine.bybit_fee = 0.0
        engine.mexc_fee = 0.0
        engine.panora_fee = 0.0
        engine.min_profit = 0.0
        return engine

    # ------------------------------------------------------------------ #
    #  DEX-CEX: Panora vs Bybit
    # ------------------------------------------------------------------ #
    async def test_buy_panora_sell_bybit_profitable(self):
        """Panora price lower than Bybit bid → buy Panora, sell Bybit."""
        collector = PriceCollector()

        # Panora: bid=ask=0.0070 (DEX, no spread)
        collector.update("panora", self.PANORA_SYMBOL,
                         bid=0.0070, ask=0.0070,
                         bid_qty=10000.0, ask_qty=10000.0)

        # Bybit: bid=0.0080 (higher than panora ask)
        collector.update("bybit", settings.cex_symbol,
                         bid=0.0080, ask=0.0081,
                         bid_qty=5000.0, ask_qty=5000.0)

        engine = self._make_engine_zero_fees(collector, panora_price=0.007)
        panora = collector.get_exchange(self.PANORA_SYMBOL, "panora")
        bybit = collector.get_exchange(settings.cex_symbol, "bybit")

        with patch("core.arbitrage_engine.logger.success") as mock_success:
            await engine._check_dex_cex(panora, bybit, "Bybit")

        # Should detect: buy Panora @ 0.0070, sell Bybit @ 0.0080
        self.assertGreaterEqual(mock_success.call_count, 1)
        call_args = mock_success.call_args[0][0]
        self.assertIn("Panora", call_args)
        self.assertIn("Bybit", call_args)

    async def test_buy_bybit_sell_panora_profitable(self):
        """Bybit ask lower than Panora bid → buy Bybit, sell Panora."""
        collector = PriceCollector()

        # Panora: bid=ask=0.0090
        collector.update("panora", self.PANORA_SYMBOL,
                         bid=0.0090, ask=0.0090,
                         bid_qty=10000.0, ask_qty=10000.0)

        # Bybit: ask=0.0080 (lower than panora bid)
        collector.update("bybit", settings.cex_symbol,
                         bid=0.0079, ask=0.0080,
                         bid_qty=5000.0, ask_qty=5000.0)

        engine = self._make_engine_zero_fees(collector, panora_price=0.009)
        panora = collector.get_exchange(self.PANORA_SYMBOL, "panora")
        bybit = collector.get_exchange(settings.cex_symbol, "bybit")

        with patch("core.arbitrage_engine.logger.success") as mock_success:
            await engine._check_dex_cex(panora, bybit, "Bybit")

        # Should detect: buy Bybit @ 0.0080, sell Panora @ 0.0090
        self.assertGreaterEqual(mock_success.call_count, 1)
        call_args = mock_success.call_args[0][0]
        self.assertIn("Bybit", call_args)
        self.assertIn("Panora", call_args)

    # ------------------------------------------------------------------ #
    #  DEX-CEX: Panora vs MEXC
    # ------------------------------------------------------------------ #
    async def test_buy_panora_sell_mexc_profitable(self):
        """Panora price lower than MEXC bid → buy Panora, sell MEXC."""
        collector = PriceCollector()

        collector.update("panora", self.PANORA_SYMBOL,
                         bid=0.0070, ask=0.0070,
                         bid_qty=10000.0, ask_qty=10000.0)

        collector.update("mexc", settings.cex_symbol,
                         bid=0.0082, ask=0.0083,
                         bid_qty=3000.0, ask_qty=3000.0)

        engine = self._make_engine_zero_fees(collector, panora_price=0.007)
        panora = collector.get_exchange(self.PANORA_SYMBOL, "panora")
        mexc = collector.get_exchange(settings.cex_symbol, "mexc")

        with patch("core.arbitrage_engine.logger.success") as mock_success:
            await engine._check_dex_cex(panora, mexc, "MEXC")

        self.assertGreaterEqual(mock_success.call_count, 1)
        call_args = mock_success.call_args[0][0]
        self.assertIn("Panora", call_args)
        self.assertIn("MEXC", call_args)

    async def test_buy_mexc_sell_panora_profitable(self):
        """MEXC ask lower than Panora bid → buy MEXC, sell Panora."""
        collector = PriceCollector()

        collector.update("panora", self.PANORA_SYMBOL,
                         bid=0.0090, ask=0.0090,
                         bid_qty=10000.0, ask_qty=10000.0)

        collector.update("mexc", settings.cex_symbol,
                         bid=0.0079, ask=0.0080,
                         bid_qty=3000.0, ask_qty=3000.0)

        engine = self._make_engine_zero_fees(collector, panora_price=0.009)
        panora = collector.get_exchange(self.PANORA_SYMBOL, "panora")
        mexc = collector.get_exchange(settings.cex_symbol, "mexc")

        with patch("core.arbitrage_engine.logger.success") as mock_success:
            await engine._check_dex_cex(panora, mexc, "MEXC")

        self.assertGreaterEqual(mock_success.call_count, 1)

    # ------------------------------------------------------------------ #
    #  No arbitrage cases
    # ------------------------------------------------------------------ #
    async def test_no_arb_panora_bybit_same_price(self):
        """No arbitrage when prices are equal (zero-fee scenario)."""
        collector = PriceCollector()

        collector.update("panora", self.PANORA_SYMBOL,
                         bid=0.0080, ask=0.0080,
                         bid_qty=10000.0, ask_qty=10000.0)

        collector.update("bybit", settings.cex_symbol,
                         bid=0.0080, ask=0.0080,
                         bid_qty=5000.0, ask_qty=5000.0)

        engine = self._make_engine_zero_fees(collector)
        panora = collector.get_exchange(self.PANORA_SYMBOL, "panora")
        bybit = collector.get_exchange(settings.cex_symbol, "bybit")

        with patch("core.arbitrage_engine.logger.success") as mock_success:
            await engine._check_dex_cex(panora, bybit, "Bybit")

        mock_success.assert_not_called()

    async def test_no_arb_with_fees_eating_profit(self):
        """Small price diff eaten by fees → no arb logged."""
        collector = PriceCollector()

        # Panora ask=0.00800, Bybit bid=0.00802
        # Spread ~0.25% but fees total 0.4% (panora 0.3% + bybit 0.1%)
        collector.update("panora", self.PANORA_SYMBOL,
                         bid=0.00800, ask=0.00800,
                         bid_qty=10000.0, ask_qty=10000.0)

        collector.update("bybit", settings.cex_symbol,
                         bid=0.00802, ask=0.00803,
                         bid_qty=5000.0, ask_qty=5000.0)

        engine = self._make_engine(collector)  # with real fees
        panora = collector.get_exchange(self.PANORA_SYMBOL, "panora")
        bybit = collector.get_exchange(settings.cex_symbol, "bybit")

        with patch("core.arbitrage_engine.logger.success") as mock_success:
            await engine._check_dex_cex(panora, bybit, "Bybit")

        mock_success.assert_not_called()

    async def test_no_arb_panora_more_expensive(self):
        """Panora is more expensive than CEX → no arb for buy-panora direction."""
        collector = PriceCollector()

        # Panora: 0.0090 (expensive)
        collector.update("panora", self.PANORA_SYMBOL,
                         bid=0.0090, ask=0.0090,
                         bid_qty=10000.0, ask_qty=10000.0)

        # Bybit bid: 0.0080 (lower than panora)
        collector.update("bybit", settings.cex_symbol,
                         bid=0.0080, ask=0.0081,
                         bid_qty=5000.0, ask_qty=5000.0)

        engine = self._make_engine_zero_fees(collector, panora_price=0.009)
        panora = collector.get_exchange(self.PANORA_SYMBOL, "panora")
        bybit = collector.get_exchange(settings.cex_symbol, "bybit")

        with patch("core.arbitrage_engine.logger.success") as mock_success:
            await engine._check_dex_cex(panora, bybit, "Bybit")

        # Direction 1 (buy panora @ 0.009, sell bybit @ 0.008) → loss
        # Direction 2 (buy bybit @ 0.0081, sell panora @ 0.009) → profit
        # Only direction 2 should trigger
        self.assertEqual(mock_success.call_count, 1)
        call_args = mock_success.call_args[0][0]
        self.assertIn("BUY Bybit", call_args)
        self.assertIn("SELL Panora", call_args)

    # ------------------------------------------------------------------ #
    #  Profit calculation tests
    # ------------------------------------------------------------------ #
    async def test_profit_calculation_with_fees(self):
        """Verify exact profit calculation for DEX-CEX arb with fees."""
        collector = PriceCollector()
        engine = self._make_engine(collector)

        buy_price = 0.0070   # buy on Panora
        sell_price = 0.0080  # sell on Bybit
        qty = 10000.0

        bv, sv, profit = engine._calc_profit(
            buy_price, sell_price, qty,
            engine.panora_fee, engine.bybit_fee,
        )

        expected_buy_vol = qty * buy_price       # 70.0
        expected_sell_vol = qty * sell_price      # 80.0
        expected_profit = (
            expected_sell_vol - expected_buy_vol
            - expected_buy_vol * 0.003   # panora fee
            - expected_sell_vol * 0.001  # bybit fee
        )

        self.assertAlmostEqual(bv, expected_buy_vol)
        self.assertAlmostEqual(sv, expected_sell_vol)
        self.assertAlmostEqual(profit, expected_profit, places=6)
        self.assertGreater(profit, 0)  # Should be profitable

    async def test_profit_negative_when_spread_too_small(self):
        """Fees exceed spread → negative profit."""
        collector = PriceCollector()
        engine = self._make_engine(collector)

        buy_price = 0.00800   # buy on Panora
        sell_price = 0.00802  # sell on Bybit — only 0.25% spread
        qty = 10000.0

        bv, sv, profit = engine._calc_profit(
            buy_price, sell_price, qty,
            engine.panora_fee, engine.bybit_fee,
        )

        # Fees (0.4%) > spread (0.25%) → loss
        self.assertLess(profit, 0)

    # ------------------------------------------------------------------ #
    #  Stale data tests
    # ------------------------------------------------------------------ #
    async def test_stale_panora_data_skipped(self):
        """Stale Panora data should be skipped."""
        collector = PriceCollector()

        collector.update("panora", self.PANORA_SYMBOL,
                         bid=0.0070, ask=0.0070,
                         bid_qty=10000.0, ask_qty=10000.0)

        collector.update("bybit", settings.cex_symbol,
                         bid=0.0080, ask=0.0081,
                         bid_qty=5000.0, ask_qty=5000.0)

        engine = self._make_engine_zero_fees(collector)
        panora = collector.get_exchange(self.PANORA_SYMBOL, "panora")
        bybit = collector.get_exchange(settings.cex_symbol, "bybit")

        # Force panora data to be stale
        panora.timestamp = 0.0

        with patch("core.arbitrage_engine.logger.success") as mock_success:
            await engine._check_dex_cex(panora, bybit, "Bybit")

        mock_success.assert_not_called()

    # ------------------------------------------------------------------ #
    #  Qty-limited tests
    # ------------------------------------------------------------------ #
    async def test_qty_is_min_of_both_sides(self):
        """Trade qty should be min(panora_ask_qty, cex_bid_qty)."""
        collector = PriceCollector()

        collector.update("panora", self.PANORA_SYMBOL,
                         bid=0.0070, ask=0.0070,
                         bid_qty=10000.0, ask_qty=10000.0)

        # Bybit with small bid qty
        collector.update("bybit", settings.cex_symbol,
                         bid=0.0090, ask=0.0091,
                         bid_qty=50.0, ask_qty=5000.0)

        engine = self._make_engine_zero_fees(collector, panora_price=0.007)
        panora = collector.get_exchange(self.PANORA_SYMBOL, "panora")
        bybit = collector.get_exchange(settings.cex_symbol, "bybit")

        with patch("core.arbitrage_engine.logger.success") as mock_success:
            await engine._check_dex_cex(panora, bybit, "Bybit")

        # Direction 1: Buy panora ask_qty=10000, sell bybit bid_qty=50 → qty=50
        self.assertGreaterEqual(mock_success.call_count, 1)
        call_args = mock_success.call_args_list[0][0][0]
        self.assertIn("QTY=50.000000", call_args)


class TestPanoraArbitrageFullLoop(unittest.IsolatedAsyncioTestCase):
    """Test the full arbitrage engine run loop with Panora integrated."""

    PANORA_SYMBOL = f"{settings.ami_token_address[:4]}_{settings.usdt_token_address[:4]}"

    async def test_run_loop_detects_panora_bybit_arb(self):
        """ArbitrageEngine.run() checks Panora-Bybit in main loop."""
        collector = PriceCollector()

        # Set up profitable panora-bybit scenario
        collector.update("panora", self.PANORA_SYMBOL,
                         bid=0.0070, ask=0.0070,
                         bid_qty=10000.0, ask_qty=10000.0)
        collector.update("bybit", settings.cex_symbol,
                         bid=0.0090, ask=0.0091,
                         bid_qty=5000.0, ask_qty=5000.0)

        engine = ArbitrageEngine(collector, panora_client=_mock_panora_client(0.007))
        engine.bybit_fee = 0.0
        engine.mexc_fee = 0.0
        engine.panora_fee = 0.0
        engine.min_profit = 0.0
        engine.poll_interval = 0.01

        detected = []

        with patch("core.arbitrage_engine.logger.success") as mock_success:
            mock_success.side_effect = lambda msg: detected.append(msg)

            async def stop_after_one():
                await asyncio.sleep(0.05)
                raise asyncio.CancelledError()

            try:
                await asyncio.wait_for(engine.run(), timeout=0.1)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        # Should have detected panora-bybit opportunity
        panora_opps = [d for d in detected if "Panora" in d]
        self.assertGreater(len(panora_opps), 0)

    async def test_run_loop_detects_panora_mexc_arb(self):
        """ArbitrageEngine.run() checks Panora-MEXC in main loop."""
        collector = PriceCollector()

        collector.update("panora", self.PANORA_SYMBOL,
                         bid=0.0070, ask=0.0070,
                         bid_qty=10000.0, ask_qty=10000.0)
        collector.update("mexc", settings.cex_symbol,
                         bid=0.0085, ask=0.0086,
                         bid_qty=3000.0, ask_qty=3000.0)

        engine = ArbitrageEngine(collector, panora_client=_mock_panora_client(0.007))
        engine.bybit_fee = 0.0
        engine.mexc_fee = 0.0
        engine.panora_fee = 0.0
        engine.min_profit = 0.0
        engine.poll_interval = 0.01

        detected = []

        with patch("core.arbitrage_engine.logger.success") as mock_success:
            mock_success.side_effect = lambda msg: detected.append(msg)

            try:
                await asyncio.wait_for(engine.run(), timeout=0.1)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        panora_opps = [d for d in detected if "Panora" in d]
        self.assertGreater(len(panora_opps), 0)


if __name__ == "__main__":
    unittest.main()
