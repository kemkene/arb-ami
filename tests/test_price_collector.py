import unittest

from core.price_collector import PriceCollector


class TestPriceCollector(unittest.TestCase):
    def test_update_stores_bid_ask_and_quantities(self):
        collector = PriceCollector()

        collector.update(
            exchange="bybit",
            symbol="AMIUSDT",
            bid=1.23,
            ask=1.24,
            bid_qty=10.0,
            ask_qty=11.0,
        )

        prices = collector.get("AMIUSDT")
        self.assertIn("bybit", prices)
        bybit = prices["bybit"]
        self.assertEqual(bybit.bid, 1.23)
        self.assertEqual(bybit.ask, 1.24)
        self.assertEqual(bybit.bid_qty, 10.0)
        self.assertEqual(bybit.ask_qty, 11.0)
        self.assertAlmostEqual(bybit.mid, (1.23 + 1.24) / 2)
        self.assertAlmostEqual(bybit.spread, 0.01)

    def test_update_skips_invalid_quote(self):
        collector = PriceCollector()

        collector.update(exchange="mexc", symbol="AMIUSDT", bid=0.0, ask=1.0)
        self.assertEqual(collector.get("AMIUSDT"), {})

        collector.update(exchange="mexc", symbol="AMIUSDT", bid=1.0, ask=-1.0)
        self.assertEqual(collector.get("AMIUSDT"), {})

    def test_get_exchange_returns_specific_exchange_data(self):
        collector = PriceCollector()
        collector.update("bybit", "APTUSDT", 10.0, 10.2, 100, 100)

        apt_bybit = collector.get_exchange("APTUSDT", "bybit")
        apt_mexc = collector.get_exchange("APTUSDT", "mexc")

        self.assertIsNotNone(apt_bybit)
        self.assertIsNone(apt_mexc)
        self.assertEqual(apt_bybit.bid, 10.0)

    def test_get_all_symbols_lists_known_symbols(self):
        collector = PriceCollector()
        collector.update("bybit", "AMIUSDT", 1.0, 1.1)
        collector.update("mexc", "APTUSDT", 10.0, 10.1)

        symbols = collector.get_all_symbols()
        self.assertIn("AMIUSDT", symbols)
        self.assertIn("APTUSDT", symbols)


if __name__ == "__main__":
    unittest.main()
