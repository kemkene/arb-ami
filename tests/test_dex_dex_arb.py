import unittest
from unittest.mock import patch, MagicMock
import time
from core.arbitrage_engine import ArbitrageEngine
from core.price_collector import PriceCollector

class TestDexDexArb(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.collector = PriceCollector()
        self.engine = ArbitrageEngine(self.collector)
        self.engine.enable_dex_dex_arb = True
        self.engine.min_profit_dex_dex = 0.01
        self.engine.gas_fee_dex_dex_apt = 0.005
        self.engine.cellana_fee = 0.001
        self.engine.hyperion_fee = 0.003

    async def test_check_dex_dex_profitable_direction_1(self):
        """Test APT -> Cellana -> AMI -> Hyperion -> APT is profitable."""
        # 1. Setup Cellana state (APT/AMI Price higher on Cellana or lower on Hyperion)
        # Let's say 1 APT = 120 AMI on Cellana (Cheap AMI)
        # r_apt = 1,000, r_ami = 120,000 => Spot 120 AMI/APT
        self.engine.cellana_reserves_apt = 1000 * 10**8
        self.engine.cellana_reserves_ami = 120000 * 10**8
        self.engine.cellana_last_update_ts = time.time()

        # 2. Setup Hyperion state (Sell AMI for 110 APT)
        # sqrt_price_x64 for ~110 AMI/APT
        # sqrt(110) * 2^64
        import math
        sqrt_p = math.sqrt(110)
        self.engine.hyperion_sqrt_price_x64 = int(sqrt_p * (2**64))
        self.engine.hyperion_liquidity = 10**14
        self.engine.hyperion_last_update_ts = time.time()

        # 3. Setup reference APT price
        apt_mid = 10.0 # 1 APT = 10 USDT

        # APT start = $100 / $10 = 10 APT
        results = self.engine._check_dex_dex(apt_mid)

        # Direction 1 calculation:
        # 10 APT -> Cellana -> ~1188 AMI (approx)
        # 1188 AMI -> Hyperion -> ~10.7 APT (approx)
        # 10.7 - 10 - 0.005 = 0.695 APT profit (~$6.95)
        self.assertGreater(len(results), 0)
        opp = results[0]
        self.assertEqual(opp.direction, "DEX_DEX_CELLANA_HYPERION")
        self.assertGreater(opp.profit_usdt, 0)

    async def test_check_dex_dex_unprofitable(self):
        """Test when both sides are unprofitable."""
        self.engine.cellana_reserves_apt = 1000 * 10**8
        self.engine.cellana_reserves_ami = 100000 * 10**8 # 100 AMI/APT
        self.engine.cellana_last_update_ts = time.time()

        import math
        sqrt_p = math.sqrt(100) # 100 AMI/APT
        self.engine.hyperion_sqrt_price_x64 = int(sqrt_p * (2**64))
        self.engine.hyperion_liquidity = 10**14
        self.engine.hyperion_last_update_ts = time.time()

        apt_mid = 10.0

        results = self.engine._check_dex_dex(apt_mid)
        self.assertEqual(len(results), 0)

if __name__ == "__main__":
    unittest.main()
