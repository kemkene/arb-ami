"""
Panora DEX price poller using REST API.
Fetches swap quotes and updates price collector.
"""

import asyncio
from typing import Optional

from config.settings import settings
from core.price_collector import PriceCollector, PriceData
from exchanges.panora import PanoraClient
from utils.logger import get_logger

logger = get_logger()


class PanoraPoller:
    """Poll Panora DEX for price quotes and update collector.

    Since Panora is a DEX there is no order book — bid == ask.
    We call get_price() which fetches a forward quote (ExactIn)
    and derives a single price used for both bid and ask.
    """

    def __init__(
        self,
        collector: PriceCollector,
        from_amount: float = 1.0,
        from_token_address: Optional[str] = None,
        to_token_address: Optional[str] = None,
        to_wallet_address: Optional[str] = None,
    ):
        self.collector = collector
        self.from_amount = from_amount
        self.from_token_address = from_token_address or settings.ami_token_address
        self.to_token_address = to_token_address or settings.usdt_token_address
        # Use provided wallet, or fall back to settings (APTOS_WALLET_ADDRESS)
        _wallet = to_wallet_address or settings.aptos_wallet_address or None
        self.client = PanoraClient(
            from_token_address=self.from_token_address,
            to_token_address=self.to_token_address,
            to_wallet_address=_wallet,
        )
        self.symbol = f"{self.from_token_address[:4]}_{self.to_token_address[:4]}"

    async def poll(self):
        """Main polling loop for Panora prices."""
        logger.info(
            f"Panora poller started | symbol={self.symbol} | "
            f"poll_interval={settings.panora_poll_interval}s | "
            f"from_amount={self.from_amount}"
        )
        
        while True:
            try:
                result = await self.client.get_price(self.from_amount)
                
                if result:
                    bid_price, ask_price = result  # DEX: bid == ask

                    # DEX has no order book qty; use a large default
                    self.collector.update(
                        "panora",
                        self.symbol,
                        bid=bid_price,
                        ask=ask_price,
                        bid_qty=10000.0,
                        ask_qty=10000.0,
                    )
                else:
                    if self.client.rate_limited:
                        logger.warning(
                            f"Panora price fetch skipped (rate limited) | "
                            f"{self.client.rate_limit_stats()}"
                        )
                    else:
                        logger.warning("Panora price fetch failed")
                
            except Exception as e:
                logger.error(f"Panora poller error: {e}")
            
            await asyncio.sleep(settings.panora_poll_interval)

    async def close(self):
        """Close Panora client session."""
        logger.info(f"Panora poller closing | {self.client.rate_limit_stats()}")
        await self.client.close()
