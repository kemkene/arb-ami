import asyncio
import aiohttp
import time
from typing import Optional
from config.settings import settings
from utils.logger import get_logger

logger = get_logger()

class GasMonitor:
    """Monitors Aptos network gas prices via REST API."""

    def __init__(self):
        self.node_url = settings.aptos_node_url
        self.poll_interval = settings.gas_poll_interval_s
        self.gas_unit_price: int = 100  # Default fallback (octas)
        self.last_update_ts: float = 0.0
        self._is_running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        """Start the background polling task."""
        if self._is_running:
            return
        self._is_running = True
        self._task = asyncio.create_task(self._poll_loop())
        logger.info(f"GasMonitor started | interval={self.poll_interval}s")

    async def stop(self):
        """Stop the background polling task."""
        self._is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("GasMonitor stopped")

    async def _poll_loop(self):
        async with aiohttp.ClientSession() as session:
            while self._is_running:
                try:
                    await self._update_gas_price(session)
                except Exception as e:
                    logger.warning(f"GasMonitor update failed: {e}")
                await asyncio.sleep(self.poll_interval)

    async def _update_gas_price(self, session: aiohttp.ClientSession):
        urls = settings.aptos_node_urls
        for url in urls:
            try:
                estimate_url = f"{url}/estimate_gas_price"
                async with session.get(estimate_url, timeout=5) as resp:
                    if resp.status == 429:
                        continue
                    if resp.status == 200:
                        data = await resp.json()
                        new_price = data.get("gas_estimate", 100)
                        if new_price != self.gas_unit_price:
                            logger.debug(f"Gas price updated: {new_price} octas (via {url})")
                        self.gas_unit_price = int(new_price)
                        self.last_update_ts = time.time()
                        return # Success
            except Exception:
                continue
        logger.warning("Failed to fetch gas price from all nodes")

    def get_gas_unit_price(self) -> int:
        """Returns the latest gas unit price in octas."""
        return self.gas_unit_price
