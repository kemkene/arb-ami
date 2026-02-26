import asyncio
import aiohttp

from config.settings import settings
from core.price_collector import PriceCollector
from utils.logger import get_logger

logger = get_logger()

MEXC_REST = settings.mexc_rest_url


class MexcWS:
    """MEXC price poller using REST bookTicker endpoint."""

    def __init__(self, collector: PriceCollector, symbol: str | None = None):
        self.collector = collector
        self.symbol = symbol or settings.cex_symbol

    async def connect(self) -> None:
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                try:
                    async with session.get(
                        MEXC_REST,
                        params={"symbol": self.symbol},
                    ) as resp:
                        if resp.status != 200:
                            logger.warning(f"MEXC HTTP {resp.status}")
                            await asyncio.sleep(settings.mexc_poll_interval)
                            continue

                        data = await resp.json()

                        bid = round(float(data["bidPrice"]), 8)
                        ask = round(float(data["askPrice"]), 8)
                        bid_qty = float(data.get("bidQty", 0))
                        ask_qty = float(data.get("askQty", 0))

                        self.collector.update(
                            "mexc",
                            self.symbol,
                            bid,
                            ask,
                            bid_qty=bid_qty,
                            ask_qty=ask_qty,
                        )

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    logger.warning(f"MEXC REST network error: {e}")
                except (KeyError, ValueError) as e:
                    logger.error(f"MEXC REST parse error: {e}")
                except Exception as e:
                    logger.error(f"MEXC REST unexpected error: {e}")

                await asyncio.sleep(settings.mexc_poll_interval)
