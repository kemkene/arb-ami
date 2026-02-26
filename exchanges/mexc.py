import asyncio
import aiohttp
from typing import List

from config.settings import settings
from core.price_collector import PriceCollector
from utils.logger import get_logger

logger = get_logger()

MEXC_REST = settings.mexc_rest_url


class MexcWS:
    """MEXC price poller using REST bookTicker endpoint (multi-symbol)."""

    def __init__(
        self,
        collector: PriceCollector,
        symbols: List[str] | str | None = None,
    ):
        self.collector = collector
        if symbols is None:
            self.symbols = [settings.cex_symbol]
        elif isinstance(symbols, str):
            self.symbols = [symbols]
        else:
            self.symbols = list(symbols)

    async def _poll_symbol(self, session: aiohttp.ClientSession, symbol: str) -> None:
        """Fetch bookTicker for a single symbol and update collector."""
        try:
            async with session.get(MEXC_REST, params={"symbol": symbol}) as resp:
                if resp.status != 200:
                    logger.warning(f"MEXC HTTP {resp.status} for {symbol}")
                    return
                data = await resp.json()
                bid     = round(float(data["bidPrice"]), 8)
                ask     = round(float(data["askPrice"]), 8)
                bid_qty = float(data.get("bidQty", 0))
                ask_qty = float(data.get("askQty", 0))
                self.collector.update(
                    "mexc", symbol, bid, ask,
                    bid_qty=bid_qty, ask_qty=ask_qty,
                )
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"MEXC REST network error [{symbol}]: {e}")
        except (KeyError, ValueError) as e:
            logger.error(f"MEXC REST parse error [{symbol}]: {e}")
        except Exception as e:
            logger.error(f"MEXC REST unexpected error [{symbol}]: {e}")

    async def connect(self) -> None:
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                # Fetch all symbols concurrently each poll cycle
                await asyncio.gather(
                    *[self._poll_symbol(session, s) for s in self.symbols]
                )
                await asyncio.sleep(settings.mexc_poll_interval)
