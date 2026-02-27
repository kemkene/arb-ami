import time
from typing import Any, Dict, Optional

from utils.logger import get_logger

logger = get_logger()


class PriceData:
    """Structured price data for a single exchange/symbol."""

    __slots__ = ("bid", "ask", "bid_qty", "ask_qty", "timestamp")

    def __init__(
        self,
        bid: float,
        ask: float,
        bid_qty: float = 0.0,
        ask_qty: float = 0.0,
    ):
        self.bid = bid
        self.ask = ask
        self.bid_qty = bid_qty
        self.ask_qty = ask_qty
        self.timestamp = time.time()

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2

    @property
    def spread(self) -> float:
        return self.ask - self.bid

    @property
    def age(self) -> float:
        """Seconds since last update."""
        return time.time() - self.timestamp

    def is_stale(self, max_age: float = 10.0) -> bool:
        return self.age > max_age

    def __repr__(self) -> str:
        return (
            f"PriceData(bid={self.bid}, ask={self.ask}, "
            f"bid_qty={self.bid_qty}, ask_qty={self.ask_qty})"
        )


class PriceCollector:
    """Central price storage for multiple exchanges and symbols."""

    def __init__(self):
        # {symbol: {exchange: PriceData}}
        self._prices: Dict[str, Dict[str, PriceData]] = {}

    def update(
        self,
        exchange: str,
        symbol: str,
        bid: float,
        ask: float,
        bid_qty: float = 0.0,
        ask_qty: float = 0.0,
    ) -> None:
        if bid <= 0 or ask <= 0:
            logger.warning(
                f"Skip invalid quote from {exchange} {symbol}: bid={bid} ask={ask}"
            )
            return

        self._prices.setdefault(symbol, {})
        self._prices[symbol][exchange] = PriceData(bid, ask, bid_qty, ask_qty)
        logger.debug(
            f"{exchange} {symbol} bid={bid} ask={ask} "
            f"bid_qty={bid_qty} ask_qty={ask_qty}"
        )

    def get(self, symbol: str) -> Dict[str, PriceData]:
        """Return all exchange prices for a symbol."""
        return self._prices.get(symbol, {})

    def get_exchange(
        self, symbol: str, exchange: str
    ) -> Optional[PriceData]:
        """Return price data for a specific exchange/symbol, or None."""
        return self._prices.get(symbol, {}).get(exchange)

    def get_all_symbols(self) -> list[str]:
        return list(self._prices.keys())
