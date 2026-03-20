import time
from typing import Any, Dict, List, Optional, Tuple

from utils.logger import get_logger

logger = get_logger()

# Type alias for orderbook level: (price, quantity)
OrderbookLevel = Tuple[float, float]


class PriceData:
    """Structured price data for a single exchange/symbol.

    Supports multi-level orderbook depth.  When *bids* / *asks* lists are
    populated the ``bid_qty`` and ``ask_qty`` properties return the **total**
    quantity across all depth levels (enabling larger trade sizing).  L1-only
    quantities are still available via ``best_bid_qty`` / ``best_ask_qty``.
    """

    __slots__ = (
        "bid", "ask", "_l1_bid_qty", "_l1_ask_qty", "timestamp",
        "bids", "asks",
    )

    def __init__(
        self,
        bid: float,
        ask: float,
        bid_qty: float = 0.0,
        ask_qty: float = 0.0,
        bids: Optional[List[OrderbookLevel]] = None,
        asks: Optional[List[OrderbookLevel]] = None,
    ):
        self.bid = bid
        self.ask = ask
        self._l1_bid_qty = bid_qty
        self._l1_ask_qty = ask_qty
        self.timestamp = time.time()
        # Full depth: bids sorted descending by price, asks ascending
        self.bids: List[OrderbookLevel] = bids or []
        self.asks: List[OrderbookLevel] = asks or []

    # ── Quantity properties (total depth or L1 fallback) ──────────────

    @property
    def bid_qty(self) -> float:
        """Total bid quantity across all depth levels (L1 fallback)."""
        if self.bids:
            return sum(q for _, q in self.bids)
        return self._l1_bid_qty

    @property
    def ask_qty(self) -> float:
        """Total ask quantity across all depth levels (L1 fallback)."""
        if self.asks:
            return sum(q for _, q in self.asks)
        return self._l1_ask_qty

    @property
    def best_bid_qty(self) -> float:
        """L1 best-bid quantity only."""
        if self.bids:
            return self.bids[0][1]
        return self._l1_bid_qty

    @property
    def best_ask_qty(self) -> float:
        """L1 best-ask quantity only."""
        if self.asks:
            return self.asks[0][1]
        return self._l1_ask_qty

    @property
    def total_bid_liquidity_usdt(self) -> float:
        """Sum(price × qty) across all bid levels."""
        if self.bids:
            return sum(p * q for p, q in self.bids)
        return self._l1_bid_qty * self.bid if self._l1_bid_qty > 0 else 0.0

    @property
    def total_ask_liquidity_usdt(self) -> float:
        """Sum(price × qty) across all ask levels."""
        if self.asks:
            return sum(p * q for p, q in self.asks)
        return self._l1_ask_qty * self.ask if self._l1_ask_qty > 0 else 0.0

    @property
    def depth_levels(self) -> int:
        """Number of orderbook levels available (max of bids/asks)."""
        return max(len(self.bids), len(self.asks))

    # ── Effective fill-price helpers ──────────────────────────────────

    def effective_buy_price(self, target_usdt: float) -> float:
        """Weighted-average ask price to **spend** *target_usdt* buying.

        Walks through ask levels (ascending price) and computes the
        volume-weighted average fill price.  Falls back to BBO ask when
        no depth data is available.
        """
        if not self.asks or target_usdt <= 0:
            return self.ask
        spent = 0.0
        bought = 0.0
        for price, qty in self.asks:
            remaining = target_usdt - spent
            if remaining <= 0:
                break
            level_usdt = qty * price
            fill_usdt = min(level_usdt, remaining)
            fill_qty = fill_usdt / price
            spent += fill_usdt
            bought += fill_qty
        return spent / bought if bought > 0 else self.ask

    def effective_sell_price(self, target_qty: float) -> float:
        """Weighted-average bid price for **selling** *target_qty* units.

        Walks through bid levels (descending price) and computes the
        volume-weighted average fill price.  Falls back to BBO bid when
        no depth data is available.
        """
        if not self.bids or target_qty <= 0:
            return self.bid
        sold = 0.0
        received = 0.0
        for price, qty in self.bids:
            remaining = target_qty - sold
            if remaining <= 0:
                break
            fill_qty = min(qty, remaining)
            fill_usdt = fill_qty * price
            sold += fill_qty
            received += fill_usdt
        return received / sold if sold > 0 else self.bid

    # ── Standard properties ───────────────────────────────────────────

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
        depth = self.depth_levels
        return (
            f"PriceData(bid={self.bid}, ask={self.ask}, "
            f"bid_qty={self._l1_bid_qty}, ask_qty={self._l1_ask_qty}, "
            f"depth={depth})"
        )


class PriceCollector:
    """Central price storage for multiple exchanges and symbols.

    Supports an optional *on_update* callback that fires on every price
    change so that downstream consumers (e.g. arb engine) can react
    immediately instead of polling on a timer.
    """

    def __init__(self, on_update=None):
        # {symbol: {exchange: PriceData}}
        self._prices: Dict[str, Dict[str, PriceData]] = {}
        # Callback: on_update(exchange: str, symbol: str)
        self._on_update = on_update

    def set_on_update(self, callback) -> None:
        """Register (or replace) the on-update callback."""
        self._on_update = callback

    def update(
        self,
        exchange: str,
        symbol: str,
        bid: float,
        ask: float,
        bid_qty: float = 0.0,
        ask_qty: float = 0.0,
        bids: Optional[List[OrderbookLevel]] = None,
        asks: Optional[List[OrderbookLevel]] = None,
    ) -> None:
        if bid <= 0 or ask <= 0:
            logger.warning(
                f"Skip invalid quote from {exchange} {symbol}: bid={bid} ask={ask}"
            )
            return

        self._prices.setdefault(symbol, {})
        self._prices[symbol][exchange] = PriceData(
            bid, ask, bid_qty, ask_qty, bids=bids, asks=asks,
        )
        depth = max(len(bids or []), len(asks or []))
        logger.debug(
            f"{exchange} {symbol} bid={bid} ask={ask} "
            f"bid_qty={bid_qty} ask_qty={ask_qty} depth={depth}"
        )

    def update_data_age(self, exchange: str, symbol: str) -> None:
        """Manually mark the data for an exchange/symbol as 'fresh' by updating its timestamp."""
        if symbol in self._prices and exchange in self._prices[symbol]:
            self._prices[symbol][exchange].timestamp = time.time()
            # Still fire callback so engine can react if needed
            if self._on_update is not None:
                try:
                    self._on_update(exchange, symbol)
                except Exception:
                    pass

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
