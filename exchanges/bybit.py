import asyncio
import json
import time
from typing import List, Optional
import websockets
import aiohttp
import hmac
import hashlib

from config.settings import settings
from core.price_collector import PriceCollector
from utils.logger import get_logger

logger = get_logger()

_LOG_INTERVAL_S = 15.0  # log Bybit prices at INFO every ~15 seconds
_FEE_CACHE_TTL_S = 24 * 3600  # 24 hours

BYBIT_WS = settings.bybit_ws_url
BYBIT_ACCOUNT_API = "https://api.bybit.com/v5/account/fee-rate"
MAX_RECONNECT_DELAY = 60  # seconds


class BybitWS:
    """Subscribe to multiple symbols on a single Bybit WebSocket connection.

    Uses ``orderbook.{depth}`` topic for multi-level depth with
    snapshot + delta processing.
    """

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
        self._last_log: dict = {}  # {symbol: last_log_timestamp}
        self._depth = settings.bybit_orderbook_depth  # 1, 50, 200, …

        # Local orderbook state for snapshot/delta processing
        # {symbol: {"bids": {price_str: qty_float}, "asks": {price_str: qty_float}}}
        self._local_books: dict[str, dict[str, dict[str, float]]] = {}
        
        # Fee cache
        self._maker_fee: Optional[float] = None
        self._taker_fee: Optional[float] = None
        self._fee_cache_time: float = 0.0
        
        # REST fallback cooldown tracker: {symbol: last_fetch_ts}
        self._rest_last_fetch: dict[str, float] = {}

    async def _fetch_fees(self) -> None:
        """Fetch trading fees from Bybit API (with 24-hour cache)."""
        now = time.time()
        if self._maker_fee is not None and (now - self._fee_cache_time) < _FEE_CACHE_TTL_S:
            return  # Cache still valid
        
        try:
            # Fetch fees via direct HTTP API (requires API key/secret)
            if not settings.bybit_api_key or not settings.bybit_api_secret:
                logger.warning("Bybit credentials not configured, using default fees")
                self._maker_fee = settings.bybit_fee
                self._taker_fee = settings.bybit_fee
                return
            
            # Make HTTP request directly for fee info
            timestamp = int(time.time() * 1000)
            params = {
                "category": "spot",
                "symbol": settings.cex_symbol,
            }
            
            # Build query string for signing
            query_string = "&".join([f"{k}={v}" for k, v in params.items()])
            
            # Build signature
            recv_window = "5000"
            sign_string = f"{timestamp}{settings.bybit_api_key}{recv_window}{query_string}"
            signature = hmac.new(
                settings.bybit_api_secret.encode(),
                sign_string.encode(),
                hashlib.sha256
            ).hexdigest()
            
            headers = {
                "X-BAPI-SIGN": signature,
                "X-BAPI-API-KEY": settings.bybit_api_key,
                "X-BAPI-TIMESTAMP": str(timestamp),
                "X-BAPI-RECV-WINDOW": recv_window,
            }
            
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    BYBIT_ACCOUNT_API,
                    params=params,
                    headers=headers
                ) as resp:
                    if resp.status != 200:
                        logger.warning(f"Bybit fee API returned {resp.status}")
                        # Use default fees
                        if self._maker_fee is None:
                            self._maker_fee = settings.bybit_fee
                            self._taker_fee = settings.bybit_fee
                        return
                    
                    data = await resp.json()
                    if data.get("retCode") != 0:
                        logger.warning(f"Bybit API error: {data.get('retMsg')}")
                        # Use default fees
                        if self._maker_fee is None:
                            self._maker_fee = settings.bybit_fee
                            self._taker_fee = settings.bybit_fee
                        return
                    
                    result = data.get("result", {})
                    fee_list = result.get("list", []) if isinstance(result, dict) else []
                    fee_item = fee_list[0] if fee_list else {}

                    maker = fee_item.get("makerFeeRate")
                    taker = fee_item.get("takerFeeRate")
                    
                    if maker is not None and taker is not None:
                        self._maker_fee = float(maker)
                        self._taker_fee = float(taker)
                        self._fee_cache_time = now
                        
                        logger.success(
                            f"✅ [Bybit] Fees updated: maker={self._maker_fee*100:.4f}% "
                            f"taker={self._taker_fee*100:.4f}% (cache: 24h)"
                        )
                    else:
                        logger.warning(f"Bybit API returned None fees, using defaults")
                        if self._maker_fee is None:
                            self._maker_fee = settings.bybit_fee
                            self._taker_fee = settings.bybit_fee
        except Exception as e:
            logger.error(f"Failed to fetch Bybit fees: {e}")
            # Keep existing fees or use default
            if self._maker_fee is None:
                self._maker_fee = settings.bybit_fee
                self._taker_fee = settings.bybit_fee
    
    def get_fee(self, fee_type: str = "taker") -> float:
        """Get current fee (maker or taker)."""
        if fee_type == "maker":
            return self._maker_fee if self._maker_fee is not None else settings.bybit_fee
        return self._taker_fee if self._taker_fee is not None else settings.bybit_fee

    # ── REST fallback for stale symbols ──────────────────────────────
    BYBIT_DEPTH_URL = "https://api.bybit.com/v5/market/orderbook"
    _REST_COOLDOWN_S = 2.0  # minimum interval between REST fetches per symbol

    async def fetch_orderbook_rest(self, symbol: str) -> bool:
        """Fetch orderbook via REST API as fallback when WS data is stale.

        Returns True if new data was successfully pushed to collector.
        Enforces a per-symbol cooldown to avoid hammering the API.
        """
        now = time.time()
        last = self._rest_last_fetch.get(symbol, 0.0)
        if now - last < self._REST_COOLDOWN_S:
            return False  # cooldown active
        self._rest_last_fetch[symbol] = now

        try:
            limit = min(self._depth, 50)  # REST supports 1/50/200
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=3)
            ) as session:
                params = {"category": "spot", "symbol": symbol, "limit": limit}
                async with session.get(self.BYBIT_DEPTH_URL, params=params) as resp:
                    if resp.status != 200:
                        logger.warning(f"Bybit REST HTTP {resp.status} for {symbol}")
                        return False
                    body = await resp.json()
                    result = body.get("result", {})
                    raw_bids = result.get("b", [])
                    raw_asks = result.get("a", [])
                    if not raw_bids or not raw_asks:
                        return False

                    bids = [(float(p), float(q)) for p, q in raw_bids]
                    asks = [(float(p), float(q)) for p, q in raw_asks]

                    self.collector.update(
                        "bybit", symbol,
                        bid=bids[0][0], ask=asks[0][0],
                        bid_qty=bids[0][1], ask_qty=asks[0][1],
                        bids=bids, asks=asks,
                    )
                    # Also update local book for WS delta continuity
                    book: dict[str, dict[str, float]] = {"bids": {}, "asks": {}}
                    for p, q in raw_bids:
                        book["bids"][p] = float(q)
                    for p, q in raw_asks:
                        book["asks"][p] = float(q)
                    self._local_books[symbol] = book

                    logger.debug(
                        f"[Bybit REST fallback] {symbol} bid={bids[0][0]} "
                        f"ask={asks[0][0]} depth={len(bids)}"
                    )
                    return True
        except Exception as e:
            logger.warning(f"Bybit REST fallback error [{symbol}]: {e}")
            return False

    async def connect(self) -> None:
        # Fetch fees on startup
        await self._fetch_fees()
        
        reconnect_delay = 1
        fee_refresh_counter = 0
        while True:
            try:
                logger.info(f"Bybit WS connecting for {self.symbols} (depth={self._depth})...")
                # Reset local books on reconnect
                self._local_books.clear()

                async with websockets.connect(
                    BYBIT_WS, open_timeout=20, ping_interval=20
                ) as ws:
                    reconnect_delay = 1  # reset on successful connect

                    # Subscribe to orderbook.{depth}.SYMBOL for each symbol
                    args = [f"orderbook.{self._depth}.{s}" for s in self.symbols]
                    await ws.send(json.dumps({"op": "subscribe", "args": args}))
                    logger.info(f"Bybit WS subscribed to {args}")

                    async for raw in ws:
                        msg = json.loads(raw)
                        if "data" not in msg:
                            continue

                        data = msg["data"]
                        topic = msg.get("topic", "")
                        # topic = "orderbook.50.APTUSDT" → symbol = last segment
                        symbol = topic.split(".")[-1] if topic else ""
                        msg_type = msg.get("type", "")

                        if not symbol:
                            continue

                        # ── Snapshot / Delta processing ──────────────
                        if self._depth == 1:
                            # Level-1: every message is a snapshot (simple)
                            bids_raw = data.get("b", [])
                            asks_raw = data.get("a", [])
                            if bids_raw and asks_raw:
                                bid = float(bids_raw[0][0])
                                ask = float(asks_raw[0][0])
                                bid_qty = float(bids_raw[0][1])
                                ask_qty = float(asks_raw[0][1])
                                self.collector.update(
                                    "bybit", symbol, bid, ask,
                                    bid_qty=bid_qty, ask_qty=ask_qty,
                                )
                        else:
                            # Multi-level: snapshot + delta
                            if msg_type == "snapshot":
                                book: dict[str, dict[str, float]] = {"bids": {}, "asks": {}}
                                for p, q in data.get("b", []):
                                    book["bids"][p] = float(q)
                                for p, q in data.get("a", []):
                                    book["asks"][p] = float(q)
                                self._local_books[symbol] = book
                            elif msg_type == "delta":
                                book = self._local_books.get(symbol)
                                if not book:
                                    continue  # skip delta before snapshot
                                for p, q in data.get("b", []):
                                    if float(q) == 0:
                                        book["bids"].pop(p, None)
                                    else:
                                        book["bids"][p] = float(q)
                                for p, q in data.get("a", []):
                                    if float(q) == 0:
                                        book["asks"].pop(p, None)
                                    else:
                                        book["asks"][p] = float(q)
                            else:
                                continue

                            # Build sorted depth and push to collector
                            book = self._local_books.get(symbol)
                            if book:
                                sorted_bids = sorted(
                                    [(float(p), q) for p, q in book["bids"].items()],
                                    reverse=True,
                                )
                                sorted_asks = sorted(
                                    [(float(p), q) for p, q in book["asks"].items()],
                                )
                                if sorted_bids and sorted_asks:
                                    self.collector.update(
                                        "bybit", symbol,
                                        bid=sorted_bids[0][0],
                                        ask=sorted_asks[0][0],
                                        bid_qty=sorted_bids[0][1],
                                        ask_qty=sorted_asks[0][1],
                                        bids=sorted_bids[:self._depth],
                                        asks=sorted_asks[:self._depth],
                                    )

                        # ── Periodic logging ─────────────────────────
                        now = time.time()
                        if now - self._last_log.get(symbol, 0) >= _LOG_INTERVAL_S:
                            self._last_log[symbol] = now
                            pd = self.collector.get_exchange(symbol, "bybit")
                            if pd:
                                logger.info(
                                    f"[Bybit] {symbol} bid={pd.bid} ask={pd.ask} "
                                    f"bid_qty={pd._l1_bid_qty} ask_qty={pd._l1_ask_qty} "
                                    f"depth={pd.depth_levels} "
                                    f"total_bid_liq=${pd.total_bid_liquidity_usdt:.2f} "
                                    f"total_ask_liq=${pd.total_ask_liquidity_usdt:.2f}"
                                )
                        
                        # Refresh fees periodically
                        fee_refresh_counter += 1
                        if fee_refresh_counter >= 100000:
                            await self._fetch_fees()
                            fee_refresh_counter = 0

            except websockets.ConnectionClosed as e:
                logger.warning(f"Bybit WS closed: {e}. Reconnecting in {reconnect_delay}s...")
            except Exception as e:
                logger.error(f"Bybit WS error: {e}. Reconnecting in {reconnect_delay}s...")

            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, MAX_RECONNECT_DELAY)
