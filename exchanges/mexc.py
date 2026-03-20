import asyncio
import aiohttp
import time
import json
from typing import List, Optional

from config.settings import settings
from core.price_collector import PriceCollector
from utils.logger import get_logger

logger = get_logger()

_LOG_INTERVAL_S = 15.0  # log MEXC prices at INFO every ~15 seconds
_FEE_CACHE_TTL_S = 24 * 3600  # 24 hours
_FEE_RETRY_TTL_S = 300  # retry fee API every 5 minutes when using fallback/default fees

MEXC_REST = settings.mexc_rest_url
MEXC_TRADE_FEE_API = "https://api.mexc.com/api/v3/tradeFee"


class MexcWS:
    """MEXC price collector using V3 WebSocket."""

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
        
        # Fee cache
        self._maker_fee: Optional[float] = None
        self._taker_fee: Optional[float] = None
        self._symbol_fees: dict[str, dict[str, float]] = {}
        self._fee_cache_time: float = 0.0
        self._fee_is_fallback: bool = True
        
        # Rate limit handling
        self._backoff_until: float = 0.0
        self._current_backoff_s: float = 0.0
        # Hybrid Polling / Recovery
        self._ws_blocked: bool = False
        self._rest_polling_interval: float = 0.5  # 500ms fallback
        self._last_rest_update: float = 0.0

    async def _fetch_fees(self) -> None:
        """Fetch trading fees from MEXC API (with 24-hour cache)."""
        now = time.time()
        if self._maker_fee is not None:
            cache_age = now - self._fee_cache_time
            cache_ttl = _FEE_RETRY_TTL_S if self._fee_is_fallback else _FEE_CACHE_TTL_S
            if cache_age < cache_ttl:
                return  # Cache still valid
        
        try:
            import hmac
            import hashlib
            
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                headers = {"X-MEXC-APIKEY": settings.mexc_api_key}
                symbols_to_fetch = sorted({*self.symbols, settings.cex_symbol, settings.apt_cex_symbol})
                had_fallback = False

                for symbol in symbols_to_fetch:
                    timestamp = int(time.time() * 1000)
                    params = {
                        "timestamp": timestamp,
                        "recvWindow": 5000,
                        "symbol": symbol,
                    }

                    query_string = "&".join([f"{k}={v}" for k, v in params.items()])
                    signature = hmac.new(
                        settings.mexc_api_secret.encode(),
                        query_string.encode(),
                        hashlib.sha256
                    ).hexdigest()
                    params["signature"] = signature

                    try:
                        async with session.get(
                            MEXC_TRADE_FEE_API,
                            params=params,
                            headers=headers
                        ) as resp:
                            if resp.status == 429:
                                retry_after = resp.headers.get("Retry-After")
                                if retry_after:
                                    wait_s = float(retry_after) + 0.1
                                    self._backoff_until = time.time() + wait_s
                                    logger.warning(f"⚠️ [MEXC] Fee API limited. Backing off for {wait_s:.1f}s (Retry-After)")
                                else:
                                    self._current_backoff_s = max(self._MIN_BACKOFF_S, min(self._current_backoff_s * 2, self._MAX_BACKOFF_S))
                                    self._backoff_until = time.time() + self._current_backoff_s
                                    logger.warning(f"⚠️ [MEXC] Fee API limited. Backing off for {self._current_backoff_s:.1f}s")
                                return # Abort fee fetch for now

                            if resp.status != 200:
                                body = (await resp.text())[:300]
                                logger.warning(f"MEXC fee API returned {resp.status} [{symbol}]: {body}")
                                self._symbol_fees[symbol] = {
                                    "maker": settings.mexc_fee,
                                    "taker": settings.mexc_fee,
                                }
                                had_fallback = True
                                continue

                            data = await resp.json()
                            code = data.get("code")
                            fee_data = data.get("data") if isinstance(data, dict) else None

                            if code not in (0, "0"):
                                logger.warning(f"MEXC fee API business error [{symbol}]: {str(data)[:300]}")
                                self._symbol_fees[symbol] = {
                                    "maker": settings.mexc_fee,
                                    "taker": settings.mexc_fee,
                                }
                                had_fallback = True
                                continue

                            maker = fee_data.get("makerCommission") if isinstance(fee_data, dict) else None
                            taker = fee_data.get("takerCommission") if isinstance(fee_data, dict) else None

                            if maker is None or taker is None:
                                logger.warning(f"MEXC API returned invalid fee payload [{symbol}], using defaults: {str(data)[:300]}")
                                self._symbol_fees[symbol] = {
                                    "maker": settings.mexc_fee,
                                    "taker": settings.mexc_fee,
                                }
                                had_fallback = True
                                continue

                            maker_fee = float(maker)
                            taker_fee = float(taker)
                            self._symbol_fees[symbol] = {
                                "maker": maker_fee,
                                "taker": taker_fee,
                            }
                            logger.success(
                                f"✅ [MEXC] Fees updated ({symbol}): maker={maker_fee*100:.4f}% "
                                f"taker={taker_fee*100:.4f}% (cache: 24h)"
                            )
                    except Exception as symbol_error:
                        logger.error(f"Failed to fetch MEXC fees [{symbol}]: {type(symbol_error).__name__}: {symbol_error!r}")
                        self._symbol_fees[symbol] = {
                            "maker": settings.mexc_fee,
                            "taker": settings.mexc_fee,
                        }
                        had_fallback = True

                base_symbol = settings.cex_symbol
                if base_symbol in self._symbol_fees:
                    self._maker_fee = self._symbol_fees[base_symbol]["maker"]
                    self._taker_fee = self._symbol_fees[base_symbol]["taker"]
                elif self._maker_fee is None:
                    self._maker_fee = settings.mexc_fee
                    self._taker_fee = settings.mexc_fee

                self._fee_cache_time = now
                self._fee_is_fallback = had_fallback
        except Exception as e:
            logger.error(f"Failed to fetch MEXC fees: {type(e).__name__}: {e!r}")
            # Keep existing fees or use default
            if self._maker_fee is None:
                self._maker_fee = settings.mexc_fee
                self._taker_fee = settings.mexc_fee
            if not self._symbol_fees:
                self._symbol_fees[settings.cex_symbol] = {
                    "maker": settings.mexc_fee,
                    "taker": settings.mexc_fee,
                }
                self._symbol_fees[settings.apt_cex_symbol] = {
                    "maker": settings.mexc_fee,
                    "taker": settings.mexc_fee,
                }
            self._fee_cache_time = now
            self._fee_is_fallback = True
    
    def get_fee(self, fee_type: str = "taker", symbol: Optional[str] = None) -> float:
        """Get current fee (maker or taker)."""
        symbol_key = symbol or settings.cex_symbol
        symbol_fee = self._symbol_fees.get(symbol_key)
        if symbol_fee is not None:
            if fee_type == "maker":
                return symbol_fee.get("maker", settings.mexc_fee)
            return symbol_fee.get("taker", settings.mexc_fee)
        if fee_type == "maker":
            return self._maker_fee if self._maker_fee is not None else settings.mexc_fee
        return self._taker_fee if self._taker_fee is not None else settings.mexc_fee

    async def _handle_message(self, msg: dict) -> None:
        """Parse MEXC WebSocket ticker data."""
        # Ticker format: {"c": "spot@public.bookTicker.v3.api@AMIUSDT", "d": {"A": "qty", "B": "qty", "a": "price", "b": "price"}}
        channel = msg.get("c", "")
        data = msg.get("d", {})
        
        if "bookTicker" in channel:
            symbol = channel.split("@")[-1].upper()
            self._process_ticker_data(symbol, data)
        elif msg.get("msg") == "PONG":
            logger.debug("MEXC WS PONG received")

    def _process_ticker_data(self, symbol: str, data: dict) -> None:
        bid = float(data.get("b", 0))
        ask = float(data.get("a", 0))
        bid_qty = float(data.get("B", 0))
        ask_qty = float(data.get("A", 0))

        if bid > 0 and ask > 0:
            self.collector.update(
                "mexc", symbol, bid, ask, bid_qty, ask_qty
            )
            
            now = time.time()
            if now - self._last_log.get(symbol, 0) >= _LOG_INTERVAL_S:
                self._last_log[symbol] = now
                if self._ws_blocked:
                    logger.info(
                        f"[MEXC REST-POLL] {symbol} bid={bid} ask={ask} "
                        f"bid_qty={bid_qty} ask_qty={ask_qty}"
                    )
                else:
                    logger.info(
                        f"[MEXC WS] {symbol} bid={bid} ask={ask} "
                        f"bid_qty={bid_qty} ask_qty={ask_qty}"
                    )

    async def connect(self) -> None:
        """Main entry point. We now use REST Polling primarily due to WS stability issues."""
        await self._fetch_fees()
        
        # Start REST polling task as PRIMARY
        logger.info("MEXC: Starting REST Polling loop (WS disabled for stability)")
        polling_task = asyncio.create_task(self._rest_polling_loop())
        
        # Periodic fee refresh
        fee_task = asyncio.create_task(self._periodic_fee_refresh())
        
        try:
            # Just keep the tasks alive
            await asyncio.gather(polling_task, fee_task)
        except asyncio.CancelledError:
            polling_task.cancel()
            fee_task.cancel()
            raise
        except Exception as e:
            logger.error(f"MEXC: Connector error: {e}")
            polling_task.cancel()
            fee_task.cancel()

    async def _rest_polling_loop(self) -> None:
        """Fetch prices via REST API periodically for each symbol."""
        base_url = f"{MEXC_REST}/api/v3/ticker/bookTicker"
        
        while True:
            if time.time() < self._backoff_until:
                await asyncio.sleep(0.5)
                continue
                
            try:
                async with aiohttp.ClientSession() as session:
                    for symbol in self.symbols:
                        url = f"{base_url}?symbol={symbol.upper()}"
                        async with session.get(url, timeout=5) as resp:
                            if resp.status == 200:
                                t = await resp.json()
                                await self._handle_rest_ticker(t)
                            elif resp.status == 429:
                                self._handle_429(resp.headers)
                                break # Stop current cycle on 429
                            elif resp.status == 400:
                                # Often happens if symbol is invalid or not found on MEXC
                                logger.warning(f"MEXC REST Polling 400 for {symbol}: invalid symbol?")
                            else:
                                logger.warning(f"MEXC REST Polling status {resp.status} for {symbol}")
            except Exception as e:
                logger.warning(f"MEXC REST Polling error: {e}")
            
            await asyncio.sleep(self._rest_polling_interval)

    async def _periodic_fee_refresh(self) -> None:
        """Refresh fees every few hours."""
        while True:
            await asyncio.sleep(3600)  # Refresh every 1 hour
            await self._fetch_fees()

    async def _handle_rest_ticker(self, ticker: dict) -> None:
        """Parse ticker data from REST response."""
        # MEXC REST Format: {"symbol": "AMIUSDT", "bidPrice": "...", "bidQty": "...", "askPrice": "...", "askQty": "..."}
        symbol = ticker.get("symbol")
        data = {
            "b": ticker.get("bidPrice"),
            "a": ticker.get("askPrice"),
            "B": ticker.get("bidQty"),
            "A": ticker.get("askQty")
        }
        self._process_ticker_data(symbol, data)

    def _handle_429(self, headers: dict) -> None:
        """Handle Rate Limit (429) from MEXC."""
        retry_after = headers.get("Retry-After")
        if retry_after:
            wait_s = float(retry_after) + 0.5
        else:
            # Default backoff if no header provided
            wait_s = 5.0
            
        self._backoff_until = time.time() + wait_s
        logger.warning(f"⚠️ [MEXC] 429 Rate Limit. Backing off for {wait_s:.1f}s")
