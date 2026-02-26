import asyncio
import time
import aiohttp
from typing import Any, Dict, Optional, Tuple

from config.settings import settings
from utils.logger import get_logger

logger = get_logger()

# How long (seconds) a cached quote is considered fresh.
# Set equal to poll interval so the cache expires just as a new poll arrives.
_QUOTE_CACHE_TTL: float = settings.panora_poll_interval


class PanoraClient:
    """Unified Panora DEX client with session reuse and rate limiting handling."""

    def __init__(
        self,
        from_token_address: str | None = None,
        to_token_address: str | None = None,
        to_wallet_address: str | None = None,
        max_retries: int = 3,
        base_retry_delay: float = 1.0,
    ):
        self.api_url = settings.panora_api_url
        self.api_key = settings.panora_api_key
        self.from_token_address = from_token_address or settings.ami_token_address
        self.to_token_address = to_token_address or settings.usdt_token_address
        self.to_wallet_address = to_wallet_address
        self._session: Optional[aiohttp.ClientSession] = None

        # ── Quote cache ──────────────────────────────────────────────────
        # key: (from_token_address, to_token_address, rounded_amount)
        # value: (quote_dict, fetched_at_timestamp)
        # Prevents hammering Panora when the arb engine verifies the same
        # opportunity repeatedly within one poll window.
        self._quote_cache: Dict[tuple, Tuple[Dict[str, Any], float]] = {}

        # ── Unit-price cache ─────────────────────────────────────────────
        # key: (from_token_address, to_token_address)
        # value: (price_per_unit, fetched_at_timestamp)
        # When the arb engine calls get_swap_quote(actual_qty) for
        # verification, the amount differs from the poller's amount=1.0
        # so the regular quote cache always misses.  The unit-price cache
        # stores price/unit from the most recent real HTTP call and returns
        # a lightweight *synthetic* quote (toTokenAmount = price × qty)
        # without any extra HTTP request while the price is still fresh.
        # Synthetic quotes are intentionally missing txData so the executor
        # fetches a real quote only at execution time.
        self._unit_price_cache: Dict[Tuple[str, str], Tuple[float, float]] = {}

        # Rate limiting tracking
        self.max_retries = max_retries
        self.base_retry_delay = base_retry_delay
        self.rate_limited = False
        self._rate_limit_count = 0
        self._last_rate_limit_time = 0.0
        self._total_requests = 0
        self._total_rate_limits = 0
        self._cache_hits = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=10)
            self._session = aiohttp.ClientSession(
                headers={"x-api-key": self.api_key},
                timeout=timeout,
            )
        return self._session

    @staticmethod
    def _cache_key(from_addr: str, to_addr: str, amount: float) -> tuple:
        """Round amount to 6 sig-figs so near-identical quantities share a cache slot."""
        rounded = float(f"{amount:.6g}")
        return (from_addr, to_addr, rounded)

    # ── Unit-price cache helpers ──────────────────────────────────────────

    def _get_unit_price(self, from_addr: str, to_addr: str) -> Optional[float]:
        """Return cached price-per-unit if still fresh, else None."""
        entry = self._unit_price_cache.get((from_addr, to_addr))
        if entry and (time.time() - entry[1]) < _QUOTE_CACHE_TTL:
            return entry[0]
        return None

    def _store_unit_price(
        self, from_addr: str, to_addr: str, from_amount: float, to_amount: float
    ) -> None:
        """Persist price-per-unit from a real HTTP response."""
        if from_amount > 0 and to_amount > 0:
            unit_price = to_amount / from_amount
            self._unit_price_cache[(from_addr, to_addr)] = (unit_price, time.time())

    @staticmethod
    def is_synthetic(quote: Dict[str, Any]) -> bool:
        """Return True if *quote* was built from the unit-price cache (no txData)."""
        return bool(quote.get("_synthetic"))

    def _get_cached_quote(self, from_addr: str, to_addr: str, amount: float
                          ) -> Optional[Dict[str, Any]]:
        key = self._cache_key(from_addr, to_addr, amount)
        entry = self._quote_cache.get(key)
        if entry and (time.time() - entry[1]) < _QUOTE_CACHE_TTL:
            return entry[0]
        return None

    def _store_cached_quote(self, from_addr: str, to_addr: str, amount: float,
                             quote: Dict[str, Any]) -> None:
        key = self._cache_key(from_addr, to_addr, amount)
        self._quote_cache[key] = (quote, time.time())
        # Evict entries older than 2× TTL to avoid unbounded growth
        cutoff = time.time() - _QUOTE_CACHE_TTL * 2
        self._quote_cache = {
            k: v for k, v in self._quote_cache.items() if v[1] > cutoff
        }

    async def get_swap_quote(
        self,
        from_token_amount: float,
        from_token_address: str | None = None,
        to_token_address: str | None = None,
        force_fresh: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Get a swap quote from Panora API with retry logic for rate limits.

        Returns the full API response dict, or None on error.
        Repeated calls with the same (from, to, amount) within one poll window
        are served from cache — no extra HTTP request.

        Set force_fresh=True to bypass ALL caches (used by executor which
        needs a real response with txData, not a lightweight synthetic quote).
        """
        _from = from_token_address or self.from_token_address
        _to   = to_token_address   or self.to_token_address

        if not force_fresh:
            # ── Exact cache hit (same amount)? ─────────────────────────
            cached = self._get_cached_quote(_from, _to, from_token_amount)
            if cached is not None:
                self._cache_hits += 1
                logger.debug(
                    f"Panora quote cache hit | "
                    f"from={_from[:16]}… to={_to[:16]}… amount={from_token_amount} "
                    f"(hits={self._cache_hits})"
                )
                return cached

            # ── Unit-price cache hit (different amount, same direction)? ───
            # Triggered when the arb engine verifies with actual trade qty
            # while the recent poller result (fetched with amount=1.0) is
            # still fresh.  Returns a *synthetic* quote so we skip the HTTP
            # call entirely.  The quote contains no txData — the executor
            # will call with force_fresh=True to get the real one.
            unit_price = self._get_unit_price(_from, _to)
            if unit_price is not None:
                synthetic_amount = unit_price * from_token_amount
                synthetic_quote: Dict[str, Any] = {
                    "toTokenAmount": str(synthetic_amount),
                    "_synthetic": True,
                    "_unit_price": unit_price,
                }
                self._cache_hits += 1
                logger.debug(
                    f"Panora unit-price cache hit | "
                    f"unit_price={unit_price:.8f} × {from_token_amount} "
                    f"= {synthetic_amount:.6f} (hits={self._cache_hits})"
                )
                return synthetic_quote

        self._total_requests += 1
        params: Dict[str, Any] = {
            "fromTokenAddress": _from,
            "toTokenAddress":   _to,
            "fromTokenAmount":  from_token_amount,
        }
        if self.to_wallet_address:
            params["toWalletAddress"] = self.to_wallet_address

        for attempt in range(self.max_retries):
            try:
                session = await self._get_session()
                async with session.post(self.api_url, params=params) as resp:
                    if resp.status == 200:
                        if self.rate_limited:
                            logger.info(
                                "Panora API recovered from rate limiting"
                            )
                        self.rate_limited = False
                        quote = await resp.json()
                        self._store_cached_quote(_from, _to, from_token_amount, quote)
                        # Persist unit price so subsequent verify calls with
                        # different amounts can use the synthetic-quote path.
                        to_amount = self.parse_to_token_amount(quote)
                        if to_amount is not None:
                            self._store_unit_price(
                                _from, _to, from_token_amount, to_amount
                            )
                        return quote
                    
                    elif resp.status in (429, 503):
                        self._total_rate_limits += 1
                        self._rate_limit_count += 1
                        self._last_rate_limit_time = time.time()
                        self.rate_limited = True

                        body = await resp.text()
                        retry_after = resp.headers.get("Retry-After")
                        
                        logger.warning(
                            f"🚫 PANORA RATE LIMITED | HTTP {resp.status} | "
                            f"attempt {attempt + 1}/{self.max_retries} | "
                            f"total_limits={self._total_rate_limits}/{self._total_requests} reqs | "
                            f"Retry-After={retry_after or 'N/A'} | "
                            f"body={body[:100]}"
                        )
                        
                        if attempt < self.max_retries - 1:
                            if retry_after and retry_after.isdigit():
                                wait_time = int(retry_after)
                            else:
                                wait_time = self.base_retry_delay * (2 ** attempt)
                            logger.info(
                                f"⏳ Panora backoff: waiting {wait_time:.1f}s before retry..."
                            )
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.error(
                                f"❌ Panora rate limited after {self.max_retries} attempts. "
                                f"Consider increasing PANORA_POLL_INTERVAL "
                                f"(current: {settings.panora_poll_interval}s)"
                            )
                            return None
                    
                    else:
                        body = await resp.text()
                        logger.error(f"Panora API HTTP {resp.status}: {body[:200]}")
                        return None
                        
            except asyncio.TimeoutError:
                logger.error(
                    f"Panora API timeout - attempt {attempt + 1}/{self.max_retries}"
                )
                if attempt < self.max_retries - 1:
                    wait_time = self.base_retry_delay * (2 ** attempt)
                    await asyncio.sleep(wait_time)
                    continue
                return None
                    
            except aiohttp.ClientError as e:
                logger.error(f"Panora API network error: {e}")
                return None
                
            except Exception as e:
                logger.error(f"Panora API unexpected error: {e}")
                return None

        return None

    def parse_to_token_amount(self, data: Dict[str, Any]) -> Optional[float]:
        """Extract toTokenAmount from an API response."""
        try:
            if "toTokenAmount" in data:
                return float(data["toTokenAmount"])
            quotes = data.get("quotes", [])
            if quotes and "toTokenAmount" in quotes[0]:
                return float(quotes[0]["toTokenAmount"])
        except (IndexError, ValueError, TypeError) as e:
            logger.error(f"Failed to parse toTokenAmount: {e}")
        return None

    def parse_from_token_amount(self, data: Dict[str, Any]) -> Optional[float]:
        """Extract fromTokenAmount from an API response (for ExactOut quotes)."""
        try:
            if "fromTokenAmount" in data:
                return float(data["fromTokenAmount"])
            quotes = data.get("quotes", [])
            if quotes and "fromTokenAmount" in quotes[0]:
                return float(quotes[0]["fromTokenAmount"])
        except (IndexError, ValueError, TypeError) as e:
            logger.error(f"Failed to parse fromTokenAmount: {e}")
        return None

    async def get_price(
        self, amount: float = 1.0
    ) -> Optional[Tuple[float, float]]:
        """Get DEX price as (forward_price, reverse_price).

        Forward: how much toToken you get for `amount` fromToken  (sell price / bid)
        Reverse: how much fromToken you need to get `amount` toToken (buy price / ask)

        Since Panora is a DEX, bid == ask. The two quotes give the same price
        when liquidity is deep, but may differ under slippage.

        Returns (price, price) — a single price used for both bid and ask.
        """
        # ExactIn: send `amount` AMI → get X USDC  (forward)
        forward_quote = await self.get_swap_quote(amount)
        if not forward_quote:
            return None

        forward_amount = self.parse_to_token_amount(forward_quote)
        if forward_amount is None or forward_amount <= 0:
            return None

        # Price = toTokenAmount / fromTokenAmount
        price = forward_amount / amount
        return (price, price)

    def rate_limit_stats(self) -> str:
        """Return a human-readable rate-limit stats summary."""
        total = self._total_requests
        pct = (self._total_rate_limits / total * 100) if total > 0 else 0
        total_with_hits = total + self._cache_hits
        saved_pct = (self._cache_hits / total_with_hits * 100) if total_with_hits > 0 else 0
        return (
            f"requests={total} "
            f"cache_hits={self._cache_hits} (saved {saved_pct:.0f}%) "
            f"rate_limits={self._total_rate_limits} ({pct:.1f}%) "
            f"currently_limited={self.rate_limited}"
        )

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

