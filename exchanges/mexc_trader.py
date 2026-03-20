"""
MEXC spot order executor using REST API v3.
"""
import asyncio
import hashlib
import hmac
import time
import urllib.parse
from dataclasses import dataclass
from typing import Dict, Optional

import aiohttp

from config.settings import settings
from utils.logger import get_logger

logger = get_logger()

BASE_URL = "https://api.mexc.com"


# Re-sync server time every 30 minutes to prevent clock drift
_TIME_RESYNC_INTERVAL_S = 30 * 60
# recvWindow (ms) — generous to tolerate minor drift
_RECV_WINDOW_MS = 10000


@dataclass
class OrderResult:
    """Fill details from a completed MEXC order."""
    order_id: str
    filled_qty: float = 0.0     # base coin qty actually filled
    filled_price: float = 0.0   # average fill price
    status: str = ""             # "FILLED", "PARTIALLY_FILLED", etc.


class MexcTrader:
    """Place spot market orders on MEXC via REST API v3."""

    def __init__(self) -> None:
        self.api_key = settings.mexc_api_key
        self.api_secret = settings.mexc_api_secret
        # Offset (ms) = server_time - local_time
        self._time_offset_ms: int = 0
        self._last_sync_ts: float = 0.0

    def _is_configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

    async def sync_server_time(self) -> None:
        """Fetch MEXC server time and compute clock offset."""
        try:
            async with aiohttp.ClientSession() as session:
                t0 = int(time.time() * 1000)
                async with session.get(
                    f"{BASE_URL}/api/v3/time",
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    data = await resp.json()
                t1 = int(time.time() * 1000)

            server_ts = int(data.get("serverTime", 0))
            if server_ts > 0:
                local_mid = (t0 + t1) // 2
                self._time_offset_ms = server_ts - local_mid
                self._last_sync_ts = time.time()
                logger.info(
                    f"⏱️  MEXC time-sync: offset={self._time_offset_ms:+d} ms  "
                    f"(server={server_ts}, local_mid={local_mid})"
                )
            else:
                logger.warning("MEXC time-sync: could not parse server time, offset stays 0")
        except Exception as e:
            logger.warning(f"MEXC time-sync failed ({e}), offset stays {self._time_offset_ms} ms")

    async def _ensure_time_synced(self) -> None:
        """Re-sync clock if overdue (every 30 min)."""
        if time.time() - self._last_sync_ts > _TIME_RESYNC_INTERVAL_S:
            await self.sync_server_time()

    def _now_ms(self) -> str:
        """Return current timestamp (ms) adjusted by server offset."""
        return str(int(time.time() * 1000) + self._time_offset_ms)

    def _sign(self, query_string: str) -> str:
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    async def get_balance(self, coins: list[str] | str | None = None) -> Dict[str, float]:
        """Fetch balances for specific coins (via REST)."""
        if not self._is_configured():
            logger.error("MexcTrader.get_balance: API key/secret not configured")
            return {}

        if isinstance(coins, str):
            coins = [coins]

        await self._ensure_time_synced()

        timestamp    = self._now_ms()
        params       = {"timestamp": timestamp, "recvWindow": str(_RECV_WINDOW_MS)}
        query_string = urllib.parse.urlencode(params)
        params["signature"] = self._sign(query_string)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{BASE_URL}/api/v3/account",
                    params=params,
                    headers={"X-MEXC-APIKEY": self.api_key},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = await resp.json()

            if "code" in data and data["code"] != 200:
                # Auto-resync clock on timestamp error and retry once
                if data.get("code") == 700003:
                    logger.warning("MEXC timestamp drift detected, re-syncing...")
                    await self.sync_server_time()
                    # Retry with fresh timestamp
                    timestamp = self._now_ms()
                    params = {"timestamp": timestamp, "recvWindow": str(_RECV_WINDOW_MS)}
                    query_string = urllib.parse.urlencode(params)
                    params["signature"] = self._sign(query_string)
                    async with aiohttp.ClientSession() as session:
                        async with session.get(
                            f"{BASE_URL}/api/v3/account",
                            params=params,
                            headers={"X-MEXC-APIKEY": self.api_key},
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as resp:
                            data = await resp.json()
                    if "code" in data and data["code"] != 200:
                        logger.error(
                            f"MexcTrader.get_balance error after resync: code={data.get('code')} "
                            f"msg={data.get('msg')}"
                        )
                        return {}
                else:
                    logger.error(
                        f"MexcTrader.get_balance error: code={data.get('code')} "
                        f"msg={data.get('msg')}"
                    )
                    return {}

            result: Dict[str, float] = {}
            for bal in data.get("balances", []):
                asset = bal.get("asset", "")
                free  = float(bal.get("free", 0))
                if free > 0 or (coins and asset in coins):
                    result[asset] = free

            if coins:
                result = {c: result.get(c, 0.0) for c in coins}
            return result

        except Exception as e:
            logger.error(f"MexcTrader.get_balance exception: {type(e).__name__}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return {}

    async def _poll_order_fill(
        self, symbol: str, order_id: str,
        max_polls: int = 5, poll_interval: float = 0.3,
    ) -> OrderResult:
        """Poll MEXC /api/v3/order until filled or max retries."""
        for attempt in range(max_polls):
            if attempt > 0:
                await asyncio.sleep(poll_interval)
            try:
                timestamp = self._now_ms()
                params = {
                    "symbol": symbol,
                    "orderId": order_id,
                    "timestamp": timestamp,
                    "recvWindow": str(_RECV_WINDOW_MS),
                }
                query_string = urllib.parse.urlencode(params)
                params["signature"] = self._sign(query_string)

                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{BASE_URL}/api/v3/order",
                        params=params,
                        headers={"X-MEXC-APIKEY": self.api_key},
                        timeout=aiohttp.ClientTimeout(total=5),
                    ) as resp:
                        data = await resp.json()

                status = data.get("status", "")
                exec_qty = float(data.get("executedQty", 0))
                cum_quote = float(data.get("cummulativeQuoteQty", 0))
                avg_price = cum_quote / exec_qty if exec_qty > 0 else 0

                if status in ("FILLED", "PARTIALLY_FILLED", "CANCELED",
                              "EXPIRED", "REJECTED"):
                    result = OrderResult(
                        order_id=order_id,
                        filled_qty=exec_qty,
                        filled_price=avg_price,
                        status=status,
                    )
                    logger.info(
                        f"📋 MEXC fill | {order_id} | "
                        f"status={status} qty={exec_qty} "
                        f"avg_price={avg_price:.8f}"
                    )
                    return result

                logger.debug(
                    f"MEXC order {order_id} status={status}, "
                    f"attempt {attempt + 1}/{max_polls}"
                )
            except Exception as e:
                logger.warning(f"MEXC poll_order_fill error: {e}")

        logger.warning(
            f"⚠️ MEXC fill poll timeout for {order_id} after {max_polls} attempts"
        )
        return OrderResult(order_id=order_id, status="POLL_TIMEOUT")

    async def place_market_order(
        self,
        symbol: str,
        side: str,          # "BUY" or "SELL"
        qty: float,         # base AMI qty for SELL; quote USDT qty for BUY
        is_quote_qty: bool = False,  # True → use quoteOrderQty (for BUY in USDT)
    ) -> Optional["OrderResult"]:
        """Place a spot market order. Returns OrderResult or None on failure.

        For Buy AMI:  side="BUY",  is_quote_qty=True,  qty=usdt_amount
        For Sell AMI: side="SELL", is_quote_qty=False, qty=ami_amount
        """
        if not self._is_configured():
            logger.error("MexcTrader: API key/secret not configured")
            return None

        await self._ensure_time_synced()

        timestamp = self._now_ms()
        params: dict = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "timestamp": timestamp,
            "recvWindow": str(_RECV_WINDOW_MS),
        }
        if is_quote_qty:
            params["quoteOrderQty"] = str(qty)
        else:
            params["quantity"] = str(qty)

        query_string = urllib.parse.urlencode(params)
        signature = self._sign(query_string)
        params["signature"] = signature

        headers = {"X-MEXC-APIKEY": self.api_key}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{BASE_URL}/api/v3/order",
                    headers={
                        "X-MEXC-APIKEY": self.api_key,
                        "Content-Type": "application/json",
                    },
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = await resp.json()

            if "orderId" in data:
                order_id = str(data["orderId"])
                logger.success(
                    f"✅ MEXC order placed | {side} {qty} {symbol} | orderId={order_id}"
                )
                # Poll for fill confirmation
                fill = await self._poll_order_fill(symbol, order_id)
                return fill
            else:
                logger.error(
                    f"❌ MEXC order failed | code={data.get('code')} "
                    f"msg={data.get('msg')} | symbol={symbol} side={side} qty={qty}"
                )
                return None

        except Exception as e:
            logger.error(f"❌ MEXC order exception: {e}")
            return None

    async def close(self) -> None:
        """Cleanup resources."""
        # MexcTrader currently creates a new session per request,
        # but we provide this for consistency with other traders.
        pass
