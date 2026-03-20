"""
Bybit spot order executor using V5 REST API.
"""
import asyncio
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from typing import Dict, Optional

import aiohttp

from config.settings import settings
from utils.logger import get_logger

logger = get_logger()

BASE_URL = "https://api.bybit.com"
RECV_WINDOW = "10000"  # 10 s — generous to absorb residual drift
_TIME_RESYNC_INTERVAL_S = 30 * 60  # re-sync every 30 min


@dataclass
class OrderResult:
    """Fill details from a completed Bybit order."""
    order_id: str
    filled_qty: float = 0.0     # base coin qty actually filled
    filled_price: float = 0.0   # average fill price
    status: str = ""             # "Filled", "PartiallyFilled", etc.


class BybitTrader:
    """Place spot market orders on Bybit via REST API V5."""

    def __init__(self) -> None:
        self.api_key = settings.bybit_api_key
        self.api_secret = settings.bybit_api_secret
        # Offset (ms) = server_time - local_time.  Added to every request.
        self._time_offset_ms: int = 0
        self._last_sync_ts: float = 0.0

    def _is_configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

    # ── server-time sync ────────────────────────────────────────────
    async def sync_server_time(self) -> None:
        """Fetch Bybit server time and compute clock offset.

        Call once at startup (and optionally periodically) so that
        request timestamps stay within recv_window even when the
        local clock drifts.
        """
        try:
            async with aiohttp.ClientSession() as session:
                t0 = int(time.time() * 1000)
                async with session.get(
                    f"{BASE_URL}/v5/market/time",
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    data = await resp.json()
                t1 = int(time.time() * 1000)

            server_ts = int(data.get("result", {}).get("timeNano", "0")) // 1_000_000
            if server_ts <= 0:
                # fallback: some API versions return timeSecond
                server_ts = int(float(data.get("result", {}).get("timeSecond", "0")) * 1000)

            if server_ts > 0:
                local_mid = (t0 + t1) // 2
                self._time_offset_ms = server_ts - local_mid
                self._last_sync_ts = time.time()
                logger.info(
                    f"⏱️  Bybit time-sync: offset={self._time_offset_ms:+d} ms  "
                    f"(server={server_ts}, local_mid={local_mid})"
                )
            else:
                logger.warning("Bybit time-sync: could not parse server time, offset stays 0")
        except Exception as e:
            logger.warning(f"Bybit time-sync failed ({e}), offset stays {self._time_offset_ms} ms")

    async def _ensure_time_synced(self) -> None:
        """Re-sync clock if overdue (every 30 min)."""
        if time.time() - self._last_sync_ts > _TIME_RESYNC_INTERVAL_S:
            await self.sync_server_time()

    def _now_ms(self) -> str:
        """Return current timestamp (ms) adjusted by server offset."""
        return str(int(time.time() * 1000) + self._time_offset_ms)

    def _sign(self, timestamp: str, recv_window: str, body: str) -> str:
        """HMAC-SHA256(secret, timestamp + api_key + recv_window + body)."""
        message = timestamp + self.api_key + recv_window + body
        return hmac.new(
            self.api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _auth_headers(self, body: str) -> dict:
        timestamp = self._now_ms()
        return {
            "X-BAPI-API-KEY":     self.api_key,
            "X-BAPI-SIGN":        self._sign(timestamp, RECV_WINDOW, body),
            "X-BAPI-SIGN-TYPE":   "2",
            "X-BAPI-TIMESTAMP":   timestamp,
            "X-BAPI-RECV-WINDOW": RECV_WINDOW,
            "Content-Type":       "application/json",
        }

    async def get_balance(
        self, coins: list[str] | str | None = None, account_type: str = "UNIFIED"
    ) -> Dict[str, float]:
        """Fetch available balance for specific coins.
        account_type can be "UNIFIED" (trading) or "FUND" (funding).
        Returns {} on error.
        """
        if isinstance(coins, str):
            coins = [coins]
        if not self._is_configured():
            logger.error("BybitTrader.get_balance: API key/secret not configured")
            return {}

        await self._ensure_time_synced()

        timestamp    = self._now_ms()
        recv_window  = RECV_WINDOW
        # GET request — body is empty string for signature
        qs = f"accountType={account_type}"
        sign_body = timestamp + self.api_key + recv_window + qs
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            sign_body.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "X-BAPI-API-KEY":     self.api_key,
            "X-BAPI-SIGN":        signature,
            "X-BAPI-SIGN-TYPE":   "2",
            "X-BAPI-TIMESTAMP":   timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{BASE_URL}/v5/account/wallet-balance",
                    params={"accountType": account_type},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = await resp.json()

            if data.get("retCode") != 0:
                logger.error(
                    f"BybitTrader.get_balance error: retCode={data.get('retCode')} "
                    f"retMsg={data.get('retMsg')}"
                )
                return {}

            result: Dict[str, float] = {}
            for account in data.get("result", {}).get("list", []):
                for coin_data in account.get("coin", []):
                    coin = coin_data.get("coin", "")
                    # For Unified accounts, availableToWithdraw can be empty string. 
                    # Prefer availableToWithdraw if non-empty, else walletBalance, else equity.
                    val = coin_data.get("availableToWithdraw")
                    if not val:
                        val = coin_data.get("walletBalance") or coin_data.get("equity") or coin_data.get("free") or "0"
                    
                    try:
                        free = float(val)
                    except (ValueError, TypeError):
                        free = 0.0
                    result[coin] = free

            if coins:
                result = {c: result.get(c, 0.0) for c in coins}
            return result

        except Exception as e:
            logger.error(f"BybitTrader.get_balance exception: {e}")
            return {}

    async def check_trading_enabled(self) -> bool:
        """Probe whether trading is allowed on this Bybit account.

        Issues a wallet-balance request.  If the API returns retCode != 0
        (e.g. 10024 — regulatory restriction) we treat the account as
        blocked and return False.  A network error also returns False.
        """
        if not self._is_configured():
            return False

        await self._ensure_time_synced()

        timestamp = self._now_ms()
        recv_window = RECV_WINDOW
        qs = "accountType=UNIFIED"
        sign_body = timestamp + self.api_key + recv_window + qs
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            sign_body.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{BASE_URL}/v5/account/wallet-balance",
                    params={"accountType": "UNIFIED"},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = await resp.json()

            ret_code = data.get("retCode")
            if ret_code == 0:
                return True
            else:
                logger.error(
                    f"🚫 Bybit account blocked: retCode={ret_code} "
                    f"retMsg={data.get('retMsg')}"
                )
                return False
        except Exception as e:
            logger.error(f"🚫 Bybit health-check failed: {e}")
            return False

    async def _poll_order_fill(
        self, symbol: str, order_id: str,
        max_polls: int = 5, poll_interval: float = 0.3,
    ) -> OrderResult:
        """Poll Bybit /v5/order/realtime until filled or max retries."""
        for attempt in range(max_polls):
            if attempt > 0:
                await asyncio.sleep(poll_interval)
            try:
                timestamp = self._now_ms()
                recv_window = RECV_WINDOW
                qs = f"category=spot&orderId={order_id}"
                sign_body = timestamp + self.api_key + recv_window + qs
                signature = hmac.new(
                    self.api_secret.encode("utf-8"),
                    sign_body.encode("utf-8"),
                    hashlib.sha256,
                ).hexdigest()
                headers = {
                    "X-BAPI-API-KEY": self.api_key,
                    "X-BAPI-SIGN": signature,
                    "X-BAPI-SIGN-TYPE": "2",
                    "X-BAPI-TIMESTAMP": timestamp,
                    "X-BAPI-RECV-WINDOW": recv_window,
                }
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{BASE_URL}/v5/order/realtime",
                        params={"category": "spot", "orderId": order_id},
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=5),
                    ) as resp:
                        data = await resp.json()

                if data.get("retCode") == 0:
                    orders = data.get("result", {}).get("list", [])
                    if orders:
                        o = orders[0]
                        status = o.get("orderStatus", "")
                        filled_qty = float(o.get("cumExecQty", 0))
                        avg_price = float(o.get("avgPrice", 0))

                        if status in ("Filled", "PartiallyFilled", "Cancelled",
                                      "Rejected", "Deactivated"):
                            result = OrderResult(
                                order_id=order_id,
                                filled_qty=filled_qty,
                                filled_price=avg_price,
                                status=status,
                            )
                            logger.info(
                                f"📋 Bybit fill | {order_id} | "
                                f"status={status} qty={filled_qty} "
                                f"avg_price={avg_price}"
                            )
                            return result

                        # Still open — continue polling
                        logger.debug(
                            f"Bybit order {order_id} status={status}, "
                            f"attempt {attempt + 1}/{max_polls}"
                        )
            except Exception as e:
                logger.warning(f"Bybit poll_order_fill error: {e}")

        logger.warning(
            f"⚠️ Bybit fill poll timeout for {order_id} after {max_polls} attempts"
        )
        return OrderResult(order_id=order_id, status="POLL_TIMEOUT")

    async def place_market_order(
        self,
        symbol: str,
        side: str,          # "Buy" or "Sell"
        qty: float,         # base qty for Sell; quote qty for Buy
        market_unit: Optional[str] = None,  # "baseCoinQty" | "quoteCoinQty"
    ) -> Optional["OrderResult"]:
        """Place a spot market order. Returns OrderResult or None on failure.

        For Buy AMI:  side="Buy",  market_unit="quoteCoinQty", qty=usdt_amount
        For Sell AMI: side="Sell", market_unit="baseCoinQty",  qty=ami_amount
        """
        if not self._is_configured():
            logger.error("BybitTrader: API key/secret not configured")
            return None

        body_dict = {
            "category": "spot",
            "symbol": symbol,
            "side": side,
            "orderType": "Market",
            "qty": str(qty),
        }
        if market_unit:
            body_dict["marketUnit"] = market_unit
        body_str = json.dumps(body_dict, separators=(",", ":"))
        timestamp = self._now_ms()
        recv_window = RECV_WINDOW
        signature = self._sign(timestamp, recv_window, body_str)

        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "Content-Type": "application/json",
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{BASE_URL}/v5/order/create",
                    headers=headers,
                    data=body_str,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    data = await resp.json()

            ret_code = data.get("retCode")
            if ret_code == 0:
                order_id = data.get("result", {}).get("orderId", "?")
                logger.success(
                    f"✅ Bybit order placed | {side} {qty} {symbol} | orderId={order_id}"
                )
                # Poll for fill confirmation
                fill = await self._poll_order_fill(symbol, order_id)
                return fill
            else:
                logger.error(
                    f"❌ Bybit order failed | retCode={ret_code} "
                    f"retMsg={data.get('retMsg')} | body={body_str}"
                )
                return None

        except Exception as e:
            logger.error(f"❌ Bybit order exception: {e}")
            return None

    async def close(self) -> None:
        """Cleanup resources."""
        # BybitTrader currently creates a new session per request,
        # but we provide this for consistency with other traders.
        pass
