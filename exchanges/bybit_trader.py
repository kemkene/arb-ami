"""
Bybit spot order executor using V5 REST API.
"""
import hashlib
import hmac
import json
import time
from typing import Dict, Optional

import aiohttp

from config.settings import settings
from utils.logger import get_logger

logger = get_logger()

BASE_URL = "https://api.bybit.com"


class BybitTrader:
    """Place spot market orders on Bybit via REST API V5."""

    def __init__(self) -> None:
        self.api_key = settings.bybit_api_key
        self.api_secret = settings.bybit_api_secret

    def _is_configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def _sign(self, timestamp: str, recv_window: str, body: str) -> str:
        """HMAC-SHA256(secret, timestamp + api_key + recv_window + body)."""
        message = timestamp + self.api_key + recv_window + body
        return hmac.new(
            self.api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _auth_headers(self, timestamp: str, recv_window: str, body: str) -> dict:
        return {
            "X-BAPI-API-KEY":     self.api_key,
            "X-BAPI-SIGN":        self._sign(timestamp, recv_window, body),
            "X-BAPI-SIGN-TYPE":   "2",
            "X-BAPI-TIMESTAMP":   timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "Content-Type":       "application/json",
        }

    async def get_balance(self, coins: list[str] | None = None) -> Dict[str, float]:
        """Return spot wallet balances as {coin: free_qty}.

        If coins is provided, only those coins are returned.
        Returns {} on error.
        """
        if not self._is_configured():
            logger.error("BybitTrader.get_balance: API key/secret not configured")
            return {}

        timestamp    = str(int(time.time() * 1000))
        recv_window  = "5000"
        # GET request — body is empty string for signature
        qs = "accountType=UNIFIED"
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
                    params={"accountType": "UNIFIED"},
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
                    free = float(coin_data.get("availableToWithdraw") or coin_data.get("free") or 0)
                    result[coin] = free

            if coins:
                result = {c: result.get(c, 0.0) for c in coins}
            return result

        except Exception as e:
            logger.error(f"BybitTrader.get_balance exception: {e}")
            return {}

    async def place_market_order(
        self,
        symbol: str,
        side: str,          # "Buy" or "Sell"
        qty: float,         # base qty for Sell; quote qty for Buy
        market_unit: str = "baseCoinQty",  # "baseCoinQty" | "quoteCoinQty"
    ) -> Optional[str]:
        """Place a spot market order. Returns orderId or None on failure.

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
            "marketUnit": market_unit,
        }
        body_str = json.dumps(body_dict, separators=(",", ":"))
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"
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
                return order_id
            else:
                logger.error(
                    f"❌ Bybit order failed | retCode={ret_code} "
                    f"retMsg={data.get('retMsg')} | body={body_str}"
                )
                return None

        except Exception as e:
            logger.error(f"❌ Bybit order exception: {e}")
            return None
