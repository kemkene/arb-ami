"""
MEXC spot order executor using REST API v3.
"""
import hashlib
import hmac
import time
import urllib.parse
from typing import Dict, Optional

import aiohttp

from config.settings import settings
from utils.logger import get_logger

logger = get_logger()

BASE_URL = "https://api.mexc.com"


class MexcTrader:
    """Place spot market orders on MEXC via REST API v3."""

    def __init__(self) -> None:
        self.api_key = settings.mexc_api_key
        self.api_secret = settings.mexc_api_secret

    def _is_configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def _sign(self, query_string: str) -> str:
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    async def get_balance(self, coins: list[str] | None = None) -> Dict[str, float]:
        """Return spot account balances as {coin: free_qty}.

        If coins is provided, only those coins are returned.
        Returns {} on error.
        """
        if not self._is_configured():
            logger.error("MexcTrader.get_balance: API key/secret not configured")
            return {}

        timestamp    = str(int(time.time() * 1000))
        params       = {"timestamp": timestamp}
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
            logger.error(f"MexcTrader.get_balance exception: {e}")
            return {}

    async def place_market_order(
        self,
        symbol: str,
        side: str,          # "BUY" or "SELL"
        qty: float,         # base AMI qty for SELL; quote USDT qty for BUY
        is_quote_qty: bool = False,  # True → use quoteOrderQty (for BUY in USDT)
    ) -> Optional[str]:
        """Place a spot market order. Returns orderId or None on failure.

        For Buy AMI:  side="BUY",  is_quote_qty=True,  qty=usdt_amount
        For Sell AMI: side="SELL", is_quote_qty=False, qty=ami_amount
        """
        if not self._is_configured():
            logger.error("MexcTrader: API key/secret not configured")
            return None

        timestamp = str(int(time.time() * 1000))
        params: dict = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "timestamp": timestamp,
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
                logger.success(
                    f"✅ MEXC order placed | {side} {qty} {symbol} | orderId={data['orderId']}"
                )
                return str(data["orderId"])
            else:
                logger.error(
                    f"❌ MEXC order failed | code={data.get('code')} "
                    f"msg={data.get('msg')} | symbol={symbol} side={side} qty={qty}"
                )
                return None

        except Exception as e:
            logger.error(f"❌ MEXC order exception: {e}")
            return None
