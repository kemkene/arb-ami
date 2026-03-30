import aiohttp
import asyncio
import json
import time
import hmac
import hashlib
import urllib.parse
import uuid
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

from config.settings import settings
from utils.logger import get_logger

logger = get_logger()

BASE_URL = "https://api.bybit.com"
RECV_WINDOW = 10000  # Increased for better resilience during withdrawals

@dataclass
class OrderResult:
    order_id: str
    status: str
    filled_price: float = 0.0
    filled_qty: float = 0.0

class BybitTrader:
    def __init__(self):
        self.api_key = settings.bybit_api_key
        self.api_secret = settings.bybit_api_secret
        self._time_offset_ms: int = 0
        self._last_sync_ts: float = 0.0
        self.instrument_info: Dict[str, dict] = {}

    def _is_configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

    async def get_server_time(self) -> int:
        """Fetch current Bybit server time directly."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{BASE_URL}/v5/market/time",
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    data = await resp.json()
                return int(data.get("time", 0))
        except Exception as e:
            logger.warning(f"Failed to fetch Bybit server time: {e}")
            return int(time.time() * 1000)

    async def sync_server_time(self) -> None:
        """Fetch Bybit server time and compute clock offset."""
        try:
            async with aiohttp.ClientSession() as session:
                t0 = int(time.time() * 1000)
                async with session.get(
                    f"{BASE_URL}/v5/market/time",
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    data = await resp.json()
                t1 = int(time.time() * 1000)

            server_ts = int(data.get("time", 0))
            if server_ts > 0:
                local_mid = (t0 + t1) // 2
                self._time_offset_ms = server_ts - local_mid
                self._last_sync_ts = time.time()
                logger.info(
                    f"⏱️  Bybit time-sync: offset={self._time_offset_ms:+d} ms "
                    f"(server={server_ts}, local_mid={local_mid})"
                )
            else:
                logger.warning("Bybit time-sync: could not parse server time, offset stays 0")
        except Exception as e:
            logger.warning(f"Bybit time-sync failed ({e}), offset stays {self._time_offset_ms} ms")

    async def get_balance(self, coin: str | list[str] | None = None) -> Any:
        """Fetch balance from Bybit (UNIFIED account or FUND account)."""
        if not self._is_configured():
            return 0.0 if isinstance(coin, str) else {}
        
        await self._ensure_time_synced()
        
        # Standardize coin to a list for internal processing if provided
        search_coins = []
        if isinstance(coin, str):
            search_coins = [coin]
        elif isinstance(coin, list):
            search_coins = coin

        all_balances = {}

        # 1. Try UNIFIED ACCOUNT (Trading Account)
        try:
            # Bybit V5 UNIFIED balance endpoint doesn't support multiple coins in one 'coin' param string easily
            # If search_coins has only 1, use it. Otherwise, fetch all and filter.
            unified_params = "accountType=UNIFIED"
            if len(search_coins) == 1:
                unified_params += f"&coin={search_coins[0]}"
            
            ts = self._now_ms()
            sig = self._sign(ts, str(RECV_WINDOW), unified_params)
            h = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-SIGN": sig,
                "X-BAPI-SIGN-TYPE": "2",
                "X-BAPI-TIMESTAMP": ts,
                "X-BAPI-RECV-WINDOW": str(RECV_WINDOW),
            }

            async with aiohttp.ClientSession() as session:
                url_params = {"accountType": "UNIFIED"}
                if len(search_coins) == 1:
                    url_params["coin"] = search_coins[0]
                
                async with session.get(
                    f"{BASE_URL}/v5/account/wallet-balance",
                    headers=h,
                    params=url_params,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    data = await resp.json()

            if data.get("retCode") == 0:
                list_data = data.get("result", {}).get("list", [])
                if list_data:
                    coins_data = list_data[0].get("coin", [])
                    for c_item in coins_data:
                        c_name = c_item.get("coin")
                        val = float(c_item.get("walletBalance", 0.0))
                        if c_name:
                            all_balances[c_name] = val
        except Exception as e:
            logger.error(f"Bybit UNIFIED balance error: {e}")

        # 2. Try FUND ACCOUNT (Funding Account)
        try:
            # Logic: If specific coins requested, we may need multiple calls or fetch all and filter
            # For simplicity, we fetch all if search_coins is not exactly 1
            fund_endpoint = "/v5/asset/transfer/query-account-coin-balance" if len(search_coins) == 1 else "/v5/asset/transfer/query-account-coins-balance"
            fund_params = "accountType=FUND"
            if len(search_coins) == 1:
                fund_params += f"&coin={search_coins[0]}"
            
            ts = self._now_ms()
            sig = self._sign(ts, str(RECV_WINDOW), fund_params)
            h = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-SIGN": sig,
                "X-BAPI-SIGN-TYPE": "2",
                "X-BAPI-TIMESTAMP": ts,
                "X-BAPI-RECV-WINDOW": str(RECV_WINDOW),
            }

            async with aiohttp.ClientSession() as session:
                url_params = {"accountType": "FUND"}
                if len(search_coins) == 1:
                    url_params["coin"] = search_coins[0]
                
                async with session.get(
                    f"{BASE_URL}{fund_endpoint}",
                    headers=h,
                    params=url_params,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    fund_data = await resp.json()

            if fund_data.get("retCode") == 0:
                result = fund_data.get("result", {})
                if len(search_coins) == 1:
                    balance_item = result.get("balance", {})
                    val = float(balance_item.get("walletBalance", 0.0))
                    c_name = search_coins[0]
                    all_balances[c_name] = all_balances.get(c_name, 0.0) + val
                else:
                    balance_items = result.get("balance", [])
                    if isinstance(balance_items, list):
                        for item in balance_items:
                            c = item.get("coin")
                            val = float(item.get("walletBalance", 0.0))
                            if c: all_balances[c] = all_balances.get(c, 0.0) + val
        except Exception as e:
             logger.error(f"Bybit FUND balance error: {e}")

        # Return results
        if isinstance(coin, str):
            return all_balances.get(coin, 0.0)
        elif isinstance(coin, list):
            # Return a dict for only the requested coins
            return {c: all_balances.get(c, 0.0) for c in coin}
        return all_balances

    async def get_funding_balances(self) -> Dict[str, float]:
        """Fetch all balances from Bybit FUND account."""
        if not self._is_configured():
            return {}
        
        await self._ensure_time_synced()
        all_balances = {}
        
        try:
            fund_endpoint = "/v5/asset/transfer/query-account-coins-balance"
            fund_params = "accountType=FUND"
            ts = self._now_ms()
            sig = self._sign(ts, str(RECV_WINDOW), fund_params)
            h = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-SIGN": sig,
                "X-BAPI-SIGN-TYPE": "2",
                "X-BAPI-TIMESTAMP": ts,
                "X-BAPI-RECV-WINDOW": str(RECV_WINDOW),
            }

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{BASE_URL}{fund_endpoint}",
                    headers=h,
                    params={"accountType": "FUND"},
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    fund_data = await resp.json()

            if fund_data.get("retCode") == 0:
                result = fund_data.get("result", {})
                balance_items = result.get("balance", [])
                if isinstance(balance_items, list):
                    for item in balance_items:
                        c = item.get("coin")
                        val = float(item.get("walletBalance", 0.0))
                        if c: all_balances[c] = val
        except Exception as e:
             logger.error(f"Bybit get_funding_balances error: {e}")
             
        return all_balances

    async def _ensure_time_synced(self) -> None:
        """Re-sync clock if overdue (every 30 min)."""
        if time.time() - self._last_sync_ts > 1800: # 30 min
            await self.sync_server_time()

    def _now_ms(self, sensitive: bool = False) -> str:
        # Using a slightly laggy timestamp (-5s) can help bypass "future window" errors
        # but sensitive endpoints (Withdraw) need it closer to server time.
        offset = 0 if sensitive else -5000
        return str(int(time.time() * 1000) + self._time_offset_ms + offset)

    def _sign(self, timestamp: Any, recv_window: Any, payload: str) -> str:
        """Create Bybit V5 signature: timestamp + api_key + recv_window + payload"""
        param_str = f"{timestamp}{self.api_key}{recv_window}{payload}"
        # DEBUG: Hidden API Key for security
        logger.debug(f"BYBIT SIGN STRING: {timestamp}API_KEY_HIDDEN{recv_window}{payload}")
        hash = hmac.new(
            bytes(self.api_secret, "utf-8"),
            bytes(param_str, "utf-8"),
            hashlib.sha256
        )
        return hash.hexdigest()

    async def _poll_order_fill(self, symbol: str, order_id: str, max_polls: int = 15) -> Optional[OrderResult]:
        for i in range(max_polls):
            await asyncio.sleep(0.5)
            params = f"category=spot&symbol={symbol}&orderId={order_id}"
            timestamp = self._now_ms()
            recv_window = str(RECV_WINDOW)
            signature = self._sign(timestamp, recv_window, params)

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
                        f"{BASE_URL}/v5/order/history",
                        headers=headers,
                        params={"category": "spot", "symbol": symbol, "orderId": order_id},
                        timeout=aiohttp.ClientTimeout(total=5)
                    ) as resp:
                        data = await resp.json()

                if data.get("retCode") == 0:
                    rows = data.get("result", {}).get("list", [])
                    if rows:
                        order = rows[0]
                        status = order.get("orderStatus")
                        if status == "Filled":
                            filled_price = float(order.get("avgPrice", 0))
                            filled_qty = float(order.get("cumExecQty", 0))
                            logger.info(f"✅ Bybit order FILLED: {order_id} | price={filled_price} | qty={filled_qty}")
                            return OrderResult(order_id=order_id, status=status, filled_price=filled_price, filled_qty=filled_qty)
                        elif status in ("Cancelled", "Rejected"):
                            logger.warning(f"❌ Bybit order {status}: {order_id} | reason={order.get('rejectReason')}")
                            return OrderResult(order_id=order_id, status=status)
                        else:
                            logger.debug(f"⏳ Bybit order {status}: {order_id} (poll {i+1}/{max_polls})")
            except Exception as e:
                logger.error(f"Bybit fill poll error: {e}")

        logger.warning(f"⚠️ Bybit fill poll timeout for {order_id}")
        return OrderResult(order_id=order_id, status="POLL_TIMEOUT")

    async def sync_instrument_info(self, symbol: str) -> None:
        """Fetch lotSizeFilter (precision) for a symbol from Bybit V5."""
        try:
            params = {"category": "spot", "symbol": symbol}
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{BASE_URL}/v5/market/instruments-info",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as resp:
                    data = await resp.json()
            
            if data.get("retCode") == 0:
                list_data = data.get("result", {}).get("list", [])
                if list_data:
                    self.instrument_info[symbol] = list_data[0]
                    logger.info(f"📊 [Bybit] Synced instrument info for {symbol}")
            else:
                logger.warning(f"⚠️ Bybit: Failed to sync info for {symbol}: {data.get('retMsg')}")
        except Exception as e:
            logger.error(f"Bybit sync_instrument_info error: {e}")

    def round_quantity(self, symbol: str, qty: float) -> float:
        """Round quantity to the required basePrecision/qtyStep of the symbol."""
        info = self.instrument_info.get(symbol)
        if not info:
            # Fallback to 2 decimal places if info not yet synced
            return float(int(qty * 100) / 100.0)
            
        lot_size = info.get("lotSizeFilter", {})
        base_precision = lot_size.get("basePrecision") # e.g., "0.01" or "1"
        
        if not base_precision:
            return float(int(qty * 100) / 100.0)
            
        step = float(base_precision)
        rounded = (qty // step) * step
        
        # Format to avoid precision noise (e.g., 30.870000000000001)
        # Use decimal-based precision for high accuracy on floating point
        import decimal
        d_step = decimal.Decimal(str(base_precision))
        d_qty = decimal.Decimal(str(qty))
        
        # Formula: Math.floor(qty / step) * step
        rounded_d = (d_qty // d_step) * d_step
        return float(rounded_d)

    async def place_market_order(
        self, 
        symbol: str, 
        side: str, 
        qty: float, 
        is_quote_qty: bool = False,
        market_unit: Optional[str] = None
    ) -> Optional[OrderResult]:
        """
        Place a spot market order.
        For Buy AMI with USDT: side="Buy", is_quote_qty=True, qty=usdt_amount
        For Sell AMI: side="Sell", is_quote_qty=False, qty=ami_amount
        """
        if not self._is_configured():
            return None

        await self._ensure_time_synced()
        
        # 0. Check Max Order Quantity from instrument_info
        info = self.instrument_info.get(symbol, {})
        lot_size = info.get("lotSizeFilter", {})
        max_qty = float(lot_size.get("maxOrderQty", 0))
        min_qty = float(lot_size.get("minOrderQty", 0))

        # Determine market_unit if not explicitly provided
        if market_unit is None:
            if is_quote_qty:
                market_unit = "quoteCoin"

        # Round quantity to exchange precision
        raw_qty = qty
        if market_unit == "quoteCoin":
            # For USDT (quote), 2 decimal places is usually safe
            qty = float(int(qty * 100) / 100.0)
            
            # If we have max_qty, we should check if our quote amount might exceed it
            # We use a conservative estimate (e.g. current price or slightly lower)
            if max_qty > 0:
                # We don't have the exact price here, but if we're buying AMI, we can estimate
                # This is a bit tricky without price, but we can use a safe cap for AMI
                # For AMI/USDT, max_qty is 410,000. Price ~0.007. Max USDT ~ 2870.
                # If qty (USDT) > something suspicious, we can log it.
                # A better way is to pass estimated price to this function.
                pass
        else:
            qty = self.round_quantity(symbol, qty)
            if max_qty > 0 and qty > max_qty:
                logger.warning(f"⚠️ [Bybit] Qty {qty} exceeds max {max_qty} for {symbol}. Capping to max.")
                qty = max_qty
        
        # Final min check
        if not market_unit == "quoteCoin" and min_qty > 0 and qty < min_qty:
             logger.error(f"❌ Bybit order aborted: quantity {qty} < min {min_qty} for {symbol}")
             return None

        # Format qty to string strictly
        # Bybit Spot V5 Market Buy (baseCoinQty) or Market Sell needs string qty
        info = self.instrument_info.get(symbol, {})
        lot_size = info.get("lotSizeFilter", {})
        base_p = str(lot_size.get("basePrecision", "0.000001"))
        
        # Calculate decimal places from basePrecision (e.g., "0.001" -> 3)
        try:
            if "." in base_p:
                prec = len(base_p.split(".")[1])
            else:
                prec = 0
        except:
            prec = 6
            
        qty_str = format(qty, f".{prec}f").rstrip('0').rstrip('.')
        if not qty_str or qty_str == "0":
             qty_str = format(qty, f".{prec}f") # fallback if rstrip killed it

        logger.debug(f"⚖️ [Bybit] Rounding qty for {symbol}: {raw_qty} -> {qty} (str: {qty_str})")
        
        if qty <= 0:
            logger.error(f"❌ Bybit order aborted: quantity {qty} (raw: {raw_qty}) is too small after rounding.")
            return None

        # MINIMALIST PAYLOAD for Spot Market Order
        body_dict = {
            "category": "spot",
            "symbol": symbol,
            "side": side, # "Buy" or "Sell"
            "orderType": "Market",
            "qty": qty_str,
        }
        
        # marketUnit is ONLY for Spot Market Buy
        if side.upper() == "BUY" and market_unit:
            body_dict["marketUnit"] = market_unit

        body_str = json.dumps(body_dict, separators=(",", ":"))
        timestamp = self._now_ms()
        recv_window = str(RECV_WINDOW)
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
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    data = await resp.json()

            if data.get("retCode") == 0:
                order_id = data.get("result", {}).get("orderId", "?")
                logger.success(f"✅ Bybit order placed | {side} {qty_str} {symbol}")
                return await self._poll_order_fill(symbol, order_id)
            else:
                logger.error(f"❌ Bybit order failed | {data.get('retMsg')} | Payload: {body_str} | Data: {data}")
                return None
        except Exception as e:
            logger.error(f"Bybit order exception: {e}")
            return None

    async def internal_transfer(self, coin: str, amount: float, from_account: str, to_account: str) -> bool:
        """Transfer assets between accounts (e.g., UNIFIED to FUND)."""
        if not self._is_configured():
            return False
            
        await self._ensure_time_synced()
        
        try:
            transfer_id = str(uuid.uuid4())
            body_dict = {
                "transferId": transfer_id,
                "coin": coin,
                "amount": str(amount),
                "fromAccountType": from_account,
                "toAccountType": to_account,
                "timestamp": int(self._now_ms(sensitive=True)),
            }
            
            b_str = json.dumps(body_dict, separators=(",", ":"))
            ts_ms = str(body_dict["timestamp"])
            recv_w = str(RECV_WINDOW)
            sig = self._sign(ts_ms, recv_w, b_str)
            
            h = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-SIGN": sig,
                "X-BAPI-SIGN-TYPE": "2",
                "X-BAPI-TIMESTAMP": ts_ms,
                "X-BAPI-RECV-WINDOW": recv_w,
                "Content-Type": "application/json",
            }
            
            logger.info(f"🔄 Bybit internal transfer: {amount} {coin} {from_account} -> {to_account}")
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{BASE_URL}/v5/asset/transfer/inter-transfer",
                    headers=h,
                    data=b_str,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    data = await resp.json()
                    
            if data.get("retCode") == 0:
                logger.success(f"✅ Bybit internal transfer successful: {transfer_id}")
                return True
            else:
                logger.error(f"❌ Bybit internal transfer failed: {data.get('retMsg')} | Data: {data}")
                return False
        except Exception as e:
            logger.error(f"Bybit internal transfer exception: {e}")
            return False

    async def withdraw(self, coin: str, amount: float, address: str, chain: str, tag: Optional[str] = None) -> Optional[str]:
        if not self._is_configured():
            return None

        await self._ensure_time_synced()
        
        async def _perform_withdraw(ts_ms: str):
            # Automatic chain mapping for common errors
            chain_map = {"APT": "APTOS", "USDT": "ERC20"}
            final_chain = chain_map.get(chain.upper(), chain.upper())
            
            # MINIMALIST BODY: Remove 'tag' entirely if empty
            body_dict = {
                "coin": coin.upper(),
                "chain": final_chain,
                "address": address,
                "amount": str(amount),
                "forceChain": 1, # Force on-chain for external wallets
                "accountType": "FUND",
                "feeType": 1,    # Auto-deduct fee from amount
                "requestId": str(uuid.uuid4()).replace("-", ""), # Idempotency key
                "timestamp": int(ts_ms), # MANDATORY for Bybit V5 withdrawal body
            }
            if tag and tag.strip():
                body_dict["tag"] = tag

            b_str = json.dumps(body_dict, separators=(",", ":"))
            recv_w = str(RECV_WINDOW)
            sig = self._sign(ts_ms, recv_w, b_str)

            h = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-SIGN": sig,
                "X-BAPI-SIGN-TYPE": "2",
                "X-BAPI-TIMESTAMP": ts_ms,
                "X-BAPI-RECV-WINDOW": recv_w,
                "Content-Type": "application/json",
            }
            
            logger.debug(f"🔍 [Bybit Internal] Request Details:")
            logger.debug(f"   - Timestamp: {ts_ms} (Local Unix: {int(time.time()*1000)})")
            logger.debug(f"   - RecvWindow: {recv_w}")
            logger.debug(f"   - Body: {b_str}")
            logger.debug(f"   - Signature: {sig}")

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{BASE_URL}/v5/asset/withdraw/create",
                    headers=h,
                    data=b_str,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != 200:
                        return None, await resp.text(), resp.status
                    return await resp.json(), None, 200

        # Perform withdrawal sequence
        try:
            # 0. Check FUND balance. If insufficient, try to transfer from UNIFIED.
            try:
                # Query FUND balance specifically
                ts_bal = self._now_ms()
                sig_bal = self._sign(ts_bal, str(RECV_WINDOW), f"accountType=FUND&coin={coin}")
                h_bal = {
                    "X-BAPI-API-KEY": self.api_key,
                    "X-BAPI-SIGN": sig_bal,
                    "X-BAPI-SIGN-TYPE": "2",
                    "X-BAPI-TIMESTAMP": ts_bal,
                    "X-BAPI-RECV-WINDOW": str(RECV_WINDOW),
                }
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{BASE_URL}/v5/asset/transfer/query-account-coin-balance",
                        headers=h_bal,
                        params={"accountType": "FUND", "coin": coin},
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as resp:
                        bal_data = await resp.json()
                
                fund_bal = 0.0
                if bal_data.get("retCode") == 0:
                    fund_bal = float(bal_data.get("result", {}).get("balance", {}).get("walletBalance", 0.0))
                
                if fund_bal < amount:
                    logger.info(f"⚠️ [Bybit] Insufficient FUND balance ({fund_bal} < {amount}). Checking UNIFIED...")
                    # Try to transfer enough from UNIFIED to FUND
                    success = await self.internal_transfer(coin, amount, "UNIFIED", "FUND")
                    if success:
                        logger.info("⏳ Waiting 3s for transfer to settle...")
                        await asyncio.sleep(3)
                        # Re-verify FUND balance
                        try:
                            ts_v = self._now_ms()
                            sig_v = self._sign(ts_v, str(RECV_WINDOW), f"accountType=FUND&coin={coin}")
                            h_v = {
                                "X-BAPI-API-KEY": self.api_key,
                                "X-BAPI-SIGN": sig_v,
                                "X-BAPI-SIGN-TYPE": "2",
                                "X-BAPI-TIMESTAMP": ts_v,
                                "X-BAPI-RECV-WINDOW": str(RECV_WINDOW),
                            }
                            async with aiohttp.ClientSession() as session:
                                async with session.get(
                                    f"{BASE_URL}/v5/asset/transfer/query-account-coin-balance",
                                    headers=h_v,
                                    params={"accountType": "FUND", "coin": coin},
                                    timeout=aiohttp.ClientTimeout(total=5)
                                ) as resp:
                                    v_data = await resp.json()
                                    if v_data.get("retCode") == 0:
                                        new_fund_bal = float(v_data.get("result", {}).get("balance", {}).get("walletBalance", 0.0))
                                        logger.info(f"📊 [Bybit] Verified FUND balance: {new_fund_bal}")
                        except Exception as ve:
                            logger.warning(f"Bybit post-transfer balance verification failed: {ve}")
                    else:
                        # If no transfer was needed, still verify current balance for clarity
                        logger.info(f"📊 [Bybit] FUND balance already sufficient: {fund_bal}")
            except Exception as e:
                logger.warning(f"Bybit pre-withdraw balance check/transfer failed: {e}")

            # Try 1: With sensitive timing (0 offset)
            timestamp = self._now_ms(sensitive=True)
            data, err_text, status = await _perform_withdraw(timestamp)

            if data and data.get("retCode") == 131002:
                server_time_from_err = int(data.get("time", 0))
                if server_time_from_err > 0:
                    logger.warning(f"⚠️ Bybit 131002 detected. Syncing and retrying...")
                    # 10.5s delay for Bybit withdrawal rate limit (131001)
                    logger.info(f"⏳ Withdrawal rate limit: Waiting 10.5s before retry...")
                    await asyncio.sleep(10.5)
                    
                    # Retry with exactly synced server time
                    retry_ts = str(int(time.time() * 1000) + self._time_offset_ms)
                    logger.info(f"🔄 Retrying with precise timestamp: {retry_ts}")
                    data, err_text, status = await _perform_withdraw(retry_ts)

            if data and data.get("retCode") == 0:
                result = data.get("result", {})
                wid = result.get("id") or result.get("withdrawId")
                logger.success(f"✅ Bybit withdrawal requested: {coin} {amount} | ID={wid}")
                return wid or "SUCCESS_NO_ID"
            elif data and data.get("retCode") == 131001:
                # 131001 can indicate a 'fake failure' where the withdrawal is actually submitted
                # or is under manual review. Since we confirmed it can result in a balance decrease:
                logger.warning(f"⚠️ Bybit 131001 (openapi svc error) but withdrawal MIGHT be successful. Check history manually.")
                # We return a placeholder ID to indicate it is not a hard failure
                return f"PROBABLE_SUCCESS_131001"
            elif status != 200:
                logger.error(f"❌ Bybit withdrawal HTTP error {status}: {err_text}")
                return None
            else:
                msg = data.get("retMsg") if data else "No response"
                logger.error(f"❌ Bybit withdrawal failed | retCode={data.get('retCode') if data else 'N/A'} | msg={msg} | full_data={data}")
                return None
        except Exception as e:
            logger.error(f"Bybit withdrawal exception: {e}")
            return None

    async def get_deposit_address(self, coin: str, chain: str) -> Optional[str]:
        if not self._is_configured():
            return None

        await self._ensure_time_synced()
        
        # We include the chain in the query if provided to help trigger generation
        params = {"coin": coin}
        if chain:
            params["chain"] = chain.upper()

        params_str = urllib.parse.urlencode(params)
        timestamp = self._now_ms()
        recv_window = str(RECV_WINDOW)
        signature = self._sign(timestamp, recv_window, params_str)

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
                    f"{BASE_URL}/v5/asset/deposit/query-address",
                    headers=headers,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != 200:
                        err_text = await resp.text()
                        logger.error(f"❌ Bybit get_deposit_address HTTP error {resp.status}: {err_text}")
                        return None
                    data = await resp.json()

            if data and data.get("retCode") == 0:
                rows = data.get("result", {}).get("rows", [])
                logger.debug(f"🔍 Bybit deposit rows for {coin}: {rows}")
                for row in rows:
                    if row.get("chain", "").upper() == chain.upper() and row.get("coin", "").upper() == coin.upper():
                        addr = row.get("address")
                        if addr:
                            logger.info(f"📡 [Bybit] Deposit address for {coin} on {chain}: {addr}")
                            return addr
                
                # Attempt 2: Nested structure (e.g., if querying all coins and then filtering)
                if rows:
                    # Find current coin
                    for asset in rows:
                        if asset.get("coin", "").upper() == coin.upper():
                            # Find chain
                            chains_list = asset.get("chains", [])
                            for c in chains_list:
                                if c.get("chain", "").upper() == chain.upper():
                                    addr = c.get("address")
                                    if addr:
                                        logger.info(f"📡 [Bybit] Deposit address for {coin} on {chain}: {addr}")
                                        return addr
                            # If chain not found for this coin in the nested structure
                            logger.warning(f"⚠️ Bybit: Chain '{chain}' not found for {coin} in nested structure. Available: {[x.get('chain') for x in chains_list]}")
                
            # FALLBACK: Use manual address from settings if API result is empty or no match found
            if settings.bybit_deposit_address:
                logger.info(f"ℹ️ Bybit: Using manual deposit address from settings: {settings.bybit_deposit_address}")
                return settings.bybit_deposit_address

            msg = data.get("retMsg") if data else "No response"
            logger.error(f"❌ Bybit get_deposit_address failed or address not found for {coin} on {chain} | retCode={data.get('retCode') if data else 'N/A'} | msg={msg} | Data: {data}")
            logger.warning(f"⚠️ Bybit: Deposit address for {coin} on {chain} not found and no manual fallback set.")
            return None
        except Exception as e:
            logger.error(f"Bybit get_deposit_address error: {e}")
            return None

    async def get_api_key_info(self) -> Any:
        """Query Bybit for API key permissions and IP restrictions."""
        if not self.api_key or not self.api_secret: return None
        await self._ensure_time_synced()
        ts = self._now_ms() # Standard sync is fine for info query
        params = {"timestamp": ts, "recvWindow": str(RECV_WINDOW)}
        query_str = urllib.parse.urlencode(params)
        sig = self._sign(str(ts), str(RECV_WINDOW), "")
        
        try:
            async with aiohttp.ClientSession() as session:
                h = {
                    "X-BAPI-API-KEY": self.api_key,
                    "X-BAPI-SIGN": sig,
                    "X-BAPI-TIMESTAMP": str(ts),
                    "X-BAPI-RECV-WINDOW": str(RECV_WINDOW)
                }
                async with session.get(
                    f"{BASE_URL}/v5/user/query-api",
                    headers=h,
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    return await resp.json()
        except Exception as e:
            logger.error(f"Bybit get_api_key_info exception: {e}")
            return None

    async def close(self) -> None:
        pass
