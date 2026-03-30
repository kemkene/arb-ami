"""
BalanceManager — track and validate exchange balances before trade execution.

Features:
  - Periodic async refresh from Bybit, MEXC, and Aptos (DEX)
  - In-memory cache with TTL (avoids hammering APIs on every check)
  - Pre-trade validation: ensure sufficient balance for each leg
  - Reserve buffer: keep a configurable safety margin on each asset
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, TYPE_CHECKING, List, Tuple

import aiohttp
from aptos_sdk.account import Account
from aptos_sdk.account_address import AccountAddress
from aptos_sdk.async_client import RestClient

from config.settings import settings
from utils.logger import get_logger

if TYPE_CHECKING:
    from exchanges.bybit_trader import BybitTrader
    from exchanges.mexc_trader import MexcTrader
    from core.price_collector import PriceCollector

logger = get_logger()

# How often we re-fetch balances (seconds)
_BALANCE_REFRESH_TTL_S = 10.0

# Default safety reserve: keep at least this fraction of the required amount
# as a buffer (e.g. 0.02 = 2% extra beyond what the trade needs).
_DEFAULT_RESERVE_BUFFER_PCT = 0.02


@dataclass
class AssetBalance:
    """Snapshot of a single asset balance on one exchange."""
    free: float = 0.0
    locked: float = 0.0
    timestamp: float = 0.0  # when fetched

    @property
    def total(self) -> float:
        return self.free + self.locked

    def is_stale(self, max_age: float = _BALANCE_REFRESH_TTL_S) -> bool:
        return (time.time() - self.timestamp) > max_age


@dataclass
class LegRequirement:
    """Describes what one trade leg needs."""
    exchange: str       # "bybit", "mexc", or "dex"
    asset: str          # "USDT", "AMI", "APT"
    amount: float       # how much of the asset is needed
    side: str           # "buy" or "sell"
    symbol: str = ""    # CEX pair or DEX description


@dataclass
class BalanceCheckResult:
    """Result of a pre-trade balance check."""
    ok: bool
    details: Dict[str, Dict[str, float]] = field(default_factory=dict)
    reason: str = ""


class BalanceManager:
    """Central balance tracker for Bybit, MEXC, and Aptos wallet."""

    def __init__(
        self,
        bybit_trader: Optional["BybitTrader"] = None,
        mexc_trader: Optional["MexcTrader"] = None,
        price_collector: Optional["PriceCollector"] = None,
        reserve_buffer_pct: float = _DEFAULT_RESERVE_BUFFER_PCT,
        refresh_ttl: float = _BALANCE_REFRESH_TTL_S,
    ) -> None:
        self.bybit_trader = bybit_trader
        self.mexc_trader = mexc_trader
        self.price_collector = price_collector
        self.reserve_buffer_pct = reserve_buffer_pct
        self.refresh_ttl = refresh_ttl

        # Aptos setup
        headers = {"x-api-key": settings.cellana_grpc_api_key} if settings.cellana_grpc_api_key else None
        self.aptos_client = RestClient(settings.aptos_node_url)
        self._aptos_headers = headers
        self._aptos_account: Optional[Account] = None
        if settings.aptos_private_key:
            self._aptos_account = Account.load_key(settings.aptos_private_key)

        self._http_session: Optional[aiohttp.ClientSession] = None

        # {exchange: {asset: AssetBalance}}
        self._cache: Dict[str, Dict[str, AssetBalance]] = {
            "bybit": {},
            "mexc": {},
            "dex": {},  # For Aptos wallet
        }
        # Initial balances snapshot for profit analysis
        self.initial_balances: Dict[str, Dict[str, float]] = {}
        self.initial_total_equity: float = 0.0

        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    #  Refresh
    # ------------------------------------------------------------------ #
    async def refresh_all(self, force: bool = False) -> None:
        """Alias for refresh() to maintain compatibility."""
        await self.refresh()

    async def refresh(self, exchange: Optional[str] = None) -> None:
        """Fetch latest balances from specified exchange or all."""
        async with self._lock:
            tasks = []
            if exchange is None or exchange == "bybit":
                tasks.append(self._refresh_bybit())
            if exchange is None or exchange == "mexc":
                tasks.append(self._refresh_mexc())
            if exchange is None or exchange == "dex":
                tasks.append(self._refresh_aptos())
            
            if tasks:
                await asyncio.gather(*tasks)

    async def _refresh_bybit(self) -> None:
        if not self.bybit_trader: return
        try:
            raw = await self.bybit_trader.get_balance()
            now = time.time()
            for coin, free_qty in raw.items():
                self._cache["bybit"][coin] = AssetBalance(free=free_qty, timestamp=now)
            
            apt = raw.get("APT", 0.0)
            ami = raw.get("AMI", 0.0)
            logger.debug(f"BalanceManager: Bybit refreshed | APT={apt:.2f} | AMI={ami:.0f}")
        except Exception as e:
            logger.error(f"BalanceManager: Bybit refresh failed: {e}")

    async def _refresh_mexc(self) -> None:
        if not self.mexc_trader: return
        try:
            raw = await self.mexc_trader.get_balance()
            now = time.time()
            for coin, free_qty in raw.items():
                self._cache["mexc"][coin] = AssetBalance(free=free_qty, timestamp=now)
            
            apt = raw.get("APT", 0.0)
            ami = raw.get("AMI", 0.0)
            logger.debug(f"BalanceManager: MEXC refreshed | APT={apt:.2f} | AMI={ami:.0f}")
        except Exception as e:
            logger.error(f"BalanceManager: MEXC refresh failed: {e}")

    async def _refresh_aptos(self) -> None:
        """Fetch balances from Aptos wallet (Asset-based)."""
        if not self._aptos_account: return
        try:
            now = time.time()
            # 1. APT
            apt_bal = await self._get_aptos_asset_balance("0xa", 8)
            self._cache["dex"]["APT"] = AssetBalance(free=apt_bal, timestamp=now)
            # 2. AMI
            ami_bal = await self._get_aptos_asset_balance(settings.ami_token_address, 8)
            self._cache["dex"]["AMI"] = AssetBalance(free=ami_bal, timestamp=now)
            # 3. USDT
            usdt_bal = await self._get_aptos_asset_balance(settings.usdt_token_address, 6)
            self._cache["dex"]["USDT"] = AssetBalance(free=usdt_bal, timestamp=now)
            
            logger.debug(f"BalanceManager: Aptos refreshed | APT={apt_bal:.2f} | AMI={ami_bal:.0f} | USDT={usdt_bal:.2f}")
        except Exception as e:
            logger.error(f"BalanceManager: Aptos refresh failed: {e}")

    async def _get_aptos_asset_balance(self, metadata_addr: str, decimals: int) -> float:
        """Helper to call view function for FA balance."""
        try:
            url = f"{settings.aptos_node_url}/view"
            payload = {
                "function": "0x1::primary_fungible_store::balance",
                "type_arguments": ["0x1::fungible_asset::Metadata"],
                "arguments": [str(self._aptos_account.address()), metadata_addr]
            }
            if not self._http_session or self._http_session.closed:
                self._http_session = aiohttp.ClientSession(headers=self._aptos_headers)
            
            async with self._http_session.post(url, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data and len(data) > 0:
                        return int(data[0]) / (10 ** decimals)
        except Exception:
            pass
        return 0.0

    async def ensure_fresh(self, exchange: Optional[str] = None) -> None:
        """Refresh only if cache is stale."""
        now = time.time()
        need_bybit = (exchange is None or exchange == "bybit") and self._is_stale("bybit", now)
        need_mexc = (exchange is None or exchange == "mexc") and self._is_stale("mexc", now)
        need_dex = (exchange is None or exchange == "dex") and self._is_stale("dex", now)
        
        if need_bybit or need_mexc or need_dex:
            await self.refresh(exchange)

    def _is_stale(self, exchange: str, now: float) -> bool:
        balances = self._cache.get(exchange, {})
        if not balances: return True
        for ab in balances.values():
            if (now - ab.timestamp) < self.refresh_ttl:
                return False
        return True

    # ------------------------------------------------------------------ #
    #  Query
    # ------------------------------------------------------------------ #
    def get_free(self, exchange: str, asset: str) -> float:
        return self._cache.get(exchange.lower(), {}).get(asset.upper(), AssetBalance()).free

    def get_all_balances(self, exchange: str) -> Dict[str, AssetBalance]:
        """Return all asset balances for a specific exchange."""
        return self._cache.get(exchange.lower(), {})

    def get_total_usd_value(self, price_collector: Optional["PriceCollector"] = None) -> float:
        """Alias for get_total_equity_usdt to maintain compatibility with TradeExecutor."""
        return self.get_total_equity_usdt()

    def get_total_equity_usdt(self, current_prices: Optional[Dict[str, float]] = None) -> float:
        """
        Calculate total equity across all exchanges in USDT.
        If current_prices is not provided, tries to use self.price_collector.
        """
        prices = current_prices
        if not prices and self.price_collector:
            apt_data = self.price_collector.get_exchange("APTUSDT", "bybit")
            ami_data = self.price_collector.get_exchange("AMIUSDT", "bybit")
            prices = {
                "APT": apt_data.mid if apt_data else 0.0,
                "AMI": ami_data.mid if ami_data else 0.0,
            }
        
        if not prices:
             prices = {"APT": 0.0, "AMI": 0.0}

        total = 0.0
        for exch in self._cache:
            # USDT
            total += self.get_free(exch, "USDT")
            # APT
            total += self.get_free(exch, "APT") * prices.get("APT", 0.0)
            # AMI
            total += self.get_free(exch, "AMI") * prices.get("AMI", 0.0)
        return total

    def get_total_available_usdt(self) -> float:
        """Deprecated: use get_total_equity_usdt for full value."""
        return self.get_total_equity_usdt()

    # ------------------------------------------------------------------ #
    #  Snapshots & Profit Analysis
    # ------------------------------------------------------------------ #
    async def save_initial_snapshot(self) -> None:
        """Capture current balances and total equity as the baseline."""
        await self.refresh()
        
        prices = {"APT": 0.0, "AMI": 0.0}
        if self.price_collector:
            apt_data = self.price_collector.get_exchange("APTUSDT", "bybit")
            ami_data = self.price_collector.get_exchange("AMIUSDT", "bybit")
            prices = {
                "APT": apt_data.mid if apt_data else 0.0,
                "AMI": ami_data.mid if ami_data else 0.0,
            }

        async with self._lock:
            for exch, assets in self._cache.items():
                self.initial_balances[exch] = {
                    asset: ab.total for asset, ab in assets.items()
                }
            
            # Save the point-in-time equity as the baseline
            self.initial_total_equity = self.get_total_equity_usdt(prices)
            
            # Detailed assets for logging
            total_usdt = sum(self.get_free(exch, "USDT") for exch in self._cache)
            total_apt = sum(self.get_free(exch, "APT") for exch in self._cache)
            total_ami = sum(self.get_free(exch, "AMI") for exch in self._cache)
            
            logger.success(
                f"📊 [INITIAL SNAPSHOT] Total Equity: {self.initial_total_equity:.2f} USDT | "
                f"USDT: {total_usdt:.2f} | APT: {total_apt:.4f} | AMI: {total_ami:.0f}"
            )

    def get_profit_summary(self, current_prices: Dict[str, float]) -> str:
        """
        Compare current balances vs initial_balances.
        current_prices: {"APT": price_in_usdt, "AMI": price_in_usdt}
        """
        if not self.initial_balances:
            return "<i>No initial snapshot available.</i>"

        summary = []
        total_initial_usdt = 0.0
        total_current_usdt = 0.0

        for exch in ["bybit", "mexc", "dex"]:
            initial = self.initial_balances.get(exch, {})
            current_assets = self._cache.get(exch, {})
            
            # Calculate Equity for this exchange
            exch_initial_usdt = 0.0
            exch_current_usdt = 0.0
            
            for asset in ["USDT", "APT", "AMI"]:
                price = 1.0 if asset == "USDT" else current_prices.get(asset, 0.0)
                
                init_qty = initial.get(asset, 0.0)
                curr_qty = current_assets.get(asset, AssetBalance()).total
                
                exch_initial_usdt += init_qty * price
                exch_current_usdt += curr_qty * price

            total_initial_usdt += exch_initial_usdt
            total_current_usdt += exch_current_usdt
            
            diff = exch_current_usdt - exch_initial_usdt
            sign = "+" if diff >= 0 else ""
            summary.append(f"• <b>{exch.upper()}</b>: {exch_current_usdt:.2f} USDT ({sign}{diff:.2f})")

        total_diff = total_current_usdt - total_initial_usdt
        total_sign = "+" if total_diff >= 0 else ""
        
        report = "\n".join(summary)
        report += f"\n\n💰 <b>Total Equity:</b> {total_current_usdt:.2f} USDT"
        report += f"\n📈 <b>Profit since start:</b> {total_sign}{total_diff:.4f} USDT"
        
        return report

    # ------------------------------------------------------------------ #
    #  Validation
    # ------------------------------------------------------------------ #
    async def check_legs(self, legs: List[LegRequirement]) -> BalanceCheckResult:
        """Validate that all legs can be funded."""
        exchanges_needed = {leg.exchange.lower() for leg in legs}
        for exch in exchanges_needed:
            await self.ensure_fresh(exch)

        details: Dict[str, Dict] = {}
        all_ok = True
        reasons: List[str] = []

        for leg in legs:
            exch = leg.exchange.lower()
            asset = leg.asset.upper()
            need = leg.amount * (1.0 + self.reserve_buffer_pct)

            free = self.get_free(exch, asset)
            ok = free >= need

            key = f"{exch}:{asset}"
            details[key] = {"free": round(free, 6), "need": round(need, 6), "ok": ok}

            if not ok:
                all_ok = False
                reasons.append(f"{exch.upper()} {asset}: need {need:.6f} but free={free:.6f}")

        return BalanceCheckResult(ok=all_ok, details=details, reason="; ".join(reasons) if reasons else "")

    async def run_refresh_loop(self, interval: float = 0.0) -> None:
        interval = interval or self.refresh_ttl
        logger.info(f"BalanceManager: starting refresh loop (every {interval:.1f}s)")
        while True:
            try:
                await self.refresh()
            except Exception as e:
                logger.error(f"BalanceManager refresh loop error: {e}")
            await asyncio.sleep(interval)
