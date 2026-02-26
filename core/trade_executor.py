"""
TradeExecutor — orchestrates concurrent order placement for arbitrage legs.

Modes:
  DRY_RUN=true  (default): log what would be executed, no real orders.
  DRY_RUN=false           : execute real orders on both legs simultaneously.

CEX-CEX arb  → Bybit market order + MEXC market order in parallel.
DEX-CEX arb  → Panora DEX swap + CEX market order in parallel.
Triangular   → Panora swap (sequential, CEX hedges pre-positioned balance).
"""
from __future__ import annotations

import asyncio
from typing import Optional, TYPE_CHECKING

from config.settings import settings
from exchanges.bybit_trader import BybitTrader
from exchanges.mexc_trader import MexcTrader
from utils.logger import get_logger

if TYPE_CHECKING:
    from exchanges.panora_executor import PanoraExecutor

logger = get_logger()

# Per-leg execution timeout (seconds). Abort if a single API call takes longer.
_LEG_TIMEOUT_S = 30


class TradeExecutor:
    """Coordinate simultaneous execution of arbitrage legs."""

    def __init__(
        self,
        panora_executor: "Optional[PanoraExecutor]" = None,
    ) -> None:
        self.bybit = BybitTrader()
        self.mexc = MexcTrader()
        self.panora_executor = panora_executor
        self.dry_run = settings.dry_run

        # Prevents concurrent triangular executions from overlapping
        self._tri_lock: asyncio.Lock | None = None  # lazy-init (event loop may not exist yet)

        mode = "DRY-RUN (paper)" if self.dry_run else "🔴 LIVE TRADING"
        logger.info(
            f"TradeExecutor initialised | mode={mode} "
            f"max_trade={settings.trade_amount_usdt} USDT"
        )

    def _get_tri_lock(self) -> asyncio.Lock:
        """Lazy-initialise the triangular execution lock (needs a running event loop)."""
        if self._tri_lock is None:
            self._tri_lock = asyncio.Lock()
        return self._tri_lock

    # ------------------------------------------------------------------ #
    #  CEX ↔ CEX  (Bybit / MEXC)
    # ------------------------------------------------------------------ #
    async def execute_cex_cex(
        self,
        buy_exchange: str,   # "Bybit" or "MEXC"
        sell_exchange: str,  # "Bybit" or "MEXC"
        symbol: str,
        buy_price: float,
        sell_price: float,
        qty: float,
    ) -> bool:
        """Place buy on buy_exchange and sell on sell_exchange simultaneously.

        qty is capped by settings.trade_amount_usdt / buy_price for safety.
        Returns True if both legs succeeded.
        """
        # Safety cap on quantity
        max_qty = settings.trade_amount_usdt / buy_price
        safe_qty = min(qty, max_qty)

        logger.info(
            f"{'[DRY]' if self.dry_run else '[LIVE]'} CEX-CEX EXECUTE | "
            f"BUY {buy_exchange} @ {buy_price:.8f}  "
            f"SELL {sell_exchange} @ {sell_price:.8f}  "
            f"QTY={safe_qty:.6f} {symbol}"
        )

        if self.dry_run:
            return True

        # ---- real execution ---- #
        buy_task = self._cex_buy(buy_exchange, symbol, safe_qty, buy_price)
        sell_task = self._cex_sell(sell_exchange, symbol, safe_qty)

        buy_id, sell_id = await asyncio.gather(buy_task, sell_task)

        if buy_id and sell_id:
            logger.success(
                f"✅ CEX-CEX executed | buy={buy_id} sell={sell_id}"
            )
            return True
        else:
            logger.error(
                f"❌ CEX-CEX partial fill | buy_ok={bool(buy_id)} sell_ok={bool(sell_id)} "
                f"→ manual intervention may be required"
            )
            return False

    # ------------------------------------------------------------------ #
    #  DEX ↔ CEX  (Panora / Bybit|MEXC)
    # ------------------------------------------------------------------ #
    async def execute_dex_cex(
        self,
        direction: str,      # "BUY_DEX_SELL_CEX" | "BUY_CEX_SELL_DEX"
        cex_name: str,       # "Bybit" or "MEXC"
        cex_symbol: str,
        buy_price: float,
        sell_price: float,
        qty: float,
        prefetched_quote: "Optional[dict]" = None,  # reuse verified quote, skip 2nd API call
    ) -> bool:
        """Execute one DEX leg (Panora) and one CEX leg in parallel.

        direction="BUY_DEX_SELL_CEX": buy AMI on Panora (USDT→AMI), sell AMI on CEX.
        direction="BUY_CEX_SELL_DEX": buy AMI on CEX, sell AMI on Panora (AMI→USDT).
        Returns True if both legs succeeded.
        """
        if not self.panora_executor:
            logger.error("TradeExecutor: no PanoraExecutor — cannot execute DEX leg")
            return False

        # Safety cap
        max_qty = settings.trade_amount_usdt / buy_price
        safe_qty = min(qty, max_qty)

        logger.info(
            f"{'[DRY]' if self.dry_run else '[LIVE]'} DEX-CEX EXECUTE | "
            f"dir={direction} cex={cex_name} "
            f"buy@{buy_price:.8f} sell@{sell_price:.8f} QTY={safe_qty:.6f}"
        )

        if self.dry_run:
            return True

        # ---- real execution ---- #
        if direction == "BUY_DEX_SELL_CEX":
            # Panora: USDT → AMI (spend USDT, receive AMI)
            usdt_to_spend = safe_qty * buy_price
            dex_task = self.panora_executor.execute_swap(
                usdt_to_spend,
                from_token_address=settings.usdt_token_address,
                to_token_address=settings.ami_token_address,
                prefetched_quote=prefetched_quote,
            )
            cex_task = self._cex_sell(cex_name, cex_symbol, safe_qty)

        elif direction == "BUY_CEX_SELL_DEX":
            # CEX: buy AMI with USDT; Panora: AMI → USDT
            cex_task = self._cex_buy(cex_name, cex_symbol, safe_qty, buy_price)
            dex_task = self.panora_executor.execute_swap(
                safe_qty,
                from_token_address=settings.ami_token_address,
                to_token_address=settings.usdt_token_address,
                prefetched_quote=prefetched_quote,
            )
        else:
            logger.error(f"TradeExecutor: unknown direction={direction}")
            return False

        dex_result, cex_result = await asyncio.gather(dex_task, cex_task)

        if dex_result and cex_result:
            logger.success(
                f"✅ DEX-CEX executed | dex_tx={dex_result} cex_order={cex_result}"
            )
            return True
        else:
            logger.error(
                f"❌ DEX-CEX partial fill | dex_ok={bool(dex_result)} "
                f"cex_ok={bool(cex_result)} → manual intervention may be required"
            )
            return False

    # ------------------------------------------------------------------ #
    #  Triangular  (Panora APT/AMI + CEX hedge)
    # ------------------------------------------------------------------ #
    async def execute_triangular(
        self,
        direction: str,          # "APT_TO_AMI" | "AMI_TO_APT"
        cex_name: str,           # "Bybit" | "MEXC"
        apt_symbol: str,         # e.g. "APTUSDT"
        ami_symbol: str,         # e.g. "AMIUSDT"
        prefetched_quote: "Optional[dict]" = None,
        # Dir APT_TO_AMI
        qty_apt: float = 0.0,
        cex_apt_ask: float = 0.0,
        cex_ami_bid: float = 0.0,
        # Dir AMI_TO_APT
        qty_ami: float = 0.0,
        cex_ami_ask: float = 0.0,
        cex_apt_bid: float = 0.0,
    ) -> bool:
        """Execute triangular arb: Panora swap + CEX hedge (sequential).

        Direction APT_TO_AMI:
          Leg 1 — Panora: swap qty_apt APT → AMI  (from Aptos wallet)
          Leg 2 — CEX:    sell equivalent AMI      (pre-positioned CEX balance)

        Direction AMI_TO_APT:
          Leg 1 — Panora: swap qty_ami AMI → APT  (from Aptos wallet)
          Leg 2 — CEX:    sell equivalent APT      (pre-positioned CEX balance)

        Guards:
          • Execution lock: only one triangular trade runs at a time.
          • Balance gate: pre-checks Aptos wallet + CEX balance before committing.
          • Per-leg timeout: each leg aborted after _LEG_TIMEOUT_S seconds.
          • Panora failure → CEX leg aborted to avoid unhedged position.
        """
        if not self.panora_executor:
            logger.error("TradeExecutor: no PanoraExecutor — cannot execute triangular")
            return False

        lock = self._get_tri_lock()
        if lock.locked():
            logger.warning(
                f"TradeExecutor: triangular trade already in progress — skipping {direction}"
            )
            return False

        async with lock:
            return await self._execute_triangular_locked(
                direction, cex_name, apt_symbol, ami_symbol, prefetched_quote,
                qty_apt, cex_apt_ask, cex_ami_bid,
                qty_ami, cex_ami_ask, cex_apt_bid,
            )

    async def _execute_triangular_locked(
        self,
        direction: str,
        cex_name: str,
        apt_symbol: str,
        ami_symbol: str,
        prefetched_quote: "Optional[dict]",
        qty_apt: float,
        cex_apt_ask: float,
        cex_ami_bid: float,
        qty_ami: float,
        cex_ami_ask: float,
        cex_apt_bid: float,
    ) -> bool:
        """Inner execution (called while _tri_lock is held)."""

        wallet = getattr(
            self.panora_executor._get_account(),
            "account_address",
            None,
        )
        wallet_str = str(wallet) if wallet else None

        if direction == "APT_TO_AMI":
            safe_qty    = min(qty_apt, settings.trade_amount_usdt / max(cex_apt_ask, 1e-12))
            ami_to_sell = safe_qty * (cex_apt_ask / max(cex_ami_bid, 1e-12))

            logger.info(
                f"{'[DRY]' if self.dry_run else '[LIVE]'} TRI-DIR1 | {cex_name} | "
                f"Panora {safe_qty:.4f} APT→AMI  then sell ~{ami_to_sell:.2f} AMI @ {cex_ami_bid:.8f}"
            )

            if self.dry_run:
                return True

            # --- Balance gate ---
            if not await self._check_tri_balances_apt_to_ami(
                cex_name, wallet_str, safe_qty, ami_to_sell
            ):
                return False

            # Leg 1: Panora APT→AMI (with timeout)
            try:
                tx = await asyncio.wait_for(
                    self.panora_executor.execute_swap(
                        safe_qty,
                        from_token_address=settings.apt_token_address,
                        to_token_address=settings.ami_token_address,
                        prefetched_quote=prefetched_quote,
                    ),
                    timeout=_LEG_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                logger.error(
                    f"❌ TRI-DIR1: Panora APT→AMI timed out after {_LEG_TIMEOUT_S}s — aborting"
                )
                return False

            if not tx:
                logger.error("❌ TRI-DIR1: Panora swap APT→AMI failed → aborting CEX leg")
                return False
            logger.info(f"✅ TRI-DIR1 Leg1 done | tx={tx}")

            # Leg 2: CEX sell AMI (with timeout)
            try:
                order_id = await asyncio.wait_for(
                    self._cex_sell(cex_name, ami_symbol, ami_to_sell),
                    timeout=_LEG_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                logger.error(
                    f"❌ TRI-DIR1: CEX sell AMI timed out (Panora tx={tx}) "
                    f"— manual rebalance required"
                )
                return False

            if not order_id:
                logger.error(
                    f"❌ TRI-DIR1: CEX sell AMI failed (Panora swap done tx={tx}) "
                    f"— manual rebalance required"
                )
                return False
            logger.success(f"✅ TRI-DIR1 complete | panora_tx={tx} cex_order={order_id}")
            return True

        elif direction == "AMI_TO_APT":
            safe_qty    = min(qty_ami, settings.trade_amount_usdt / max(cex_ami_ask, 1e-12))
            apt_to_sell = safe_qty * (cex_ami_ask / max(cex_apt_bid, 1e-12))

            logger.info(
                f"{'[DRY]' if self.dry_run else '[LIVE]'} TRI-DIR2 | {cex_name} | "
                f"Panora {safe_qty:.2f} AMI→APT  then sell ~{apt_to_sell:.4f} APT @ {cex_apt_bid:.4f}"
            )

            if self.dry_run:
                return True

            # --- Balance gate ---
            if not await self._check_tri_balances_ami_to_apt(
                cex_name, wallet_str, safe_qty, apt_to_sell
            ):
                return False

            # Leg 1: Panora AMI→APT (with timeout)
            try:
                tx = await asyncio.wait_for(
                    self.panora_executor.execute_swap(
                        safe_qty,
                        from_token_address=settings.ami_token_address,
                        to_token_address=settings.apt_token_address,
                        prefetched_quote=prefetched_quote,
                    ),
                    timeout=_LEG_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                logger.error(
                    f"❌ TRI-DIR2: Panora AMI→APT timed out after {_LEG_TIMEOUT_S}s — aborting"
                )
                return False

            if not tx:
                logger.error("❌ TRI-DIR2: Panora swap AMI→APT failed → aborting CEX leg")
                return False
            logger.info(f"✅ TRI-DIR2 Leg1 done | tx={tx}")

            # Leg 2: CEX sell APT (with timeout)
            try:
                order_id = await asyncio.wait_for(
                    self._cex_sell(cex_name, apt_symbol, apt_to_sell),
                    timeout=_LEG_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                logger.error(
                    f"❌ TRI-DIR2: CEX sell APT timed out (Panora tx={tx}) "
                    f"— manual rebalance required"
                )
                return False

            if not order_id:
                logger.error(
                    f"❌ TRI-DIR2: CEX sell APT failed (Panora swap done tx={tx}) "
                    f"— manual rebalance required"
                )
                return False
            logger.success(f"✅ TRI-DIR2 complete | panora_tx={tx} cex_order={order_id}")
            return True

        else:
            logger.error(f"TradeExecutor: unknown triangular direction={direction}")
            return False

    # ------------------------------------------------------------------ #
    #  Balance gates  (triangular)
    # ------------------------------------------------------------------ #
    async def _check_tri_balances_apt_to_ami(
        self,
        cex_name: str,
        wallet_str: Optional[str],
        qty_apt: float,
        ami_to_sell: float,
    ) -> bool:
        """Return True only if Aptos wallet has enough APT and CEX has enough AMI."""
        ok = True

        # Aptos wallet: APT
        if wallet_str and self.panora_executor:
            apt_bal = await self.panora_executor.get_token_balance(
                wallet_str, settings.apt_token_address
            )
            if apt_bal is not None and apt_bal < qty_apt:
                logger.warning(
                    f"⚠️  Balance gate FAILED: Aptos APT={apt_bal:.6f} < needed {qty_apt:.6f} "
                    f"→ aborting TRI-DIR1"
                )
                ok = False
            else:
                logger.debug(f"Balance gate APT: wallet={apt_bal} need={qty_apt:.6f}")

        # CEX: AMI (pre-positioned hedge balance)
        ami_coin = _cex_coin_for(settings.cex_symbol)  # "AMI" from "AMIUSDT"
        cex_bals = await self._cex_balances(cex_name, [ami_coin])
        ami_bal  = cex_bals.get(ami_coin, 0.0)
        if ami_bal < ami_to_sell:
            logger.warning(
                f"⚠️  Balance gate FAILED: {cex_name} AMI={ami_bal:.6f} < needed {ami_to_sell:.6f} "
                f"→ aborting TRI-DIR1"
            )
            ok = False
        else:
            logger.debug(f"Balance gate {cex_name} AMI: {ami_bal:.6f} >= {ami_to_sell:.6f}")

        return ok

    async def _check_tri_balances_ami_to_apt(
        self,
        cex_name: str,
        wallet_str: Optional[str],
        qty_ami: float,
        apt_to_sell: float,
    ) -> bool:
        """Return True only if Aptos wallet has enough AMI and CEX has enough APT."""
        ok = True

        # Aptos wallet: AMI
        if wallet_str and self.panora_executor:
            ami_decimals = getattr(settings, "ami_decimals", 8)
            ami_bal = await self.panora_executor.get_token_balance(
                wallet_str, settings.ami_token_address, decimals=ami_decimals
            )
            if ami_bal is not None and ami_bal < qty_ami:
                logger.warning(
                    f"⚠️  Balance gate FAILED: Aptos AMI={ami_bal:.6f} < needed {qty_ami:.6f} "
                    f"→ aborting TRI-DIR2"
                )
                ok = False
            else:
                logger.debug(f"Balance gate AMI: wallet={ami_bal} need={qty_ami:.6f}")

        # CEX: APT
        apt_coin = _cex_coin_for(settings.apt_cex_symbol)  # "APT" from "APTUSDT"
        cex_bals = await self._cex_balances(cex_name, [apt_coin])
        apt_bal  = cex_bals.get(apt_coin, 0.0)
        if apt_bal < apt_to_sell:
            logger.warning(
                f"⚠️  Balance gate FAILED: {cex_name} APT={apt_bal:.6f} < needed {apt_to_sell:.6f} "
                f"→ aborting TRI-DIR2"
            )
            ok = False
        else:
            logger.debug(f"Balance gate {cex_name} APT: {apt_bal:.6f} >= {apt_to_sell:.6f}")

        return ok

    async def _cex_balances(self, cex_name: str, coins: list) -> dict:
        """Helper: fetch balances from the given CEX."""
        try:
            if cex_name == "Bybit":
                return await self.bybit.get_balance(coins=coins)
            elif cex_name == "MEXC":
                return await self.mexc.get_balance(coins=coins)
        except Exception as e:
            logger.warning(f"TradeExecutor._cex_balances({cex_name}) error: {e}")
        return {}

    # ------------------------------------------------------------------ #
    #  Internal helpers
    # ------------------------------------------------------------------ #
    async def _cex_buy(
        self,
        exchange: str,
        symbol: str,
        qty: float,
        price: float,
    ) -> Optional[str]:
        """Buy `qty` AMI at approximate `price` (market order, qty in base)."""
        if exchange == "Bybit":
            # Bybit Buy: use baseCoinQty
            return await self.bybit.place_market_order(
                symbol, "Buy", qty, market_unit="baseCoinQty"
            )
        elif exchange == "MEXC":
            return await self.mexc.place_market_order(
                symbol, "BUY", qty, is_quote_qty=False
            )
        else:
            logger.error(f"Unknown exchange: {exchange}")
            return None

    async def _cex_sell(
        self,
        exchange: str,
        symbol: str,
        qty: float,
    ) -> Optional[str]:
        """Sell `qty` AMI (market order)."""
        if exchange == "Bybit":
            return await self.bybit.place_market_order(
                symbol, "Sell", qty, market_unit="baseCoinQty"
            )
        elif exchange == "MEXC":
            return await self.mexc.place_market_order(
                symbol, "SELL", qty, is_quote_qty=False
            )
        else:
            logger.error(f"Unknown exchange: {exchange}")
            return None


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _cex_coin_for(cex_symbol: str) -> str:
    """Extract the base coin from a CEX trading pair symbol.

    Examples:
        "AMIUSDT" → "AMI"
        "APTUSDT" → "APT"
        "BTCUSDT" → "BTC"
    """
    for stable in ("USDT", "USDC", "BUSD", "USD", "BTC", "ETH"):
        if cex_symbol.endswith(stable):
            return cex_symbol[: -len(stable)]
    return cex_symbol
