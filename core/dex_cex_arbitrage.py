"""
DEX-to-CEX arbitrage engine for AMI token.

Monitors Cellana DEX prices and compares with CEX prices (Bybit/MEXC)
to identify profitable arbitrage opportunities.

Price format:
- Cellana DEX: AMI/APT = 1 AMI = X APT (quote in APT)
- CEX: AMIUSDT = 1 AMI = Y USDT (quote in USDT)
- Requires: APT/USDT rate to convert between them
"""

import asyncio
import time
from datetime import datetime
from typing import Dict, Optional, Tuple, TYPE_CHECKING, Any, List

from config.settings import settings
from core.price_collector import PriceData
from core.balance_manager import BalanceManager, LegRequirement
from utils.logger import get_logger, log_signal, log_arbitrage_opportunity

if TYPE_CHECKING:
    from core.price_collector import PriceCollector
    from core.trade_executor import TradeExecutor

logger = get_logger()


class OpportunityDeduplicator:
    """De-duplicate arbitrage opportunities to avoid spam logging."""

    def __init__(self, cooldown_sec: float = 5.0, price_decimals: int = 4):
        self.cooldown_sec = cooldown_sec
        self.price_decimals = price_decimals
        self._last_logged: Dict[str, float] = {}  # key -> last_timestamp

    def _make_key(self, direction: str, buy_price: float, sell_price: float) -> str:
        """Create unique key for (direction, rounded_prices)."""
        bp = round(buy_price, self.price_decimals)
        sp = round(sell_price, self.price_decimals)
        return f"{direction}:{bp}:{sp}"

    def should_log(self, direction: str, buy_price: float, sell_price: float) -> bool:
        """Return True if this opportunity should be logged (not a duplicate)."""
        key = self._make_key(direction, buy_price, sell_price)
        now = time.time()
        last_ts = self._last_logged.get(key, 0.0)

        if now - last_ts >= self.cooldown_sec:
            self._last_logged[key] = now
            return True
        return False


class DexCexArbitrage:
    """Detect and execute DEX-to-CEX arbitrage for AMI/APT pool."""

    def __init__(
        self,
        cex_collector: "PriceCollector",
        trade_executor: Optional["TradeExecutor"] = None,
        balance_manager: Optional["BalanceManager"] = None,
        bybit_ws: Optional[Any] = None,
        mexc_ws: Optional[Any] = None,
    ) -> None:
        """
        Args:
            cex_collector: PriceCollector for CEX prices (AMIUSDT)
            trade_executor: TradeExecutor for executing trades
            balance_manager: BalanceManager for pre-trade balance checks
            bybit_ws: BybitWS instance for dynamic Bybit fees
            mexc_ws: MexcWS instance for dynamic MEXC fees
        """
        self.cex_collector = cex_collector
        self.trade_executor = trade_executor
        self.balance_manager = balance_manager
        self.bybit_ws = bybit_ws
        self.mexc_ws = mexc_ws

        # Configuration
        self.min_profit_pct = settings.min_profit_threshold
        self.poll_interval = settings.arb_check_interval

        # Cellana DEX configuration
        self.cellana_fee = settings.cellana_volatile_fee  # from env/settings (default 0.1%, on-chain swap_fee_bps=10)
        self.cellana_decimals_ami = 8
        self.cellana_decimals_apt = 8

        # CEX configuration (fallback to settings if exchange not provided)
        self.cex_symbol = settings.cex_symbol  # AMIUSDT

        # Trade size limits (USDT)
        self.max_trade_usdt = settings.max_trade_usdt
        self.min_trade_usdt = settings.min_trade_usdt
        self.optimal_size_enabled = settings.optimal_size_enabled
        self.optimal_size_steps = settings.optimal_size_steps
        self.min_profit_usd = 0.0    # Minimum profit in USD to trigger (fallback)

        # Fee optimization: use maker (limit order) or taker (market order)
        self._fee_type = "maker" if settings.use_maker_fee else "taker"

        # Gas cost per DEX swap (subtracted from gross profit)
        self.gas_cost_usd = settings.gas_cost_usd

        # Strategy-specific thresholds (absolute USD)
        self.min_profit_dex_to_cex = settings.min_profit_dex_to_cex
        self.min_profit_ami_cycle = settings.min_profit_ami_cycle
        self.min_profit_cross_cex = settings.min_profit_cross_cex
        self.min_profit_cex_to_cex = settings.min_profit_cex_to_cex
        self.min_profit_apt_start = settings.min_profit_apt_start
        self.min_profit_ami_start = settings.min_profit_ami_start

        # Strategy-specific thresholds (percentage) — triggers if EITHER met
        self.min_profit_pct_dex_to_cex = settings.min_profit_pct_dex_to_cex
        self.min_profit_pct_ami_cycle = settings.min_profit_pct_ami_cycle
        self.min_profit_pct_cross_cex = settings.min_profit_pct_cross_cex
        self.min_profit_pct_cex_to_cex = settings.min_profit_pct_cex_to_cex
        self.min_profit_pct_apt_start = settings.min_profit_pct_apt_start
        self.min_profit_pct_ami_start = settings.min_profit_pct_ami_start

        # DEX execution realism
        self.dex_block_delay_ms = settings.dex_block_delay_ms
        self.dex_slippage_buffer_pct = settings.dex_slippage_buffer_pct / 100.0

        # Adaptive slippage parameters
        self.adaptive_slippage_enabled = settings.adaptive_slippage_enabled
        self.adaptive_slippage_base_pct = settings.adaptive_slippage_base_pct / 100.0
        self.adaptive_slippage_impact_mult = settings.adaptive_slippage_impact_mult
        self.adaptive_slippage_max_pct = settings.adaptive_slippage_max_pct / 100.0

        # De-duplication
        self.deduplicator = OpportunityDeduplicator(
            cooldown_sec=settings.arb_dedup_cooldown_sec,
            price_decimals=settings.arb_price_round_decimals,
        )

        # ── Execution safety guards ──
        # Lock: only ONE trade can execute at a time (prevents concurrent
        # CEX orders / Aptos nonce collisions)
        self._execution_lock = asyncio.Lock()
        # Cooldown: minimum seconds between trade executions
        self._trade_cooldown_s: float = settings.trade_cooldown_s
        self._last_trade_ts: float = 0.0
        # Minimum APT balance required on wallet before attempting DEX swap
        self._min_gas_apt: float = settings.min_gas_apt

        # Price tracking (per-exchange timers for separate logging)
        self._last_price_log: Dict[str, float] = {}  # exchange -> last_ts
        self._PRICE_LOG_INTERVAL_S = 5.0

        # Near-miss logging (diagnostic): log when profit is within 70% of threshold
        self._NEAR_MISS_RATIO = 0.70  # 70% of threshold
        self._last_near_miss_log: float = 0.0
        self._NEAR_MISS_LOG_INTERVAL_S = 10.0  # rate-limit near-miss logs

        # Log optimization settings at startup
        logger.info(
            f"⚙️  Fee type: {self._fee_type} | Gas cost: ${self.gas_cost_usd:.4f} | "
            f"Reserve poll: {settings.reserve_poll_interval_s}s | "
            f"Thresholds: D2C=${self.min_profit_dex_to_cex}/{self.min_profit_pct_dex_to_cex}% "
            f"AMI=${self.min_profit_ami_cycle}/{self.min_profit_pct_ami_cycle}%"
        )

        # Latest prices
        self.cellana_price_ami_apt: Optional[float] = None  # 1 AMI = X APT
        self.cellana_timestamp: float = 0.0
        self.cellana_reserve_ami: Optional[float] = None
        self.cellana_reserve_apt: Optional[float] = None
        self.apt_usdt_price: Optional[float] = None  # 1 APT = X USDT
        self.apt_usdt_timestamp: float = 0.0

    def _get_bybit_fee(self, fee_type: str = "taker") -> float:
        """Get current Bybit fee (with fallback to settings)."""
        if self.bybit_ws and hasattr(self.bybit_ws, 'get_fee'):
            return self.bybit_ws.get_fee(fee_type)
        return settings.bybit_fee

    def _get_mexc_fee(self, fee_type: str = "taker", symbol: Optional[str] = None) -> float:
        """Get current MEXC fee (with fallback to settings)."""
        if self.mexc_ws and hasattr(self.mexc_ws, 'get_fee'):
            return self.mexc_ws.get_fee(fee_type, symbol)
        return settings.mexc_fee

    def _calculate_trade_size_usdt(
        self, 
        base_amount_usdt: float,
        price_data: PriceData,
        side: str = "buy"  # "buy" or "sell"
    ) -> float:
        """Calculate actual trade size based on available orderbook depth.
        
        Uses full depth liquidity (sum across all levels) when multi-level
        data is available, falling back to L1 only.
        
        Args:
            base_amount_usdt: Desired trade size in USDT
            price_data: PriceData with bid/ask quantities (+ optional depth)
            side: "buy" (use asks) or "sell" (use bids)
            
        Returns:
            Actual trade size in USDT (capped by liquidity and max_trade_usdt)
        """
        # Cap by max_trade_usdt
        trade_usdt = min(base_amount_usdt, self.max_trade_usdt)
        
        if side == "buy":
            # Use total ask depth liquidity (USDT)
            available_usdt = price_data.total_ask_liquidity_usdt
        else:
            # Use total bid depth liquidity (USDT)
            available_usdt = price_data.total_bid_liquidity_usdt
        
        if available_usdt > 0:
            trade_usdt = min(trade_usdt, available_usdt)
        
        return trade_usdt

    def _cap_trade_usdt_by_downstream_qty(
        self,
        base_trade_usdt: float,
        buy_price: float,
        buy_fee: float,
        downstream_max_qty: float,
        convert_fn,
    ) -> float:
        """Cap USDT trade size so downstream output qty stays within liquidity."""
        if base_trade_usdt <= 0 or buy_price <= 0 or downstream_max_qty <= 0:
            return 0.0

        fee_factor = 1.0 + buy_fee
        if fee_factor <= 0:
            return 0.0

        def out_qty(usdt_in: float) -> float:
            bought = usdt_in / (buy_price * fee_factor)
            return float(convert_fn(bought))

        if out_qty(base_trade_usdt) <= downstream_max_qty:
            return base_trade_usdt

        lo, hi = 0.0, base_trade_usdt
        for _ in range(36):
            mid = (lo + hi) / 2.0
            if out_qty(mid) <= downstream_max_qty:
                lo = mid
            else:
                hi = mid
        return lo

    def _adaptive_slippage(self, trade_input: float, reserve_input: float) -> float:
        """Calculate adaptive slippage based on trade size vs pool reserves.

        Formula:
          slippage = base + impact_mult × (trade_input / reserve_input)

        The idea is that large trades relative to pool depth suffer more
        slippage from front-running, block-delay, and reserve movement
        between observation and execution.

        Returns:
            Slippage fraction (e.g. 0.003 = 0.3%).  Always in
            [0, adaptive_slippage_max_pct].
        """
        if not self.adaptive_slippage_enabled:
            return self.dex_slippage_buffer_pct

        if reserve_input <= 0 or trade_input <= 0:
            return self.adaptive_slippage_base_pct

        ratio = trade_input / reserve_input
        slip = self.adaptive_slippage_base_pct + self.adaptive_slippage_impact_mult * ratio
        return min(slip, self.adaptive_slippage_max_pct)

    def _swap_apt_to_ami(self, apt_in: float) -> float:
        """Swap APT to AMI on DEX using constant-product formula.

        When reserves are known the AMM formula already embeds the exact
        price impact — no additional slippage buffer is applied.
        A small buffer is only used in the fallback path (no reserves).
        """
        if apt_in <= 0:
            return 0.0
        if (
            self.cellana_reserve_ami
            and self.cellana_reserve_apt
            and self.cellana_reserve_ami > 0
            and self.cellana_reserve_apt > 0
        ):
            dx_eff = apt_in * (1.0 - self.cellana_fee)
            raw_out = self.cellana_reserve_ami * dx_eff / (self.cellana_reserve_apt + dx_eff)
            return raw_out  # AMM output is exact — no extra buffer
        if self.cellana_price_ami_apt and self.cellana_price_ami_apt > 0:
            raw_out = apt_in / self.cellana_price_ami_apt * (1.0 - self.cellana_fee)
            slip = self._adaptive_slippage(apt_in, 0.0)  # no reserves → keep buffer
            return raw_out * (1.0 - slip)
        return 0.0

    def _swap_ami_to_apt(self, ami_in: float) -> float:
        """Swap AMI to APT on DEX using constant-product formula.

        When reserves are known the AMM formula already embeds the exact
        price impact — no additional slippage buffer is applied.
        A small buffer is only used in the fallback path (no reserves).
        """
        if ami_in <= 0:
            return 0.0
        if (
            self.cellana_reserve_ami
            and self.cellana_reserve_apt
            and self.cellana_reserve_ami > 0
            and self.cellana_reserve_apt > 0
        ):
            dx_eff = ami_in * (1.0 - self.cellana_fee)
            raw_out = self.cellana_reserve_apt * dx_eff / (self.cellana_reserve_ami + dx_eff)
            return raw_out  # AMM output is exact — no extra buffer
        if self.cellana_price_ami_apt and self.cellana_price_ami_apt > 0:
            raw_out = ami_in * self.cellana_price_ami_apt * (1.0 - self.cellana_fee)
            slip = self._adaptive_slippage(ami_in, 0.0)
            return raw_out * (1.0 - slip)
        return 0.0

    # ------------------------------------------------------------------ #
    #  Optimal trade-size calculator
    # ------------------------------------------------------------------ #

    def _find_optimal_trade_size(
        self,
        profit_fn,
        max_usdt: float,
        orderbook_boundaries: Optional[List[float]] = None,
    ) -> Tuple[float, float]:
        """Find the trade size (USDT) that maximises absolute profit.

        Strategy:
        1. If *orderbook_boundaries* are provided (cumulative USDT at each
           orderbook level), evaluate profit at each boundary — these are the
           natural breakpoints where marginal price changes.  Also test a
           few sub-level samples within the best interval.
        2. Otherwise fall back to a coarse grid + golden-section refinement.

        Returns:
            (best_size_usdt, best_profit_usdt)
        """
        lo = self.min_trade_usdt
        hi = max(max_usdt, lo + 1.0)

        # ── Orderbook-driven scan ─────────────────────────────────────
        if orderbook_boundaries:
            # Build candidate sizes from orderbook level boundaries
            candidates = [lo]
            for boundary in orderbook_boundaries:
                if lo < boundary <= hi:
                    candidates.append(boundary)
            candidates.append(hi)
            # Remove duplicates and sort
            candidates = sorted(set(candidates))

            best_size = lo
            best_profit = profit_fn(lo)
            best_idx = 0

            for i, sz in enumerate(candidates):
                pf = profit_fn(sz)
                if pf > best_profit:
                    best_profit = pf
                    best_size = sz
                    best_idx = i

            # Refine within the best interval — the profit peak may sit
            # between two orderbook boundaries (because the AMM curve is
            # continuous while the orderbook is discrete).
            if len(candidates) >= 2:
                ref_lo = candidates[max(best_idx - 1, 0)]
                ref_hi = candidates[min(best_idx + 1, len(candidates) - 1)]
                if ref_hi - ref_lo > 1.0:
                    # Golden-section within this narrow interval
                    gr = 0.6180339887
                    a, b = ref_lo, ref_hi
                    for _ in range(10):
                        if b - a < 0.5:
                            break
                        x1 = b - gr * (b - a)
                        x2 = a + gr * (b - a)
                        if profit_fn(x1) < profit_fn(x2):
                            a = x1
                        else:
                            b = x2
                    mid = (a + b) / 2.0
                    mid_pf = profit_fn(mid)
                    if mid_pf > best_profit:
                        best_size = mid
                        best_profit = mid_pf

            return best_size, best_profit

        # ── Fallback: coarse grid + golden-section ────────────────────
        n_steps = max(self.optimal_size_steps, 3)
        step = (hi - lo) / n_steps

        best_size = lo
        best_profit = profit_fn(lo)
        prev_size = lo
        for i in range(1, n_steps + 1):
            sz = lo + step * i
            pf = profit_fn(sz)
            if pf > best_profit:
                best_profit = pf
                best_size = sz
                prev_size = lo + step * (i - 1)

        a = max(prev_size, lo)
        b = min(best_size + step, hi)
        gr = 0.6180339887
        for _ in range(12):
            if b - a < 0.5:
                break
            x1 = b - gr * (b - a)
            x2 = a + gr * (b - a)
            if profit_fn(x1) < profit_fn(x2):
                a = x1
            else:
                b = x2
        final_size = (a + b) / 2.0
        final_profit = profit_fn(final_size)
        if final_profit > best_profit:
            best_size = final_size
            best_profit = final_profit

        return best_size, best_profit

    @staticmethod
    def _orderbook_cumulative_usdt(levels: List[Tuple[float, float]]) -> List[float]:
        """Convert orderbook levels [(price, qty), ...] to cumulative USDT boundaries.

        Each entry is the total USDT spent/received through that level.
        """
        boundaries: List[float] = []
        total = 0.0
        for price, qty in levels:
            total += price * qty
            boundaries.append(total)
        return boundaries

    @staticmethod
    def _orderbook_cumulative_qty_as_usdt(
        levels: List[Tuple[float, float]], ref_price: float
    ) -> List[float]:
        """Convert sell-side orderbook to cumulative USDT equivalent.

        For sell orders the natural unit is qty.  Multiply by *ref_price* to
        get approximate USDT boundaries for the optimizer.
        """
        boundaries: List[float] = []
        total_qty = 0.0
        for _price, qty in levels:
            total_qty += qty
            boundaries.append(total_qty * ref_price)
        return boundaries

    def update_cellana_price(
        self,
        price_ami_apt: float,
        price_with_fee: Optional[float] = None,
        timestamp: Optional[float] = None,
        reserves_ami: Optional[float] = None,
        reserves_apt: Optional[float] = None,
    ) -> None:
        """Update Cellana DEX price from listener.

        Args:
            price_ami_apt: 1 AMI = X APT (spot price)
            price_with_fee: 1 AMI = X APT (with trading fee)
            timestamp: When price was observed
            reserves_ami: Current AMI reserve in pool (human units)
            reserves_apt: Current APT reserve in pool (human units)
        """
        self.cellana_price_ami_apt = price_ami_apt
        self.cellana_timestamp = timestamp or time.time()
        if reserves_ami is not None and reserves_apt is not None:
            self.cellana_reserve_ami = reserves_ami
            self.cellana_reserve_apt = reserves_apt

    def update_apt_usdt_price(self, apt_price: float) -> None:
        """Update APT/USDT price (needed to convert DEX price to USDT).

        Can fetch from CEX mid-price or other source.
        """
        self.apt_usdt_price = apt_price
        self.apt_usdt_timestamp = time.time()

    def get_cellana_price_in_usdt(self) -> Optional[float]:
        """Convert Cellana AMI/APT price to AMI/USDT.

        Returns: 1 AMI = X USDT
        """
        if not self.cellana_price_ami_apt or not self.apt_usdt_price:
            return None
        # 1 AMI = X APT, 1 APT = Y USDT => 1 AMI = X*Y USDT
        return self.cellana_price_ami_apt * self.apt_usdt_price

    # ------------------------------------------------------------------ #
    #  Log + Execute (wires detection → TradeExecutor)
    # ------------------------------------------------------------------ #
    def _log_and_execute(self, payload: dict) -> None:
        """Log opportunity AND schedule trade execution if executor exists."""
        log_arbitrage_opportunity(payload)
        if not self.trade_executor:
            return
        trade_usdt = float(payload.get("trade_size_usdt", 0))
        if trade_usdt <= 0:
            return
        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(self._execute_opportunity(payload))
            task.add_done_callback(self._task_exception_handler)
        except RuntimeError:
            # No running event loop — likely called from a non-async context
            logger.debug("No running event loop for trade execution, skipping")
        except Exception as e:
            logger.error(f"Failed to schedule trade execution: {e}")

    @staticmethod
    def _task_exception_handler(task: asyncio.Task) -> None:
        """Log unhandled exceptions from fire-and-forget trade tasks."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc:
            logger.error(f"🚨 Unhandled trade task exception: {exc}", exc_info=exc)

    async def _execute_opportunity(self, payload: dict) -> None:
        """Build TradeLeg list and execute: CEX-first, then DEX with retry.

        Strategy (pre-funded inventory model):
          1. Execute ALL CEX legs in parallel (fast, cheap to fail/rollback)
          2. If CEX OK and direction involves DEX → execute DEX swap on-chain
          3. If DEX fails → re-quote DEX to check price still viable → retry
          4. After max retries with no success → rollback CEX legs

        Safety: skip if another trade is already executing (non-blocking).
        Stale signals are discarded — the next price update will generate
        a fresh signal if the opportunity still exists.
        """
        if self._execution_lock.locked():
            direction = payload.get("direction", "?")
            logger.debug(
                f"⏭️ Lock busy — skipping {direction} "
                f"(will catch next signal)"
            )
            return
        async with self._execution_lock:
            await self._execute_opportunity_inner(payload)

    async def _execute_opportunity_inner(self, payload: dict) -> None:
        """Inner implementation (lock already held by caller)."""
        from core.trade_executor import TradeLeg, LegSide

        direction = str(payload.get("direction", ""))
        trade_usdt = float(payload.get("trade_size_usdt", 0))
        profit_usd = float(payload.get("profit_usd", 0))
        if trade_usdt <= 0:
            return

        # ── Cooldown guard ──
        now = time.time()
        elapsed_since_last = now - self._last_trade_ts
        if elapsed_since_last < self._trade_cooldown_s:
            logger.debug(
                f"Trade cooldown: {self._trade_cooldown_s - elapsed_since_last:.1f}s "
                f"remaining — skipping {direction}"
            )
            return

        # ── Execution risk buffer ──
        # Subtract a safety margin from detected profit to account for
        # slippage, latency, partial fills, and price movement.
        risk_buffer_pct = settings.execution_risk_buffer_pct
        if risk_buffer_pct > 0 and trade_usdt > 0:
            buffer_usd = trade_usdt * (risk_buffer_pct / 100.0)
            profit_after_buffer = profit_usd - buffer_usd
            if profit_after_buffer <= 0:
                logger.debug(
                    f"⛔ Risk buffer killed profit: ${profit_usd:.4f} "
                    f"- ${buffer_usd:.4f} buffer ({risk_buffer_pct}%) = "
                    f"${profit_after_buffer:.4f} — skipping {direction}"
                )
                return
            logger.debug(
                f"🛡️ Risk buffer: ${profit_usd:.4f} → ${profit_after_buffer:.4f} "
                f"(−${buffer_usd:.4f}, {risk_buffer_pct}%) for {direction}"
            )
            profit_usd = profit_after_buffer

        ami_sym = settings.cex_symbol        # AMIUSDT
        apt_sym = settings.apt_cex_symbol    # APTUSDT
        legs: list = []
        involves_dex = True
        dex_swap_dir = ""    # "apt_to_ami" or "ami_to_apt"
        dex_swap_amt = 0.0   # amount to swap on DEX

        try:
            # ── CEX-to-CEX APT (same token, 2 exchanges) ──
            if direction.startswith("CEX_TO_CEX_APT_"):
                involves_dex = False
                buy_ex = str(payload.get("buy_exchange", "")).lower()
                sell_ex = str(payload.get("sell_exchange", "")).lower()
                buy_price = float(payload.get(f"{buy_ex}_apt_ask", 0))
                sell_price = float(payload.get(f"{sell_ex}_apt_bid", 0))
                if buy_price <= 0 or sell_price <= 0:
                    return
                qty = trade_usdt / buy_price
                legs = [
                    TradeLeg(buy_ex, apt_sym, LegSide.BUY, qty, buy_price, f"buy_APT_{buy_ex}"),
                    TradeLeg(sell_ex, apt_sym, LegSide.SELL, qty, sell_price, f"sell_APT_{sell_ex}"),
                ]

            # ── CEX-to-CEX AMI (same token, 2 exchanges) ──
            elif direction.startswith("CEX_TO_CEX_"):
                involves_dex = False
                buy_ex = str(payload.get("buy_exchange", "")).lower()
                sell_ex = str(payload.get("sell_exchange", "")).lower()
                buy_price = float(payload.get("buy_price", 0))
                sell_price = float(payload.get("sell_price", 0))
                if buy_price <= 0 or sell_price <= 0:
                    return
                qty = trade_usdt / buy_price
                legs = [
                    TradeLeg(buy_ex, ami_sym, LegSide.BUY, qty, buy_price, f"buy_AMI_{buy_ex}"),
                    TradeLeg(sell_ex, ami_sym, LegSide.SELL, qty, sell_price, f"sell_AMI_{sell_ex}"),
                ]

            # ── DEX_TO_CEX: buy APT(CEX) + sell AMI(CEX) → DEX swap APT→AMI ──
            elif direction.startswith("DEX_TO_CEX"):
                buy_ex = str(payload.get("buy_exchange", "")).lower()
                sell_ex = str(payload.get("sell_exchange", "")).lower()
                buy_price = float(payload.get("buy_apt_ask", payload.get("apt_ask", payload.get("buy_price", 0))))
                sell_price = float(payload.get("sell_ami_bid", payload.get("cex_bid", payload.get("sell_price", 0))))
                if buy_price <= 0 or sell_price <= 0:
                    return
                apt_qty = trade_usdt / buy_price
                ami_qty = self._swap_apt_to_ami(apt_qty) if self.cellana_reserve_ami else 0
                if ami_qty <= 0:
                    ami_qty = trade_usdt / sell_price
                legs = [
                    TradeLeg(buy_ex, apt_sym, LegSide.BUY, apt_qty, buy_price, f"buy_APT_{buy_ex}"),
                    TradeLeg(sell_ex, ami_sym, LegSide.SELL, ami_qty, sell_price, f"sell_AMI_{sell_ex}"),
                ]
                dex_swap_dir = "apt_to_ami"
                dex_swap_amt = apt_qty

            # ── AMI_CYCLE: buy AMI(CEX) + sell APT(CEX) → DEX swap AMI→APT ──
            elif direction.startswith("AMI_CYCLE"):
                buy_ex = str(payload.get("buy_exchange", "")).lower()
                sell_ex = str(payload.get("sell_exchange", "")).lower()
                buy_price = float(payload.get("buy_cex_ask", payload.get("cex_ask", payload.get("buy_price", 0))))
                sell_price = float(payload.get("sell_apt_bid", payload.get("apt_bid", payload.get("sell_price", 0))))
                if buy_price <= 0 or sell_price <= 0:
                    return
                ami_qty = trade_usdt / buy_price
                apt_qty = self._swap_ami_to_apt(ami_qty) if self.cellana_reserve_apt else 0
                if apt_qty <= 0:
                    apt_qty = trade_usdt / sell_price
                legs = [
                    TradeLeg(buy_ex, ami_sym, LegSide.BUY, ami_qty, buy_price, f"buy_AMI_{buy_ex}"),
                    TradeLeg(sell_ex, apt_sym, LegSide.SELL, apt_qty, sell_price, f"sell_APT_{sell_ex}"),
                ]
                dex_swap_dir = "ami_to_apt"
                dex_swap_amt = ami_qty

            # ── APT_START / AMI_START: sell AMI + buy APT (CEX) → DEX swap APT→AMI ──
            elif direction.startswith("APT_START") or direction.startswith("AMI_START"):
                sell_ex = str(payload.get("sell_ami_exchange", payload.get("sell_exchange", payload.get("exchange", "")))).lower()
                buy_ex = str(payload.get("buy_apt_exchange", payload.get("buy_exchange", payload.get("exchange", "")))).lower()
                sell_price = float(payload.get("sell_ami_bid", payload.get("cex_bid", 0)))
                buy_price = float(payload.get("buy_apt_ask", payload.get("apt_ask", 0)))
                if buy_price <= 0 or sell_price <= 0:
                    return
                ami_qty = trade_usdt / sell_price
                apt_qty = trade_usdt / buy_price
                legs = [
                    TradeLeg(sell_ex, ami_sym, LegSide.SELL, ami_qty, sell_price, f"sell_AMI_{sell_ex}"),
                    TradeLeg(buy_ex, apt_sym, LegSide.BUY, apt_qty, buy_price, f"buy_APT_{buy_ex}"),
                ]
                dex_swap_dir = "apt_to_ami"
                dex_swap_amt = apt_qty

            # ── APT_REVERSE / AMI_REVERSE: sell APT + buy AMI (CEX) → DEX swap AMI→APT ──
            elif direction.startswith("APT_REVERSE") or direction.startswith("AMI_REVERSE"):
                sell_ex = str(payload.get("sell_apt_exchange", payload.get("sell_exchange", payload.get("exchange", "")))).lower()
                buy_ex = str(payload.get("buy_ami_exchange", payload.get("buy_exchange", payload.get("exchange", "")))).lower()
                sell_price = float(payload.get("sell_apt_bid", payload.get("apt_bid", 0)))
                buy_price = float(payload.get("buy_ami_ask", payload.get("cex_ask", 0)))
                if sell_price <= 0 or buy_price <= 0:
                    return
                apt_qty = trade_usdt / sell_price
                ami_qty = trade_usdt / buy_price
                legs = [
                    TradeLeg(sell_ex, apt_sym, LegSide.SELL, apt_qty, sell_price, f"sell_APT_{sell_ex}"),
                    TradeLeg(buy_ex, ami_sym, LegSide.BUY, ami_qty, buy_price, f"buy_AMI_{buy_ex}"),
                ]
                dex_swap_dir = "ami_to_apt"
                dex_swap_amt = ami_qty

            else:
                logger.warning(f"⚠️ Unknown direction for execution: {direction}")
                return

            if not legs:
                return

            # ── Mark trade timestamp (for cooldown) ──
            self._last_trade_ts = time.time()

            # ────────────────────────────────────────────────────────────
            # STEP 1 — Execute CEX legs (fast, cheap to fail)
            # ────────────────────────────────────────────────────────────
            cex_result = await self.trade_executor.execute_multi_leg(
                legs=legs,
                direction=direction,
                profit_est=profit_usd,
                parallel=True,  # CEX legs run in parallel (pre-funded model)
            )

            # CEX failed or dry-run → done
            if not cex_result.ok:
                return

            # No DEX leg needed (CEX-CEX only) → done
            if not involves_dex or not dex_swap_dir or dex_swap_amt <= 0:
                return

            # ────────────────────────────────────────────────────────────
            # STEP 2 — Execute DEX swap on-chain (with retry)
            # ────────────────────────────────────────────────────────────
            if self.trade_executor.dry_run:
                logger.info(
                    f"[DRY-SIGNAL] DEX {dex_swap_dir} "
                    f"amount={dex_swap_amt:.6f} — skipped (dry run)"
                )
                return

            # Pre-check: enough APT for gas?
            gas_ok = await self._check_gas_balance(direction)
            if not gas_ok:
                # Not enough gas → rollback CEX immediately (no retry)
                logger.error(
                    f"❌ Insufficient gas APT for DEX swap — "
                    f"rolling back CEX legs for {direction}"
                )
                succeeded_legs = [lr for lr in cex_result.legs if lr.ok]
                await self.trade_executor._rollback_legs(succeeded_legs)
                log_signal({
                    "type": "DEX_SKIP_NO_GAS",
                    "direction": direction,
                    "dex_direction": dex_swap_dir,
                })
                return

            dex_ok = await self._execute_dex_with_retry(
                dex_direction=dex_swap_dir,
                dex_amount=dex_swap_amt,
                direction=direction,
                max_retries=settings.dex_swap_max_retries,
                retry_delay_s=settings.dex_swap_retry_delay_s,
            )

            # ────────────────────────────────────────────────────────────
            # STEP 3 — DEX failed after retries → rollback CEX legs
            # ────────────────────────────────────────────────────────────
            if not dex_ok:
                logger.error(
                    f"❌ DEX {dex_swap_dir} exhausted retries — "
                    f"rolling back CEX legs for {direction}"
                )
                succeeded_legs = [lr for lr in cex_result.legs if lr.ok]
                rollback_results = await self.trade_executor._rollback_legs(
                    succeeded_legs
                )
                rollback_ok = (
                    all(r.ok for r in rollback_results) if rollback_results else True
                )
                log_signal({
                    "type": "DEX_FAIL_CEX_ROLLBACK",
                    "direction": direction,
                    "dex_direction": dex_swap_dir,
                    "dex_amount": dex_swap_amt,
                    "rollback_ok": rollback_ok,
                    "profit_est": profit_usd,
                })
                if not rollback_ok:
                    logger.error(
                        f"🚨 ROLLBACK PARTIALLY FAILED for {direction} — "
                        f"MANUAL INTERVENTION MAY BE REQUIRED"
                    )

        except Exception as e:
            logger.error(f"Trade execution error for {direction}: {e}")

    # ------------------------------------------------------------------ #
    #  DEX swap with retry + price re-check
    # ------------------------------------------------------------------ #
    async def _execute_dex_with_retry(
        self,
        dex_direction: str,   # "apt_to_ami" or "ami_to_apt"
        dex_amount: float,
        direction: str,       # cycle name for logging
        max_retries: int = 2,
        retry_delay_s: float = 1.0,
    ) -> bool:
        """Execute on-chain DEX swap. On failure, re-quote price and retry.

        Flow per attempt:
          1. (after 1st) Re-quote DEX → if quote=0 or error → abort
          2. (after 1st) Sleep retry_delay_s
          3. Execute swap
          4. If success → return True
          5. If fail → loop to next attempt

        Returns True if swap succeeded within budget, False otherwise.
        """
        dex = getattr(self.trade_executor, "dex_swap", None)
        if not dex:
            logger.error(
                "CellanaDexSwap not initialised — DEX leg skipped for "
                f"{direction}"
            )
            return False

        total_attempts = 1 + max_retries

        for attempt in range(total_attempts):
            # ── Re-check price before retry (skip first attempt) ──
            if attempt > 0:
                try:
                    if dex_direction == "apt_to_ami":
                        quote = await dex.get_amount_out_apt_to_ami(dex_amount)
                        label = f"{dex_amount:.6f} APT → {quote:.4f} AMI"
                    else:
                        quote = await dex.get_amount_out_ami_to_apt(dex_amount)
                        label = f"{dex_amount:.4f} AMI → {quote:.6f} APT"

                    if quote <= 0:
                        logger.warning(
                            f"DEX re-quote=0 for {direction} — aborting retry"
                        )
                        break

                    logger.info(
                        f"🔄 DEX retry {attempt}/{max_retries} for "
                        f"{direction} | re-quote: {label}"
                    )
                except Exception as e:
                    logger.warning(
                        f"DEX re-quote failed for {direction} ({e}) — "
                        f"aborting retry"
                    )
                    break

                await asyncio.sleep(retry_delay_s)

            # ── Execute DEX swap (with timeout) ──
            try:
                if dex_direction == "apt_to_ami":
                    result = await asyncio.wait_for(
                        dex.swap_apt_to_ami(dex_amount),
                        timeout=120,
                    )
                else:
                    result = await asyncio.wait_for(
                        dex.swap_ami_to_apt(dex_amount),
                        timeout=120,
                    )

                if result.ok:
                    logger.success(
                        f"✅ DEX {dex_direction} OK for {direction} | "
                        f"tx={result.tx_hash[:16]}… | "
                        f"in={dex_amount:.6f} out={result.amount_out:.6f} | "
                        f"gas={result.gas_used:.6f} APT | "
                        f"attempt={attempt + 1}/{total_attempts} | "
                        f"{result.elapsed_ms:.0f}ms"
                    )
                    log_signal({
                        "type": "DEX_SWAP_OK",
                        "direction": direction,
                        "dex_direction": dex_direction,
                        "amount_in": dex_amount,
                        "amount_out": result.amount_out,
                        "tx_hash": result.tx_hash,
                        "gas_apt": result.gas_used,
                        "attempt": attempt + 1,
                    })
                    return True
                else:
                    logger.warning(
                        f"❌ DEX {dex_direction} failed "
                        f"(attempt {attempt + 1}/{total_attempts}) for "
                        f"{direction}: {result.error}"
                    )
            except asyncio.TimeoutError:
                logger.error(
                    f"DEX swap TIMEOUT (120s) "
                    f"(attempt {attempt + 1}/{total_attempts}) for "
                    f"{direction}"
                )
            except Exception as e:
                logger.error(
                    f"DEX swap exception "
                    f"(attempt {attempt + 1}/{total_attempts}) for "
                    f"{direction}: {e}"
                )

        # All attempts exhausted
        log_signal({
            "type": "DEX_SWAP_EXHAUSTED",
            "direction": direction,
            "dex_direction": dex_direction,
            "dex_amount": dex_amount,
            "total_attempts": total_attempts,
        })
        return False

    async def _check_gas_balance(self, direction: str) -> bool:
        """Check if wallet has enough APT for DEX gas fees.

        Returns True if balance >= min_gas_apt, False otherwise.
        """
        if self._min_gas_apt <= 0:
            return True  # check disabled

        dex = getattr(self.trade_executor, "dex_swap", None)
        if not dex:
            return False

        try:
            balance = await dex.get_apt_balance()
            if balance < self._min_gas_apt:
                logger.warning(
                    f"⛽ APT gas balance too low: {balance:.6f} APT "
                    f"< {self._min_gas_apt:.6f} min — {direction}"
                )
                return False
            return True
        except Exception as e:
            logger.warning(f"Gas balance check failed ({e}) — assuming OK")
            return True  # don't block trade on balance-check errors

    def check_arbitrage_for_exchange(
        self,
        exchange: str,
        cex_price_data: PriceData,
        apt_price_data: Optional[PriceData] = None,
        override_max_trade: float = 0,
    ) -> None:
        """Check DEX↔CEX arbitrage independently for one exchange.

        Args:
            exchange: Exchange name ("bybit" or "mexc")
            cex_price_data: AMIUSDT price data for selected exchange
            apt_price_data: Optional APTUSDT price data for selected exchange
            override_max_trade: If > 0, use this as max trade size instead
                of ``self.max_trade_usdt``.  Used for micro-size fallback.
        """
        exchange_lower = exchange.lower()

        # Update APT/USDT from the same exchange when available.
        if apt_price_data and not apt_price_data.is_stale(max_age=30):
            self.update_apt_usdt_price(apt_price_data.mid)

        # Check data freshness — REST fallback in _monitor_prices keeps
        # Bybit symbols refreshed every ~2s, so 30s is a generous limit.
        if not self.cellana_price_ami_apt:
            return
        if cex_price_data.is_stale(max_age=30):
            logger.debug(f"{exchange_lower.upper()} price too stale ({cex_price_data.age:.0f}s), skipping arb check")
            return

        cellana_price_usdt = self.get_cellana_price_in_usdt()
        if not cellana_price_usdt or not self.apt_usdt_price:
            return

        cex_ask = cex_price_data.ask  # Buy price on CEX
        cex_bid = cex_price_data.bid  # Sell price on CEX
        dex_price = cellana_price_usdt  # DEX price (assume mid-market)

        # Use APT prices from provided data if fresh
        apt_bid = apt_price_data.bid if (apt_price_data and not apt_price_data.is_stale(max_age=30)) else self.apt_usdt_price
        apt_ask = apt_price_data.ask if (apt_price_data and not apt_price_data.is_stale(max_age=30)) else self.apt_usdt_price

        # Get current fees for the selected exchange (dynamic or fallback)
        # Uses maker or taker based on USE_MAKER_FEE setting
        if exchange_lower == "bybit":
            ami_cex_fee = self._get_bybit_fee(self._fee_type)
            apt_cex_fee = self._get_bybit_fee(self._fee_type)
        else:
            ami_cex_fee = self._get_mexc_fee(self._fee_type, settings.cex_symbol)
            apt_cex_fee = self._get_mexc_fee(self._fee_type, settings.apt_cex_symbol)

        base_trade_usdt = override_max_trade if override_max_trade > 0 else self.max_trade_usdt

        # ────────────────────────────────────────────────────────────────
        #  Per-direction profit functions (closures over local prices/fees)
        #  Each returns profit_usdt for a given USDT trade size.
        # ────────────────────────────────────────────────────────────────

        # def _pf_dex_to_cex(sz: float) -> float:
        #     """USDT → buy APT(CEX) → swap APT→AMI(DEX) → sell AMI(CEX) → USDT."""
        #     if sz <= 0:
        #         return -999.0
        #     ea = apt_price_data.effective_buy_price(sz) if (apt_price_data and apt_price_data.asks) else apt_ask
        #     if ea <= 0:
        #         return -sz
        #     ab = sz / (ea * (1.0 + apt_cex_fee))
        #     ao = self._swap_apt_to_ami(ab)
        #     if ao <= 0:
        #         return -sz
        #     eb = cex_price_data.effective_sell_price(ao)
        #     return ao * eb * (1.0 - ami_cex_fee) - sz - self.gas_cost_usd

        # def _pf_ami_cycle(sz: float) -> float:
        #     """USDT → buy AMI(CEX) → swap AMI→APT(DEX) → sell APT(CEX) → USDT."""
        #     if sz <= 0:
        #         return -999.0
        #     ea = cex_price_data.effective_buy_price(sz)
        #     if ea <= 0:
        #         return -sz
        #     ab = sz / (ea * (1.0 + ami_cex_fee))
        #     ao = self._swap_ami_to_apt(ab)
        #     if ao <= 0:
        #         return -sz
        #     eb = apt_price_data.effective_sell_price(ao) if (apt_price_data and apt_price_data.bids) else apt_bid
        #     return ao * eb * (1.0 - apt_cex_fee) - sz - self.gas_cost_usd

        def _pf_apt_start(sz: float) -> float:
            """APT → AMI(DEX) → USDT(sell AMI CEX) → APT(buy CEX). Profit in USDT equiv."""
            _apt_mid = (apt_ask + apt_bid) / 2.0 if (apt_ask and apt_bid and apt_ask > 0 and apt_bid > 0) else 0.0
            if sz <= 0 or _apt_mid <= 0:
                return -999.0
            a_start = sz / _apt_mid
            ao = self._swap_apt_to_ami(a_start)
            if ao <= 0:
                return -sz
            eb = cex_price_data.effective_sell_price(ao)
            u_out = ao * eb * (1.0 - ami_cex_fee)
            ea = apt_price_data.effective_buy_price(u_out) if (apt_price_data and apt_price_data.asks) else apt_ask
            if ea <= 0:
                return -sz
            a_end = u_out / (ea * (1.0 + apt_cex_fee))
            return (a_end - a_start) * _apt_mid - self.gas_cost_usd

        def _pf_ami_start(sz: float) -> float:
            """AMI → USDT(sell AMI CEX) → APT(buy CEX) → AMI(DEX). Profit in USDT equiv."""
            _ami_mid = (cex_ask + cex_bid) / 2.0 if (cex_ask > 0 and cex_bid > 0) else 0.0
            if sz <= 0 or _ami_mid <= 0:
                return -999.0
            m_start = sz / _ami_mid
            eb = cex_price_data.effective_sell_price(m_start)
            u_out = m_start * eb * (1.0 - ami_cex_fee)
            ea = apt_price_data.effective_buy_price(u_out) if (apt_price_data and apt_price_data.asks) else apt_ask
            if ea <= 0:
                return -sz
            ab = u_out / (ea * (1.0 + apt_cex_fee))
            m_end = self._swap_apt_to_ami(ab)  # DEX: APT → AMI
            if m_end <= 0:
                return -sz
            return (m_end - m_start) * _ami_mid - self.gas_cost_usd

        def _pf_apt_reverse(sz: float) -> float:
            """APT → USDT(sell APT CEX) → AMI(buy CEX) → APT(DEX). Profit in USDT equiv."""
            _apt_mid = (apt_ask + apt_bid) / 2.0 if (apt_ask and apt_bid and apt_ask > 0 and apt_bid > 0) else 0.0
            if sz <= 0 or _apt_mid <= 0:
                return -999.0
            a_start = sz / _apt_mid
            eb = apt_price_data.effective_sell_price(a_start) if (apt_price_data and apt_price_data.bids) else apt_bid
            u_out = a_start * eb * (1.0 - apt_cex_fee)
            ea = cex_price_data.effective_buy_price(u_out)
            if ea <= 0:
                return -sz
            mb = u_out / (ea * (1.0 + ami_cex_fee))
            a_end = self._swap_ami_to_apt(mb)  # DEX: AMI → APT
            if a_end <= 0:
                return -sz
            return (a_end - a_start) * _apt_mid - self.gas_cost_usd

        def _pf_ami_reverse(sz: float) -> float:
            """AMI → APT(DEX) → USDT(sell APT CEX) → AMI(buy CEX). Profit in USDT equiv."""
            _ami_mid = (cex_ask + cex_bid) / 2.0 if (cex_ask > 0 and cex_bid > 0) else 0.0
            if sz <= 0 or _ami_mid <= 0:
                return -999.0
            m_start = sz / _ami_mid
            ao = self._swap_ami_to_apt(m_start)  # DEX: AMI → APT
            if ao <= 0:
                return -sz
            eb = apt_price_data.effective_sell_price(ao) if (apt_price_data and apt_price_data.bids) else apt_bid
            u_out = ao * eb * (1.0 - apt_cex_fee)
            ea = cex_price_data.effective_buy_price(u_out)
            if ea <= 0:
                return -sz
            m_end = u_out / (ea * (1.0 + ami_cex_fee))
            return (m_end - m_start) * _ami_mid - self.gas_cost_usd

        # ────────────────────────────────────────────────────────────────
        #  Compute max feasible size per direction (liquidity caps)
        #  + collect orderbook boundaries for qty-driven optimization
        # ────────────────────────────────────────────────────────────────

        # Pre-compute cumulative USDT boundaries from orderbook levels.
        # These are the natural points where marginal price changes.
        apt_ask_bounds = self._orderbook_cumulative_usdt(apt_price_data.asks) if (apt_price_data and apt_price_data.asks) else []
        apt_bid_bounds = self._orderbook_cumulative_qty_as_usdt(apt_price_data.bids, apt_bid or 0) if (apt_price_data and apt_price_data.bids and apt_bid) else []
        ami_ask_bounds = self._orderbook_cumulative_usdt(cex_price_data.asks) if cex_price_data.asks else []
        ami_bid_bounds = self._orderbook_cumulative_qty_as_usdt(cex_price_data.bids, cex_bid) if (cex_price_data.bids and cex_bid > 0) else []

        def _run_direction(pf_fn, max_feas: float, ob_bounds: Optional[List[float]] = None):
            """Run the optimizer for a direction, using orderbook boundaries."""
            if max_feas < self.min_trade_usdt:
                return 0.0, -999.0
            if self.optimal_size_enabled:
                sz, pf = self._find_optimal_trade_size(pf_fn, max_feas, ob_bounds)
                return sz, pf
            return max_feas, pf_fn(max_feas)

        # Merge two boundary lists (entry + exit orderbooks) for directions
        # that touch two CEX orderbooks (buy-side + sell-side).
        def _merge_bounds(*bound_lists):
            merged = set()
            for bl in bound_lists:
                merged.update(bl)
            return sorted(merged) if merged else None

        # ── Direction 1: DEX_TO_CEX (USDT → buy APT asks → DEX → sell AMI bids) ──
        profit_usdt_dex_to_cex = -999.0
        trade_usdt_dex_to_cex = 0.0

        if apt_ask and apt_ask > 0:
            max_feas = base_trade_usdt
            if apt_price_data:
                max_feas = self._calculate_trade_size_usdt(max_feas, apt_price_data, side="buy")
            if cex_price_data.bid_qty > 0:
                max_feas = min(
                    max_feas,
                    self._cap_trade_usdt_by_downstream_qty(
                        max_feas, apt_ask, apt_cex_fee,
                        cex_price_data.bid_qty, self._swap_apt_to_ami,
                    ),
                )
            # Boundaries: APT ask levels (entry) + AMI bid levels (exit)
            bounds = _merge_bounds(apt_ask_bounds, ami_bid_bounds)
            # trade_usdt_dex_to_cex, profit_usdt_dex_to_cex = _run_direction(
            #     _pf_dex_to_cex, max_feas, bounds)

        # ── Direction 2: AMI_CYCLE (USDT → buy AMI asks → DEX → sell APT bids) ──
        profit_usdt_ami_cycle = -999.0
        trade_usdt_ami_cycle = 0.0

        if cex_ask > 0 and apt_bid and apt_bid > 0:
            max_feas = self._calculate_trade_size_usdt(
                base_trade_usdt, cex_price_data, side="buy")
            if apt_price_data and apt_price_data.bid_qty > 0:
                max_feas = min(
                    max_feas,
                    self._cap_trade_usdt_by_downstream_qty(
                        max_feas, cex_ask, ami_cex_fee,
                        apt_price_data.bid_qty, self._swap_ami_to_apt,
                    ),
                )
            bounds = _merge_bounds(ami_ask_bounds, apt_bid_bounds)
            # trade_usdt_ami_cycle, profit_usdt_ami_cycle = _run_direction(
            #     _pf_ami_cycle, max_feas, bounds)

        # ── Direction 3: APT_START — APT → AMI(DEX) → sell AMI bids → buy APT asks ──
        profit_usdt_apt_start = -999.0
        trade_usdt_apt_start = 0.0

        if apt_ask and apt_ask > 0 and apt_bid and apt_bid > 0:
            apt_mid = (apt_ask + apt_bid) / 2.0
            ami_mid_3 = (cex_ask + cex_bid) / 2.0 if (cex_ask > 0 and cex_bid > 0) else 0.0
            if apt_mid > 0 and ami_mid_3 > 0:
                max_feas = base_trade_usdt
                if cex_price_data.bid_qty > 0:
                    max_ami = cex_price_data.bid_qty * ami_mid_3
                    max_feas = min(max_feas, max_ami)
                bounds = _merge_bounds(ami_bid_bounds, apt_ask_bounds)
                trade_usdt_apt_start, profit_usdt_apt_start = _run_direction(
                    _pf_apt_start, max_feas, bounds)

        # ── Direction 4: AMI_START — sell AMI bids → buy APT asks → DEX → AMI ──
        profit_usdt_ami_start = -999.0
        trade_usdt_ami_start = 0.0

        if cex_bid > 0 and apt_ask and apt_ask > 0:
            ami_mid_4 = (cex_ask + cex_bid) / 2.0 if cex_ask > 0 else cex_bid
            if ami_mid_4 > 0:
                max_feas = base_trade_usdt
                if cex_price_data.bid_qty > 0:
                    max_feas = min(max_feas, cex_price_data.bid_qty * ami_mid_4)
                bounds = _merge_bounds(ami_bid_bounds, apt_ask_bounds)
                trade_usdt_ami_start, profit_usdt_ami_start = _run_direction(
                    _pf_ami_start, max_feas, bounds)

        # ── Direction 5: APT_REVERSE — sell APT bids → buy AMI asks → DEX → APT ──
        profit_usdt_apt_reverse = -999.0
        trade_usdt_apt_reverse = 0.0

        if apt_bid and apt_bid > 0 and cex_ask > 0:
            apt_mid_5 = (apt_ask + apt_bid) / 2.0 if (apt_ask and apt_ask > 0) else apt_bid
            if apt_mid_5 > 0:
                max_feas = base_trade_usdt
                if apt_price_data and apt_price_data.bid_qty > 0:
                    max_feas = min(max_feas, apt_price_data.bid_qty * apt_mid_5)
                bounds = _merge_bounds(apt_bid_bounds, ami_ask_bounds)
                trade_usdt_apt_reverse, profit_usdt_apt_reverse = _run_direction(
                    _pf_apt_reverse, max_feas, bounds)

        # ── Direction 6: AMI_REVERSE — DEX AMI→APT → sell APT bids → buy AMI asks ──
        profit_usdt_ami_reverse = -999.0
        trade_usdt_ami_reverse = 0.0

        if cex_ask > 0 and apt_bid and apt_bid > 0:
            ami_mid_6 = (cex_ask + cex_bid) / 2.0 if cex_bid > 0 else cex_ask
            if ami_mid_6 > 0:
                max_feas = base_trade_usdt
                if apt_price_data and apt_price_data.bid_qty > 0:
                    apt_mid_6 = (apt_ask + apt_bid) / 2.0 if (apt_ask and apt_ask > 0) else apt_bid
                    max_feas = min(max_feas, apt_price_data.bid_qty * apt_mid_6)
                bounds = _merge_bounds(apt_bid_bounds, ami_ask_bounds)
                trade_usdt_ami_reverse, profit_usdt_ami_reverse = _run_direction(
                    _pf_ami_reverse, max_feas, bounds)

        # Log prices periodically (per-exchange timer)
        now = time.time()
        last_ts = self._last_price_log.get(exchange_lower, 0.0)
        if now - last_ts >= self._PRICE_LOG_INTERVAL_S:
            # Compute CEX-implied AMI/APT vs DEX rate for visibility
            cex_implied = cex_ask / apt_bid if (apt_bid and apt_bid > 0) else 0.0
            dex_rate = self.cellana_price_ami_apt or 0.0
            gap_pct = ((dex_rate / cex_implied) - 1.0) * 100 if cex_implied > 0 else 0.0
            # Best profit across all 6 directions
            all_profits = [
                ("D2C", profit_usdt_dex_to_cex, trade_usdt_dex_to_cex),
                ("AMI", profit_usdt_ami_cycle, trade_usdt_ami_cycle),
                ("APS", profit_usdt_apt_start, trade_usdt_apt_start),
                ("AMS", profit_usdt_ami_start, trade_usdt_ami_start),
                ("APR", profit_usdt_apt_reverse, trade_usdt_apt_reverse),
                ("AMR", profit_usdt_ami_reverse, trade_usdt_ami_reverse),
            ]
            best_tag, best_pf, best_sz = max(all_profits, key=lambda x: x[1])
            best_pct = (best_pf / best_sz * 100) if best_sz > 0 else 0.0
            logger.info(
                f"💹 [{exchange_lower.upper()}] "
                f"DEX={dex_rate:.8f} CEX_impl={cex_implied:.8f} gap={gap_pct:+.3f}%  "
                f"AMI a/b={cex_ask:.6f}/{cex_bid:.6f} "
                f"APT a/b={apt_ask:.4f}/{apt_bid:.4f}  "
                f"best={best_tag} ${best_pf:+.4f}({best_pct:+.2f}%) @${best_sz:.0f}"
            )
            self._last_price_log[exchange_lower] = now

        # ── Check thresholds and log opportunities (USD AND %, with dedup) ──
        pct_dex_to_cex = (profit_usdt_dex_to_cex / trade_usdt_dex_to_cex * 100) if trade_usdt_dex_to_cex > 0 else 0
        if profit_usdt_dex_to_cex > self.min_profit_dex_to_cex and pct_dex_to_cex > self.min_profit_pct_dex_to_cex:
            # De-duplicate: only log if not recently logged with similar prices
            if self.deduplicator.should_log("DEX_TO_CEX", apt_ask, cex_bid):
                profit_pct = pct_dex_to_cex
                det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                logger.success(
                    f"🎯 [{exchange_lower.upper()}] DEX→CEX ARB FOUND @ {det_time}  "
                    f"buy_apt={apt_ask:.6f}  sell_ami={cex_bid:.6f}  "
                    f"profit=${profit_usdt_dex_to_cex:.4f} ({profit_pct:.2f}%)  size=${trade_usdt_dex_to_cex:.2f}"
                )
                log_signal({
                    "type": "dex_to_cex",
                    "buy_exchange": exchange_lower,  # Buy APT here
                    "sell_exchange": exchange_lower,  # Sell AMI here
                    "buy_price": apt_ask,
                    "sell_price": cex_bid,
                    "profit_usd": profit_usdt_dex_to_cex,
                    "profit_pct": profit_pct,
                    "trade_size_usdt": trade_usdt_dex_to_cex,
                })
                self._log_and_execute({
                    "direction": "DEX_TO_CEX",
                    "buy_exchange": exchange_lower,  # Step 1: buy APT on this CEX
                    "sell_exchange": exchange_lower,  # Step 3: sell AMI on this CEX
                    "buy_price": apt_ask,
                    "sell_price": cex_bid,
                    "profit_usd": profit_usdt_dex_to_cex,
                    "profit_token": "USDT",
                    "profit_amount": profit_usdt_dex_to_cex,
                    "profit_pct": profit_pct,
                    "trade_size_usdt": trade_usdt_dex_to_cex,
                    "dex_price_ami_apt": self.cellana_price_ami_apt,
                    "dex_price_usdt": cellana_price_usdt,
                    "cex_bid": cex_bid,
                    "cex_ask": cex_ask,
                    "apt_usdt_rate": self.apt_usdt_price,
                    "apt_bid": apt_bid,
                    "apt_ask": apt_ask,
                    "dex_fee": self.cellana_fee,
                    "cex_fee": ami_cex_fee,
                    "apt_cex_fee": apt_cex_fee,
                    "pool_reserve_ami": self.cellana_reserve_ami,
                    "pool_reserve_apt": self.cellana_reserve_apt,
                    "ami_bid_qty": cex_price_data.bid_qty,
                    "ami_ask_qty": cex_price_data.ask_qty,
                })

        pct_ami_cycle = (profit_usdt_ami_cycle / trade_usdt_ami_cycle * 100) if trade_usdt_ami_cycle > 0 else 0
        if profit_usdt_ami_cycle > self.min_profit_ami_cycle and pct_ami_cycle > self.min_profit_pct_ami_cycle:
            # De-duplicate
            if self.deduplicator.should_log(f"AMI_CYCLE_{exchange_lower.upper()}", cex_ask, apt_bid):
                profit_pct = pct_ami_cycle
                det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                logger.success(
                    f"🎯 [{exchange_lower.upper()}] AMI CYCLE ARB FOUND @ {det_time}  "
                    f"buy_ami={cex_ask:.6f}  sell_apt={apt_bid:.6f}  "
                    f"profit=${profit_usdt_ami_cycle:.4f} ({profit_pct:.2f}%)  size=${trade_usdt_ami_cycle:.2f}"
                )
                log_signal({
                    "type": "ami_cycle",
                    "buy_exchange": exchange_lower,
                    "sell_exchange": exchange_lower,
                    "buy_price": cex_ask,
                    "sell_price": apt_bid,
                    "profit_usd": profit_usdt_ami_cycle,
                    "profit_pct": profit_pct,
                    "trade_size_usdt": trade_usdt_ami_cycle,
                })
                self._log_and_execute({
                    "direction": "AMI_CYCLE",
                    "exchange": exchange_lower,
                    "buy_exchange": exchange_lower,
                    "sell_exchange": exchange_lower,
                    "profit_usd": profit_usdt_ami_cycle,
                    "profit_pct": profit_pct,
                    "trade_size_usdt": trade_usdt_ami_cycle,
                    "cex_ask": cex_ask,
                    "apt_bid": apt_bid,
                    "cex_fee": ami_cex_fee,
                    "apt_cex_fee": apt_cex_fee,
                    "dex_price_ami_apt": self.cellana_price_ami_apt,
                    "dex_price_usdt": cellana_price_usdt,
                    "apt_usdt_rate": self.apt_usdt_price,
                    "dex_fee": self.cellana_fee,
                    "pool_reserve_ami": self.cellana_reserve_ami,
                    "pool_reserve_apt": self.cellana_reserve_apt,
                    "ami_bid_qty": cex_price_data.bid_qty,
                    "ami_ask_qty": cex_price_data.ask_qty,
                })

        # ── APT_START: APT → AMI(DEX) → USDT(CEX) → APT(CEX) — profit in APT ──
        pct_apt_start = (profit_usdt_apt_start / trade_usdt_apt_start * 100) if trade_usdt_apt_start > 0 else 0
        if profit_usdt_apt_start > self.min_profit_apt_start and pct_apt_start > self.min_profit_pct_apt_start:
            if self.deduplicator.should_log(f"APT_START_{exchange_lower.upper()}", apt_ask or 0, cex_bid):
                profit_pct = pct_apt_start
                det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                apt_mid_log = ((apt_ask or 0) + (apt_bid or 0)) / 2.0
                apt_profit = profit_usdt_apt_start / apt_mid_log if apt_mid_log > 0 else 0
                logger.success(
                    f"🎯 [{exchange_lower.upper()}] APT START ARB FOUND @ {det_time}  "
                    f"APT→AMI(DEX)→USDT→APT  "
                    f"profit={apt_profit:.6f} APT (${profit_usdt_apt_start:.4f}, {profit_pct:.2f}%)  "
                    f"size=${trade_usdt_apt_start:.2f}"
                )
                log_signal({
                    "type": "apt_start_cycle",
                    "buy_exchange": exchange_lower,
                    "sell_exchange": exchange_lower,
                    "buy_price": apt_ask,
                    "sell_price": cex_bid,
                    "profit_usd": profit_usdt_apt_start,
                    "profit_pct": profit_pct,
                    "profit_apt": apt_profit,
                    "trade_size_usdt": trade_usdt_apt_start,
                })
                self._log_and_execute({
                    "direction": "APT_START_CYCLE",
                    "exchange": exchange_lower,
                    "start_token": "APT",
                    "profit_usd": profit_usdt_apt_start,
                    "profit_token": "APT",
                    "profit_amount": apt_profit,
                    "profit_pct": profit_pct,
                    "trade_size_usdt": trade_usdt_apt_start,
                    "cex_bid": cex_bid,
                    "apt_ask": apt_ask,
                    "apt_bid": apt_bid,
                    "cex_fee": ami_cex_fee,
                    "apt_cex_fee": apt_cex_fee,
                    "dex_price_ami_apt": self.cellana_price_ami_apt,
                    "dex_price_usdt": cellana_price_usdt,
                    "apt_usdt_rate": self.apt_usdt_price,
                    "dex_fee": self.cellana_fee,
                    "pool_reserve_ami": self.cellana_reserve_ami,
                    "pool_reserve_apt": self.cellana_reserve_apt,
                })

        # ── AMI_START: AMI → USDT(CEX) → APT(CEX) → AMI(DEX) — profit in AMI ──
        pct_ami_start = (profit_usdt_ami_start / trade_usdt_ami_start * 100) if trade_usdt_ami_start > 0 else 0
        if profit_usdt_ami_start > self.min_profit_ami_start and pct_ami_start > self.min_profit_pct_ami_start:
            if self.deduplicator.should_log(f"AMI_START_{exchange_lower.upper()}", cex_bid, apt_ask or 0):
                profit_pct = pct_ami_start
                det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                ami_mid_log = (cex_ask + cex_bid) / 2.0 if cex_ask > 0 else cex_bid
                ami_profit = profit_usdt_ami_start / ami_mid_log if ami_mid_log > 0 else 0
                logger.success(
                    f"🎯 [{exchange_lower.upper()}] AMI START ARB FOUND @ {det_time}  "
                    f"AMI→USDT→APT(CEX)→AMI(DEX)  "
                    f"profit={ami_profit:.2f} AMI (${profit_usdt_ami_start:.4f}, {profit_pct:.2f}%)  "
                    f"size=${trade_usdt_ami_start:.2f}"
                )
                log_signal({
                    "type": "ami_start_cycle",
                    "buy_exchange": exchange_lower,
                    "sell_exchange": exchange_lower,
                    "buy_price": cex_bid,
                    "sell_price": apt_ask,
                    "profit_usd": profit_usdt_ami_start,
                    "profit_pct": profit_pct,
                    "profit_ami": ami_profit,
                    "trade_size_usdt": trade_usdt_ami_start,
                })
                self._log_and_execute({
                    "direction": "AMI_START_CYCLE",
                    "exchange": exchange_lower,
                    "start_token": "AMI",
                    "profit_usd": profit_usdt_ami_start,
                    "profit_token": "AMI",
                    "profit_amount": ami_profit,
                    "profit_pct": profit_pct,
                    "trade_size_usdt": trade_usdt_ami_start,
                    "cex_bid": cex_bid,
                    "cex_ask": cex_ask,
                    "apt_ask": apt_ask,
                    "apt_bid": apt_bid,
                    "cex_fee": ami_cex_fee,
                    "apt_cex_fee": apt_cex_fee,
                    "dex_price_ami_apt": self.cellana_price_ami_apt,
                    "dex_price_usdt": cellana_price_usdt,
                    "apt_usdt_rate": self.apt_usdt_price,
                    "dex_fee": self.cellana_fee,
                    "pool_reserve_ami": self.cellana_reserve_ami,
                    "pool_reserve_apt": self.cellana_reserve_apt,
                })

        # ── APT_REVERSE: APT → USDT(sell APT) → AMI(buy CEX) → APT(DEX AMI→APT) — profit in APT ──
        pct_apt_reverse = (profit_usdt_apt_reverse / trade_usdt_apt_reverse * 100) if trade_usdt_apt_reverse > 0 else 0
        if profit_usdt_apt_reverse > self.min_profit_apt_start and pct_apt_reverse > self.min_profit_pct_apt_start:
            if self.deduplicator.should_log(f"APT_REVERSE_{exchange_lower.upper()}", apt_bid or 0, cex_ask):
                profit_pct = pct_apt_reverse
                det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                apt_mid_log = ((apt_ask or 0) + (apt_bid or 0)) / 2.0
                apt_profit = profit_usdt_apt_reverse / apt_mid_log if apt_mid_log > 0 else 0
                logger.success(
                    f"🎯 [{exchange_lower.upper()}] APT REVERSE ARB FOUND @ {det_time}  "
                    f"APT→USDT→AMI(CEX)→APT(DEX)  "
                    f"profit={apt_profit:.6f} APT (${profit_usdt_apt_reverse:.4f}, {profit_pct:.2f}%)  "
                    f"size=${trade_usdt_apt_reverse:.2f}"
                )
                log_signal({
                    "type": "apt_reverse_cycle",
                    "buy_exchange": exchange_lower,
                    "sell_exchange": exchange_lower,
                    "buy_price": apt_bid,
                    "sell_price": cex_ask,
                    "profit_usd": profit_usdt_apt_reverse,
                    "profit_pct": profit_pct,
                    "profit_apt": apt_profit,
                    "trade_size_usdt": trade_usdt_apt_reverse,
                })
                self._log_and_execute({
                    "direction": "APT_REVERSE_CYCLE",
                    "exchange": exchange_lower,
                    "start_token": "APT",
                    "dex_direction": "AMI→APT",
                    "profit_usd": profit_usdt_apt_reverse,
                    "profit_token": "APT",
                    "profit_amount": apt_profit,
                    "profit_pct": profit_pct,
                    "trade_size_usdt": trade_usdt_apt_reverse,
                    "cex_ask": cex_ask,
                    "apt_bid": apt_bid,
                    "apt_ask": apt_ask,
                    "cex_fee": ami_cex_fee,
                    "apt_cex_fee": apt_cex_fee,
                    "dex_price_ami_apt": self.cellana_price_ami_apt,
                    "dex_price_usdt": cellana_price_usdt,
                    "apt_usdt_rate": self.apt_usdt_price,
                    "dex_fee": self.cellana_fee,
                    "pool_reserve_ami": self.cellana_reserve_ami,
                    "pool_reserve_apt": self.cellana_reserve_apt,
                })

        # ── AMI_REVERSE: AMI → APT(DEX AMI→APT) → USDT(sell APT) → AMI(buy CEX) — profit in AMI ──
        pct_ami_reverse = (profit_usdt_ami_reverse / trade_usdt_ami_reverse * 100) if trade_usdt_ami_reverse > 0 else 0
        if profit_usdt_ami_reverse > self.min_profit_ami_start and pct_ami_reverse > self.min_profit_pct_ami_start:
            if self.deduplicator.should_log(f"AMI_REVERSE_{exchange_lower.upper()}", cex_ask, apt_bid or 0):
                profit_pct = pct_ami_reverse
                det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                ami_mid_log = (cex_ask + cex_bid) / 2.0 if cex_bid > 0 else cex_ask
                ami_profit = profit_usdt_ami_reverse / ami_mid_log if ami_mid_log > 0 else 0
                logger.success(
                    f"🎯 [{exchange_lower.upper()}] AMI REVERSE ARB FOUND @ {det_time}  "
                    f"AMI→APT(DEX)→USDT→AMI(CEX)  "
                    f"profit={ami_profit:.2f} AMI (${profit_usdt_ami_reverse:.4f}, {profit_pct:.2f}%)  "
                    f"size=${trade_usdt_ami_reverse:.2f}"
                )
                log_signal({
                    "type": "ami_reverse_cycle",
                    "buy_exchange": exchange_lower,
                    "sell_exchange": exchange_lower,
                    "buy_price": cex_ask,
                    "sell_price": apt_bid,
                    "profit_usd": profit_usdt_ami_reverse,
                    "profit_pct": profit_pct,
                    "profit_ami": ami_profit,
                    "trade_size_usdt": trade_usdt_ami_reverse,
                })
                self._log_and_execute({
                    "direction": "AMI_REVERSE_CYCLE",
                    "exchange": exchange_lower,
                    "start_token": "AMI",
                    "dex_direction": "AMI→APT",
                    "profit_usd": profit_usdt_ami_reverse,
                    "profit_token": "AMI",
                    "profit_amount": ami_profit,
                    "profit_pct": profit_pct,
                    "trade_size_usdt": trade_usdt_ami_reverse,
                    "cex_ask": cex_ask,
                    "cex_bid": cex_bid,
                    "apt_bid": apt_bid,
                    "apt_ask": apt_ask,
                    "cex_fee": ami_cex_fee,
                    "apt_cex_fee": apt_cex_fee,
                    "dex_price_ami_apt": self.cellana_price_ami_apt,
                    "dex_price_usdt": cellana_price_usdt,
                    "apt_usdt_rate": self.apt_usdt_price,
                    "dex_fee": self.cellana_fee,
                    "pool_reserve_ami": self.cellana_reserve_ami,
                    "pool_reserve_apt": self.cellana_reserve_apt,
                })

        # ── Near-miss / best-direction diagnostic logging ─────────────────
        # Always log the BEST direction (closest to profit) every N seconds
        # to show how close we are to an opportunity.
        now_nm = time.time()
        if now_nm - self._last_near_miss_log >= self._NEAR_MISS_LOG_INTERVAL_S:
            all_dirs = [
                ("D2C", profit_usdt_dex_to_cex, pct_dex_to_cex, trade_usdt_dex_to_cex),
                ("AMI", profit_usdt_ami_cycle, pct_ami_cycle, trade_usdt_ami_cycle),
                ("APS", profit_usdt_apt_start, pct_apt_start, trade_usdt_apt_start),
                ("AMS", profit_usdt_ami_start, pct_ami_start, trade_usdt_ami_start),
                ("APR", profit_usdt_apt_reverse, pct_apt_reverse, trade_usdt_apt_reverse),
                ("AMR", profit_usdt_ami_reverse, pct_ami_reverse, trade_usdt_ami_reverse),
            ]
            # Sort by profit descending (best / least negative first)
            all_dirs.sort(key=lambda x: x[1], reverse=True)
            best = all_dirs[0]
            tag, pf, pct_val, sz = best
            if pf > 0:
                emoji = "🟢"
            elif pct_val > -0.15:
                emoji = "🟡"
            else:
                emoji = "🔴"
            logger.info(
                f"{emoji} [{exchange_lower.upper()}] CLOSEST  {tag} ${pf:+.4f}({pct_val:+.3f}%) @${sz:.0f}  "
                f"| " + "  ".join(f"{d[0]}={d[1]:+.3f}" for d in all_dirs)
            )
            self._last_near_miss_log = now_nm

    def check_arbitrage(self, cex_price_data: PriceData) -> None:
        """Backward-compatible wrapper (defaults to MEXC)."""
        self.check_arbitrage_for_exchange("mexc", cex_price_data)

    def check_cross_cex_ami_cycle_arbitrage(
        self,
        bybit_ami_price_data: Optional[PriceData],
        mexc_ami_price_data: Optional[PriceData],
        bybit_apt_price_data: Optional[PriceData],
        mexc_apt_price_data: Optional[PriceData],
    ) -> None:
        """Check cross-CEX AMI cycle arbitrage.

        Two directions:
        1) Buy AMI on MEXC -> swap on DEX -> sell APT on Bybit (USDT -> AMI -> APT -> USDT)
        2) Buy AMI on Bybit -> swap on DEX -> sell APT on MEXC (USDT -> AMI -> APT -> USDT)
        """
        if not self.cellana_price_ami_apt or self.cellana_price_ami_apt <= 0:
            return

        # Per-direction staleness: each direction checks only the data it needs
        bybit_ami_fresh = bybit_ami_price_data and not bybit_ami_price_data.is_stale(max_age=30)
        mexc_ami_fresh = mexc_ami_price_data and not mexc_ami_price_data.is_stale(max_age=30)
        if not bybit_ami_fresh and not mexc_ami_fresh:
            return  # neither side has fresh AMI data

        # APT stale check: Ensure APT prices are fresh
        if bybit_apt_price_data and bybit_apt_price_data.is_stale(max_age=30):
            bybit_apt_price_data = None
        if mexc_apt_price_data and mexc_apt_price_data.is_stale(max_age=30):
            mexc_apt_price_data = None

        bybit_fee_ami = self._get_bybit_fee(self._fee_type)
        mexc_fee_ami = self._get_mexc_fee(self._fee_type, settings.cex_symbol)
        bybit_fee_apt = self._get_bybit_fee(self._fee_type)
        mexc_fee_apt = self._get_mexc_fee(self._fee_type, settings.apt_cex_symbol)

        # Direction 1: BUY AMI on MEXC, SELL APT on Bybit
        trade_usdt_d1 = 0.0
        profit_usdt_d1 = -999.0

        if mexc_ami_fresh and bybit_apt_price_data and bybit_apt_price_data.bid > 0 and mexc_ami_price_data.ask > 0:
            # Orderbook boundaries
            buy_ob = self._orderbook_cumulative_usdt(mexc_ami_price_data.asks) if mexc_ami_price_data.asks else []
            sell_ob = self._orderbook_cumulative_qty_as_usdt(bybit_apt_price_data.bids, bybit_apt_price_data.bid) if (bybit_apt_price_data.bids and bybit_apt_price_data.bid > 0) else []
            bounds = sorted(set(buy_ob + sell_ob)) or None
            max_feas = self.max_trade_usdt

            def _pf_d1(sz):
                if sz <= 0:
                    return -999.0
                ea = mexc_ami_price_data.effective_buy_price(sz)
                if ea <= 0:
                    return -999.0
                ami_bought = sz / (ea * (1.0 + mexc_fee_ami))
                apt_from_swap = self._swap_ami_to_apt(ami_bought)
                if apt_from_swap <= 0:
                    return -999.0
                eb = bybit_apt_price_data.effective_sell_price(apt_from_swap)
                if eb <= 0:
                    return -999.0
                return apt_from_swap * eb * (1.0 - bybit_fee_apt) - sz - self.gas_cost_usd

            if max_feas >= self.min_trade_usdt and self.optimal_size_enabled:
                trade_usdt_d1, profit_usdt_d1 = self._find_optimal_trade_size(_pf_d1, max_feas, bounds)
            else:
                trade_usdt_d1 = max_feas
                profit_usdt_d1 = _pf_d1(max_feas) if max_feas >= self.min_trade_usdt else -999.0

            profit_pct = (profit_usdt_d1 / trade_usdt_d1 * 100) if trade_usdt_d1 > 0 else 0
            if profit_usdt_d1 > self.min_profit_cross_cex and profit_pct > self.min_profit_pct_cross_cex:
                # De-duplicate
                if self.deduplicator.should_log("AMI_CYCLE_CROSS_MEXC_BYBIT", mexc_ami_price_data.ask, bybit_apt_price_data.bid):
                    det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    logger.success(
                        f"🎯 [AMI CYCLE CROSS] MEXC→DEX→BYBIT ARB FOUND @ {det_time}  "
                        f"profit=${profit_usdt_d1:.4f} ({profit_pct:.2f}%)  size=${trade_usdt_d1:.2f}"
                    )
                    log_signal({
                        "type": "ami_cycle_cross_cex",
                        "buy_exchange": "mexc",
                        "sell_exchange": "bybit",
                        "profit_usd": profit_usdt_d1,
                        "profit_pct": profit_pct,
                        "trade_size_usdt": trade_usdt_d1,
                    })
                    self._log_and_execute({
                        "direction": "AMI_CYCLE_CROSS_MEXC_BYBIT",
                        "buy_exchange": "mexc",
                        "sell_exchange": "bybit",
                        "profit_usd": profit_usdt_d1,
                        "profit_token": "USDT",
                        "profit_amount": profit_usdt_d1,
                        "profit_pct": profit_pct,
                        "trade_size_usdt": trade_usdt_d1,
                        "buy_cex_ask": mexc_ami_price_data.ask,
                        "sell_apt_bid": bybit_apt_price_data.bid,
                        "buy_cex_fee": mexc_fee_ami,
                        "sell_apt_fee": bybit_fee_apt,
                        "buy_ami_ask_qty": mexc_ami_price_data.ask_qty,
                        "sell_apt_bid_qty": bybit_apt_price_data.bid_qty,
                        "dex_price_ami_apt": self.cellana_price_ami_apt,
                        "dex_price_usdt": self.get_cellana_price_in_usdt(),
                        "apt_usdt_rate": self.apt_usdt_price,
                        "dex_fee": self.cellana_fee,
                        "pool_reserve_ami": self.cellana_reserve_ami,
                        "pool_reserve_apt": self.cellana_reserve_apt,
                    })

        # Direction 2: BUY AMI on Bybit, SELL APT on MEXC
        if bybit_ami_fresh and mexc_apt_price_data and mexc_apt_price_data.bid > 0 and bybit_ami_price_data.ask > 0:
            # Orderbook boundaries
            buy_ob = self._orderbook_cumulative_usdt(bybit_ami_price_data.asks) if bybit_ami_price_data.asks else []
            sell_ob = self._orderbook_cumulative_qty_as_usdt(mexc_apt_price_data.bids, mexc_apt_price_data.bid) if (mexc_apt_price_data.bids and mexc_apt_price_data.bid > 0) else []
            bounds = sorted(set(buy_ob + sell_ob)) or None
            max_feas = self.max_trade_usdt

            def _pf_d2(sz):
                if sz <= 0:
                    return -999.0
                ea = bybit_ami_price_data.effective_buy_price(sz)
                if ea <= 0:
                    return -999.0
                ami_bought = sz / (ea * (1.0 + bybit_fee_ami))
                apt_from_swap = self._swap_ami_to_apt(ami_bought)
                if apt_from_swap <= 0:
                    return -999.0
                eb = mexc_apt_price_data.effective_sell_price(apt_from_swap)
                if eb <= 0:
                    return -999.0
                return apt_from_swap * eb * (1.0 - mexc_fee_apt) - sz - self.gas_cost_usd

            if max_feas >= self.min_trade_usdt and self.optimal_size_enabled:
                trade_usdt_d2, profit_usdt_d2 = self._find_optimal_trade_size(_pf_d2, max_feas, bounds)
            else:
                trade_usdt_d2 = max_feas
                profit_usdt_d2 = _pf_d2(max_feas) if max_feas >= self.min_trade_usdt else -999.0

            profit_pct = (profit_usdt_d2 / trade_usdt_d2 * 100) if trade_usdt_d2 > 0 else 0
            if profit_usdt_d2 > self.min_profit_cross_cex and profit_pct > self.min_profit_pct_cross_cex:
                # De-duplicate
                if self.deduplicator.should_log("AMI_CYCLE_CROSS_BYBIT_MEXC", bybit_ami_price_data.ask, mexc_apt_price_data.bid):
                    det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    logger.success(
                        f"🎯 [AMI CYCLE CROSS] BYBIT→DEX→MEXC ARB FOUND @ {det_time}  "
                        f"profit=${profit_usdt_d2:.4f} ({profit_pct:.2f}%)  size=${trade_usdt_d2:.2f}"
                    )
                    log_signal({
                        "type": "ami_cycle_cross_cex",
                        "buy_exchange": "bybit",
                        "sell_exchange": "mexc",
                        "profit_usd": profit_usdt_d2,
                        "profit_pct": profit_pct,
                        "trade_size_usdt": trade_usdt_d2,
                    })
                    self._log_and_execute({
                        "direction": "AMI_CYCLE_CROSS_BYBIT_MEXC",
                        "buy_exchange": "bybit",
                        "sell_exchange": "mexc",
                        "profit_usd": profit_usdt_d2,
                        "profit_token": "USDT",
                        "profit_amount": profit_usdt_d2,
                        "profit_pct": profit_pct,
                        "trade_size_usdt": trade_usdt_d2,
                        "buy_cex_ask": bybit_ami_price_data.ask,
                        "sell_apt_bid": mexc_apt_price_data.bid,
                        "buy_cex_fee": bybit_fee_ami,
                        "sell_apt_fee": mexc_fee_apt,
                        "buy_ami_ask_qty": bybit_ami_price_data.ask_qty,
                        "sell_apt_bid_qty": mexc_apt_price_data.bid_qty,
                        "dex_price_ami_apt": self.cellana_price_ami_apt,
                        "dex_price_usdt": self.get_cellana_price_in_usdt(),
                        "apt_usdt_rate": self.apt_usdt_price,
                        "dex_fee": self.cellana_fee,
                        "pool_reserve_ami": self.cellana_reserve_ami,
                        "pool_reserve_apt": self.cellana_reserve_apt,
                    })
    def check_cross_cex_dex_to_cex_arbitrage(
        self,
        bybit_ami_price_data: Optional[PriceData],
        mexc_ami_price_data: Optional[PriceData],
        bybit_apt_price_data: Optional[PriceData],
        mexc_apt_price_data: Optional[PriceData],
    ) -> None:
        """Check cross-CEX DEX_TO_CEX arbitrage.

        Two directions:
        1) Buy APT on Bybit -> swap APT->AMI on DEX -> sell AMI on MEXC
        2) Buy APT on MEXC -> swap APT->AMI on DEX -> sell AMI on Bybit
        """
        if not self.cellana_price_ami_apt or self.cellana_price_ami_apt <= 0:
            return

        # Per-direction staleness: each direction checks only the data it needs
        bybit_ami_fresh = bybit_ami_price_data and not bybit_ami_price_data.is_stale(max_age=30)
        mexc_ami_fresh = mexc_ami_price_data and not mexc_ami_price_data.is_stale(max_age=30)
        if not bybit_ami_fresh and not mexc_ami_fresh:
            return  # neither side has fresh AMI data

        bybit_fee_ami = self._get_bybit_fee(self._fee_type)
        mexc_fee_ami = self._get_mexc_fee(self._fee_type, settings.cex_symbol)
        bybit_fee_apt = self._get_bybit_fee(self._fee_type)
        mexc_fee_apt = self._get_mexc_fee(self._fee_type, settings.apt_cex_symbol)

        # Direction 1: Buy APT on Bybit, sell AMI on MEXC
        if (
            mexc_ami_fresh
            and bybit_apt_price_data
            and not bybit_apt_price_data.is_stale(max_age=30)
            and bybit_apt_price_data.ask > 0
            and mexc_ami_price_data.bid > 0
        ):
            # Orderbook boundaries
            buy_ob = self._orderbook_cumulative_usdt(bybit_apt_price_data.asks) if bybit_apt_price_data.asks else []
            sell_ob = self._orderbook_cumulative_qty_as_usdt(mexc_ami_price_data.bids, mexc_ami_price_data.bid) if (mexc_ami_price_data.bids and mexc_ami_price_data.bid > 0) else []
            bounds = sorted(set(buy_ob + sell_ob)) or None
            max_feas = self.max_trade_usdt

            def _pf_d1(sz):
                if sz <= 0:
                    return -999.0
                ea = bybit_apt_price_data.effective_buy_price(sz)
                if ea <= 0:
                    return -999.0
                apt_bought = sz / (ea * (1.0 + bybit_fee_apt))
                ami_from_swap = self._swap_apt_to_ami(apt_bought)
                if ami_from_swap <= 0:
                    return -999.0
                eb = mexc_ami_price_data.effective_sell_price(ami_from_swap)
                if eb <= 0:
                    return -999.0
                return ami_from_swap * eb * (1.0 - mexc_fee_ami) - sz - self.gas_cost_usd

            if max_feas >= self.min_trade_usdt and self.optimal_size_enabled:
                trade_usdt, profit_usdt = self._find_optimal_trade_size(_pf_d1, max_feas, bounds)
            else:
                trade_usdt = max_feas
                profit_usdt = _pf_d1(max_feas) if max_feas >= self.min_trade_usdt else -999.0

            profit_pct = (profit_usdt / trade_usdt * 100) if trade_usdt > 0 else 0
            if profit_usdt > self.min_profit_cross_cex and profit_pct > self.min_profit_pct_cross_cex:
                # De-duplicate
                if self.deduplicator.should_log("DEX_TO_CEX_CROSS_BYBIT_MEXC", bybit_apt_price_data.ask, mexc_ami_price_data.bid):
                    det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    logger.success(
                        f"🎯 [DEX→CEX CROSS] BYBIT→DEX→MEXC ARB FOUND @ {det_time}  "
                        f"profit=${profit_usdt:.4f} ({profit_pct:.2f}%)  size=${trade_usdt:.2f}"
                    )
                    log_signal({
                        "type": "dex_to_cex_cross_cex",
                        "buy_exchange": "bybit",
                        "sell_exchange": "mexc",
                        "profit_usd": profit_usdt,
                        "profit_pct": profit_pct,
                        "trade_size_usdt": trade_usdt,
                    })
                    self._log_and_execute({
                        "direction": "DEX_TO_CEX_CROSS_BYBIT_MEXC",
                        "buy_exchange": "bybit",
                        "sell_exchange": "mexc",
                        "profit_usd": profit_usdt,
                        "profit_token": "USDT",
                        "profit_amount": profit_usdt,
                        "profit_pct": profit_pct,
                        "trade_size_usdt": trade_usdt,
                        "buy_apt_ask": bybit_apt_price_data.ask,
                        "sell_ami_bid": mexc_ami_price_data.bid,
                        "buy_apt_fee": bybit_fee_apt,
                        "sell_ami_fee": mexc_fee_ami,
                        "buy_apt_ask_qty": bybit_apt_price_data.ask_qty,
                        "sell_ami_bid_qty": mexc_ami_price_data.bid_qty,
                        "dex_price_ami_apt": self.cellana_price_ami_apt,
                        "dex_price_usdt": self.get_cellana_price_in_usdt(),
                        "apt_usdt_rate": self.apt_usdt_price,
                        "dex_fee": self.cellana_fee,
                        "pool_reserve_ami": self.cellana_reserve_ami,
                        "pool_reserve_apt": self.cellana_reserve_apt,
                    })

        # Direction 2: Buy APT on MEXC, sell AMI on Bybit
        if (
            bybit_ami_fresh
            and mexc_apt_price_data
            and not mexc_apt_price_data.is_stale(max_age=30)
            and mexc_apt_price_data.ask > 0
            and bybit_ami_price_data.bid > 0
        ):
            # Orderbook boundaries
            buy_ob = self._orderbook_cumulative_usdt(mexc_apt_price_data.asks) if mexc_apt_price_data.asks else []
            sell_ob = self._orderbook_cumulative_qty_as_usdt(bybit_ami_price_data.bids, bybit_ami_price_data.bid) if (bybit_ami_price_data.bids and bybit_ami_price_data.bid > 0) else []
            bounds = sorted(set(buy_ob + sell_ob)) or None
            max_feas = self.max_trade_usdt

            def _pf_d2(sz):
                if sz <= 0:
                    return -999.0
                ea = mexc_apt_price_data.effective_buy_price(sz)
                if ea <= 0:
                    return -999.0
                apt_bought = sz / (ea * (1.0 + mexc_fee_apt))
                ami_from_swap = self._swap_apt_to_ami(apt_bought)
                if ami_from_swap <= 0:
                    return -999.0
                eb = bybit_ami_price_data.effective_sell_price(ami_from_swap)
                if eb <= 0:
                    return -999.0
                return ami_from_swap * eb * (1.0 - bybit_fee_ami) - sz - self.gas_cost_usd

            if max_feas >= self.min_trade_usdt and self.optimal_size_enabled:
                trade_usdt, profit_usdt = self._find_optimal_trade_size(_pf_d2, max_feas, bounds)
            else:
                trade_usdt = max_feas
                profit_usdt = _pf_d2(max_feas) if max_feas >= self.min_trade_usdt else -999.0

            profit_pct = (profit_usdt / trade_usdt * 100) if trade_usdt > 0 else 0
            if profit_usdt > self.min_profit_cross_cex and profit_pct > self.min_profit_pct_cross_cex:
                # De-duplicate
                if self.deduplicator.should_log("DEX_TO_CEX_CROSS_MEXC_BYBIT", mexc_apt_price_data.ask, bybit_ami_price_data.bid):
                    det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    logger.success(
                        f"🎯 [DEX→CEX CROSS] MEXC→DEX→BYBIT ARB FOUND @ {det_time}  "
                        f"profit=${profit_usdt:.4f} ({profit_pct:.2f}%)  size=${trade_usdt:.2f}"
                    )
                    log_signal({
                        "type": "dex_to_cex_cross_cex",
                        "buy_exchange": "mexc",
                        "sell_exchange": "bybit",
                        "profit_usd": profit_usdt,
                        "profit_pct": profit_pct,
                        "trade_size_usdt": trade_usdt,
                    })
                    self._log_and_execute({
                        "direction": "DEX_TO_CEX_CROSS_MEXC_BYBIT",
                        "buy_exchange": "mexc",
                        "sell_exchange": "bybit",
                        "profit_usd": profit_usdt,
                        "profit_token": "USDT",
                        "profit_amount": profit_usdt,
                        "profit_pct": profit_pct,
                        "trade_size_usdt": trade_usdt,
                        "buy_apt_ask": mexc_apt_price_data.ask,
                        "sell_ami_bid": bybit_ami_price_data.bid,
                        "buy_apt_fee": mexc_fee_apt,
                        "sell_ami_fee": bybit_fee_ami,
                        "buy_apt_ask_qty": mexc_apt_price_data.ask_qty,
                        "sell_ami_bid_qty": bybit_ami_price_data.bid_qty,
                        "dex_price_ami_apt": self.cellana_price_ami_apt,
                        "dex_price_usdt": self.get_cellana_price_in_usdt(),
                        "apt_usdt_rate": self.apt_usdt_price,
                        "dex_fee": self.cellana_fee,
                        "pool_reserve_ami": self.cellana_reserve_ami,
                        "pool_reserve_apt": self.cellana_reserve_apt,
                    })
    def check_cross_cex_apt_start_cycle(
        self,
        bybit_ami_price_data: Optional[PriceData],
        mexc_ami_price_data: Optional[PriceData],
        bybit_apt_price_data: Optional[PriceData],
        mexc_apt_price_data: Optional[PriceData],
    ) -> None:
        """Cross-CEX APT-start cycle: APT → AMI(DEX) → USDT(CEX1) → APT(CEX2).

        Two directions:
        1) APT → AMI(DEX) → sell AMI on MEXC → buy APT on Bybit
        2) APT → AMI(DEX) → sell AMI on Bybit → buy APT on MEXC
        Profit measured in APT.
        """
        if not self.cellana_price_ami_apt or self.cellana_price_ami_apt <= 0:
            return

        # Per-direction staleness: each direction checks only the data it needs
        bybit_ami_fresh = bybit_ami_price_data and not bybit_ami_price_data.is_stale(max_age=30)
        mexc_ami_fresh = mexc_ami_price_data and not mexc_ami_price_data.is_stale(max_age=30)
        if not bybit_ami_fresh and not mexc_ami_fresh:
            return  # neither side has fresh AMI data

        bybit_fee_ami = self._get_bybit_fee(self._fee_type)
        mexc_fee_ami = self._get_mexc_fee(self._fee_type, settings.cex_symbol)
        bybit_fee_apt = self._get_bybit_fee(self._fee_type)
        mexc_fee_apt = self._get_mexc_fee(self._fee_type, settings.apt_cex_symbol)

        # Direction 1: APT → AMI(DEX) → sell AMI on MEXC → buy APT on Bybit
        if (
            mexc_ami_fresh
            and mexc_ami_price_data.bid > 0
            and bybit_apt_price_data
            and not bybit_apt_price_data.is_stale(max_age=30)
            and bybit_apt_price_data.ask > 0
        ):
            apt_mid = bybit_apt_price_data.mid if bybit_apt_price_data.mid > 0 else bybit_apt_price_data.ask

            # Orderbook boundaries: MEXC AMI bids (sell) + Bybit APT asks (buy)
            sell_ob = self._orderbook_cumulative_qty_as_usdt(mexc_ami_price_data.bids, mexc_ami_price_data.bid) if (mexc_ami_price_data.bids and mexc_ami_price_data.bid > 0) else []
            buy_ob = self._orderbook_cumulative_usdt(bybit_apt_price_data.asks) if bybit_apt_price_data.asks else []
            bounds = sorted(set(sell_ob + buy_ob)) or None
            max_feas = self.max_trade_usdt

            def _pf_d1(sz, _apt_mid=apt_mid):
                if sz <= 0 or _apt_mid <= 0:
                    return -999.0
                apt_start = sz / _apt_mid
                ami_out = self._swap_apt_to_ami(apt_start)
                if ami_out <= 0:
                    return -999.0
                eb = mexc_ami_price_data.effective_sell_price(ami_out)
                if eb <= 0:
                    return -999.0
                usdt_out = ami_out * eb * (1.0 - mexc_fee_ami)
                ea = bybit_apt_price_data.effective_buy_price(usdt_out)
                if ea <= 0:
                    return -999.0
                apt_end = usdt_out / (ea * (1.0 + bybit_fee_apt))
                return (apt_end - apt_start) * _apt_mid - self.gas_cost_usd

            if max_feas >= self.min_trade_usdt and self.optimal_size_enabled:
                trade_usdt, profit_usdt = self._find_optimal_trade_size(_pf_d1, max_feas, bounds)
            else:
                trade_usdt = max_feas
                profit_usdt = _pf_d1(max_feas) if max_feas >= self.min_trade_usdt else -999.0

            profit_apt = profit_usdt / apt_mid if apt_mid > 0 else 0
            profit_pct = (profit_usdt / trade_usdt * 100) if trade_usdt > 0 else 0
            if profit_usdt > self.min_profit_apt_start and profit_pct > self.min_profit_pct_apt_start:
                if self.deduplicator.should_log("APT_START_CROSS_MEXC_BYBIT", mexc_ami_price_data.bid, bybit_apt_price_data.ask):
                    det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    logger.success(
                        f"🎯 [APT START CROSS] DEX→MEXC→BYBIT @ {det_time}  "
                        f"profit={profit_apt:.6f} APT (${profit_usdt:.4f}, {profit_pct:.2f}%)  "
                        f"size=${trade_usdt:.2f}"
                    )
                    log_signal({
                        "type": "apt_start_cross_cex",
                        "sell_ami_exchange": "mexc",
                        "buy_apt_exchange": "bybit",
                        "profit_usd": profit_usdt,
                        "profit_pct": profit_pct,
                        "profit_apt": profit_apt,
                        "trade_size_usdt": trade_usdt,
                    })
                    self._log_and_execute({
                        "direction": "APT_START_CROSS_MEXC_BYBIT",
                        "start_token": "APT",
                        "sell_ami_exchange": "mexc",
                        "buy_apt_exchange": "bybit",
                        "profit_usd": profit_usdt,
                        "profit_token": "APT",
                        "profit_amount": profit_apt,
                        "profit_pct": profit_pct,
                        "trade_size_usdt": trade_usdt,
                        "sell_ami_bid": mexc_ami_price_data.bid,
                        "buy_apt_ask": bybit_apt_price_data.ask,
                        "dex_price_ami_apt": self.cellana_price_ami_apt,
                        "dex_fee": self.cellana_fee,
                        "pool_reserve_ami": self.cellana_reserve_ami,
                        "pool_reserve_apt": self.cellana_reserve_apt,
                    })

        # Direction 2: APT → AMI(DEX) → sell AMI on Bybit → buy APT on MEXC
        if (
            bybit_ami_fresh
            and bybit_ami_price_data.bid > 0
            and mexc_apt_price_data
            and not mexc_apt_price_data.is_stale(max_age=30)
            and mexc_apt_price_data.ask > 0
        ):
            apt_mid = mexc_apt_price_data.mid if mexc_apt_price_data.mid > 0 else mexc_apt_price_data.ask

            # Orderbook boundaries: Bybit AMI bids (sell) + MEXC APT asks (buy)
            sell_ob = self._orderbook_cumulative_qty_as_usdt(bybit_ami_price_data.bids, bybit_ami_price_data.bid) if (bybit_ami_price_data.bids and bybit_ami_price_data.bid > 0) else []
            buy_ob = self._orderbook_cumulative_usdt(mexc_apt_price_data.asks) if mexc_apt_price_data.asks else []
            bounds = sorted(set(sell_ob + buy_ob)) or None
            max_feas = self.max_trade_usdt

            def _pf_d2(sz, _apt_mid=apt_mid):
                if sz <= 0 or _apt_mid <= 0:
                    return -999.0
                apt_start = sz / _apt_mid
                ami_out = self._swap_apt_to_ami(apt_start)
                if ami_out <= 0:
                    return -999.0
                eb = bybit_ami_price_data.effective_sell_price(ami_out)
                if eb <= 0:
                    return -999.0
                usdt_out = ami_out * eb * (1.0 - bybit_fee_ami)
                ea = mexc_apt_price_data.effective_buy_price(usdt_out)
                if ea <= 0:
                    return -999.0
                apt_end = usdt_out / (ea * (1.0 + mexc_fee_apt))
                return (apt_end - apt_start) * _apt_mid - self.gas_cost_usd

            if max_feas >= self.min_trade_usdt and self.optimal_size_enabled:
                trade_usdt, profit_usdt = self._find_optimal_trade_size(_pf_d2, max_feas, bounds)
            else:
                trade_usdt = max_feas
                profit_usdt = _pf_d2(max_feas) if max_feas >= self.min_trade_usdt else -999.0

            profit_apt = profit_usdt / apt_mid if apt_mid > 0 else 0
            profit_pct = (profit_usdt / trade_usdt * 100) if trade_usdt > 0 else 0
            if profit_usdt > self.min_profit_apt_start and profit_pct > self.min_profit_pct_apt_start:
                if self.deduplicator.should_log("APT_START_CROSS_BYBIT_MEXC", bybit_ami_price_data.bid, mexc_apt_price_data.ask):
                    det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    logger.success(
                        f"🎯 [APT START CROSS] DEX→BYBIT→MEXC @ {det_time}  "
                        f"profit={profit_apt:.6f} APT (${profit_usdt:.4f}, {profit_pct:.2f}%)  "
                        f"size=${trade_usdt:.2f}"
                    )
                    log_signal({
                        "type": "apt_start_cross_cex",
                        "sell_ami_exchange": "bybit",
                        "buy_apt_exchange": "mexc",
                        "profit_usd": profit_usdt,
                        "profit_pct": profit_pct,
                        "profit_apt": profit_apt,
                        "trade_size_usdt": trade_usdt,
                    })
                    self._log_and_execute({
                        "direction": "APT_START_CROSS_BYBIT_MEXC",
                        "start_token": "APT",
                        "sell_ami_exchange": "bybit",
                        "buy_apt_exchange": "mexc",
                        "profit_usd": profit_usdt,
                        "profit_token": "APT",
                        "profit_amount": profit_apt,
                        "profit_pct": profit_pct,
                        "trade_size_usdt": trade_usdt,
                        "sell_ami_bid": bybit_ami_price_data.bid,
                        "buy_apt_ask": mexc_apt_price_data.ask,
                        "dex_price_ami_apt": self.cellana_price_ami_apt,
                        "dex_fee": self.cellana_fee,
                        "pool_reserve_ami": self.cellana_reserve_ami,
                        "pool_reserve_apt": self.cellana_reserve_apt,
                    })
    def check_cross_cex_ami_start_cycle(
        self,
        bybit_ami_price_data: Optional[PriceData],
        mexc_ami_price_data: Optional[PriceData],
        bybit_apt_price_data: Optional[PriceData],
        mexc_apt_price_data: Optional[PriceData],
    ) -> None:
        """Cross-CEX AMI-start cycle: AMI → USDT(CEX1) → APT(CEX2) → AMI(DEX).

        Two directions:
        1) Sell AMI on MEXC → buy APT on Bybit → swap APT→AMI(DEX)
        2) Sell AMI on Bybit → buy APT on MEXC → swap APT→AMI(DEX)
        Profit measured in AMI.
        """
        if not self.cellana_price_ami_apt or self.cellana_price_ami_apt <= 0:
            return

        # Per-direction staleness: each direction checks only the data it needs
        bybit_ami_fresh = bybit_ami_price_data and not bybit_ami_price_data.is_stale(max_age=30)
        mexc_ami_fresh = mexc_ami_price_data and not mexc_ami_price_data.is_stale(max_age=30)
        if not bybit_ami_fresh and not mexc_ami_fresh:
            return  # neither side has fresh AMI data

        bybit_fee_ami = self._get_bybit_fee(self._fee_type)
        mexc_fee_ami = self._get_mexc_fee(self._fee_type, settings.cex_symbol)
        bybit_fee_apt = self._get_bybit_fee(self._fee_type)
        mexc_fee_apt = self._get_mexc_fee(self._fee_type, settings.apt_cex_symbol)

        # Direction 1: Sell AMI on MEXC → buy APT on Bybit → swap APT→AMI(DEX)
        if (
            mexc_ami_fresh
            and mexc_ami_price_data.bid > 0
            and bybit_apt_price_data
            and not bybit_apt_price_data.is_stale(max_age=30)
            and bybit_apt_price_data.ask > 0
        ):
            ami_mid = mexc_ami_price_data.mid if mexc_ami_price_data.mid > 0 else mexc_ami_price_data.bid

            # Orderbook boundaries: MEXC AMI bids (sell) + Bybit APT asks (buy)
            sell_ob = self._orderbook_cumulative_qty_as_usdt(mexc_ami_price_data.bids, mexc_ami_price_data.bid) if (mexc_ami_price_data.bids and mexc_ami_price_data.bid > 0) else []
            buy_ob = self._orderbook_cumulative_usdt(bybit_apt_price_data.asks) if bybit_apt_price_data.asks else []
            bounds = sorted(set(sell_ob + buy_ob)) or None
            max_feas = self.max_trade_usdt

            def _pf_d1(sz, _ami_mid=ami_mid):
                if sz <= 0 or _ami_mid <= 0:
                    return -999.0
                ami_start = sz / _ami_mid
                eb = mexc_ami_price_data.effective_sell_price(ami_start)
                if eb <= 0:
                    return -999.0
                usdt_out = ami_start * eb * (1.0 - mexc_fee_ami)
                ea = bybit_apt_price_data.effective_buy_price(usdt_out)
                if ea <= 0:
                    return -999.0
                apt_bought = usdt_out / (ea * (1.0 + bybit_fee_apt))
                ami_end = self._swap_apt_to_ami(apt_bought)
                if ami_end <= 0:
                    return -999.0
                return (ami_end - ami_start) * _ami_mid - self.gas_cost_usd

            if max_feas >= self.min_trade_usdt and self.optimal_size_enabled:
                trade_usdt, profit_usdt = self._find_optimal_trade_size(_pf_d1, max_feas, bounds)
            else:
                trade_usdt = max_feas
                profit_usdt = _pf_d1(max_feas) if max_feas >= self.min_trade_usdt else -999.0

            profit_ami = profit_usdt / ami_mid if ami_mid > 0 else 0
            profit_pct = (profit_usdt / trade_usdt * 100) if trade_usdt > 0 else 0
            if profit_usdt > self.min_profit_ami_start and profit_pct > self.min_profit_pct_ami_start:
                if self.deduplicator.should_log("AMI_START_CROSS_MEXC_BYBIT", mexc_ami_price_data.bid, bybit_apt_price_data.ask):
                    det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    logger.success(
                        f"🎯 [AMI START CROSS] MEXC→BYBIT→DEX @ {det_time}  "
                        f"profit={profit_ami:.2f} AMI (${profit_usdt:.4f}, {profit_pct:.2f}%)  "
                        f"size=${trade_usdt:.2f}"
                    )
                    log_signal({
                        "type": "ami_start_cross_cex",
                        "sell_ami_exchange": "mexc",
                        "buy_apt_exchange": "bybit",
                        "profit_usd": profit_usdt,
                        "profit_pct": profit_pct,
                        "profit_ami": profit_ami,
                        "trade_size_usdt": trade_usdt,
                    })
                    self._log_and_execute({
                        "direction": "AMI_START_CROSS_MEXC_BYBIT",
                        "start_token": "AMI",
                        "sell_ami_exchange": "mexc",
                        "buy_apt_exchange": "bybit",
                        "profit_usd": profit_usdt,
                        "profit_token": "AMI",
                        "profit_amount": profit_ami,
                        "profit_pct": profit_pct,
                        "trade_size_usdt": trade_usdt,
                        "sell_ami_bid": mexc_ami_price_data.bid,
                        "buy_apt_ask": bybit_apt_price_data.ask,
                        "dex_price_ami_apt": self.cellana_price_ami_apt,
                        "dex_fee": self.cellana_fee,
                        "pool_reserve_ami": self.cellana_reserve_ami,
                        "pool_reserve_apt": self.cellana_reserve_apt,
                    })

        # Direction 2: Sell AMI on Bybit → buy APT on MEXC → swap APT→AMI(DEX)
        if (
            bybit_ami_fresh
            and bybit_ami_price_data.bid > 0
            and mexc_apt_price_data
            and not mexc_apt_price_data.is_stale(max_age=30)
            and mexc_apt_price_data.ask > 0
        ):
            ami_mid = bybit_ami_price_data.mid if bybit_ami_price_data.mid > 0 else bybit_ami_price_data.bid

            # Orderbook boundaries: Bybit AMI bids (sell) + MEXC APT asks (buy)
            sell_ob = self._orderbook_cumulative_qty_as_usdt(bybit_ami_price_data.bids, bybit_ami_price_data.bid) if (bybit_ami_price_data.bids and bybit_ami_price_data.bid > 0) else []
            buy_ob = self._orderbook_cumulative_usdt(mexc_apt_price_data.asks) if mexc_apt_price_data.asks else []
            bounds = sorted(set(sell_ob + buy_ob)) or None
            max_feas = self.max_trade_usdt

            def _pf_d2(sz, _ami_mid=ami_mid):
                if sz <= 0 or _ami_mid <= 0:
                    return -999.0
                ami_start = sz / _ami_mid
                eb = bybit_ami_price_data.effective_sell_price(ami_start)
                if eb <= 0:
                    return -999.0
                usdt_out = ami_start * eb * (1.0 - bybit_fee_ami)
                ea = mexc_apt_price_data.effective_buy_price(usdt_out)
                if ea <= 0:
                    return -999.0
                apt_bought = usdt_out / (ea * (1.0 + mexc_fee_apt))
                ami_end = self._swap_apt_to_ami(apt_bought)
                if ami_end <= 0:
                    return -999.0
                return (ami_end - ami_start) * _ami_mid - self.gas_cost_usd

            if max_feas >= self.min_trade_usdt and self.optimal_size_enabled:
                trade_usdt, profit_usdt = self._find_optimal_trade_size(_pf_d2, max_feas, bounds)
            else:
                trade_usdt = max_feas
                profit_usdt = _pf_d2(max_feas) if max_feas >= self.min_trade_usdt else -999.0

            profit_ami = profit_usdt / ami_mid if ami_mid > 0 else 0
            profit_pct = (profit_usdt / trade_usdt * 100) if trade_usdt > 0 else 0
            if profit_usdt > self.min_profit_ami_start and profit_pct > self.min_profit_pct_ami_start:
                if self.deduplicator.should_log("AMI_START_CROSS_BYBIT_MEXC", bybit_ami_price_data.bid, mexc_apt_price_data.ask):
                    det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    logger.success(
                        f"🎯 [AMI START CROSS] BYBIT→MEXC→DEX @ {det_time}  "
                        f"profit={profit_ami:.2f} AMI (${profit_usdt:.4f}, {profit_pct:.2f}%)  "
                        f"size=${trade_usdt:.2f}"
                    )
                    log_signal({
                        "type": "ami_start_cross_cex",
                        "sell_ami_exchange": "bybit",
                        "buy_apt_exchange": "mexc",
                        "profit_usd": profit_usdt,
                        "profit_pct": profit_pct,
                        "profit_ami": profit_ami,
                        "trade_size_usdt": trade_usdt,
                    })
                    self._log_and_execute({
                        "direction": "AMI_START_CROSS_BYBIT_MEXC",
                        "start_token": "AMI",
                        "sell_ami_exchange": "bybit",
                        "buy_apt_exchange": "mexc",
                        "profit_usd": profit_usdt,
                        "profit_token": "AMI",
                        "profit_amount": profit_ami,
                        "profit_pct": profit_pct,
                        "trade_size_usdt": trade_usdt,
                        "sell_ami_bid": bybit_ami_price_data.bid,
                        "buy_apt_ask": mexc_apt_price_data.ask,
                        "dex_price_ami_apt": self.cellana_price_ami_apt,
                        "dex_fee": self.cellana_fee,
                        "pool_reserve_ami": self.cellana_reserve_ami,
                        "pool_reserve_apt": self.cellana_reserve_apt,
                    })
    def check_cross_cex_apt_reverse_cycle(
        self,
        bybit_ami_price_data: Optional[PriceData],
        mexc_ami_price_data: Optional[PriceData],
        bybit_apt_price_data: Optional[PriceData],
        mexc_apt_price_data: Optional[PriceData],
    ) -> None:
        """Cross-CEX APT reverse cycle: APT → USDT(sell APT CEX1) → AMI(buy CEX2) → APT(DEX AMI→APT).

        Cycle B from APT. DEX direction: AMI→APT (opposite of APT_START_CROSS).
        Two directions:
        1) Sell APT on Bybit → buy AMI on MEXC → swap AMI→APT on DEX
        2) Sell APT on MEXC → buy AMI on Bybit → swap AMI→APT on DEX
        Profit measured in APT.
        """
        if not self.cellana_price_ami_apt or self.cellana_price_ami_apt <= 0:
            return

        # Per-direction staleness: each direction checks only the data it needs
        bybit_ami_fresh = bybit_ami_price_data and not bybit_ami_price_data.is_stale(max_age=30)
        mexc_ami_fresh = mexc_ami_price_data and not mexc_ami_price_data.is_stale(max_age=30)
        if not bybit_ami_fresh and not mexc_ami_fresh:
            return  # neither side has fresh AMI data

        bybit_fee_ami = self._get_bybit_fee(self._fee_type)
        mexc_fee_ami = self._get_mexc_fee(self._fee_type, settings.cex_symbol)
        bybit_fee_apt = self._get_bybit_fee(self._fee_type)
        mexc_fee_apt = self._get_mexc_fee(self._fee_type, settings.apt_cex_symbol)

        # Direction 1: Sell APT on Bybit → buy AMI on MEXC → swap AMI→APT(DEX)
        if (
            mexc_ami_fresh
            and bybit_apt_price_data
            and not bybit_apt_price_data.is_stale(max_age=30)
            and bybit_apt_price_data.bid > 0
            and mexc_ami_price_data.ask > 0
        ):
            apt_mid = bybit_apt_price_data.mid if bybit_apt_price_data.mid > 0 else bybit_apt_price_data.bid

            # Orderbook boundaries: Bybit APT bids (sell) + MEXC AMI asks (buy)
            sell_ob = self._orderbook_cumulative_qty_as_usdt(bybit_apt_price_data.bids, bybit_apt_price_data.bid) if (bybit_apt_price_data.bids and bybit_apt_price_data.bid > 0) else []
            buy_ob = self._orderbook_cumulative_usdt(mexc_ami_price_data.asks) if mexc_ami_price_data.asks else []
            bounds = sorted(set(sell_ob + buy_ob)) or None
            max_feas = self.max_trade_usdt

            def _pf_d1(sz, _apt_mid=apt_mid):
                if sz <= 0 or _apt_mid <= 0:
                    return -999.0
                apt_start = sz / _apt_mid
                eb = bybit_apt_price_data.effective_sell_price(apt_start)
                if eb <= 0:
                    return -999.0
                usdt_out = apt_start * eb * (1.0 - bybit_fee_apt)
                ea = mexc_ami_price_data.effective_buy_price(usdt_out)
                if ea <= 0:
                    return -999.0
                ami_bought = usdt_out / (ea * (1.0 + mexc_fee_ami))
                apt_end = self._swap_ami_to_apt(ami_bought)
                if apt_end <= 0:
                    return -999.0
                return (apt_end - apt_start) * _apt_mid - self.gas_cost_usd

            if max_feas >= self.min_trade_usdt and self.optimal_size_enabled:
                trade_usdt, profit_usdt = self._find_optimal_trade_size(_pf_d1, max_feas, bounds)
            else:
                trade_usdt = max_feas
                profit_usdt = _pf_d1(max_feas) if max_feas >= self.min_trade_usdt else -999.0

            profit_apt = profit_usdt / apt_mid if apt_mid > 0 else 0
            profit_pct = (profit_usdt / trade_usdt * 100) if trade_usdt > 0 else 0
            if profit_usdt > self.min_profit_apt_start and profit_pct > self.min_profit_pct_apt_start:
                if self.deduplicator.should_log("APT_REVERSE_CROSS_BYBIT_MEXC", bybit_apt_price_data.bid, mexc_ami_price_data.ask):
                    det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    logger.success(
                        f"🎯 [APT REVERSE CROSS] BYBIT→MEXC→DEX @ {det_time}  "
                        f"profit={profit_apt:.6f} APT (${profit_usdt:.4f}, {profit_pct:.2f}%)  "
                        f"size=${trade_usdt:.2f}"
                    )
                    log_signal({
                        "type": "apt_reverse_cross_cex",
                        "sell_apt_exchange": "bybit",
                        "buy_ami_exchange": "mexc",
                        "profit_usd": profit_usdt,
                        "profit_pct": profit_pct,
                        "profit_apt": profit_apt,
                        "trade_size_usdt": trade_usdt,
                    })
                    self._log_and_execute({
                        "direction": "APT_REVERSE_CROSS_BYBIT_MEXC",
                        "start_token": "APT",
                        "dex_direction": "AMI→APT",
                        "sell_apt_exchange": "bybit",
                        "buy_ami_exchange": "mexc",
                        "profit_usd": profit_usdt,
                        "profit_token": "APT",
                        "profit_amount": profit_apt,
                        "profit_pct": profit_pct,
                        "trade_size_usdt": trade_usdt,
                        "sell_apt_bid": bybit_apt_price_data.bid,
                        "buy_ami_ask": mexc_ami_price_data.ask,
                        "dex_price_ami_apt": self.cellana_price_ami_apt,
                        "dex_fee": self.cellana_fee,
                        "pool_reserve_ami": self.cellana_reserve_ami,
                        "pool_reserve_apt": self.cellana_reserve_apt,
                    })

        # Direction 2: Sell APT on MEXC → buy AMI on Bybit → swap AMI→APT(DEX)
        if (
            bybit_ami_fresh
            and mexc_apt_price_data
            and not mexc_apt_price_data.is_stale(max_age=30)
            and mexc_apt_price_data.bid > 0
            and bybit_ami_price_data.ask > 0
        ):
            apt_mid = mexc_apt_price_data.mid if mexc_apt_price_data.mid > 0 else mexc_apt_price_data.bid

            # Orderbook boundaries: MEXC APT bids (sell) + Bybit AMI asks (buy)
            sell_ob = self._orderbook_cumulative_qty_as_usdt(mexc_apt_price_data.bids, mexc_apt_price_data.bid) if (mexc_apt_price_data.bids and mexc_apt_price_data.bid > 0) else []
            buy_ob = self._orderbook_cumulative_usdt(bybit_ami_price_data.asks) if bybit_ami_price_data.asks else []
            bounds = sorted(set(sell_ob + buy_ob)) or None
            max_feas = self.max_trade_usdt

            def _pf_d2(sz, _apt_mid=apt_mid):
                if sz <= 0 or _apt_mid <= 0:
                    return -999.0
                apt_start = sz / _apt_mid
                eb = mexc_apt_price_data.effective_sell_price(apt_start)
                if eb <= 0:
                    return -999.0
                usdt_out = apt_start * eb * (1.0 - mexc_fee_apt)
                ea = bybit_ami_price_data.effective_buy_price(usdt_out)
                if ea <= 0:
                    return -999.0
                ami_bought = usdt_out / (ea * (1.0 + bybit_fee_ami))
                apt_end = self._swap_ami_to_apt(ami_bought)
                if apt_end <= 0:
                    return -999.0
                return (apt_end - apt_start) * _apt_mid - self.gas_cost_usd

            if max_feas >= self.min_trade_usdt and self.optimal_size_enabled:
                trade_usdt, profit_usdt = self._find_optimal_trade_size(_pf_d2, max_feas, bounds)
            else:
                trade_usdt = max_feas
                profit_usdt = _pf_d2(max_feas) if max_feas >= self.min_trade_usdt else -999.0

            profit_apt = profit_usdt / apt_mid if apt_mid > 0 else 0
            profit_pct = (profit_usdt / trade_usdt * 100) if trade_usdt > 0 else 0
            if profit_usdt > self.min_profit_apt_start and profit_pct > self.min_profit_pct_apt_start:
                if self.deduplicator.should_log("APT_REVERSE_CROSS_MEXC_BYBIT", mexc_apt_price_data.bid, bybit_ami_price_data.ask):
                    det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    logger.success(
                        f"🎯 [APT REVERSE CROSS] MEXC→BYBIT→DEX @ {det_time}  "
                        f"profit={profit_apt:.6f} APT (${profit_usdt:.4f}, {profit_pct:.2f}%)  "
                        f"size=${trade_usdt:.2f}"
                    )
                    log_signal({
                        "type": "apt_reverse_cross_cex",
                        "sell_apt_exchange": "mexc",
                        "buy_ami_exchange": "bybit",
                        "profit_usd": profit_usdt,
                        "profit_pct": profit_pct,
                        "profit_apt": profit_apt,
                        "trade_size_usdt": trade_usdt,
                    })
                    self._log_and_execute({
                        "direction": "APT_REVERSE_CROSS_MEXC_BYBIT",
                        "start_token": "APT",
                        "dex_direction": "AMI→APT",
                        "sell_apt_exchange": "mexc",
                        "buy_ami_exchange": "bybit",
                        "profit_usd": profit_usdt,
                        "profit_token": "APT",
                        "profit_amount": profit_apt,
                        "profit_pct": profit_pct,
                        "trade_size_usdt": trade_usdt,
                        "sell_apt_bid": mexc_apt_price_data.bid,
                        "buy_ami_ask": bybit_ami_price_data.ask,
                        "dex_price_ami_apt": self.cellana_price_ami_apt,
                        "dex_fee": self.cellana_fee,
                        "pool_reserve_ami": self.cellana_reserve_ami,
                        "pool_reserve_apt": self.cellana_reserve_apt,
                    })
    def check_cross_cex_ami_reverse_cycle(
        self,
        bybit_ami_price_data: Optional[PriceData],
        mexc_ami_price_data: Optional[PriceData],
        bybit_apt_price_data: Optional[PriceData],
        mexc_apt_price_data: Optional[PriceData],
    ) -> None:
        """Cross-CEX AMI reverse cycle: AMI → APT(DEX AMI→APT) → USDT(sell APT CEX1) → AMI(buy CEX2).

        Cycle B from AMI. DEX direction: AMI→APT (opposite of AMI_START_CROSS).
        Two directions:
        1) Swap AMI→APT(DEX) → sell APT on Bybit → buy AMI on MEXC
        2) Swap AMI→APT(DEX) → sell APT on MEXC → buy AMI on Bybit
        Profit measured in AMI.
        """
        if not self.cellana_price_ami_apt or self.cellana_price_ami_apt <= 0:
            return

        # Per-direction staleness: each direction checks only the data it needs
        bybit_ami_fresh = bybit_ami_price_data and not bybit_ami_price_data.is_stale(max_age=30)
        mexc_ami_fresh = mexc_ami_price_data and not mexc_ami_price_data.is_stale(max_age=30)
        if not bybit_ami_fresh and not mexc_ami_fresh:
            return  # neither side has fresh AMI data

        bybit_fee_ami = self._get_bybit_fee(self._fee_type)
        mexc_fee_ami = self._get_mexc_fee(self._fee_type, settings.cex_symbol)
        bybit_fee_apt = self._get_bybit_fee(self._fee_type)
        mexc_fee_apt = self._get_mexc_fee(self._fee_type, settings.apt_cex_symbol)

        # Direction 1: AMI → APT(DEX) → sell APT on Bybit → buy AMI on MEXC
        if (
            mexc_ami_fresh
            and bybit_apt_price_data
            and not bybit_apt_price_data.is_stale(max_age=30)
            and bybit_apt_price_data.bid > 0
            and mexc_ami_price_data.ask > 0
        ):
            ami_mid = mexc_ami_price_data.mid if mexc_ami_price_data.mid > 0 else mexc_ami_price_data.ask

            # Orderbook boundaries: Bybit APT bids (sell) + MEXC AMI asks (buy)
            sell_ob = self._orderbook_cumulative_qty_as_usdt(bybit_apt_price_data.bids, bybit_apt_price_data.bid) if (bybit_apt_price_data.bids and bybit_apt_price_data.bid > 0) else []
            buy_ob = self._orderbook_cumulative_usdt(mexc_ami_price_data.asks) if mexc_ami_price_data.asks else []
            bounds = sorted(set(sell_ob + buy_ob)) or None
            max_feas = self.max_trade_usdt

            def _pf_d1(sz, _ami_mid=ami_mid):
                if sz <= 0 or _ami_mid <= 0:
                    return -999.0
                ami_start = sz / _ami_mid
                apt_out = self._swap_ami_to_apt(ami_start)
                if apt_out <= 0:
                    return -999.0
                eb = bybit_apt_price_data.effective_sell_price(apt_out)
                if eb <= 0:
                    return -999.0
                usdt_out = apt_out * eb * (1.0 - bybit_fee_apt)
                ea = mexc_ami_price_data.effective_buy_price(usdt_out)
                if ea <= 0:
                    return -999.0
                ami_end = usdt_out / (ea * (1.0 + mexc_fee_ami))
                return (ami_end - ami_start) * _ami_mid - self.gas_cost_usd

            if max_feas >= self.min_trade_usdt and self.optimal_size_enabled:
                trade_usdt, profit_usdt = self._find_optimal_trade_size(_pf_d1, max_feas, bounds)
            else:
                trade_usdt = max_feas
                profit_usdt = _pf_d1(max_feas) if max_feas >= self.min_trade_usdt else -999.0

            profit_ami = profit_usdt / ami_mid if ami_mid > 0 else 0
            profit_pct = (profit_usdt / trade_usdt * 100) if trade_usdt > 0 else 0
            if profit_usdt > self.min_profit_ami_start and profit_pct > self.min_profit_pct_ami_start:
                if self.deduplicator.should_log("AMI_REVERSE_CROSS_BYBIT_MEXC", bybit_apt_price_data.bid, mexc_ami_price_data.ask):
                    det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    logger.success(
                        f"🎯 [AMI REVERSE CROSS] DEX→BYBIT→MEXC @ {det_time}  "
                        f"profit={profit_ami:.2f} AMI (${profit_usdt:.4f}, {profit_pct:.2f}%)  "
                        f"size=${trade_usdt:.2f}"
                    )
                    log_signal({
                        "type": "ami_reverse_cross_cex",
                        "sell_apt_exchange": "bybit",
                        "buy_ami_exchange": "mexc",
                        "profit_usd": profit_usdt,
                        "profit_pct": profit_pct,
                        "profit_ami": profit_ami,
                        "trade_size_usdt": trade_usdt,
                    })
                    self._log_and_execute({
                        "direction": "AMI_REVERSE_CROSS_BYBIT_MEXC",
                        "start_token": "AMI",
                        "dex_direction": "AMI→APT",
                        "sell_apt_exchange": "bybit",
                        "buy_ami_exchange": "mexc",
                        "profit_usd": profit_usdt,
                        "profit_token": "AMI",
                        "profit_amount": profit_ami,
                        "profit_pct": profit_pct,
                        "trade_size_usdt": trade_usdt,
                        "sell_apt_bid": bybit_apt_price_data.bid,
                        "buy_ami_ask": mexc_ami_price_data.ask,
                        "dex_price_ami_apt": self.cellana_price_ami_apt,
                        "dex_fee": self.cellana_fee,
                        "pool_reserve_ami": self.cellana_reserve_ami,
                        "pool_reserve_apt": self.cellana_reserve_apt,
                    })

        # Direction 2: AMI → APT(DEX) → sell APT on MEXC → buy AMI on Bybit
        if (
            bybit_ami_fresh
            and mexc_apt_price_data
            and not mexc_apt_price_data.is_stale(max_age=30)
            and mexc_apt_price_data.bid > 0
            and bybit_ami_price_data.ask > 0
        ):
            ami_mid = bybit_ami_price_data.mid if bybit_ami_price_data.mid > 0 else bybit_ami_price_data.ask

            # Orderbook boundaries: MEXC APT bids (sell) + Bybit AMI asks (buy)
            sell_ob = self._orderbook_cumulative_qty_as_usdt(mexc_apt_price_data.bids, mexc_apt_price_data.bid) if (mexc_apt_price_data.bids and mexc_apt_price_data.bid > 0) else []
            buy_ob = self._orderbook_cumulative_usdt(bybit_ami_price_data.asks) if bybit_ami_price_data.asks else []
            bounds = sorted(set(sell_ob + buy_ob)) or None
            max_feas = self.max_trade_usdt

            def _pf_d2(sz, _ami_mid=ami_mid):
                if sz <= 0 or _ami_mid <= 0:
                    return -999.0
                ami_start = sz / _ami_mid
                apt_out = self._swap_ami_to_apt(ami_start)
                if apt_out <= 0:
                    return -999.0
                eb = mexc_apt_price_data.effective_sell_price(apt_out)
                if eb <= 0:
                    return -999.0
                usdt_out = apt_out * eb * (1.0 - mexc_fee_apt)
                ea = bybit_ami_price_data.effective_buy_price(usdt_out)
                if ea <= 0:
                    return -999.0
                ami_end = usdt_out / (ea * (1.0 + bybit_fee_ami))
                return (ami_end - ami_start) * _ami_mid - self.gas_cost_usd

            if max_feas >= self.min_trade_usdt and self.optimal_size_enabled:
                trade_usdt, profit_usdt = self._find_optimal_trade_size(_pf_d2, max_feas, bounds)
            else:
                trade_usdt = max_feas
                profit_usdt = _pf_d2(max_feas) if max_feas >= self.min_trade_usdt else -999.0

            profit_ami = profit_usdt / ami_mid if ami_mid > 0 else 0
            profit_pct = (profit_usdt / trade_usdt * 100) if trade_usdt > 0 else 0
            if profit_usdt > self.min_profit_ami_start and profit_pct > self.min_profit_pct_ami_start:
                if self.deduplicator.should_log("AMI_REVERSE_CROSS_MEXC_BYBIT", mexc_apt_price_data.bid, bybit_ami_price_data.ask):
                    det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    logger.success(
                        f"🎯 [AMI REVERSE CROSS] DEX→MEXC→BYBIT @ {det_time}  "
                        f"profit={profit_ami:.2f} AMI (${profit_usdt:.4f}, {profit_pct:.2f}%)  "
                        f"size=${trade_usdt:.2f}"
                    )
                    log_signal({
                        "type": "ami_reverse_cross_cex",
                        "sell_apt_exchange": "mexc",
                        "buy_ami_exchange": "bybit",
                        "profit_usd": profit_usdt,
                        "profit_pct": profit_pct,
                        "profit_ami": profit_ami,
                        "trade_size_usdt": trade_usdt,
                    })
                    self._log_and_execute({
                        "direction": "AMI_REVERSE_CROSS_MEXC_BYBIT",
                        "start_token": "AMI",
                        "dex_direction": "AMI→APT",
                        "sell_apt_exchange": "mexc",
                        "buy_ami_exchange": "bybit",
                        "profit_usd": profit_usdt,
                        "profit_token": "AMI",
                        "profit_amount": profit_ami,
                        "profit_pct": profit_pct,
                        "trade_size_usdt": trade_usdt,
                        "sell_apt_bid": mexc_apt_price_data.bid,
                        "buy_ami_ask": bybit_ami_price_data.ask,
                        "dex_price_ami_apt": self.cellana_price_ami_apt,
                        "dex_fee": self.cellana_fee,
                        "pool_reserve_ami": self.cellana_reserve_ami,
                        "pool_reserve_apt": self.cellana_reserve_apt,
                    })
    def check_cex_to_cex_arbitrage(
        self,
        bybit_price_data: Optional[PriceData],
        mexc_price_data: Optional[PriceData],
        bybit_apt_price_data: Optional[PriceData] = None,
        mexc_apt_price_data: Optional[PriceData] = None,
    ) -> None:
        """Check CEX-to-CEX arbitrage between Bybit and MEXC.
        
        Two directions:
        1. Buy AMI on Bybit, Sell on MEXC (USDT -> AMI -> USDT)
        2. Buy AMI on MEXC, Sell on Bybit (USDT -> AMI -> USDT)
        
        Profit calculated in USD.
        """
        if not bybit_price_data or not mexc_price_data:
            return
        
        if bybit_price_data.is_stale(max_age=30) or mexc_price_data.is_stale(max_age=30):
            return
        
        bybit_ask = bybit_price_data.ask  # Buy price on Bybit
        bybit_bid = bybit_price_data.bid  # Sell price on Bybit
        mexc_ask = mexc_price_data.ask    # Buy price on MEXC
        mexc_bid = mexc_price_data.bid    # Sell price on MEXC
        
        bybit_fee = self._get_bybit_fee(self._fee_type)
        mexc_fee = self._get_mexc_fee(self._fee_type)
        bybit_apt_fee = self._get_bybit_fee(self._fee_type)
        mexc_apt_fee = self._get_mexc_fee(self._fee_type, settings.apt_cex_symbol)
        
        # Direction 1: Buy AMI on Bybit -> Sell AMI on MEXC (USDT -> AMI -> USDT)
        profit_usdt_bybit_mexc = -999.0
        trade_usdt_bybit_mexc = 0.0

        if bybit_ask > 0 and mexc_bid > 0:
            # Orderbook boundaries
            buy_ob = self._orderbook_cumulative_usdt(bybit_price_data.asks) if bybit_price_data.asks else []
            sell_ob = self._orderbook_cumulative_qty_as_usdt(mexc_price_data.bids, mexc_price_data.bid) if (mexc_price_data.bids and mexc_price_data.bid > 0) else []
            bounds = sorted(set(buy_ob + sell_ob)) or None
            max_feas = self.max_trade_usdt

            def _pf_ami_bm(sz):
                if sz <= 0:
                    return -999.0
                ea = bybit_price_data.effective_buy_price(sz)
                if ea <= 0:
                    return -999.0
                ami_bought = sz / (ea * (1.0 + bybit_fee))
                eb = mexc_price_data.effective_sell_price(ami_bought)
                if eb <= 0:
                    return -999.0
                return ami_bought * eb * (1.0 - mexc_fee) - sz

            if max_feas >= self.min_trade_usdt and self.optimal_size_enabled:
                trade_usdt_bybit_mexc, profit_usdt_bybit_mexc = self._find_optimal_trade_size(_pf_ami_bm, max_feas, bounds)
            else:
                trade_usdt_bybit_mexc = max_feas
                profit_usdt_bybit_mexc = _pf_ami_bm(max_feas) if max_feas >= self.min_trade_usdt else -999.0

        # Direction 2: Buy AMI on MEXC -> Sell AMI on Bybit (USDT -> AMI -> USDT)
        profit_usdt_mexc_bybit = -999.0
        trade_usdt_mexc_bybit = 0.0

        if mexc_ask > 0 and bybit_bid > 0:
            # Orderbook boundaries
            buy_ob = self._orderbook_cumulative_usdt(mexc_price_data.asks) if mexc_price_data.asks else []
            sell_ob = self._orderbook_cumulative_qty_as_usdt(bybit_price_data.bids, bybit_price_data.bid) if (bybit_price_data.bids and bybit_price_data.bid > 0) else []
            bounds = sorted(set(buy_ob + sell_ob)) or None
            max_feas = self.max_trade_usdt

            def _pf_ami_mb(sz):
                if sz <= 0:
                    return -999.0
                ea = mexc_price_data.effective_buy_price(sz)
                if ea <= 0:
                    return -999.0
                ami_bought = sz / (ea * (1.0 + mexc_fee))
                eb = bybit_price_data.effective_sell_price(ami_bought)
                if eb <= 0:
                    return -999.0
                return ami_bought * eb * (1.0 - bybit_fee) - sz

            if max_feas >= self.min_trade_usdt and self.optimal_size_enabled:
                trade_usdt_mexc_bybit, profit_usdt_mexc_bybit = self._find_optimal_trade_size(_pf_ami_mb, max_feas, bounds)
            else:
                trade_usdt_mexc_bybit = max_feas
                profit_usdt_mexc_bybit = _pf_ami_mb(max_feas) if max_feas >= self.min_trade_usdt else -999.0

        # Log opportunities (only if profit > threshold USD AND %, with dedup)
        pct_bybit_mexc = (profit_usdt_bybit_mexc / trade_usdt_bybit_mexc * 100) if trade_usdt_bybit_mexc > 0 else 0
        if profit_usdt_bybit_mexc > self.min_profit_cex_to_cex and pct_bybit_mexc > self.min_profit_pct_cex_to_cex:
            # De-duplicate
            if self.deduplicator.should_log("CEX_TO_CEX_BYBIT_MEXC", bybit_ask, mexc_bid):
                profit_pct = pct_bybit_mexc
                det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                logger.success(
                    f"🎯 [CEX-CEX] BYBIT→MEXC ARB FOUND @ {det_time}  "
                    f"buy_bybit={bybit_ask:.6f}  sell_mexc={mexc_bid:.6f}  "
                    f"profit=${profit_usdt_bybit_mexc:.4f} ({profit_pct:.2f}%)  size=${trade_usdt_bybit_mexc:.2f}"
                )
                log_signal({
                    "type": "cex_to_cex",
                    "buy_exchange": "bybit",
                    "sell_exchange": "mexc",
                    "buy_price": bybit_ask,
                    "sell_price": mexc_bid,
                    "profit_usd": profit_usdt_bybit_mexc,
                    "profit_pct": profit_pct,
                    "trade_size_usdt": trade_usdt_bybit_mexc,
                })
                self._log_and_execute({
                    "direction": "CEX_TO_CEX_BYBIT_MEXC",
                    "buy_exchange": "bybit",
                    "sell_exchange": "mexc",
                    "buy_price": bybit_ask,
                    "sell_price": mexc_bid,
                    "profit_usd": profit_usdt_bybit_mexc,
                    "profit_pct": profit_pct,
                    "trade_size_usdt": trade_usdt_bybit_mexc,
                    "bybit_ask": bybit_ask,
                    "bybit_bid": bybit_bid,
                    "mexc_ask": mexc_ask,
                    "mexc_bid": mexc_bid,
                    "bybit_fee": bybit_fee,
                    "mexc_fee": mexc_fee,
                    "bybit_bid_qty": bybit_price_data.bid_qty,
                    "bybit_ask_qty": bybit_price_data.ask_qty,
                    "mexc_bid_qty": mexc_price_data.bid_qty,
                    "mexc_ask_qty": mexc_price_data.ask_qty,
                })

        pct_mexc_bybit = (profit_usdt_mexc_bybit / trade_usdt_mexc_bybit * 100) if trade_usdt_mexc_bybit > 0 else 0
        if profit_usdt_mexc_bybit > self.min_profit_cex_to_cex and pct_mexc_bybit > self.min_profit_pct_cex_to_cex:
            # De-duplicate
            if self.deduplicator.should_log("CEX_TO_CEX_MEXC_BYBIT", mexc_ask, bybit_bid):
                profit_pct = pct_mexc_bybit
                det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                logger.success(
                    f"🎯 [CEX-CEX] MEXC→BYBIT ARB FOUND @ {det_time}  "
                    f"buy_mexc={mexc_ask:.6f}  sell_bybit={bybit_bid:.6f}  "
                    f"profit=${profit_usdt_mexc_bybit:.4f} ({profit_pct:.2f}%)  size=${trade_usdt_mexc_bybit:.2f}"
                )
                log_signal({
                    "type": "cex_to_cex",
                    "buy_exchange": "mexc",
                    "sell_exchange": "bybit",
                    "buy_price": mexc_ask,
                    "sell_price": bybit_bid,
                    "profit_usd": profit_usdt_mexc_bybit,
                    "profit_pct": profit_pct,
                    "trade_size_usdt": trade_usdt_mexc_bybit,
                })
                self._log_and_execute({
                    "direction": "CEX_TO_CEX_MEXC_BYBIT",
                    "buy_exchange": "mexc",
                    "sell_exchange": "bybit",
                    "buy_price": mexc_ask,
                    "sell_price": bybit_bid,
                    "profit_usd": profit_usdt_mexc_bybit,
                    "profit_pct": profit_pct,
                    "trade_size_usdt": trade_usdt_mexc_bybit,
                    "bybit_ask": bybit_ask,
                    "bybit_bid": bybit_bid,
                    "mexc_ask": mexc_ask,
                    "mexc_bid": mexc_bid,
                    "bybit_fee": bybit_fee,
                    "mexc_fee": mexc_fee,
                    "bybit_bid_qty": bybit_price_data.bid_qty,
                    "bybit_ask_qty": bybit_price_data.ask_qty,
                    "mexc_bid_qty": mexc_price_data.bid_qty,
                    "mexc_ask_qty": mexc_price_data.ask_qty,
                })

        # Direction 3: Buy APT on Bybit -> Sell APT on MEXC (USDT -> APT -> USDT)
        profit_usdt_apt_bybit_mexc = -999.0
        trade_usdt_apt_bybit_mexc = 0.0
        if (
            bybit_apt_price_data
            and mexc_apt_price_data
            and not bybit_apt_price_data.is_stale(max_age=30)
            and not mexc_apt_price_data.is_stale(max_age=30)
            and bybit_apt_price_data.ask > 0
            and mexc_apt_price_data.bid > 0
        ):
            # Orderbook boundaries
            buy_ob = self._orderbook_cumulative_usdt(bybit_apt_price_data.asks) if bybit_apt_price_data.asks else []
            sell_ob = self._orderbook_cumulative_qty_as_usdt(mexc_apt_price_data.bids, mexc_apt_price_data.bid) if (mexc_apt_price_data.bids and mexc_apt_price_data.bid > 0) else []
            bounds = sorted(set(buy_ob + sell_ob)) or None
            max_feas = self.max_trade_usdt

            def _pf_apt_bm(sz):
                if sz <= 0:
                    return -999.0
                ea = bybit_apt_price_data.effective_buy_price(sz)
                if ea <= 0:
                    return -999.0
                apt_bought = sz / (ea * (1.0 + bybit_apt_fee))
                eb = mexc_apt_price_data.effective_sell_price(apt_bought)
                if eb <= 0:
                    return -999.0
                return apt_bought * eb * (1.0 - mexc_apt_fee) - sz

            if max_feas >= self.min_trade_usdt and self.optimal_size_enabled:
                trade_usdt_apt_bybit_mexc, profit_usdt_apt_bybit_mexc = self._find_optimal_trade_size(_pf_apt_bm, max_feas, bounds)
            else:
                trade_usdt_apt_bybit_mexc = max_feas
                profit_usdt_apt_bybit_mexc = _pf_apt_bm(max_feas) if max_feas >= self.min_trade_usdt else -999.0

        # Direction 4: Buy APT on MEXC -> Sell APT on Bybit (USDT -> APT -> USDT)
        profit_usdt_apt_mexc_bybit = -999.0
        trade_usdt_apt_mexc_bybit = 0.0
        if (
            bybit_apt_price_data
            and mexc_apt_price_data
            and not bybit_apt_price_data.is_stale(max_age=30)
            and not mexc_apt_price_data.is_stale(max_age=30)
            and mexc_apt_price_data.ask > 0
            and bybit_apt_price_data.bid > 0
        ):
            # Orderbook boundaries
            buy_ob = self._orderbook_cumulative_usdt(mexc_apt_price_data.asks) if mexc_apt_price_data.asks else []
            sell_ob = self._orderbook_cumulative_qty_as_usdt(bybit_apt_price_data.bids, bybit_apt_price_data.bid) if (bybit_apt_price_data.bids and bybit_apt_price_data.bid > 0) else []
            bounds = sorted(set(buy_ob + sell_ob)) or None
            max_feas = self.max_trade_usdt

            def _pf_apt_mb(sz):
                if sz <= 0:
                    return -999.0
                ea = mexc_apt_price_data.effective_buy_price(sz)
                if ea <= 0:
                    return -999.0
                apt_bought = sz / (ea * (1.0 + mexc_apt_fee))
                eb = bybit_apt_price_data.effective_sell_price(apt_bought)
                if eb <= 0:
                    return -999.0
                return apt_bought * eb * (1.0 - bybit_apt_fee) - sz

            if max_feas >= self.min_trade_usdt and self.optimal_size_enabled:
                trade_usdt_apt_mexc_bybit, profit_usdt_apt_mexc_bybit = self._find_optimal_trade_size(_pf_apt_mb, max_feas, bounds)
            else:
                trade_usdt_apt_mexc_bybit = max_feas
                profit_usdt_apt_mexc_bybit = _pf_apt_mb(max_feas) if max_feas >= self.min_trade_usdt else -999.0

        pct_apt_bybit_mexc = (profit_usdt_apt_bybit_mexc / trade_usdt_apt_bybit_mexc * 100) if trade_usdt_apt_bybit_mexc > 0 else 0
        if profit_usdt_apt_bybit_mexc > self.min_profit_cex_to_cex and pct_apt_bybit_mexc > self.min_profit_pct_cex_to_cex:
            # De-duplicate
            if self.deduplicator.should_log("CEX_TO_CEX_APT_BYBIT_MEXC", bybit_apt_price_data.ask, mexc_apt_price_data.bid):
                profit_pct = pct_apt_bybit_mexc
                det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                logger.success(
                    f"🎯 [CEX-CEX APT] BYBIT→MEXC ARB FOUND @ {det_time}  "
                    f"buy_bybit_apt={bybit_apt_price_data.ask:.6f}  sell_mexc_apt={mexc_apt_price_data.bid:.6f}  "
                    f"profit=${profit_usdt_apt_bybit_mexc:.4f} ({profit_pct:.2f}%)  size=${trade_usdt_apt_bybit_mexc:.2f}"
                )
                log_signal({
                    "type": "cex_to_cex_apt",
                    "buy_exchange": "bybit",
                    "sell_exchange": "mexc",
                    "buy_price": bybit_apt_price_data.ask,
                    "sell_price": mexc_apt_price_data.bid,
                    "profit_usd": profit_usdt_apt_bybit_mexc,
                    "profit_pct": profit_pct,
                    "trade_size_usdt": trade_usdt_apt_bybit_mexc,
                })
                self._log_and_execute({
                    "direction": "CEX_TO_CEX_APT_BYBIT_MEXC",
                    "buy_exchange": "bybit",
                    "sell_exchange": "mexc",
                    "profit_usd": profit_usdt_apt_bybit_mexc,
                    "profit_token": "USDT",
                    "profit_amount": profit_usdt_apt_bybit_mexc,
                    "profit_pct": profit_pct,
                    "trade_size_usdt": trade_usdt_apt_bybit_mexc,
                    "bybit_apt_ask": bybit_apt_price_data.ask,
                    "bybit_apt_bid": bybit_apt_price_data.bid,
                    "mexc_apt_ask": mexc_apt_price_data.ask,
                    "mexc_apt_bid": mexc_apt_price_data.bid,
                    "bybit_apt_fee": bybit_apt_fee,
                    "mexc_apt_fee": mexc_apt_fee,
                    "bybit_apt_bid_qty": bybit_apt_price_data.bid_qty,
                    "bybit_apt_ask_qty": bybit_apt_price_data.ask_qty,
                    "mexc_apt_bid_qty": mexc_apt_price_data.bid_qty,
                    "mexc_apt_ask_qty": mexc_apt_price_data.ask_qty,
                })

        pct_apt_mexc_bybit = (profit_usdt_apt_mexc_bybit / trade_usdt_apt_mexc_bybit * 100) if trade_usdt_apt_mexc_bybit > 0 else 0
        if profit_usdt_apt_mexc_bybit > self.min_profit_cex_to_cex and pct_apt_mexc_bybit > self.min_profit_pct_cex_to_cex:
            # De-duplicate
            if self.deduplicator.should_log("CEX_TO_CEX_APT_MEXC_BYBIT", mexc_apt_price_data.ask, bybit_apt_price_data.bid):
                profit_pct = pct_apt_mexc_bybit
                det_time = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                logger.success(
                    f"🎯 [CEX-CEX APT] MEXC→BYBIT ARB FOUND @ {det_time}  "
                    f"buy_mexc_apt={mexc_apt_price_data.ask:.6f}  sell_bybit_apt={bybit_apt_price_data.bid:.6f}  "
                    f"profit=${profit_usdt_apt_mexc_bybit:.4f} ({profit_pct:.2f}%)  size=${trade_usdt_apt_mexc_bybit:.2f}"
                )
                log_signal({
                    "type": "cex_to_cex_apt",
                    "buy_exchange": "mexc",
                    "sell_exchange": "bybit",
                    "buy_price": mexc_apt_price_data.ask,
                    "sell_price": bybit_apt_price_data.bid,
                    "profit_usd": profit_usdt_apt_mexc_bybit,
                    "profit_pct": profit_pct,
                    "trade_size_usdt": trade_usdt_apt_mexc_bybit,
                })
                self._log_and_execute({
                    "direction": "CEX_TO_CEX_APT_MEXC_BYBIT",
                    "buy_exchange": "mexc",
                    "sell_exchange": "bybit",
                    "profit_usd": profit_usdt_apt_mexc_bybit,
                    "profit_token": "USDT",
                    "profit_amount": profit_usdt_apt_mexc_bybit,
                    "profit_pct": profit_pct,
                    "trade_size_usdt": trade_usdt_apt_mexc_bybit,
                    "bybit_apt_ask": bybit_apt_price_data.ask,
                    "bybit_apt_bid": bybit_apt_price_data.bid,
                    "mexc_apt_ask": mexc_apt_price_data.ask,
                    "mexc_apt_bid": mexc_apt_price_data.bid,
                    "bybit_apt_fee": bybit_apt_fee,
                    "mexc_apt_fee": mexc_apt_fee,
                    "bybit_apt_bid_qty": bybit_apt_price_data.bid_qty,
                    "bybit_apt_ask_qty": bybit_apt_price_data.ask_qty,
                    "mexc_apt_bid_qty": mexc_apt_price_data.bid_qty,
                    "mexc_apt_ask_qty": mexc_apt_price_data.ask_qty,
                })
