"""
TradeExecutor — orchestrates concurrent order placement for arbitrage legs.

Modes:
  DRY_RUN=true  (default): log what would be executed, no real orders.
  DRY_RUN=false           : execute real orders on both legs simultaneously.

Execution strategies:
  CEX-CEX arb  → Bybit market order + MEXC market order in parallel.
  Multi-leg    → Sequential or parallel legs with rollback on partial failure.

Balance checking:
  Before every trade, BalanceManager validates sufficient funds on each
  exchange for every leg. Trade is rejected if balance is insufficient.

Atomic multi-leg:
  When one leg of a multi-leg trade fails, the executor attempts to
  reverse (unwind) completed legs to minimise position risk.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

import math
import time
from datetime import datetime

from config.settings import settings
from exchanges.bybit_trader import BybitTrader, OrderResult
from exchanges.mexc_trader import MexcTrader
from core.balance_manager import BalanceManager, LegRequirement
from core.cellana_dex_swap import CellanaDexSwap, SwapResult as CellanaSwapResult
from core.price_collector import PriceCollector
from core.hyperion_dex_swap import HyperionDexSwap, SwapResult as HyperionSwapResult
from utils.logger import get_logger, log_signal
from utils.telegram_notifier import notifier as tg_notifier

logger = get_logger()

# Per-leg execution timeout (seconds). Abort if a single API call takes longer.
_LEG_TIMEOUT_S = 30

# Minimum notional value (USDT) per exchange to avoid "lower limit" errors.
# Bybit requires 1-5 USDT depending on pair. AMI/USDT is 5.0.
# MEXC requires 5.0 USDT.
EXCH_MIN_NOTIONAL = {
    "bybit": 5.0,
    "mexc": 5.0,
}


# ---------------------------------------------------------------------------
#  Data structures
# ---------------------------------------------------------------------------
class LegSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass
class TradeLeg:
    """Describes a single order to place."""
    exchange: str       # "bybit", "mexc", or "dex"
    symbol: str         # "AMIUSDT", "APTUSDT", or "APT_AMI" / "AMI_APT"
    side: LegSide       # buy or sell (for DEX: BUY=swap_in→out, SELL unused)
    qty: float          # base-coin quantity
    price_est: float    # estimated fill price (for logging / profit calc)
    tag: str = ""       # human label, e.g. "buy_APT_bybit", "dex_apt_to_ami"
    # DEX-specific fields
    is_dex: bool = False            # True for on-chain DEX swap legs
    dex_direction: str = ""         # "apt_to_ami" or "ami_to_apt"
    dex_min_out: Optional[float] = None  # explicit min output (overrides slippage)


@dataclass
class LegResult:
    """Outcome of one leg execution."""
    leg: TradeLeg
    ok: bool = False
    order_id: Optional[str] = None
    filled_qty: float = 0.0      # actual filled quantity (base coin)
    filled_price: float = 0.0    # average fill price
    fill_status: str = ""        # "Filled", "PartiallyFilled", etc.
    error: str = ""
    elapsed_ms: float = 0.0


@dataclass
class ExecutionResult:
    """Aggregate outcome of a multi-leg trade."""
    ok: bool = False
    legs: List[LegResult] = field(default_factory=list)
    rollback_legs: List[LegResult] = field(default_factory=list)
    profit_est: float = 0.0
    reason: str = ""            # human description if not ok
    balance_details: dict = field(default_factory=dict)


class TradeExecutor:
    """Coordinate simultaneous execution of arbitrage legs."""

    def __init__(
        self,
        balance_manager: Optional[BalanceManager] = None,
        price_collector: Optional[PriceCollector] = None,
    ) -> None:
        self.bybit = BybitTrader()
        self.mexc = MexcTrader()
        self.dry_run = settings.dry_run
        self.price_collector = price_collector

        # On-chain DEX swap executors (lazy init)
        self.cellana_swap: Optional[CellanaDexSwap] = None
        self.hyperion_swap: Optional[HyperionDexSwap] = None
        
        if settings.aptos_private_key:
            try:
                self.cellana_swap = CellanaDexSwap()
            except Exception as e:
                logger.warning(f"CellanaDexSwap init failed: {e}")

            try:
                self.hyperion_swap = HyperionDexSwap()
            except Exception as e:
                logger.warning(f"HyperionDexSwap init failed: {e}")

        # BalanceManager (optional — if None, balance checks are skipped)
        self.balance_manager = balance_manager

        mode = "DRY-RUN (paper)" if self.dry_run else "🔴 LIVE TRADING"
        dex_status = f"Cellana={'ON' if self.cellana_swap else 'OFF'}, Hyperion={'ON' if self.hyperion_swap else 'OFF'}"
        logger.info(
            f"TradeExecutor initialised | mode={mode} "
            f"max_trade={settings.trade_amount_usdt} USDT  "
            f"balance_check={'ON' if balance_manager else 'OFF'}  "
            f"dex_swap=[{dex_status}]"
        )

    async def init_traders(self) -> None:
        """Initialize traders by syncing server time and instrument info."""
        # Main symbols to sync
        symbols = [settings.cex_symbol, settings.apt_cex_symbol]
        
        # sync_server_time is already called inside get_balance/place_order 
        # but we do it here once to be ready.
        await asyncio.gather(
            self.bybit.sync_server_time(),
            self.mexc.sync_server_time()
        )
        
        # Sync instrument info for rounding
        tasks = []
        for sym in symbols:
            tasks.append(self.bybit.sync_instrument_info(sym))
            tasks.append(self.mexc.sync_instrument_info(sym))
        
        await asyncio.gather(*tasks)
        logger.info("✅ Traders initialized with instrument precision info")

    async def close(self) -> None:
        """Close all traders and executors."""
        try:
            if self.cellana_swap:
                await self.cellana_swap.close()
            if self.hyperion_swap:
                await self.hyperion_swap.close()
            await self.bybit.close()
            await self.mexc.close()
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    #  Signal logging helper
    # ------------------------------------------------------------------ #
    def _emit_signal(self, payload: dict) -> None:
        """Write a structured signal block to console + logs/signals.jsonl."""
        now = time.time()
        payload["dry_run"] = self.dry_run
        payload["ts"]      = now
        payload["detected_at"] = datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        mode_tag = "[DRY-SIGNAL]" if self.dry_run else "[LIVE-SIGNAL]"
        time_str = payload["detected_at"]

        lines = [f"\n{'━'*56}  {mode_tag}  {time_str}"]
        for k, v in payload.items():
            if k in ("dry_run", "ts"):
                continue
            if isinstance(v, float):
                lines.append(f"  {k:<22}: {v:.8g}")
            elif isinstance(v, dict):
                lines.append(f"  {k:<22}:")
                for bk, bv in v.items():
                    if isinstance(bv, dict):
                        status = "✅" if bv.get("ok") else "⚠️ LOW"
                        lines.append(
                            f"    {bk:<20}: bal={bv.get('free')!s:>12}  "
                            f"need={bv.get('need')!s:>12}  {status}"
                        )
                    else:
                        lines.append(f"    {bk:<20}: {bv}")
            else:
                lines.append(f"  {k:<22}: {v}")
        lines.append(f"{'━'*64}")
        logger.info("\n".join(lines))
        log_signal(payload)

    # ------------------------------------------------------------------ #
    #  Pre-trade balance check
    # ------------------------------------------------------------------ #
    async def _check_balances(
        self,
        legs: List[TradeLeg],
        direction: str,
        parallel: bool = True,
    ) -> tuple[bool, dict]:
        """Validate balances for all legs.

        If parallel=True (default), we ensure we have enough for ALL legs upfront.
        If parallel=False, we simulate the flow of funds (proceeds from leg i satisfy leg i+1).
        
        Returns (ok, details_dict).
        """
        if not self.balance_manager:
            return True, {}

        # 1. Fresh balances for all involved exchanges
        # Normalization: Map all DEX-involved legs (Hyperion, Cellana) to "dex" key
        exchanges_needed = {
            "dex" if leg.is_dex else leg.exchange.lower() 
            for leg in legs
        }
        for exch in exchanges_needed:
            await self.balance_manager.ensure_fresh(exch)

        # 2. Parallel Validation (Legacy approach)
        if parallel:
            requirements: list[LegRequirement] = []
            for leg in legs:
                if leg.is_dex:
                    asset = "APT" if leg.dex_direction == "apt_to_ami" else "AMI"
                    requirements.append(LegRequirement(
                        exchange="dex", asset=asset, amount=leg.qty, side="sell",
                        symbol=f"DEX_{leg.dex_direction}",
                    ))
                elif leg.side == LegSide.BUY:
                    usdt_needed = leg.qty * leg.price_est * 1.002
                    requirements.append(LegRequirement(
                        exchange=leg.exchange, asset="USDT", amount=usdt_needed, side="buy",
                        symbol=leg.symbol,
                    ))
                else:
                    base_coin = _cex_coin_for(leg.symbol)
                    requirements.append(LegRequirement(
                        exchange=leg.exchange, asset=base_coin, amount=leg.qty, side="sell",
                        symbol=leg.symbol,
                    ))

            result = await self.balance_manager.check_legs(requirements)
            if not result.ok:
                logger.warning(f"⚠️ Parallel balance check FAILED for {direction}: {result.reason}")
            return result.ok, result.details

        # 3. Sequential (Virtual) Validation
        # Start with a snapshot of current free balances
        virtual_balances = {}
        for exch in exchanges_needed:
            virtual_balances[exch] = {
                "USDT": self.balance_manager.get_free(exch, "USDT"),
                "AMI": self.balance_manager.get_free(exch, "AMI"),
                "APT": self.balance_manager.get_free(exch, "APT"),
            }

        buffer_pct = self.balance_manager.reserve_buffer_pct
        all_ok = True
        reasons = []
        details = {}

        for i, leg in enumerate(legs):
            # Normalization: Map all DEX-involved legs to "dex" key
            ex_id = "dex" if leg.is_dex else leg.exchange.lower()
            
            if leg.is_dex:
                asset_in = "APT" if leg.dex_direction == "apt_to_ami" else "AMI"
                asset_out = "AMI" if leg.dex_direction == "apt_to_ami" else "APT"
                need = leg.qty * (1.0 + buffer_pct)
                free = virtual_balances[ex_id].get(asset_in, 0.0)
                
                if free < need:
                    all_ok = False
                    reasons.append(f"Leg {i+1} ({leg.tag}): {ex_id} {asset_in} low (need {need:.4f}, have {free:.4f})")
                
                # Update virtual: Spend input, receive output (conservative estimate)
                virtual_balances[ex_id][asset_in] -= leg.qty
                # Use price_est - 1% slippage buffer for what we expect to GET
                est_out = leg.qty * leg.price_est * 0.99
                virtual_balances[ex_id][asset_out] = virtual_balances[ex_id].get(asset_out, 0.0) + est_out

            elif leg.side == LegSide.BUY:
                base_coin = _cex_coin_for(leg.symbol)
                usdt_needed = leg.qty * leg.price_est * (1.0 + buffer_pct + 0.002) # + buffer + 0.2% fee
                free_usdt = virtual_balances[ex_id].get("USDT", 0.0)
                
                if free_usdt < usdt_needed:
                    all_ok = False
                    reasons.append(f"Leg {i+1} ({leg.tag}): {ex_id} USDT low (need {usdt_needed:.2f}, have {free_usdt:.2f})")
                
                # Update virtual: Spend USDT, receive base coin
                virtual_balances[ex_id]["USDT"] -= (leg.qty * leg.price_est * 1.002)
                virtual_balances[ex_id][base_coin] = virtual_balances[ex_id].get(base_coin, 0.0) + leg.qty
            
            else: # SELL
                base_coin = _cex_coin_for(leg.symbol)
                need = leg.qty * (1.0 + buffer_pct)
                free_base = virtual_balances[ex_id].get(base_coin, 0.0)
                
                if free_base < need:
                    all_ok = False
                    reasons.append(f"Leg {i+1} ({leg.tag}): {ex_id} {base_coin} low (need {need:.4f}, have {free_base:.4f})")
                
                # Update virtual: Spend base coin, receive USDT proceeds (conservative)
                virtual_balances[ex_id][base_coin] -= leg.qty
                usdt_proceeds = leg.qty * leg.price_est * 0.997 # 0.2% fee + tiny price buffer
                virtual_balances[ex_id]["USDT"] = virtual_balances[ex_id].get("USDT", 0.0) + usdt_proceeds

        # Prepare details for signal logging (show initial free balances)
        for exch in exchanges_needed:
            for asset in ["USDT", "AMI", "APT"]:
                key = f"{exch}:{asset}"
                free_now = self.balance_manager.get_free(exch, asset)
                details[key] = {
                    "free": round(free_now, 6),
                    "need": "virtual",
                    "ok": True # We mark it OK for the signal block unless it was a terminal failure
                }

        if not all_ok:
            logger.warning(f"⚠️ Sequential check FAILED for {direction}: {'; '.join(reasons)}")
            # Mark the specific key that failed in details so UI shows ⚠️ LOW
            for r in reasons:
                for k in details:
                    if k.replace(":", " ").upper() in r.upper():
                        details[k]["ok"] = False

        return all_ok, details

    def _validate_leg_limits(self, legs: List[TradeLeg]) -> tuple[bool, str]:
        """Verify if all legs meet the exchange minimum notional requirements."""
        for leg in legs:
            if leg.is_dex:
                continue
            
            exch = leg.exchange.lower()
            min_limit = EXCH_MIN_NOTIONAL.get(exch, 0.0)
            if min_limit <= 0:
                continue
            
            # Value calculation
            # For CEX, if side is BUY, we use qty * price_est (USDT)
            # If side is SELL, we also use qty * price_est (USDT value of base coin)
            trade_value = leg.qty * leg.price_est
            
            if trade_value < min_limit:
                return False, f"Leg {leg.tag} value ${trade_value:.4f} is below {exch.upper()} minimum of ${min_limit:.2f}"
                
        return True, ""

    # ------------------------------------------------------------------ #
    #  Low-level leg execution
    # ------------------------------------------------------------------ #
    async def _execute_leg(self, leg: TradeLeg) -> LegResult:
        """Execute a single TradeLeg on the correct exchange or DEX."""
        t0 = time.time()

        # ── DEX on-chain swap ──
        if leg.is_dex:
            return await self._execute_dex_leg(leg, t0)

        # ── CEX order ──
        try:
            fill = None
            logger.debug(f"⏳ [TradeExecutor] Executing {leg.side.value} {leg.qty} {leg.symbol} on {leg.exchange}...")

            if leg.side == LegSide.BUY:
                fill = await asyncio.wait_for(
                    self._cex_buy(leg.exchange, leg.symbol, leg.qty, leg.price_est),
                    timeout=_LEG_TIMEOUT_S,
                )
            else:
                fill = await asyncio.wait_for(
                    self._cex_sell(leg.exchange, leg.symbol, leg.qty),
                    timeout=_LEG_TIMEOUT_S,
                )

            elapsed = (time.time() - t0) * 1000
            if fill:
                res = LegResult(
                    leg=leg, ok=True,
                    order_id=fill.order_id,
                    filled_qty=fill.filled_qty,
                    filled_price=fill.filled_price,
                    fill_status=fill.status,
                    elapsed_ms=elapsed,
                )
            else:
                res = LegResult(leg=leg, ok=False, error="order returned None", elapsed_ms=elapsed)

            # Notify step
            await self._notify_leg_result(res)
            return res

        except asyncio.TimeoutError:
            elapsed = (time.time() - t0) * 1000
            res = LegResult(leg=leg, ok=False, error=f"timeout after {_LEG_TIMEOUT_S}s", elapsed_ms=elapsed)
            await self._notify_leg_result(res)
            return res
        except Exception as e:
            elapsed = (time.time() - t0) * 1000
            res = LegResult(leg=leg, ok=False, error=str(e), elapsed_ms=elapsed)
            await self._notify_leg_result(res)
            return res

    # ------------------------------------------------------------------ #
    #  DEX on-chain leg execution
    # ------------------------------------------------------------------ #
    async def _execute_dex_leg(self, leg: TradeLeg, t0: float) -> LegResult:
        """Execute an on-chain DEX swap via Cellana or Hyperion."""
        # Route to the correct DEX executor
        dex_executor = None
        label = "DEX"
        
        if leg.exchange.lower() == "hyperion":
            dex_executor = self.hyperion_swap
            label = "Hyperion"
        else:
            # Default to Cellana for "dex" or "cellana"
            dex_executor = self.cellana_swap
            label = "Cellana"

        if not dex_executor:
            return LegResult(
                leg=leg, ok=False,
                error=f"{label}Swap not initialised (missing APTOS_PRIVATE_KEY?)",
                elapsed_ms=(time.time() - t0) * 1000,
            )

        try:
            swap_result = None

            if leg.dex_direction == "apt_to_ami":
                swap_result = await asyncio.wait_for(
                    dex_executor.swap_apt_to_ami(
                        amount_apt=leg.qty,
                        min_ami_out=leg.dex_min_out,
                    ),
                    timeout=_LEG_TIMEOUT_S,
                )
            elif leg.dex_direction == "ami_to_apt":
                swap_result = await asyncio.wait_for(
                    dex_executor.swap_ami_to_apt(
                        amount_ami=leg.qty,
                        min_apt_out=leg.dex_min_out,
                    ),
                    timeout=_LEG_TIMEOUT_S,
                )
            else:
                return LegResult(
                    leg=leg, ok=False,
                    error=f"Unknown dex_direction: {leg.dex_direction}",
                    elapsed_ms=(time.time() - t0) * 1000,
                )

            res = LegResult(
                leg=leg,
                ok=swap_result.ok,
                order_id=swap_result.tx_hash,
                error=swap_result.error,
                elapsed_ms=swap_result.elapsed_ms,
            )
            await self._notify_leg_result(res)
            return res

        except asyncio.TimeoutError:
            elapsed = (time.time() - t0) * 1000
            res = LegResult(
                leg=leg, ok=False,
                error=f"DEX swap timeout after {_LEG_TIMEOUT_S}s",
                elapsed_ms=elapsed,
            )
            await self._notify_leg_result(res)
            return res
        except Exception as e:
            elapsed = (time.time() - t0) * 1000
            res = LegResult(leg=leg, ok=False, error=str(e), elapsed_ms=elapsed)
            await self._notify_leg_result(res)
            return res

    # ------------------------------------------------------------------ #
    #  Rollback / unwind
    # ------------------------------------------------------------------ #
    async def _rollback_legs(self, completed: List[LegResult]) -> List[LegResult]:
        """Attempt to reverse completed legs to unwind a partial fill.

        For each completed BUY → place a SELL of same qty.
        For each completed SELL → place a BUY of same qty.

        Returns list of rollback LegResults.
        """
        rollback_results: List[LegResult] = []

        for lr in completed:
            if not lr.ok:
                continue  # nothing to unwind

            # DEX leg rollback: reverse direction
            if lr.leg.is_dex:
                reverse_dir = (
                    "ami_to_apt" if lr.leg.dex_direction == "apt_to_ami"
                    else "apt_to_ami"
                )
                rollback_qty = lr.filled_qty if lr.filled_qty > 0 else lr.leg.qty
                reverse_leg = TradeLeg(
                    exchange="dex",
                    symbol=lr.leg.symbol,
                    side=LegSide.BUY,
                    qty=rollback_qty,
                    price_est=lr.leg.price_est,
                    tag=f"ROLLBACK_{lr.leg.tag}",
                    is_dex=True,
                    dex_direction=reverse_dir,
                    dex_min_out=None,  # use default slippage
                )
                logger.warning(
                    f"↩️  Rolling back DEX {lr.leg.tag}: "
                    f"{reverse_dir} {rollback_qty:.6f}"
                )
            else:
                # CEX leg rollback: reverse side, use actual filled qty
                reverse_side = LegSide.SELL if lr.leg.side == LegSide.BUY else LegSide.BUY
                rollback_qty = lr.filled_qty if lr.filled_qty > 0 else lr.leg.qty
                rollback_price = lr.filled_price if lr.filled_price > 0 else lr.leg.price_est
                reverse_leg = TradeLeg(
                    exchange=lr.leg.exchange,
                    symbol=lr.leg.symbol,
                    side=reverse_side,
                    qty=rollback_qty,
                    price_est=rollback_price,
                    tag=f"ROLLBACK_{lr.leg.tag}",
                )
                logger.warning(
                    f"↩️  Rolling back {lr.leg.tag}: {reverse_side.value} "
                    f"{rollback_qty:.6f} {lr.leg.symbol} on {lr.leg.exchange}"
                )

            r = await self._execute_leg(reverse_leg)
            rollback_results.append(r)

            if r.ok:
                logger.info(f"↩️  Rollback OK: {r.order_id}")
            else:
                logger.error(
                    f"❌ Rollback FAILED for {reverse_leg.tag}: {r.error}  "
                    f"→ MANUAL INTERVENTION REQUIRED"
                )
                await tg_notifier.send_message(f"🚨 <b>Rollback FAILED</b>\nTag: <code>{reverse_leg.tag}</code>\nError: <code>{r.error}</code>")

        return rollback_results

    async def execute_multi_leg(
        self,
        direction: str,
        legs: List[TradeLeg],
        profit_est: float = 0.0,
        parallel: bool = True,
        trade_steps: Optional[List[dict]] = None,
    ) -> ExecutionResult:
        """Execute multiple trade legs with balance check and rollback.

        Args:
            direction: cycle name for logging (e.g. "CEX_TO_CEX_BYBIT_MEXC").
            legs: ordered list of TradeLeg objects.
            profit_est: estimated profit in USDT.
            parallel: if True, execute all legs simultaneously.
            trade_steps: optional list of steps for detailed logging.

        Returns:
            ExecutionResult with per-leg details and rollback info.
        """
        result = ExecutionResult(profit_est=profit_est)

        # Proactive logging of trade steps (ensure visibility even if fail)
        if trade_steps:
             logger.info(f"🚀 [TradeExecutor] Starting {direction} | steps={len(trade_steps)}")
             for i, step in enumerate(trade_steps):
                 logger.debug(f"  Step {i+1}: {step.get('exchange')} {step.get('side')} {step.get('qty')} {step.get('symbol')}")

        # ── Step 1: Minimum Notional Check ──
        limits_ok, limit_reason = self._validate_leg_limits(legs)
        if not limits_ok:
            result.reason = limit_reason
            logger.warning(f"⚠️ {direction} REJECTED: {limit_reason}")
            self._emit_signal({
                "type": "LIMIT_REJECTED",
                "direction": direction,
                "profit_est": profit_est,
                "reason": limit_reason,
                "trade_steps": trade_steps
            })
            return result

        # ── Step 2: Pre-trade balance check (All legs) ──
        if legs:
            balance_ok, balance_details = await self._check_balances(legs, direction, parallel=parallel)
            result.balance_details = balance_details
            if not balance_ok:
                result.reason = "insufficient balance"
                self._emit_signal({
                    "type": "BALANCE_REJECTED",
                    "direction": direction,
                    "profit_est": profit_est,
                    "balances": balance_details,
                    "trade_steps": trade_steps
                })
                return result

        # ── Step 2: DRY RUN mode ──
        if self.dry_run:
            leg_info = []
            for leg in legs:
                leg_info.append({
                    "tag": leg.tag,
                    "exchange": leg.exchange,
                    "symbol": leg.symbol,
                    "side": leg.side.value,
                    "qty": leg.qty,
                    "price_est": leg.price_est,
                })
            self._emit_signal({
                "type": "MULTI_LEG_DRY",
                "direction": direction,
                "legs": leg_info,
                "profit_est": profit_est,
                "balances": {},
                "trade_steps": trade_steps
            })
            result.ok = True
            result.legs = [LegResult(leg=l, ok=True, order_id="DRY") for l in legs]
            return result

        # ── Step 3: LIVE execution ──
        # Take balance snapshot before trade
        bal_before = 0.0
        pre_trade_balances = {}
        if self.balance_manager:
            try:
                await self.balance_manager.refresh_all(force=True)
                bal_before = self.balance_manager.get_total_usd_value(self.price_collector)
                for ex in ["bybit", "mexc", "dex"]:
                    pre_trade_balances[ex] = {
                        "USDT": self.balance_manager.get_free(ex, "USDT"),
                        "AMI": self.balance_manager.get_free(ex, "AMI"),
                        "APT": self.balance_manager.get_free(ex, "APT")
                    }
            except Exception as b_err:
                logger.warning(f"Failed to take pre-trade balance snapshot: {b_err}")

        logger.info(
            f"🔴 EXECUTING {direction} | {len(legs)} legs | est_profit=${profit_est:.4f}"
        )

        # ── Execution Loop ─────────────────────────────────────────────
        ok_legs = []
        failed_legs = []
        statuses = []
        order_ids = []
        
        # Accumulate status updates to send a single TG message at the end
        tg_updates = [f"<b>🚀 Executing {direction}</b>"]

        for i, leg in enumerate(legs):
            leg_name = leg.tag or f"leg_{i}"
            logger.info(f"👉 [{direction}] Executing {leg_name}: {leg.side.value} {leg.qty} {leg.symbol} on {leg.exchange}")
            
            # Step notification (Internal log only, store for single TG msg)
            leg_info = f"• {leg.exchange.upper()}: {leg.side.value} {leg.qty:.4f} {leg.symbol}"
            tg_updates.append(leg_info)

            res = await self._execute_leg(leg)
            if res and res.ok:
                order_ids.append(res.order_id)
                ok_legs.append(leg_name)
                tg_updates[-1] += f" ✅"
                result.legs.append(res)
            else:
                failed_legs.append(leg_name)
                tg_updates[-1] += f" ❌ (Fail: {res.error if res else 'None'})"
                logger.error(f"❌ [{direction}] {leg_name} FAILED on {leg.exchange}")
                if res: result.legs.append(res)
                if not parallel: break

        # ── Post-Execution Report ──────────────────────────────────────
        is_success = len(failed_legs) == 0
        real_profit = 0.0
        
        if is_success:
            # Re-fetch balances to calculate real profit
            await self.balance_manager.refresh_all(force=True)
            bal_after = self.balance_manager.get_total_usd_value(self.price_collector)
            real_profit = bal_after - bal_before
            
            msg = (
                f"✅ {direction} SUCCESS | Est: ${profit_est:.4f} | Real: ${real_profit:.4f}"
            )
            logger.success(msg)

            # --- Detailed Terminal Balance Log ---
            try:
                b_log = f"📊 [{direction}] Balance Changes:\n"
                for ex in ["bybit", "mexc", "dex"]:
                    pre = pre_trade_balances.get(ex, {"USDT": 0.0, "AMI": 0.0, "APT": 0.0})
                    # Re-fetch cur from cache (already refreshed above)
                    cur = {
                        "USDT": self.balance_manager.get_free(ex, "USDT"),
                        "AMI": self.balance_manager.get_free(ex, "AMI"),
                        "APT": self.balance_manager.get_free(ex, "APT")
                    }
                    
                    changes = []
                    for asset in ["USDT", "AMI", "APT"]:
                        p_val = float(pre.get(asset, 0.0))
                        c_val = float(cur.get(asset, 0.0))
                        diff = c_val - p_val
                        if abs(diff) < 1e-9 and c_val == 0: continue
                        
                        sign = "+" if diff > 0 else ""
                        if asset == "USDT":
                            changes.append(f"{asset} {p_val:.2f} -> {c_val:.2f} ({sign}{diff:.2f})")
                        elif asset == "AMI":
                            changes.append(f"{asset} {p_val:.0f} -> {c_val:.0f} ({sign}{diff:.0f})")
                        else: # APT
                            changes.append(f"{asset} {p_val:.4f} -> {c_val:.4f} ({sign}{diff:.4f})")
                    
                    if changes:
                        label = ex.upper() if ex != "dex" else "APTOS"
                        b_log += f"  • {label:6}: {' | '.join(changes)}\n"
                
                logger.success(b_log.strip())
                
                # --- Add to Telegram message too ---
                tg_bal_sections = []
                for ex in ["bybit", "mexc", "dex"]:
                    pre = pre_trade_balances.get(ex, {"USDT": 0.0, "AMI": 0.0, "APT": 0.0})
                    cur = {
                        "USDT": self.balance_manager.get_free(ex, "USDT"),
                        "AMI": self.balance_manager.get_free(ex, "AMI"),
                        "APT": self.balance_manager.get_free(ex, "APT")
                    }
                    ex_chg = []
                    for asset in ["USDT", "AMI", "APT"]:
                        p_v = float(pre.get(asset, 0.0))
                        c_v = float(cur.get(asset, 0.0))
                        d = c_v - p_v
                        if abs(d) < 1e-9: continue
                        
                        s = "+" if d > 0 else ""
                        if asset == "USDT":
                            ex_chg.append(f"{asset} {p_v:.1f}→{c_v:.1f}")
                        elif asset == "AMI":
                            ex_chg.append(f"{asset} {p_v:.0f}→{c_v:.0f}")
                        else:
                            ex_chg.append(f"{asset} {p_v:.2f}→{c_v:.2f}")
                    
                    if ex_chg:
                        label = ex.upper() if ex != "dex" else "APTOS"
                        tg_bal_sections.append(f"• <b>{label}</b>: {', '.join(ex_chg)}")
                
                if tg_bal_sections:
                    tg_updates.append("\n⚖️ <b>Balance Changes:</b>")
                    tg_updates.extend(tg_bal_sections)
                    
            except Exception as bl_err:
                logger.debug(f"Failed to format Telegram balance report: {bl_err}")

            tg_updates.append(f"\n💰 <b>Profit (Real): ${real_profit:.4f}</b>")
            tg_updates.append(f"📈 Est: ${profit_est:.4f}")
        else:
            msg = f"❌ {direction} PARTIAL FAIL | ok={ok_legs} failed={failed_legs}"
            logger.error(msg)
            tg_updates.append(f"\n❌ <b>FAIL: {', '.join(failed_legs)}</b>")

        # Final Single Telegram Notification
        try:
            full_tg_msg = "\n".join(tg_updates)
            await tg_notifier.send_message(full_tg_msg)
        except Exception as e:
            logger.error(f"Failed to send consolidated TG message: {e}")

        # ── Step 5: Finalize & Reporting ──
        try:
            await self._finalize_trade_report(
                direction, result, pre_trade_balances, profit_est, trade_steps,
                real_profit_override=real_profit
            )
        except Exception as f_err:
            logger.error(f"Error generating final report: {f_err}")

        return result

    # ------------------------------------------------------------------ #
    #  CEX ↔ CEX  (Bybit / MEXC)  — convenience wrapper
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

        Wraps execute_multi_leg with balance check and rollback.
        Returns True if both legs succeeded.
        """
        # Safety cap on quantity
        max_qty = settings.trade_amount_usdt / buy_price
        safe_qty = min(qty, max_qty)

        buy_fee  = settings.bybit_fee if buy_exchange.lower()  == "bybit" else settings.mexc_fee
        sell_fee = settings.bybit_fee if sell_exchange.lower() == "bybit" else settings.mexc_fee
        net_profit_est = (
            (sell_price - buy_price) * safe_qty
            - buy_price  * safe_qty * buy_fee
            - sell_price * safe_qty * sell_fee
        )

        legs = [
            TradeLeg(
                exchange=buy_exchange.lower(),
                symbol=symbol,
                side=LegSide.BUY,
                qty=safe_qty,
                price_est=buy_price,
                tag=f"buy_{symbol}_{buy_exchange}",
            ),
            TradeLeg(
                exchange=sell_exchange.lower(),
                symbol=symbol,
                side=LegSide.SELL,
                qty=safe_qty,
                price_est=sell_price,
                tag=f"sell_{symbol}_{sell_exchange}",
            ),
        ]

        direction = f"CEX_CEX_{buy_exchange.upper()}_{sell_exchange.upper()}"
        result = await self.execute_multi_leg(
            legs=legs,
            direction=direction,
            profit_est=net_profit_est,
            parallel=True,
        )
        return result.ok

    # ------------------------------------------------------------------ #
    #  Multi-leg DEX-involving cycles — convenience wrapper
    # ------------------------------------------------------------------ #
    async def execute_dex_to_cex(
        self,
        buy_exchange: str,    # CEX to buy APT
        sell_exchange: str,   # CEX to sell AMI
        apt_qty: float,       # APT quantity to buy
        ami_qty: float,       # expected AMI from DEX swap
        apt_price: float,     # APT/USDT price
        ami_price: float,     # AMI/USDT price (for sell)
        profit_est: float = 0.0,
    ) -> ExecutionResult:
        """Execute USDT → APT(CEX) → AMI(DEX) → USDT(CEX) cycle.

        Note: DEX swap leg is NOT executed here (requires on-chain tx).
        Only the CEX legs are placed. Caller must handle DEX swap separately.
        """
        legs = [
            TradeLeg(
                exchange=buy_exchange.lower(),
                symbol=settings.apt_cex_symbol,
                side=LegSide.BUY,
                qty=apt_qty,
                price_est=apt_price,
                tag=f"buy_APT_{buy_exchange}",
            ),
            TradeLeg(
                exchange=sell_exchange.lower(),
                symbol=settings.cex_symbol,
                side=LegSide.SELL,
                qty=ami_qty,
                price_est=ami_price,
                tag=f"sell_AMI_{sell_exchange}",
            ),
        ]

        direction = f"DEX_TO_CEX_{buy_exchange.upper()}_{sell_exchange.upper()}"
        return await self.execute_multi_leg(
            legs=legs,
            direction=direction,
            profit_est=profit_est,
            parallel=False,  # sequential: buy APT first, then sell AMI after DEX swap
        )

    async def execute_ami_cycle(
        self,
        buy_exchange: str,    # CEX to buy AMI
        sell_exchange: str,   # CEX to sell APT
        ami_qty: float,       # AMI quantity to buy
        apt_qty: float,       # expected APT from DEX swap
        ami_price: float,     # AMI/USDT price (for buy)
        apt_price: float,     # APT/USDT price (for sell)
        profit_est: float = 0.0,
    ) -> ExecutionResult:
        """Execute USDT → AMI(CEX) → APT(DEX) → USDT(CEX) cycle.

        Note: DEX swap leg is NOT executed here.
        Only the CEX legs are placed.
        """
        legs = [
            TradeLeg(
                exchange=buy_exchange.lower(),
                symbol=settings.cex_symbol,
                side=LegSide.BUY,
                qty=ami_qty,
                price_est=ami_price,
                tag=f"buy_AMI_{buy_exchange}",
            ),
            TradeLeg(
                exchange=sell_exchange.lower(),
                symbol=settings.apt_cex_symbol,
                side=LegSide.SELL,
                qty=apt_qty,
                price_est=apt_price,
                tag=f"sell_APT_{sell_exchange}",
            ),
        ]

        direction = f"AMI_CYCLE_{buy_exchange.upper()}_{sell_exchange.upper()}"
        return await self.execute_multi_leg(
            legs=legs,
            direction=direction,
            profit_est=profit_est,
            parallel=False,  # sequential
        )

    # ------------------------------------------------------------------ #
    #  Telegram Notification Helpers
    # ------------------------------------------------------------------ #
    async def _notify_leg_result(self, lr: LegResult) -> None:
        """Send a brief Telegram notice for each finished leg."""
        if self.dry_run:
            status = "🧪 DRY OK"
        else:
            status = "✅ OK" if lr.ok else f"❌ FAIL ({lr.error})"
        
        icon = "⛓️" if lr.leg.is_dex else "🏛️"
        side = "BUY" if lr.leg.side == LegSide.BUY else "SELL"
        
        msg = (
            f"{icon} <b>Step Finish: {lr.leg.tag}</b>\n"
            f"Status: {status}\n"
            f"Action: <code>{side} {lr.leg.qty} {lr.leg.symbol}</code>\n"
            f"Price: <code>{lr.filled_price if lr.filled_price > 0 else lr.leg.price_est}</code>\n"
            f"Time: <code>{lr.elapsed_ms:.0f}ms</code>"
        )
        await tg_notifier.send_message(msg)

    async def _finalize_trade_report(
        self,
        direction: str,
        result: ExecutionResult,
        pre_trade_balances: dict,
        profit_est: float,
        trade_steps: Optional[List[dict]],
        real_profit_override: Optional[float] = None,
    ) -> None:
        """Centralized result logic + Telegram reporting."""
        succeeded = [lr for lr in result.legs if lr.ok]
        failed = [lr for lr in result.legs if not lr.ok]
        
        if not failed and result.legs:
            result.ok = True
            logger.success(f"✅ {direction} OK | profit=${profit_est:.4f}")
            
            if real_profit_override is not None:
                real_profit_usdt = real_profit_override
            else:
                # Fallback: estimate from CEX legs only (less accurate for DEX trades)
                real_profit_usdt = 0.0
                for lr in succeeded:
                    if not lr.leg.is_dex:
                        if lr.leg.side == LegSide.SELL:
                            real_profit_usdt += (lr.filled_price * lr.filled_qty)
                        else:
                            real_profit_usdt -= (lr.filled_price * lr.filled_qty)
            
            self._emit_signal({
                "type": "MULTI_LEG_OK",
                "direction": direction,
                "profit_est": profit_est,
                "profit_real": real_profit_usdt,
                "leg_count": len(result.legs),
                "order_ids": [lr.order_id for lr in succeeded],
                "trade_steps": trade_steps
            })

            tg_tag = "✅ SUCCESS" if not self.dry_run else "🧪 DRY ✅"
            await tg_notifier.send_message(
                f"<b>{tg_tag} Trade Finished</b>\n"
                f"Direction: <code>{direction}</code>\n"
                f"Est. Profit: <b>${profit_est:.4f}</b>\n"
                f"Real Profit: <b>${real_profit_usdt:.4f}</b>"
            )
        elif failed:
            fail_tags = [lr.leg.tag for lr in failed]
            ok_tags = [lr.leg.tag for lr in succeeded]
            fail_reasons = [f"{lr.leg.tag}: {lr.error}" for lr in failed]
            
            logger.error(f"❌ {direction} PARTIAL FAIL | ok={ok_tags} failed={fail_tags}")
            result.rollback_legs = await self._rollback_legs(succeeded)
            rollback_ok = all(r.ok for r in result.rollback_legs) if result.rollback_legs else True
            result.reason = f"partial failure: {fail_reasons}"

            self._emit_signal({
                "type": "MULTI_LEG_PARTIAL",
                "direction": direction,
                "failed_legs": fail_tags,
                "rollback_ok": rollback_ok,
                "trade_steps": trade_steps
            })

            tg_status = "⚠️ PARTIAL FAIL" if rollback_ok else "❌ CRITICAL FAIL"
            tg_tag = f"🧪 DRY {tg_status}" if self.dry_run else tg_status
            await tg_notifier.send_message(
                f"<b>{tg_tag}</b>\n"
                f"Direction: <code>{direction}</code>\n"
                f"Failed: <code>{', '.join(fail_tags)}</code>\n"
                f"Rollback: {'OK' if rollback_ok else 'FAILED!'}"
            )

        # Always send balance comparison if we have pre-balances and ANY leg was attempted
        if self.balance_manager and result.legs:
            try:
                await self.balance_manager.ensure_fresh()
                b_summ = "<b>💰 Balance Comparison:</b>\n"
                has_data = False
                total_diffs = {"USDT": 0.0, "AMI": 0.0, "APT": 0.0}
                
                for ex in ["bybit", "mexc", "dex"]:
                    pre = pre_trade_balances.get(ex, {"USDT": 0.0, "AMI": 0.0, "APT": 0.0})
                    cur = {
                        "USDT": float(self.balance_manager.get_free(ex, "USDT")),
                        "AMI": float(self.balance_manager.get_free(ex, "AMI")),
                        "APT": float(self.balance_manager.get_free(ex, "APT"))
                    }
                    
                    # Only show if there's a significant change or non-zero balance
                    if any(cur[a] > 0 or pre[a] > 0 for a in cur):
                        has_data = True
                        label = ex.upper() if ex != "dex" else "🔗 APTOS"
                        b_summ = b_summ + f"\n• <b>{label}</b>:\n"
                        for asset in ["USDT", "AMI", "APT"]:
                            p_val = float(pre.get(asset, 0.0))
                            diff = cur[asset] - p_val
                            total_diffs[asset] += diff
                            
                            if abs(diff) < 1e-9 and cur[asset] == 0: continue
                            
                            sign = "+" if diff > 0 else ""
                            # Format based on asset
                            if asset == "USDT":
                                fmt_cur, fmt_pre, fmt_diff = f"{cur[asset]:.2f}", f"{p_val:.2f}", f"{sign}{diff:.2f}"
                            elif asset == "AMI":
                                fmt_cur, fmt_pre, fmt_diff = f"{cur[asset]:.0f}", f"{p_val:.0f}", f"{sign}{diff:.0f}"
                            else: # APT
                                fmt_cur, fmt_pre, fmt_diff = f"{cur[asset]:.4f}", f"{p_val:.4f}", f"{sign}{diff:.4f}"
                                
                            b_summ = b_summ + f"  {asset}: <code>{fmt_cur}</code> ({fmt_pre} → {fmt_diff})\n"
                
                # Add Total Change section
                if has_data:
                    b_summ += "\n• <b>TOTAL CHANGE</b>:\n"
                    any_total_change = False
                    for asset in ["USDT", "AMI", "APT"]:
                        t_diff = total_diffs[asset]
                        if abs(t_diff) < 1e-9: continue
                        
                        any_total_change = True
                        t_sign = "+" if t_diff > 0 else ""
                        if asset == "USDT":
                            fmt_t_diff = f"{t_sign}{t_diff:.2f}"
                        elif asset == "AMI":
                            fmt_t_diff = f"{t_sign}{t_diff:.0f}"
                        else: # APT
                            fmt_t_diff = f"{t_sign}{t_diff:.4f}"
                        
                        b_summ += f"  {asset}: <b>{fmt_t_diff}</b>\n"
                    
                    if not any_total_change:
                        b_summ += "  <i>(No net change)</i>\n"
                        
                    await tg_notifier.send_message(b_summ)
            except Exception as b_err:
                logger.error(f"Post-trade balance error: {b_err}")


    # ------------------------------------------------------------------ #
    #  Internal CEX helpers
    # ------------------------------------------------------------------ #
    async def _cex_buy(
        self,
        exchange: str,
        symbol: str,
        qty: float,
        price: float,
    ) -> Optional[OrderResult]:
        """Buy `qty` base coin (market order, qty in base)."""
        if qty <= 0:
            logger.error(f"_cex_buy: qty is zero for {symbol}")
            return None
        exch = exchange.lower()
        if exch == "bybit":
            # Spot Market Buy on Bybit often requires quoteCoin (USDT).
            # We convert base quantity to USDT with a small buffer (0.5%) to ensure fill.
            usdt_to_spend = qty * price * 1.005
            logger.debug(f"🔄 [Bybit] Converting {qty:.2f} base to {usdt_to_spend:.4f} USDT for Market Buy")
            return await self.bybit.place_market_order(
                symbol, "Buy", usdt_to_spend, is_quote_qty=True
            )
        elif exch == "mexc":
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
    ) -> Optional[OrderResult]:
        """Sell `qty` base coin (market order)."""
        if qty <= 0:
            logger.error(f"_cex_sell: qty is zero for {symbol}")
            return None
        exch = exchange.lower()
        if exch == "bybit":
            # Use the new unified interface
            return await self.bybit.place_market_order(
                symbol, "Sell", qty, is_quote_qty=False
            )
        elif exch == "mexc":
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


def _floor_qty(qty: float) -> float:
    """Floor quantity to a sane exchange lot-size precision.

    Rules (conservative):
      qty ≥ 100  → integer (AMI-scale)     e.g.  1222.3 → 1222
      qty ≥ 1    → 2 decimal places         e.g.  10.416 → 10.41
      qty ≥ 0.01 → 4 decimal places         e.g.  0.2083 → 0.2083
      otherwise  → 6 decimal places
    """
    if qty >= 100:
        return float(math.floor(qty))
    elif qty >= 1:
        return math.floor(qty * 100) / 100
    elif qty >= 0.01:
        return math.floor(qty * 10_000) / 10_000
    else:
        return math.floor(qty * 1_000_000) / 1_000_000
