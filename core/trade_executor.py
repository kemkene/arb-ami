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
from exchanges.bybit_trader import BybitTrader
from exchanges.mexc_trader import MexcTrader
from core.balance_manager import BalanceManager, LegRequirement
from core.cellana_dex_swap import CellanaDexSwap, SwapResult as CellanaSwapResult
from core.hyperion_dex_swap import HyperionDexSwap, SwapResult as HyperionSwapResult
from utils.logger import get_logger, log_signal
from utils.telegram_notifier import notifier as tg_notifier

logger = get_logger()

# Per-leg execution timeout (seconds). Abort if a single API call takes longer.
_LEG_TIMEOUT_S = 30


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
    ) -> None:
        self.bybit = BybitTrader()
        self.mexc = MexcTrader()
        self.dry_run = settings.dry_run

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
    ) -> tuple[bool, dict]:
        """Validate balances for all legs.

        Returns (ok, details_dict).
        """
        if not self.balance_manager:
            return True, {}

        requirements: list[LegRequirement] = []
        for leg in legs:
            if leg.is_dex:
                # DEX leg check
                asset = "APT" if leg.dex_direction == "apt_to_ami" else "AMI"
                requirements.append(LegRequirement(
                    exchange="dex",
                    asset=asset,
                    amount=leg.qty,
                    side="sell", # Spending this asset
                    symbol=f"DEX_{leg.dex_direction}",
                ))
            elif leg.side == LegSide.BUY:
                # Buying base coin → need USDT
                usdt_needed = leg.qty * leg.price_est * 1.002  # +0.2% for fees
                requirements.append(LegRequirement(
                    exchange=leg.exchange,
                    asset="USDT",
                    amount=usdt_needed,
                    side="buy",
                    symbol=leg.symbol,
                ))
            else:
                # Selling base coin → need the base coin
                base_coin = _cex_coin_for(leg.symbol)
                requirements.append(LegRequirement(
                    exchange=leg.exchange,
                    asset=base_coin,
                    amount=leg.qty,
                    side="sell",
                    symbol=leg.symbol,
                ))

        result = await self.balance_manager.check_legs(requirements)

        if not result.ok:
            logger.warning(
                f"⚠️ Balance check FAILED for {direction}: {result.reason}"
            )

        return result.ok, result.details

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
        qty = _floor_qty(leg.qty)
        if qty <= 0:
            return LegResult(leg=leg, ok=False, error="qty rounded to zero")

        try:
            fill = None

            if leg.side == LegSide.BUY:
                fill = await asyncio.wait_for(
                    self._cex_buy(leg.exchange, leg.symbol, qty, leg.price_est),
                    timeout=_LEG_TIMEOUT_S,
                )
            else:
                fill = await asyncio.wait_for(
                    self._cex_sell(leg.exchange, leg.symbol, qty),
                    timeout=_LEG_TIMEOUT_S,
                )

            elapsed = (time.time() - t0) * 1000
            if fill:
                return LegResult(
                    leg=leg, ok=True,
                    order_id=fill.order_id,
                    filled_qty=fill.filled_qty,
                    filled_price=fill.filled_price,
                    fill_status=fill.status,
                    elapsed_ms=elapsed,
                )
            else:
                return LegResult(leg=leg, ok=False, error="order returned None", elapsed_ms=elapsed)

        except asyncio.TimeoutError:
            elapsed = (time.time() - t0) * 1000
            return LegResult(leg=leg, ok=False, error=f"timeout after {_LEG_TIMEOUT_S}s", elapsed_ms=elapsed)
        except Exception as e:
            elapsed = (time.time() - t0) * 1000
            return LegResult(leg=leg, ok=False, error=str(e), elapsed_ms=elapsed)

    # ------------------------------------------------------------------ #
    #  DEX on-chain leg execution
    # ------------------------------------------------------------------ #
    async def _execute_dex_leg(self, leg: TradeLeg, t0: float) -> LegResult:
        """Execute an on-chain DEX swap via Cellana or Hyperion."""
        # Route to the correct DEX executor
        dex_executor = None
        label = "DEX"
        
        if leg.exchange == "hyperion":
            dex_executor = self.hyperion_swap
            label = "Hyperion"
        else:
            # Default to Cellana for backward compatibility or explicit "cellana" / "dex"
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

            return LegResult(
                leg=leg,
                ok=swap_result.ok,
                order_id=swap_result.tx_hash,
                error=swap_result.error,
                elapsed_ms=swap_result.elapsed_ms,
            )

        except asyncio.TimeoutError:
            elapsed = (time.time() - t0) * 1000
            return LegResult(
                leg=leg, ok=False,
                error=f"DEX swap timeout after {_LEG_TIMEOUT_S}s",
                elapsed_ms=elapsed,
            )
        except Exception as e:
            elapsed = (time.time() - t0) * 1000
            return LegResult(leg=leg, ok=False, error=str(e), elapsed_ms=elapsed)

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

        # ── Step 1: Pre-trade balance check (CEX legs only) ──
        cex_legs = [l for l in legs if not l.is_dex]
        if cex_legs:
            balance_ok, balance_details = await self._check_balances(cex_legs, direction)
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
        logger.info(
            f"🔴 EXECUTING {direction} | {len(legs)} legs | est_profit=${profit_est:.4f}"
        )

        # Notify via Telegram
        tg_tag = "🔴 LIVE" if not self.dry_run else "🧪 DRY"
        msg = f"<b>{tg_tag} Trade Started</b>\nDirection: <code>{direction}</code>\nEst. Profit: <b>${profit_est:.4f}</b>"
        if trade_steps:
            msg += f"\nSteps: {len(trade_steps)}"
        await tg_notifier.send_message(msg)

        if parallel:
            tasks = [self._execute_leg(leg) for leg in legs]
            result.legs = list(await asyncio.gather(*tasks))
        else:
            for leg in legs:
                lr = await self._execute_leg(leg)
                result.legs.append(lr)
                if not lr.ok: break

        # ── Step 4: Evaluate results ──
        succeeded = [lr for lr in result.legs if lr.ok]
        failed = [lr for lr in result.legs if not lr.ok]

        if not failed:
            result.ok = True
            logger.success(f"✅ {direction} OK | profit=${profit_est:.4f}")
            self._emit_signal({
                "type": "MULTI_LEG_OK",
                "direction": direction,
                "profit_est": profit_est,
                "leg_count": len(legs),
                "order_ids": [lr.order_id for lr in succeeded],
                "trade_steps": trade_steps
            })

            tg_tag = "✅ SUCCESS" if not self.dry_run else "🧪 DRY ✅ SUCCESS"
            await tg_notifier.send_message(f"<b>{tg_tag}</b>\nDirection: <code>{direction}</code>\nProfit: <b>${profit_est:.4f}</b>")

            if self.balance_manager:
                try:
                    await self.balance_manager.ensure_fresh()
                    b_summ = "<b>💰 Balance (Post-Trade):</b>\n"
                    for ex in ["bybit", "mexc", "dex"]:
                        u, am, ap = self.balance_manager.get_free(ex, "USDT"), self.balance_manager.get_free(ex, "AMI"), self.balance_manager.get_free(ex, "APT")
                        if u > 0 or am > 0 or ap > 0:
                            lbl = ex.upper() if ex != "dex" else "🔗 APTOS"
                            b_summ += f"• <b>{lbl}</b>: USDT: <code>{u:.2f}</code> | AMI: <code>{am:.0f}</code> | APT: <code>{ap:.4f}</code>\n"
                    await tg_notifier.send_message(b_summ)
                except Exception as b_err: logger.error(f"Post-trade balance error: {b_err}")
            return result

        # ── Step 5: Partial failure → rollback ──
        fail_tags, ok_tags = [lr.leg.tag for lr in failed], [lr.leg.tag for lr in succeeded]
        logger.error(f"❌ {direction} PARTIAL FAIL | ok={ok_tags} failed={fail_tags}")
        result.rollback_legs = await self._rollback_legs(succeeded)
        rollback_ok = all(r.ok for r in result.rollback_legs) if result.rollback_legs else True
        result.reason = f"partial failure (failed: {fail_tags}), rollback {'OK' if rollback_ok else 'FAILED'}"

        self._emit_signal({
            "type": "MULTI_LEG_PARTIAL",
            "direction": direction,
            "profit_est": profit_est,
            "failed_legs": fail_tags,
            "ok_legs": ok_tags,
            "rollback_ok": rollback_ok,
            "trade_steps": trade_steps
        })

        tg_status = "⚠️ PARTIAL FAIL" if rollback_ok else "❌ CRITICAL FAIL"
        tg_tag = f"🧪 DRY {tg_status}" if self.dry_run else tg_status
        await tg_notifier.send_message(f"<b>{tg_tag}</b>\nDirection: <code>{direction}</code>\nFailed: <code>{', '.join(fail_tags)}</code>\nRollback: {'OK' if rollback_ok else 'FAILED!'}")
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
    #  Internal helpers
    # ------------------------------------------------------------------ #
    async def _cex_buy(
        self,
        exchange: str,
        symbol: str,
        qty: float,
        price: float,
    ) -> Optional[str]:
        """Buy `qty` base coin (market order, qty in base)."""
        qty = _floor_qty(qty)
        if qty <= 0:
            logger.error(f"_cex_buy: qty rounded to zero for {symbol}")
            return None
        exch = exchange.lower()
        if exch == "bybit":
            return await self.bybit.place_market_order(
                symbol, "Buy", qty, market_unit="baseCoinQty"
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
    ) -> Optional[str]:
        """Sell `qty` base coin (market order)."""
        qty = _floor_qty(qty)
        if qty <= 0:
            logger.error(f"_cex_sell: qty rounded to zero for {symbol}")
            return None
        exch = exchange.lower()
        if exch == "bybit":
            return await self.bybit.place_market_order(
                symbol, "Sell", qty, market_unit="baseCoinQty"
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
