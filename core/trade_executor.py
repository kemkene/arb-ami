"""
TradeExecutor — orchestrates concurrent order placement for arbitrage legs.

Modes:
  DRY_RUN=true  (default): log what would be executed, no real orders.
  DRY_RUN=false           : execute real orders on both legs simultaneously.

CEX-CEX arb  → Bybit market order + MEXC market order in parallel.
"""
from __future__ import annotations

import asyncio
from typing import Optional

import math
import time

from config.settings import settings
from exchanges.bybit_trader import BybitTrader
from exchanges.mexc_trader import MexcTrader
from utils.logger import get_logger, log_signal

logger = get_logger()

# Per-leg execution timeout (seconds). Abort if a single API call takes longer.
_LEG_TIMEOUT_S = 30


class TradeExecutor:
    """Coordinate simultaneous execution of arbitrage legs."""

    def __init__(self) -> None:
        self.bybit = BybitTrader()
        self.mexc = MexcTrader()
        self.dry_run = settings.dry_run

        mode = "DRY-RUN (paper)" if self.dry_run else "🔴 LIVE TRADING"
        logger.info(
            f"TradeExecutor initialised | mode={mode} "
            f"max_trade={settings.trade_amount_usdt} USDT"
        )

    # ------------------------------------------------------------------ #
    #  Signal logging helper
    # ------------------------------------------------------------------ #
    def _emit_signal(self, payload: dict) -> None:
        """Write a structured signal block to console + logs/signals.jsonl.

        Every signal includes mode (DRY/LIVE) and timestamp so offline
        analysis can distinguish paper signals from real executions.
        """
        payload["dry_run"] = self.dry_run
        payload["ts"]      = time.time()

        mode_tag = "[DRY-SIGNAL]" if self.dry_run else "[LIVE-SIGNAL]"

        lines = [f"\n{'━'*56}  {mode_tag}"]
        for k, v in payload.items():
            if k in ("dry_run", "ts"):
                continue
            if isinstance(v, float):
                lines.append(f"  {k:<22}: {v:.8g}")
            elif isinstance(v, dict):
                lines.append(f"  {k:<22}:")
                for bk, bv in v.items():
                    status = "✅" if bv.get("ok") else "⚠️ LOW"
                    lines.append(
                        f"    {bk:<20}: bal={bv.get('bal')!s:>12}  "
                        f"need={bv.get('need')!s:>12}  {status}"
                    )
            else:
                lines.append(f"  {k:<22}: {v}")
        lines.append(f"{'━'*64}")
        logger.info("\n".join(lines))
        log_signal(payload)

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

        buy_fee  = settings.bybit_fee if buy_exchange  == "Bybit" else settings.mexc_fee
        sell_fee = settings.bybit_fee if sell_exchange == "Bybit" else settings.mexc_fee
        net_profit_est = (
            (sell_price - buy_price) * safe_qty
            - buy_price  * safe_qty * buy_fee
            - sell_price * safe_qty * sell_fee
        )

        logger.info(
            f"{'[DRY]' if self.dry_run else '[LIVE]'} CEX-CEX EXECUTE | "
            f"BUY {buy_exchange} @ {buy_price:.8f}  "
            f"SELL {sell_exchange} @ {sell_price:.8f}  "
            f"QTY={safe_qty:.6f} {symbol}  PROFIT_EST={net_profit_est:.4f} USDT"
        )

        if self.dry_run:
            self._emit_signal({
                "type":          "CEX_CEX",
                "symbol":        symbol,
                "buy_exchange":  buy_exchange,
                "sell_exchange": sell_exchange,
                "buy_price":     buy_price,
                "sell_price":    sell_price,
                "qty":           safe_qty,
                "buy_volume_usdt": buy_price * safe_qty,
                "sell_volume_usdt": sell_price * safe_qty,
                "profit_usdt":   net_profit_est,
            })
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
        if exchange == "Bybit":
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
        """Sell `qty` base coin (market order)."""
        qty = _floor_qty(qty)
        if qty <= 0:
            logger.error(f"_cex_sell: qty rounded to zero for {symbol}")
            return None
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
