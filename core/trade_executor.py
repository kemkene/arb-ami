"""
TradeExecutor — orchestrates concurrent order placement for arbitrage legs.

Modes:
  DRY_RUN=true  (default): log what would be executed, no real orders.
  DRY_RUN=false           : execute real orders on both legs simultaneously.

CEX-CEX arb  → Bybit market order + MEXC market order in parallel.
DEX-CEX arb  → Panora DEX swap + CEX market order in parallel.
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

        mode = "DRY-RUN (paper)" if self.dry_run else "🔴 LIVE TRADING"
        logger.info(
            f"TradeExecutor initialised | mode={mode} "
            f"max_trade={settings.trade_amount_usdt} USDT"
        )

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
