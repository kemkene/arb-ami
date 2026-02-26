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
          Leg 2 — CEX:    sell equivalent AMI      (from pre-positioned CEX balance)
          Profit source: Panora gives more AMI per APT than CEX implied rate.

        Direction AMI_TO_APT:
          Leg 1 — Panora: swap qty_ami AMI → APT  (from Aptos wallet)
          Leg 2 — CEX:    sell equivalent APT      (from pre-positioned CEX balance)

        Sequential execution: Panora first. If it fails, CEX leg is aborted
        to avoid an unhedged position.
        """
        if not self.panora_executor:
            logger.error("TradeExecutor: no PanoraExecutor — cannot execute triangular")
            return False

        if direction == "APT_TO_AMI":
            safe_qty   = min(qty_apt, settings.trade_amount_usdt / max(cex_apt_ask, 1e-12))
            ami_to_sell = safe_qty * (cex_apt_ask / max(cex_ami_bid, 1e-12))  # approx

            logger.info(
                f"{'[DRY]' if self.dry_run else '[LIVE]'} TRI-DIR1 | {cex_name} | "
                f"Panora {safe_qty:.4f} APT\u2192AMI  then sell ~{ami_to_sell:.2f} AMI @ {cex_ami_bid:.8f}"
            )
            if self.dry_run:
                return True

            # Leg 1: Panora APT→AMI
            tx = await self.panora_executor.execute_swap(
                safe_qty,
                from_token_address=settings.apt_token_address,
                to_token_address=settings.ami_token_address,
                prefetched_quote=prefetched_quote,
            )
            if not tx:
                logger.error("❌ TRI-DIR1: Panora swap APT→AMI failed → aborting CEX leg")
                return False
            logger.info(f"✅ TRI-DIR1 Leg1 done | tx={tx}")

            # Leg 2: CEX sell AMI
            order_id = await self._cex_sell(cex_name, ami_symbol, ami_to_sell)
            if not order_id:
                logger.error(
                    f"❌ TRI-DIR1: CEX sell AMI failed (Panora swap already done tx={tx}) "
                    f"— manual rebalance required"
                )
                return False
            logger.success(f"✅ TRI-DIR1 complete | panora_tx={tx} cex_order={order_id}")
            return True

        elif direction == "AMI_TO_APT":
            safe_qty   = min(qty_ami, settings.trade_amount_usdt / max(cex_ami_ask, 1e-12))
            apt_to_sell = safe_qty * (cex_ami_ask / max(cex_apt_bid, 1e-12))  # approx

            logger.info(
                f"{'[DRY]' if self.dry_run else '[LIVE]'} TRI-DIR2 | {cex_name} | "
                f"Panora {safe_qty:.2f} AMI\u2192APT  then sell ~{apt_to_sell:.4f} APT @ {cex_apt_bid:.4f}"
            )
            if self.dry_run:
                return True

            # Leg 1: Panora AMI→APT
            tx = await self.panora_executor.execute_swap(
                safe_qty,
                from_token_address=settings.ami_token_address,
                to_token_address=settings.apt_token_address,
                prefetched_quote=prefetched_quote,
            )
            if not tx:
                logger.error("❌ TRI-DIR2: Panora swap AMI→APT failed → aborting CEX leg")
                return False
            logger.info(f"✅ TRI-DIR2 Leg1 done | tx={tx}")

            # Leg 2: CEX sell APT
            order_id = await self._cex_sell(cex_name, apt_symbol, apt_to_sell)
            if not order_id:
                logger.error(
                    f"❌ TRI-DIR2: CEX sell APT failed (Panora swap already done tx={tx}) "
                    f"— manual rebalance required"
                )
                return False
            logger.success(f"✅ TRI-DIR2 complete | panora_tx={tx} cex_order={order_id}")
            return True

        else:
            logger.error(f"TradeExecutor: unknown triangular direction={direction}")
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
