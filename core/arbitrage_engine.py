import asyncio
import time
from typing import Optional, Tuple, TYPE_CHECKING

from config.settings import settings
from core.price_collector import PriceCollector, PriceData
from utils.logger import get_logger

if TYPE_CHECKING:
    from core.trade_executor import TradeExecutor

logger = get_logger()


class ArbitrageEngine:
    """Detect arbitrage opportunities between Bybit and MEXC."""

    def __init__(
        self,
        collector: PriceCollector,
        cex_symbol: str | None = None,
        trade_executor: "Optional[TradeExecutor]" = None,
        enable_bybit_arb: bool = True,
        enable_mexc_arb: bool = True,
    ) -> None:
        self.collector = collector
        self.cex_symbol = cex_symbol or settings.cex_symbol
        self.trade_executor = trade_executor
        self.bybit_fee = settings.bybit_fee
        self.mexc_fee = settings.mexc_fee
        self.min_profit = settings.min_profit_threshold
        self.poll_interval = settings.arb_check_interval
        self.enable_bybit_arb = enable_bybit_arb
        self.enable_mexc_arb = enable_mexc_arb

        # Price summary log interval
        self._PRICE_LOG_INTERVAL_S = 5.0
        self._last_price_log: float = 0.0

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _calc_profit(
        buy_price: float,
        sell_price: float,
        qty: float,
        buy_fee_rate: float,
        sell_fee_rate: float,
    ) -> Tuple[float, float, float]:
        """Return (buy_volume, sell_volume, net_profit)."""
        buy_vol = qty * buy_price
        sell_vol = qty * sell_price
        profit = sell_vol - buy_vol - (buy_vol * buy_fee_rate) - (sell_vol * sell_fee_rate)
        return buy_vol, sell_vol, profit

    @staticmethod
    def _log_opportunity(
        buy_exchange: str,
        sell_exchange: str,
        buy_price: float,
        sell_price: float,
        qty: float,
        buy_vol: float,
        sell_vol: float,
        profit: float,
    ) -> None:
        logger.success(
            f"ARB OPPORTUNITY  BUY {buy_exchange} @ {buy_price:.8f}  "
            f"SELL {sell_exchange} @ {sell_price:.8f}  "
            f"QTY={qty:.6f}  BUY_VOL={buy_vol:.4f}  "
            f"SELL_VOL={sell_vol:.4f}  PROFIT={profit:.4f}"
        )

    # ------------------------------------------------------------------ #
    #  CEX-CEX  (Bybit <-> MEXC,  same symbol)
    # ------------------------------------------------------------------ #
    def _check_cex_cex(self, bybit: PriceData, mexc: PriceData) -> None:
        if bybit.is_stale() or mexc.is_stale():
            return

        # Direction 1: Buy Bybit ask -> Sell MEXC bid
        qty = min(bybit.ask_qty, mexc.bid_qty)
        if qty > 0:
            bv, sv, profit = self._calc_profit(
                bybit.ask, mexc.bid, qty, self.bybit_fee, self.mexc_fee
            )
            if profit > self.min_profit:
                self._log_opportunity(
                    "Bybit", "MEXC", bybit.ask, mexc.bid, qty, bv, sv, profit
                )
                if self.trade_executor:
                    asyncio.create_task(
                        self.trade_executor.execute_cex_cex(
                            "Bybit", "MEXC", self.cex_symbol,
                            bybit.ask, mexc.bid, qty,
                        ),
                        name="exec_cex_cex",
                    )

        # Direction 2: Buy MEXC ask -> Sell Bybit bid
        qty = min(mexc.ask_qty, bybit.bid_qty)
        if qty > 0:
            bv, sv, profit = self._calc_profit(
                mexc.ask, bybit.bid, qty, self.mexc_fee, self.bybit_fee
            )
            if profit > self.min_profit:
                self._log_opportunity(
                    "MEXC", "Bybit", mexc.ask, bybit.bid, qty, bv, sv, profit
                )
                if self.trade_executor:
                    asyncio.create_task(
                        self.trade_executor.execute_cex_cex(
                            "MEXC", "Bybit", self.cex_symbol,
                            mexc.ask, bybit.bid, qty,
                        ),
                        name="exec_cex_cex",
                    )

    # ------------------------------------------------------------------ #
    #  Main loop
    # ------------------------------------------------------------------ #
    async def run(self) -> None:
        logger.info(
            f"ArbitrageEngine started | symbol={self.cex_symbol} "
            f"bybit_fee={self.bybit_fee*100:.2f}% mexc_fee={self.mexc_fee*100:.2f}% | "
            f"bybit_arb={'ON' if self.enable_bybit_arb else 'OFF'} "
            f"mexc_arb={'ON' if self.enable_mexc_arb else 'OFF'}"
        )
        while True:
            cex_prices = self.collector.get(self.cex_symbol)
            bybit = cex_prices.get("bybit")
            mexc = cex_prices.get("mexc")

            now = time.time()
            if now - self._last_price_log >= self._PRICE_LOG_INTERVAL_S:
                self._last_price_log = now
                bybit_str = f"{bybit.bid:.6f}/{bybit.ask:.6f}" if bybit else "N/A"
                mexc_str = f"{mexc.bid:.6f}/{mexc.ask:.6f}" if mexc else "N/A"
                logger.info(f"[PRICES] {self.cex_symbol} | Bybit={bybit_str}  MEXC={mexc_str}")

            if self.enable_bybit_arb and self.enable_mexc_arb and bybit and mexc:
                self._check_cex_cex(bybit, mexc)

            await asyncio.sleep(self.poll_interval)
