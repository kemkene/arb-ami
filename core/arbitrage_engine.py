import asyncio
from typing import Optional, Tuple, TYPE_CHECKING

from config.settings import settings
from core.price_collector import PriceCollector, PriceData
from utils.logger import get_logger

if TYPE_CHECKING:
    from exchanges.panora import PanoraClient
    from core.trade_executor import TradeExecutor

logger = get_logger()


class ArbitrageEngine:
    """Detect arbitrage opportunities between Bybit, MEXC, and Panora DEX."""

    def __init__(
        self,
        collector: PriceCollector,
        cex_symbol: str | None = None,
        panora_client: "Optional[PanoraClient]" = None,
        panora_apt_client: "Optional[PanoraClient]" = None,
        trade_executor: "Optional[TradeExecutor]" = None,
        enable_panora_arb: bool = True,
        enable_bybit_arb: bool = True,
        enable_mexc_arb: bool = True,
    ):
        self.collector = collector
        self.cex_symbol = cex_symbol or settings.cex_symbol
        self.apt_cex_symbol = settings.apt_cex_symbol
        self.panora_client     = panora_client       # AMI/USDT client
        self.panora_apt_client = panora_apt_client   # APT/AMI and AMI/APT client
        self.trade_executor = trade_executor
        self.bybit_fee = settings.bybit_fee
        self.mexc_fee = settings.mexc_fee
        self.panora_fee = settings.panora_fee
        self.min_profit = settings.min_profit_threshold
        self.poll_interval = settings.arb_check_interval
        self.enable_panora_arb = enable_panora_arb
        self.enable_bybit_arb  = enable_bybit_arb
        self.enable_mexc_arb   = enable_mexc_arb

        # Collector symbol keys for Panora pollers
        # PanoraPoller uses f"{from[:4]}_{to[:4]}" as symbol
        self._sym_ami_usdt = (
            f"{settings.ami_token_address[:4]}_{settings.usdt_token_address[:4]}"
        )
        self._sym_apt_ami = (
            f"{settings.apt_token_address[:4]}_{settings.ami_token_address[:4]}"
        )
        self._sym_ami_apt = (
            f"{settings.ami_token_address[:4]}_{settings.apt_token_address[:4]}"
        )

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
    #  DEX-CEX  (Panora DEX vs Bybit/MEXC) — AMI/USDT
    # ------------------------------------------------------------------ #
    async def _verify_panora_sell(self, qty: float) -> "Optional[Tuple[float, dict]]":
        """Verify selling AMI on Panora: send qty AMI → receive ? USDC.

        Returns (verified_price_per_ami, raw_quote) so the caller can reuse
        the quote for on-chain submission without a second API call.
        Returns None on failure.
        """
        if not self.panora_client:
            return None

        quote = await self.panora_client.get_swap_quote(
            qty,
            from_token_address=settings.ami_token_address,
            to_token_address=settings.usdt_token_address,
        )
        if not quote:
            return None

        usdc_out = self.panora_client.parse_to_token_amount(quote)
        if usdc_out is None or usdc_out <= 0:
            return None

        return usdc_out / qty, quote  # (verified USDC per AMI, raw quote)

    async def _verify_panora_buy(self, qty: float, estimated_price: float) -> "Optional[Tuple[float, float, dict]]":
        """Verify buying AMI from Panora: send USDC → receive ? AMI.

        Returns (verified_price, verified_ami_qty, raw_quote) so the caller
        can reuse the quote for on-chain submission without a second API call.
        Returns None on failure.
        """
        if not self.panora_client:
            return None

        usdc_to_spend = qty * estimated_price
        quote = await self.panora_client.get_swap_quote(
            usdc_to_spend,
            from_token_address=settings.usdt_token_address,
            to_token_address=settings.ami_token_address,
        )
        if not quote:
            return None

        ami_out = self.panora_client.parse_to_token_amount(quote)
        if ami_out is None or ami_out <= 0:
            return None

        verified_price = usdc_to_spend / ami_out  # actual USDC per AMI
        return verified_price, ami_out, quote  # (price, qty, raw quote)

    async def _check_dex_cex(self, panora: PriceData, cex: PriceData, cex_name: str) -> None:
        """Check arbitrage between Panora DEX and CEX, with Panora quote verification."""
        if panora.is_stale() or cex.is_stale():
            return

        cex_fee = self.bybit_fee if cex_name == "Bybit" else self.mexc_fee

        # Direction 1: Buy Panora (ask) -> Sell CEX (bid)
        qty = min(panora.ask_qty, cex.bid_qty)
        if qty > 0:
            bv, sv, profit = self._calc_profit(
                panora.ask, cex.bid, qty, self.panora_fee, cex_fee
            )
            if profit > self.min_profit:
                # Verify buy from Panora: USDC → AMI
                logger.info(
                    f"🔍 Verifying Panora price | BUY Panora → SELL {cex_name} | "
                    f"est_price={panora.ask:.8f} qty={qty:.6f} est_profit={profit:.4f}"
                )
                verified = await self._verify_panora_buy(qty, panora.ask)
                if verified:
                    v_price, v_qty, v_quote = verified  # reuse quote — no second API call
                    slippage = (v_price - panora.ask) / panora.ask * 100
                    bv2, sv2, profit2 = self._calc_profit(
                        v_price, cex.bid, v_qty, self.panora_fee, cex_fee
                    )
                    if profit2 > self.min_profit:
                        logger.success(
                            f"✅ VERIFIED ARB  BUY Panora @ {v_price:.8f} "
                            f"(est {panora.ask:.8f}, slip {slippage:+.3f}%)  "
                            f"SELL {cex_name} @ {cex.bid:.8f}  "
                            f"QTY={v_qty:.6f}  BUY_VOL={bv2:.4f}  "
                            f"SELL_VOL={sv2:.4f}  PROFIT={profit2:.4f}"
                        )
                        if self.trade_executor:
                            asyncio.create_task(
                                self.trade_executor.execute_dex_cex(
                                    "BUY_DEX_SELL_CEX", cex_name,
                                    self.cex_symbol, v_price, cex.bid, v_qty,
                                    prefetched_quote=v_quote,
                                ),
                                name="exec_dex_cex",
                            )
                    else:
                        logger.warning(
                            f"❌ ARB CANCELED after verify | BUY Panora @ {v_price:.8f} "
                            f"(est {panora.ask:.8f}, slip {slippage:+.3f}%) "
                            f"→ profit {profit2:.4f} < threshold"
                        )
                else:
                    logger.warning(
                        f"⚠️ ARB UNVERIFIED (Panora API fail) | "
                        f"BUY Panora @ {panora.ask:.8f} SELL {cex_name} @ {cex.bid:.8f} "
                        f"est_profit={profit:.4f}"
                    )

        # Direction 2: Buy CEX (ask) -> Sell Panora (bid)
        qty = min(cex.ask_qty, panora.bid_qty)
        if qty > 0:
            bv, sv, profit = self._calc_profit(
                cex.ask, panora.bid, qty, cex_fee, self.panora_fee
            )
            if profit > self.min_profit:
                # Verify sell on Panora: AMI → USDC
                logger.info(
                    f"🔍 Verifying Panora price | BUY {cex_name} → SELL Panora | "
                    f"est_price={panora.bid:.8f} qty={qty:.6f} est_profit={profit:.4f}"
                )
                result = await self._verify_panora_sell(qty)
                if result is not None:
                    v_price, v_quote = result  # reuse quote — no second API call
                    slippage = (v_price - panora.bid) / panora.bid * 100
                    bv2, sv2, profit2 = self._calc_profit(
                        cex.ask, v_price, qty, cex_fee, self.panora_fee
                    )
                    if profit2 > self.min_profit:
                        logger.success(
                            f"✅ VERIFIED ARB  BUY {cex_name} @ {cex.ask:.8f}  "
                            f"SELL Panora @ {v_price:.8f} "
                            f"(est {panora.bid:.8f}, slip {slippage:+.3f}%)  "
                            f"QTY={qty:.6f}  BUY_VOL={bv2:.4f}  "
                            f"SELL_VOL={sv2:.4f}  PROFIT={profit2:.4f}"
                        )
                        if self.trade_executor:
                            asyncio.create_task(
                                self.trade_executor.execute_dex_cex(
                                    "BUY_CEX_SELL_DEX", cex_name,
                                    self.cex_symbol, cex.ask, v_price, qty,
                                    prefetched_quote=v_quote,
                                ),
                                name="exec_dex_cex",
                            )
                    else:
                        logger.warning(
                            f"❌ ARB CANCELED after verify | SELL Panora @ {v_price:.8f} "
                            f"(est {panora.bid:.8f}, slip {slippage:+.3f}%) "
                            f"→ profit {profit2:.4f} < threshold"
                        )
                else:
                    logger.warning(
                        f"⚠️ ARB UNVERIFIED (Panora API fail) | "
                        f"BUY {cex_name} @ {cex.ask:.8f} SELL Panora @ {panora.bid:.8f} "
                        f"est_profit={profit:.4f}"
                    )

    # ------------------------------------------------------------------ #
    #  Triangular Arb  (APT/AMI on Panora DEX vs CEX implied rate)
    # ------------------------------------------------------------------ #
    async def _verify_panora_apt_to_ami(
        self, qty_apt: float
    ) -> "Optional[Tuple[float, dict]]":
        """Send qty_apt APT → receive ? AMI on Panora.

        Returns (ami_per_apt, raw_quote) or None.
        """
        if not self.panora_apt_client:
            return None
        quote = await self.panora_apt_client.get_swap_quote(
            qty_apt,
            from_token_address=settings.apt_token_address,
            to_token_address=settings.ami_token_address,
        )
        if not quote:
            return None
        ami_out = self.panora_apt_client.parse_to_token_amount(quote)
        if ami_out is None or ami_out <= 0:
            return None
        return ami_out / qty_apt, quote

    async def _verify_panora_ami_to_apt(
        self, qty_ami: float
    ) -> "Optional[Tuple[float, dict]]":
        """Send qty_ami AMI → receive ? APT on Panora.

        Returns (apt_per_ami, raw_quote) or None.
        """
        if not self.panora_apt_client:
            return None
        quote = await self.panora_apt_client.get_swap_quote(
            qty_ami,
            from_token_address=settings.ami_token_address,
            to_token_address=settings.apt_token_address,
        )
        if not quote:
            return None
        apt_out = self.panora_apt_client.parse_to_token_amount(quote)
        if apt_out is None or apt_out <= 0:
            return None
        return apt_out / qty_ami, quote

    async def _check_triangular_apt_ami(
        self,
        cex_ami: PriceData,
        cex_apt: PriceData,
        cex_name: str,
    ) -> None:
        """Detect and verify triangular arb between Panora APT/AMI and CEX.

        Direction 1 — APT → AMI via Panora, sell AMI on CEX:
          Start with USDT → buy APT on CEX → swap APT→AMI on Panora
          → sell AMI on CEX.  Profitable when Panora's APT/AMI rate is
          higher than the CEX-implied rate (apt_bid / ami_ask).

        Direction 2 — AMI → APT via Panora, sell APT on CEX:
          Start with USDT → buy AMI on CEX → swap AMI→APT on Panora
          → sell APT on CEX.  Profitable when Panora's AMI/APT rate is
          higher than the CEX-implied rate (ami_bid / apt_ask).

        Uses settings.trade_amount_usdt as the notional trade size.
        """
        if cex_ami.is_stale() or cex_apt.is_stale():
            return
        if not self.panora_apt_client:
            return

        cex_fee = self.bybit_fee if cex_name == "Bybit" else self.mexc_fee
        notional = settings.trade_amount_usdt  # USDT to deploy per direction

        # ── Direction 1: buy APT on CEX → APT→AMI on Panora → sell AMI on CEX ──
        pan_apt_ami_prices = self.collector.get(self._sym_apt_ami)
        pan_apt_ami = pan_apt_ami_prices.get("panora")

        if pan_apt_ami and not pan_apt_ami.is_stale():
            # Estimated qty using current market prices
            qty_apt_est  = notional / cex_apt.ask
            ami_est      = qty_apt_est * pan_apt_ami.ask          # Panora gives this many AMI
            usdt_out_est = ami_est * cex_ami.bid
            fees_est = (
                notional     * cex_fee        # buy APT on CEX
                + notional   * self.panora_fee  # Panora swap fee (on APT input value)
                + usdt_out_est * cex_fee       # sell AMI on CEX
            )
            profit_est = usdt_out_est - notional - fees_est

            # CEX implied APT/AMI rate vs Panora
            cex_implied  = cex_apt.bid / cex_ami.ask          # AMI you'd get per APT on CEX
            spread_pct   = (pan_apt_ami.ask - cex_implied) / cex_implied * 100

            if profit_est > self.min_profit:
                logger.info(
                    f"🔍 [TRI-DIR1] {cex_name} | "
                    f"Panora APT→AMI={pan_apt_ami.ask:.4f}  "
                    f"CEX-implied={cex_implied:.4f}  "
                    f"spread={spread_pct:+.3f}%  est_profit={profit_est:.4f} USDT"
                )
                result = await self._verify_panora_apt_to_ami(qty_apt_est)
                if result:
                    v_rate, v_quote = result
                    slippage = (v_rate - pan_apt_ami.ask) / pan_apt_ami.ask * 100
                    v_ami_out  = qty_apt_est * v_rate
                    v_usdt_out = v_ami_out * cex_ami.bid
                    v_fees = (
                        notional     * cex_fee
                        + notional   * self.panora_fee
                        + v_usdt_out * cex_fee
                    )
                    v_profit = v_usdt_out - notional - v_fees

                    if v_profit > self.min_profit:
                        logger.success(
                            f"✅ [TRI-DIR1] VERIFIED | {cex_name} | "
                            f"buy {qty_apt_est:.4f} APT @ {cex_apt.ask:.4f} USDT "
                            f"→ Panora APT→AMI @ {v_rate:.4f} "
                            f"(est {pan_apt_ami.ask:.4f}, slip {slippage:+.3f}%) "
                            f"→ sell {v_ami_out:.2f} AMI @ {cex_ami.bid:.8f} USDT | "
                            f"PROFIT={v_profit:.4f} USDT"
                        )
                        if self.trade_executor:
                            asyncio.create_task(
                                self.trade_executor.execute_triangular(
                                    direction="APT_TO_AMI",
                                    cex_name=cex_name,
                                    apt_symbol=self.apt_cex_symbol,
                                    ami_symbol=self.cex_symbol,
                                    qty_apt=qty_apt_est,
                                    cex_apt_ask=cex_apt.ask,
                                    cex_ami_bid=cex_ami.bid,
                                    prefetched_quote=v_quote,
                                ),
                                name="exec_tri_dir1",
                            )
                    else:
                        logger.warning(
                            f"❌ [TRI-DIR1] CANCELED after verify | "
                            f"Panora rate={v_rate:.4f} (slip {slippage:+.3f}%) "
                            f"→ profit {v_profit:.4f} < threshold"
                        )
                else:
                    logger.warning(
                        f"⚠️ [TRI-DIR1] UNVERIFIED (Panora API fail) | "
                        f"est_profit={profit_est:.4f}"
                    )

        # ── Direction 2: buy AMI on CEX → AMI→APT on Panora → sell APT on CEX ──
        pan_ami_apt_prices = self.collector.get(self._sym_ami_apt)
        pan_ami_apt = pan_ami_apt_prices.get("panora")

        if pan_ami_apt and not pan_ami_apt.is_stale():
            qty_ami_est  = notional / cex_ami.ask
            apt_est      = qty_ami_est * pan_ami_apt.ask
            usdt_out_est = apt_est * cex_apt.bid
            fees_est = (
                notional       * cex_fee
                + notional     * self.panora_fee
                + usdt_out_est * cex_fee
            )
            profit_est = usdt_out_est - notional - fees_est

            cex_implied_rev = cex_ami.bid / cex_apt.ask      # APT per AMI on CEX
            spread_pct_rev  = (pan_ami_apt.ask - cex_implied_rev) / cex_implied_rev * 100

            if profit_est > self.min_profit:
                logger.info(
                    f"🔍 [TRI-DIR2] {cex_name} | "
                    f"Panora AMI→APT={pan_ami_apt.ask:.8f}  "
                    f"CEX-implied={cex_implied_rev:.8f}  "
                    f"spread={spread_pct_rev:+.3f}%  est_profit={profit_est:.4f} USDT"
                )
                result = await self._verify_panora_ami_to_apt(qty_ami_est)
                if result:
                    v_rate, v_quote = result
                    slippage = (v_rate - pan_ami_apt.ask) / pan_ami_apt.ask * 100
                    v_apt_out  = qty_ami_est * v_rate
                    v_usdt_out = v_apt_out * cex_apt.bid
                    v_fees = (
                        notional       * cex_fee
                        + notional     * self.panora_fee
                        + v_usdt_out   * cex_fee
                    )
                    v_profit = v_usdt_out - notional - v_fees

                    if v_profit > self.min_profit:
                        logger.success(
                            f"✅ [TRI-DIR2] VERIFIED | {cex_name} | "
                            f"buy {qty_ami_est:.2f} AMI @ {cex_ami.ask:.8f} USDT "
                            f"→ Panora AMI→APT @ {v_rate:.8f} "
                            f"(est {pan_ami_apt.ask:.8f}, slip {slippage:+.3f}%) "
                            f"→ sell {v_apt_out:.4f} APT @ {cex_apt.bid:.4f} USDT | "
                            f"PROFIT={v_profit:.4f} USDT"
                        )
                        if self.trade_executor:
                            asyncio.create_task(
                                self.trade_executor.execute_triangular(
                                    direction="AMI_TO_APT",
                                    cex_name=cex_name,
                                    apt_symbol=self.apt_cex_symbol,
                                    ami_symbol=self.cex_symbol,
                                    qty_ami=qty_ami_est,
                                    cex_ami_ask=cex_ami.ask,
                                    cex_apt_bid=cex_apt.bid,
                                    prefetched_quote=v_quote,
                                ),
                                name="exec_tri_dir2",
                            )
                    else:
                        logger.warning(
                            f"❌ [TRI-DIR2] CANCELED after verify | "
                            f"Panora rate={v_rate:.8f} (slip {slippage:+.3f}%) "
                            f"→ profit {v_profit:.4f} < threshold"
                        )
                else:
                    logger.warning(
                        f"⚠️ [TRI-DIR2] UNVERIFIED (Panora API fail) | "
                        f"est_profit={profit_est:.4f}"
                    )

    # ------------------------------------------------------------------ #
    #  Main loop
    # ------------------------------------------------------------------ #
    async def run(self) -> None:
        logger.info(
            f"ArbitrageEngine started | symbol={self.cex_symbol} "
            f"bybit_fee={self.bybit_fee*100:.2f}% mexc_fee={self.mexc_fee*100:.2f}% "
            f"panora_fee={self.panora_fee*100:.2f}% | "
            f"panora_arb={'ON' if self.enable_panora_arb else 'OFF'} "
            f"bybit_arb={'ON' if self.enable_bybit_arb else 'OFF'} "
            f"mexc_arb={'ON' if self.enable_mexc_arb else 'OFF'}"
        )
        while True:
            # ── CEX-CEX: Bybit <-> MEXC (requires both CEX accounts) ──
            cex_prices = self.collector.get(self.cex_symbol)
            bybit = cex_prices.get("bybit")
            mexc  = cex_prices.get("mexc")

            if self.enable_bybit_arb and self.enable_mexc_arb and bybit and mexc:
                self._check_cex_cex(bybit, mexc)

            # ── DEX-CEX: Panora AMI/USDT <-> Bybit/MEXC ──
            panora_prices = self.collector.get(self._sym_ami_usdt)
            panora        = panora_prices.get("panora")

            if self.enable_panora_arb and self.enable_bybit_arb and panora and bybit:
                await self._check_dex_cex(panora, bybit, "Bybit")
            if self.enable_panora_arb and self.enable_mexc_arb and panora and mexc:
                await self._check_dex_cex(panora, mexc, "MEXC")

            # ── Triangular: Panora APT/AMI vs CEX implied rate ──
            if self.enable_panora_arb and self.panora_apt_client:
                apt_prices = self.collector.get(self.apt_cex_symbol)
                bybit_apt  = apt_prices.get("bybit")
                mexc_apt   = apt_prices.get("mexc")

                if self.enable_bybit_arb and bybit and bybit_apt:
                    await self._check_triangular_apt_ami(bybit, bybit_apt, "Bybit")
                if self.enable_mexc_arb and mexc and mexc_apt:
                    await self._check_triangular_apt_ami(mexc, mexc_apt, "MEXC")

            await asyncio.sleep(self.poll_interval)
