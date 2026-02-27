import asyncio
import time
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
        panora_ami_apt_client: "Optional[PanoraClient]" = None,
        trade_executor: "Optional[TradeExecutor]" = None,
        enable_panora_arb: bool = True,
        enable_bybit_arb: bool = True,
        enable_mexc_arb: bool = True,
    ):
        self.collector = collector
        self.cex_symbol = cex_symbol or settings.cex_symbol
        self.apt_cex_symbol = settings.apt_cex_symbol
        self.panora_client       = panora_client        # AMI→USDT client
        self.panora_apt_client   = panora_apt_client    # APT→AMI client
        self.panora_ami_apt_client = panora_ami_apt_client  # AMI→APT client
        self.trade_executor = trade_executor
        self.bybit_fee = settings.bybit_fee
        self.mexc_fee = settings.mexc_fee
        self.panora_fee = settings.panora_fee
        self.min_profit = settings.min_profit_threshold
        self.poll_interval = settings.arb_check_interval
        self.enable_panora_arb = enable_panora_arb
        self.enable_bybit_arb  = enable_bybit_arb
        self.enable_mexc_arb   = enable_mexc_arb
        self.skip_panora_verify = settings.skip_panora_verify

        # Collector symbol keys for Panora pollers
        self._sym_ami_usdt = (
            f"{settings.ami_token_address[:4]}_{settings.usdt_token_address[:4]}"
        )
        self._sym_apt_ami = (
            f"{settings.apt_token_address[:4]}_{settings.ami_token_address[:4]}"
        )
        self._sym_ami_apt = (
            f"{settings.ami_token_address[:4]}_{settings.apt_token_address[:4]}"
        )

        # Per-direction verify cooldown: don't hammer Panora verify calls when arb
        # persists across consecutive 0.1s engine ticks.  Key = direction string.
        self._VERIFY_COOLDOWN_S = 3.0  # minimum seconds between verify calls per direction
        self._last_verify: dict = {}

        # Slippage tolerance (as decimal, e.g., 0.001 = 0.1%)
        self.slippage_tolerance_pct = settings.slippage_tolerance_pct / 100.0
        self.panora_api_slippage_pct = settings.panora_api_slippage_pct / 100.0

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
                if self.skip_panora_verify:
                    logger.warning(
                        f"⚠️ SKIP VERIFY | BUY Panora → SELL {cex_name} | "
                        f"est_price={panora.ask:.8f} qty={qty:.6f} est_profit={profit:.4f}"
                    )
                    prefetched = await self.panora_client.get_swap_quote(
                        qty * panora.ask,
                        from_token_address=settings.usdt_token_address,
                        to_token_address=settings.ami_token_address,
                        slippage_pct=settings.panora_api_slippage_pct,
                    )
                    if self.trade_executor:
                        asyncio.create_task(
                            self.trade_executor.execute_dex_cex(
                                "BUY_DEX_SELL_CEX", cex_name,
                                self.cex_symbol, panora.ask, cex.bid, qty,
                                prefetched_quote=prefetched,
                            ),
                            name="exec_dex_cex_skip_verify",
                        )
                    return
                _vkey = f"DEX_BUY_{cex_name}"
                if time.time() - self._last_verify.get(_vkey, 0) < self._VERIFY_COOLDOWN_S:
                    return
                # Verify buy from Panora: USDC → AMI
                logger.info(
                    f"🔍 Verifying Panora price | BUY Panora → SELL {cex_name} | "
                    f"est_price={panora.ask:.8f} qty={qty:.6f} est_profit={profit:.4f}"
                )
                self._last_verify[_vkey] = time.time()
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
                if self.skip_panora_verify:
                    logger.warning(
                        f"⚠️ SKIP VERIFY | BUY {cex_name} → SELL Panora | "
                        f"est_price={panora.bid:.8f} qty={qty:.6f} est_profit={profit:.4f}"
                    )
                    prefetched = await self.panora_client.get_swap_quote(
                        qty,
                        from_token_address=settings.ami_token_address,
                        to_token_address=settings.usdt_token_address,
                        slippage_pct=settings.panora_api_slippage_pct,
                    )
                    if self.trade_executor:
                        asyncio.create_task(
                            self.trade_executor.execute_dex_cex(
                                "BUY_CEX_SELL_DEX", cex_name,
                                self.cex_symbol, cex.ask, panora.bid, qty,
                                prefetched_quote=prefetched,
                            ),
                            name="exec_dex_cex_skip_verify",
                        )
                    return
                _vkey = f"DEX_SELL_{cex_name}"
                if time.time() - self._last_verify.get(_vkey, 0) < self._VERIFY_COOLDOWN_S:
                    return
                # Verify sell on Panora: AMI → USDC
                logger.info(
                    f"🔍 Verifying Panora price | BUY {cex_name} → SELL Panora | "
                    f"est_price={panora.bid:.8f} qty={qty:.6f} est_profit={profit:.4f}"
                )
                self._last_verify[_vkey] = time.time()
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
        Uses panora_ami_apt_client (AMI→APT direction) so the unit-price
        cache from the AMI→APT poller is hit correctly.
        """
        client = self.panora_ami_apt_client or self.panora_apt_client
        if not client:
            return None
        quote = await client.get_swap_quote(
            qty_ami,
            from_token_address=settings.ami_token_address,
            to_token_address=settings.apt_token_address,
        )
        if not quote:
            return None
        apt_out = client.parse_to_token_amount(quote)
        if apt_out is None or apt_out <= 0:
            return None
        return apt_out / qty_ami, quote

    async def _check_triangular_apt_ami(
        self,
        cex_ami: PriceData,
        cex_apt: PriceData,
        cex_name: str,
    ) -> None:
        """Detect and verify triangular arb between Panora APT/AMI and CEX."""
        if cex_ami.is_stale() or cex_apt.is_stale():
            return
        if not self.panora_apt_client:
            return

        cex_fee  = self.bybit_fee if cex_name == "Bybit" else self.mexc_fee
        notional = settings.trade_amount_usdt

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
                if self.skip_panora_verify:
                    logger.warning(
                        f"⚠️ [TRI-DIR1] SKIP VERIFY | {cex_name} | "
                        f"est_profit={profit_est:.4f} USDT"
                    )
                    prefetched = await self.panora_apt_client.get_swap_quote(
                        qty_apt_est,
                        from_token_address=settings.apt_token_address,
                        to_token_address=settings.ami_token_address,
                        slippage_pct=settings.panora_api_slippage_pct,
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
                                prefetched_quote=prefetched,
                            ),
                            name="exec_tri_dir1_skip_verify",
                        )
                    return
                logger.info(
                    f"🔍 [TRI-DIR1] {cex_name} | "
                    f"Panora APT→AMI={pan_apt_ami.ask:.4f}  "
                    f"CEX-implied={cex_implied:.4f}  "
                    f"spread={spread_pct:+.3f}%  est_profit={profit_est:.4f} USDT"
                )
                # Cooldown: skip verify if we already verified this direction recently
                _vkey = f"TRI_DIR1_{cex_name}"
                if time.time() - self._last_verify.get(_vkey, 0) < self._VERIFY_COOLDOWN_S:
                    return
                self._last_verify[_vkey] = time.time()
                result = await self._verify_panora_apt_to_ami(qty_apt_est)
                if result:
                    v_rate, v_quote = result
                    slippage = (v_rate - pan_apt_ami.ask) / pan_apt_ami.ask * 100
                    
                    # Allow slippage tolerance: if actual rate is worse than expected by
                    # more than tolerance, use expected rate for profit calc (pessimistic)
                    # Otherwise assume we got within tolerance and use actual rate
                    adjusted_rate = v_rate
                    if slippage < -self.slippage_tolerance_pct * 100:
                        # Worse than tolerance, but still try with adjusted estimate
                        adjusted_rate = pan_apt_ami.ask * (1 - self.slippage_tolerance_pct)
                        logger.warning(
                            f"⚠️ [TRI-DIR1] Slippage {slippage:.3f}% exceeds tolerance "
                            f"{-self.slippage_tolerance_pct*100:.3f}% — using conservative rate"
                        )
                    
                    v_ami_out  = qty_apt_est * adjusted_rate
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
                if self.skip_panora_verify:
                    logger.warning(
                        f"⚠️ [TRI-DIR2] SKIP VERIFY | {cex_name} | "
                        f"est_profit={profit_est:.4f} USDT"
                    )
                    client = self.panora_ami_apt_client or self.panora_apt_client
                    prefetched = await client.get_swap_quote(
                        qty_ami_est,
                        from_token_address=settings.ami_token_address,
                        to_token_address=settings.apt_token_address,
                        slippage_pct=settings.panora_api_slippage_pct,
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
                                prefetched_quote=prefetched,
                            ),
                            name="exec_tri_dir2_skip_verify",
                        )
                    return
                logger.info(
                    f"🔍 [TRI-DIR2] {cex_name} | "
                    f"Panora AMI→APT={pan_ami_apt.ask:.8f}  "
                    f"CEX-implied={cex_implied_rev:.8f}  "
                    f"spread={spread_pct_rev:+.3f}%  est_profit={profit_est:.4f} USDT"
                )
                _vkey = f"TRI_DIR2_{cex_name}"
                if time.time() - self._last_verify.get(_vkey, 0) < self._VERIFY_COOLDOWN_S:
                    return
                self._last_verify[_vkey] = time.time()
                result = await self._verify_panora_ami_to_apt(qty_ami_est)
                if result:
                    v_rate, v_quote = result
                    slippage = (v_rate - pan_ami_apt.ask) / pan_ami_apt.ask * 100
                    
                    # Allow slippage tolerance: if actual rate is worse than expected by
                    # more than tolerance, use expected rate for profit calc (pessimistic)
                    adjusted_rate = v_rate
                    if slippage < -self.slippage_tolerance_pct * 100:
                        adjusted_rate = pan_ami_apt.ask * (1 - self.slippage_tolerance_pct)
                        logger.warning(
                            f"⚠️ [TRI-DIR2] Slippage {slippage:.3f}% exceeds tolerance "
                            f"{-self.slippage_tolerance_pct*100:.3f}% — using conservative rate"
                        )
                    
                    v_apt_out  = qty_ami_est * adjusted_rate
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

            # [DISABLED] Tầng 1: CEX-CEX
            # if self.enable_bybit_arb and self.enable_mexc_arb and bybit and mexc:
            #     self._check_cex_cex(bybit, mexc)

            # [DISABLED] Tầng 2: DEX-CEX Panora AMI/USDT <-> Bybit/MEXC
            # panora_prices = self.collector.get(self._sym_ami_usdt)
            # panora        = panora_prices.get("panora")
            # if self.enable_panora_arb and self.enable_bybit_arb and panora and bybit:
            #     await self._check_dex_cex(panora, bybit, "Bybit")
            # if self.enable_panora_arb and self.enable_mexc_arb and panora and mexc:
            #     await self._check_dex_cex(panora, mexc, "MEXC")

            # ── Triangular: Panora APT/AMI vs CEX implied rate ──
            if self.enable_panora_arb and self.panora_apt_client:
                apt_prices = self.collector.get(self.apt_cex_symbol)
                bybit_apt  = apt_prices.get("bybit")
                mexc_apt   = apt_prices.get("mexc")

                # ── Periodic price summary log ──
                now = time.time()
                if now - self._last_price_log >= self._PRICE_LOG_INTERVAL_S:
                    self._last_price_log = now
                    pan_apt_ami_px = self.collector.get(self._sym_apt_ami).get("panora")
                    pan_ami_apt_px = self.collector.get(self._sym_ami_apt).get("panora")

                    apt_ask = bybit_apt.ask if bybit_apt else (mexc_apt.ask if mexc_apt else None)
                    apt_bid = bybit_apt.bid if bybit_apt else (mexc_apt.bid if mexc_apt else None)
                    ami_ask = bybit.ask    if bybit    else (mexc.ask    if mexc    else None)
                    ami_bid = bybit.bid    if bybit    else (mexc.bid    if mexc    else None)

                    apt_str = f"{apt_bid:.4f}/{apt_ask:.4f}" if apt_ask else "N/A"
                    ami_str = f"{ami_bid:.6f}/{ami_ask:.6f}" if ami_ask else "N/A"

                    apt_ami_str = f"{pan_apt_ami_px.ask:.4f}" if pan_apt_ami_px and not pan_apt_ami_px.is_stale() else "N/A"
                    ami_apt_str = f"{pan_ami_apt_px.ask:.8f}" if pan_ami_apt_px and not pan_ami_apt_px.is_stale() else "N/A"

                    # CEX implied rates
                    if apt_ask and ami_ask and ami_bid and apt_bid:
                        implied_apt_ami = apt_bid / ami_ask
                        implied_ami_apt = ami_bid / apt_ask
                        implied_str = f"CEX-implied APT/AMI={implied_apt_ami:.4f}  AMI/APT={implied_ami_apt:.8f}"
                    else:
                        implied_str = ""

                    logger.info(
                        f"[PRICES] "
                        f"APT/USDT={apt_str}  "
                        f"AMI/USDT={ami_str}  "
                        f"Panora APT→AMI={apt_ami_str}  AMI→APT={ami_apt_str}  "
                        f"{implied_str}"
                    )

                if self.enable_bybit_arb and bybit and bybit_apt:
                    await self._check_triangular_apt_ami(bybit, bybit_apt, "Bybit")
                if self.enable_mexc_arb and mexc and mexc_apt:
                    await self._check_triangular_apt_ami(mexc, mexc_apt, "MEXC")

            await asyncio.sleep(self.poll_interval)
