import asyncio
import time
from dataclasses import dataclass
from typing import Optional, Tuple, TYPE_CHECKING, Dict, List

from config.settings import settings
from core.price_collector import PriceCollector, PriceData
from utils.logger import get_logger, log_arbitrage_opportunity
from utils.telegram_notifier import notifier as tg_notifier
from core.hyperion_math import decode_sqrt_price, calculate_amount_out

from core.trade_executor import TradeExecutor, TradeLeg, LegSide

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


@dataclass
class Opportunity:
    """Represents a detected arbitrage opportunity."""
    direction: str
    profit_usdt: float
    legs: List[TradeLeg]
    buy_price: float
    sell_price: float
    log_msg: str


class ArbitrageEngine:
    """Detect arbitrage opportunities across CEX-CEX and Cellana DEX-CEX."""

    def __init__(
        self,
        collector: PriceCollector,
        cex_symbol: str | None = None,
        trade_executor: "Optional[TradeExecutor]" = None,
        enable_bybit_arb: bool = True,
        enable_mexc_arb: bool = True,
        cellana_listener: "Optional[CellanaSwapListener]" = None,
        hyperion_listener: "Optional[HyperionSwapListener]" = None,
        gas_monitor: "Optional[GasMonitor]" = None,
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
        self.apt_cex_symbol = settings.apt_cex_symbol
        
        # Listeners for gRPC health status
        self.cellana_listener = cellana_listener
        self.hyperion_listener = hyperion_listener
        self.gas_monitor = gas_monitor

        # Cellana AMI/APT pool state
        self.cellana_fee = 0.001
        self.token_decimals = 10 ** 8
        self.cellana_reserves_ami: Optional[int] = None
        self.cellana_reserves_apt: Optional[int] = None
        self.cellana_version: Optional[int] = None
        self.cellana_last_update_ts: float = 0.0
        self.cellana_last_spot: Optional[float] = None

        # Hyperion AMI/APT pool state (CLMM)
        self.hyperion_fee = settings.hyperion_fee_rate
        self.hyperion_sqrt_price_x64: Optional[int] = None
        self.hyperion_liquidity: Optional[int] = None
        self.hyperion_last_update_ts: float = 0.0

        # DEX-DEX Settings
        self.enable_dex_dex_arb = settings.enable_dex_dex_arb
        self.min_profit_dex_dex = settings.min_profit_dex_dex
        self.gas_fee_dex_dex_apt = settings.dex_dex_gas_fee_apt

        # Optimal Sizing
        self.optimal_size_enabled = settings.optimal_size_enabled
        self.optimal_size_steps = settings.optimal_size_steps
        self.min_trade_usdt = settings.min_trade_usdt
        self.max_trade_usdt = settings.max_trade_usdt
        self._phi = (5**0.5 - 1) / 2  # Golden ratio (~0.618)

        # Price summary log interval
        self._PRICE_LOG_INTERVAL_S = 5.0
        self._last_price_log: float = 0.0

        # De-duplication
        self.deduplicator = OpportunityDeduplicator(
            cooldown_sec=settings.arb_dedup_cooldown_sec,
            price_decimals=settings.arb_price_round_decimals,
        )

        # Execution safety guards
        self._execution_lock = asyncio.Lock()
        self._trade_cooldown_s = settings.trade_cooldown_s
        self._last_trade_ts: float = 0.0
        self._is_running = True # Added for the while loop
        self.gas_cost_usd = settings.gas_cost_usd # Initial value, will be updated dynamically

    def update_cellana_state(self, payload: dict) -> None:
        """Receive latest Cellana SyncEvent state from listener callback."""
        parsed = payload.get("parsed") if isinstance(payload, dict) else None
        if not isinstance(parsed, dict):
            return

        reserves_ami = parsed.get("reserves_1")
        reserves_apt = parsed.get("reserves_2")
        version = payload.get("version")

        if reserves_ami is None or reserves_apt is None:
            return

        try:
            self.cellana_reserves_ami = int(reserves_ami)
            self.cellana_reserves_apt = int(reserves_apt)
            self.cellana_version = int(version) if version is not None else None
            self.cellana_last_update_ts = time.time()
            if self.cellana_reserves_ami is not None and self.cellana_reserves_apt is not None:
                self.cellana_last_spot = float(self.cellana_reserves_apt) / float(self.cellana_reserves_ami)
            
            # Record latest data age
            self.collector.update_data_age("cellana", self.cex_symbol) # Standardize symbol for age tracking

            # Trigger immediate check (Reactive)
            # Use create_task because this is called from a listener callback (potentially threaded)
            asyncio.create_task(self._trigger_dex_involved_checks())
        except (ValueError, TypeError):
            return

    def _on_hyperion_update(self, payload: dict) -> None:
        """Receive latest Hyperion state from listener callback."""
        # Handle both swap events and bootstrap events
        sqrt_price = payload.get("sqrt_price_x64") or payload.get("sqrt_price_x96")
        liquidity = payload.get("liquidity")
        
        if sqrt_price is not None:
            self.hyperion_sqrt_price_x64 = int(sqrt_price)
        if liquidity is not None:
            self.hyperion_liquidity = int(liquidity)
            
        self.hyperion_last_update_ts = time.time()
        
        # Trigger immediate check (Reactive)
        asyncio.create_task(self._trigger_dex_involved_checks())

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

    def _log_and_execute(
        self,
        direction: str,
        buy_price: float,
        sell_price: float,
        log_msg: str,
        legs: Optional[list[TradeLeg]] = None,
        profit_est: float = 0.0,
        is_shadow: bool = False,
        skip_reason: Optional[str] = None
    ) -> None:
        """Centralized logic for de-duplication, logging and execution."""
        # 1. De-duplication
        if not self.deduplicator.should_log(direction, buy_price, sell_price):
            return

        # 2. Log
        if is_shadow:
            logger.info(f"👻 [SHADOW] {log_msg} | REASON: {skip_reason}")
            return # Don't execute shadow deals or send to TG usually (to avoid spam)
            
        logger.success(log_msg)
        # 2. Extract trade steps for logging
        trade_steps = []
        if legs:
            for leg in legs:
                try:
                    trade_steps.append({
                        "exchange": getattr(leg, "exchange", "unknown"),
                        "symbol": getattr(leg, "symbol", "unknown"),
                        "side": leg.side.value if hasattr(leg.side, "value") else str(leg.side),
                        "qty": float(leg.qty),
                        "price_est": float(leg.price_est),
                        "tag": getattr(leg, "tag", "")
                    })
                except Exception as e:
                    logger.error(f"Error extracting leg info for log: {e}")

        # Prepare payload
        log_payload = {
            "direction": direction,
            "buy_price": float(buy_price),
            "sell_price": float(sell_price),
            "log_msg": log_msg,
            "profit_usdt": float(profit_est),
            "is_shadow": is_shadow,
            "skip_reason": skip_reason,
            "dry_run": settings.dry_run,
            "trade_steps": trade_steps
        }
        
        if self.cellana_reserves_ami is not None:
             log_payload.update({
                 "pool_reserve_ami": self.cellana_reserves_ami,
                 "pool_reserve_apt": self.cellana_reserves_apt,
                 "dex_price_ami_apt": self.cellana_last_spot
             })

        # 3. Execution - HIGH PRIORITY
        if self.trade_executor and legs:
            logger.info(f"🚀 Executing {direction} (Steps: {len(trade_steps)})")
            exec_coro = self.trade_executor.execute_multi_leg(
                direction=direction,
                legs=legs,
                profit_est=profit_est,
                trade_steps=trade_steps,
                parallel=False,
            )
            asyncio.create_task(self._safe_execute(direction, exec_coro))

        # 4. Logging & Notification (ASYNC)
        asyncio.create_task(self._async_log_and_notify(log_payload))

    async def _async_log_and_notify(self, log_payload: dict) -> None:
        """Handle background logging and notifications to avoid blocking execution."""
        try:
            # Log to file
            log_arbitrage_opportunity(log_payload)
            
            # Telegram notification
            direction = log_payload.get("direction", "Unknown")
            profit_est = log_payload.get("profit_usdt", 0.0)
            log_msg = log_payload.get("log_msg", "")
            
            tg_tag = "💎 " if not settings.dry_run else "🧪 [DRY] 💎 "
            
            try:
                # Truncate log_msg to avoid 400 Bad Request: message is too long
                disp_log = log_msg[:200] + "..." if len(log_msg) > 200 else log_msg
                text = (
                    f"<b>{tg_tag}Opportunity Detected</b>\n"
                    f"Direction: <code>{direction}</code>\n"
                    f"Profit: <b>${profit_est:.4f}</b>\n"
                    f"Detail: <code>{disp_log}</code>"
                )
                if len(text) > 4000:
                    text = text[:3900] + "\n... (truncated)"
                await tg_notifier.send_message(text)
            except Exception as e:
                logger.error(f"Error sending TG notification: {e}")
                
        except Exception as e:
            logger.error(f"Error in _async_log_and_notify: {e}")

    async def _safe_execute(self, direction: str, coro) -> None:
        """Wrapper to enforce lock and cooldown per trade."""
        if self._execution_lock.locked():
            logger.debug(f"⏭️ Lock busy — skipping execution for {direction}")
            return

        async with self._execution_lock:
            # Cooldown check
            now = time.time()
            elapsed = now - self._last_trade_ts
            if elapsed < self._trade_cooldown_s:
                logger.debug(
                    f"Trade cooldown: {self._trade_cooldown_s - elapsed:.1f}s "
                    f"remaining — skipping {direction}"
                )
                return

            self._last_trade_ts = now
            try:
                await coro
            except Exception as e:
                logger.error(f"Trade execution task failed for {direction}: {e}")

    def _amm_out(
        self,
        amount_in: float,
        reserve_in: int,
        reserve_out: int,
        fee_rate: float,
    ) -> float:
        """Constant product AMM output with fee.

        amount_in is in human units (not raw decimals).
        reserves are raw integer on-chain units (1e8 decimals).
        returns output in human units.
        """
        if amount_in <= 0:
            return 0.0
        if reserve_in <= 0 or reserve_out <= 0:
            return 0.0

        amount_in_raw = amount_in * self.token_decimals
        amount_in_after_fee = amount_in_raw * (1.0 - fee_rate)

        numerator = amount_in_after_fee * reserve_out
        denominator = reserve_in + amount_in_after_fee
        if denominator <= 0:
            return 0.0

        amount_out_raw = numerator / denominator
        return amount_out_raw / self.token_decimals

    def _check_all_routes(
        self,
        bybit: Optional[PriceData],
        mexc: Optional[PriceData],
        bybit_apt: Optional[PriceData],
        mexc_apt: Optional[PriceData]
    ) -> List[Opportunity]:
        """Collect and return all profitable arbitrage opportunities."""
        opportunities = []

        # 1. CEX-CEX
        if self.enable_bybit_arb and self.enable_mexc_arb and bybit and mexc:
            opp = self._check_cex_cex(bybit, mexc)
            if opp: opportunities.append(opp)

        # 2. DEX-CEX (Bybit)
        if self.enable_bybit_arb and bybit and bybit_apt:
            opp_dc = self._check_dex_cex_for_exchange("Bybit", bybit, bybit_apt, self.bybit_fee)
            if opp_dc: opportunities.append(opp_dc)
            
            opp_ca = self._check_circular_arbitrage_ami("Bybit", bybit, bybit_apt, self.bybit_fee)
            if opp_ca: opportunities.append(opp_ca)
            
            opps_h = self._check_hyperion_dex_cex("Bybit", bybit, bybit_apt, self.bybit_fee)
            if opps_h: opportunities.extend(opps_h)

        # 3. DEX-CEX (MEXC)
        if self.enable_mexc_arb and mexc and mexc_apt:
            opp_dc_m = self._check_dex_cex_for_exchange("MEXC", mexc, mexc_apt, self.mexc_fee)
            if opp_dc_m: opportunities.append(opp_dc_m)
            
            opp_ca_m = self._check_circular_arbitrage_ami("MEXC", mexc, mexc_apt, self.mexc_fee)
            if opp_ca_m: opportunities.append(opp_ca_m)
            
            opps_h_m = self._check_hyperion_dex_cex("MEXC", mexc, mexc_apt, self.mexc_fee)
            if opps_h_m: opportunities.extend(opps_h_m)

        # 4. Cross-CEX routes
        cross_opps = self._check_cross_cex_routes(bybit, mexc, bybit_apt, mexc_apt)
        if cross_opps:
            opportunities.extend(cross_opps)

        # 5. DEX-DEX
        apt_ref = bybit_apt.mid if bybit_apt else (mexc_apt.mid if mexc_apt else 0.0)
        if apt_ref > 0:
            dex_dex_opps = self._check_dex_dex(apt_ref)
            if dex_dex_opps:
                opportunities.extend(dex_dex_opps)

        return opportunities

    async def _trigger_dex_involved_checks(self) -> None:
        """Trigger arbitrage checks that involve DEX prices immediately after a DEX update."""
        cex_prices = self.collector.get(self.cex_symbol)
        bybit = cex_prices.get("bybit")
        mexc = cex_prices.get("mexc")

        apt_prices = self.collector.get(self.apt_cex_symbol)
        bybit_apt = apt_prices.get("bybit")
        mexc_apt = apt_prices.get("mexc")

        opportunities = self._check_all_routes(bybit, mexc, bybit_apt, mexc_apt)
        
        if not opportunities:
            return

        # 1. Identity feasible opportunities (those we have enough balance for)
        feasible_opps: List[Tuple[Opportunity, dict]] = []
        for opp in opportunities:
            if self.trade_executor:
                # We use a helper to check if we HAVE the assets needed for this specific opp
                ok, details = await self.trade_executor._check_balances(opp.legs, opp.direction)
                if ok:
                    feasible_opps.append((opp, details))
                else:
                    # Log as shadow immediately if not feasible due to balance
                    self._log_and_execute(
                        opp.direction, opp.buy_price, opp.sell_price,
                        opp.log_msg, is_shadow=True, skip_reason=f"Insufficient Balance"
                    )

        if not feasible_opps:
            # If no opportunities are feasible, we might still want to log the best one as shadow if it was excluded only by balance
            if opportunities:
                best_raw = max(opportunities, key=lambda x: x.profit_usdt)
                logger.info(f"Tournament: {len(opportunities)} opps found, but NONE are feasible due to balance. Best was {best_raw.direction} (${best_raw.profit_usdt:.4f})")
            return

        # 2. Pick the best one among FEASIBLE ones
        best_opp, best_details = max(feasible_opps, key=lambda x: x[0].profit_usdt)
        
        if len(feasible_opps) > 1:
            logger.info(f"🏆 Tournament: Found {len(feasible_opps)} feasible opportunities. Best: {best_opp.direction} (${best_opp.profit_usdt:.4f})")

        # 3. Handle the winner
        is_shadow = False
        skip_reason = None
        
        if best_opp.profit_usdt < self.min_profit:
            is_shadow = True
            skip_reason = f"Profit ${best_opp.profit_usdt:.4f} < Threshold ${self.min_profit}"

        # Execute the winner
        self._log_and_execute(
            best_opp.direction,
            best_opp.buy_price,
            best_opp.sell_price,
            best_opp.log_msg,
            best_opp.legs,
            best_opp.profit_usdt,
            is_shadow=is_shadow,
            skip_reason=skip_reason
        )

        # 4. Log remaining feasible ones as shadows
        for opp, _ in feasible_opps:
            if opp == best_opp: continue
            self._log_and_execute(
                opp.direction, opp.buy_price, opp.sell_price,
                opp.log_msg, is_shadow=True, skip_reason="Not best feasible in tournament"
            )

    # ------------------------------------------------------------------ #
    #  CEX-CEX  (Bybit <-> MEXC, same symbol)
    # ------------------------------------------------------------------ #
    def _check_cex_cex(self, bybit: PriceData, mexc: PriceData) -> Optional[Opportunity]:
        if bybit.is_stale() or mexc.is_stale():
            return None

        # Direction 1: Buy Bybit ask -> Sell MEXC bid
        qty = min(bybit.ask_qty, mexc.bid_qty)
        if qty > 0:
            bv, sv, profit = self._calc_profit(
                bybit.ask, mexc.bid, qty, self.bybit_fee, self.mexc_fee
            )
            if profit > self.min_profit:
                direction = "CEX_CEX_BYBIT_MEXC"
                log_msg = (
                    f"💎 [CEX-CEX] {direction} | Profit: ${profit:.4f} | "
                    f"Buy Bybit: {bybit.ask:.4f} Sell MEXC: {mexc.bid:.4f} | Qty: {qty:.2f}"
                )
                legs = [
                    TradeLeg("Bybit", self.cex_symbol, LegSide.BUY, bybit.ask, qty),
                    TradeLeg("MEXC", self.cex_symbol, LegSide.SELL, mexc.bid, qty),
                ]
                return Opportunity(direction, profit, legs, bybit.ask, mexc.bid, log_msg)

        # Direction 2: Buy MEXC ask -> Sell Bybit bid
        qty = min(mexc.ask_qty, bybit.bid_qty)
        if qty > 0:
            bv, sv, profit = self._calc_profit(
                mexc.ask, bybit.bid, qty, self.mexc_fee, self.bybit_fee
            )
            if profit > self.min_profit:
                direction = "CEX_CEX_MEXC_BYBIT"
                log_msg = (
                    f"💎 [CEX-CEX] {direction} | Profit: ${profit:.4f} | "
                    f"Buy MEXC: {mexc.ask:.4f} Sell Bybit: {bybit.bid:.4f} | Qty: {qty:.2f}"
                )
                legs = [
                    TradeLeg("MEXC", self.cex_symbol, LegSide.BUY, mexc.ask, qty),
                    TradeLeg("Bybit", self.cex_symbol, LegSide.SELL, bybit.bid, qty),
                ]
                return Opportunity(direction, profit, legs, mexc.ask, bybit.bid, log_msg)
        
        return None

    # ------------------------------------------------------------------ #
    #  DEX-CEX  (Cellana AMI/APT <-> CEX AMIUSDT + APTUSDT)
    # ------------------------------------------------------------------ #
    def _check_dex_cex_for_exchange(
        self,
        exchange_name: str,
        ami_quote: PriceData,
        apt_quote: PriceData,
        cex_fee: float,
    ) -> Optional[Opportunity]:
        if ami_quote.is_stale(5.0) or apt_quote.is_stale(5.0):
            return None
        if self.cellana_reserves_ami is None or self.cellana_reserves_apt is None:
            return None
            
        # gRPC-aware staleness: only marks stale if gRPC is down AND age > 40s
        grpc_active = self.cellana_listener and self.cellana_listener.is_grpc_active
        if not grpc_active and (time.time() - self.cellana_last_update_ts > 40.0):
            return None

        apt_mid = apt_quote.mid
        if apt_mid <= 0:
            return None

        r_ami = self.cellana_reserves_ami
        r_apt = self.cellana_reserves_apt
            
        # FAST PATH: Check with baseline size first
        # Optimization: Don't call _find_optimal_size if baseline isn't profitable
        baseline_profit = self._eval_profit(settings.trade_amount_usdt, "DEX_CEX_APT_CYCLE", 
                                          apt_mid=apt_mid, r_apt=r_apt, r_ami=r_ami,
                                          ami_bid=ami_quote.bid, apt_ask=apt_quote.ask, cex_fee=cex_fee)
        
        if baseline_profit <= 0:
            return None

        # Optimization: Find optimal size
        opt_size_usdt = self._find_optimal_size(
            "DEX_CEX_APT_CYCLE",
            apt_mid=apt_mid, r_apt=r_apt, r_ami=r_ami,
            ami_bid=ami_quote.bid, apt_ask=apt_quote.ask, cex_fee=cex_fee
        )
        apt_start = opt_size_usdt / apt_mid
        
        # Step 1: Swap APT → AMI on Cellana DEX
        ami_out = self._amm_out(
            amount_in=apt_start,
            reserve_in=int(r_apt),
            reserve_out=int(r_ami),
            fee_rate=self.cellana_fee,
        )
        if ami_out > 0:
            # Step 2: Sell AMI on CEX for USDT
            usdt_from_ami = ami_out * ami_quote.bid * (1.0 - cex_fee)
            
            # Step 3: Buy APT back on CEX with USDT
            apt_end = (usdt_from_ami / apt_quote.ask) * (1.0 - cex_fee)
            
            # Calculate profit in APT terms
            profit_apt = apt_end - apt_start
            profit_usdt = profit_apt * apt_mid
            
            if profit_usdt > self.min_profit:
                direction = f"CIRCULAR_APT_{exchange_name.upper()}"
                log_msg = (
                    f"💎 [DEX-CEX] {direction} | Profit: ${profit_usdt:.4f} | "
                    f"Buy Cellana DEX AMI (with APT) -> Sell {exchange_name} AMI | Qty: {ami_out:.2f}"
                )
                
                legs = [
                    TradeLeg(
                        exchange="cellana",
                        symbol="APT_AMI",
                        side=LegSide.BUY,
                        qty=apt_start,
                        price_est=ami_out / apt_start,
                        tag="cellana_apt_to_ami",
                        is_dex=True,
                        dex_direction="apt_to_ami",
                    ),
                    TradeLeg(
                        exchange=exchange_name.lower(),
                        symbol=settings.cex_symbol,
                        side=LegSide.SELL,
                        qty=ami_out,
                        price_est=ami_quote.bid,
                        tag=f"cex_sell_ami_{exchange_name}",
                    ),
                    TradeLeg(
                        exchange=exchange_name.lower(),
                        symbol=settings.apt_cex_symbol,
                        side=LegSide.BUY,
                        qty=apt_end,
                        price_est=apt_quote.ask,
                        tag=f"cex_buy_apt_{exchange_name}",
                    ),
                ]
                return Opportunity(direction, profit_usdt, legs, ami_quote.bid, apt_quote.ask, log_msg)
        
        return None

        # Direction B: Circular USDT cycle (TẠM COMMENT)
        # USDT → AMI (CEX) → APT (DEX) → USDT (CEX)
        # usdt_start = notional_usdt
        # 
        # Step 1: Buy AMI on CEX with USDT
        # ami_bought = (usdt_start / ami_quote.ask) * (1.0 - cex_fee)
        # 
        # Step 2: Swap AMI → APT on Cellana DEX
        # apt_out = self._amm_out(
        #     amount_in=ami_bought,
        #     reserve_in=self.cellana_reserves_ami,
        #     reserve_out=self.cellana_reserves_apt,
        #     fee_rate=self.cellana_fee,
        # )
        # if apt_out > 0:
        #     # Step 3: Sell APT on CEX for USDT (use bid price)
        #     usdt_end = apt_out * apt_quote.bid * (1.0 - cex_fee)
        #     
        #     # Calculate profit in USDT terms
        #     profit_usdt = usdt_end - usdt_start
        #     
        #     if profit_usdt > self.min_profit:
        #         logger.success(
        #             f"CEX->DEX (Circular USDT) {exchange_name} | "
        #             f"notional={notional_usdt:.2f}  ami_bought={ami_bought:.6f}  apt_out={apt_out:.6f}  "
        #             f"usdt_end={usdt_end:.2f}  profit={profit_usdt:.4f} USDT | "
        #             f"ami_ask={ami_quote.ask:.8f}  apt_bid={apt_quote.bid:.8f}"
        #         )

    def _check_circular_arbitrage_apt(
        self,
        exchange_name: str,
        ami_quote: PriceData,
        apt_quote: PriceData,
        cex_fee: float,
    ) -> Optional[Opportunity]:
        """Circular APT arbitrage: APT → AMI (DEX) → USDT (CEX) → APT (CEX)"""
        # Note: This is now largely redundant with _check_dex_cex_for_exchange
        # which implements the same circular cycle.
        return None

        """
        Circular AMI arbitrage: AMI → USDT (CEX) → APT (CEX) → AMI (DEX)
        Track profit in AMI terms, report in USDT equivalent.
        """
    def _check_circular_arbitrage_ami(
        self,
        exchange_name: str,
        ami_quote: PriceData,
        apt_quote: PriceData,
        cex_fee: float,
    ) -> Optional[Opportunity]:
        """Circular AMI arbitrage: AMI → USDT (CEX) → APT (CEX) → AMI (DEX)"""
        if ami_quote.is_stale(5.0) or apt_quote.is_stale(5.0):
            return None
        
        r_ami = self.cellana_reserves_ami
        r_apt = self.cellana_reserves_apt
        if r_ami is None or r_apt is None:
            return None
            
        # gRPC-aware staleness
        grpc_active = self.cellana_listener and self.cellana_listener.is_grpc_active
        if not grpc_active and (time.time() - self.cellana_last_update_ts > 40.0):
            return None

        ami_mid = ami_quote.mid
        apt_mid = apt_quote.mid
        if ami_mid <= 0 or apt_mid <= 0:
            return None

        # FAST PATH: Check with baseline size first
        # Optimization: Don't call _find_optimal_size if baseline isn't profitable
        baseline_profit = self._eval_profit(settings.trade_amount_usdt, "CIRCULAR_AMI", 
                                          ami_mid=ami_mid, r_apt=r_apt, r_ami=r_ami,
                                          ami_bid=ami_quote.bid, apt_ask=apt_quote.ask, cex_fee=cex_fee)
        
        if baseline_profit <= 0:
            return None

        # Optimization: Find optimal size only if baseline is promising
        opt_size_usdt = self._find_optimal_size(
            "CIRCULAR_AMI",
            ami_mid=ami_mid, r_apt=r_apt, r_ami=r_ami,
            ami_bid=ami_quote.bid, apt_ask=apt_quote.ask, cex_fee=cex_fee
        )
        ami_start = opt_size_usdt / ami_mid

        # Step 1: Sell AMI on CEX for USDT
        usdt_out = ami_start * ami_quote.bid * (1.0 - cex_fee)

        # Step 2: Buy APT on CEX with USDT
        apt_out = (usdt_out / apt_quote.ask) * (1.0 - cex_fee)

        # Step 3: Swap APT → AMI on Cellana DEX
        ami_end = self._amm_out(
            amount_in=apt_out,
            reserve_in=int(r_apt),
            reserve_out=int(r_ami),
            fee_rate=self.cellana_fee,
        )
        if ami_end <= 0:
            return None

        # Calculate profit
        profit_ami = ami_end - ami_start
        profit_usdt = profit_ami * ami_mid

        if profit_usdt > self.min_profit:
            direction = f"CIRCULAR_AMI_{exchange_name.upper()}"
            log_msg = (
                f"💎 [Circular AMI] {direction} | Profit: ${profit_usdt:.4f} | "
                f"Sell {exchange_name} AMI -> Buy APT on CEX -> Buy Cellana DEX AMI | Qty: {ami_start:.2f}"
            )
            
            legs = [
                TradeLeg(
                    exchange=exchange_name.lower(),
                    symbol=settings.cex_symbol,
                    side=LegSide.SELL,
                    qty=ami_start,
                    price_est=ami_quote.bid,
                    tag=f"cex_sell_ami_{exchange_name}",
                ),
                TradeLeg(
                    exchange=exchange_name.lower(),
                    symbol=settings.apt_cex_symbol,
                    side=LegSide.BUY,
                    qty=apt_out,
                    price_est=apt_quote.ask,
                    tag=f"cex_buy_apt_{exchange_name}",
                ),
                TradeLeg(
                    exchange="cellana",
                    symbol="APT_AMI",
                    side=LegSide.BUY,
                    qty=apt_out,
                    price_est=ami_end / apt_out,
                    tag="cellana_apt_to_ami",
                    is_dex=True,
                    dex_direction="apt_to_ami",
                ),
            ]
            return Opportunity(direction, profit_usdt, legs, float(ami_quote.bid), float(ami_end / apt_out), log_msg)
        
        return None

    def _check_hyperion_dex_cex(
        self,
        exchange_name: str,
        ami_quote: PriceData,
        apt_quote: PriceData,
        cex_fee: float,
    ) -> List[Opportunity]:
        """Check arbitrage between Hyperion (CLMM) and CEX."""
        results = []
        if ami_quote.is_stale(5.0) or apt_quote.is_stale(5.0):
            return []
        if self.hyperion_sqrt_price_x64 is None or self.hyperion_liquidity is None:
            return []
            
        # gRPC-aware staleness
        grpc_active = self.hyperion_listener and self.hyperion_listener.is_grpc_active
        if not grpc_active and (time.time() - self.hyperion_last_update_ts > 40.0):
            return []

        apt_mid = apt_quote.mid
        if apt_mid <= 0:
            return []

        # ------------------------------------------------------------------ #
        # Direction A: Circular APT (APT -> AMI (Hyperion) -> USDT (CEX) -> APT (CEX))
        # ------------------------------------------------------------------ #
        h_sqrt_p = self.hyperion_sqrt_price_x64
        h_liq = self.hyperion_liquidity

        # FAST PATH: Check with baseline size first
        # Optimization: Don't call _find_optimal_size if baseline isn't profitable
        baseline_profit = self._eval_profit(settings.trade_amount_usdt, "HYPERION_APT_CYCLE", 
                                          apt_mid=apt_mid, h_sqrt_p=h_sqrt_p, h_liq=h_liq,
                                          ami_bid=ami_quote.bid, apt_ask=apt_quote.ask, cex_fee=cex_fee)
        
        if baseline_profit <= 0:
            ami_out = 0 # Skip calculation
        else:
            # Optimization: Find optimal size
            opt_size_usdt = self._find_optimal_size(
                "HYPERION_APT_CYCLE",
                apt_mid=apt_mid, h_sqrt_p=h_sqrt_p, h_liq=h_liq,
                ami_bid=ami_quote.bid, apt_ask=apt_quote.ask, cex_fee=cex_fee
            )
            apt_start = opt_size_usdt / apt_mid
            
            ami_out, impact = calculate_amount_out(
                h_sqrt_p,
                h_liq,
                apt_start,
                fee_rate=self.hyperion_fee,
                is_token0_to_token1=True
            )
        
        if ami_out > 0:
            # Step 2: Sell AMI on CEX for USDT
            usdt_from_ami = ami_out * ami_quote.bid * (1.0 - cex_fee)
            # Step 3: Buy APT back on CEX with USDT
            apt_end = (usdt_from_ami / apt_quote.ask) * (1.0 - cex_fee)
            
            profit_apt = apt_end - apt_start
            profit_usdt = profit_apt * apt_mid
            if profit_usdt > self.min_profit:
                direction = f"HYPERION_APT_CYCLE_{exchange_name.upper()}"
                log_msg = (
                    f"💎 [Hyperion Circular APT] {direction} | Profit: ${profit_usdt:.4f} | "
                    f"Buy Hyperion AMI (with APT) -> Sell {exchange_name} AMI | Qty: {ami_out:.2f}"
                )
                
                legs = [
                    TradeLeg(
                        exchange="hyperion",
                        symbol="APT_AMI",
                        side=LegSide.BUY,
                        qty=apt_start,
                        price_est=ami_out / apt_start,
                        tag="hyp_apt_to_ami",
                        is_dex=True,
                        dex_direction="apt_to_ami",
                    ),
                    TradeLeg(
                        exchange=exchange_name.lower(),
                        symbol=settings.cex_symbol,
                        side=LegSide.SELL,
                        qty=ami_out,
                        price_est=ami_quote.bid,
                        tag=f"cex_sell_ami_{exchange_name}",
                    ),
                    TradeLeg(
                        exchange=exchange_name.lower(),
                        symbol=settings.apt_cex_symbol,
                        side=LegSide.BUY,
                        qty=apt_end,
                        price_est=apt_quote.ask,
                        tag=f"cex_buy_apt_{exchange_name}",
                    ),
                ]
                results.append(Opportunity(direction, profit_usdt, legs, float(ami_out / apt_start), float(ami_quote.bid), log_msg))

        # ------------------------------------------------------------------ #
        # Direction B: Circular AMI (AMI -> APT (Hyperion) -> USDT (CEX) -> AMI (CEX))
        # ------------------------------------------------------------------ #
        ami_mid = ami_quote.mid
        if ami_mid <= 0:
            return results
            
        # FAST PATH: Check with baseline size (100 USDT) first
        # Optimization: Don't call _find_optimal_size if baseline isn't profitable
        baseline_profit_b = self._eval_profit(100.0, "HYPERION_AMI_CYCLE", 
                                            ami_mid=ami_mid, h_sqrt_p=h_sqrt_p, h_liq=h_liq,
                                            ami_bid=ami_quote.bid, apt_ask=apt_quote.ask, cex_fee=cex_fee)
        
        if baseline_profit_b <= 0:
            apt_out = 0 # Skip
        else:
            # Optimization: Find optimal size
            opt_size_usdt_b = self._find_optimal_size(
                "HYPERION_AMI_CYCLE",
                ami_mid=ami_mid, h_sqrt_p=h_sqrt_p, h_liq=h_liq,
                ami_bid=ami_quote.bid, apt_ask=apt_quote.ask, cex_fee=cex_fee
            )
            ami_start = opt_size_usdt_b / ami_mid
            
            # Swap AMI -> APT means token1 -> token0 (is_token0_to_token1=False)
            apt_out, impact_b = calculate_amount_out(
                h_sqrt_p,
                h_liq,
                ami_start,
                fee_rate=self.hyperion_fee,
                is_token0_to_token1=False
            )
        
        if apt_out > 0:
            # Step 2: Sell APT on CEX for USDT
            usdt_from_apt = apt_out * apt_quote.bid * (1.0 - cex_fee)
            # Step 3: Buy AMI back on CEX with USDT
            ami_end = (usdt_from_apt / ami_quote.ask) * (1.0 - cex_fee)
            
            profit_ami = ami_end - ami_start
            profit_usdt_b = profit_ami * ami_mid
            
            if profit_usdt_b > self.min_profit:
                direction = f"HYPERION_AMI_CYCLE_{exchange_name.upper()}"
                log_msg = (
                    f"💎 [Hyperion Circular AMI] {direction} | Profit: ${profit_usdt_b:.4f} | "
                    f"Sell {exchange_name} AMI -> Buy APT on CEX -> Buy Hyperion AMI | Qty: {ami_start:.2f}"
                )

                legs_b = [
                    TradeLeg(
                        exchange="hyperion",
                        symbol="AMI_APT",
                        side=LegSide.BUY,
                        qty=ami_start,
                        price_est=apt_out / ami_start,
                        tag="hyp_ami_to_apt",
                        is_dex=True,
                        dex_direction="ami_to_apt",
                    ),
                    TradeLeg(
                        exchange=exchange_name.lower(),
                        symbol=settings.apt_cex_symbol,
                        side=LegSide.SELL,
                        qty=apt_out,
                        price_est=apt_quote.bid,
                        tag=f"cex_sell_apt_{exchange_name}",
                    ),
                    TradeLeg(
                        exchange=exchange_name.lower(),
                        symbol=settings.cex_symbol,
                        side=LegSide.BUY,
                        qty=ami_end,
                        price_est=ami_quote.ask,
                        tag=f"cex_buy_ami_{exchange_name}",
                    ),
                ]
                results.append(Opportunity(direction, profit_usdt_b, legs_b, float(apt_out / ami_start), float(apt_quote.bid), log_msg))

        return results

    def _check_dex_dex(self, apt_mid: float) -> List[Opportunity]:
        """Check direct arbitrage between Cellana (XYK) and Hyperion (CLMM)."""
        results = []
        if not self.enable_dex_dex_arb:
            return []
        if apt_mid <= 0:
            return []
            
        # Check Cellana freshness (gRPC-aware)
        cellana_grpc = self.cellana_listener and self.cellana_listener.is_grpc_active
        if (self.cellana_reserves_ami is None or self.cellana_reserves_apt is None or 
            (not cellana_grpc and time.time() - self.cellana_last_update_ts > 40.0)):
            return []
            
        # Check Hyperion freshness (gRPC-aware)
        hyperion_grpc = self.hyperion_listener and self.hyperion_listener.is_grpc_active
        if (self.hyperion_sqrt_price_x64 is None or self.hyperion_liquidity is None or 
            (not hyperion_grpc and time.time() - self.hyperion_last_update_ts > 40.0)):
            return []

        notional_usdt_start = settings.trade_amount_usdt
        
        r_ami = self.cellana_reserves_ami
        r_apt = self.cellana_reserves_apt
        h_sqrt_p = self.hyperion_sqrt_price_x64
        h_liq = self.hyperion_liquidity
        
        # ------------------------------------------------------------------ #
        # Direction 1: APT -> Cellana -> AMI -> Hyperion -> APT
        # ------------------------------------------------------------------ #
        # FAST PATH: Check with baseline size first
        baseline_profit = self._eval_profit(
            settings.trade_amount_usdt, "DEX_DEX_CELLANA_HYPERION",
            apt_mid=apt_mid, r_apt=r_apt, r_ami=r_ami, h_sqrt_p=h_sqrt_p, h_liq=h_liq
        )
        
        if baseline_profit > self.min_profit_dex_dex:
            # Find optimal sizing
            opt_size_usdt = self._find_optimal_size(
                "DEX_DEX_CELLANA_HYPERION", 
                apt_mid=apt_mid, r_apt=r_apt, r_ami=r_ami, h_sqrt_p=h_sqrt_p, h_liq=h_liq
            )
            apt_start = opt_size_usdt / apt_mid
            
            # We know r_apt, r_ami, h_sqrt_p, h_liq are not None here
            ami_mid_out = self._amm_out(apt_start, int(r_apt), int(r_ami), self.cellana_fee)
            apt_end, impact = calculate_amount_out(
                int(h_sqrt_p), int(h_liq), ami_mid_out, self.hyperion_fee, is_token0_to_token1=False
            )
        else:
            # Baseline not profitable
            apt_start = settings.trade_amount_usdt / apt_mid
            ami_mid_out = 0
            apt_end = 0
            profit_usdt = -1
        
        profit_apt = apt_end - apt_start - self.gas_fee_dex_dex_apt
        profit_usdt = profit_apt * apt_mid
        spread_pct = (apt_end / apt_start - 1) * 100 if apt_start > 0 else 0
        
        if profit_usdt > self.min_profit_dex_dex:
            direction = "DEX_DEX_CELLANA_HYPERION"
            log_msg = (
                f"💎 [DEX-DEX] {direction} | Profit: ${profit_usdt:.4f} | "
                f"apt_in={apt_start:.4f} -> Cellana AMI -> Hyperion APT | Qty: {ami_mid_out:.2f}"
            )

            legs = [
                TradeLeg(
                    exchange="cellana",
                    symbol="APT_AMI",
                    side=LegSide.BUY,
                    qty=apt_start,
                    price_est=ami_mid_out / apt_start if apt_start > 0 else 0,
                    tag="dex_dex_cellana_apt_to_ami",
                    is_dex=True,
                    dex_direction="apt_to_ami",
                ),
                TradeLeg(
                    exchange="hyperion",
                    symbol="AMI_APT",
                    side=LegSide.BUY,
                    qty=ami_mid_out,
                    price_est=apt_end / ami_mid_out if ami_mid_out > 0 else 0,
                    tag="dex_dex_hyp_ami_to_apt",
                    is_dex=True,
                    dex_direction="ami_to_apt",
                ),
            ]
            results.append(Opportunity(direction, profit_usdt, legs, float(ami_mid_out / apt_start) if apt_start > 0 else 0, float(apt_end / ami_mid_out) if ami_mid_out > 0 else 0, log_msg))

        # ------------------------------------------------------------------ #
        # Direction 2: APT -> Hyperion -> AMI -> Cellana -> APT
        # ------------------------------------------------------------------ #
        # FAST PATH: Check with baseline size (100 USDT) first
        baseline_profit2 = self._eval_profit(
            100.0, "DEX_DEX_HYPERION_CELLANA",
            apt_mid=apt_mid, r_apt=r_apt, r_ami=r_ami, h_sqrt_p=h_sqrt_p, h_liq=h_liq
        )
        
        if baseline_profit2 > self.min_profit_dex_dex:
            # Find optimal sizing
            opt_size_usdt2 = self._find_optimal_size(
                "DEX_DEX_HYPERION_CELLANA",
                apt_mid=apt_mid, r_apt=r_apt, r_ami=r_ami, h_sqrt_p=h_sqrt_p, h_liq=h_liq
            )
            apt_start2 = opt_size_usdt2 / apt_mid
            ami_hyp_out, impact2 = calculate_amount_out(
                int(h_sqrt_p), int(h_liq), apt_start2, self.hyperion_fee, is_token0_to_token1=True
            )
            if ami_hyp_out > 0:
                apt_end2 = self._amm_out(ami_hyp_out, int(r_ami), int(r_apt), self.cellana_fee)
            else:
                apt_end2 = 0
        else:
            # Baseline not profitable
            apt_start2 = 100.0 / apt_mid
            apt_end2 = 0
            ami_hyp_out = 0
            profit_usdt2 = -1
        
        if apt_end2 > 0:
            profit_apt2 = apt_end2 - apt_start2 - self.gas_fee_dex_dex_apt
            profit_usdt2 = profit_apt2 * apt_mid
            spread_pct2 = (apt_end2 / apt_start2 - 1) * 100 if apt_start2 > 0 else 0
            
            if profit_usdt2 > self.min_profit_dex_dex:
                direction = "DEX_DEX_HYPERION_CELLANA"
                log_msg = (
                    f"💎 [DEX-DEX] {direction} | Profit: ${profit_usdt2:.4f} | "
                    f"apt_in={apt_start2:.4f} -> Hyperion AMI -> Cellana APT | Qty: {ami_hyp_out:.2f}"
                )

                legs = [
                    TradeLeg(
                        exchange="hyperion",
                        symbol="APT_AMI",
                        side=LegSide.BUY,
                        qty=apt_start2,
                        price_est=ami_hyp_out / apt_start2 if apt_start2 > 0 else 0,
                        tag="dex_dex_hyp_apt_to_ami",
                        is_dex=True,
                        dex_direction="apt_to_ami",
                    ),
                    TradeLeg(
                        exchange="cellana",
                        symbol="AMI_APT",
                        side=LegSide.BUY,
                        qty=ami_hyp_out,
                        price_est=apt_end2 / ami_hyp_out if ami_hyp_out > 0 else 0,
                        tag="dex_dex_cellana_ami_to_apt",
                        is_dex=True,
                        dex_direction="ami_to_apt",
                    ),
                ]
                results.append(Opportunity(direction, profit_usdt2, legs, float(ami_hyp_out / apt_start2) if apt_start2 > 0 else 0, float(apt_end2 / ami_hyp_out) if ami_hyp_out > 0 else 0, log_msg))
        
        return results

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
        while self._is_running:
            # Update dynamic gas cost for this cycle
            current_gas_cost = self._get_dynamic_gas_cost_usd()
            self.gas_cost_usd = current_gas_cost

            # 1. Collect potential opportunities
            cex_prices = self.collector.get(self.cex_symbol)
            bybit = cex_prices.get("bybit")
            mexc = cex_prices.get("mexc")

            apt_prices = self.collector.get(self.apt_cex_symbol)
            bybit_apt = apt_prices.get("bybit")
            mexc_apt = apt_prices.get("mexc")

            now = time.time()
            if now - self._last_price_log >= self._PRICE_LOG_INTERVAL_S:
                self._last_price_log = now
                bybit_str = f"{bybit.bid:.6f}/{bybit.ask:.6f}" if bybit else "N/A"
                mexc_str = f"{mexc.bid:.6f}/{mexc.ask:.6f}" if mexc else "N/A"
                bybit_apt_str = f"{bybit_apt.bid:.6f}/{bybit_apt.ask:.6f}" if bybit_apt else "N/A"
                mexc_apt_str = f"{mexc_apt.bid:.6f}/{mexc_apt.ask:.6f}" if mexc_apt else "N/A"
                dex_str = "N/A"
                r_ami_p = self.cellana_reserves_ami
                r_apt_p = self.cellana_reserves_apt
                if r_ami_p and r_apt_p:
                    dex_spot = float(r_apt_p) / float(r_ami_p)
                    dex_str = f"{dex_spot:.8f} APT/AMI"
                
                hyp_str = "N/A"
                hyperion_spot = None
                if self.hyperion_sqrt_price_x64 is not None:
                    # pylint: disable=no-value-for-parameter
                    hyperion_spot = float(decode_sqrt_price(int(self.hyperion_sqrt_price_x64))**2)
                    hyp_str = f"{hyperion_spot:.8f} AMI/APT"

                logger.info(
                    f"[PRICES] AMIUSDT | Bybit={bybit_str} MEXC={mexc_str} | "
                    f"APTUSDT Bybit={bybit_apt_str} MEXC={mexc_apt_str} | "
                    f"Cellana={self.cellana_last_spot if self.cellana_last_spot else 'N/A'} APT/AMI "
                    f"Hyperion={f'{hyperion_spot:.8f}' if hyperion_spot else 'N/A'} AMI/APT | "
                    f"Gas: ${self.gas_cost_usd:.4f}"
                )

            # Use tournament logic in main loop
            opportunities = self._check_all_routes(bybit, mexc, bybit_apt, mexc_apt)
            if opportunities:
                best_opp = max(opportunities, key=lambda x: x.profit_usdt)
                self._log_and_execute(
                    best_opp.direction,
                    best_opp.buy_price,
                    best_opp.sell_price,
                    best_opp.log_msg,
                    best_opp.legs,
                    best_opp.profit_usdt
                )

            # Performance Optimization: If gRPC is active, we don't need to poll so fast
            is_streaming = (self.cellana_listener and self.cellana_listener.is_grpc_active) or \
                           (self.hyperion_listener and self.hyperion_listener.is_grpc_active)
            
            p_interval = self.poll_interval
            if is_streaming:
                # If streaming is active, polling is just a backup. 3-5 seconds is plenty.
                p_interval = max(p_interval, 3.0)
                
            await asyncio.sleep(p_interval)

    def _get_dynamic_gas_cost_usd(self, gas_limit: Optional[int] = None) -> float:
        """Calculate gas cost in USD based on current network gas price and APT price."""
        if not self.gas_monitor:
            return settings.gas_cost_usd # Fallback to static setting
            
        gas_unit_price = self.gas_monitor.get_gas_unit_price()
        # Get APT price for conversion (use mid price of any avail exchange)
        apt_prices = self.collector.get(self.apt_cex_symbol)
        apt_price = 10.0  # fallback
        if apt_prices:
            # Try to get a non-stale APT price from any exchange
            for ex_data in apt_prices.values():
                if ex_data and not ex_data.is_stale():
                    apt_price = ex_data.mid
                    break
            
        # cost = (units * unit_price / 1e8) * apt_usd_price
        target_limit = gas_limit if gas_limit is not None else settings.swap_gas_limit
        cost_usd = (target_limit * gas_unit_price / 1e8) * apt_price
        return float(cost_usd)

    def _find_optimal_size(self, route_type: str, **kwargs) -> float:
        """Find optimal USDT size using Golden-section search to maximize net profit.
        
        The search range is capped by the actual available balance from BalanceManager
        to avoid proposing trades that exceed wallet funds.
        """
        if not self.optimal_size_enabled:
            return settings.trade_amount_usdt

        low = self.min_trade_usdt
        high = self.max_trade_usdt

        # Cap by actual wallet balance (if BalanceManager is available) - DISABLED if virtual_sizing_enabled
        if not settings.virtual_sizing_enabled and self.trade_executor and self.trade_executor.balance_manager:
            # Refresh balances to get fresh data
            asyncio.create_task(self.trade_executor.balance_manager.refresh_all())
            
            avail_usdt = self.trade_executor.balance_manager.get_total_available_usdt()
            if avail_usdt > 0:
                # Leave 2-3% buffer for fees/slippage/gas
                safety_buffer = 0.98 
                balance_cap = avail_usdt * safety_buffer
                
                # If we are doing a CIRCULAR_APT route, we might also be capped by APT balance
                # but USDT is a good universal proxy for "available capital" in this bot's search
                high = min(high, balance_cap)
                
                if high < low:
                    return low # Not enough balance for even minimum trade
        
        
        # Golden-section search iterations
        for _ in range(self.optimal_size_steps):
            d = self._phi * (high - low)
            x1 = low + d
            x2 = high - d
            
            p1 = self._eval_profit(x1, route_type, **kwargs)
            p2 = self._eval_profit(x2, route_type, **kwargs)
            
            if p1 > p2:
                low = x2
            else:
                high = x1
                
        optimal = (low + high) / 2
        
        # Safety: don't let it go below min_trade_usdt
        return max(self.min_trade_usdt, optimal)

    def _eval_profit(self, size_usdt: float, route_type: str, **kwargs) -> float:
        """Evaluate expected net profit (in USDT) for a given trade size."""
        if route_type == "DEX_DEX_CELLANA_HYPERION":
            try:
                apt_mid_val = float(kwargs["apt_mid"])
                r_apt_val = int(kwargs["r_apt"])
                r_ami_val = int(kwargs["r_ami"])
                h_sqrt_p_val = int(kwargs["h_sqrt_p"])
                h_liq_val = int(kwargs["h_liq"])
                apt_start = float(size_usdt / apt_mid_val)
                ami_out = self._amm_out(apt_start, r_apt_val, r_ami_val, self.cellana_fee)
                if ami_out <= 0: return -1.0
                apt_end, _ = calculate_amount_out(h_sqrt_p_val, h_liq_val, ami_out, self.hyperion_fee, False)
                
                # Total gas = Cellana + Hyperion
                total_gas_usd = self._get_dynamic_gas_cost_usd(settings.cellana_swap_gas_limit + settings.hyperion_swap_gas_limit)
                return (float(apt_end) - apt_start) * apt_mid_val - total_gas_usd
            except (KeyError, TypeError, ValueError):
                return -1.0

        # 2. APT -> Hyperion -> Cellana -> APT
        elif route_type == "DEX_DEX_HYPERION_CELLANA":
            try:
                apt_mid_val = float(kwargs["apt_mid"])
                r_apt_val = int(kwargs["r_apt"])
                r_ami_val = int(kwargs["r_ami"])
                h_sqrt_p_val = int(kwargs["h_sqrt_p"])
                h_liq_val = int(kwargs["h_liq"])
                apt_start = float(size_usdt / apt_mid_val)
                ami_out, _ = calculate_amount_out(h_sqrt_p_val, h_liq_val, apt_start, self.hyperion_fee, True)
                if ami_out <= 0: return -1.0
                apt_end2 = self._amm_out(ami_out, r_ami_val, r_apt_val, self.cellana_fee)
                
                # Total gas = Cellana + Hyperion
                total_gas_usd = self._get_dynamic_gas_cost_usd(settings.cellana_swap_gas_limit + settings.hyperion_swap_gas_limit)
                return (float(apt_end2) - apt_start) * apt_mid_val - total_gas_usd
            except (KeyError, TypeError, ValueError):
                return -1.0

        # 3. APT -> Cellana -> CEX -> APT
        elif route_type == "DEX_CEX_APT_CYCLE":
            try:
                apt_usdt = float(kwargs["apt_mid"])
                r_apt_val = int(kwargs["r_apt"])
                r_ami_val = int(kwargs["r_ami"])
                apt_start = float(size_usdt / apt_usdt)
                ami_out = self._amm_out(apt_start, r_apt_val, r_ami_val, self.cellana_fee)
                if ami_out <= 0: return -1.0
                usdt_from_ami = float(ami_out * kwargs["ami_bid"] * (1.0 - kwargs["cex_fee"]))
                apt_end = float((usdt_from_ami / kwargs["apt_ask"]) * (1.0 - kwargs["cex_fee"]))
                gas_usd = self._get_dynamic_gas_cost_usd(settings.cellana_swap_gas_limit)
                return (apt_end - apt_start) * apt_usdt - gas_usd
            except (KeyError, TypeError, ValueError):
                return -1.0

        # 4. APT -> Hyperion -> CEX -> APT
        elif route_type == "HYPERION_APT_CYCLE":
            try:
                apt_mid_val = float(kwargs["apt_mid"])
                h_sqrt_p = int(kwargs["h_sqrt_p"])
                h_liq = int(kwargs["h_liq"])
                apt_start = float(size_usdt / apt_mid_val)
                ami_out, _ = calculate_amount_out(h_sqrt_p, h_liq, apt_start, self.hyperion_fee, True)
                if ami_out <= 0: return -1.0
                usdt_from_ami = float(ami_out * kwargs["ami_bid"] * (1.0 - kwargs["cex_fee"]))
                apt_end = float((usdt_from_ami / kwargs["apt_ask"]) * (1.0 - kwargs["cex_fee"]))
                gas_usd = self._get_dynamic_gas_cost_usd(settings.hyperion_swap_gas_limit)
                return (apt_end - apt_start) * apt_mid_val - gas_usd
            except (KeyError, TypeError, ValueError):
                return -1.0

        # 5. AMI -> CEX -> Hyperion -> AMI
        elif route_type == "HYPERION_AMI_CYCLE":
            try:
                ami_mid_val = float(kwargs["ami_mid"])
                h_sqrt_p = int(kwargs["h_sqrt_p"])
                h_liq = int(kwargs["h_liq"])
                ami_start = float(size_usdt / ami_mid_val)
                usdt_out = float(ami_start * kwargs["ami_bid"] * (1.0 - kwargs["cex_fee"]))
                apt_out = float((usdt_out / kwargs["apt_ask"]) * (1.0 - kwargs["cex_fee"]))
                ami_end, _ = calculate_amount_out(h_sqrt_p, h_liq, apt_out, self.hyperion_fee, False)
                gas_usd = self._get_dynamic_gas_cost_usd(settings.hyperion_swap_gas_limit)
                return (float(ami_end) - ami_start) * ami_mid_val - gas_usd
            except (KeyError, TypeError, ValueError):
                return -1.0

        # 6. CIRCULAR_AMI (AMI -> CEX -> Cellana -> AMI)
        elif route_type == "CIRCULAR_AMI":
            try:
                ami_mid_val = float(kwargs["ami_mid"])
                r_apt_val = int(kwargs["r_apt"])
                r_ami_val = int(kwargs["r_ami"])
                ami_start = float(size_usdt / ami_mid_val)
                usdt_out = float(ami_start * kwargs["ami_bid"] * (1.0 - kwargs["cex_fee"]))
                apt_out = float((usdt_out / kwargs["apt_ask"]) * (1.0 - kwargs["cex_fee"]))
                ami_end = self._amm_out(apt_out, r_apt_val, r_ami_val, self.cellana_fee)
                gas_usd = self._get_dynamic_gas_cost_usd(settings.cellana_swap_gas_limit)
                return (float(ami_end) - ami_start) * ami_mid_val - gas_usd
            except (KeyError, TypeError, ValueError):
                return -1.0

        # 7. CIRCULAR_APT (APT -> Cellana -> CEX -> APT)
        elif route_type == "CIRCULAR_APT":
            try:
                apt_mid_val = float(kwargs["apt_mid"])
                r_apt_val = int(kwargs["r_apt"])
                r_ami_val = int(kwargs["r_ami"])
                apt_start = float(size_usdt / apt_mid_val)
                ami_out = self._amm_out(apt_start, r_apt_val, r_ami_val, self.cellana_fee)
                if ami_out <= 0: return -1.0
                usdt_out = float(ami_out * kwargs["ami_bid"] * (1.0 - kwargs["cex_fee"]))
                apt_end = float((usdt_out / kwargs["apt_ask"]) * (1.0 - kwargs["cex_fee"]))
                gas_usd = self._get_dynamic_gas_cost_usd(settings.cellana_swap_gas_limit)
                return (apt_end - apt_start) * apt_mid_val - gas_usd
            except (KeyError, TypeError, ValueError):
                return -1.0

        # 8. CROSS_CEX_CIRCULAR_AMI (AMI -> CEX1(Sell AMI) -> CEX2(Buy APT) -> DEX -> AMI)
        elif route_type == "CROSS_CEX_CIRCULAR_AMI":
            try:
                ami_mid_val = float(kwargs["ami_mid"])
                r_apt_val = int(kwargs["r_apt"])
                r_ami_val = int(kwargs["r_ami"])
                ami_start = float(size_usdt / ami_mid_val)
                # Sell AMI on CEX1
                usdt_out = float(ami_start * kwargs["ami_bid"] * (1.0 - kwargs["ami_fee"]))
                # Buy APT on CEX2
                apt_out = float((usdt_out / kwargs["apt_ask"]) * (1.0 - kwargs["apt_fee"]))
                # Swap back to AMI on DEX
                ami_end = self._amm_out(apt_out, r_apt_val, r_ami_val, self.cellana_fee)
                gas_usd = self._get_dynamic_gas_cost_usd(settings.cellana_swap_gas_limit)
                return (float(ami_end) - ami_start) * ami_mid_val - gas_usd
            except (KeyError, TypeError, ValueError):
                return -1.0

        # 9. CROSS_CEX_CIRCULAR_APT (APT -> CEX1(Sell APT) -> CEX2(Buy AMI) -> DEX -> APT)
        elif route_type == "CROSS_CEX_CIRCULAR_APT":
            try:
                apt_mid_val = float(kwargs["apt_mid"])
                r_apt_val = int(kwargs["r_apt"])
                r_ami_val = int(kwargs["r_ami"])
                apt_start = float(size_usdt / apt_mid_val)
                # Sell APT on CEX1
                usdt_out = float(apt_start * kwargs["apt_bid"] * (1.0 - kwargs["apt_fee"]))
                # Buy AMI on CEX2
                ami_out = float((usdt_out / kwargs["ami_ask"]) * (1.0 - kwargs["ami_fee"]))
                # Swap back to APT on DEX
                apt_end = self._amm_out(ami_out, r_ami_val, r_apt_val, self.cellana_fee)
                gas_usd = self._get_dynamic_gas_cost_usd(settings.cellana_swap_gas_limit)
                return (float(apt_end) - apt_start) * apt_mid_val - gas_usd
            except (KeyError, TypeError, ValueError):
                return -1.0

        return -1.0

    def _check_cross_cex_routes(self, bybit, mexc, bybit_apt, mexc_apt) -> List[Opportunity]:
        """Find best bid/ask across exchanges and check for cross-cex circular arb."""
        results = []
        if not (self.enable_bybit_arb and self.enable_mexc_arb):
            return []

        # 1. AMI Direction: Find best place to Sell AMI and Buy APT
        # Sell AMI: max binary profit
        ami_bids = []
        if bybit and not bybit.is_stale(5.0): 
            ami_bids.append(("Bybit", bybit.bid, self.bybit_fee))
        if mexc and not mexc.is_stale(5.0): 
            ami_bids.append(("MEXC", mexc.bid, self.mexc_fee))
        
        # Buy APT: min binary cost
        apt_asks = []
        if bybit_apt and not bybit_apt.is_stale(5.0): 
            apt_asks.append(("Bybit", bybit_apt.ask, self.bybit_fee))
        if mexc_apt and not mexc_apt.is_stale(5.0): 
            apt_asks.append(("MEXC", mexc_apt.ask, self.mexc_fee))

        if ami_bids and apt_asks:
            best_ami_bid_ex, best_ami_bid, bid_fee = max(ami_bids, key=lambda x: x[1] * (1 - x[2]))
            best_apt_ask_ex, best_apt_ask, ask_fee = min(apt_asks, key=lambda x: x[1] / (1 - x[2]))

            r_ami = self.cellana_reserves_ami
            r_apt = self.cellana_reserves_apt
            if r_ami and r_apt:
                ami_mid = float(best_ami_bid)
                # Optimization: Find optimal size
                opt_size = self._find_optimal_size(
                    "CROSS_CEX_CIRCULAR_AMI",
                    ami_mid=ami_mid, r_apt=r_apt, r_ami=r_ami,
                    ami_bid=best_ami_bid, ami_fee=bid_fee,
                    apt_ask=best_apt_ask, apt_fee=ask_fee
                )
                profit = self._eval_profit(
                    opt_size, "CROSS_CEX_CIRCULAR_AMI",
                    ami_mid=ami_mid, r_apt=r_apt, r_ami=r_ami,
                    ami_bid=best_ami_bid, ami_fee=bid_fee,
                    apt_ask=best_apt_ask, apt_fee=ask_fee
                )
                if profit > self.min_profit:
                    direction = "CROSS_CEX_CIRCULAR_AMI"
                    log_msg = (
                        f"💎 [Cross-CEX Circular AMI] {direction} | Profit: ${profit:.4f} | "
                        f"Sell AMI on {best_ami_bid_ex} -> Buy APT on {best_apt_ask_ex} | Qty: {opt_size / ami_mid:.2f}"
                    )

                    ami_qty = opt_size / ami_mid
                    usdt_out = ami_qty * best_ami_bid * (1.0 - bid_fee)
                    apt_qty = usdt_out / best_apt_ask
                    # Re-calculate ami_end to log correct DEX price
                    ami_end = self._amm_out(apt_qty, int(r_apt), int(r_ami), self.cellana_fee)
                    
                    legs = [
                        TradeLeg(best_ami_bid_ex.lower(), self.cex_symbol, LegSide.SELL, ami_qty, best_ami_bid, tag=f"sell_ami_{best_ami_bid_ex}"),
                        TradeLeg(best_apt_ask_ex.lower(), self.apt_cex_symbol, LegSide.BUY, apt_qty, best_apt_ask, tag=f"buy_apt_{best_apt_ask_ex}"),
                        TradeLeg("cellana", "AMI/APT", LegSide.BUY, apt_qty, float(ami_end / apt_qty) if apt_qty > 0 else 0.0, is_dex=True, dex_direction="apt_to_ami", tag="dex_apt_to_ami")
                    ]
                    results.append(Opportunity(direction, profit, legs, float(best_ami_bid), float(best_apt_ask), log_msg))

        # 2. APT Direction: Find best place to Sell APT and Buy AMI
        # Sell APT: max binary profit
        apt_bids = []
        if bybit_apt and not bybit_apt.is_stale(5.0): 
            apt_bids.append(("Bybit", bybit_apt.bid, self.bybit_fee))
        if mexc_apt and not mexc_apt.is_stale(5.0): 
            apt_bids.append(("MEXC", mexc_apt.bid, self.mexc_fee))
        
        # Buy AMI: min binary cost
        ami_asks = []
        if bybit and not bybit.is_stale(5.0): 
            ami_asks.append(("Bybit", bybit.ask, self.bybit_fee))
        if mexc and not mexc.is_stale(5.0): 
            ami_asks.append(("MEXC", mexc.ask, self.mexc_fee))

        if apt_bids and ami_asks:
            best_apt_bid_ex, best_apt_bid, apt_bid_fee = max(apt_bids, key=lambda x: x[1] * (1 - x[2]))
            best_ami_ask_ex, best_ami_ask, ami_ask_fee = min(ami_asks, key=lambda x: x[1] / (1 - x[2]))

            r_ami = self.cellana_reserves_ami
            r_apt = self.cellana_reserves_apt
            if r_ami and r_apt:
                apt_mid = float(best_apt_bid)
                # FAST PATH: Check with baseline size (100 USDT) first
                # Optimization: Don't call _find_optimal_size if baseline isn't profitable
                baseline_profit_apt = self._eval_profit(100.0, "CROSS_CEX_CIRCULAR_APT", 
                                                      apt_mid=apt_mid, r_apt=r_apt, r_ami=r_ami,
                                                      apt_bid=best_apt_bid, apt_fee=apt_bid_fee,
                                                      ami_ask=best_ami_ask, ami_fee=ami_ask_fee)
                
                if baseline_profit_apt <= 0:
                    profit_apt = -1
                else:
                    # Optimization: Find optimal size
                    opt_size_apt = self._find_optimal_size(
                        "CROSS_CEX_CIRCULAR_APT",
                        apt_mid=apt_mid, r_apt=r_apt, r_ami=r_ami,
                        apt_bid=best_apt_bid, apt_fee=apt_bid_fee,
                        ami_ask=best_ami_ask, ami_fee=ami_ask_fee
                    )
                    profit_apt = self._eval_profit(
                        opt_size_apt, "CROSS_CEX_CIRCULAR_APT",
                        apt_mid=apt_mid, r_apt=r_apt, r_ami=r_ami,
                        apt_bid=best_apt_bid, apt_fee=apt_bid_fee,
                        ami_ask=best_ami_ask, ami_fee=ami_ask_fee
                    )
                if profit_apt > self.min_profit:
                    direction = "CROSS_CEX_CIRCULAR_APT"
                    log_msg = (
                        f"💎 [Cross-CEX Circular APT] {direction} | Profit: ${profit_apt:.4f} | "
                        f"Sell APT on {best_apt_bid_ex} -> Buy AMI on {best_ami_ask_ex} | Qty: {opt_size_apt / apt_mid:.2f}"
                    )

                    apt_qty = opt_size_apt / apt_mid
                    usdt_out = apt_qty * best_apt_bid * (1.0 - apt_bid_fee)
                    ami_qty = usdt_out / best_ami_ask
                    # Re-calculate apt_end to log correct DEX price
                    apt_end = self._amm_out(ami_qty, int(r_ami), int(r_apt), self.cellana_fee)

                    legs = [
                        TradeLeg(best_apt_bid_ex.lower(), self.apt_cex_symbol, LegSide.SELL, apt_qty, best_apt_bid, tag=f"sell_apt_{best_apt_bid_ex}"),
                        TradeLeg(best_ami_ask_ex.lower(), self.cex_symbol, LegSide.BUY, ami_qty, best_ami_ask, tag=f"buy_ami_{best_ami_ask_ex}"),
                        TradeLeg("cellana", "AMI/APT", LegSide.BUY, ami_qty, float(apt_end / ami_qty) if ami_qty > 0 else 0.0, is_dex=True, dex_direction="ami_to_apt", tag="dex_ami_to_apt")
                    ]
                    results.append(Opportunity(direction, profit_apt, legs, float(best_apt_bid), float(best_ami_ask), log_msg))
        
        return results
