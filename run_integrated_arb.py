#!/usr/bin/env python3
"""
Integrated DEX-CEX arbitrage monitor for AMI.

Real-time tracking of:
1. Cellana DEX prices (AMI/APT from gRPC stream)
2. CEX prices (AMIUSDT from Bybit/MEXC WebSocket)
3. Arbitrage opportunities between them
"""

import asyncio
import json
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config.settings import settings
from core.cellana_swap_listener import CellanaSwapListener
from core.dex_cex_arbitrage import DexCexArbitrage
from core.price_collector import PriceCollector, PriceData
from core.balance_manager import BalanceManager
from core.trade_executor import TradeExecutor
from exchanges.bybit import BybitWS
from exchanges.bybit_trader import BybitTrader
from exchanges.mexc import MexcWS
from exchanges.mexc_trader import MexcTrader
from utils.logger import get_logger

logger = get_logger()


class IntegratedArbMonitor:
    """Monitor DEX prices + CEX prices and detect arbitrage."""

    def __init__(self):
        self.cellana_listener = CellanaSwapListener(
            on_swap_event=self._on_cellana_event
        )
        self.price_collector = PriceCollector()
        
        # Register event-driven callback so arb checks fire on EVERY
        # CEX price update (Bybit WS push ~20ms, MEXC poll ~200ms)
        self.price_collector.set_on_update(self._on_cex_price_update)
        
        # Throttle: min interval between event-driven arb checks (seconds)
        self._min_check_interval = 0.02  # 50 checks/sec max
        self._last_check_ts = 0.0
        
        # DEX spike detection: track previous price, bypass throttle on spike
        self._prev_dex_price: float = 0.0
        self._spike_priority_until: float = 0.0  # timestamp until priority mode active
        
        # Exchange instances (will be set after creation)
        self.bybit_ws = None
        self.mexc_ws = None
        
        # Traders for balance manager & execution
        self._bybit_trader = BybitTrader()
        self._mexc_trader = MexcTrader()

        # Balance manager
        self.balance_manager = BalanceManager(
            bybit_trader=self._bybit_trader,
            mexc_trader=self._mexc_trader,
            reserve_buffer_pct=settings.balance_reserve_buffer_pct,
            refresh_ttl=settings.balance_refresh_ttl,
        )

        # Trade executor with balance checking
        self.trade_executor = TradeExecutor(
            balance_manager=self.balance_manager,
        )

        self.dex_cex_arb = DexCexArbitrage(
            cex_collector=self.price_collector,
            trade_executor=self.trade_executor,
            balance_manager=self.balance_manager,
            bybit_ws=self.bybit_ws,
            mexc_ws=self.mexc_ws,
        )

        self._enabled_exchanges = []

    def _on_cellana_event(self, payload: dict) -> None:
        """Callback when Cellana emits a price event."""
        try:
            version = payload.get("version")
            source = payload.get("source", payload.get("type", "grpc"))
            price_ami_apt = payload.get("price_ami_per_apt_spot")
            reserves_1 = payload.get("reserves_1")
            reserves_2 = payload.get("reserves_2")

            if price_ami_apt:
                reserves_ami = None
                reserves_apt = None
                if reserves_1 is not None and reserves_2 is not None:
                    reserves_ami = reserves_1 / 1e8
                    reserves_apt = reserves_2 / 1e8

                # Detect significant reserve change (log for visibility)
                old_ratio = 0.0
                if (
                    self.dex_cex_arb.cellana_reserve_ami
                    and self.dex_cex_arb.cellana_reserve_apt
                    and self.dex_cex_arb.cellana_reserve_apt > 0
                ):
                    old_ratio = (
                        self.dex_cex_arb.cellana_reserve_ami
                        / self.dex_cex_arb.cellana_reserve_apt
                    )
                new_ratio = reserves_ami / reserves_apt if (reserves_ami and reserves_apt and reserves_apt > 0) else 0.0
                if old_ratio > 0 and new_ratio > 0:
                    ratio_delta_pct = abs(new_ratio - old_ratio) / old_ratio * 100
                    if ratio_delta_pct >= 0.1:  # Log if ratio changed by ≥ 0.1%
                        logger.info(
                            f"📊 [RESERVE Δ] src={source} "
                            f"ratio {old_ratio:.2f} → {new_ratio:.2f} "
                            f"({ratio_delta_pct:+.2f}%) "
                            f"AMI={reserves_ami:,.0f} APT={reserves_apt:,.0f}"
                        )

                self.dex_cex_arb.update_cellana_price(
                    price_ami_apt,
                    timestamp=payload.get("timestamp", None),
                    reserves_ami=reserves_ami,
                    reserves_apt=reserves_apt,
                )

                # ── DEX spike detection ──
                # Compare new price with previous. If the pool moved
                # significantly (e.g., large swap), enter priority mode
                # so the next few CEX updates bypass the 20ms throttle.
                import time as _time
                if self._prev_dex_price > 0:
                    delta_pct = abs(price_ami_apt - self._prev_dex_price) / self._prev_dex_price * 100
                    if delta_pct >= settings.dex_spike_threshold_pct:
                        direction = "↑" if price_ami_apt > self._prev_dex_price else "↓"
                        logger.warning(
                            f"🚨 DEX SPIKE {direction} {delta_pct:.2f}% | "
                            f"v={version} | "
                            f"old={self._prev_dex_price:.8f} new={price_ami_apt:.8f}"
                        )
                        self._spike_priority_until = _time.time() + settings.dex_spike_priority_duration
                self._prev_dex_price = price_ami_apt

                # For logging purposes
                logger.debug(
                    f"📡 Cellana update v={version} | "
                    f"AMI/APT={price_ami_apt:.8f}"
                )

                # Check arbitrage for all enabled exchanges on every DEX update
                self._check_arbitrage_all_enabled_exchanges()

        except Exception as e:
            logger.error(f"Error in cellana callback: {e}")

    def _on_cex_price_update(self, exchange: str, symbol: str) -> None:
        """Event-driven callback: fires on EVERY CEX price update.

        This replaces the 100ms polling loop as the primary trigger for
        arb checks.  Bybit WS pushes every ~20ms, MEXC polls every
        ~200ms — both now trigger an immediate arb check.

        Throttled to max 50 checks/sec to avoid CPU spikes.
        During DEX spike priority window, throttle is bypassed.
        """
        if not self._enabled_exchanges:
            return
        import time as _time
        now = _time.time()
        # Bypass throttle during spike priority window
        if now < self._spike_priority_until:
            pass  # no throttle
        elif now - self._last_check_ts < self._min_check_interval:
            return  # throttled
        self._last_check_ts = now
        try:
            self._check_arbitrage_all_enabled_exchanges()
        except Exception as e:
            logger.error(f"Error in CEX price callback: {e}")

    async def _start_bybit_feed(self) -> None:
        """Start Bybit price feed with automatic reconnection."""
        backoff = 1.0
        while True:
            try:
                self.bybit_ws = BybitWS(
                    collector=self.price_collector,
                    symbols=[settings.cex_symbol, settings.apt_cex_symbol],
                )
                # Update arbitrage engine with exchange instance
                self.dex_cex_arb.bybit_ws = self.bybit_ws
                logger.info(f"🟡 Starting Bybit feed for {settings.cex_symbol}")
                await self.bybit_ws.connect()
                # connect() returned → WS closed normally, reconnect
                logger.warning("Bybit feed ended, reconnecting...")
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Bybit feed error: {e}. Restarting in {backoff:.0f}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    async def _start_mexc_feed(self) -> None:
        """Start MEXC price feed with automatic reconnection."""
        backoff = 1.0
        while True:
            try:
                self.mexc_ws = MexcWS(
                    collector=self.price_collector,
                    symbols=[settings.cex_symbol, settings.apt_cex_symbol],
                )
                # Update arbitrage engine with exchange instance
                self.dex_cex_arb.mexc_ws = self.mexc_ws
                logger.info(
                    f"🔵 Starting MEXC feed for {settings.cex_symbol} and {settings.apt_cex_symbol}"
                )
                await self.mexc_ws.connect()
                # connect() returned → poll loop ended, reconnect
                logger.warning("MEXC feed ended, reconnecting...")
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"MEXC feed error: {e}. Restarting in {backoff:.0f}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    def _check_arbitrage_all_enabled_exchanges(self) -> None:
        """Check DEX↔CEX arbitrage independently for each enabled exchange.

        Two-pass strategy:
        1. Check 3-hop closed cycles at full ``max_trade_usdt``
           with optimal trade-size search (if enabled)
        The deduplicator prevents double-logging the same opportunity.
        """
        for exchange in self._enabled_exchanges:
            cex_data = self.price_collector.get_exchange(settings.cex_symbol, exchange)
            apt_data = self.price_collector.get_exchange(settings.apt_cex_symbol, exchange)

            if not cex_data:
                continue

            # 3-hop closed cycles only (no inventory risk)
            self.dex_cex_arb.check_arbitrage_for_exchange(
                exchange=exchange,
                cex_price_data=cex_data,
                apt_price_data=apt_data,
            )
        
        # Check CEX-to-CEX arbitrage (Bybit ↔ MEXC)
        if "bybit" in self._enabled_exchanges and "mexc" in self._enabled_exchanges:
            bybit_prices = self.price_collector.get_exchange(settings.cex_symbol, "bybit")
            mexc_prices = self.price_collector.get_exchange(settings.cex_symbol, "mexc")
            bybit_apt_prices = self.price_collector.get_exchange(settings.apt_cex_symbol, "bybit")
            mexc_apt_prices = self.price_collector.get_exchange(settings.apt_cex_symbol, "mexc")
            
            if bybit_prices and mexc_prices:
                self.dex_cex_arb.check_cex_to_cex_arbitrage(
                    bybit_price_data=bybit_prices,
                    mexc_price_data=mexc_prices,
                    bybit_apt_price_data=bybit_apt_prices,
                    mexc_apt_price_data=mexc_apt_prices,
                )

            self.dex_cex_arb.check_cross_cex_ami_cycle_arbitrage(
                bybit_ami_price_data=bybit_prices,
                mexc_ami_price_data=mexc_prices,
                bybit_apt_price_data=bybit_apt_prices,
                mexc_apt_price_data=mexc_apt_prices,
            )

            self.dex_cex_arb.check_cross_cex_dex_to_cex_arbitrage(
                bybit_ami_price_data=bybit_prices,
                mexc_ami_price_data=mexc_prices,
                bybit_apt_price_data=bybit_apt_prices,
                mexc_apt_price_data=mexc_apt_prices,
            )

            # Cross-CEX APT-start: APT → AMI(DEX) → USDT(CEX1) → APT(CEX2)
            self.dex_cex_arb.check_cross_cex_apt_start_cycle(
                bybit_ami_price_data=bybit_prices,
                mexc_ami_price_data=mexc_prices,
                bybit_apt_price_data=bybit_apt_prices,
                mexc_apt_price_data=mexc_apt_prices,
            )

            # Cross-CEX AMI-start: AMI → USDT(CEX1) → APT(CEX2) → AMI(DEX)
            self.dex_cex_arb.check_cross_cex_ami_start_cycle(
                bybit_ami_price_data=bybit_prices,
                mexc_ami_price_data=mexc_prices,
                bybit_apt_price_data=bybit_apt_prices,
                mexc_apt_price_data=mexc_apt_prices,
            )

            # Cross-CEX APT-reverse: APT → USDT(sell APT CEX1) → AMI(buy CEX2) → APT(DEX AMI→APT)
            self.dex_cex_arb.check_cross_cex_apt_reverse_cycle(
                bybit_ami_price_data=bybit_prices,
                mexc_ami_price_data=mexc_prices,
                bybit_apt_price_data=bybit_apt_prices,
                mexc_apt_price_data=mexc_apt_prices,
            )

            # Cross-CEX AMI-reverse: AMI → APT(DEX AMI→APT) → USDT(sell APT CEX1) → AMI(buy CEX2)
            self.dex_cex_arb.check_cross_cex_ami_reverse_cycle(
                bybit_ami_price_data=bybit_prices,
                mexc_ami_price_data=mexc_prices,
                bybit_apt_price_data=bybit_apt_prices,
                mexc_apt_price_data=mexc_apt_prices,
            )

    async def _monitor_prices(self) -> None:
        """Heartbeat: periodic arb check as safety net.

        Primary arb detection is now event-driven via
        ``_on_cex_price_update`` (fires on every Bybit/MEXC update).
        This loop is a 1-second heartbeat to catch anything that slipped.

        Also refreshes stale Bybit symbols via REST fallback when WS
        data hasn't updated for >10 seconds.
        """
        while True:
            try:
                # ── Stale Bybit REST fallback ──
                await self._refresh_stale_bybit_symbols()

                self._check_arbitrage_all_enabled_exchanges()
                await asyncio.sleep(1.0)  # Safety heartbeat only

            except Exception as e:
                logger.error(f"Price monitor error: {e}")
                await asyncio.sleep(1)

    async def _refresh_stale_bybit_symbols(self) -> None:
        """If any Bybit symbol data is stale (>10s with no WS update),
        fetch fresh orderbook via REST API so arb checks aren't skipped."""
        if not self.bybit_ws or "bybit" not in self._enabled_exchanges:
            return
        for symbol in self.bybit_ws.symbols:
            pd = self.price_collector.get_exchange(symbol, "bybit")
            if pd is None or pd.is_stale(max_age=10):
                refreshed = await self.bybit_ws.fetch_orderbook_rest(symbol)
                if refreshed:
                    logger.debug(
                        f"♻️  Bybit REST refresh for {symbol} "
                        f"(was stale {pd.age:.1f}s)" if pd else
                        f"♻️  Bybit REST refresh for {symbol} (no prior data)"
                    )

    async def run(self) -> None:
        """Run integrated arbitrage monitor."""
        logger.info("🚀 Starting Integrated DEX-CEX Arbitrage Monitor")
        logger.info("=" * 70)

        # Validate exchanges
        enable_bybit = bool(settings.bybit_api_key and settings.bybit_api_secret)
        enable_mexc = bool(settings.mexc_api_key and settings.mexc_api_secret)

        # ── Sync exchange clocks first (needed for auth probes) ──
        if enable_bybit:
            await self._bybit_trader.sync_server_time()
        if enable_mexc:
            await self._mexc_trader.sync_server_time()

        # ── Bybit: probe if trading is actually allowed ──
        if enable_bybit:
            if await self._bybit_trader.check_trading_enabled():
                self._enabled_exchanges.append("bybit")
                logger.success("✓ Bybit enabled (trading allowed)")
            else:
                enable_bybit = False
                logger.warning(
                    "✗ Bybit auto-disabled — account blocked or restricted "
                    "(retCode=10024). Will NOT start Bybit feeds."
                )
        else:
            logger.warning("✗ Bybit disabled (no credentials)")

        if enable_mexc:
            self._enabled_exchanges.append("mexc")
            logger.success("✓ MEXC enabled")
        else:
            logger.warning("✗ MEXC disabled (no credentials)")

        if not self._enabled_exchanges:
            logger.error("No exchanges enabled, exiting")
            return

        logger.info("=" * 70)
        logger.info("📊 Monitoring:")
        logger.info(f"  • Cellana DEX: AMI/APT pool (gRPC stream)")
        logger.info(f"  • CEX: {settings.cex_symbol} ({', '.join(self._enabled_exchanges)})")
        logger.info(f"  • Min profit threshold: {settings.min_profit_threshold}%")
        logger.info(f"  • Balance checking: ON (refresh every {settings.balance_refresh_ttl}s)")
        logger.info(f"  • Adaptive slippage: {'ON' if settings.adaptive_slippage_enabled else 'OFF'}")
        logger.info(f"  • Trading mode: {'DRY-RUN' if settings.dry_run else '🔴 LIVE'}")
        logger.info("=" * 70)

        # Create tasks
        tasks = [
            asyncio.create_task(self.cellana_listener.run()),
            asyncio.create_task(self._monitor_prices()),
            asyncio.create_task(self.balance_manager.run_refresh_loop()),
        ]

        # REST-based pool reserve polling (fresh reserves every ~2s)
        if settings.reserve_poll_enabled:
            tasks.append(
                asyncio.create_task(self.cellana_listener.run_reserve_poll_loop())
            )
            logger.info(
                f"  • REST reserve polling: ON (every {settings.reserve_poll_interval_s}s)"
            )

        if enable_bybit:
            tasks.append(asyncio.create_task(self._start_bybit_feed()))

        if enable_mexc:
            tasks.append(asyncio.create_task(self._start_mexc_feed()))

        try:
            # Run all tasks concurrently
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            logger.info("\n⏹️  Monitor stopped by user")
            for task in tasks:
                task.cancel()
        except Exception as e:
            logger.error(f"Monitor error: {e}")
            raise


async def main():
    monitor = IntegratedArbMonitor()
    await monitor.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
