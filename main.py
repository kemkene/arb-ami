import asyncio
import signal
from typing import Tuple

from config.settings import settings
from core.arbitrage_engine import ArbitrageEngine
from core.cellana_swap_listener import CellanaSwapListener
from core.price_collector import PriceCollector
from core.trade_executor import TradeExecutor
from exchanges.bybit import BybitWS
from exchanges.mexc import MexcWS
from utils.logger import get_logger

logger = get_logger()


def validate_accounts() -> Tuple[bool, bool]:
    """Check which exchange accounts are configured and log results.

    Returns (enable_bybit, enable_mexc).
    An exchange is "enabled" only when its credentials are present and
    parseable — otherwise the arb engine skips that direction entirely.
    """
    logger.info("━━━━━━━━━━━━━━━━━━━━  ACCOUNT VALIDATION  ━━━━━━━━━━━━━━━━━━━━")

    # ── Bybit ─────────────────────────────────────────────────────────
    enable_bybit = False
    if not settings.bybit_api_key or not settings.bybit_api_secret:
        logger.warning(
            "[Bybit]        ✗  BYBIT_API_KEY / BYBIT_API_SECRET not set "
            "→ Bybit arb DISABLED"
        )
    else:
        enable_bybit = True
        masked = settings.bybit_api_key[:6] + "*" * (len(settings.bybit_api_key) - 6)
        logger.success(f"[Bybit]        ✓  api_key={masked}  (arb ENABLED)")

    # ── MEXC ──────────────────────────────────────────────────────────
    enable_mexc = False
    if not settings.mexc_api_key or not settings.mexc_api_secret:
        logger.warning(
            "[MEXC]         ✗  MEXC_API_KEY / MEXC_API_SECRET not set "
            "→ MEXC arb DISABLED"
        )
    else:
        enable_mexc = True
        masked = settings.mexc_api_key[:6] + "*" * (len(settings.mexc_api_key) - 6)
        logger.success(f"[MEXC]         ✓  api_key={masked}  (arb ENABLED)")

    # ── Summary ───────────────────────────────────────────────────────
    enabled = []
    if enable_bybit:
        enabled.append("Bybit")
    if enable_mexc:
        enabled.append("MEXC")

    if enabled:
        logger.info(f"[Arb] Active exchanges: {', '.join(enabled)}")
    else:
        logger.error(
            "[Arb] No valid accounts found — bot will monitor prices only "
            "(DRY_RUN mode forced, no trades)"
        )

    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    return enable_bybit, enable_mexc


async def main() -> None:
    # Validate accounts before anything else
    enable_bybit, enable_mexc = validate_accounts()

    if not enable_bybit:
        logger.info("[Bybit] feed disabled (missing credentials)")
    if not enable_mexc:
        logger.info("[MEXC] feed disabled (missing credentials)")

    collector = PriceCollector()

    # --- Exchange connectors (subscribe to both AMI/USDT and APT/USDT) ---
    cex_symbols = [settings.cex_symbol, settings.apt_cex_symbol]
    bybit = BybitWS(collector, symbols=cex_symbols) if enable_bybit else None
    mexc  = MexcWS(collector,  symbols=cex_symbols) if enable_mexc else None

    # --- Trade execution ---
    trade_executor  = TradeExecutor()

    arb = ArbitrageEngine(
        collector,
        trade_executor=trade_executor,
        enable_bybit_arb=enable_bybit,
        enable_mexc_arb=enable_mexc,
    )

    cellana_listener = None
    if settings.cellana_swap_listener_enabled:
        cellana_listener = CellanaSwapListener()
        logger.info("[Cellana] swap listener enabled")
    else:
        logger.info("[Cellana] swap listener disabled")

    # --- Graceful shutdown ---
    shutdown_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("Shutdown signal received — stopping…")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # --- Launch all tasks ---
    tasks = []
    if bybit:
        tasks.append(asyncio.create_task(bybit.connect(), name="bybit"))
    if mexc:
        tasks.append(asyncio.create_task(mexc.connect(), name="mexc"))
    if enable_bybit or enable_mexc:
        tasks.append(asyncio.create_task(arb.run(), name="arb_engine"))
    else:
        logger.warning("[Arb] All exchanges disabled — arb engine not started")
    if cellana_listener:
        tasks.append(asyncio.create_task(cellana_listener.run(), name="cellana_swaps"))

    logger.info(
        f"Arb bot started | symbols={cex_symbols} "
        f"bybit_fee={settings.bybit_fee*100:.2f}% mexc_fee={settings.mexc_fee*100:.2f}%"
    )

    # Wait until shutdown is requested
    await shutdown_event.wait()

    # Cancel all tasks gracefully
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("Arb bot shut down cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
