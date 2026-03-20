import asyncio
import signal
from typing import Tuple

from config.settings import settings
from core.arbitrage_engine import ArbitrageEngine
from core.cellana_swap_listener import CellanaSwapListener
from core.hyperion_swap_listener import HyperionSwapListener
from core.price_collector import PriceCollector
from core.trade_executor import TradeExecutor
from core.gas_monitor import GasMonitor
from core.balance_manager import BalanceManager
from exchanges.bybit import BybitWS
from exchanges.mexc import MexcWS
from utils.logger import get_logger
from utils.telegram_notifier import notifier as tg_notifier

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
    gas_monitor = GasMonitor()

    # --- Exchange connectors (subscribe to both AMI/USDT and APT/USDT) ---
    cex_symbols = [settings.cex_symbol, settings.apt_cex_symbol]
    bybit = BybitWS(collector, symbols=cex_symbols) if enable_bybit else None
    mexc  = MexcWS(collector,  symbols=cex_symbols) if enable_mexc else None

    # --- Trade execution ---
    trade_executor  = TradeExecutor()
    balance_manager = BalanceManager(
        bybit_trader=trade_executor.bybit, 
        mexc_trader=trade_executor.mexc,
        price_collector=collector
    )
    trade_executor.balance_manager = balance_manager
    
    # Save initial balance snapshot for profit analysis
    if trade_executor.balance_manager:
        await trade_executor.balance_manager.save_initial_snapshot()
        bm = trade_executor.balance_manager
        
        initial_equity = bm.initial_total_equity
        total_usdt = sum(bm.get_free(exch, "USDT") for exch in bm._cache)
        total_apt = sum(bm.get_free(exch, "APT") for exch in bm._cache)
        total_ami = sum(bm.get_free(exch, "AMI") for exch in bm._cache)
        
        logger.info(f"💰 Initial Total Equity: {initial_equity:.2f} USDT")
        if tg_notifier.enabled:
            detail_msg = (
                f"🚀 <b>Bot Arbitrage đã khởi động!</b>\n"
                f"💰 <b>Tổng vốn (Equity):</b> <code>{initial_equity:.2f}</code> USDT\n"
                f"💵 USDT: <code>{total_usdt:.2f}</code>\n"
                f"💎 APT: <code>{total_apt:.4f}</code>\n"
                f"🌕 AMI: <code>{total_ami:.0f}</code>"
            )
            await tg_notifier.send_message(detail_msg)

    arb = ArbitrageEngine(
        collector,
        gas_monitor=gas_monitor,
        trade_executor=trade_executor,
        enable_bybit_arb=enable_bybit,
        enable_mexc_arb=enable_mexc,
    )

    # --- Listeners ---
    loop = asyncio.get_event_loop()
    
    cellana_listener = None
    if settings.cellana_swap_listener_enabled:
        cellana_listener = CellanaSwapListener(on_swap_event=arb.update_cellana_state, loop=loop)
        arb.cellana_listener = cellana_listener
        logger.info("[Cellana] swap listener enabled (callback -> ArbitrageEngine)")
    else:
        logger.info("[Cellana] swap listener disabled")

    hyperion_listener = None
    if settings.hyperion_swap_listener_enabled:
        hyperion_listener = HyperionSwapListener(on_swap_event=arb._on_hyperion_update, loop=loop)
        arb.hyperion_listener = hyperion_listener
        logger.info("[Hyperion] swap listener enabled (callback -> ArbitrageEngine)")
    else:
        logger.info("[Hyperion] swap listener disabled")

    # --- Graceful shutdown ---
    shutdown_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("Shutdown signal received — stopping…")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # --- Launch all tasks ---
    tasks = []
    if bybit:
        tasks.append(asyncio.create_task(bybit.connect(), name="bybit"))
    if mexc:
        tasks.append(asyncio.create_task(mexc.connect(), name="mexc"))
    if enable_bybit or enable_mexc:
        tasks.append(asyncio.create_task(gas_monitor.start(), name="gas_monitor"))
        tasks.append(asyncio.create_task(arb.run(), name="arb_engine"))
    else:
        logger.warning("[Arb] All exchanges disabled — arb engine not started")
    if cellana_listener:
        tasks.append(asyncio.create_task(cellana_listener.run(), name="cellana_swaps"))
    if hyperion_listener:
        tasks.append(asyncio.create_task(hyperion_listener.run(), name="hyperion_swaps"))

    # --- Balance auto-refresh loop (optional but recommended for /status freshness) ---
    tasks.append(asyncio.create_task(balance_manager.run_refresh_loop(), name="balance_refresher"))

    # --- Telegram command listener (/stop, /status) ---
    if tg_notifier.enabled:
        context = {
            "trade_executor": trade_executor,
            "bybit": trade_executor.bybit,
            "mexc": trade_executor.mexc,
            "cellana": trade_executor.cellana_swap,
            "hyperion": trade_executor.hyperion_swap
        }
        tasks.append(asyncio.create_task(tg_notifier.run_listener(shutdown_event, context), name="telegram_listener"))

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
