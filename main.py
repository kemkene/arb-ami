import asyncio
import signal
from typing import Tuple

from config.settings import settings
from core.arbitrage_engine import ArbitrageEngine
from core.price_collector import PriceCollector
from core.trade_executor import TradeExecutor
from exchanges.bybit import BybitWS
from exchanges.mexc import MexcWS
from exchanges.panora_poller import PanoraPoller
from exchanges.panora_executor import PanoraExecutor
from utils.logger import get_logger

logger = get_logger()


def validate_accounts() -> Tuple[bool, bool, bool]:
    """Check which exchange accounts are configured and log results.

    Returns (enable_panora, enable_bybit, enable_mexc).
    An exchange is "enabled" only when its credentials are present and
    parseable — otherwise the arb engine skips that direction entirely.
    """
    logger.info("━━━━━━━━━━━━━━━━━━━━  ACCOUNT VALIDATION  ━━━━━━━━━━━━━━━━━━━━")

    # ── Aptos / Panora ────────────────────────────────────────────────
    enable_panora = False
    if not settings.aptos_private_key:
        logger.warning("[Panora/Aptos] ✗  APTOS_PRIVATE_KEY not set → DEX arb DISABLED")
    else:
        try:
            from aptos_sdk.account import Account
            acct = Account.load_key(settings.aptos_private_key)
            addr = str(acct.address())
            enable_panora = True
            logger.success(
                f"[Panora/Aptos] ✓  wallet loaded → {addr[:20]}…"
                f"  (arb ENABLED)"
            )
        except Exception as e:
            logger.error(
                f"[Panora/Aptos] ✗  APTOS_PRIVATE_KEY invalid ({e}) "
                f"→ DEX arb DISABLED"
            )

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
    if enable_panora:
        enabled.append("Panora")
    if enable_bybit:
        enabled.append("Bybit")
    if enable_mexc:
        enabled.append("MEXC")

    if enabled:
        # Warn about combinations that can't form a full arb leg
        if enable_panora and not (enable_bybit or enable_mexc):
            logger.warning(
                "[Arb] Panora enabled but no CEX credentials → "
                "DEX-CEX arb requires at least one of Bybit/MEXC"
            )
        if (enable_bybit or enable_mexc) and not enable_panora:
            logger.warning(
                "[Arb] CEX(s) enabled but no Aptos wallet → "
                "only CEX-CEX arb (Bybit↔MEXC) will run"
            )
        logger.info(f"[Arb] Active exchanges: {', '.join(enabled)}")
    else:
        logger.error(
            "[Arb] No valid accounts found — bot will monitor prices only "
            "(DRY_RUN mode forced, no trades)"
        )

    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    return enable_panora, enable_bybit, enable_mexc


async def main() -> None:
    # Validate accounts before anything else
    enable_panora, enable_bybit, enable_mexc = validate_accounts()

    collector = PriceCollector()

    # --- Exchange connectors ---
    bybit = BybitWS(collector)
    mexc = MexcWS(collector)
    panora = PanoraPoller(
        collector,
        from_amount=1.0,
        from_token_address=settings.ami_token_address,
        to_token_address=settings.usdt_token_address,
    )

    # --- Trade execution ---
    panora_executor = PanoraExecutor(panora.client)
    trade_executor = TradeExecutor(panora_executor=panora_executor)

    arb = ArbitrageEngine(
        collector,
        panora_client=panora.client,
        trade_executor=trade_executor,
        enable_panora_arb=enable_panora,
        enable_bybit_arb=enable_bybit,
        enable_mexc_arb=enable_mexc,
    )

    # --- Graceful shutdown ---
    shutdown_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("Shutdown signal received — stopping…")
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # --- Launch all tasks ---
    tasks = [
        asyncio.create_task(bybit.connect(), name="bybit"),
        asyncio.create_task(mexc.connect(), name="mexc"),
        asyncio.create_task(panora.poll(), name="panora"),
        asyncio.create_task(arb.run(), name="arb_engine"),
    ]

    logger.info(
        f"Arb bot started | symbol={settings.cex_symbol} "
        f"bybit_fee={settings.bybit_fee*100:.2f}% mexc_fee={settings.mexc_fee*100:.2f}% "
        f"panora_fee={settings.panora_fee*100:.2f}%"
    )

    # Wait until shutdown is requested
    await shutdown_event.wait()

    # Cancel all tasks gracefully
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    
    # Close Panora sessions
    await panora.close()
    await panora_executor.close()

    logger.info("Arb bot shut down cleanly.")


if __name__ == "__main__":
    asyncio.run(main())
