"""
Snapshot balances across all trading venues: Bybit, MEXC, and the Aptos wallet.

Usage:
    python scripts/check_balances.py

Prints a table like:
  Venue      │ Coin │   Balance   │ Min Needed
  ───────────┼──────┼─────────────┼───────────
  Bybit      │ AMI  │    5000.00  │   1000.00  ✅
  Bybit      │ APT  │      12.40  │      5.00  ✅
  Bybit      │ USDT │     200.00  │     50.00  ✅
  ...
"""
from __future__ import annotations

import asyncio
import sys
import os

# Resolve project root so `config` / `exchanges` are importable.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import settings
from exchanges.bybit_trader import BybitTrader
from exchanges.mexc_trader import MexcTrader
from exchanges.panora_executor import PanoraExecutor
from exchanges.panora import PanoraClient
from utils.logger import get_logger

logger = get_logger()


# ── minimum recommended balances ───────────────────────────────────────────
MIN_BALANCES: dict[str, float] = {
    "AMI":  1_000.0,
    "APT":  5.0,
    "USDT": 50.0,
}

COINS = ["AMI", "APT", "USDT"]

# ── helpers ─────────────────────────────────────────────────────────────────

def _fmt(val: float | None) -> str:
    if val is None:
        return "  N/A     "
    return f"{val:>12.6f}"


def _status(coin: str, bal: float | None) -> str:
    if bal is None:
        return "❓"
    minimum = MIN_BALANCES.get(coin, 0.0)
    return "✅" if bal >= minimum else "⚠️ LOW"


async def _aptos_balances(wallet_str: str) -> dict[str, float | None]:
    """Fetch AMI and APT balance from the Aptos wallet."""
    panora_client   = PanoraClient()
    executor        = PanoraExecutor(panora_client)
    balances: dict  = {}

    # APT
    apt_bal = await executor.get_token_balance(
        wallet_str,
        settings.apt_token_address,
        decimals=8,
    )
    balances["APT"] = apt_bal

    # AMI  (FA token, 8 decimals assumed — adjust via ami_decimals setting if added)
    ami_decimals = getattr(settings, "ami_decimals", 8)
    ami_bal = await executor.get_token_balance(
        wallet_str,
        settings.ami_token_address,
        decimals=ami_decimals,
    )
    balances["AMI"] = ami_bal

    # No USDT on Aptos wallet needed (we use CEX USDT)
    balances["USDT"] = None

    return balances


def _print_table(rows: list[tuple]) -> None:
    """rows: (venue, coin, balance, min_bal)"""
    col_widths = (12, 6, 14, 12, 8)
    header = (
        f"{'Venue':<{col_widths[0]}} │ "
        f"{'Coin':<{col_widths[1]}} │ "
        f"{'Balance':>{col_widths[2]}} │ "
        f"{'Min Needed':>{col_widths[3]}} │ "
        f"{'Status'}"
    )
    sep = "─" * len(header)
    print(sep)
    print(header)
    print(sep)

    for venue, coin, bal, minimum in rows:
        status = _status(coin, bal)
        bal_str = _fmt(bal)
        min_str = f"{minimum:>12.2f}" if minimum else "          —"
        print(
            f"{venue:<{col_widths[0]}} │ "
            f"{coin:<{col_widths[1]}} │ "
            f"{bal_str} │ "
            f"{min_str} │ "
            f"{status}"
        )

    print(sep)


async def main() -> None:
    bybit = BybitTrader()
    mexc  = MexcTrader()

    print("\n📊  Checking balances …\n")

    # ── Bybit ──────────────────────────────────────────────────────────────
    bybit_bals  = await bybit.get_balance(coins=COINS)

    # ── MEXC ───────────────────────────────────────────────────────────────
    mexc_bals   = await mexc.get_balance(coins=COINS)

    # ── Aptos wallet ───────────────────────────────────────────────────────
    aptos_bals: dict = {}
    if settings.aptos_private_key:
        from aptos_sdk.account import Account
        try:
            acct        = Account.load_key(settings.aptos_private_key)
            wallet_str  = str(acct.account_address)
            print(f"  Aptos wallet: {wallet_str}")
            aptos_bals  = await _aptos_balances(wallet_str)
        except Exception as e:
            logger.error(f"Failed to load Aptos key for balance check: {e}")
    else:
        logger.warning("APTOS_PRIVATE_KEY not set — skipping Aptos balance check")

    # ── Build table ────────────────────────────────────────────────────────
    rows = []
    for coin in COINS:
        rows.append(("Bybit",        coin, bybit_bals.get(coin),  MIN_BALANCES.get(coin, 0)))
    for coin in COINS:
        rows.append(("MEXC",         coin, mexc_bals.get(coin),   MIN_BALANCES.get(coin, 0)))
    for coin in ["APT", "AMI"]:
        rows.append(("Aptos wallet", coin, aptos_bals.get(coin),  MIN_BALANCES.get(coin, 0)))

    print()
    _print_table(rows)

    # ── Summary ────────────────────────────────────────────────────────────
    all_venues = {
        "Bybit":        bybit_bals,
        "MEXC":         mexc_bals,
        "Aptos wallet": {k: v for k, v in aptos_bals.items() if v is not None},
    }
    warnings = [
        f"{venue} {coin}={bal:.4f} (need ≥ {MIN_BALANCES[coin]:.2f})"
        for venue, bals in all_venues.items()
        for coin, bal in bals.items()
        if coin in MIN_BALANCES and bal is not None and bal < MIN_BALANCES[coin]
    ]
    if warnings:
        print("\n⚠️  LOW BALANCE WARNINGS:")
        for w in warnings:
            print(f"   • {w}")
    else:
        print("\n✅  All balances above minimum thresholds.")
    print()


if __name__ == "__main__":
    asyncio.run(main())
