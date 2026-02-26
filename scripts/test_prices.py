"""
test_prices.py — Fetch live prices for all token pairs from Bybit, MEXC, and Panora.

Checks:
  • AMI/USDT   on Bybit (WebSocket, 5s snapshot) and MEXC (REST)
  • APT/USDT   on Bybit (WebSocket, 5s snapshot) and MEXC (REST)
  • AMI→USDT   on Panora DEX (REST swap quote)
  • APT→AMI    on Panora DEX (REST swap quote)
  • AMI→APT    on Panora DEX (REST swap quote)
  • Implied APT/AMI from CEX vs actual Panora APT/AMI
  • Potential triangular arb spread before fees

Usage:
    python scripts/test_prices.py
    python scripts/test_prices.py --amount 5.0   # test with 5 APT / 5 AMI
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Optional

import aiohttp
import websockets
from dotenv import load_dotenv

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from config.settings import settings
from utils.logger import get_logger

logger = get_logger()

# ─────────────────────────────────────────────────────────────────────────────
#  Config
# ─────────────────────────────────────────────────────────────────────────────
AMI_TOKEN  = settings.ami_token_address
USDT_TOKEN = settings.usdt_token_address
APT_TOKEN  = settings.apt_token_address   # "0x1::aptos_coin::AptosCoin"

PANORA_FEE = settings.panora_fee          # 0.003
BYBIT_FEE  = settings.bybit_fee           # 0.001
MEXC_FEE   = settings.mexc_fee            # 0.001


# ─────────────────────────────────────────────────────────────────────────────
#  Panora: fetch swap quote
# ─────────────────────────────────────────────────────────────────────────────
async def panora_quote(
    session: aiohttp.ClientSession,
    from_token: str,
    to_token: str,
    from_amount: float,
) -> Optional[float]:
    """Return toTokenAmount from Panora, or None on failure."""
    try:
        params = {
            "fromTokenAddress":  from_token,
            "toTokenAddress":    to_token,
            "fromTokenAmount":   from_amount,
        }
        async with session.post(
            settings.panora_api_url,
            params=params,
            headers={"x-api-key": settings.panora_api_key},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.error(f"Panora HTTP {resp.status}: {body[:120]}")
                return None
            data = await resp.json()
            # Parse toTokenAmount (primary path then quotes array)
            if "toTokenAmount" in data:
                return float(data["toTokenAmount"])
            quotes = data.get("quotes", [])
            if quotes and "toTokenAmount" in quotes[0]:
                return float(quotes[0]["toTokenAmount"])
            logger.error(f"Panora: cannot find toTokenAmount in {list(data.keys())}")
            return None
    except Exception as e:
        logger.error(f"Panora quote error: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  MEXC: REST bookTicker
# ─────────────────────────────────────────────────────────────────────────────
async def mexc_price(session: aiohttp.ClientSession, symbol: str) -> Optional[dict]:
    """Return {bid, ask, bid_qty, ask_qty} or None."""
    try:
        async with session.get(
            settings.mexc_rest_url,
            params={"symbol": symbol},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            if resp.status != 200:
                logger.warning(f"MEXC HTTP {resp.status} for {symbol}")
                return None
            data = await resp.json()
            return {
                "bid":     float(data["bidPrice"]),
                "ask":     float(data["askPrice"]),
                "bid_qty": float(data.get("bidQty", 0)),
                "ask_qty": float(data.get("askQty", 0)),
            }
    except Exception as e:
        logger.error(f"MEXC price error [{symbol}]: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  Bybit: short WS snapshot (collect first update for each symbol, then close)
# ─────────────────────────────────────────────────────────────────────────────
async def bybit_snapshot(symbols: list[str], timeout: float = 8.0) -> dict[str, dict]:
    """Return {symbol: {bid, ask, bid_qty, ask_qty}} for each symbol in list."""
    results: dict[str, dict] = {}
    args = [f"orderbook.1.{s}" for s in symbols]
    try:
        async with websockets.connect(
            settings.bybit_ws_url, open_timeout=10, ping_interval=None
        ) as ws:
            await ws.send(json.dumps({"op": "subscribe", "args": args}))
            deadline = time.time() + timeout
            while len(results) < len(symbols) and time.time() < deadline:
                remaining = deadline - time.time()
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                msg = json.loads(raw)
                if "data" not in msg:
                    continue
                data   = msg["data"]
                topic  = msg.get("topic", "")
                symbol = topic.split(".")[-1] if topic else ""
                bids   = data.get("b", [])
                asks   = data.get("a", [])
                if symbol and bids and asks:
                    results[symbol] = {
                        "bid":     float(bids[0][0]),
                        "ask":     float(asks[0][0]),
                        "bid_qty": float(bids[0][1]),
                        "ask_qty": float(asks[0][1]),
                    }
    except Exception as e:
        logger.error(f"Bybit WS snapshot error: {e}")
    return results


# ─────────────────────────────────────────────────────────────────────────────
#  Pretty print
# ─────────────────────────────────────────────────────────────────────────────
def _pct(val: float) -> str:
    return f"{val*100:+.4f}%"

def _fmt(label: str, val: Optional[float], unit: str = "") -> None:
    if val is None:
        print(f"  {label:<40} N/A")
    else:
        print(f"  {label:<40} {val:.8f}  {unit}")


# ─────────────────────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────────────────────
async def run(amount: float) -> None:
    print("\n" + "═" * 70)
    print(f"  PRICE CHECK  |  amount={amount}  |  {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("═" * 70)

    async with aiohttp.ClientSession() as session:
        # ── Fetch all in parallel ────────────────────────────────────────────
        bybit_task  = asyncio.create_task(bybit_snapshot([settings.cex_symbol, settings.apt_cex_symbol]))
        mexc_ami_t  = asyncio.create_task(mexc_price(session, settings.cex_symbol))
        mexc_apt_t  = asyncio.create_task(mexc_price(session, settings.apt_cex_symbol))
        pan_ami_usdt= asyncio.create_task(panora_quote(session, AMI_TOKEN,  USDT_TOKEN, amount))
        pan_apt_ami = asyncio.create_task(panora_quote(session, APT_TOKEN,  AMI_TOKEN,  amount))
        pan_ami_apt = asyncio.create_task(panora_quote(session, AMI_TOKEN,  APT_TOKEN,  amount))
        pan_apt_usdt= asyncio.create_task(panora_quote(session, APT_TOKEN,  USDT_TOKEN, amount))

        (bybit_data, mexc_ami, mexc_apt,
         p_ami_usdt, p_apt_ami, p_ami_apt, p_apt_usdt) = await asyncio.gather(
            bybit_task, mexc_ami_t, mexc_apt_t,
            pan_ami_usdt, pan_apt_ami, pan_ami_apt, pan_apt_usdt,
        )

    bybit_ami = bybit_data.get(settings.cex_symbol)
    bybit_apt = bybit_data.get(settings.apt_cex_symbol)

    # ── CEX prices ───────────────────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print(f"  CEX  —  AMI/USDT ({settings.cex_symbol})")
    print(f"{'─'*70}")
    if bybit_ami:
        print(f"  Bybit   bid={bybit_ami['bid']:.8f}  ask={bybit_ami['ask']:.8f}"
              f"  bid_qty={bybit_ami['bid_qty']:.2f}  ask_qty={bybit_ami['ask_qty']:.2f}")
    else:
        print("  Bybit   N/A")
    if mexc_ami:
        print(f"  MEXC    bid={mexc_ami['bid']:.8f}  ask={mexc_ami['ask']:.8f}"
              f"  bid_qty={mexc_ami['bid_qty']:.2f}  ask_qty={mexc_ami['ask_qty']:.2f}")
    else:
        print("  MEXC    N/A")

    print(f"\n{'─'*70}")
    print(f"  CEX  —  APT/USDT ({settings.apt_cex_symbol})")
    print(f"{'─'*70}")
    if bybit_apt:
        print(f"  Bybit   bid={bybit_apt['bid']:.4f}  ask={bybit_apt['ask']:.4f}"
              f"  bid_qty={bybit_apt['bid_qty']:.4f}  ask_qty={bybit_apt['ask_qty']:.4f}")
    else:
        print("  Bybit   N/A")
    if mexc_apt:
        print(f"  MEXC    bid={mexc_apt['bid']:.4f}  ask={mexc_apt['ask']:.4f}"
              f"  bid_qty={mexc_apt['bid_qty']:.4f}  ask_qty={mexc_apt['ask_qty']:.4f}")
    else:
        print("  MEXC    N/A")

    # ── Panora DEX quotes ────────────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print(f"  Panora DEX  (send {amount} of each token)")
    print(f"{'─'*70}")
    _fmt(f"AMI→USDT  ({amount} AMI → ? USDT)",    p_ami_usdt, "USDT")
    _fmt(f"APT→AMI   ({amount} APT → ? AMI)",     p_apt_ami,  "AMI")
    _fmt(f"AMI→APT   ({amount} AMI → ? APT)",     p_ami_apt,  "APT")
    _fmt(f"APT→USDT  ({amount} APT → ? USDT)",    p_apt_usdt, "USDT")

    # ── Derived prices ───────────────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("  Derived price-per-unit (Panora)")
    print(f"{'─'*70}")

    pan_ami_usdt_price = (p_ami_usdt / amount) if p_ami_usdt else None
    pan_apt_ami_price  = (p_apt_ami  / amount) if p_apt_ami  else None
    pan_ami_apt_price  = (p_ami_apt  / amount) if p_ami_apt  else None
    pan_apt_usdt_price = (p_apt_usdt / amount) if p_apt_usdt else None

    _fmt("1 AMI → USDT  (Panora)",   pan_ami_usdt_price, "USDT/AMI")
    _fmt("1 APT → AMI   (Panora)",   pan_apt_ami_price,  "AMI/APT")
    _fmt("1 AMI → APT   (Panora)",   pan_ami_apt_price,  "APT/AMI")
    _fmt("1 APT → USDT  (Panora)",   pan_apt_usdt_price, "USDT/APT")

    # ── Implied APT/AMI from CEX ─────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("  Triangular Arb Analysis  (fees not yet deducted)")
    print(f"{'─'*70}")

    # For each CEX: implied APT/AMI = APT_USDT / AMI_USDT
    # Direction 1: Buy APT on CEX → Swap APT→AMI on Panora → Sell AMI on CEX
    # Direction 2: Buy AMI on CEX → Swap AMI→APT on Panora → Sell APT on CEX

    total_fee_3leg = BYBIT_FEE + PANORA_FEE + BYBIT_FEE  # approx

    for cex_name, cex_ami, cex_apt in [
        ("Bybit", bybit_ami, bybit_apt),
        ("MEXC",  mexc_ami,  mexc_apt),
    ]:
        if not cex_ami or not cex_apt:
            print(f"\n  [{cex_name}]  N/A (missing price data)")
            continue

        cex_fee = BYBIT_FEE if cex_name == "Bybit" else MEXC_FEE

        # implied APT/AMI from CEX perspective:
        # how many AMI per 1 APT according to CEX?
        implied_apt_to_ami = cex_apt["bid"] / cex_ami["ask"]   # sell APT, buy AMI on CEX
        implied_ami_to_apt = cex_ami["bid"] / cex_apt["ask"]   # sell AMI, buy APT on CEX

        print(f"\n  [{cex_name}] Implied 1 APT = {implied_apt_to_ami:.6f} AMI  "
              f"| Implied 1 AMI = {implied_ami_to_apt:.8f} APT")

        # Direction 1: Buy APT on CEX → Swap APT→AMI on Panora → Sell AMI on CEX
        if pan_apt_ami_price:
            spread1 = (pan_apt_ami_price - implied_apt_to_ami) / implied_apt_to_ami
            usdt_in = amount * cex_apt["ask"]
            ami_out_panora = pan_apt_ami_price * amount
            usdt_from_ami  = ami_out_panora * cex_ami["bid"]
            fee_cost = (usdt_in * cex_fee) + (usdt_in * PANORA_FEE) + (usdt_from_ami * cex_fee)
            raw_profit = usdt_from_ami - usdt_in
            net_profit = raw_profit - fee_cost
            print(f"  Dir1  Buy APT@{cex_name} → APT→AMI Panora → Sell AMI@{cex_name}")
            print(f"        APT ask={cex_apt['ask']:.4f}  Panora gives {pan_apt_ami_price:.6f} AMI/APT"
                  f"  AMI bid={cex_ami['bid']:.8f}")
            print(f"        spend={usdt_in:.4f} USDT → get {ami_out_panora:.4f} AMI"
                  f" → sell for {usdt_from_ami:.4f} USDT")
            print(f"        raw_profit={raw_profit:.4f}  fees≈{fee_cost:.4f}"
                  f"  net={net_profit:.4f} USDT  spread={_pct(spread1)}")

        # Direction 2: Buy AMI on CEX → Swap AMI→APT on Panora → Sell APT on CEX
        if pan_ami_apt_price:
            spread2 = (pan_ami_apt_price - implied_ami_to_apt) / implied_ami_to_apt
            usdt_in2    = amount * cex_ami["ask"]
            apt_out_pan = pan_ami_apt_price * amount
            usdt_from_apt = apt_out_pan * cex_apt["bid"]
            fee_cost2 = (usdt_in2 * cex_fee) + (usdt_in2 * PANORA_FEE) + (usdt_from_apt * cex_fee)
            raw_profit2 = usdt_from_apt - usdt_in2
            net_profit2 = raw_profit2 - fee_cost2
            print(f"  Dir2  Buy AMI@{cex_name} → AMI→APT Panora → Sell APT@{cex_name}")
            print(f"        AMI ask={cex_ami['ask']:.8f}  Panora gives {pan_ami_apt_price:.8f} APT/AMI"
                  f"  APT bid={cex_apt['bid']:.4f}")
            print(f"        spend={usdt_in2:.4f} USDT → get {apt_out_pan:.6f} APT"
                  f" → sell for {usdt_from_apt:.4f} USDT")
            print(f"        raw_profit={raw_profit2:.4f}  fees≈{fee_cost2:.4f}"
                  f"  net={net_profit2:.4f} USDT  spread={_pct(spread2)}")

    print(f"\n{'═'*70}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Test live prices for all trading pairs")
    parser.add_argument("--amount", type=float, default=1.0,
                        help="Amount to test swap quotes with (default: 1.0)")
    args = parser.parse_args()
    asyncio.run(run(args.amount))


if __name__ == "__main__":
    main()
