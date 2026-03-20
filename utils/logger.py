import json
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict

from loguru import logger

os.makedirs("logs", exist_ok=True)

# Remove default handler and configure once
logger.remove()

# Console output with color
logger.add(
    sys.stdout,
    format=(
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level:<8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
        "{message}"
    ),
    level="INFO",
)

# Rotating file log — all levels including DEBUG price ticks
logger.add(
    "logs/arb_bot_{time:YYYY-MM-DD}.log",
    rotation="50 MB",
    retention="7 days",
    compression="gz",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {name}:{function}:{line} | {message}",
    level="DEBUG",
)

# ── Structured signal log (NDJSON) ─────────────────────────────────────────
_SIGNAL_LOG_PATH = "logs/signals.jsonl"
_PRICE_LOG_PATH = "logs/prices.jsonl"
_ARBITRAGE_LOG_PATH = "logs/arbitrage_opportunities.jsonl"


def log_signal(payload: Dict[str, Any]) -> None:
    now = time.time()
    payload.setdefault("ts", now)
    payload.setdefault("detected_at", datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
    try:
        with open(_SIGNAL_LOG_PATH, "a") as f:
            f.write(json.dumps(payload, default=str) + "\n")
    except Exception as e:
        logger.warning(f"log_signal write error: {e}")


def log_price_update(payload: Dict[str, Any]) -> None:
    payload.setdefault("ts", time.time())
    try:
        with open(_PRICE_LOG_PATH, "a") as f:
            f.write(json.dumps(payload, default=str) + "\n")
    except Exception as e:
        logger.warning(f"log_price_update write error: {e}")


def log_arbitrage_opportunity(payload: Dict[str, Any]) -> None:
    """Append one JSON line to logs/arbitrage_opportunities.jsonl."""
    now = time.time()
    payload.setdefault("ts", now)
    payload.setdefault("detected_at", datetime.fromtimestamp(now).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])

    trade_usdt = float(payload.get("trade_size_usdt", payload.get("trade_amount_usdt", 0.0)) or 0.0)
    apt_usdt_rate = float(payload.get("apt_usdt_rate", 0.0))
    dex_rate = float(payload.get("dex_price_ami_apt", 0.0))
    cex_fee = float(payload.get("cex_fee", 0.001) or 0.001)
    apt_cex_fee = float(payload.get("apt_cex_fee", cex_fee) or cex_fee)
    dex_fee = float(payload.get("dex_fee", 0.001) or 0.001)
    direction = payload.get("direction")

    reserve_ami = payload.get("pool_reserve_ami")
    reserve_apt = payload.get("pool_reserve_apt")
    reserve_ami = float(reserve_ami) if reserve_ami is not None else None
    reserve_apt = float(reserve_apt) if reserve_apt is not None else None

    # CRITICAL FIX: Do NOT reset trade_steps if already present in payload (from engine)
    trade_steps = payload.get("trade_steps", [])
    profit_token = payload.get("profit_token", "")
    profit_amount = float(payload.get("profit_amount", 0.0))

    # Fallback to legacy calc if trade_steps is missing and it's a known legacy direction
    if not trade_steps:
        if direction == "DEX_TO_CEX" and trade_usdt > 0:
            cex_bid = float(payload.get("sell_price", 0.0))
            cex_apt_ask = float(payload.get("apt_ask", payload.get("apt_usdt_rate", 0.0)))
            if cex_bid > 0 and apt_usdt_rate > 0 and cex_apt_ask > 0:
                usdt_spent = trade_usdt
                apt_amount_initial = trade_usdt / (cex_apt_ask * (1.0 + apt_cex_fee))
                if reserve_ami and reserve_apt and reserve_ami > 0 and reserve_apt > 0:
                    dx_eff = apt_amount_initial * (1.0 - dex_fee)
                    amt_ami = reserve_ami * dx_eff / (reserve_apt + dx_eff)
                elif dex_rate > 0:
                    amt_ami = apt_amount_initial * dex_rate * (1.0 - dex_fee)
                else: amt_ami = 0.0
                usdt_received = amt_ami * cex_bid * (1.0 - cex_fee)
                profit_amount = usdt_received - usdt_spent
                profit_token = "USDT"
                trade_steps = [
                    {"step": 1, "exchange": payload.get("sell_exchange", "cex"), "action": "BUY", "input_token": "USDT", "input_amount": usdt_spent, "output_token": "APT", "output_amount": apt_amount_initial, "fee_pct": apt_cex_fee * 100},
                    {"step": 2, "exchange": "cellana_dex", "action": "SWAP", "input_token": "APT", "input_amount": apt_amount_initial, "output_token": "AMI", "output_amount": amt_ami, "fee_pct": dex_fee * 100},
                    {"step": 3, "exchange": payload.get("sell_exchange", "cex"), "action": "SELL", "input_token": "AMI", "input_amount": amt_ami, "output_token": "USDT", "output_amount": usdt_received, "fee_pct": cex_fee * 100},
                    {"step": 4, "action": "SUMMARY", "initial_token": "USDT", "initial_amount": usdt_spent, "final_amount": usdt_received, "profit_token": profit_token, "profit_amount": profit_amount}
                ]

        elif direction == "AMI_CYCLE" and trade_usdt > 0:
            cex_ask = float(payload.get("cex_ask", 0.0))
            apt_bid = float(payload.get("apt_bid", 0.0))
            if cex_ask > 0 and apt_bid > 0:
                ami_initial = trade_usdt / (cex_ask * (1.0 + cex_fee))
                if reserve_ami and reserve_apt and reserve_ami > 0 and reserve_apt > 0:
                    dx_eff = ami_initial * (1.0 - dex_fee)
                    apt_from_swap = reserve_apt * dx_eff / (reserve_ami + dx_eff)
                elif dex_rate > 0:
                    apt_from_swap = ami_initial * dex_rate * (1.0 - dex_fee)
                else: apt_from_swap = 0.0
                usdt_from_apt = apt_from_swap * apt_bid * (1.0 - apt_cex_fee)
                profit_amount = usdt_from_apt - trade_usdt
                profit_token = "USDT"
                trade_steps = [
                    {"step": 1, "exchange": payload.get("exchange", "cex"), "action": "BUY", "input_token": "USDT", "input_amount": trade_usdt, "output_token": "AMI", "output_amount": ami_initial, "fee_pct": cex_fee * 100},
                    {"step": 2, "exchange": "cellana_dex", "action": "SWAP", "input_token": "AMI", "input_amount": ami_initial, "output_token": "APT", "output_amount": apt_from_swap, "fee_pct": dex_fee * 100},
                    {"step": 3, "exchange": payload.get("exchange", "cex"), "action": "SELL", "input_token": "APT", "input_amount": apt_from_swap, "output_token": "USDT", "output_amount": usdt_from_apt, "fee_pct": apt_cex_fee * 100},
                    {"step": 4, "action": "SUMMARY", "initial_token": "USDT", "initial_amount": trade_usdt, "final_amount": usdt_from_apt, "profit_token": profit_token, "profit_amount": profit_amount}
                ]

    payload["profit_token"] = profit_token
    payload["profit_amount"] = profit_amount
    payload["trade_steps"] = trade_steps
    
    try:
        with open(_ARBITRAGE_LOG_PATH, "a") as f:
            f.write(json.dumps(payload, default=str) + "\n")
    except Exception as e:
        logger.warning(f"log_arbitrage_opportunity write error: {e}")

def get_logger():
    return logger
