import json
import os
import sys
import time
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
# Each line is a JSON object written when the engine detects a trade signal.
# Use this file to evaluate opportunity quality offline.
_SIGNAL_LOG_PATH = "logs/signals.jsonl"


def log_signal(payload: Dict[str, Any]) -> None:
    """Append one JSON line to logs/signals.jsonl.

    Fields always present:
      ts          — Unix timestamp (float)
      type        — signal type string
      dry_run     — bool
      profit_usdt — estimated net profit (float)
    """
    payload.setdefault("ts", time.time())
    try:
        with open(_SIGNAL_LOG_PATH, "a") as f:
            f.write(json.dumps(payload, default=str) + "\n")
    except Exception as e:
        logger.warning(f"log_signal write error: {e}")


def get_logger():
    return logger
