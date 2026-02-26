import sys

from loguru import logger

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

# Rotating file log (optional — remove if not needed)
logger.add(
    "logs/arb_bot_{time:YYYY-MM-DD}.log",
    rotation="50 MB",
    retention="7 days",
    compression="gz",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | {name}:{function}:{line} | {message}",
    level="DEBUG",
)


def get_logger():
    return logger
