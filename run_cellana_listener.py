#!/usr/bin/env python3
"""
Cellana AMI/APT Price Listener
Monitors Cellana DEX for AMI/APT pool price updates via Aptos gRPC stream
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from core.cellana_swap_listener import CellanaSwapListener
from utils.logger import logger


async def main():
    """Run the Cellana price listener"""
    logger.info("🚀 Starting Cellana AMI/APT Price Listener")
    logger.info("=" * 60)
    logger.info("📊 Monitoring pool: 0x4a34ac7b916cc941530a99dfc0de27843bf20eba5e580f5c93d0a21e3bcb3464")
    logger.info("💾 Price logs: logs/prices.jsonl")
    logger.info("⚡ Press Ctrl+C to stop")
    logger.info("=" * 60)
    
    # Initialize listener
    listener = CellanaSwapListener()
    
    try:
        # Start listening
        await listener.run()
    except KeyboardInterrupt:
        logger.info("\n⏹️  Listener stopped by user")
    except Exception as e:
        logger.error(f"❌ Error in listener: {e}")
        raise
    finally:
        logger.info("👋 Shutting down...")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
