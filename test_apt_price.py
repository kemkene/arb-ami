#!/usr/bin/env python3
"""
Test APT/USDT price feed from Bybit and MEXC
"""
import asyncio
import sys
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config.settings import settings
from core.price_collector import PriceCollector
from exchanges.bybit import BybitWS
from exchanges.mexc import MexcWS
from utils.logger import get_logger

logger = get_logger()


async def main():
    logger.info("🔍 Testing APT/USDT Price Feed")
    logger.info("=" * 60)
    
    collector = PriceCollector()
    
    # Subscribe to APTUSDT
    symbols = ["APTUSDT"]
    
    # Start feeds
    tasks = []
    
    if settings.mexc_api_key:
        mexc = MexcWS(collector, symbols=symbols)
        tasks.append(asyncio.create_task(mexc.connect()))
        logger.info("✓ MEXC feed enabled")
    
    if settings.bybit_api_key:
        bybit = BybitWS(collector, symbols=symbols)
        tasks.append(asyncio.create_task(bybit.connect()))
        logger.info("✓ Bybit feed enabled")
    
    if not tasks:
        logger.error("No exchanges enabled (check API keys)")
        return
    
    logger.info("=" * 60)
    
    # Monitor prices
    async def monitor():
        await asyncio.sleep(5)  # Wait for initial prices
        
        for i in range(20):
            logger.info(f"\n📊 Price Check #{i+1}")
            logger.info("-" * 60)
            
            # Check MEXC
            mexc_apt = collector.get_exchange("APTUSDT", "mexc")
            if mexc_apt:
                logger.info(
                    f"[MEXC]  APTUSDT | "
                    f"bid={mexc_apt.bid:.6f} | "
                    f"ask={mexc_apt.ask:.6f} | "
                    f"mid={mexc_apt.mid:.6f} | "
                    f"age={mexc_apt.age:.1f}s"
                )
            else:
                logger.warning("[MEXC]  APTUSDT | No data")
            
            # Check Bybit
            bybit_apt = collector.get_exchange("APTUSDT", "bybit")
            if bybit_apt:
                logger.info(
                    f"[Bybit] APTUSDT | "
                    f"bid={bybit_apt.bid:.6f} | "
                    f"ask={bybit_apt.ask:.6f} | "
                    f"mid={bybit_apt.mid:.6f} | "
                    f"age={bybit_apt.age:.1f}s"
                )
            else:
                logger.warning("[Bybit] APTUSDT | No data")
            
            # Calculate average
            if mexc_apt and bybit_apt:
                avg_mid = (mexc_apt.mid + bybit_apt.mid) / 2
                spread = abs(mexc_apt.mid - bybit_apt.mid)
                spread_pct = (spread / avg_mid) * 100
                logger.success(
                    f"[AVG]   APTUSDT | "
                    f"mid={avg_mid:.6f} | "
                    f"spread={spread:.6f} ({spread_pct:.3f}%)"
                )
            
            await asyncio.sleep(3)
    
    tasks.append(asyncio.create_task(monitor()))
    
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.info("\n⏹️  Stopped")
        for task in tasks:
            task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
