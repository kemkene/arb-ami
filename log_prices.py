import asyncio
from config.settings import settings
from core.price_collector import PriceCollector
from exchanges.bybit import BybitWS
from exchanges.mexc import MexcWS
from utils.logger import get_logger

logger = get_logger()

async def log_prices():
    """Fetch and log current prices from Bybit and MEXC."""
    
    logger.info("=" * 80)
    logger.info("FETCHING PRICES FROM ALL EXCHANGES")
    logger.info("=" * 80)
    
    collector = PriceCollector()
    
    # Bybit
    try:
        logger.info("\n📊 BYBIT")
        bybit = BybitWS(collector=collector, symbols=[settings.cex_symbol])
        # Start bybit in background and let it collect one update
        bybit_task = asyncio.create_task(bybit.connect())
        await asyncio.sleep(2)  # Wait for connection and first update
        bybit_task.cancel()
        
        price_data = collector.get_exchange(settings.cex_symbol, "bybit")
        if price_data:
            logger.info(f"  {settings.cex_symbol}:  Bid={price_data.bid:.8f}  Ask={price_data.ask:.8f}  Spread={price_data.spread:.8f}")
        else:
            logger.warning("  No price data")
    except asyncio.CancelledError:
        logger.info("  Bybit fetch complete")
    except Exception as e:
        logger.error(f"  Bybit error: {e}")
    
    # MEXC
    try:
        logger.info("\n📊 MEXC")
        mexc = MexcWS(collector=collector, symbols=[settings.cex_symbol])
        mexc_task = asyncio.create_task(mexc.connect())
        await asyncio.sleep(2)  # Wait for first poll
        mexc_task.cancel()
        
        price_data = collector.get_exchange(settings.cex_symbol, "mexc")
        if price_data:
            logger.info(f"  {settings.cex_symbol}:  Bid={price_data.bid:.8f}  Ask={price_data.ask:.8f}  Spread={price_data.spread:.8f}")
        else:
            logger.warning("  No price data")
    except asyncio.CancelledError:
        logger.info("  MEXC fetch complete")
    except Exception as e:
        logger.error(f"  MEXC error: {e}")
    
    logger.info("\n" + "=" * 80)

if __name__ == "__main__":
    asyncio.run(log_prices())
