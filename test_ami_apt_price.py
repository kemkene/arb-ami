#!/usr/bin/env python3
"""Monitor AMI/APT price from Cellana SyncEvent."""

import asyncio
from core.cellana_swap_listener import CellanaSwapListener
from config.settings import settings
from utils.logger import get_logger

logger = get_logger()

# Price tracking
latest_price = None
price_history = []
MAX_HISTORY = 10


def on_sync_event(payload):
    """Handle SyncEvent and track AMI/APT price."""
    global latest_price, price_history
    
    price_spot = payload.get('price_ami_per_apt_spot')
    price_with_fee = payload.get('price_ami_per_apt_with_fee')
    price = price_spot  # Use spot price for tracking
    parsed = payload.get('parsed', {})
    pool = parsed.get('pool', '')
    
    # Only track AMI/APT pool
    if pool and pool.lower() == CellanaSwapListener.AMI_APT_POOL.lower():
        if price:
            latest_price = price
            price_history.append(price)
            if len(price_history) > MAX_HISTORY:
                price_history.pop(0)
            
            # Calculate price change
            price_change = ""
            if len(price_history) >= 2:
                prev_price = price_history[-2]
                change_pct = ((price - prev_price) / prev_price) * 100
                direction = "📈" if change_pct > 0 else "📉" if change_pct < 0 else "➡️"
                price_change = f" {direction} {change_pct:+.4f}%"
            
            version = payload.get('version')
            reserves_ami = parsed.get('reserves_1')
            reserves_apt = parsed.get('reserves_2')
            
            print()
            print("=" * 80)
            print(f"💰 AMI/APT PRICE UPDATE")
            print("=" * 80)
            print(f"  Transaction:     {version}")
            print(f"  Pool:            {pool[:16]}...")
            print(f"  Spot Price:      {price:.8f}{price_change}")
            if price_with_fee:
                print(f"  Price + Fee:     {price_with_fee:.8f} (includes 0.1% fee)")
            print(f"  Price APT/AMI:   {1/price:.2f}")
            print(f"  Reserves AMI:    {reserves_ami:,}")
            print(f"  Reserves APT:    {reserves_apt:,}")
            if len(price_history) >= 2:
                avg_price = sum(price_history) / len(price_history)
                print(f"  Avg (last {len(price_history)}):    {avg_price:.8f}")
            print("=" * 80)
            print()


async def main():
    """Monitor AMI/APT price from latest transactions."""
    print()
    print("=" * 80)
    print("🚀 AMI/APT PRICE MONITOR - CELLANA DEX")
    print("=" * 80)
    print()
    print(f"Configuration:")
    print(f"  Endpoint:        {settings.cellana_grpc_endpoint}")
    print(f"  AMI/APT Pool:    {CellanaSwapListener.AMI_APT_POOL}")
    print(f"  Starting:        Latest transactions")
    print()
    print("Monitoring AMI/APT price updates...")
    print("(Press Ctrl+C to stop)")
    print()
    print("=" * 80)
    print()
    
    # Create listener starting from version 0 to get latest
    listener = CellanaSwapListener(on_swap_event=on_sync_event)
    
    # Start from a recent known version with AMI/APT activity
    listener.starting_version = 4425718000
    
    try:
        await listener.run()
    except KeyboardInterrupt:
        print("\n\n👋 Stopping price monitor...")
        if latest_price:
            print(f"Last AMI/APT price: {latest_price:.8f}")


if __name__ == "__main__":
    asyncio.run(main())
