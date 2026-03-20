#!/usr/bin/env python3
"""Test with specific transaction version."""

import asyncio
import sys
from core.cellana_swap_listener import CellanaSwapListener
from config.settings import settings
from utils.logger import get_logger

logger = get_logger()


def on_swap_event(payload):
    """Handle incoming sync event (sync)."""
    parsed = payload.get("parsed", {})
    price_spot = payload.get('price_ami_per_apt_spot')
    price_with_fee = payload.get('price_ami_per_apt_with_fee')
    pool = parsed.get('pool', '')
    
    print()
    print("=" * 80)
    print(f"✓ SYNC EVENT DETECTED")
    print("=" * 80)
    print(f"  Version:      {payload.get('version')}")
    print(f"  Sender:       {payload.get('sender')}")
    print(f"  Pool:         {pool[:16]}..." if len(pool) > 16 else f"  Pool:         {pool}")
    print(f"  Reserves 1:   {parsed.get('reserves_1')}")
    print(f"  Reserves 2:   {parsed.get('reserves_2')}")
    if price_spot:
        print(f"  Spot Price:    {price_spot:.8f}")
    if price_with_fee:
        print(f"  Price + Fee:   {price_with_fee:.8f} (0.1%)")
    print("=" * 80)
    print()


async def main():
    """Run the listener with specific transaction version."""
    version = 4425718052
    
    print()
    print("=" * 80)
    print(f"🔍 AMI/APT PRICE LISTENER - TEST FROM TX v={version}")
    print("=" * 80)
    print()
    print(f"Configuration:")
    print(f"  Endpoint:          {settings.cellana_grpc_endpoint}")
    print(f"  Event Type:        {settings.cellana_swap_event_type}")
    print(f"  Target Pool:       {settings.cellana_swap_pool_address}")
    print(f"  Pool Name:         AMI/APT")
    print(f"  Starting Version:  {version}")
    print()
    print("Listening for AMI/APT price updates only...")
    print("(Press Ctrl+C to stop)")
    print()
    print("=" * 80)
    print()
    
    # Create listener
    listener = CellanaSwapListener(on_swap_event=on_swap_event)
    listener.starting_version = version
    listener.pool_address = ""  # No pool filter
    
    try:
        await listener.run()
    except KeyboardInterrupt:
        print()
        print("Listener stopped.")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
