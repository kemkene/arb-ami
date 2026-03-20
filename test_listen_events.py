#!/usr/bin/env python3
"""Test Cellana swap event listener - connect to Aptos fullnode and stream events."""

import asyncio
import sys
from core.cellana_swap_listener import CellanaSwapListener
from config.settings import settings
from utils.logger import get_logger

logger = get_logger()


async def on_swap_event(payload):
    """Handle incoming swap event."""
    parsed = payload.get("parsed", {})
    print()
    print("=" * 70)
    print(f"✓ SWAP EVENT DETECTED (Version: {payload.get('version')})")
    print("=" * 70)
    print(f"  Sender:      {payload.get('sender')}")
    print(f"  Pool:        {parsed.get('pool')}")
    print(f"  Amount In:   {parsed.get('amount_in')} (from {parsed.get('token_in')})")
    print(f"  Amount Out:  {parsed.get('amount_out')} (to {parsed.get('token_out')})")
    print("=" * 70)
    print()


async def main():
    """Run the listener."""
    print()
    print("=" * 70)
    print("CELLANA SWAP EVENT LISTENER - TEST")
    print("=" * 70)
    print()
    print(f"Configuration:")
    print(f"  Endpoint:         {settings.cellana_grpc_endpoint}")
    print(f"  Use TLS:          {settings.cellana_grpc_use_tls}")
    print(f"  API Key Set:      {bool(settings.cellana_grpc_api_key)}")
    print(f"  Event Type:       {settings.cellana_swap_event_type}")
    print(f"  Pool Filter:      {settings.cellana_swap_pool_address or '(all pools)'}")
    print(f"  Starting Version: {settings.cellana_swap_starting_version}")
    print()
    print("Connecting to Aptos fullnode gRPC...")
    print("(Press Ctrl+C to stop)")
    print()
    print("=" * 70)
    print()
    
    # Create listener with callback
    listener = CellanaSwapListener(on_swap_event=on_swap_event)
    
    try:
        await listener.run()
    except KeyboardInterrupt:
        print()
        print("Listener stopped.")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
