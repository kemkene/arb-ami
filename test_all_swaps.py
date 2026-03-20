#!/usr/bin/env python3
"""Test Cellana listener - all events (no pool filter)."""

import asyncio
import sys
from core.cellana_swap_listener import CellanaSwapListener
from config.settings import settings
from utils.logger import get_logger
from aptos.indexer.v1 import raw_data_pb2, raw_data_pb2_grpc

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
    print(f"  Amount In:   {parsed.get('amount_in')}")
    print(f"  Amount Out:  {parsed.get('amount_out')}")
    print(f"  Token In:    {parsed.get('token_in')}")
    print(f"  Token Out:   {parsed.get('token_out')}")
    print("=" * 70)
    print()


async def main():
    """Run the listener."""
    print()
    print("=" * 70)
    print("CELLANA SWAP EVENT LISTENER - ALL EVENTS TEST")
    print("=" * 70)
    print()
    
    # Get latest version
    try:
        import urllib.request, json
        url = "https://fullnode.mainnet.aptoslabs.com/v1"
        req = urllib.request.Request(url, headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read())
            latest_version = int(data['ledger_version'])
            starting_version = max(0, latest_version - 50)  # Last 50 versions
    except:
        latest_version = None
        starting_version = 0
    
    print(f"Configuration:")
    print(f"  Endpoint:         {settings.cellana_grpc_endpoint}")
    print(f"  Event Type:       {settings.cellana_swap_event_type}")
    if latest_version:
        print(f"  Latest Version:   {latest_version}")
        print(f"  Starting Version: {starting_version} (last 50 versions)")
    print()
    print("Listening for ANY Cellana swap events (no pool filter)...")
    print("(Press Ctrl+C to stop)")
    print()
    
    # Create listener without pool filter
    listener = CellanaSwapListener(on_swap_event=on_swap_event)
    listener.starting_version = starting_version
    listener.pool_address = ""  # Disable pool filter
    
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
