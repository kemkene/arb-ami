#!/usr/bin/env python3
"""Test Cellana swap event listener - with recent version."""

import asyncio
import sys
import grpc
from core.cellana_swap_listener import CellanaSwapListener
from config.settings import settings
from utils.logger import get_logger
from aptos.indexer.v1 import raw_data_pb2, raw_data_pb2_grpc

logger = get_logger()


def get_latest_version():
    """Get latest ledger version from Aptos."""
    try:
        # Query REST API for latest ledger version
        import urllib.request
        import json
        
        url = "https://fullnode.mainnet.aptoslabs.com/v1"
        req = urllib.request.Request(url, headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read())
            if 'ledger_version' in data:
                return int(data['ledger_version'])
    except Exception as e:
        logger.warning(f"Could not get latest version: {e}")
    
    return None


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
    print("CELLANA SWAP EVENT LISTENER - TEST (RECENT VERSION)")
    print("=" * 70)
    print()
    print(f"Configuration:")
    print(f"  Endpoint:         {settings.cellana_grpc_endpoint}")
    print(f"  Use TLS:          {settings.cellana_grpc_use_tls}")
    print(f"  API Key Set:      {bool(settings.cellana_grpc_api_key)}")
    print(f"  Event Type:       {settings.cellana_swap_event_type}")
    print(f"  Pool Filter:      {settings.cellana_swap_pool_address or '(all pools)'}")
    print()
    
    # Get latest version
    latest_version = get_latest_version()
    if latest_version:
        # Start from a recent version (100 versions back)
        starting_version = max(0, latest_version - 100)
        print(f"  Latest Version:   {latest_version}")
        print(f"  Starting Version: {starting_version} (100 versions back)")
    else:
        starting_version = settings.cellana_swap_starting_version
        print(f"  Starting Version: {starting_version}")
    
    print()
    print("Connecting to Aptos gRPC stream...")
    print("(Press Ctrl+C to stop)")
    print()
    print("=" * 70)
    print()
    
    # Create listener with modified starting version
    listener = CellanaSwapListener(on_swap_event=on_swap_event)
    listener.starting_version = starting_version
    
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
