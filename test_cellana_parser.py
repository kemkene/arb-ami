#!/usr/bin/env python3
"""Test Cellana swap event parser against real transaction data."""

from core.cellana_swap_listener import CellanaSwapListener

# Real SwapEvent from tx v4424008031
REAL_SWAP_EVENT = {
    "amount_in": "1600000000000",
    "amount_out": "13094035212",
    "from_token": "0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451",
    "pool": "0x4a34ac7b916cc941530a99dfc0de27843bf20eba5e580f5c93d0a21e3bcb3464",
    "to_token": "0x1::aptos_coin::AptosCoin",
}

def test_parser():
    """Test parser with real Cellana swap event."""
    listener = CellanaSwapListener()
    
    # Parse the real event
    parsed = listener._parse_swap_event(REAL_SWAP_EVENT)
    
    print("=" * 60)
    print("CELLANA SWAP EVENT PARSER TEST")
    print("=" * 60)
    print("\nOriginal Event:")
    for k, v in REAL_SWAP_EVENT.items():
        print(f"  {k}: {v}")
    
    print("\nParsed Result:")
    for k, v in parsed.items():
        if k != "raw_keys":
            print(f"  {k}: {v}")
    
    print("\nValidation:")
    expected = {
        "amount_in": 1600000000000,
        "amount_out": 13094035212,
        "token_in": "0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451",
        "pool": "0x4a34ac7b916cc941530a99dfc0de27843bf20eba5e580f5c93d0a21e3bcb3464",
        "token_out": "0x1::aptos_coin::AptosCoin",
    }
    
    all_ok = True
    for key, expected_val in expected.items():
        actual_val = parsed.get(key)
        ok = actual_val == expected_val
        status = "✓" if ok else "✗"
        print(f"  {status} {key}: {actual_val} {'(OK)' if ok else f'(expected {expected_val})'}")
        if not ok:
            all_ok = False
    
    print("\n" + "=" * 60)
    if all_ok:
        print("✓ PARSER VALIDATION PASSED")
    else:
        print("✗ PARSER VALIDATION FAILED")
    print("=" * 60)
    
    return all_ok

if __name__ == "__main__":
    import sys
    success = test_parser()
    sys.exit(0 if success else 1)
