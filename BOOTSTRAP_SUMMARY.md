# AMI/APT Pool Bootstrap Logic - Summary

## Overview

When the Cellana swap listener starts, it now performs a **two-phase initialization**:

```
┌─────────────────────────────────────────────────────────────────────┐
│ CellanaSwapListener.run()                                           │
└─────────────────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────────────────┐
│ [PHASE 1] BOOTSTRAP - Get initial pool state                        │
├─────────────────────────────────────────────────────────────────────┤
│ 1. Query pool_reserves view function via REST API                   │
│    - Module: 0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f...    │
│    - Function: liquidity_pool::pool_reserves                        │
│    - Arguments: pool_id=0x4a34ac7b916cc941530a99dfc0de27843bf...   │
│                                                                     │
│ 2. Parse response: (reserves_ami, reserves_apt)                    │
│                                                                     │
│ 3. Calculate prices:                                                │
│    - Spot price: reserves_apt / reserves_ami                        │
│    - With fee: spot_price * (1 + 0.001)                            │
│                                                                     │
│ 4. Trigger on_swap_event callback with bootstrap data              │
│    → ArbitrageEngine gets initial price immediately                │
│                                                                     │
│ 5. Log to prices.jsonl (source: "bootstrap")                       │
└─────────────────────────────────────────────────────────────────────┘
         ↓
         ✓ Success: Arbitrage engine has initial price
         ✗ Timeout: Continue to Phase 2, wait for first SyncEvent
         ↓
┌─────────────────────────────────────────────────────────────────────┐
│ [PHASE 2] STREAM - Listen for SyncEvents                            │
├─────────────────────────────────────────────────────────────────────┤
│ 1. Connect to Aptos Indexer gRPC endpoint                           │
│                                                                     │
│ 2. Auto-detect starting_version from latest block                  │
│    (or use CELLANA_SWAP_STARTING_VERSION from config)              │
│                                                                     │
│ 3. Stream transactions with exponential backoff on errors          │
│                                                                     │
│ 4. Filter for SyncEvent from AMI/APT pool                          │
│                                                                     │
│ 5. Parse event data (reserves_1, reserves_2)                       │
│                                                                     │
│ 6. Calculate and update prices continuously                         │
│                                                                     │
│ 7. Trigger callback for each price update                          │
└─────────────────────────────────────────────────────────────────────┘
         ↓
         → Arbitrage engine monitors continuously
         → Logs all price updates to prices.jsonl
```

## Key Parameters for pool_reserves Query

**View Function:** `liquidity_pool::pool_reserves<T0, T1, T2>`

| Parameter | Value |
|-----------|-------|
| **Module** | `0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool` |
| **Function** | `pool_reserves` |
| **T0** (constraint) | `0x1::object::ObjectCore` |
| **T1** (coin 1) | AMI token type |
| **T2** (coin 2) | `0x1::aptos_coin::AptosCoin` |
| **Pool ID** | `0x4a34ac7b916cc941530a99dfc0de27843bf20eba5e580f5c93d0a21e3bcb3464` |
| **Return** | `(u128, u128)` = `(reserves_ami, reserves_apt)` |

## Code Changes

### File: `core/cellana_swap_listener.py`

**Added Method:**
```python
def _bootstrap_pool_reserves(self) -> bool:
    """Bootstrap current pool reserves from Cellana at startup."""
    # Queries pool_reserves view function
    # Returns: True if successful, False otherwise
    # Side effect: Calls self.on_swap_event() with bootstrap data
```

**Modified Method:**
```python
async def run(self) -> None:
    # ... existing code ...
    
    # NEW: Bootstrap phase
    logger.info("🔄 [BOOTSTRAP] Initializing AMI/APT pool reserves...")
    bootstrap_ok = await asyncio.get_event_loop().run_in_executor(
        None, self._bootstrap_pool_reserves
    )
    if bootstrap_ok:
        logger.success("✅ [BOOTSTRAP] Success - pool reserves initialized")
    else:
        logger.warning("⚠️ [BOOTSTRAP] Failed - will wait for first SyncEvent")
    
    # ... existing code: latest_version detection ...
    # ... existing code: gRPC stream connection ...
```

## Reserve Mapping

For the AMI/APT pool:
- **reserves_1** (result[0]) = AMI reserves (8 decimals)
- **reserves_2** (result[1]) = APT reserves (8 decimals)

### Price Calculations

```python
# AMI/APT pool: reserves are in the same order as coin types
reserve_ami = result[0]      # reserve of first coin (AMI)
reserve_apt = result[1]      # reserve of second coin (APT)

# Spot price (no fees)
price_ami_per_apt = reserve_apt / reserve_ami
# Example: If reserves are (1M AMI, 10M APT)
#         = 10M / 1M = 10.0 (1 AMI = 10 APT)

# Effective price (with Cellana 0.1% fee for volatile pools)
price_with_fee = price_ami_per_apt * (1 + 0.001)
#               = 10.0 * 1.001 = 10.001000
```

## Execution Timeline

**At Startup:**
```
T=0ms:   Start CellanaSwapListener.run()
T=10ms:  Bootstrap REST API call begins
T=100ms: pool_reserves response received
T=150ms: Price calculated, callback triggered
T=160ms: Prices logged to prices.jsonl
T=200ms: Fetch latest blockchain version
T=300ms: Connect to gRPC stream
T=400ms: Start listening for SyncEvents
         ↓
         Bot is now ready to detect arbitrage!
```

## Fallback Behavior

- **Bootstrap succeeds**: Arbitrage engine has initial price immediately
- **Bootstrap times out (10s)**: Warning logged, bot continues with stream
- **Both bootstrap and first SyncEvent fail**: Bot waits for next SyncEvent
- **All SyncEvents fail**: Exponential backoff, max 5 reconnection retries

## Testing Checklist

- [ ] Bot starts with `CELLANA_SWAP_LISTENER_ENABLED=true`
- [ ] Bootstrap log message appears
- [ ] Initial price logged to `logs/prices.jsonl` with source="bootstrap"
- [ ] Arbitrage engine receives initial price (check logs)
- [ ] Subsequent SyncEvents update prices normally
- [ ] Compare bootstrap price with Aptos explorer pool_reserves result
- [ ] Verify reserves are non-zero and reasonable

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| Bootstrap times out | REST API unreachable | Check network, REST endpoint |
| "Unexpected view function response" | Types mismatch | Verify token addresses in .env |
| Price is 0 | Division by zero in reserves | Check if AMI or APT reserves is 0 |
| Callback not triggered | callback is None | Verify on_swap_event is set |
| Different price after first SyncEvent | Normal - SyncEvent has updated reserves | Expected behavior |

## Related Documentation

- **Explorer Link**: https://explorer.aptoslabs.com/account/0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1/modules/view/liquidity_pool/pool_reserves
- **Pool Address**: 0x4a34ac7b916cc941530a99dfc0de27843bf20eba5e580f5c93d0a21e3bcb3464
- **Cellana Module**: 0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1
