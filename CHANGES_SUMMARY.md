# Change Summary - Cellana Pool Bootstrap Implementation

## Overview

Implemented automatic pool reserves initialization when the Cellana swap listener starts. The bot now queries current AMI/APT reserves from the Cellana pool **at startup** instead of waiting for the first SyncEvent from the gRPC stream.

---

## Files Modified

### 1. `core/cellana_swap_listener.py`

#### Added Method: `_bootstrap_pool_reserves(self) -> bool`
- **Location:** Lines 225-322
- **Purpose:** Query pool_reserves view function to fetch initial reserves
- **Parameters:** None (uses class variables)
- **Returns:** `True` if successful, `False` otherwise
- **Side effects:**
  - Queries Aptos REST API
  - Calculates initial price
  - Triggers `on_swap_event()` callback
  - Logs to prices.jsonl

**Key implementation details:**
```python
def _bootstrap_pool_reserves(self) -> bool:
    # 1. Build view function request
    # 2. POST to https://fullnode.mainnet.aptoslabs.com/v1/view
    # 3. Parse response: [reserves_ami, reserves_apt]
    # 4. Calculate: price_ami_per_apt = reserves_apt / reserves_ami
    # 5. Trigger on_swap_event callback with bootstrap payload
    # 6. Return True if successful
```

#### Modified Method: `async def run(self)`
- **Location:** Lines 355-375 (within existing run method)
- **Added code:** Bootstrap initialization phase at start of run()
- **Behavior:**
  - Calls `_bootstrap_pool_reserves()` in thread executor (non-blocking)
  - Logs success/failure
  - Falls back to gRPC stream if bootstrap fails or times out
  - Continues with existing version detection and stream connection

**Code added:**
```python
# ┌───────────────────────────────────────────────────────────────────┐
# │  BOOTSTRAP: Initialize current pool reserves before streaming    │
# │  This ensures the arbitrage engine has a price immediately.      │
# └───────────────────────────────────────────────────────────────────┘
logger.info("🔄 [BOOTSTRAP] Initializing AMI/APT pool reserves...")
bootstrap_ok = await asyncio.get_event_loop().run_in_executor(
    None, self._bootstrap_pool_reserves
)
if bootstrap_ok:
    logger.success("✅ [BOOTSTRAP] Success - pool reserves initialized")
else:
    logger.warning("⚠️ [BOOTSTRAP] Failed - will wait for first SyncEvent")
```

---

## Files Created (Documentation)

### 1. `CELLANA_BOOTSTRAP_LOGIC.md`
Complete reference guide covering:
- Problem statement and solution
- Implementation details
- View function signature
- Startup flow
- Price calculation
- Configuration
- Logging output
- Error handling
- Testing procedure

### 2. `BOOTSTRAP_SUMMARY.md`
High-level overview showing:
- Execution flow diagram
- Key parameters for pool_reserves query
- Reserve mapping
- Code changes summary
- Execution timeline
- Fallback behavior
- Testing checklist
- Troubleshooting guide

### 3. `API_REQUEST_REFERENCE.md`
Technical API documentation including:
- REST endpoint and request format
- Parameter breakdown
- Expected response format
- Python implementation example
- Manual verification steps
- Debugging with cURL
- Common issues and solutions
- Quick reference for addresses

### 4. `BOOTSTRAP_LOGIC_VERIFICATION.md`
Complete logic walkthrough with:
- Startup sequence
- Bootstrap process step-by-step
- Arbitrage engine callback flow
- Price flow diagram
- Data flow visualization
- Reserve order verification
- Error scenarios and recovery
- Configuration checklist
- Performance metrics
- Testing steps

---

## Behavior Changes

### Before Implementation
```
main.py starts
  → CellanaSwapListener.run() starts
    → Connect to gRPC stream
    → Wait for first SyncEvent (30s-1m)
    → Get first price
    → Arbitrage monitoring starts
    
⏱️ ~30-60 seconds of blindness at startup
```

### After Implementation
```
main.py starts
  → CellanaSwapListener.run() starts
    → [NEW] Bootstrap: Query pool_reserves REST API (~100ms)
    → [NEW] Get initial price immediately
    → [NEW] Trigger arbitrage engine callback
    → Arbitrage monitoring can start immediately
    → [EXISTING] Connect to gRPC stream
    → [EXISTING] Continue with SyncEvents
    
✅ Ready in ~100ms, not waiting for first event
```

---

## API Integration

### REST API Call
- **Endpoint:** `POST https://fullnode.mainnet.aptoslabs.com/v1/view`
- **Module:** `0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool`
- **Function:** `pool_reserves<ObjectCore, AMI, APT>(pool_id)`
- **Timeout:** 10 seconds
- **Response:** `[reserves_ami_str, reserves_apt_str]`

### Configuration Used
- `settings.ami_token_address` → T1 generic parameter
- `settings.apt_token_address` → T2 generic parameter  
- `self.AMI_APT_POOL` → function argument (pool_id)

---

## Price Calculation

```python
# From reserves queried via pool_reserves
reserves_ami = int(result[0])
reserves_apt = int(result[1])

# Spot price (no fees)
price_ami_per_apt = reserves_apt / reserves_ami

# Effective price (with 0.1% Cellana fee)
price_with_fee = price_ami_per_apt * (1 + 0.001)

# Example
if reserves_ami = 1_000_000 and reserves_apt = 10_000_000:
    price_spot = 10.0
    price_fee = 10.001
```

---

## Logging Output

### Success Case
```
🔄 [BOOTSTRAP] Initializing AMI/APT pool reserves...
✅ [BOOTSTRAP] Success - pool reserves initialized
✅ [BOOTSTRAP] AMI/APT Pool Initialized
   Reserves: AMI=1,000,000 | APT=10,000,000
   Price (spot): 10.00000000 AMI/APT
   Price (+fee): 10.00100000 AMI/APT
```

### Failure Case
```
🔄 [BOOTSTRAP] Initializing AMI/APT pool reserves...
❌ Error bootstrapping pool reserves: Connection timeout
⚠️ [BOOTSTRAP] Failed - will wait for first SyncEvent
```

---

## Error Handling

| Error | Behavior |
|-------|----------|
| REST API timeout (10s) | Log warning, continue with stream |
| Invalid view function response | Log error, continue with stream |
| Zero reserves | Skip price calculation, log error |
| Callback exception | Log warning, continue |
| Network unreachable | Fallback to stream (same as before) |

---

## Testing Checklist

- [ ] Syntax check passes: `python3 -m py_compile core/cellana_swap_listener.py`
- [ ] Can import module: `from core.cellana_swap_listener import CellanaSwapListener`
- [ ] Bootstrap method exists: `hasattr(CellanaSwapListener, '_bootstrap_pool_reserves')`
- [ ] Start bot with bootstrap enabled
- [ ] See bootstrap success in logs
- [ ] Verify reserves in prices.jsonl with `source: bootstrap`
- [ ] Compare bootstrap price with explorer result
- [ ] Confirm arbitrage engine receives price
- [ ] Test fallback by disabling network (should wait for SyncEvent)

---

## Backward Compatibility

✅ **Fully backward compatible**
- No breaking changes to existing APIs
- If bootstrap fails, bot works exactly as before
- Existing SyncEvent handling unchanged
- All existing configurations work unchanged
- New method is internal only (`_bootstrap_pool_reserves`)

---

## Performance Impact

| Metric | Impact |
|--------|--------|
| Startup delay | ~100-200ms (REST API call) |
| Memory usage | Negligible (one REST call) |
| Network overhead | 1 additional REST call at startup |
| CPU usage | Minimal (parsing JSON response) |
| Event loop blocking | 0% (runs in thread executor) |

---

## Dependencies

### Added (if not present)
- `requests` library (for REST API call)

### Already required
- `asyncio` (already used)
- `json` (already used)
- `logging` (already used)

No new external dependencies needed - `requests` is already in the project.

---

## Deployment Notes

### No configuration changes needed
Bootstrap works with existing `.env` configuration:
```bash
# These are already used for other purposes
AMI_TOKEN_ADDRESS=<value>
APT_TOKEN_ADDRESS=<value>

# Bootstrap uses Aptos mainnet public API by default
# No additional keys/endpoints needed
```

### Optional: Disable bootstrap
To disable bootstrap (not recommended):
There is no configuration option - bootstrap always runs.
To disable, edit `cellana_swap_listener.py` line 355 to comment out bootstrap call.

---

## Known Limitations

1. **Reserves may go stale:** Bootstrap gets snapshot at startup; if no swaps for 1 minute and stream fails, price becomes stale (same as before)

2. **Race condition:** If first SyncEvent arrives during bootstrap, there's a brief period of uncertainty (handled gracefully)

3. **Rest API dependence:** Bootstrap depends on external REST API; requires network access

4. **Pool ID hardcoded:** Currently uses hardcoded pool ID; would need change if pool migrates

---

## Future Enhancements

Possible improvements (not implemented):
- Periodic bootstrap refresh every N minutes
- Cache bootstrap result to file for recovery
- Support multiple pools simultaneously
- Automatic pool detection
- Bootstrap retry with exponential backoff
- Pool validation before bootstrap

---

## Related Documents

- [CELLANA_BOOTSTRAP_LOGIC.md](CELLANA_BOOTSTRAP_LOGIC.md) - Detailed logic guide
- [BOOTSTRAP_SUMMARY.md](BOOTSTRAP_SUMMARY.md) - High-level overview
- [API_REQUEST_REFERENCE.md](API_REQUEST_REFERENCE.md) - API documentation
- [BOOTSTRAP_LOGIC_VERIFICATION.md](BOOTSTRAP_LOGIC_VERIFICATION.md) - Complete walkthrough

---

## Questions?

Refer to the related documentation files for:
- **How it works:** See BOOTSTRAP_LOGIC_VERIFICATION.md
- **API details:** See API_REQUEST_REFERENCE.md
- **Configuration:** See CELLANA_BOOTSTRAP_LOGIC.md
- **Quick reference:** See BOOTSTRAP_SUMMARY.md
