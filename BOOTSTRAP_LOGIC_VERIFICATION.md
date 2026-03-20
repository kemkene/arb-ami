# Cellana Bootstrap - Complete Logic Verification

## Summary of Changes

✅ **Added pool_reserves bootstrap** to `CellanaSwapListener.run()`
- Queries initial reserves via Aptos REST API
- Initializes arbitrage engine with current price
- Fallback to gRPC stream if bootstrap fails

---

## Execution Flow

### 1. Bot Startup Sequence

```
main.py
  └─ asyncio.run(main())
      ├─ validate_accounts()
      ├─ PriceCollector() created
      ├─ ArbitrageEngine(collector, ...) created
      ├─ CellanaSwapListener(on_swap_event=arb.update_cellana_state) created
      └─ Task: celiana_listener.run()
          ↓ (runs on event loop)
```

### 2. CellanaSwapListener.run() - NEW Bootstrap Phase

```python
async def run(self) -> None:
    # Step 1: Check proto dependencies
    # Step 2: ✅ NEW: Bootstrap pool reserves
    
    logger.info("🔄 [BOOTSTRAP] Initializing AMI/APT pool reserves...")
    bootstrap_ok = await asyncio.get_event_loop().run_in_executor(
        None, self._bootstrap_pool_reserves  # ← Blocking call in thread executor
    )
    
    if bootstrap_ok:
        logger.success("✅ [BOOTSTRAP] Success - pool reserves initialized")
        # Callback was already triggered in _bootstrap_pool_reserves
        # ArbitrageEngine now has initial price
    else:
        logger.warning("⚠️ [BOOTSTRAP] Failed - will wait for first SyncEvent")
    
    # Step 3: Auto-detect starting_version
    # Step 4: Connect to gRPC stream
    # Step 5: Listen for SyncEvents continuously
```

### 3. Bootstrap Process - _bootstrap_pool_reserves()

```
_bootstrap_pool_reserves() [SYNC - runs in thread executor]
│
├─ Build REST API request
│  │
│  ├─ Module: 0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1
│  ├─ Function: liquidity_pool::pool_reserves
│  ├─ Type args: [ObjectCore, AMI, APT]
│  └─ Arguments: [pool_id]
│
├─ POST to https://fullnode.mainnet.aptoslabs.com/v1/view
│  │
│  └─ Response: [reserves_ami_str, reserves_apt_str]
│
├─ Parse result
│  ├─ reserves_ami = int(result[0])
│  ├─ reserves_apt = int(result[1])
│  └─ Validate: both > 0
│
├─ Calculate prices
│  ├─ price_spot = reserves_apt / reserves_ami
│  ├─ price_with_fee = price_spot * (1 + 0.001)
│  └─ {price_ami_per_apt, price_ami_per_apt_with_fee}
│
├─ Create payload (synthetic SyncEvent)
│  ├─ type: "pool_reserves_bootstrap"
│  ├─ pool: 0x4a34ac7b...
│  ├─ reserves_1: reserves_ami
│  ├─ reserves_2: reserves_apt
│  ├─ price_ami_per_apt_spot: price_spot
│  ├─ price_ami_per_apt_with_fee: price_with_fee
│  └─ source: "bootstrap"
│
├─ Log to prices.jsonl
│  └─ {pool, pool_name, reserves_ami, reserves_apt, price_spot, source, ...}
│
├─ Trigger callback
│  └─ self.on_swap_event(payload)
│      ↓
│      ArbitrageEngine.update_cellana_state(payload) [async]
│          ├─ Extracts price_ami_per_apt
│          ├─ Updates self.cellana_price
│          ├─ Triggers arbitrage check
│          └─ Logs status
│
└─ Return True if successful, False if error
```

### 4. Arbitrage Engine Callback

```python
# core/arbitrage_engine.py
async def update_cellana_state(self, payload: dict) -> None:
    """Callback from CellanaSwapListener when price updates."""
    
    try:
        # Extract price from bootstrap or SyncEvent
        price = payload.get('price_ami_per_apt_spot')
        version = payload.get('version')
        
        self.cellana_price = {
            'price': price,
            'timestamp': time.time(),
            'version': version,
            'source': payload.get('source')  # 'bootstrap' or event version
        }
        
        logger.debug(f"Cellana price updated: {price}")
        
        # Now arbitrage engine knows current DEX price!
        # Can start checking: CEX vs DEX spreads
        
    except Exception as e:
        logger.error(f"Error updating cellana state: {e}")
```

### 5. Price Flow

```
Bootstrap Success
│
├─ reserves_ami: 1,000,000
├─ reserves_apt: 10,000,000
│
├─ Calculate: 10,000,000 / 1,000,000 = 10.0
│
├─ ArbitrageEngine.cellana_price = {
│    'price': 10.0,  ← This is now available!
│    'source': 'bootstrap'
│  }
│
└─ Arbitrage check can now run
   ├─ Check CEX prices (from Bybit/MEXC feeds)
   ├─ Compare: CEX price vs DEX price (10.0)
   ├─ Calculate opportunity
   └─ Log or execute if profitable
```

---

## Data Flow Diagram

```
┌──────────────────────────────────────────────────────────┐
│ When CellanaSwapListener starts:                          │
└──────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────────┐
│ BOOTSTRAP PHASE (NEW)                                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  REST API Query                   Aptos REST API          │
│  ┌────────────────┐              ┌──────────────┐         │
│  │ pool_reserves  │─────POST────→│ /v1/view     │         │
│  │ AMI/APT pool   │              │              │         │
│  └────────────────┘              └──────────────┘         │
│                                         ↓                  │
│  Result: [reserves_ami,          [1000000, 10000000]      │
│           reserves_apt]                                    │
│                ↓                                           │
│  Calculate:  10000000 / 1000000 = 10.0 AMI/APT            │
│                ↓                                           │
│  Create Payload                  Callback                 │
│  ┌────────────────┐              ┌──────────────────┐    │
│  │ {               │─────call────→│ on_swap_event()  │    │
│  │  price: 10.0   │              │ ↓                │    │
│  │  reserves: ... │              │ ArbitrageEngine. │    │
│  │ }              │              │ update_cellana   │    │
│  └────────────────┘              │ _state()         │    │
│         ↓                        └──────────────────┘    │
│  Log to prices.jsonl                                      │
│                                                           │
│  ✅ Arbitrage engine now has initial DEX price           │
│                                                           │
└─────────────────────────────────────────────────────────────┘
         ↓
┌──────────────────────────────────────────────────────────┐
│ STREAM PHASE (EXISTING)                                  │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Connect to Aptos Indexer gRPC                          │
│  ↓                                                       │
│  Stream transactions from starting_version              │
│  ↓                                                       │
│  Filter for SyncEvents                                  │
│  ↓                                                       │
│  Parse reserves from event                              │
│  ↓                                                       │
│  Calculate price (same as bootstrap)                    │
│  ↓                                                       │
│  Trigger callback with updated price                    │
│                                                          │
│  Continuous price updates from on-chain swaps           │
│                                                          │
└──────────────────────────────────────────────────────────┘
         ↓
┌──────────────────────────────────────────────────────────┐
│ ARBITRAGE MONITORING (ACTIVE)                            │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  Every time DEX price updates:                          │
│  ├─ Get CEX prices (from Bybit/MEXC feeds)            │
│  ├─ Calculate: CEX implied price                       │
│  ├─ Compare with DEX price                            │
│  ├─ Calculate profit opportunity                       │
│  └─ Execute if profitable                             │
│                                                          │
│  Bot is ready from second 1!                            │
│  (before first SyncEvent arrives)                       │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

---

## Reserve Order Verification

**Important:** Verify that reserves_1 corresponds to AMI and reserves_2 to APT

```
pool_reserves<ObjectCore, T1: Coin, T2: Coin>(pool_id)
                         ↓                ↓
Called with:  [ObjectCore, AMI_type, APT_type]
                                     
Response: [reserves_of_T1, reserves_of_T2]
           [reserves_of_AMI, reserves_of_APT]
           [result[0],       result[1]]
```

### To verify coin order:

1. Check Cellana module for pool creation order
2. Or verify by comparing with explorer:
   - Go to pool page
   - See which coin is first in definition
3. Or test with known swap event:
   - Find a SyncEvent with known reserves
   - Compare with bootstrap result

---

## Error Scenarios & Recovery

### Scenario 1: Bootstrap Success → Normal Operation
```
✅ Bootstrap completes
   ↓
   Arbitrage engine has initial price
   ↓
   CEX feeds connect
   ↓
   Monitoring active immediately
```

### Scenario 2: Bootstrap Timeout → Wait for Stream
```
❌ Bootstrap times out (10s)
   ↓
   ⚠️ Fallback to gRPC stream
   ↓
   ~30 seconds later: First SyncEvent arrives
   ↓
   Arbitrage engine gets first price from event
   ↓
   Monitoring starts (delayed but works)
```

### Scenario 3: Bootstrap API Error → Retry on Stream
```
❌ REST API returns 400/500 error
   ↓
   ⚠️ Logged as warning, fallback to stream
   ↓
   Bot continues normally
   ↓
   First SyncEvent provides price
```

### Scenario 4: Both Fail → Manual Fix Needed
```
❌ Bootstrap fails AND first SyncEvent never arrives
   ↓
   ❌ Bot has no DEX price
   ↓
   🔧 Check:
      - Cellana pool exists on-chain
      - Token addresses correct in .env
      - gRPC endpoint reachable
      - Starting version not in future
```

---

## Configuration Checklist

For bootstrap to work, verify in `.env`:

```bash
# ✅ Must have these set correctly
AMI_TOKEN_ADDRESS=0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451
APT_TOKEN_ADDRESS=0x1::aptos_coin::AptosCoin

# ✅ Optional but recommended
CELLANA_SWAP_LISTENER_ENABLED=true
CELLANA_SWAP_STARTING_VERSION=0   # Auto-detect from latest

# ✅ For gRPC stream (if bootstrap is only mechanism)
CELLANA_GRPC_ENDPOINT=<your-endpoint>
CELLANA_GRPC_API_KEY=<your-key>
```

---

## Performance Impact

- **Bootstrap timing:** ~100-200ms (REST API call)
- **Blocking thread:** Runs in executor, doesn't block event loop
- **Retry logic:** 10 second timeout, no retries (falls back to stream)
- **Memory:** Minimal (one REST call result cached in callback)

Bootstrap is fast and non-blocking because it runs in a thread executor while the event loop stays responsive.

---

## Next Steps to Test

1. Enable bootstrap in config:
   ```bash
   CELLANA_SWAP_LISTENER_ENABLED=true
   ```

2. Start bot:
   ```bash
   python3 run_integrated_arb.py
   ```

3. Check logs for:
   ```
   🔄 [BOOTSTRAP] Initializing...
   ✅ [BOOTSTRAP] Success
   ✅ [BOOTSTRAP] AMI/APT Pool Initialized
   ```

4. Verify prices.jsonl has bootstrap entry:
   ```json
   {"source": "bootstrap", "reserves_ami": ..., "price_spot": ...}
   ```

5. Compare bootstrap price with explorer 👍
