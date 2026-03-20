# Cellana Pool Reserves Bootstrap Logic

## Problem
When the arbitrage bot starts, it waits for the first `SyncEvent` from the Cellana gRPC stream to get AMI/APT price data. This means:
- **No price data at startup** → Arbitrage engine can't detect opportunities immediately
- **First swap event may take minutes to arrive** → Bot is blind until then
- **Missed arbitrage opportunities** in the first few minutes after startup

## Solution: Pool Reserves Bootstrap

The bot now **initializes AMI/APT reserves from the Cellana pool** at startup before subscribing to the gRPC stream.

### Implementation Details

#### 1. **Bootstrap Function** (_bootstrap_pool_reserves)
Located in `core/cellana_swap_listener.py`

**What it does:**
- Calls the `pool_reserves` view function on the Cellana module
- Gets the current AMI and APT reserve amounts
- Calculates the current spot price (AMI per APT)
- Creates a synthetic bootstrap payload
- Triggers the arbitrage engine callback with initial price

**View Function Signature:**
```solidity
// Module: 0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool
// Function: pool_reserves<T0: ObjectCore, T1: CoinType, T2: CoinType>(pool_id: address) -> (u128, u128)

// For AMI/APT pool:
// - T0: 0x1::object::ObjectCore (pool_id type constraint)
// - T1: AMI token type (from settings.ami_token_address)
// - T2: APT token type (from settings.apt_token_address)
// - pool_id: 0x4a34ac7b916cc941530a99dfc0de27843bf20eba5e580f5c93d0a21e3bcb3464
```

#### 2. **Startup Flow**
In `async def run()`:
```
1. Import proto dependencies
2. ✅ BOOTSTRAP: Call _bootstrap_pool_reserves()
   ├─ Query pool_reserves view function
   ├─ Calculate initial price
   ├─ Trigger on_swap_event callback
   └─ Log to prices.jsonl
3. Auto-detect latest version (if starting_version=0)
4. Connect to gRPC stream
5. Stream SyncEvents on loop
```

#### 3. **Price Calculation**
From reserves → price using constant product formula:

```python
# Spot price (no fee)
price_ami_per_apt = reserves_apt / reserves_ami

# Effective price (with Cellana fee)
price_with_fee = price_spot * (1 + 0.001)  # 0.1% volatile pool fee
```

**Example:**
- AMI reserves: 1,000,000 (1M AMI)
- APT reserves: 10,000,000 (10M APT, 8 decimals = 100 APT)
- Price (spot): 10.0 AMI/APT
- Price (+fee): 10.001 AMI/APT

#### 4. **Reserve Mapping**
For AMI/APT pool (0x4a34ac7b916cc941530a99dfc0de27843bf20eba5e580f5c93d0a21e3bcb3464):
- **reserves_1 (result[0])** = AMI reserves
- **reserves_2 (result[1])** = APT reserves

### Configuration

**Environment Variables** (in `.env`):
```bash
# Enable Cellana swap listener
CELLANA_SWAP_LISTENER_ENABLED=true

# Bootstrap will query REST API regardless of this, but stream requires gRPC endpoint
CELLANA_GRPC_ENDPOINT=<your-grpc-endpoint>

# Token addresses (must match deployed addresses)
AMI_TOKEN_ADDRESS=0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451
APT_TOKEN_ADDRESS=0x1::aptos_coin::AptosCoin
```

### Logging Output

**Successful Bootstrap:**
```
🔄 [BOOTSTRAP] Initializing AMI/APT pool reserves...
✅ [BOOTSTRAP] Success - pool reserves initialized
✅ [BOOTSTRAP] AMI/APT Pool Initialized
   Reserves: AMI=1,000,000 | APT=10,000,000
   Price (spot): 10.00000000 AMI/APT
   Price (+fee): 10.00100000 AMI/APT
```

**Failed Bootstrap (will continue with gRPC stream):**
```
🔄 [BOOTSTRAP] Initializing AMI/APT pool reserves...
❌ Error bootstrapping pool reserves: <error details>
⚠️ [BOOTSTRAP] Failed - will wait for first SyncEvent
```

### Benefits

✅ **Immediate price data** - No delay at startup
✅ **Fallback mechanism** - If bootstrap fails, waits for first SyncEvent
✅ **Consistent state** - Initial price snapshot logged to prices.jsonl
✅ **Arbitrage ready** - Engine can start checking opportunities immediately
✅ **No breaking changes** - Backward compatible with existing code

### Error Handling

- **REST API failure**: Logs warning, continues with stream
- **Wrong token types**: View function returns error, falls back to stream
- **Network timeout**: Retries with exponential backoff
- **Invalid reserves**: Skips if reserves_1 or reserves_2 are 0/null

### Testing

Check bot initialization:
1. Start bot with `CELLANA_SWAP_LISTENER_ENABLED=true`
2. Look for bootstrap success message in logs
3. Verify `reserves_ami` and `reserves_apt` in logs/prices.jsonl
4. Compare with Aptos explorer pool_reserves view function result

Expected flow at startup:
```
[11:25:34] 🔄 [BOOTSTRAP] Initializing AMI/APT pool reserves...
[11:25:35] ✅ [BOOTSTRAP] Success - pool reserves initialized
[11:25:35] 💰 [AMI/APT PRICE] ... (bootstrap data)
[11:25:35] CellanaSwapListener connected | endpoint=<endpoint> start_version=<version>
[11:25:36] 📡 Cellana update v=<version> | AMI/APT=<price> (from SyncEvent)
```

## Troubleshooting

**Q: Bootstrap says "Success" but no price in logs**
A: Check if callback `on_swap_event` is properly set in ArbitrageEngine

**Q: Getting "Unexpected view function response"**
A: Verify view function returns tuple (u128, u128) with reserves

**Q: Why is bootstrap price different from actual trades?**
A: Bootstrap gets spot price; actual trades include slippage and fees

**Q: Bootstrap fails but bot keeps running?**
A: By design - bot will wait for first SyncEvent from stream, just won't have initial price

## Related Files

- `core/cellana_swap_listener.py` - Bootstrap logic
- `config/settings.py` - Configuration
- `logs/prices.jsonl` - Price history (includes bootstrap)
- `core/arbitrage_engine.py` - Callback handler
