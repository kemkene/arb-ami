# Liquidity Quantity Check Implementation

## Changes Made

### 1. Added Minimum Quantity Threshold
**File:** `core/dex_cex_arbitrage.py` (line 49)

```python
# Minimum quantity requirements (base units)
self.min_qty = 100.0  # Reject if bid_qty or ask_qty < 100
```

- **Purpose:** Filter out opportunities with insufficient orderbook depth
- **Default:** 100 units (base currency of the pair)
- **Can be adjusted:** Modify `self.min_qty` in `__init__` method

### 2. Added Liquidity Check Method
**File:** `core/dex_cex_arbitrage.py` (line 88-102)

```python
def _has_sufficient_liquidity(self, price_data: PriceData, min_qty: Optional[float] = None) -> bool:
    """Check if bid/ask quantities meet minimum threshold."""
    if min_qty is None:
        min_qty = self.min_qty
    
    if price_data.bid_qty < min_qty or price_data.ask_qty < min_qty:
        return False
    return True
```

### 3. Applied Liquidity Check to DEX-CEX Arbitrage
**File:** `core/dex_cex_arbitrage.py` (line 203-209)

```python
# Check liquidity (bid_qty and ask_qty must be >= minimum)
if not self._has_sufficient_liquidity(cex_price_data):
    logger.debug(
        f"{exchange_lower.upper()} AMIUSDT insufficient liquidity: bid_qty={cex_price_data.bid_qty:.2f} "
        f"ask_qty={cex_price_data.ask_qty:.2f} (min required: {self.min_qty})"
    )
    return
```

- Checks AMIUSDT prices before processing
- Also validates APT/USDT prices when available

### 4. Applied Liquidity Check to CEX-CEX Arbitrage
**File:** `core/dex_cex_arbitrage.py` (line 403-416)

```python
# Check liquidity on both exchanges
if not self._has_sufficient_liquidity(bybit_price_data):
    logger.debug(...)
    return

if not self._has_sufficient_liquidity(mexc_price_data):
    logger.debug(...)
    return
```

- Ensures both Bybit AND MEXC have sufficient depth
- Rejects opportunities if either exchange lacks liquidity

---

## Expected Impact

### Before
```
[MEXC] AMIUSDT bid=0.008048 ask=0.008074 bid_qty=35411.31 ask_qty=21373.4
[Bybit] AMIUSDT bid=0.00806 ask=0.00807 bid_qty=100.5 ask_qty=50.2
↓
Processes both (even though Bybit has only ~50 units ask_qty)
Result: Possible slippage on thin orderbook
```

### After
```
[MEXC] AMIUSDT bid=0.008048 ask=0.008074 bid_qty=35411.31 ask_qty=21373.4
↓ min_qty=100 check ✓ PASS

[Bybit] AMIUSDT bid=0.00806 ask=0.00807 bid_qty=100.5 ask_qty=50.2
↓ min_qty=100 check ✗ FAIL (ask_qty=50.2 < 100)
↓
SKIP - Insufficient liquidity on Bybit
Result: No slippage concerns on thin orderbook
```

---

## Log Output

When opportunities are rejected due to insufficient liquidity, you'll see:

```
DEBUG | Bybit AMIUSDT insufficient liquidity: bid_qty=50.20 ask_qty=100.50 (min required: 100.0)
DEBUG | MEXC AMIUSDT insufficient liquidity: bid_qty=95.00 ask_qty=150.00 (min required: 100.0)
```

---

## Configuration

### Adjust Minimum Quantity

Edit `core/dex_cex_arbitrage.py` line 49:

```python
self.min_qty = 100.0  # Change this value
```

**Suggested values:**
- **100** (default): Safe, only accepts deep liquidity
- **50**: More inclusive, captures more opportunities  
- **10**: Very permissive, includes thin orderbooks
- **0**: Disable check (not recommended)

---

## Testing

Run bot to see quantity filtering:

```bash
timeout 30 python3 run_integrated_arb.py 2>&1 | grep -E "insufficient liquidity|ARB FOUND" | head -20
```

Expected:
```
DEBUG | Bybit AMIUSDT insufficient liquidity: bid_qty=60.5 ask_qty=120.3 (min required: 100.0)
DEBUG | MEXC AMIUSDT insufficient liquidity: bid_qty=95.0 ask_qty=200.1 (min required: 100.0)
SUCCESS | 🎯 [MEXC] DEX→CEX ARB FOUND profit=-1.5%
```

---

## Summary

| Aspect | Detail |
|--------|--------|
| **What** | Added minimum liquidity (bid_qty, ask_qty) validation |
| **Why** | Prevent execution with insufficient orderbook depth |
| **Where** | DEX-CEX and CEX-CEX arbitrage checks |
| **Default Min** | 100 units (base currency) |
| **Impact** | Filters thin orderbooks, reduces slippage risk |

