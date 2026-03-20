# Liquidity Quantity Check - Quick Reference

## What Was Changed

Added **minimum liquidity validation** to prevent trading when orderbook depth is too shallow.

### Key Changes

1. **Added `min_qty = 100.0` constant** 
   - Rejects opportunities if `bid_qty < 100` OR `ask_qty < 100`
   - Located: `/home/truong/Desktop/arbitrage/arb-bot/core/dex_cex_arbitrage.py` line 49

2. **New method: `_has_sufficient_liquidity()`**
   - Checks if both bid and ask quantities meet the minimum
   - Usage: `if not self._has_sufficient_liquidity(price_data): return`

3. **Applied to all arbitrage checks:**
   - ✅ DEX-CEX arbitrage (AMIUSDT + APTUSDT prices)
   - ✅ CEX-CEX arbitrage (Bybit ↔ MEXC both checked)

---

## Effect on Bot Behavior

### Before
```
Bot accepts ANY price, even with thin orderbooks
bid_qty=10, ask_qty=50 ✓ AUTO ACCEPT (risky!)
```

### After
```
Bot rejects prices with insufficient depth
bid_qty=10, ask_qty=50 ✗ REJECTED (min_qty=100)
bid_qty=150, ask_qty=200 ✓ ACCEPTED (safe)
```

---

## Configuration

### Adjust Threshold
Edit [core/dex_cex_arbitrage.py](/home/truong/Desktop/arbitrage/arb-bot/core/dex_cex_arbitrage.py) line 49:

```python
self.min_qty = 100.0  # Change this value

# Recommendations:
# 100 = strict (only deep liquidity)
# 50  = moderate (balanced)
# 10  = permissive (include thin books)
# 0   = disabled (not recommended)
```

---

## Run Bot with Changes

```bash
timeout 30 python3 run_integrated_arb.py 2>&1 | grep -E "liquidity|ARB FOUND" | head -20
```

### Expected Log Output

```
DEBUG | Bybit AMIUSDT insufficient liquidity: bid_qty=45.0 ask_qty=150.0 (min required: 100.0)
DEBUG | MEXC AMIUSDT insufficient liquidity: bid_qty=95.0 ask_qty=110.0 (min required: 100.0)
SUCCESS | 🎯 [MEXC] DEX→CEX ARB FOUND ... profit=-1.5%
SUCCESS | 🎯 [BYBIT] CEX→DEX ARB FOUND ... profit=-2.1%
```

---

## Verify Implementation

All unit tests passed ✅:

```
✓ Test 1 - Good liquidity (bid_qty=150, ask_qty=200): True
✓ Test 2 - Thin ask (bid_qty=150, ask_qty=50): False
✓ Test 3 - Thin bid (bid_qty=50, ask_qty=200): False
✓ Test 4 - Custom min=20: True
✓ Test 5 - At threshold (bid_qty=100, ask_qty=100): True
```

---

## Why This Matters

**Problem:** Trading on shallow orderbooks → high slippage → losses

**Solution:** Filter out opportunities where:
- Bid side has < 100 units available (can't sell enough)
- Ask side has < 100 units available (can't buy enough)

**Result:** Only trade on deep, liquid orderbooks → better execution

---

## Technical Details

### Files Modified
- `core/dex_cex_arbitrage.py` (+20 lines of validation code)

### New Code Patterns
```python
# Pattern 1: Simple liquidity check
if not self._has_sufficient_liquidity(price_data):
    return  # Skip if insufficient liquidity

# Pattern 2: Check with custom threshold
is_liquid = self._has_sufficient_liquidity(price_data, min_qty=50)

# Pattern 3: Used in both DEX-CEX and CEX-CEX contexts
# - DEX-CEX: checks both AMIUSDT and APTUSDT
# - CEX-CEX: checks Bybit AND MEXC independently
```

---

## Next Steps

1. **Test with current settings** (min_qty=100)
   ```bash
   timeout 30 python3 run_integrated_arb.py 2>&1 | wc -l
   # Count how many opportunities get filtered
   ```

2. **Tune if needed**
   - If too many get filtered: lower to 50
   - If too few: raise to 150

3. **Monitor liquidity stats**
   - How often do we hit insufficient liquidity?
   - Which exchange is more liquid?

