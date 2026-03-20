# 🔧 Quick Implementation Guide

## Test 1: Chạy với TRADE_AMOUNT_USDT=100 (5 phút)

### Lý do
- Hiện tại: 1000 USDT → 2% slippage
- Thử: 100 USDT → 0.2% slippage
- Nếu spread 0.32% - fees 0.05% = +0.27%, có thể sẽ thấy positive profit

### Lệnh
```bash
cd /home/truong/Desktop/arbitrage/arb-bot

# Backup current .env
cp .env .env.backup.1000usdt

# Edit .env
sed -i 's/TRADE_AMOUNT_USDT=1000/TRADE_AMOUNT_USDT=100/g' .env

# Verify
grep TRADE_AMOUNT_USDT .env

# Run 30 seconds
timeout 30 python3 run_integrated_arb.py 2>&1 | tee test_100usdt.log

# Check results
echo "=== NEGATIVE PROFITS ==="
grep "profit=-" test_100usdt.log | wc -l

echo "=== POSITIVE PROFITS ==="
grep "profit=" test_100usdt.log | grep -E "profit=0\.[0-9]|profit=[1-9]" | wc -l

echo "=== Sample opportunities ==="
grep -o "profit=-*[0-9.]*%" test_100usdt.log | sort -u | head -20
```

### Expected output
```
=== NEGATIVE PROFITS ===
45

=== POSITIVE PROFITS ===
5-15  (hopefully!)

=== Sample opportunities ===
profit=-0.15%
profit=-0.08%
profit=0.02%
profit=0.15%
...
```

---

## Test 2: Chạy với MIN_PROFIT_THRESHOLD=-0.3%

### Lý do
- Capture cơ hội gần break-even
- Có thể trong execution, slippage nhỏ hơn simulation

### Lệnh
```bash
# Set threshold to -0.3%
sed -i 's/MIN_PROFIT_THRESHOLD=.*/MIN_PROFIT_THRESHOLD=-0.3/g' .env

# Verify
grep MIN_PROFIT_THRESHOLD .env

# Run
timeout 30 python3 run_integrated_arb.py 2>&1 | tee test_threshold_neg03.log

# Check what got logged
echo "=== Opportunities captured ==="
jq '.profit_pct' logs/signals.jsonl 2>/dev/null | tail -20
```

---

## Test 3: Explore Kích Thước Tối Ưu (Size Sweep)

```python
# sizes_to_test.py
import subprocess
import json
import re

SIZES = [10, 25, 50, 100, 250, 500, 750, 1000, 1500]
RESULTS = []

for size in SIZES:
    print(f"\n{'='*60}")
    print(f"Testing TRADE_AMOUNT_USDT = {size}")
    print(f"{'='*60}")
    
    # Update .env
    subprocess.run(
        f"sed -i 's/TRADE_AMOUNT_USDT=.*/TRADE_AMOUNT_USDT={size}/g' .env",
        shell=True
    )
    
    # Run bot for 20 seconds
    result = subprocess.run(
        "timeout 20 python3 run_integrated_arb.py",
        shell=True,
        capture_output=True,
        text=True
    )
    
    # Count opportunities
    log_output = result.stderr + result.stdout
    opportunities = re.findall(r'profit=(-?\d+\.\d+)%', log_output)
    
    if opportunities:
        avg_profit = sum(float(x) for x in opportunities) / len(opportunities)
        positive_count = sum(1 for x in opportunities if float(x) > 0)
        
        RESULTS.append({
            "size": size,
            "total_opps": len(opportunities),
            "positive_opps": positive_count,
            "avg_profit_pct": round(avg_profit, 3)
        })
        
        print(f"Opportunities: {len(opportunities)}")
        print(f"Positive: {positive_count}")
        print(f"Avg profit: {avg_profit:.3f}%")
        print(f"Best: {max(float(x) for x in opportunities):.3f}%")
        print(f"Worst: {min(float(x) for x in opportunities):.3f}%")

print("\n" + "="*60)
print("SUMMARY")
print("="*60)
for r in RESULTS:
    print(f"Size {r['size']:>4} USDT: {r['positive_opps']:>3} positive / {r['total_opps']:>3} total | Avg: {r['avg_profit_pct']:>6.2f}%")

# Find optimal size
if RESULTS:
    best = max(RESULTS, key=lambda x: x['avg_profit_pct'])
    print(f"\n✓ OPTIMAL SIZE: {best['size']} USDT (avg profit: {best['avg_profit_pct']:.3f}%)")
```

### Run
```bash
python3 sizes_to_test.py
```

### Output example
```
SUMMARY
Size   10 USDT:   8 positive /  65 total | Avg:  +0.12%
Size   25 USDT:   6 positive /  62 total | Avg:  +0.08%
Size   50 USDT:   4 positive /  59 total | Avg:  +0.02%
Size  100 USDT:   2 positive /  56 total | Avg:  -0.05%
Size  250 USDT:   0 positive /  53 total | Avg:  -0.18%
Size  500 USDT:   0 positive /  52 total | Avg:  -0.35%
Size 1000 USDT:   0 positive /  50 total | Avg:  -0.52%

✓ OPTIMAL SIZE: 10 USDT (avg profit: +0.12%)
```

---

## Test 4: Chạy Bot Lâu Dài (24h Monitoring)

### Setup
```bash
# Restore best configuration from tests above
# Example: if size=10 gave +0.12% avg profit
sed -i 's/TRADE_AMOUNT_USDT=.*/TRADE_AMOUNT_USDT=10/g' .env
sed -i 's/MIN_PROFIT_THRESHOLD=.*/MIN_PROFIT_THRESHOLD=0/g' .env

# Start bot in background
nohup python3 run_integrated_arb.py > bot_24h.log 2>&1 &

# Monitor
tail -f bot_24h.log

# After 1 hour, check opportunities
echo "=== Hour 1 Results ==="
jq '.profit_pct' logs/signals.jsonl | \
  awk '{sum+=$1; count++} END {print "Total: " count ", Avg: " sum/count "%"}'

# After 24h, stop and analyze
# pkill -f 'python3 run_integrated_arb.py'
```

---

## Test 5: Verify Fee Calculations (Debug)

### What to check
```python
# logs/arb_bot_*.log - look for:
# 1. MEXC fee values
grep "MEXC.*Fees updated" bot.log
# Expected: "maker=0.0000% taker=0.0500%"

# 2. Bybit fee values
grep "Bybit.*Fees updated" bot.log
# Expected: "maker=0.1000% taker=0.1000%"

# 3. Price spreads (every 5 seconds)
grep "PRICES.*DEX_AMI/APT" bot.log | head -5
# Expected: spread between DEX and CEX prices

# 4. Opportunity profits
grep -E "(DEX→CEX|CEX→DEX|CYCLE|CEX-CEX).*profit=" bot.log | head -20
# Check if spread is positive or negative
```

### Debug script
```python
# debug_profits.py
import re
import json

with open('bot.log', 'r') as f:
    lines = f.readlines()

# Extract opportunities
for line in lines[-1000:]:  # last 1000 lines
    if 'profit=' in line:
        match = re.search(r'profit=(-?\d+\.?\d*)%', line)
        if match:
            profit = float(match.group(1))
            if profit > -1:  # Show near break-even
                print(f"[{profit:>7.2f}%] {line.strip()[:120]}")
```

---

## Configuration Checklist

Before running, verify:

```bash
# 1. .env values
grep -E "TRADE_AMOUNT_USDT|MIN_PROFIT_THRESHOLD|BYBIT_FEE|MEXC_FEE" .env

# Example output should be:
# TRADE_AMOUNT_USDT=10          # <-- ADJUST THIS
# MIN_PROFIT_THRESHOLD=0         # <-- or -0.3
# BYBIT_FEE=0.001              # <-- should be 0.001 (0.1%)
# MEXC_FEE=0.001               # <-- should be 0.001 (0.1%)

# 2. API connectivity
python3 -c "
import asyncio
from exchanges.bybit import BybitWS
from exchanges.mexc import MexcWS

async def test():
    # Test Bybit
    bybit = BybitWS()
    await bybit.connect()
    print('✓ Bybit connected')
    await bybit.close()
    
    # Test MEXC
    mexc = MexcWS()
    await mexc.connect()
    print('✓ MEXC connected')
    await mexc.close()

asyncio.run(test())
"

# 3. Cellana listener
python3 -c "
import asyncio
from core.cellana_swap_listener import CellanaSwapListener

async def test():
    listener = CellanaSwapListener()
    try:
        await asyncio.wait_for(listener.run(), timeout=5)
    except asyncio.TimeoutError:
        print('✓ Cellana stream connected')
    except:
        print('✗ Cellana stream failed')

asyncio.run(test())
"
```

---

## Expected Timeline

| Timeline | Action | Expected Outcome |
|----------|--------|------------------|
| **T+5min** | Run test with TRADE_AMOUNT_USDT=100 | See if any positive profits appear |
| **T+15min** | Run size sweep | Find optimal trade size |
| **T+30min** | Apply optimal config | Update .env with best size |
| **T+2h** | Monitor 1 hour with new config | See if consistent profitability |
| **T+24h** | Full soak test | Verify sustained performance |

---

## Troubleshooting

### Q: Still seeing only negative profits?
**A:** Try even smaller sizes (5, 2, 1 USDT). Or check if fees are higher than expected:
```bash
# Verify actual exchange fees
curl -X GET "https://api.bybit.com/v5/account/fee-rate?symbol=AMIUSDT" \
  -H "X-BAPI-API-KEY: $BYBIT_API_KEY" \
  # ... (add proper signing)

# If fees > 0.1%, that's problem #1
```

### Q: Positive profits show in logs but not triggering execution?
**A:** Check `DRY_RUN` setting:
```bash
grep DRY_RUN .env
# Should be: DRY_RUN=true for testing
# Change to DRY_RUN=false to execute (CAREFUL!)
```

### Q: All opportunities negative consistently?
**A:** Market timing issue. Try:
1. Different time of day (crypto more active 00:00-08:00 UTC)
2. Different DEX pools (add Cetus listener)
3. Different token pairs (not just AMI/APT)
