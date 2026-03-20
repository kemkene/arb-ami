# Phân Tích Các Cycle Arbitrage DEX-CEX

## 📊 Giả Định Giá Hiện Tại
```
Cellana DEX (AMI/APT pool):
- Reserves: 100,000,000 AMI / 800,000 APT
- Spot price: 1 APT = 125 AMI (hoặc 1 AMI = 0.008 APT)
- Fee: 0.1%

CEX (Bybit/MEXC):
- AMIUSDT: bid=0.00807 ask=0.00813 mid=0.00810
- APTUSDT: bid=0.9490 ask=0.9510 mid=0.9500
- Fee: 0.1% mỗi giao dịch

Implied CEX rate:
- 1 APT = 0.9500/0.00810 = 117.28 AMI (tính theo mid)
- 1 APT = 0.9490/0.00813 = 116.73 AMI (worst case: APT bid / AMI ask)
- 1 APT = 0.9510/0.00807 = 117.84 AMI (worst case: APT ask / AMI bid)
```

**☀️ Phân tích:** DEX có AMI **ĐẮT HƠN** CEX (125 AMI/APT vs 117 AMI/APT)
→ Arbitrage direction: **BUY AMI on CEX, SELL AMI on DEX**

---

## ✅ CYCLE 1: CEX→DEX (Code Direction B)
**Route:** USDT → AMI (CEX) → APT (DEX) → USDT (CEX implied)

### Bước 1: Buy AMI on CEX
```
Capital: 1000 USDT
Buy AMI at ask = 0.00813 USDT
Fee: 0.1%
AMI bought = (1000 / 0.00813) × (1 - 0.001) = 122,879 AMI
```

### Bước 2: Swap AMI → APT on Cellana DEX
```
AMI in: 122,879
Formula: apt_out = (ami_in × 0.999 × reserve_apt) / (reserve_ami + ami_in × 0.999)
APT out = (122,879 × 0.999 × 800,000) / (100,000,000 + 122,879 × 0.999)
        = 98,116,172,000 / 100,122,761
        = 979.80 APT
```

### Bước 3: Sell APT on CEX (implied)
```
APT amount: 979.80
Sell at bid = 0.9490 USDT
Fee: 0.1%
USDT received = 979.80 × 0.9490 × (1 - 0.001) = 929.04 × 0.999 = 928.11 USDT
```

### Kết quả:
```
Start: 1000 USDT
End: 928.11 USDT
Loss: -71.89 USDT ❌
```

**⚠️ Không profitable vì DEX có AMI đắt hơn CEX!**

---

## ✅ CYCLE 2: DEX→CEX (Code Direction A)
**Route:** APT → AMI (DEX) → USDT (CEX)

### Bước 1: Get APT equivalent of notional
```
Capital: 1000 USDT
APT price: 0.9500 USDT (mid)
APT to spend = 1000 / 0.9500 = 1052.63 APT
```

### Bước 2: Swap APT → AMI on Cellana DEX
```
APT in: 1052.63
Formula: ami_out = (apt_in × 0.999 × reserve_ami) / (reserve_apt + apt_in × 0.999)
AMI out = (1052.63 × 0.999 × 100,000,000) / (800,000 + 1052.63 × 0.999)
        = 105,157,473,700 / 801,051.58
        = 131,273 AMI
```

### Bước 3: Sell AMI on CEX
```
AMI amount: 131,273
Sell at bid = 0.00807 USDT
Fee: 0.1%
USDT received = 131,273 × 0.00807 × (1 - 0.001) = 1059.37 × 0.999 = 1058.31 USDT
```

### Kết quả:
```
Start: 1000 USDT (as APT equivalent)
End: 1058.31 USDT
Profit: +58.31 USDT ✅
ROI: 5.83%
```

**🎉 PROFITABLE! Đây là cycle được code Direction A cover.**

---

## ✅ CYCLE 3: Circular APT→DEX→CEX→APT
**Route:** APT → AMI (DEX) → USDT (CEX) → APT (CEX)

### Bước 1: Start with APT
```
Capital: 1000 APT
```

### Bước 2: Swap APT → AMI on Cellana DEX
```
APT in: 1000
AMI out = (1000 × 0.999 × 100,000,000) / (800,000 + 1000 × 0.999)
        = 99,900,000,000 / 800,999
        = 124,719 AMI
```

### Bước 3: Sell AMI on CEX for USDT
```
AMI: 124,719
Sell at bid = 0.00807 USDT
Fee: 0.1%
USDT = 124,719 × 0.00807 × 0.999 = 1006.48 × 0.999 = 1005.47 USDT
```

### Bước 4: Buy APT on CEX with USDT
```
USDT: 1005.47
Buy APT at ask = 0.9510 USDT
Fee: 0.1%
APT = (1005.47 / 0.9510) × 0.999 = 1057.31 × 0.999 = 1056.25 APT
```

### Kết quả:
```
Start: 1000 APT
End: 1056.25 APT
Profit: +56.25 APT ✅
ROI: 5.625%
```

**🎉 PROFITABLE! Nhưng code hiện tại KHÔNG track cycle này (không buy APT back).**

---

## ✅ CYCLE 4: Circular APT→CEX→DEX→APT
**Route:** APT → USDT (CEX) → AMI (CEX) → APT (DEX)

### Bước 1: Start with APT
```
Capital: 1000 APT
```

### Bước 2: Sell APT on CEX for USDT
```
APT: 1000
Sell at bid = 0.9490 USDT
Fee: 0.1%
USDT = 1000 × 0.9490 × 0.999 = 948.05 USDT
```

### Bước 3: Buy AMI on CEX with USDT
```
USDT: 948.05
Buy AMI at ask = 0.00813 USDT
Fee: 0.1%
AMI = (948.05 / 0.00813) × 0.999 = 116,605 × 0.999 = 116,488 AMI
```

### Bước 4: Swap AMI → APT on Cellana DEX
```
AMI in: 116,488
APT out = (116,488 × 0.999 × 800,000) / (100,000,000 + 116,488 × 0.999)
        = 93,013,824,000 / 100,116,372
        = 928.93 APT
```

### Kết quả:
```
Start: 1000 APT
End: 928.93 APT
Loss: -71.07 APT ❌
```

**⚠️ Không profitable - đây là chiều ngược lại của Cycle 3.**

---

## ✅ CYCLE 5: Circular AMI→DEX→CEX→AMI
**Route:** AMI → APT (DEX) → USDT (CEX) → AMI (CEX)

### Bước 1: Start with AMI
```
Capital: 125,000 AMI
```

### Bước 2: Swap AMI → APT on Cellana DEX
```
AMI in: 125,000
APT out = (125,000 × 0.999 × 800,000) / (100,000,000 + 125,000 × 0.999)
        = 99,900,000,000 / 100,124,875
        = 997.52 APT
```

### Bước 3: Sell APT on CEX for USDT
```
APT: 997.52
Sell at bid = 0.9490 USDT
Fee: 0.1%
USDT = 997.52 × 0.9490 × 0.999 = 946.64 × 0.999 = 945.69 USDT
```

### Bước 4: Buy AMI on CEX with USDT
```
USDT: 945.69
Buy AMI at ask = 0.00813 USDT
Fee: 0.1%
AMI = (945.69 / 0.00813) × 0.999 = 116,313 × 0.999 = 116,197 AMI
```

### Kết quả:
```
Start: 125,000 AMI
End: 116,197 AMI
Loss: -8,803 AMI ❌
```

**⚠️ Không profitable - đây là chiều ngược lại.**

---

## ✅ CYCLE 6: Circular AMI→CEX→DEX→AMI
**Route:** AMI → USDT (CEX) → APT (CEX) → AMI (DEX)

### Bước 1: Start with AMI
```
Capital: 125,000 AMI
```

### Bước 2: Sell AMI on CEX for USDT
```
AMI: 125,000
Sell at bid = 0.00807 USDT
Fee: 0.1%
USDT = 125,000 × 0.00807 × 0.999 = 1008.75 × 0.999 = 1007.74 USDT
```

### Bước 3: Buy APT on CEX with USDT
```
USDT: 1007.74
Buy APT at ask = 0.9510 USDT
Fee: 0.1%
APT = (1007.74 / 0.9510) × 0.999 = 1059.75 × 0.999 = 1058.69 APT
```

### Bước 4: Swap APT → AMI on Cellana DEX
```
APT in: 1058.69
AMI out = (1058.69 × 0.999 × 100,000,000) / (800,000 + 1058.69 × 0.999)
        = 105,762,393,100 / 801,057.63
        = 132,032 AMI
```

### Kết quả:
```
Start: 125,000 AMI
End: 132,032 AMI
Profit: +7,032 AMI ✅
ROI: 5.63%
```

**🎉 PROFITABLE! Nhưng code hiện tại KHÔNG track cycle này.**

---

## 📋 TỔNG KẾT: Cycles Nào Có Profit?

### ✅ Profitable Cycles (với giá giả định hiện tại):
1. **Cycle 2** (DEX→CEX): APT → AMI (DEX) → USDT (CEX)  
   Profit: **+58.31 USDT** (5.83%)  
   **✅ Code Direction A covers this**

2. **Cycle 3** (Circular APT): APT → AMI (DEX) → USDT (CEX) → APT (CEX)  
   Profit: **+56.25 APT** (5.625%)  
   **❌ Code does NOT track this (không buy APT back)**

3. **Cycle 6** (Circular AMI): AMI → USDT (CEX) → APT (CEX) → AMI (DEX)  
   Profit: **+7,032 AMI** (5.63%)  
   **❌ Code does NOT track this (không start với AMI)**

### ❌ Unprofitable Cycles:
- **Cycle 1** (CEX→DEX): Loss -71.89 USDT
- **Cycle 4** (Circular APT reverse): Loss -71.07 APT
- **Cycle 5** (Circular AMI reverse): Loss -8,803 AMI

---

## 🧠 Phân Tích Logic

### Rule của thumb:
**Khi DEX price > CEX price** (AMI đắt hơn trên DEX):
- ✅ BUY trên CEX (rẻ) → SELL trên DEX (đắt) → **PROFIT**
- ❌ BUY trên DEX (đắt) → SELL trên CEX (rẻ) → **LOSS**

### Tại sao Cycle 3 và Cycle 6 profitable nhưng code không cover?
Code hiện tại chỉ so sánh **USDT input vs USDT output**:
- Direction A: Tính profit bằng cách so sánh USDT cost của APT vs USDT nhận được
- Direction B: Tính profit bằng cách so sánh USDT đầu vào vs USDT cuối cùng

Nhưng code KHÔNG:
- Complete circular cycle (APT → ... → APT hoặc AMI → ... → AMI)
- Track profit denominated in crypto assets (APT profit, AMI profit)
- Execute the final CEX leg to close the circle

### Về mặt kinh tế, Cycle 2 vs Cycle 3 là TƯƠNG ĐƯƠNG:
- Cycle 2: 1000 USDT → 1058 USDT (+58 USDT)
- Cycle 3: 1000 APT → 1056 APT (+56 APT ≈ +53 USDT at 0.95 $/APT)

Sự khác biệt nhỏ do:
1. Cycle 3 có thêm 1 CEX trade (more fees)
2. Cycle 2 dùng mid price cho APT, Cycle 3 dùng bid/ask

---

## 🎯 Recommendation: Extend Code

### Để track tất cả profitable cycles, cần add:

#### Option 1: Add circular arbitrage tracking
```python
def _check_circular_arbitrage_apt():
    """
    Cycle: APT → AMI (DEX) → USDT (CEX) → APT (CEX)
    Track profit in APT terms
    """
    apt_start = 1000
    
    # Step 1: APT → AMI on DEX
    ami_out = amm_out(apt_start, reserves_apt, reserves_ami, 0.001)
    
    # Step 2: AMI → USDT on CEX
    usdt_out = ami_out * ami_bid * (1 - cex_fee)
    
    # Step 3: USDT → APT on CEX
    apt_end = (usdt_out / apt_ask) * (1 - cex_fee)
    
    profit_apt = apt_end - apt_start
    profit_usdt = profit_apt * apt_mid
    
    if profit_usdt > min_profit:
        log_opportunity(...)

def _check_circular_arbitrage_ami():
    """
    Cycle: AMI → USDT (CEX) → APT (CEX) → AMI (DEX)
    Track profit in AMI terms
    """
    ami_start = 125000
    
    # Step 1: AMI → USDT on CEX
    usdt_out = ami_start * ami_bid * (1 - cex_fee)
    
    # Step 2: USDT → APT on CEX
    apt_out = (usdt_out / apt_ask) * (1 - cex_fee)
    
    # Step 3: APT → AMI on DEX
    ami_end = amm_out(apt_out, reserves_apt, reserves_ami, 0.001)
    
    profit_ami = ami_end - ami_start
    profit_usdt = profit_ami * ami_mid
    
    if profit_usdt > min_profit:
        log_opportunity(...)
```

#### Option 2: Keep current approach (RECOMMENDED)
Current code **ĐÃ ĐỦ TỐT** vì:
1. **Hiệu quả execution**: Circular arbitrage cần 4 trades, current code chỉ cần 2-3 trades
2. **Lower fees**: Ít trades hơn = ít fees hơn
3. **Lower latency**: Ít hops hơn = nhanh hơn
4. **Same economic outcome**: Profit về cơ bản là tương đương

---

## 📊 ALL PROFITABLE CASES (Danh Sách Đầy Đủ)

Given **DEX price > CEX price** (current market state):

### Primary Profitable Direction:
1. ✅ **DEX→CEX** (Direction A - covered)
   - APT → AMI (DEX) → USDT (CEX)
   - Profit: ~5.8% in USDT terms

2. ✅ **Circular APT** (not covered)
   - APT → AMI (DEX) → USDT (CEX) → APT (CEX)
   - Profit: ~5.6% in APT terms

3. ✅ **Circular AMI** (not covered)
   - AMI → USDT (CEX) → APT (CEX) → AMI (DEX)
   - Profit: ~5.6% in AMI terms

### If Market Flips (CEX price > DEX price):
4. ✅ **CEX→DEX** (Direction B - covered)
   - USDT → AMI (CEX) → APT (DEX) → USDT (CEX)
   - Would be profitable if prices flip

5. ✅ **Circular APT reverse**
   - APT → USDT (CEX) → AMI (CEX) → APT (DEX)
   - Would be profitable if prices flip

6. ✅ **Circular AMI reverse**
   - AMI → APT (DEX) → USDT (CEX) → AMI (CEX)
   - Would be profitable if prices flip

**Total: 6 possible cycles, current code covers 2 main directions (both profit calculations in USDT).**
