import asyncio
import sys
import os

# Thêm project root vào sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.trade_executor import LegResult, TradeLeg, LegSide, ExecutionResult

def test_profit_calculation():
    print("Testing Real Profit Calculation Logic...")
    
    # Giả lập 2 legs của CEX-CEX arb
    # Chân 1: Buy 1000 AMI trên Bybit @ 0.0075
    leg1 = TradeLeg(exchange="bybit", symbol="AMIUSDT", side=LegSide.BUY, qty=1000, price_est=0.0075)
    res1 = LegResult(leg=leg1, ok=True, filled_qty=1000, filled_price=0.0075)
    
    # Chân 2: Sell 1000 AMI trên MEXC @ 0.0077
    leg2 = TradeLeg(exchange="mexc", symbol="AMIUSDT", side=LegSide.SELL, qty=1000, price_est=0.0077)
    res2 = LegResult(leg=leg2, ok=True, filled_qty=1000, filled_price=0.0077)
    
    succeeded = [res1, res2]
    
    # Logic tính profit (copy từ trade_executor.py mới cập nhật)
    real_profit_usdt = 0.0
    for lr in succeeded:
        if not lr.leg.is_dex:
            if lr.leg.side == LegSide.SELL:
                real_profit_usdt += (lr.filled_price * lr.filled_qty)
            else:
                real_profit_usdt -= (lr.filled_price * lr.filled_qty)
    
    # Dự kiến: - (0.0075 * 1000) + (0.0077 * 1000) = -7.5 + 7.7 = 0.2
    print(f"Calculated Profit: ${real_profit_usdt:.4f}")
    expected = 0.2
    
    if abs(real_profit_usdt - expected) < 0.0001:
        print("✅ Profit calculation is CORRECT.")
    else:
        print(f"❌ Profit calculation is WRONG. Expected ${expected:.4f}, got ${real_profit_usdt:.4f}")

if __name__ == "__main__":
    test_profit_calculation()
