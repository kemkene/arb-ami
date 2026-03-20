import asyncio
import math
from decimal import Decimal
from core.hyperion_dex_swap import HyperionDexSwap, _to_octas, _from_octas
from core.hyperion_math import calculate_amount_out
from config.settings import settings

async def compare_quotes():
    hyp = HyperionDexSwap()
    print("--- Hyperion Live Math Verification (Corrected Q64) ---")
    
    # Dữ liệu thực từ Resource pool Hyperion chốt tại thời điểm kiểm tra
    live_liquidity = 59240939406259
    
    # 1. Lấy On-chain Quote (Swap 100 AMI)
    amount_ami = 100.0
    amount_in_raw = _to_octas(amount_ami, 8)
    
    print(f"Swap {amount_ami} AMI -> APT (On-chain)")
    onchain_raw, _ = await hyp.get_amount_out_onchain(
        settings.ami_token_address, 
        amount_in_raw
    )
    onchain_out = _from_octas(onchain_raw, 8)
    print(f"  => ON-CHAIN RESULT: {onchain_out:.8f} APT")
    
    # 2. Lấy sqrt_price từ view function realtime
    tick, sqrt_p_view = await hyp.get_current_tick_and_price()
    print(f"  - Current SqrtPrice (view): {sqrt_p_view}")
    
    # 3. Tính toán Offline sử dụng Q64 logic
    # AMI (1) -> APT (0) is is_token0_to_token1=False in our new orientation
    out_math, impact = calculate_amount_out(
        sqrt_p_view, 
        live_liquidity, 
        amount_ami, 
        fee_rate=0.001, 
        is_token0_to_token1=False # AMI In, APT Out
    )
    
    print(f"  - Offline Math Result: {out_math:.8f} APT (Impact: {impact:.4f}%)")
    diff = abs(onchain_out - out_math)
    print(f"  - Difference: {diff:.8f}")
    
    if diff < 0.0001:
        print("\n✅ MATCH! Offline calculation is consistent with On-chain.")
    else:
        print("\n❌ STILL MISMATCH. Investigating orientation...")
        # Thử hướng ngược lại phòng trường hợp vẫn nhầm
        out_alt, _ = calculate_amount_out(
            sqrt_p_view, live_liquidity, amount_ami, fee_rate=0.001, is_token0_to_token1=True
        )
        print(f"  - Alternative direction result: {out_alt:.8f}")

    await hyp.close()

if __name__ == "__main__":
    asyncio.run(compare_quotes())
