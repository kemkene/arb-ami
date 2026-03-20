import asyncio
import os
from core.hyperion_dex_swap import HyperionDexSwap, _to_octas, _from_octas
from core.hyperion_math import calculate_amount_out
from config.settings import settings

async def check_quote():
    print("--- Hyperion Quote Check ---")
    
    try:
        hyp = HyperionDexSwap()
        
        # Ví dụ: Swap 100 AMI -> ? APT
        amount_ami = 100.0
        amount_in_raw = _to_octas(amount_ami, 8)
        
        print(f"\n1. Đang lấy quote ON-CHAIN cho {amount_ami} AMI...")
        try:
            # AMI là token0 (token_in_metadata = settings.ami_token_address)
            out_raw, fee_raw = await hyp.get_amount_out_onchain(
                settings.ami_token_address, 
                amount_in_raw
            )
            out_apt = _from_octas(out_raw, 8)
            fee_apt = _from_octas(fee_raw, 8)
            
            print(f"   => Kết quả on-chain: {amount_ami} AMI -> {out_apt:.8f} APT")
            print(f"   => Phí dự kiến: {fee_apt:.8f} APT")
        except Exception as e:
            print(f"   => Lỗi lấy quote on-chain (có thể do node/network): {e}")

        print("\n2. Đang tính toán OFFLINE (Math Logic) dựa trên mẫu...")
        # Giả sử giá sqrt_price và liquidity mẫu (hoặc lấy từ listener nếu có)
        # Thông thường sqrt_price_x96 cho price ~ 0.05 APT/AMI (giả định)
        # 1 AMI = 0.05 APT -> sqrt(0.05) * 2^96
        import math
        sample_price = 0.05 
        sample_sqrt_p = int(math.sqrt(sample_price) * (2**96))
        sample_liq = 200000000000 # 200e9
        
        out_math, impact = calculate_amount_out(
            sample_sqrt_p, 
            sample_liq, 
            amount_ami, 
            fee_rate=0.001, # 0.1%
            is_token0_to_token1=True
        )
        print(f"   => Giả định giá spot: {sample_price} APT/AMI")
        print(f"   => Kết quả tính toán Math: {amount_ami} AMI -> {out_math:.8f} APT")
        print(f"   => Price Impact: {impact:.4f}%")
        
        await hyp.close()
        
    except Exception as e:
        print(f"Lỗi khởi tạo: {e}")

if __name__ == "__main__":
    asyncio.run(check_quote())
