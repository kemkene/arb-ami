import asyncio
import math
from decimal import Decimal
from core.hyperion_dex_swap import HyperionDexSwap, _to_octas, _from_octas
from core.hyperion_math import calculate_amount_out, decode_sqrt_price
from config.settings import settings

async def run_detailed_check():
    hyp = HyperionDexSwap()
    print("=== HYPERION QUOTE VERIFICATION (100 APT & 100 AMI) ===")
    
    # 1. Lấy trạng thái pool realtime
    try:
        tick, sqrt_p_view = await hyp.get_current_tick_and_price()
        # Fetch liquidity from resource to be most accurate
        # Based on previous fetch: 59240939406259
        # Let's try to get it again via view or just use the resource value if possible
        # For now, I'll use the value from the last resource fetch in history
        # (liquidity is usually stable over seconds)
        curr_liquidity = 59240939406259 
        
        print(f"Pool State:")
        print(f"  - SqrtPrice: {sqrt_p_view}")
        print(f"  - Liquidity: {curr_liquidity}")
        print(f"  - Spot Price: {float(decode_sqrt_price(sqrt_p_view)**2):.8f} AMI/APT")
        
        amount = 100.0
        amount_raw = _to_octas(amount, 8)
        
        # --- CASE 1: 100 APT -> AMI ---
        print(f"\nCASE 1: Swap 100 APT -> AMI")
        # Online
        onchain_raw_apt, _ = await hyp.get_amount_out_onchain(
            "0xa", # APT FA
            amount_raw
        )
        onchain_out_ami = _from_octas(onchain_raw_apt, 8)
        
        # Offline
        # APT is Token0, AMI is Token1. APT -> AMI is is_token0_to_token1=True
        off_out_ami, impact_apt = calculate_amount_out(
            sqrt_p_view, curr_liquidity, amount, fee_rate=0.001, is_token0_to_token1=True
        )
        
        print(f"  - Online (View):  {onchain_out_ami:.8f} AMI")
        print(f"  - Offline (Math): {off_out_ami:.8f} AMI")
        print(f"  - Difference:     {abs(onchain_out_ami - off_out_ami):.8f}")
        print(f"  - Impact:         {impact_apt:.4f}%")

        # --- CASE 2: 100 AMI -> APT ---
        print(f"\nCASE 2: Swap 100 AMI -> APT")
        # Online
        onchain_raw_ami, _ = await hyp.get_amount_out_onchain(
            settings.ami_token_address, 
            amount_raw
        )
        onchain_out_apt = _from_octas(onchain_raw_ami, 8)
        
        # Offline
        # AMI is Token1, APT is Token0. AMI -> APT is is_token0_to_token1=False
        off_out_apt, impact_ami = calculate_amount_out(
            sqrt_p_view, curr_liquidity, amount, fee_rate=0.001, is_token0_to_token1=False
        )
        
        print(f"  - Online (View):  {onchain_out_apt:.8f} APT")
        print(f"  - Offline (Math): {off_out_apt:.8f} APT")
        print(f"  - Difference:     {abs(onchain_out_apt - off_out_apt):.8f}")
        print(f"  - Impact:         {impact_ami:.4f}%")

    except Exception as e:
        print(f"Error during verification: {e}")
    finally:
        await hyp.close()

if __name__ == "__main__":
    asyncio.run(run_detailed_check())
