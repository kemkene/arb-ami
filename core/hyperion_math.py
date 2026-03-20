from decimal import Decimal, getcontext
from typing import Tuple, Optional

# Set precision higher for CLMM math
getcontext().prec = 50

Q64 = Decimal(2**64)

def decode_sqrt_price(sqrt_price_x64: int) -> Decimal:
    """Convert sqrt_price_x64 (uint128/160) to sqrt(P) Decimal."""
    return Decimal(sqrt_price_x64) / Q64

def calculate_amount_out(
    sqrt_price_x64: int,
    liquidity: int,
    amount_in: float,
    fee_rate: float = 0.003,
    is_token0_to_token1: bool = True,
    token0_decimals: int = 8,
    token1_decimals: int = 8
) -> Tuple[float, float]:
    """
    Calculate estimated amount_out and price_impact for a CLMM swap using raw units.
    
    In this pool (Hyperion AMI/APT):
    Token0 = APT (8 decimals)
    Token1 = AMI (8 decimals)
    Price P = AMI / APT (how many AMI per 1 APT)
    
    is_token0_to_token1 = True  => APT -> AMI
    is_token0_to_token1 = False => AMI -> APT
    """
    if amount_in <= 0 or liquidity <= 0:
        return 0.0, 0.0

    # 1. Convert everything to raw units (octas)
    decimals_in = token0_decimals if is_token0_to_token1 else token1_decimals
    decimals_out = token1_decimals if is_token0_to_token1 else token0_decimals
    
    amount_in_raw = Decimal(str(amount_in)) * (Decimal(10) ** decimals_in)
    L = Decimal(liquidity)
    sqrt_P_old = decode_sqrt_price(sqrt_price_x64)
    
    # 2. Net amount after fee
    amount_in_net = amount_in_raw * (Decimal(1) - Decimal(str(fee_rate)))
    
    if is_token0_to_token1:
        # APT (0) -> AMI (1) | x -> y
        # 1/sqrt_P_new = 1/sqrt_P_old + delta_x / L
        # delta_y = L * (sqrt_P_old - sqrt_P_new)
        delta_x = amount_in_net
        # Using Decimal for high precision
        one_over_sqrt_p_new = (Decimal(1) / sqrt_P_old) + (delta_x / L)
        sqrt_P_new = Decimal(1) / one_over_sqrt_p_new
        delta_y = L * (sqrt_P_old - sqrt_P_new)
        amount_out_raw = delta_y
    else:
        # AMI (1) -> APT (0) | y -> x
        # sqrt_P_new = sqrt_P_old + delta_y / L
        # delta_x = L * (1/sqrt_P_old - 1/sqrt_P_new)
        delta_y = amount_in_net
        sqrt_P_new = sqrt_P_old + (delta_y / L)
        delta_x = L * ((Decimal(1) / sqrt_P_old) - (Decimal(1) / sqrt_P_new))
        amount_out_raw = delta_x

    # 3. Convert back to human units
    amount_out = float(amount_out_raw / (Decimal(10) ** decimals_out))

    # 4. Price Impact calculation
    # spot_price = P = y/x = AMI/APT
    spot_price = float(sqrt_P_old * sqrt_P_old)
    if amount_out > 0:
        # exec_price = delta_y / delta_x = AMI / APT
        if is_token0_to_token1:
            # APT -> AMI: input is APT (delta_x), output is AMI (delta_y)
            # Wait, our logic above swapped delta_x and delta_y meanings. Let's fix.
            # Let's use simpler: amount_in is fixed, amount_out is fixed.
            # exec_price (AMI/APT) = AMI_amount / APT_amount
            # Direction APT(0) -> AMI(1): AMI is out, APT is in
            exec_price = float(amount_out / amount_in)
        else:
            # AMI(1) -> APT(0): AMI is in, APT is out
            exec_price = float(amount_in / amount_out)
            
        price_impact = abs((exec_price - spot_price) / spot_price) * 100
    else:
        price_impact = 0.0

    return amount_out, price_impact

if __name__ == "__main__":
    # Test with user example
    # 100 AMI, fee 0.3%, decimals 8/8
    # Let's assume some dummy sqrt_price and liquidity if not provided
    test_sqrt_price_x96 = 79228162514264337593543950336 # ~1
    test_liquidity = 10**12
    
    out, impact = calculate_amount_out(test_sqrt_price_x96, test_liquidity, 100.0)
    print(f"Swap 100 AMI -> {out} APT, Impact: {impact}%")
