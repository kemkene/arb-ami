import math
from decimal import Decimal

def amm_out(amount_in, reserve_in, reserve_out, fee_rate):
    if amount_in <= 0: return 0.0
    amount_in_after_fee = amount_in * (1.0 - fee_rate)
    numerator = amount_in_after_fee * reserve_out
    denominator = reserve_in + amount_in_after_fee
    return numerator / denominator

def calculate_hyp_out(sqrt_price_x64, liquidity, amount_in, fee_rate, is_token0_to_token1):
    # Simplified CLMM math for verification
    Q64 = 2**64
    sqrt_P_old = Decimal(sqrt_price_x64) / Decimal(Q64)
    amount_in_net = Decimal(str(amount_in)) * (Decimal(1) - Decimal(str(fee_rate)))
    L = Decimal(liquidity)
    
    # Octas adjustment
    amount_in_raw = amount_in_net * (Decimal(10)**8)
    
    if is_token0_to_token1:
        # APT(0) -> AMI(1)
        # delta_y = L * (sqrt_P_new - sqrt_P_old) ... wait.
        # Use calculate_amount_out logic: sqrt_P_new = sqrt_P_old + (delta_y / L)
        # No, in Hyperion APT is Token0, AMI is Token1. 
        # Price P = AMI / APT. 
        # Swap APT(0) -> AMI(1): input is token0.
        # Formula: delta_sqrt_P = L / delta_x? No.
        # Let's use the one in hyperion_math.py
        # delta_y = amount_in_raw
        sqrt_P_new = sqrt_P_old + (amount_in_raw / L)
        delta_x = L * ( (1/sqrt_P_old) - (1/sqrt_P_new) )
        amount_out_raw = delta_x
    else:
        # AMI(1) -> APT(0)
        # input is token1 (delta_y)
        sqrt_P_new = sqrt_P_old + (amount_in_raw / L)
        delta_x = L * ( (1/sqrt_P_old) - (1/sqrt_P_new) )
        amount_out_raw = delta_x
        
    return float(amount_out_raw / (10**8))

# Test data
apt_start = 10.0
cellana_fee = 0.001
hyp_fee = 0.003
gas_apt = 0.01

# Case: Cellana 120 AMI/APT, Hyperion 110 AMI/APT
r_apt = 1000 * 10**8
r_ami = 120000 * 10**8
hyp_sqrt_p = int(math.sqrt(110) * (2**64))
hyp_liq = 10**14

print(f"--- Direction 1: Cellana -> Hyperion ---")
ami_out = amm_out(apt_start, r_apt, r_ami, cellana_fee)
print(f"1. Cellana: {apt_start} APT -> {ami_out:.4f} AMI")
apt_end = calculate_hyp_out(hyp_sqrt_p, hyp_liq, ami_out, hyp_fee, False)
print(f"2. Hyperion: {ami_out:.4f} AMI -> {apt_end:.6f} APT")
profit = apt_end - apt_start - gas_apt
print(f"Result: Net Profit = {profit:.6f} APT")

print(f"\n--- Direction 2: Hyperion -> Cellana ---")
ami_out2 = calculate_hyp_out(hyp_sqrt_p, hyp_liq, apt_start, hyp_fee, True)
print(f"1. Hyperion: {apt_start} APT -> {ami_out2:.4f} AMI")
apt_end2 = amm_out(ami_out2, r_ami, r_apt, cellana_fee)
print(f"2. Cellana: {ami_out2:.4f} AMI -> {apt_end2:.6f} APT")
profit2 = apt_end2 - apt_start - gas_apt
print(f"Result: Net Profit = {profit2:.6f} APT")
