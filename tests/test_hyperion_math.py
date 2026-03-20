import math
from core.hyperion_math import calculate_amount_out, decode_sqrt_price

def run_test_case(name, sqrt_price_x96, liquidity, amount_in, fee_rate, is_token0_to_token1):
    print(f"\n--- Test Case: {name} ---")
    
    # Calculate spot price from sqrt_price
    spot_price = float(decode_sqrt_price(sqrt_price_x96)**2)
    
    out, impact = calculate_amount_out(
        sqrt_price_x96, liquidity, amount_in, fee_rate, is_token0_to_token1
    )
    
    token_in = "AMI" if is_token0_to_token1 else "APT"
    token_out = "APT" if is_token0_to_token1 else "AMI"
    
    print(f"  Spot Price: {spot_price:.8f} APT/AMI")
    print(f"  Swap {amount_in} {token_in} -> {out:.8f} {token_out}")
    # Effective price in APT/AMI units
    eff_price = (out/amount_in if is_token0_to_token1 else amount_in/out)
    print(f"  Effective Price: {eff_price:.8f} APT/AMI")
    print(f"  Impact: {impact:.4f}%")
    
    # Basic sanity checks
    assert out > 0, "Output must be positive"
    if is_token0_to_token1:
        # AMI -> APT, price should be around spot_price or slightly lower due to impact
        # 1 AMI ~ spot_price APT. So 100 AMI ~ 100 * spot_price APT.
        expected_approx = amount_in * spot_price
        assert out < expected_approx, f"Output {out} should be less than linear approx {expected_approx} due to fee and impact"
    else:
        # APT -> AMI, price should be around 1/spot_price or slightly lower
        # 1 APT ~ 1/spot_price AMI. So 100 APT ~ 100 / spot_price AMI.
        expected_approx = amount_in / spot_price
        assert out < expected_approx, f"Output {out} should be less than linear approx {expected_approx} due to fee and impact"

def test_all_scenarios():
    # Case 1: Standard price (0.05 APT/AMI), High Liquidity
    # 100 AMI -> ~5 APT
    sqrt_p_005 = int(math.sqrt(0.05) * (2**96))
    run_test_case("Standard Price, High Liquidity (AMI->APT)", 
                  sqrt_p_005, 10**12, 100.0, 0.001, True)
    
    # Case 2: Standard price (0.05 APT/AMI), Low Liquidity
    # Expect higher impact
    run_test_case("Standard Price, Low Liquidity (AMI->APT)", 
                  sqrt_p_005, 10**8, 10.0, 0.001, True)
    
    # Case 3: Reverse swap (APT -> AMI)
    # 5 APT -> ~100 AMI
    run_test_case("Standard Price, High Liquidity (APT->AMI)", 
                  sqrt_p_005, 10**12, 5.0, 0.001, False)
    
    # Case 4: Large swap relative to liquidity
    # Should have significant price impact
    run_test_case("Large Swap, High Impact (AMI->APT)", 
                  sqrt_p_005, 10**9, 500.0, 0.001, True)
    
    # Case 5: Extremely high price
    # 1 AMI = 10 APT. 1 AMI -> ~10 APT
    sqrt_p_10 = int(math.sqrt(10.0) * (2**96))
    run_test_case("High Price (AMI->APT)", 
                  sqrt_p_10, 10**12, 1.0, 0.001, True)
    
    # Case 6: Extremely low price
    # 1 AMI = 0.0001 APT. 10000 AMI -> ~1 APT
    sqrt_p_low = int(math.sqrt(0.0001) * (2**96))
    run_test_case("Low Price (AMI->APT)", 
                  sqrt_p_low, 10**12, 10000.0, 0.001, True)

if __name__ == "__main__":
    try:
        test_all_scenarios()
        print("\n✅ ALL TEST SCENARIOS PASSED!")
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
    except Exception as e:
        print(f"\n❌ AN ERROR OCCURRED: {e}")
