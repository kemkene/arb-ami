#!/usr/bin/env python3
"""
Test AMI/APT Arbitrage Calculator

Demonstrates how to calculate arbitrage opportunities using:
- Real-time Cellana DEX price from SyncEvent
- Mock CEX prices (replace with real data in production)
"""

from core.ami_apt_arbitrage import AmiAptArbitrageCalculator


def main():
    """Test arbitrage calculator with example prices."""
    print()
    print("=" * 80)
    print("🧪 AMI/APT ARBITRAGE CALCULATOR - TEST")
    print("=" * 80)
    print()
    
    # Initialize calculator with fee configuration
    calculator = AmiAptArbitrageCalculator(
        cex_fee=0.001,  # 0.1% CEX fee
        dex_fee=0.001,  # 0.1% Cellana fee
    )
    
    # Example scenario 1: DEX cheaper than CEX
    print("\n📊 Scenario 1: DEX Price < CEX Price (Buy DEX, Sell CEX)")
    print("-" * 80)
    
    dex_price = 0.00804602  # From real Cellana data (tx 4425718052)
    cex_ami_usdt = 0.85    # Example: 1 AMI = 0.85 USDT
    cex_apt_usdt = 100.0   # Example: 1 APT = 100 USDT
    
    print(f"  DEX Price (AMI/APT):    {dex_price:.8f}")
    print(f"  CEX AMI/USDT:           ${cex_ami_usdt}")
    print(f"  CEX APT/USDT:           ${cex_apt_usdt}")
    print(f"  CEX Implied AMI/APT:    {cex_ami_usdt/cex_apt_usdt:.8f}")
    print()
    
    opportunity = calculator.find_best_opportunity(dex_price, cex_ami_usdt, cex_apt_usdt)
    
    if opportunity:
        print(f"✅ PROFITABLE OPPORTUNITY FOUND!")
        print(f"  Direction:      {opportunity['direction']}")
        print(f"  Price Diff:     {opportunity['price_diff_pct']:.2f}%")
        print(f"  Total Fees:     {opportunity['fees_pct']:.2f}%")
        print(f"  Net Profit:     {opportunity['net_profit_pct']:.2f}%")
    else:
        print("❌ No profitable opportunity")
    
    # Example scenario 2: CEX cheaper than DEX
    print("\n\n📊 Scenario 2: CEX Price < DEX Price (Buy CEX, Sell DEX)")
    print("-" * 80)
    
    dex_price = 0.00900000  # DEX price higher
    cex_ami_usdt = 0.80     # CEX price lower
    cex_apt_usdt = 100.0
    
    print(f"  DEX Price (AMI/APT):    {dex_price:.8f}")
    print(f"  CEX AMI/USDT:           ${cex_ami_usdt}")
    print(f"  CEX APT/USDT:           ${cex_apt_usdt}")
    print(f"  CEX Implied AMI/APT:    {cex_ami_usdt/cex_apt_usdt:.8f}")
    print()
    
    opportunity = calculator.find_best_opportunity(dex_price, cex_ami_usdt, cex_apt_usdt)
    
    if opportunity:
        print(f"✅ PROFITABLE OPPORTUNITY FOUND!")
        print(f"  Direction:      {opportunity['direction']}")
        print(f"  Price Diff:     {opportunity['price_diff_pct']:.2f}%")
        print(f"  Total Fees:     {opportunity['fees_pct']:.2f}%")
        print(f"  Net Profit:     {opportunity['net_profit_pct']:.2f}%")
    else:
        print("❌ No profitable opportunity")
    
    # Example scenario 3: Prices too close (no arbitrage)
    print("\n\n📊 Scenario 3: Prices Too Close (No Arbitrage)")
    print("-" * 80)
    
    dex_price = 0.00805000
    cex_ami_usdt = 0.805
    cex_apt_usdt = 100.0
    
    print(f"  DEX Price (AMI/APT):    {dex_price:.8f}")
    print(f"  CEX AMI/USDT:           ${cex_ami_usdt}")
    print(f"  CEX APT/USDT:           ${cex_apt_usdt}")
    print(f"  CEX Implied AMI/APT:    {cex_ami_usdt/cex_apt_usdt:.8f}")
    print()
    
    opportunity = calculator.find_best_opportunity(dex_price, cex_ami_usdt, cex_apt_usdt)
    
    if opportunity:
        print(f"✅ PROFITABLE OPPORTUNITY FOUND!")
        print(f"  Direction:      {opportunity['direction']}")
        print(f"  Price Diff:     {opportunity['price_diff_pct']:.2f}%")
        print(f"  Total Fees:     {opportunity['fees_pct']:.2f}%")
        print(f"  Net Profit:     {opportunity['net_profit_pct']:.2f}%")
    else:
        print("❌ No profitable opportunity (profit < minimum threshold)")
    
    print()
    print("=" * 80)
    print()


if __name__ == "__main__":
    main()
