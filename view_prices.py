#!/usr/bin/env python3
"""
View AMI/APT price history from logs/prices.jsonl
"""

import json
from datetime import datetime
from pathlib import Path


def main():
    """Display price history from structured logs."""
    prices_file = Path("logs/prices.jsonl")
    
    if not prices_file.exists():
        print("❌ No price log file found at logs/prices.jsonl")
        print("   Run the listener first to collect price data.")
        return
    
    print()
    print("=" * 100)
    print(" 💰 AMI/APT PRICE HISTORY (from Cellana DEX)")
    print("=" * 100)
    print()
    print(f"{'Timestamp':<20} {'Version':<12} {'Spot Price':<14} {'Price+Fee':<14} {'Reserves AMI':<20} {'Reserves APT':<20}")
    print("-" * 100)
    
    count = 0
    with open(prices_file, 'r') as f:
        for line in f:
            try:
                data = json.loads(line.strip())
                
                # Parse timestamp
                ts = data.get('ts')
                if ts:
                    dt = datetime.fromtimestamp(ts)
                    ts_str = dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    ts_str = "N/A"
                
                version = data.get('version', 'N/A')
                price_spot = data.get('price_spot', 0)
                price_with_fee = data.get('price_with_fee', 0)
                reserves_ami = data.get('reserves_ami', 0)
                reserves_apt = data.get('reserves_apt', 0)
                
                print(f"{ts_str:<20} {version:<12} {price_spot:<14.8f} {price_with_fee:<14.8f} {reserves_ami:<20,} {reserves_apt:<20,}")
                count += 1
                
            except json.JSONDecodeError:
                continue
            except Exception as e:
                print(f"Error parsing line: {e}")
                continue
    
    print("-" * 100)
    print(f"Total records: {count}")
    print()
    
    # Calculate statistics if we have data
    if count > 0:
        print("=" * 100)
        print(" 📊 STATISTICS")
        print("=" * 100)
        print()
        
        prices = []
        with open(prices_file, 'r') as f:
            for line in f:
                try:
                    data = json.loads(line.strip())
                    price = data.get('price_spot')
                    if price:
                        prices.append(float(price))
                except:
                    continue
        
        if prices:
            avg_price = sum(prices) / len(prices)
            min_price = min(prices)
            max_price = max(prices)
            
            print(f"  Average Spot Price:  {avg_price:.8f} APT/AMI")
            print(f"  Min Spot Price:      {min_price:.8f} APT/AMI")
            print(f"  Max Spot Price:      {max_price:.8f} APT/AMI")
            print(f"  Price Range:         {(max_price - min_price):.8f} APT ({((max_price - min_price)/avg_price * 100):.2f}%)")
            print()


if __name__ == "__main__":
    main()
