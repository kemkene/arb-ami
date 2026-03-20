
import asyncio
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from exchanges.bybit_trader import BybitTrader
from exchanges.mexc_trader import MexcTrader
from core.cellana_dex_swap import CellanaDexSwap
from core.hyperion_dex_swap import HyperionDexSwap
from utils.telegram_notifier import notifier as tg_notifier
from utils.logger import get_logger

logger = get_logger()

async def test_bybit():
    print("\n--- Testing Bybit ---")
    trader = BybitTrader()
    await trader.sync_server_time()
    
    balance = await trader.get_balance(["USDT", "AMI", "APT"])
    print(f"Bybit Balance: {balance}")
    
    # Try a 5 USDT market buy (vượt mức tối thiểu của Bybit)
    print("Attempting Bybit 5 USDT Market Buy (Expecting rejection due to balance)...")
    # marketUnit="quoteCoinQty" for buying AMI using 5 USDT
    # marketUnit is defaulted by Bybit if omitted, let's see if that helps
    fill = await trader.place_market_order("AMIUSDT", "Buy", 5.0)
    if fill:
        print(f"Bybit Order Result: {fill}")
    else:
        print("Bybit Order Failed. Check console for retCode (Expected 170121).")
    return balance

async def test_mexc():
    print("\n--- Testing MEXC ---")
    trader = MexcTrader()
    await trader.sync_server_time()
    
    balance = await trader.get_balance(["USDT", "AMI", "APT"])
    print(f"MEXC Balance: {balance}")
    
    # Try a small market buy (should fail due to balance)
    print("Attempting MEXC Market Buy (Expecting rejection)...")
    fill = await trader.place_market_order("AMIUSDT", "BUY", 5.0, is_quote_qty=True)
    if fill:
        print(f"MEXC Order Result: {fill}")
    else:
        print("MEXC Order Failed as expected (or API error).")
    return balance

async def test_aptos():
    print("\n--- Testing Aptos DEX Quotes & Transaction ---")
    try:
        cellana = CellanaDexSwap()
        # Quote 1 APT -> AMI
        expected_ami = await cellana.get_amount_out_apt_to_ami(1.0)
        print(f"Cellana Quote: 1 APT -> {expected_ami:.4f} AMI")
        
        # ATTEMPT A REAL TRANSACTION (0.001 APT)
        print("Attempting REAL Cellana Swap Transaction (0.001 APT)...")
        # This will likely fail with Insufficient Balance or Execution Error, proving connectivity/auth.
        swap_result = await cellana.swap_apt_to_ami(0.001)
        print(f"Cellana Swap Result: ok={swap_result.ok} tx={swap_result.tx_hash} error={swap_result.error}")

        hyperion = HyperionDexSwap()
        # Quote 1 APT -> AMI
        from core.hyperion_dex_swap import APT_FA_METADATA
        apt_raw = int(1.0 * 10**8)
        expected_raw, fee_raw = await hyperion.get_amount_out_onchain(APT_FA_METADATA, apt_raw)
        print(f"Hyperion Quote: 1 APT -> {expected_raw / 10**8:.4f} AMI (Fee: {fee_raw / 10**8:.6f})")
        
        # ATTEMPT A REAL TRANSACTION (0.001 APT)
        print("Attempting REAL Hyperion Swap Transaction (0.001 APT)...")
        swap_result_h = await hyperion.swap_apt_to_ami(0.001)
        print(f"Hyperion Swap Result: ok={swap_result_h.ok} tx={swap_result_h.tx_hash} error={swap_result_h.error}")

        return True
    except Exception as e:
        print(f"Aptos Test Failed: {e}")
        return False

async def main():
    print("🚀 Starting Live Connectivity Test...")
    
    bybit_ok = await test_bybit()
    mexc_ok = await test_mexc()
    apt_ok = await test_aptos()
    
    status_msg = (
        "<b>🛠 Connectivity Test Report</b>\n\n"
        f"Bybit: {'✅' if bybit_ok else '❌'}\n"
        f"MEXC: {'✅' if mexc_ok else '❌'}\n"
        f"Aptos: {'✅' if apt_ok else '❌'}\n\n"
        "Check console for detailed rejection reasons."
    )
    
    print("\nSending report to Telegram...")
    await tg_notifier.send_message(status_msg)
    print("Done.")

if __name__ == "__main__":
    asyncio.run(main())
