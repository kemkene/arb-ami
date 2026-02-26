"""
Test scenario for arbitrage opportunities between Bybit and MEXC.
This script simulates price updates and checks for arbitrage opportunities.
"""

import asyncio
from core.price_collector import PriceCollector, PriceData
from core.arbitrage_engine import ArbitrageEngine
from config.settings import settings
from utils.logger import get_logger

logger = get_logger()


async def test_arbitrage_scenario():
    """Simulate realistic arbitrage scenarios."""
    
    collector = PriceCollector()
    arb = ArbitrageEngine(collector, cex_symbol=settings.cex_symbol)
    
    print("\n" + "="*80)
    print("ARBITRAGE TEST SCENARIO")
    print("="*80)
    print(f"Symbol: {settings.cex_symbol}")
    print(f"Bybit fee: {settings.bybit_fee*100:.2f}%")
    print(f"MEXC fee: {settings.mexc_fee*100:.2f}%")
    print(f"Min profit threshold: ${settings.min_profit_threshold:.4f}")
    print("="*80 + "\n")

    # =====================================================================
    # SCENARIO 1: Classic arbitrage - Buy Bybit, Sell MEXC
    # =====================================================================
    print("\n[SCENARIO 1] Buy Bybit (low ask) → Sell MEXC (high bid)")
    print("-" * 80)
    
    bybit_price = 0.1200  # Low ask price on Bybit
    mexc_price = 0.1210   # High bid price on MEXC
    quantity = 100.0
    
    collector.update("bybit", settings.cex_symbol, 
                    bid=0.1199, ask=bybit_price, 
                    bid_qty=50.0, ask_qty=quantity)
    
    collector.update("mexc", settings.cex_symbol,
                    bid=mexc_price, ask=0.1211,
                    bid_qty=quantity, ask_qty=50.0)
    
    print(f"Bybit: ask={bybit_price} (qty={quantity})")
    print(f"MEXC:  bid={mexc_price} (qty={quantity})")
    print(f"Price difference: {(mexc_price - bybit_price)*100:.4f}%")
    
    cex_prices = collector.get(settings.cex_symbol)
    bybit = cex_prices.get("bybit")
    mexc = cex_prices.get("mexc")
    
    if bybit and mexc:
        arb._check_cex_cex(bybit, mexc)
    
    # =====================================================================
    # SCENARIO 2: Reverse arbitrage - Buy MEXC, Sell Bybit
    # =====================================================================
    print("\n[SCENARIO 2] Buy MEXC (low ask) → Sell Bybit (high bid)")
    print("-" * 80)
    
    bybit_bid = 0.1220   # High bid on Bybit
    mexc_ask = 0.1210    # Low ask on MEXC
    quantity = 150.0
    
    collector.update("bybit", settings.cex_symbol,
                    bid=bybit_bid, ask=0.1221,
                    bid_qty=quantity, ask_qty=50.0)
    
    collector.update("mexc", settings.cex_symbol,
                    bid=0.1209, ask=mexc_ask,
                    bid_qty=50.0, ask_qty=quantity)
    
    print(f"Bybit: bid={bybit_bid} (qty={quantity})")
    print(f"MEXC:  ask={mexc_ask} (qty={quantity})")
    print(f"Price difference: {(bybit_bid - mexc_ask)*100:.4f}%")
    
    cex_prices = collector.get(settings.cex_symbol)
    bybit = cex_prices.get("bybit")
    mexc = cex_prices.get("mexc")
    
    if bybit and mexc:
        arb._check_cex_cex(bybit, mexc)
    
    # =====================================================================
    # SCENARIO 3: High profit arbitrage
    # =====================================================================
    print("\n[SCENARIO 3] High profit opportunity")
    print("-" * 80)
    
    bybit_ask = 0.1150   # Very low ask
    mexc_bid = 0.1250    # Very high bid
    quantity = 200.0
    
    collector.update("bybit", settings.cex_symbol,
                    bid=0.1149, ask=bybit_ask,
                    bid_qty=50.0, ask_qty=quantity)
    
    collector.update("mexc", settings.cex_symbol,
                    bid=mexc_bid, ask=0.1251,
                    bid_qty=quantity, ask_qty=50.0)
    
    print(f"Bybit: ask={bybit_ask} (qty={quantity})")
    print(f"MEXC:  bid={mexc_bid} (qty={quantity})")
    print(f"Price difference: {(mexc_bid - bybit_ask)*100:.2f}%")
    
    cex_prices = collector.get(settings.cex_symbol)
    bybit = cex_prices.get("bybit")
    mexc = cex_prices.get("mexc")
    
    if bybit and mexc:
        arb._check_cex_cex(bybit, mexc)
    
    # =====================================================================
    # SCENARIO 4: No arbitrage - prices too close
    # =====================================================================
    print("\n[SCENARIO 4] No arbitrage - prices too close (high spread)")
    print("-" * 80)
    
    bybit_ask_no_arb = 0.1200
    mexc_bid_no_arb = 0.1201
    quantity = 100.0
    
    collector.update("bybit", settings.cex_symbol,
                    bid=0.1199, ask=bybit_ask_no_arb,
                    bid_qty=50.0, ask_qty=quantity)
    
    collector.update("mexc", settings.cex_symbol,
                    bid=mexc_bid_no_arb, ask=0.1202,
                    bid_qty=quantity, ask_qty=50.0)
    
    print(f"Bybit: ask={bybit_ask_no_arb} (qty={quantity})")
    print(f"MEXC:  bid={mexc_bid_no_arb} (qty={quantity})")
    print(f"Price difference: {(mexc_bid_no_arb - bybit_ask_no_arb)*100:.4f}%")
    print("(This spread is usually consumed by fees - no profit)")
    
    cex_prices = collector.get(settings.cex_symbol)
    bybit = cex_prices.get("bybit")
    mexc = cex_prices.get("mexc")
    
    if bybit and mexc:
        arb._check_cex_cex(bybit, mexc)
    
    # =====================================================================
    # Manual Profit Calculation Example
    # =====================================================================
    print("\n[PROFIT CALCULATION] Manual example from Scenario 1")
    print("-" * 80)
    
    buy_exchange = "Bybit"
    sell_exchange = "MEXC"
    buy_price = 0.1200
    sell_price = 0.1210
    qty = 100.0
    bybit_fee_rate = settings.bybit_fee
    mexc_fee_rate = settings.mexc_fee
    
    buy_vol = qty * buy_price
    sell_vol = qty * sell_price
    profit = sell_vol - buy_vol - (buy_vol * bybit_fee_rate) - (sell_vol * mexc_fee_rate)
    
    print(f"Buy {buy_exchange}  @ {buy_price} × {qty} = ${buy_vol:.2f}")
    print(f"Sell {sell_exchange} @ {sell_price} × {qty} = ${sell_vol:.2f}")
    print(f"Gross profit: ${sell_vol - buy_vol:.4f}")
    print(f"Bybit fee (0.1%): ${buy_vol * bybit_fee_rate:.4f}")
    print(f"MEXC fee (0.1%):  ${sell_vol * mexc_fee_rate:.4f}")
    print(f"NET PROFIT: ${profit:.4f}")
    
    print("\n" + "="*80)
    print("TEST COMPLETE")
    print("="*80 + "\n")


if __name__ == "__main__":
    asyncio.run(test_arbitrage_scenario())
