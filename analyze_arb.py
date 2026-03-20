#!/usr/bin/env python3
"""Phân tích nhanh cơ hội arbitrage với giá live."""
import asyncio, json, time, requests, aiohttp

POOL_ID = "0x4a34ac7b916cc941530a99dfc0de27843bf20eba5e580f5c93d0a21e3bcb3464"
DEX_FEE = 0.001  # 0.1%


def get_reserves(retries=3):
    url = "https://fullnode.mainnet.aptoslabs.com/v1/view"
    payload = {
        "function": "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::liquidity_pool::pool_reserves",
        "type_arguments": ["0x1::object::ObjectCore"],
        "arguments": [POOL_ID],
    }
    for i in range(retries):
        try:
            r = requests.post(url, json=payload, timeout=15)
            r.raise_for_status()
            data = r.json()
            return int(data[0]), int(data[1])
        except Exception as e:
            if i < retries - 1:
                print(f"  Retry reserves ({i+1})... {e}")
                time.sleep(2)
            else:
                raise


async def get_cex_prices():
    prices = {}
    async with aiohttp.ClientSession() as session:
        async with session.get("https://api.mexc.com/api/v3/depth?symbol=AMIUSDT&limit=5") as r:
            d = await r.json()
            prices["mexc_ami_ask"] = float(d["asks"][0][0])
            prices["mexc_ami_bid"] = float(d["bids"][0][0])
        async with session.get("https://api.mexc.com/api/v3/depth?symbol=APTUSDT&limit=5") as r:
            d = await r.json()
            prices["mexc_apt_ask"] = float(d["asks"][0][0])
            prices["mexc_apt_bid"] = float(d["bids"][0][0])
        async with session.get("https://api.bybit.com/v5/market/orderbook?category=spot&symbol=AMIUSDT&limit=5") as r:
            d = await r.json()
            prices["bybit_ami_ask"] = float(d["result"]["a"][0][0])
            prices["bybit_ami_bid"] = float(d["result"]["b"][0][0])
        async with session.get("https://api.bybit.com/v5/market/orderbook?category=spot&symbol=APTUSDT&limit=5") as r:
            d = await r.json()
            prices["bybit_apt_ask"] = float(d["result"]["a"][0][0])
            prices["bybit_apt_bid"] = float(d["result"]["b"][0][0])
    return prices


def dex_swap(res_in, res_out, amount_in):
    """AMM constant product swap with fee."""
    net_in = amount_in * (1 - DEX_FEE)
    return res_out * net_in / (res_in + net_in)


async def main():
    print("Đang lấy dữ liệu...")
    res_ami_raw, res_apt_raw = get_reserves()
    res_ami = res_ami_raw / 1e8
    res_apt = res_apt_raw / 1e8
    cex = await get_cex_prices()

    dex_rate = res_ami / res_apt
    print("=" * 80)
    print(f"DEX Reserves: AMI={res_ami:,.0f}  APT={res_apt:,.0f}  ratio={dex_rate:.6f}")
    print()

    results = {}

    for exch in ["mexc", "bybit"]:
        ami_ask = cex[f"{exch}_ami_ask"]
        ami_bid = cex[f"{exch}_ami_bid"]
        apt_ask = cex[f"{exch}_apt_ask"]
        apt_bid = cex[f"{exch}_apt_bid"]

        if exch == "mexc":
            ami_fee, apt_fee = 0.0005, 0.0005
        else:
            ami_fee, apt_fee = 0.001, 0.001

        compound = 1 - (1 - ami_fee) * (1 - DEX_FEE) * (1 - apt_fee)
        cex_implied = ami_ask / apt_bid
        gap = (dex_rate / cex_implied - 1) * 100

        print(f"{'='*80}")
        print(f"--- {exch.upper()} ---")
        print(f"  AMI ask={ami_ask:.6f}  bid={ami_bid:.6f}  spread={(ami_ask/ami_bid-1)*100:.3f}%")
        print(f"  APT ask={apt_ask:.4f}  bid={apt_bid:.4f}  spread={(apt_ask/apt_bid-1)*100:.3f}%")
        print(f"  Compound fee={compound*100:.4f}%  |  DEX/CEX gap={gap:+.4f}%")
        print()

        for trade in [10, 50, 100, 200]:
            # AMI_CYCLE: buy AMI(CEX) -> AMI->APT(DEX) -> sell APT(CEX)
            ami_b = trade / ami_ask * (1 - ami_fee)
            apt_o = dex_swap(res_ami, res_apt, ami_b)
            pf1 = apt_o * apt_bid * (1 - apt_fee) - trade

            # DEX_TO_CEX: buy APT(CEX) -> APT->AMI(DEX) -> sell AMI(CEX)
            apt_b = trade / apt_ask * (1 - apt_fee)
            ami_o = dex_swap(res_apt, res_ami, apt_b)
            pf2 = ami_o * ami_bid * (1 - ami_fee) - trade

            # AMI_START: sell AMI(CEX)->USDT->buy APT(CEX)->APT->AMI(DEX)
            ami_s = trade / ami_bid
            usdt_ = ami_s * ami_bid * (1 - ami_fee)
            apt_b3 = usdt_ / apt_ask * (1 - apt_fee)
            ami_back = dex_swap(res_apt, res_ami, apt_b3)
            pf3 = (ami_back - ami_s) * ami_bid

            # APT_START: APT->AMI(DEX)->sell AMI(CEX)->USDT->buy APT(CEX)
            apt_s = trade / apt_bid
            ami_o4 = dex_swap(res_apt, res_ami, apt_s)  # Lưu ý: APT swap
            # Nhưng khoan — APT_START dùng DEX hướng APT→AMI, nghĩa là bỏ APT vào, lấy AMI ra
            usdt_4 = ami_o4 * ami_bid * (1 - ami_fee)
            apt_re = usdt_4 / apt_ask * (1 - apt_fee)
            pf4 = (apt_re - apt_s) * apt_bid

            best = max([("AMI_CYC", pf1), ("D2C   ", pf2), ("AMI_ST", pf3), ("APT_ST", pf4)], key=lambda x: x[1])
            print(f"  ${trade:>3}: AMI_CYC={pf1:+.4f}({pf1/trade*100:+.2f}%)  D2C={pf2:+.4f}({pf2/trade*100:+.2f}%)  "
                  f"AMI_ST={pf3:+.4f}({pf3/trade*100:+.2f}%)  APT_ST={pf4:+.4f}({pf4/trade*100:+.2f}%)  "
                  f"BEST={best[0]}{best[1]:+.4f}")

        print()

        # Breakeven: AMI_CYCLE, cần AMI ask giảm bao nhiêu?
        print("  === BREAKEVEN (AMI_CYCLE) ===")
        for step in range(1, 200):
            pct = step * 0.01  # 0.01% step
            test_ask = ami_ask * (1 - pct / 100)
            ami_b = 10 / test_ask * (1 - ami_fee)
            apt_o = dex_swap(res_ami, res_apt, ami_b)
            pf = apt_o * apt_bid * (1 - apt_fee) - 10
            if pf > 0:
                print(f"    AMI ask giảm {pct:.2f}% ({ami_ask:.6f} -> {test_ask:.6f}) => profit=${pf:.4f}")
                break

        # Breakeven: AMI_CYCLE, cần APT bid tăng bao nhiêu?
        for step in range(1, 200):
            pct = step * 0.01
            test_bid = apt_bid * (1 + pct / 100)
            ami_b = 10 / ami_ask * (1 - ami_fee)
            apt_o = dex_swap(res_ami, res_apt, ami_b)
            pf = apt_o * test_bid * (1 - apt_fee) - 10
            if pf > 0:
                print(f"    APT bid tăng {pct:.2f}% ({apt_bid:.4f} -> {test_bid:.4f}) => profit=${pf:.4f}")
                break

        # Breakeven: DEX_TO_CEX, cần AMI bid tăng bao nhiêu?
        print("  === BREAKEVEN (DEX_TO_CEX) ===")
        for step in range(1, 200):
            pct = step * 0.01
            test_bid = ami_bid * (1 + pct / 100)
            apt_b = 10 / apt_ask * (1 - apt_fee)
            ami_o = dex_swap(res_apt, res_ami, apt_b)
            pf = ami_o * test_bid * (1 - ami_fee) - 10
            if pf > 0:
                print(f"    AMI bid tăng {pct:.2f}% ({ami_bid:.6f} -> {test_bid:.6f}) => profit=${pf:.4f}")
                break

        # Breakeven: DEX_TO_CEX, cần APT ask giảm bao nhiêu?
        for step in range(1, 200):
            pct = step * 0.01
            test_ask = apt_ask * (1 - pct / 100)
            apt_b = 10 / test_ask * (1 - apt_fee)
            ami_o = dex_swap(res_apt, res_ami, apt_b)
            pf = ami_o * ami_bid * (1 - ami_fee) - 10
            if pf > 0:
                print(f"    APT ask giảm {pct:.2f}% ({apt_ask:.4f} -> {test_ask:.4f}) => profit=${pf:.4f}")
                break

        # Breakeven: AMI_START
        print("  === BREAKEVEN (AMI_START) ===")
        for step in range(1, 200):
            pct = step * 0.01
            # AMI_START profit nếu APT ask giảm
            test_apt_ask = apt_ask * (1 - pct / 100)
            ami_s = 10 / ami_bid
            usdt_ = ami_s * ami_bid * (1 - ami_fee)
            apt_b3 = usdt_ / test_apt_ask * (1 - apt_fee)
            ami_back = dex_swap(res_apt, res_ami, apt_b3)
            pf = (ami_back - ami_s) * ami_bid
            if pf > 0:
                print(f"    APT ask giảm {pct:.2f}% ({apt_ask:.4f} -> {test_apt_ask:.4f}) => profit=${pf:.4f}")
                break

        print()

    # ====== CROSS-CEX QUA DEX ======
    print("=" * 80)
    print("CROSS-CEX QUA DEX")
    print()
    trade = 10
    combos = [
        ("Buy AMI MEXC -> DEX AMI->APT -> Sell APT Bybit",
         cex["mexc_ami_ask"], 0.0005, cex["bybit_apt_bid"], 0.001, "ami"),
        ("Buy AMI Bybit -> DEX AMI->APT -> Sell APT MEXC",
         cex["bybit_ami_ask"], 0.001, cex["mexc_apt_bid"], 0.0005, "ami"),
        ("Buy APT MEXC -> DEX APT->AMI -> Sell AMI Bybit",
         cex["mexc_apt_ask"], 0.0005, cex["bybit_ami_bid"], 0.001, "apt"),
        ("Buy APT Bybit -> DEX APT->AMI -> Sell AMI MEXC",
         cex["bybit_apt_ask"], 0.001, cex["mexc_ami_bid"], 0.0005, "apt"),
    ]
    for desc, buy_p, buy_f, sell_p, sell_f, input_token in combos:
        tokens = trade / buy_p * (1 - buy_f)
        if input_token == "ami":
            out = dex_swap(res_ami, res_apt, tokens)
        else:
            out = dex_swap(res_apt, res_ami, tokens)
        usdt_back = out * sell_p * (1 - sell_f)
        pf = usdt_back - trade
        print(f"  {desc}")
        print(f"    ${pf:+.4f} ({pf/trade*100:+.3f}%)")

    # ====== CROSS-CEX THUẦN (không DEX) ======
    print()
    print("=" * 80)
    print("CROSS-CEX THUẦN (chỉ CEX <-> CEX)")
    print()
    for token in ["ami", "apt"]:
        for buy_ex, sell_ex in [("mexc", "bybit"), ("bybit", "mexc")]:
            bf = 0.0005 if buy_ex == "mexc" else 0.001
            sf = 0.0005 if sell_ex == "mexc" else 0.001
            ask = cex[f"{buy_ex}_{token}_ask"]
            bid = cex[f"{sell_ex}_{token}_bid"]
            net = (bid * (1 - sf)) / (ask / (1 - bf)) - 1  # Sửa: buy=pay ask, nhận qty*(1-bf); sell=nhận bid*(1-sf)
            # Đúng hơn: mua 1 token = pay ask, nhận (1-bf) tokens; bán nhận bid*(1-sf) USDT
            # profit = bid*(1-sf) - ask/(1-bf)... ko đúng
            # Mua $10 AMI trên sàn A: nhận 10/ask * (1-bf) tokens
            # Bán trên sàn B: nhận tokens * bid * (1-sf) USDT
            tokens = 10 / ask * (1 - bf)
            back = tokens * bid * (1 - sf)
            pf = back - 10
            print(f"  {token.upper()}: buy {buy_ex.upper()}@{ask:.6f} sell {sell_ex.upper()}@{bid:.6f} => ${pf:+.4f} ({pf/10*100:+.3f}%)")

    # ====== SO SÁNH FEE BYBIT VS MEXC ======
    print()
    print("=" * 80)
    print("SO SÁNH HIỆU QUẢ FEE")
    print("MEXC: AMI 0.05% + APT 0.05% = compound + DEX 0.1% = tổng ~0.20%")
    print("Bybit: AMI 0.10% + APT 0.10% = compound + DEX 0.1% = tổng ~0.30%")
    print("=> MEXC có lợi thế fee 0.10% so với Bybit")
    print("=> Dùng MEXC mua + Bybit bán (hoặc ngược lại) có thể tận dụng cả 2")


asyncio.run(main())
