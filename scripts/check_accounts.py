"""
check_accounts.py — Verify account profiles for all accounts in a CSV file.

Checks for each account (if API keys are present):
  - Bybit: API key info, account type, permissions, UID
  - MEXC:  account info, balances, trading permissions

Usage:
    python scripts/check_accounts.py --csv my_accounts.csv
    python scripts/check_accounts.py --csv my_accounts.csv --show-balances
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import hmac
import os
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import aiohttp
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

# ─────────────────────────────────────────────────────────────────────────────
BYBIT_BASE  = "https://api.bybit.com"
MEXC_BASE   = "https://api.mexc.com"
APTOS_NODE  = os.getenv("APTOS_NODE_URL", "https://fullnode.mainnet.aptoslabs.com/v1")
AMI_ADDR    = os.getenv("AMI_TOKEN_ADDRESS",  "0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451")
USDT_ADDR   = os.getenv("USDT_TOKEN_ADDRESS", "0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b")


# ─────────────────────────────────────────────────────────────────────────────
#  Result models
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class BybitProfile:
    index: int
    api_key: str
    status: str           # OK / ERROR / SKIPPED
    uid: str = ""
    account_type: str = ""
    permissions: str = ""
    ip_bound: str = ""
    vip_level: str = ""
    balances: str = ""   # only when --show-balances
    note: str = ""


@dataclass
class MexcProfile:
    index: int
    api_key: str
    status: str           # OK / ERROR / SKIPPED
    account_type: str = ""
    can_trade: bool = False
    can_withdraw: bool = False
    can_deposit: bool = False
    maker_commission: str = ""
    taker_commission: str = ""
    balances: str = ""    # "USDT:10.00, AMI:500.00" (only if --show-balances)
    note: str = ""


# ─────────────────────────────────────────────────────────────────────────────
#  Aptos
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class AptosProfile:
    index: int
    address: str
    status: str          # OK / ERROR / SKIPPED
    ami_balance: str = ""
    usdt_balance: str = ""
    note: str = ""


async def _aptos_get_decimals(
    session: aiohttp.ClientSession,
    coin_type: str,
) -> int:
    """Fetch decimal places from CoinInfo resource."""
    # CoinInfo is stored at the coin's defining module address
    module_addr = coin_type.split("::")[0]
    url = f"{APTOS_NODE}/accounts/{module_addr}/resource/0x1::coin::CoinInfo%3C{coin_type}%3E"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                data = await r.json()
                return int(data.get("data", {}).get("decimals", 8))
    except Exception:
        pass
    return 8  # safe default


async def check_aptos(
    session: aiohttp.ClientSession,
    index: int,
    address: str,
) -> AptosProfile:
    if not address:
        return AptosProfile(index, "(empty)", "SKIPPED", note="No address")

    try:
        url = f"{APTOS_NODE}/accounts/{address}/resources"
        async with session.get(
            url,
            params={"limit": "9999"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status == 404:
                return AptosProfile(index, address, "OK",
                                    ami_balance="0 (not on-chain)",
                                    usdt_balance="0 (not on-chain)")
            if resp.status != 200:
                return AptosProfile(index, address, "ERROR",
                                    note=f"HTTP {resp.status}")
            resources = await resp.json()

        ami_balance = usdt_balance = "0"
        ami_coin_type = usdt_coin_type = ""

        for res in resources:
            rtype: str = res.get("type", "")
            if not rtype.startswith("0x1::coin::CoinStore<"):
                continue
            # Extract the inner coin type
            inner = rtype[len("0x1::coin::CoinStore<"):-1]
            raw_val = int(res.get("data", {}).get("coin", {}).get("value", 0))
            if inner.startswith(AMI_ADDR):
                ami_coin_type = inner
                ami_raw = raw_val
            elif inner.startswith(USDT_ADDR):
                usdt_coin_type = inner
                usdt_raw = raw_val

        # Resolve decimals + format
        if ami_coin_type:
            dec = await _aptos_get_decimals(session, ami_coin_type)
            ami_balance = f"{ami_raw / 10**dec:.{dec}f}"
        if usdt_coin_type:
            dec = await _aptos_get_decimals(session, usdt_coin_type)
            usdt_balance = f"{usdt_raw / 10**dec:.{dec}f}"

        return AptosProfile(
            index, address, "OK",
            ami_balance=ami_balance,
            usdt_balance=usdt_balance,
        )

    except Exception as e:
        return AptosProfile(index, address, "ERROR", note=str(e))


def print_aptos(p: AptosProfile) -> None:
    icon = _status_icon(p.status)
    addr = p.address
    short = f"{addr[:20]}...{addr[-8:]}" if len(addr) > 30 else addr
    print(f"  {icon} Aptos  [{p.index}] {short}")
    if p.status == "OK":
        print(f"       full : {addr}")
        print(f"       AMI  : {p.ami_balance}")
        print(f"       USDT : {p.usdt_balance}")
    elif p.status in ("ERROR", "SKIPPED"):
        print(f"       note: {p.note}")


# ─────────────────────────────────────────────────────────────────────────────
#  Bybit
# ─────────────────────────────────────────────────────────────────────────────
def _bybit_sign(api_key: str, secret: str, timestamp: str,
                recv_window: str, body: str = "") -> str:
    message = timestamp + api_key + recv_window + body
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()


def _bybit_headers(api_key: str, api_secret: str,
                   recv_window: str = "5000",
                   qs: str = "") -> tuple[dict, str]:
    """Return signed headers + fresh timestamp."""
    ts  = str(int(time.time() * 1000))
    sig = _bybit_sign(api_key, api_secret, ts, recv_window, qs)
    return {
        "X-BAPI-API-KEY":     api_key,
        "X-BAPI-SIGN":        sig,
        "X-BAPI-SIGN-TYPE":   "2",
        "X-BAPI-TIMESTAMP":   ts,
        "X-BAPI-RECV-WINDOW": recv_window,
    }, ts


async def check_bybit(
    session: aiohttp.ClientSession,
    index: int,
    api_key: str,
    api_secret: str,
    show_balances: bool = False,
) -> BybitProfile:
    if not api_key or not api_secret:
        return BybitProfile(index, api_key or "(empty)", "SKIPPED",
                            note="No API key in CSV")

    recv_window = "5000"

    try:
        # 1. API key info
        hdrs, _ = _bybit_headers(api_key, api_secret, recv_window)
        async with session.get(
            f"{BYBIT_BASE}/v5/user/query-api",
            headers=hdrs,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            data = await resp.json()

        ret = data.get("retCode")
        if ret != 0:
            return BybitProfile(
                index, api_key, "ERROR",
                note=f"retCode={ret} {data.get('retMsg', '')}"
            )

        result   = data.get("result", {})
        perms    = result.get("permissions", {})
        perm_str = ", ".join(f"{k}:{v}" for k, v in perms.items() if v)
        ips      = result.get("ips", [])
        ip_str   = ", ".join(ips) if ips else "unrestricted"

        # 2. Account info (VIP level)
        hdrs2, _ = _bybit_headers(api_key, api_secret, recv_window)
        async with session.get(
            f"{BYBIT_BASE}/v5/account/info",
            headers=hdrs2,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp2:
            acc_data = await resp2.json()

        acc_result   = acc_data.get("result", {})
        vip_level    = str(acc_result.get("vipLevel", ""))
        account_type = str(acc_result.get("unifiedMarginStatus", ""))

        balance_str = ""
        if show_balances:
            # 3. Spot wallet balance for AMI + USDT
            coin_qs = "accountType=SPOT&coin=AMI,USDT"
            hdrs3, ts3 = _bybit_headers(api_key, api_secret, recv_window, coin_qs)
            async with session.get(
                f"{BYBIT_BASE}/v5/account/wallet-balance",
                headers=hdrs3,
                params={"accountType": "SPOT", "coin": "AMI,USDT"},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp3:
                bal_data = await resp3.json()

            coins = (
                bal_data.get("result", {}).get("list", [{}])[0].get("coin", [])
                if bal_data.get("retCode") == 0 else []
            )
            coin_map = {c["coin"]: c.get("walletBalance", "0") for c in coins}
            ami_bal  = coin_map.get("AMI",  "0")
            usdt_bal = coin_map.get("USDT", "0")
            balance_str = f"AMI:{ami_bal} | USDT:{usdt_bal}"

        return BybitProfile(
            index,
            api_key=api_key,
            status="OK",
            uid=str(result.get("userID", "")),
            account_type=account_type,
            permissions=perm_str,
            ip_bound=ip_str,
            vip_level=vip_level,
            balances=balance_str,
        )

    except Exception as e:
        return BybitProfile(index, api_key, "ERROR", note=str(e))


# ─────────────────────────────────────────────────────────────────────────────
#  MEXC
# ─────────────────────────────────────────────────────────────────────────────
def _mexc_sign(secret: str, query_string: str) -> str:
    return hmac.new(secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()


async def _mexc_get(
    session: aiohttp.ClientSession,
    api_key: str,
    api_secret: str,
    path: str,
    extra_params: Optional[dict] = None,
) -> dict:
    params: dict = {"timestamp": str(int(time.time() * 1000))}
    if extra_params:
        params.update(extra_params)
    qs  = urllib.parse.urlencode(params)
    sig = _mexc_sign(api_secret, qs)
    params["signature"] = sig
    headers = {"X-MEXC-APIKEY": api_key}
    async with session.get(
        f"{MEXC_BASE}{path}",
        headers=headers,
        params=params,
        timeout=aiohttp.ClientTimeout(total=10),
    ) as resp:
        return await resp.json()


async def check_mexc(
    session: aiohttp.ClientSession,
    index: int,
    api_key: str,
    api_secret: str,
    show_balances: bool = False,
) -> MexcProfile:
    if not api_key or not api_secret:
        return MexcProfile(index, api_key or "(empty)", "SKIPPED",
                           note="No API key in CSV")

    try:
        # Account info + permissions
        data = await _mexc_get(session, api_key, api_secret, "/api/v3/account")

        if "code" in data and data["code"] != 0:
            return MexcProfile(
                index, api_key, "ERROR",
                note=f"code={data['code']} {data.get('msg', '')}"
            )

        can_trade    = bool(data.get("canTrade", False))
        can_withdraw = bool(data.get("canWithdraw", False))
        can_deposit  = bool(data.get("canDeposit", False))
        maker_fee    = str(data.get("makerCommission", ""))
        taker_fee    = str(data.get("takerCommission", ""))
        acc_type     = str(data.get("accountType", "SPOT"))

        balance_str = ""
        if show_balances:
            raw_bals = data.get("balances", [])
            nonzero  = [
                f"{b['asset']}:{float(b['free']):.4f}"
                for b in raw_bals
                if float(b.get("free", 0)) > 0 or float(b.get("locked", 0)) > 0
            ]
            balance_str = " | ".join(nonzero) if nonzero else "all zero"

        return MexcProfile(
            index,
            api_key=api_key,
            status="OK",
            account_type=acc_type,
            can_trade=can_trade,
            can_withdraw=can_withdraw,
            can_deposit=can_deposit,
            maker_commission=maker_fee,
            taker_commission=taker_fee,
            balances=balance_str,
        )

    except Exception as e:
        return MexcProfile(index, api_key, "ERROR", note=str(e))


# ─────────────────────────────────────────────────────────────────────────────
#  Pretty print
# ─────────────────────────────────────────────────────────────────────────────
def _status_icon(status: str) -> str:
    return {"OK": "✅", "ERROR": "❌", "SKIPPED": "⏭️"}.get(status, "?")


def print_bybit(p: BybitProfile) -> None:
    icon = _status_icon(p.status)
    print(f"  {icon} Bybit  [{p.index}] key={p.api_key[:12]}...")
    if p.status == "OK":
        print(f"       UID={p.uid}  vip={p.vip_level}  account_type={p.account_type}")
        print(f"       permissions: {p.permissions}")
        print(f"       IP bound:    {p.ip_bound}")
        if p.balances:
            # parse "AMI:x | USDT:y" into two lines
            parts = {k.strip(): v.strip() for part in p.balances.split("|") for k, v in [part.split(":", 1)]}
            print(f"       AMI  : {parts.get('AMI', '0')}")
            print(f"       USDT : {parts.get('USDT', '0')}")
    elif p.status in ("ERROR", "SKIPPED"):
        print(f"       note: {p.note}")


def print_mexc(p: MexcProfile) -> None:
    icon = _status_icon(p.status)
    print(f"  {icon} MEXC   [{p.index}] key={p.api_key[:12]}...")
    if p.status == "OK":
        print(
            f"       type={p.account_type}  "
            f"canTrade={p.can_trade}  canWithdraw={p.can_withdraw}  canDeposit={p.can_deposit}"
        )
        print(f"       fees: maker={p.maker_commission} taker={p.taker_commission}")
        if p.balances:
            print(f"       balances: {p.balances}")
    elif p.status in ("ERROR", "SKIPPED"):
        print(f"       note: {p.note}")


# ─────────────────────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────────────────────
def _load_wallets_csv(csv_path: Optional[str], label: str) -> list[dict]:
    """Generic CSV loader → list of row dicts (index 0 = row 1)."""
    if not csv_path:
        return []
    path = Path(csv_path)
    if not path.is_absolute():
        path = Path(__file__).parent.parent / path
    if not path.exists():
        print(f"⚠️  {label} wallets file not found: {path}")
        return []
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"ℹ️  Loaded {len(rows)} {label} wallet(s) from {path.name}")
    return rows


def _resolve_keys(
    wallet_rows: list[dict],
    idx: int,
    fallback_key: str,
    fallback_secret: str,
) -> tuple[str, str]:
    """Return (key, secret): prefer wallet CSV row idx-1, fall back to accounts CSV field."""
    if wallet_rows and idx - 1 < len(wallet_rows):
        row = wallet_rows[idx - 1]
        key    = row.get("access_key", "").strip()
        secret = row.get("secret_key", "").strip()
        if key:
            return key, secret
    return fallback_key, fallback_secret


async def run(
    csv_path: Path,
    show_balances: bool,
    bybit_wallets_path: Optional[str] = None,
    mexc_wallets_path: Optional[str] = None,
) -> None:
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print("CSV is empty.")
        return

    bybit_rows = _load_wallets_csv(bybit_wallets_path, "Bybit")
    mexc_rows  = _load_wallets_csv(mexc_wallets_path,  "MEXC")

    print(f"\n📋 Checking {len(rows)} account(s) from {csv_path.name}\n")

    async with aiohttp.ClientSession() as session:
        for row in rows:
            idx   = int(row.get("index", 0))
            aptos = row.get("aptos_address", "")

            b_key, b_secret = _resolve_keys(
                bybit_rows, idx,
                row.get("bybit_api_key", "").strip(),
                row.get("bybit_api_secret", "").strip(),
            )
            m_key, m_secret = _resolve_keys(
                mexc_rows, idx,
                row.get("mexc_api_key", "").strip(),
                row.get("mexc_api_secret", "").strip(),
            )

            print(f"─── Account #{idx} ───────────────────────────────────")

            aptos_result, bybit_result, mexc_result = await asyncio.gather(
                check_aptos(session, idx, aptos),
                check_bybit(session, idx, b_key, b_secret, show_balances),
                check_mexc(session, idx, m_key, m_secret, show_balances),
            )

            print_aptos(aptos_result)
            print_bybit(bybit_result)
            print_mexc(mexc_result)
            print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check Bybit / MEXC account profiles from a CSV file."
    )
    parser.add_argument(
        "--csv", type=str, default="my_accounts.csv",
        help="Path to the CSV file (default: my_accounts.csv)"
    )
    parser.add_argument(
        "--show-balances", action="store_true",
        help="Also display non-zero token balances from MEXC"
    )
    parser.add_argument(
        "--bybit-wallets", type=str, default="bybit_wallets.csv",
        help="Path to bybit_wallets.csv (columns: access_key, secret_key). Default: bybit_wallets.csv"
    )
    parser.add_argument(
        "--mexc-wallets", type=str, default="mexc_wallets.csv",
        help="Path to mexc_wallets.csv (columns: access_key, secret_key). Default: mexc_wallets.csv"
    )
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.is_absolute():
        csv_path = Path(__file__).parent.parent / csv_path

    if not csv_path.exists():
        print(f"❌ File not found: {csv_path}")
        return

    asyncio.run(run(csv_path, args.show_balances, args.bybit_wallets, args.mexc_wallets))


if __name__ == "__main__":
    main()
