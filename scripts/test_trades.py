"""
test_trades.py — Test buy/sell on Bybit, MEXC, and Panora DEX.

Reads API keys from the same wallet CSV files as create_accounts.py / check_accounts.py.
For Panora, reads APTOS_PRIVATE_KEY from .env (or --aptos-key flag).

Usage:
    # Dry-run (default) — shows what would be sent, no real order
    python scripts/test_trades.py --side BUY  --usdt-amount 1.0
    python scripts/test_trades.py --side SELL --ami-amount  100

    # Live — actually places orders
    python scripts/test_trades.py --side BUY  --usdt-amount 1.0 --live
    python scripts/test_trades.py --side SELL --ami-amount  100  --live

    # MEXC — buy / sell
    python scripts/test_trades.py --exchange mexc --side BUY  --usdt-amount 1.0 --live
    python scripts/test_trades.py --exchange mexc --side SELL --ami-amount  100  --live

    # Panora DEX — buy / sell
    python scripts/test_trades.py --exchange panora --side BUY  --usdt-amount 1.0 --live
    python scripts/test_trades.py --exchange panora --side SELL --ami-amount  100  --live

    # Bybit (temporarily disabled in --exchange all due to retCode=10024)
    python scripts/test_trades.py --exchange bybit --side BUY  --usdt-amount 1.0 --live

    # Use row 2 from wallet CSVs (Bybit, MEXC, Aptos)
    python scripts/test_trades.py --account 2 --side BUY --usdt-amount 1.0 --live

Flags:
    --account N          Row index in wallet CSVs (1-based, default: 1)
    --exchange           bybit | mexc | panora | all  (default: all)
    --side               BUY | SELL  (default: BUY)
    --usdt-amount        USDT to spend on BUY (default: 1.0)
    --ami-amount         AMI to sell on SELL  (default: 10.0)
    --live               Disable dry-run, place real orders
    --bybit-wallets      Path to bybit_wallets.csv  (default: bybit_wallets.csv)
    --mexc-wallets       Path to mexc_wallets.csv   (default: mexc_wallets.csv)
    --aptos-wallets      Path to aptos_wallets.csv  (default: aptos_wallets.csv)
    --aptos-key          Aptos private key (overrides CSV / .env APTOS_PRIVATE_KEY)
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Optional

import aiohttp
from dotenv import load_dotenv

# ── project root on sys.path ──────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

# ── token addresses (mirror settings.py) ─────────────────────────────────────
AMI_TOKEN  = os.getenv("AMI_TOKEN_ADDRESS",
    "0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451")
USDT_TOKEN = os.getenv("USDT_TOKEN_ADDRESS",
    "0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b")
CEX_SYMBOL   = os.getenv("CEX_SYMBOL", "AMIUSDT")
PANORA_URL   = os.getenv("PANORA_API_URL", "https://api.panora.exchange/swap")
PANORA_KEY   = os.getenv("PANORA_API_KEY",
    "a4^KV_EaTf4MW#ZdvgGKX#HUD^3IFEAOV_kzpIE^3BQGA8pDnrkT7JcIy#HNlLGi")
APTOS_NODE   = os.getenv("APTOS_NODE_URL",
    "https://fullnode.mainnet.aptoslabs.com/v1")

# ABI param types for panora_swap::router_entry (excluding implicit &signer).
# Used for manual BCS encoding — the REST encode_submission endpoint rejects
# Option<signer> types, so we must encode + submit via BCS directly.
PANORA_ROUTER_PARAM_TYPES = [
    "0x1::option::Option<signer>",           # args[0]  integrator signer (None)
    "address",                               # args[1]  to_wallet
    "u64",                                   # args[2]
    "u8",                                    # args[3]  num_splits
    "vector<u8>",                            # args[4]  pool_type_vec
    "vector<vector<vector<u8>>>",            # args[5]  pool_info
    "vector<vector<vector<u64>>>",           # args[6]  pool_amounts
    "vector<vector<vector<bool>>>",          # args[7]  pool_flags
    "vector<vector<u8>>",                    # args[8]
    "vector<vector<vector<address>>>",       # args[9]  pool_addrs
    "vector<vector<address>>",               # args[10] from_addrs
    "vector<vector<address>>",               # args[11] to_addrs
    "0x1::option::Option<vector<vector<vector<vector<vector<u8>>>>>>",  # args[12]
    "vector<vector<vector<u64>>>",           # args[13] min_output_amounts
    "0x1::option::Option<vector<vector<vector<u8>>>>",                  # args[14]
    "address",                               # args[15] output_token
    "vector<u64>",                           # args[16] amounts
    "u64",                                   # args[17] from_amount
    "u64",                                   # args[18] min_out
    "address",                               # args[19] fee_addr
]


def _panora_bcs_encode(type_str: str, value) -> bytes:
    """BCS-encode a single Move value given its type string."""
    from aptos_sdk.bcs import Serializer
    from aptos_sdk.account_address import AccountAddress

    ser = Serializer()

    def write(t: str, v):
        t = t.strip()
        if t == "bool":
            ser.bool(bool(v))
        elif t == "u8":
            ser.u8(int(v))
        elif t == "u64":
            ser.u64(int(v))
        elif t == "u128":
            ser.u128(int(v))
        elif t == "address":
            if isinstance(v, str):
                # Pad short addresses: "0xa" → "0x000...0a" (64 hex chars)
                hex_part = v[2:] if v.startswith("0x") else v
                addr = AccountAddress.from_str("0x" + hex_part.zfill(64))
            else:
                addr = v
            addr.serialize(ser)
        elif t.startswith("vector<") and t.endswith(">"):
            inner = t[7:-1]
            ser.uleb128(len(v))
            for item in v:
                write(inner, item)
        elif t == "0x1::option::Option<signer>":
            ser.uleb128(0)  # always None — signer passed implicitly
        elif t.startswith("0x1::option::Option<") and t.endswith(">"):
            inner = t[20:-1]
            if v is None:
                ser.uleb128(0)
            else:
                ser.uleb128(1)
                write(inner, v)
        else:
            raise ValueError(f"Unsupported Move type: {t!r}")

    write(type_str, value)
    return ser.output()


# ─────────────────────────────────────────────────────────────────────────────
#  CSV helpers
# ─────────────────────────────────────────────────────────────────────────────
def _load_csv(path_str: str, label: str) -> list[dict]:
    p = Path(path_str)
    if not p.is_absolute():
        p = ROOT / p
    if not p.exists():
        print(f"  ⚠️  {label}: file not found: {p}")
        return []
    with open(p, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _get_row(rows: list[dict], account: int) -> dict:
    idx = account - 1
    if idx < 0 or idx >= len(rows):
        return {}
    return rows[idx]


# ─────────────────────────────────────────────────────────────────────────────
#  Bybit
# ─────────────────────────────────────────────────────────────────────────────
def _bybit_sign(api_key: str, secret: str, ts: str, rw: str, body: str) -> str:
    msg = ts + api_key + rw + body
    return hmac.new(secret.encode(), msg.encode(), hashlib.sha256).hexdigest()


async def bybit_order(
    api_key: str,
    api_secret: str,
    side: str,         # "Buy" | "Sell"
    qty: float,
    market_unit: str,  # "quoteCoinQty" for BUY, "baseCoinQty" for SELL
    dry_run: bool,
) -> dict:
    body_dict = {
        "category":   "spot",
        "symbol":     CEX_SYMBOL,
        "side":       side,
        "orderType":  "Market",
        "qty":        str(qty),
        "marketUnit": market_unit,
    }
    body_str = json.dumps(body_dict, separators=(",", ":"))
    ts  = str(int(time.time() * 1000))
    rw  = "5000"
    sig = _bybit_sign(api_key, api_secret, ts, rw, body_str)
    headers = {
        "X-BAPI-API-KEY":     api_key,
        "X-BAPI-SIGN":        sig,
        "X-BAPI-SIGN-TYPE":   "2",
        "X-BAPI-TIMESTAMP":   ts,
        "X-BAPI-RECV-WINDOW": rw,
        "Content-Type":       "application/json",
    }
    if dry_run:
        return {"_dry_run": True, "_body": body_dict}

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://api.bybit.com/v5/order/create",
            headers=headers,
            data=body_str,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            return await resp.json()


# ─────────────────────────────────────────────────────────────────────────────
#  MEXC
# ─────────────────────────────────────────────────────────────────────────────
def _mexc_sign(secret: str, qs: str) -> str:
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()


async def mexc_order(
    api_key: str,
    api_secret: str,
    side: str,           # "BUY" | "SELL"
    qty: float,
    is_quote_qty: bool,  # True → quoteOrderQty (USDT), False → quantity (AMI)
    dry_run: bool,
) -> dict:
    params: dict = {
        "symbol":    CEX_SYMBOL,
        "side":      side,
        "type":      "MARKET",
        "timestamp": str(int(time.time() * 1000)),
    }
    if is_quote_qty:
        params["quoteOrderQty"] = str(qty)
    else:
        params["quantity"] = str(qty)

    if dry_run:
        return {"_dry_run": True, "_params": params}

    qs  = urllib.parse.urlencode(params)
    sig = _mexc_sign(api_secret, qs)
    params["signature"] = sig

    # MEXC v3: params in query string, Content-Type must be application/json
    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://api.mexc.com/api/v3/order",
            headers={
                "X-MEXC-APIKEY": api_key,
                "Content-Type": "application/json",
            },
            params=params,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            return await resp.json()


# ─────────────────────────────────────────────────────────────────────────────
#  Panora DEX
# ─────────────────────────────────────────────────────────────────────────────
async def _panora_quote(
    from_token: str,
    to_token: str,
    amount: float,
    wallet: str,
    max_retries: int = 6,
) -> Optional[dict]:
    params = {
        "fromTokenAddress": from_token,
        "toTokenAddress":   to_token,
        "fromTokenAmount":  str(amount),
        "toWalletAddress":  wallet,
        "slippagePercentage": "1",
    }
    async with aiohttp.ClientSession() as session:
        for attempt in range(max_retries):
            try:
                async with session.post(
                    PANORA_URL,
                    headers={"x-api-key": PANORA_KEY},
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status == 200:
                        return await resp.json()

                    if resp.status in (429, 503):
                        retry_after = resp.headers.get("Retry-After")
                        wait = int(retry_after) if (retry_after and retry_after.isdigit()) \
                               else min(2 ** attempt, 30)
                        print(f"  Panora quote HTTP {resp.status} — rate limited, "
                              f"retry {attempt+1}/{max_retries} in {wait}s...")
                        await asyncio.sleep(wait)
                        continue

                    text = await resp.text()
                    print(f"  Panora quote HTTP {resp.status}: {text[:200]}")
                    return None
            except asyncio.TimeoutError:
                wait = min(2 ** attempt, 30)
                print(f"  Panora quote timeout, retry {attempt+1}/{max_retries} in {wait}s...")
                await asyncio.sleep(wait)

        print(f"  Panora quote failed after {max_retries} retries (rate limited)")
        return None


async def panora_swap(
    aptos_private_key: str,
    side: str,          # "BUY" (USDT→AMI) | "SELL" (AMI→USDT)
    usdt_amount: float,
    ami_amount: float,
    dry_run: bool,
) -> dict:
    from aptos_sdk.account import Account
    from aptos_sdk.async_client import RestClient
    from aptos_sdk.transactions import (
        EntryFunction, TransactionArgument, TransactionPayload,
    )
    from aptos_sdk.type_tag import TypeTag, StructTag

    try:
        account = Account.load_key(aptos_private_key)
    except Exception as e:
        return {"error": f"Failed to load Aptos key: {e}"}

    wallet = str(account.address())
    if side == "BUY":
        from_token, to_token, amount = USDT_TOKEN, AMI_TOKEN, usdt_amount
    else:
        from_token, to_token, amount = AMI_TOKEN, USDT_TOKEN, ami_amount

    # ── Get quote ──────────────────────────────────────────────────────────
    quote = await _panora_quote(from_token, to_token, amount, wallet)
    if not quote:
        return {"error": "Panora quote failed"}

    quotes_list = quote.get("quotes", [])
    if not quotes_list:
        return {"error": f"No quotes in Panora response. keys={list(quote.keys())}"}
    best_quote = quotes_list[0]
    tx_data = best_quote.get("txData")
    if not tx_data:
        return {"error": f"No txData in quotes[0]. keys={list(best_quote.keys())}"}

    if dry_run:
        return {
            "_dry_run": True,
            "_wallet": wallet,
            "_from": from_token[:20] + "...",
            "_to":   to_token[:20] + "...",
            "_amount": amount,
            "_to_amount": best_quote.get("toTokenAmount"),
            "_function": tx_data.get("function", "")[:80],
        }

    # ── Build BCS-encoded arguments ──────────────────────────────────────
    # encode_submission REST endpoint rejects Option<signer> args, so we
    # serialize via BCS and submit as a raw signed BCS transaction.
    try:
        raw_args = tx_data.get("arguments", [])
        if len(raw_args) != len(PANORA_ROUTER_PARAM_TYPES):
            return {
                "error": f"Unexpected argument count: got {len(raw_args)}, "
                         f"expected {len(PANORA_ROUTER_PARAM_TYPES)}. "
                         f"Panora may have updated their contract."
            }

        bcs_args = []
        for type_str, value in zip(PANORA_ROUTER_PARAM_TYPES, raw_args):
            encoded = _panora_bcs_encode(type_str, value)
            bcs_args.append(
                TransactionArgument(encoded, lambda ser, v: ser.fixed_bytes(v))
            )

        # Type tags
        type_tags = [
            TypeTag(StructTag.from_str(t))
            for t in tx_data.get("type_arguments", [])
        ]

        # Entry function
        func_parts = tx_data["function"].split("::")
        module_id  = f"{func_parts[0]}::{func_parts[1]}"
        func_name  = func_parts[2]
        entry_fn   = EntryFunction.natural(module_id, func_name, type_tags, bcs_args)

        # ── Check APT balance + compute max_gas_amount dynamically ───────
        #   Aptos requires: balance ≥ max_gas_amount × gas_unit_price
        #   SDK default is 200,000 × 100 = 20 M octas (0.2 APT) — too much
        #   for wallets with small balance, so we cap to 90 % of available.
        GAS_UNIT_PRICE = 100          # octas per gas unit (Aptos default)
        MIN_GAS_UNITS  = 5_000        # floor for a Panora swap (~0.0005 APT)
        MAX_GAS_UNITS  = 200_000      # SDK default cap
        APT_DECIMALS   = 8
        apt_octas      = None
        try:
            async with aiohttp.ClientSession() as _s:
                async with _s.post(
                    f"{APTOS_NODE}/view",
                    json={
                        "function":       "0x1::coin::balance",
                        "type_arguments": ["0x1::aptos_coin::AptosCoin"],
                        "arguments":      [wallet],
                    }
                ) as _r:
                    if _r.status == 200:
                        _data     = await _r.json()          # e.g. ["100000"]
                        apt_octas = int(_data[0])
                        apt_human = apt_octas / 10 ** APT_DECIMALS
                        print(f"  APT balance: {apt_human:.6f} APT")
                    else:
                        _body = await _r.text()
                        print(f"  ⚠️  APT balance check HTTP {_r.status}: {_body[:120]} — proceeding anyway")
        except Exception as _e:
            print(f"  ⚠️  Could not check APT balance: {_e} — proceeding anyway")

        if apt_octas is not None:
            # Reserve at most 90 % of balance for gas
            dyn_max_gas = min(MAX_GAS_UNITS,
                              int(apt_octas * 0.9) // GAS_UNIT_PRICE)
            if dyn_max_gas < MIN_GAS_UNITS:
                return {
                    "error": (
                        f"Insufficient APT for gas fee. "
                        f"Balance: {apt_octas / 10**APT_DECIMALS:.6f} APT — "
                        f"need enough for at least {MIN_GAS_UNITS:,} gas units "
                        f"({MIN_GAS_UNITS * GAS_UNIT_PRICE / 10**APT_DECIMALS:.4f} APT). "
                        f"Please deposit more APT to {wallet}"
                    )
                }
            if dyn_max_gas < MAX_GAS_UNITS:
                print(f"  ⚠️  Low APT — capping max_gas_amount={dyn_max_gas:,} "
                      f"(swap will abort if actual gas > {dyn_max_gas:,} units; "
                      f"deposit more APT to avoid this)")
            else:
                print(f"  max_gas_amount={dyn_max_gas:,} ✓")
        else:
            dyn_max_gas = MAX_GAS_UNITS   # unknown balance — use default

        # ── Sign + submit via BCS ─────────────────────────────────────────
        client = RestClient(APTOS_NODE)
        client.client_config.max_gas_amount  = dyn_max_gas
        client.client_config.gas_unit_price  = GAS_UNIT_PRICE
        signed   = await client.create_bcs_signed_transaction(
            account, TransactionPayload(entry_fn)
        )
        txn_hash = await client.submit_bcs_transaction(signed)
        print(f"  Submitted txn={txn_hash} — waiting for confirmation...")
        try:
            await client.wait_for_transaction(txn_hash)
            await client.close()
            return {"txn_hash": txn_hash, "status": "confirmed"}
        except Exception as _wait_err:
            await client.close()
            # wait_for_transaction throws when success=false; extract vm_status
            import re as _re2
            _m = _re2.search(r'"vm_status"\s*:\s*"([^"]+)"', str(_wait_err))
            vm_status = _m.group(1) if _m else str(_wait_err)[:300]
            return {"error": vm_status, "txn_hash": txn_hash}

    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────────────────
#  Pretty result printer
# ─────────────────────────────────────────────────────────────────────────────
def _print_result(exchange: str, action: str, result: dict, dry_run: bool) -> None:
    prefix = f"  {'[DRY-RUN]' if dry_run else '[LIVE]':10s} {exchange:8s} {action}"
    if result.get("_dry_run"):
        if "panora" in exchange.lower():
            print(f"{prefix} → would swap {result.get('_amount')} → ~{result.get('_to_amount')} | fn={result.get('_function')}")
        else:
            print(f"{prefix} → would send: {json.dumps(result.get('_body') or result.get('_params') or result, indent=None)}")
        return

    if "error" in result:
        # Panora on-chain failure: has BOTH txn_hash and error — give rich output
        if "txn_hash" in result:
            vm_s = result["error"]
            hint = ""
            if "EINSUFFICIENT_BALANCE" in vm_s:
                hint = "💡 Token balance too low — deposit the from-token to the Aptos wallet"
            elif "INSUFFICIENT_BALANCE_FOR_TRANSACTION_FEE" in vm_s or "vm_error_code\":5" in vm_s:
                hint = "💡 Insufficient APT for gas — deposit more APT to the Aptos wallet"
            print(f"{prefix} → ❌ {vm_s}")
            print(f"  tx={result['txn_hash']}")
            if hint:
                print(f"  {hint}")
            return
        # Panora pre-flight / network failure
        if "INSUFFICIENT_BALANCE_FOR_TRANSACTION_FEE" in str(result):
            print(f"{prefix} → ❌ {result['error']}")
            print("  💡 Insufficient APT for gas — deposit more APT to the Aptos wallet")
            return
        print(f"{prefix} → ❌ {result['error']}")
        return

    # Bybit
    if "retCode" in result:
        rc = result["retCode"]
        if rc == 0:
            oid = result.get("result", {}).get("orderId", "?")
            print(f"{prefix} → ✅ orderId={oid}")
        else:
            bybit_hints = {
                10003: "Invalid API key",
                10004: "Invalid sign",
                10024: "Account restricted by regulation — contact Bybit support",
                110007: "Insufficient balance",
                170131: "Min order size not met — increase --usdt-amount",
            }
            hint = bybit_hints.get(rc, "")
            print(f"{prefix} → ❌ retCode={rc} msg={result.get('retMsg')}" + (f"  💡 {hint}" if hint else ""))
        return

    # MEXC
    if "orderId" in result:
        print(f"{prefix} → ✅ orderId={result['orderId']}")
        return
    if "code" in result:
        code = result["code"]
        msg  = result.get("msg", "")
        hints = {
            30004: "Insufficient balance — deposit USDT/AMI first",
            30002: "Min order size not met — increase --usdt-amount or --ami-amount",
            10072: "IP not whitelisted on this API key",
            700003: "Invalid API key",
            730100: "Symbol not found",
        }
        hint = hints.get(code, "")
        print(f"{prefix} → ❌ code={code} msg={msg}" + (f"  💡 {hint}" if hint else ""))
        return

    # Panora — success (no error key, status=confirmed)
    if "txn_hash" in result:
        print(f"{prefix} → ✅ tx={result['txn_hash']}")
        return

    print(f"{prefix} → {result}")


# ─────────────────────────────────────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────────────────────────────────────
async def run(args: argparse.Namespace) -> None:
    dry_run    = not args.live
    account    = args.account
    side       = args.side.upper()   # BUY | SELL
    usdt_amt   = args.usdt_amount
    ami_amt    = args.ami_amount
    exchanges  = args.exchange.lower()

    print(f"\n{'='*60}")
    print(f"  Mode      : {'DRY-RUN ⚠️  (use --live for real orders)' if dry_run else '🔴 LIVE'}")
    print(f"  Account   : #{account}")
    print(f"  Side      : {side}")
    print(f"  Exchange  : {exchanges}")
    if side == "BUY":
        print(f"  Amount    : {usdt_amt} USDT → AMI")
    else:
        print(f"  Amount    : {ami_amt} AMI → USDT")
    print(f"{'='*60}\n")

    # ── Load wallet CSV rows ──────────────────────────────────────────────
    bybit_rows  = _load_csv(args.bybit_wallets,  "Bybit")
    mexc_rows   = _load_csv(args.mexc_wallets,   "MEXC ")
    aptos_rows  = _load_csv(args.aptos_wallets,  "Aptos")
    bybit_row   = _get_row(bybit_rows,  account)
    mexc_row    = _get_row(mexc_rows,   account)
    aptos_row   = _get_row(aptos_rows,  account)

    bybit_key    = bybit_row.get("access_key",  "").strip()
    bybit_secret = bybit_row.get("secret_key",  "").strip()
    mexc_key     = mexc_row.get("access_key",   "").strip()
    mexc_secret  = mexc_row.get("secret_key",   "").strip()

    # Panora key priority: --aptos-key flag → aptos_wallets.csv → .env
    aptos_key = (
        (args.aptos_key or "").strip()
        or aptos_row.get("private_key", "").strip()
        or os.getenv("APTOS_PRIVATE_KEY", "")
    )

    tasks = []

    # ── Bybit (disabled in "all" — retCode=10024 regulatory restriction) ──
    if exchanges == "bybit":
        if not bybit_key:
            print("  ⚠️  Bybit: no key in bybit_wallets.csv — skipped")
        else:
            print(f"  Bybit  key={bybit_key[:12]}...")
            if side == "BUY":
                tasks.append(("Bybit", "BUY",
                    bybit_order(bybit_key, bybit_secret, "Buy", usdt_amt, "quoteCoinQty", dry_run)))
            else:
                tasks.append(("Bybit", "SELL",
                    bybit_order(bybit_key, bybit_secret, "Sell", ami_amt, "baseCoinQty", dry_run)))
    elif exchanges == "all":
        print("  ⏭️  Bybit: skipped (retCode=10024 — use --exchange bybit to test explicitly)")

    # ── MEXC ──────────────────────────────────────────────────────────────
    if exchanges in ("mexc", "all"):
        if not mexc_key:
            print("  ⚠️  MEXC:  no key in mexc_wallets.csv — skipped")
        else:
            print(f"  MEXC   key={mexc_key[:12]}...")
            if side == "BUY":
                tasks.append(("MEXC", "BUY",
                    mexc_order(mexc_key, mexc_secret, "BUY", usdt_amt, True, dry_run)))
            else:
                tasks.append(("MEXC", "SELL",
                    mexc_order(mexc_key, mexc_secret, "SELL", ami_amt, False, dry_run)))

    # ── Panora ────────────────────────────────────────────────────────────
    if exchanges in ("panora", "all"):
        if not aptos_key:
            print("  ⚠️  Panora: no Aptos key (aptos_wallets.csv / --aptos-key / APTOS_PRIVATE_KEY) — skipped")
        else:
            aptos_addr = aptos_row.get("address", "") or "(unknown)"
            print(f"  Panora wallet={aptos_addr}  key={aptos_key[:12]}...")
            tasks.append(("Panora", side,
                panora_swap(aptos_key, side, usdt_amt, ami_amt, dry_run)))

    if not tasks:
        print("  Nothing to do.\n")
        return

    print()
    # Run all in parallel
    results = await asyncio.gather(*[t[2] for t in tasks], return_exceptions=True)

    print("Results:")
    print("─" * 60)
    for (exchange, action, _), result in zip(tasks, results):
        if isinstance(result, Exception):
            result = {"error": str(result)}
        _print_result(exchange, action, result, dry_run)
    print()


def main() -> None:
    p = argparse.ArgumentParser(
        description="Test buy/sell on Bybit, MEXC, and Panora DEX."
    )
    p.add_argument("--account",       type=int,   default=1,
                   help="Row index in wallet CSVs (1-based, default: 1)")
    p.add_argument("--exchange",      type=str,   default="all",
                   choices=["bybit", "mexc", "panora", "all"],
                   help="Exchange to test (default: all)")
    p.add_argument("--side",          type=str,   default="BUY",
                   choices=["BUY", "SELL", "buy", "sell"],
                   help="BUY or SELL (default: BUY)")
    p.add_argument("--usdt-amount",   type=float, default=1.0,
                   help="USDT to spend on BUY (default: 1.0)")
    p.add_argument("--ami-amount",    type=float, default=10.0,
                   help="AMI to sell on SELL (default: 10.0)")
    p.add_argument("--live",          action="store_true",
                   help="Place real orders (default: dry-run)")
    p.add_argument("--bybit-wallets",  type=str,   default="bybit_wallets.csv")
    p.add_argument("--mexc-wallets",   type=str,   default="mexc_wallets.csv")
    p.add_argument("--aptos-wallets",  type=str,   default="aptos_wallets.csv",
                   help="Path to aptos_wallets.csv (default: aptos_wallets.csv)")
    p.add_argument("--aptos-key",      type=str,   default="",
                   help="Aptos private key (overrides CSV / .env APTOS_PRIVATE_KEY)")
    args = p.parse_args()

    if args.live:
        print("\n🔴 WARNING: LIVE mode — real orders will be placed!")
        print("   Press Ctrl+C within 3 seconds to abort...")
        try:
            time.sleep(3)
        except KeyboardInterrupt:
            print("   Aborted.")
            return

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
