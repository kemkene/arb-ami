"""
Execute Panora DEX swaps by signing and submitting Aptos BCS transactions.

Flow:
  1. Call Panora POST /swap with toWalletAddress → get transaction payload.
  2. Extract payload from quotes[0]["txData"].
  3. BCS-encode the 20 router_entry arguments manually (REST endpoint rejects
     Option<signer> types so we cannot use encode_submission).
  4. Sign + submit via aptos-sdk BCS path.
  5. Pre-flight: check APT balance via view function and cap max_gas_amount
     dynamically so small wallets don't get INSUFFICIENT_BALANCE_FOR_FEE.
"""
from __future__ import annotations

import re
import asyncio
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import aiohttp
from aptos_sdk.account import Account
from aptos_sdk.account_address import AccountAddress
from aptos_sdk.async_client import RestClient
from aptos_sdk.bcs import Serializer
from aptos_sdk.transactions import (
    EntryFunction,
    TransactionPayload,
)
from aptos_sdk.type_tag import StructTag, TypeTag

from config.settings import settings
from utils.logger import get_logger

if TYPE_CHECKING:
    from exchanges.panora import PanoraClient

logger = get_logger()

# ---------------------------------------------------------------------------
# Panora router_entry parameter types (20 args, excluding implicit &signer).
# Must match the on-chain function exactly — used for manual BCS encoding.
# ---------------------------------------------------------------------------
PANORA_ROUTER_PARAM_TYPES: List[str] = [
    "0x1::option::Option<signer>",                                      # [0]  integrator signer (always None)
    "address",                                                           # [1]  to_wallet
    "u64",                                                               # [2]
    "u8",                                                                # [3]  num_splits
    "vector<u8>",                                                        # [4]  pool_type_vec
    "vector<vector<vector<u8>>>",                                        # [5]  pool_info
    "vector<vector<vector<u64>>>",                                       # [6]  pool_amounts
    "vector<vector<vector<bool>>>",                                      # [7]  pool_flags
    "vector<vector<u8>>",                                                # [8]
    "vector<vector<vector<address>>>",                                   # [9]  pool_addrs
    "vector<vector<address>>",                                           # [10] from_addrs
    "vector<vector<address>>",                                           # [11] to_addrs
    "0x1::option::Option<vector<vector<vector<vector<vector<u8>>>>>>",   # [12]
    "vector<vector<vector<u64>>>",                                       # [13] min_output_amounts
    "0x1::option::Option<vector<vector<vector<u8>>>>",                   # [14]
    "address",                                                           # [15] output_token
    "vector<u64>",                                                       # [16] amounts
    "u64",                                                               # [17] from_amount
    "u64",                                                               # [18] min_out
    "address",                                                           # [19] fee_addr
]


def _bcs_encode(type_str: str, value: Any) -> bytes:
    """BCS-encode a single Move value given its type string.

    Handles: bool, u8, u64, u128, address, vector<T>, Option<T>.
    """
    ser = Serializer()

    def write(t: str, v: Any) -> None:
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
            ser.uleb128(0)   # always None — signer passed implicitly by VM
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


class PanoraExecutor:
    """Sign and submit Aptos transactions for Panora DEX swaps."""

    GAS_UNIT_PRICE = 100        # octas per gas unit
    MIN_GAS_UNITS  = 5_000      # minimum for a Panora swap (~0.005 APT)
    MAX_GAS_UNITS  = 200_000    # SDK default cap
    APT_DECIMALS   = 8

    def __init__(self, panora_client: "PanoraClient") -> None:
        self.panora_client = panora_client
        self._aptos_client: Optional[RestClient] = None
        self._account: Optional[Account] = None

    # ------------------------------------------------------------------ #
    #  Internal helpers
    # ------------------------------------------------------------------ #
    def _get_account(self) -> Optional[Account]:
        if not settings.aptos_private_key:
            return None
        if self._account is None:
            try:
                self._account = Account.load_key(settings.aptos_private_key)
            except Exception as e:
                logger.error(f"PanoraExecutor: failed to load Aptos key: {e}")
        return self._account

    def _get_client(self) -> RestClient:
        if self._aptos_client is None:
            self._aptos_client = RestClient(settings.aptos_node_url)
        return self._aptos_client

    @staticmethod
    def _extract_payload(data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract the entry-function payload from a Panora API response.

        Primary path: data["quotes"][0]["txData"]
        Falls back to legacy key paths for robustness.
        """
        # Primary path (current Panora SDK)
        quotes = data.get("quotes")
        if isinstance(quotes, list) and quotes:
            tx_data = quotes[0].get("txData")
            if isinstance(tx_data, dict) and "function" in tx_data:
                return tx_data

        # Legacy / fallback paths
        for candidate in [
            data.get("data"),
            data.get("txData"),
            data.get("payload"),
            data.get("swap"),
            data if "function" in data else None,
        ]:
            if isinstance(candidate, dict) and (
                "function" in candidate or "fn" in candidate
            ):
                return candidate

        return None

    async def _check_apt_balance(self, wallet: str) -> Optional[int]:
        """Return APT balance in octas via Aptos view function, or None on error."""
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    f"{settings.aptos_node_url}/view",
                    json={
                        "function":       "0x1::coin::balance",
                        "type_arguments": ["0x1::aptos_coin::AptosCoin"],
                        "arguments":      [wallet],
                    },
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return int(data[0])
                    body = await resp.text()
                    logger.warning(
                        f"PanoraExecutor: APT balance check HTTP {resp.status}: {body[:120]}"
                    )
        except Exception as e:
            logger.warning(f"PanoraExecutor: could not check APT balance: {e}")
        return None

    async def get_token_balance(
        self,
        wallet: str,
        token_address: str,
        decimals: int = 8,
    ) -> Optional[float]:
        """Return human-readable token balance for any Aptos coin/FA.

        For APT (0x1::aptos_coin::AptosCoin):
            Uses ``0x1::coin::balance`` view function.
        For other tokens (FA or legacy Coin):
            Tries ``0x1::primary_fungible_store::balance`` first (FA standard),
            then falls back to ``0x1::coin::balance<token_address>``.

        Returns float balance (divided by 10^decimals), or None on error.
        """
        try:
            async with aiohttp.ClientSession() as s:
                if token_address in (
                    "0x1::aptos_coin::AptosCoin",
                    "0x000000000000000000000000000000000000000000000000000000000000000a",
                    "0xa",
                ):
                    # Native APT via coin::balance
                    async with s.post(
                        f"{settings.aptos_node_url}/view",
                        json={
                            "function":       "0x1::coin::balance",
                            "type_arguments": ["0x1::aptos_coin::AptosCoin"],
                            "arguments":      [wallet],
                        },
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            return int(data[0]) / (10 ** self.APT_DECIMALS)
                        body = await resp.text()
                        logger.warning(
                            f"PanoraExecutor.get_token_balance APT {resp.status}: {body[:120]}"
                        )
                        return None

                # --- Non-APT: try FA primary_fungible_store::balance first ---
                fa_addr = token_address.split("::")[0]  # strip module/struct portions
                async with s.post(
                    f"{settings.aptos_node_url}/view",
                    json={
                        "function":       "0x1::primary_fungible_store::balance",
                        "type_arguments": ["0x1::fungible_asset::Metadata"],
                        "arguments":      [wallet, fa_addr],
                    },
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return int(data[0]) / (10 ** decimals)

                # --- Fallback: legacy coin::balance<T> ---
                async with s.post(
                    f"{settings.aptos_node_url}/view",
                    json={
                        "function":       "0x1::coin::balance",
                        "type_arguments": [token_address],
                        "arguments":      [wallet],
                    },
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return int(data[0]) / (10 ** decimals)
                    body = await resp.text()
                    logger.warning(
                        f"PanoraExecutor.get_token_balance {token_address} {resp.status}: {body[:120]}"
                    )

        except Exception as e:
            logger.warning(f"PanoraExecutor.get_token_balance exception: {e}")
        return None

    def _compute_max_gas(self, apt_octas: Optional[int]) -> int:
        """Compute a safe max_gas_amount given available APT balance."""
        if apt_octas is None:
            return self.MAX_GAS_UNITS
        dyn = min(self.MAX_GAS_UNITS,
                  int(apt_octas * 0.9) // self.GAS_UNIT_PRICE)
        return max(dyn, 0)

    # ------------------------------------------------------------------ #
    #  Public interface
    # ------------------------------------------------------------------ #
    async def execute_swap(
        self,
        from_amount: float,
        from_token_address: str,
        to_token_address: str,
        slippage_pct: float = 1.0,
        prefetched_quote: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Execute a Panora swap. Returns confirmed Aptos tx hash, or None.

        If `prefetched_quote` is provided (already fetched during price
        verification), it is used directly — no second Panora API call.
        """
        account = self._get_account()
        if account is None:
            logger.error(
                "PanoraExecutor: APTOS_PRIVATE_KEY not set — cannot execute swap"
            )
            return None

        wallet = str(account.address())

        # ── Step 1: ensure PanoraClient sends toWalletAddress ─────────
        self.panora_client.to_wallet_address = wallet

        # ── Step 2: use prefetched quote, or fetch a fresh one ─────────
        # A "synthetic" prefetched_quote was built from the unit-price cache
        # during verification (price × qty, no txData).  We must fetch a real
        # quote here so the executor has the transaction payload it needs.
        if prefetched_quote is not None and not self.panora_client.is_synthetic(prefetched_quote):
            quote = prefetched_quote
            logger.debug(
                f"PanoraExecutor: reusing prefetched quote — skipping API call "
                f"(from={from_token_address[:16]}… amount={from_amount})"
            )
        else:
            if prefetched_quote is not None:
                logger.debug(
                    f"PanoraExecutor: prefetched quote is synthetic — "
                    f"fetching execution quote with force_fresh "
                    f"(from={from_token_address[:16]}… amount={from_amount})"
                )
            # force_fresh=True bypasses both caches so we always get a real
            # response with txData for on-chain execution.
            quote = await self.panora_client.get_swap_quote(
                from_amount,
                from_token_address=from_token_address,
                to_token_address=to_token_address,
                force_fresh=True,
            )
        if not quote:
            logger.error("PanoraExecutor: swap quote request failed")
            return None

        # ── Step 3: extract entry-function payload ─────────────────────
        payload = self._extract_payload(quote)
        if payload is None:
            logger.error(
                f"PanoraExecutor: no transaction payload in response "
                f"keys={list(quote.keys())}"
            )
            return None

        func: str = payload.get("function") or payload.get("fn", "")
        type_arg_strs: list = (
            payload.get("typeArguments")
            or payload.get("type_arguments")
            or []
        )
        raw_args: list = (
            payload.get("functionArguments")
            or payload.get("arguments")
            or []
        )

        if not func:
            logger.error("PanoraExecutor: empty function field in payload")
            return None

        # ── Step 4: BCS-encode all 20 arguments ───────────────────────
        if len(raw_args) != len(PANORA_ROUTER_PARAM_TYPES):
            logger.error(
                f"PanoraExecutor: expected {len(PANORA_ROUTER_PARAM_TYPES)} args, "
                f"got {len(raw_args)}"
            )
            return None

        bcs_args = []
        try:
            for i, (t, v) in enumerate(zip(PANORA_ROUTER_PARAM_TYPES, raw_args)):
                bcs_args.append(_bcs_encode(t, v))
        except Exception as e:
            logger.error(f"PanoraExecutor: BCS encoding failed at arg[{i}]: {e}")
            return None

        # Build type tags from Panora response
        try:
            type_tags = [TypeTag(StructTag.from_str(t)) for t in type_arg_strs]
        except Exception as e:
            logger.error(f"PanoraExecutor: type tag parsing failed: {e}")
            return None

        parts        = func.split("::")
        module_str   = "::".join(parts[:-1])
        function_name = parts[-1]
        entry_fn = EntryFunction.natural(module_str, function_name, type_tags, bcs_args)

        # ── Step 5: check APT balance → compute max_gas_amount ─────────
        apt_octas = await self._check_apt_balance(wallet)
        max_gas   = self._compute_max_gas(apt_octas)

        if apt_octas is not None:
            apt_human = apt_octas / 10 ** self.APT_DECIMALS
            if max_gas < self.MIN_GAS_UNITS:
                logger.error(
                    f"PanoraExecutor: insufficient APT for gas | "
                    f"balance={apt_human:.6f} APT  "
                    f"need ≥{self.MIN_GAS_UNITS * self.GAS_UNIT_PRICE / 10**self.APT_DECIMALS:.4f} APT"
                )
                return None
            if max_gas < self.MAX_GAS_UNITS:
                logger.warning(
                    f"PanoraExecutor: low APT ({apt_human:.6f}) — "
                    f"capping max_gas={max_gas:,} units"
                )

        # ── Step 6: sign + submit ──────────────────────────────────────
        try:
            client = self._get_client()
            client.client_config.max_gas_amount = max_gas
            client.client_config.gas_unit_price = self.GAS_UNIT_PRICE

            signed = await client.create_bcs_signed_transaction(
                account, TransactionPayload(entry_fn)
            )
            txn_hash = await client.submit_bcs_transaction(signed)
            logger.info(f"⏳ Panora swap submitted | tx={txn_hash}")

            await client.wait_for_transaction(txn_hash)
            logger.success(
                f"✅ Panora swap confirmed | "
                f"from={from_token_address[:16]}…  amount={from_amount} | "
                f"tx={txn_hash}"
            )
            return txn_hash

        except Exception as e:
            err = str(e)
            m = re.search(r'"vm_status"\s*:\s*"([^"]+)"', err)
            if m:
                err = m.group(1)
            logger.error(f"❌ PanoraExecutor swap failed: {err}")
            return None

    async def close(self) -> None:
        if self._aptos_client:
            await self._aptos_client.close()
            self._aptos_client = None
