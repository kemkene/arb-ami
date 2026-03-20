"""
HyperionDexSwap — On-chain swap execution via Hyperion DEX (CLMM V3) on Aptos.

Supports:
  - swap_apt_to_ami : APT → AMI  (Fungible Asset → Fungible Asset)
  - swap_ami_to_apt : AMI → APT  (Fungible Asset → Fungible Asset)
  - get_amount_out  : On-chain view function to simulate swap output

Hyperion Router V3:
  0x8b4a2c4bb53857c718a04c020b98f8c2e1f99a68b0f57389a8bf5434cd22e05c::router_v3

Entry function (verified from on-chain ABI):
  exact_input_swap_entry(
      signer,
      fee_tier: u8,           # Pool fee tier (e.g., 1 = 0.01%, 5 = 0.05%, 30 = 0.3%, 100 = 1%)
      amount_in: u64,
      min_amount_out: u64,
      sqrt_price_limit: u128, # 0 = no limit
      token_in:  Object<Metadata>,
      token_out: Object<Metadata>,
      recipient: address,
      deadline: u64           # Unix timestamp (seconds)
  )

NOTE: pool_v3::swap is NOT an entry function. All swaps MUST go through router_v3.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp

from aptos_sdk.account import Account
from aptos_sdk.account_address import AccountAddress
from aptos_sdk.async_client import RestClient
from aptos_sdk.bcs import Serializer
from aptos_sdk.transactions import (
    EntryFunction,
    TransactionArgument,
    TransactionPayload,
)

from config.settings import settings
from utils.logger import get_logger

logger = get_logger()

# ────────────────────────────────────────────────────────────────────────────
#  Constants
# ────────────────────────────────────────────────────────────────────────────
HYPERION_MODULE = "0x8b4a2c4bb53857c718a04c020b98f8c2e1f99a68b0f57389a8bf5434cd22e05c"
HYPERION_ROUTER_V3 = f"{HYPERION_MODULE}::router_v3"
HYPERION_POOL_V3 = f"{HYPERION_MODULE}::pool_v3"

# Standard metadata addresses for Fungible Assets
APT_METADATA_ADDRESS = "0xa"
USDT_METADATA_ADDRESS = "0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b"

# fee_rate 10000 = mapping to Tier 100 (charges actual 0.1% as per SwapEventV3)
# This Tier is mandatory to find the pool on Hyperion Router
HYPERION_FEE_TIER = 100

# Decimals
APT_DECIMALS = 8
AMI_DECIMALS = 8

# Default deadline offset (seconds from now)
DEADLINE_OFFSET_SECS = 60


# ────────────────────────────────────────────────────────────────────────────
#  Result dataclass
# ────────────────────────────────────────────────────────────────────────────
@dataclass
class SwapResult:
    ok: bool = False
    tx_hash: Optional[str] = None
    amount_in: float = 0.0
    amount_out: float = 0.0
    gas_used: float = 0.0
    elapsed_ms: float = 0.0
    error: str = ""


# ────────────────────────────────────────────────────────────────────────────
#  BCS encoding helpers
# ────────────────────────────────────────────────────────────────────────────
def _encode_u8(value: int) -> TransactionArgument:
    return TransactionArgument(value, Serializer.u8)


def _encode_u64(value: int) -> TransactionArgument:
    return TransactionArgument(value, Serializer.u64)


def _encode_u128(value: int) -> TransactionArgument:
    return TransactionArgument(value, Serializer.u128)


def _encode_address(addr: AccountAddress) -> TransactionArgument:
    return TransactionArgument(addr, Serializer.struct)


def _to_octas(amount: float, decimals: int = 8) -> int:
    return int(amount * (10 ** decimals))


def _from_octas(raw: int, decimals: int = 8) -> float:
    return raw / (10 ** decimals)


# ────────────────────────────────────────────────────────────────────────────
#  HyperionDexSwap
# ────────────────────────────────────────────────────────────────────────────
class HyperionDexSwap:
    """Execute on-chain swaps via Hyperion DEX router_v3."""

    def __init__(self) -> None:
        private_key = settings.aptos_private_key
        if not private_key:
            raise ValueError(
                "APTOS_PRIVATE_KEY not set — cannot initialise HyperionDexSwap"
            )

        self.account = Account.load_key(private_key)
        self.node_url = settings.aptos_node_url
        headers = {"x-api-key": settings.cellana_grpc_api_key} if settings.cellana_grpc_api_key else None
        self.client = RestClient(self.node_url)
        # We handle headers via our own session proxy or by passing them to sdk calls if needed.
        # But for RestClient, the simplest way in this SDK version is via a custom session.
        self._headers = headers
            
        self.max_gas = settings.aptos_max_gas
        self.default_slippage_pct = settings.dex_swap_slippage_pct
        self.pool_address = settings.hyperion_swap_pool_address

        self.ami_metadata_addr = AccountAddress.from_str(settings.ami_token_address)
        self.apt_metadata_addr = AccountAddress.from_str(APT_METADATA_ADDRESS)

        self._http_session: Optional[aiohttp.ClientSession] = None

        wallet_addr = self.account.address()
        logger.info(
            f"HyperionDexSwap initialised | wallet={wallet_addr} "
            f"node={self.node_url} max_gas={self.max_gas} "
            f"slippage={self.default_slippage_pct}%"
        )

    # ------------------------------------------------------------------ #
    #  APT → AMI swap
    # ------------------------------------------------------------------ #
    async def swap_apt_to_ami(
        self,
        amount_apt: float,
        min_ami_out: Optional[float] = None,
        slippage_pct: Optional[float] = None,
    ) -> SwapResult:
        """Swap APT → AMI via Hyperion router_v3::exact_input_swap_entry."""
        t0 = time.time()
        slippage = slippage_pct or self.default_slippage_pct
        amount_in_raw = _to_octas(amount_apt, APT_DECIMALS)

        if min_ami_out is None:
            try:
                expected_raw, fee_raw = await self.get_amount_out_onchain(
                    str(self.apt_metadata_addr), amount_in_raw
                )
                expected = _from_octas(expected_raw, AMI_DECIMALS)
                min_ami_out = expected * (1.0 - slippage / 100.0)
                logger.info(
                    f"Hyperion APT→AMI quote: {amount_apt:.6f} APT → "
                    f"~{expected:.4f} AMI (min={min_ami_out:.4f}, "
                    f"fee={_from_octas(fee_raw, AMI_DECIMALS):.6f}, "
                    f"slip={slippage}%)"
                )
            except Exception as e:
                logger.warning(f"Hyperion quote failed, using 0 min_out: {e}")
                min_ami_out = 0.0

        min_out_raw = _to_octas(min_ami_out, AMI_DECIMALS)
        deadline = int(time.time()) + DEADLINE_OFFSET_SECS

        payload = self._build_exact_input_payload(
            amount_in_raw=amount_in_raw,
            min_out_raw=min_out_raw,
            token_in_addr=self.apt_metadata_addr,
            token_out_addr=self.ami_metadata_addr,
            deadline=deadline,
        )

        return await self._submit_and_wait(payload, amount_apt, "APT→AMI(Hyperion)", t0)

    # ------------------------------------------------------------------ #
    #  AMI → APT swap
    # ------------------------------------------------------------------ #
    async def swap_ami_to_apt(
        self,
        amount_ami: float,
        min_apt_out: Optional[float] = None,
        slippage_pct: Optional[float] = None,
    ) -> SwapResult:
        """Swap AMI → APT via Hyperion router_v3::exact_input_swap_entry."""
        t0 = time.time()
        slippage = slippage_pct or self.default_slippage_pct
        amount_in_raw = _to_octas(amount_ami, AMI_DECIMALS)

        if min_apt_out is None:
            try:
                expected_raw, fee_raw = await self.get_amount_out_onchain(
                    str(self.ami_metadata_addr), amount_in_raw
                )
                expected = _from_octas(expected_raw, APT_DECIMALS)
                min_apt_out = expected * (1.0 - slippage / 100.0)
                logger.info(
                    f"Hyperion AMI→APT quote: {amount_ami:.4f} AMI → "
                    f"~{expected:.6f} APT (min={min_apt_out:.6f}, "
                    f"fee={_from_octas(fee_raw, APT_DECIMALS):.6f}, "
                    f"slip={slippage}%)"
                )
            except Exception as e:
                logger.warning(f"Hyperion quote failed, using 0 min_out: {e}")
                min_apt_out = 0.0

        min_out_raw = _to_octas(min_apt_out, APT_DECIMALS)
        deadline = int(time.time()) + DEADLINE_OFFSET_SECS

        payload = self._build_exact_input_payload(
            amount_in_raw=amount_in_raw,
            min_out_raw=min_out_raw,
            token_in_addr=self.ami_metadata_addr,
            token_out_addr=self.apt_metadata_addr,
            deadline=deadline,
        )

        return await self._submit_and_wait(payload, amount_ami, "AMI→APT(Hyperion)", t0)

    # ------------------------------------------------------------------ #
    #  Build exact_input_swap_entry payload
    # ------------------------------------------------------------------ #
    def _build_exact_input_payload(
        self,
        amount_in_raw: int,
        min_out_raw: int,
        token_in_addr: AccountAddress,
        token_out_addr: AccountAddress,
        deadline: int,
    ) -> EntryFunction:
        """
        Build router_v3::exact_input_swap_entry payload.
        
        ABI signature:
          exact_input_swap_entry(
              signer,
              fee_tier: u8,
              amount_in: u64,
              min_amount_out: u64,
              sqrt_price_limit: u128,  # 0 = no limit
              token_in:  Object<Metadata>,
              token_out: Object<Metadata>,
              recipient: address,
              deadline: u64
          )
        """
        recipient = self.account.address()

        return EntryFunction.natural(
            HYPERION_ROUTER_V3,
            "exact_input_swap_entry",
            [],  # no type arguments (pure FA swap, no generics)
            [
                _encode_u8(HYPERION_FEE_TIER),        # fee_tier
                _encode_u64(amount_in_raw),            # amount_in
                _encode_u64(min_out_raw),              # min_amount_out
                _encode_u128(0),                       # sqrt_price_limit (0 = no limit)
                _encode_address(token_in_addr),        # token_in metadata
                _encode_address(token_out_addr),       # token_out metadata
                _encode_address(recipient),            # recipient
                _encode_u64(deadline),                 # deadline (unix seconds)
            ],
        )

    # ------------------------------------------------------------------ #
    #  On-chain quote via view function
    # ------------------------------------------------------------------ #
    async def get_amount_out_onchain(
        self, token_in_metadata: str, amount_in_raw: int
    ) -> tuple[int, int]:
        """
        Query pool_v3::get_amount_out view function.
        Returns: (amount_out_raw, fee_amount_raw)
        """
        url = f"{self.node_url}/view"
        payload = {
            "function": f"{HYPERION_POOL_V3}::get_amount_out",
            "type_arguments": [],
            "arguments": [
                self.pool_address,
                token_in_metadata,
                str(amount_in_raw),
            ],
        }
        session = await self._get_http_session()
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise ValueError(f"Hyperion view function failed ({resp.status}): {body}")
            data = await resp.json()

        if isinstance(data, (list, tuple)) and len(data) >= 2:
            return int(data[0]), int(data[1])
        raise ValueError(f"Unexpected view response: {data}")

    # ------------------------------------------------------------------ #
    #  Pool state queries
    # ------------------------------------------------------------------ #
    async def get_pool_reserves(self) -> tuple[int, int]:
        """Query on-chain reserves via view function."""
        url = f"{self.node_url}/view"
        payload = {
            "function": f"{HYPERION_POOL_V3}::pool_reserve_amount",
            "type_arguments": [],
            "arguments": [self.pool_address],
        }
        session = await self._get_http_session()
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                return 0, 0
            data = await resp.json()
        return int(data[0]), int(data[1])

    async def get_current_tick_and_price(self) -> tuple[int, int]:
        """Query current tick and sqrt_price via view function."""
        url = f"{self.node_url}/view"
        payload = {
            "function": f"{HYPERION_POOL_V3}::current_tick_and_price",
            "type_arguments": [],
            "arguments": [self.pool_address],
        }
        session = await self._get_http_session()
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                return 0, 0
            data = await resp.json()
        return int(data[0]), int(data[1])

    # ------------------------------------------------------------------ #
    #  Internal: HTTP session
    # ------------------------------------------------------------------ #
    async def _get_http_session(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            headers = {"x-api-key": settings.cellana_grpc_api_key} if settings.cellana_grpc_api_key else None
            self._http_session = aiohttp.ClientSession(headers=headers)
        return self._http_session

    # ------------------------------------------------------------------ #
    #  Internal: submit tx and wait
    # ------------------------------------------------------------------ #
    async def _submit_and_wait(
        self,
        entry_fn: EntryFunction,
        amount_in_human: float,
        label: str,
        t0: float,
    ) -> SwapResult:
        """Build, sign, submit and wait for an on-chain swap transaction."""
        result = SwapResult(amount_in=amount_in_human)

        try:
            tx_payload = TransactionPayload(entry_fn)

            signed_tx = await self.client.create_bcs_signed_transaction(
                sender=self.account,
                payload=tx_payload,
            )

            logger.info(f"📤 Submitting {label} swap tx...")
            tx_response = await self.client.submit_and_wait_for_bcs_transaction(
                signed_tx
            )

            elapsed = (time.time() - t0) * 1000
            result.elapsed_ms = elapsed

            tx_hash = tx_response.get("hash", "")
            success = tx_response.get("success", False)
            vm_status = tx_response.get("vm_status", "")
            gas_used_raw = int(tx_response.get("gas_used", 0))
            gas_unit_price = int(tx_response.get("gas_unit_price", 100))
            gas_apt = (gas_used_raw * gas_unit_price) / (10 ** APT_DECIMALS)

            result.tx_hash = tx_hash
            result.gas_used = gas_apt

            if success:
                result.ok = True
                result.amount_out = self._extract_swap_output(tx_response)
                logger.success(
                    f"✅ {label} CONFIRMED | tx={tx_hash[:16]}… | "
                    f"in={amount_in_human:.6f} | "
                    f"out={result.amount_out:.6f} | "
                    f"gas={gas_apt:.6f} APT | "
                    f"{elapsed:.0f}ms"
                )
            else:
                result.error = vm_status
                logger.error(
                    f"❌ {label} FAILED | tx={tx_hash[:16]}… | "
                    f"vm_status={vm_status} | {elapsed:.0f}ms"
                )

        except Exception as e:
            elapsed = (time.time() - t0) * 1000
            result.elapsed_ms = elapsed
            result.error = str(e)
            logger.error(f"❌ {label} EXCEPTION | {e} | {elapsed:.0f}ms")

        return result

    # ------------------------------------------------------------------ #
    #  Parse swap output from transaction events
    # ------------------------------------------------------------------ #
    @staticmethod
    def _extract_swap_output(tx_response: dict) -> float:
        """Extract amount_out from SwapEventV3 or deposit events."""
        events = tx_response.get("events", [])
        for ev in events:
            ev_type = ev.get("type", "")
            data = ev.get("data", {})

            if "SwapEventV3" in ev_type:
                for key in ("amount_out", "amount_a_out", "amount_b_out"):
                    val = data.get(key)
                    if val and int(val) > 0:
                        return _from_octas(int(val), 8)

            if "DepositEvent" in ev_type or "Deposit" in ev_type:
                amount = data.get("amount")
                if amount and int(amount) > 0:
                    return _from_octas(int(amount), 8)

        return 0.0

    # ------------------------------------------------------------------ #
    #  Cleanup
    # ------------------------------------------------------------------ #
    # ------------------------------------------------------------------ #
    #  Wallet balances (Asset-based using view functions)
    # ------------------------------------------------------------------ #
    async def get_balance_of_asset(self, metadata_addr: str, decimals: int = 8) -> float:
        """Get balance of a specific fungible asset using view function."""
        try:
            url = f"{self.node_url}/view"
            payload = {
                "function": "0x1::primary_fungible_store::balance",
                "type_arguments": ["0x1::fungible_asset::Metadata"],
                "arguments": [
                    str(self.account.address()),
                    metadata_addr
                ]
            }
            session = await self._get_http_session()
            async with session.post(url, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data and len(data) > 0:
                        return _from_octas(int(data[0]), decimals)
        except Exception as e:
            logger.warning(f"Failed to get balance for {metadata_addr}: {e}")
        return 0.0

    async def get_apt_balance(self) -> float:
        """Get APT balance (human-readable) using view function (Asset-based)."""
        return await self.get_balance_of_asset(APT_METADATA_ADDRESS, 8)

    async def get_ami_balance(self) -> float:
        """Get AMI fungible asset balance (human-readable)."""
        return await self.get_balance_of_asset(str(self.ami_metadata_addr), 8)
    
    async def get_usdt_balance(self) -> float:
        """Get USDT fungible asset balance (human-readable)."""
        return await self.get_balance_of_asset(USDT_METADATA_ADDRESS, 6)

    async def close(self) -> None:
        try:
            if self._http_session and not self._http_session.closed:
                await self._http_session.close()
                self._http_session = None
            await self.client.close()
        except Exception:
            pass
