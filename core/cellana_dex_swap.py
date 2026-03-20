"""
CellanaDexSwap — On-chain swap execution via Cellana DEX on Aptos.

Supports:
  - swap_apt_to_ami : APT → AMI  (Coin<AptosCoin>  → Fungible Asset)
  - swap_ami_to_apt : AMI → APT  (Fungible Asset → Coin<AptosCoin>)
  - get_amount_out  : View function to simulate swap output (no tx)

Cellana Router:
  0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::router

Entry functions:
  swap_coin_for_asset_entry<CoinType>(
      signer, amount_in: u64, min_amount_out: u64,
      asset_metadata: Object<Metadata>, is_stable: bool, recipient: address
  )
  swap_asset_for_coin_entry<CoinType>(
      signer, amount_in: u64, min_amount_out: u64,
      asset_metadata: Object<Metadata>, is_stable: bool, recipient: address
  )
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
from aptos_sdk.type_tag import TypeTag, StructTag

from config.settings import settings
from utils.logger import get_logger

logger = get_logger()

# ────────────────────────────────────────────────────────────────────────────
#  Constants
# ────────────────────────────────────────────────────────────────────────────
CELLANA_ROUTER = (
    "0x4bf51972879e3b95c4781a5cdcb9e1ee24ef483e7d22f2d903626f126df62bd1::router"
)
APTOS_COIN_TYPE = "0x1::aptos_coin::AptosCoin"

# APT fungible-asset metadata address on Aptos mainnet
# (found from Cellana pool's token_store — NOT the legacy 0x1 coin address)
APT_FA_METADATA = "0xedc2704f2cef417a06d1756a04a16a9fa6faaed13af469be9cdfcac5a21a8e2e"

# Decimals: both APT and AMI use 8 decimals on Aptos
APT_DECIMALS = 8
AMI_DECIMALS = 8

# Standard metadata addresses for Fungible Assets
APT_METADATA_ADDRESS = "0xa"
USDT_METADATA_ADDRESS = "0x357b0b74bc833e95a115ad22604854d6b0fca151cecd94111770e5d6ffc9dc2b"


# ────────────────────────────────────────────────────────────────────────────
#  Result dataclass
# ────────────────────────────────────────────────────────────────────────────
@dataclass
class SwapResult:
    ok: bool = False
    tx_hash: Optional[str] = None
    amount_in: float = 0.0       # human-readable
    amount_out: float = 0.0      # human-readable (from events, if available)
    gas_used: float = 0.0        # APT
    elapsed_ms: float = 0.0
    error: str = ""


# ────────────────────────────────────────────────────────────────────────────
#  BCS encoding helpers
# ────────────────────────────────────────────────────────────────────────────
def _encode_u64(value: int) -> TransactionArgument:
    return TransactionArgument(value, Serializer.u64)


def _encode_bool(value: bool) -> TransactionArgument:
    return TransactionArgument(value, Serializer.bool)


def _encode_address(addr: AccountAddress) -> TransactionArgument:
    return TransactionArgument(addr, Serializer.struct)


def _to_octas(amount: float, decimals: int = 8) -> int:
    """Convert human-readable amount to on-chain integer (e.g. 1.5 APT → 150000000)."""
    return int(amount * (10 ** decimals))


def _from_octas(raw: int, decimals: int = 8) -> float:
    """Convert on-chain integer to human-readable float."""
    return raw / (10 ** decimals)


# ────────────────────────────────────────────────────────────────────────────
#  CellanaDexSwap
# ────────────────────────────────────────────────────────────────────────────
class CellanaDexSwap:
    """Execute on-chain swaps via Cellana DEX router."""

    def __init__(self) -> None:
        # Load Aptos account from private key
        private_key = settings.aptos_private_key
        if not private_key:
            raise ValueError(
                "APTOS_PRIVATE_KEY not set in .env — cannot initialise CellanaDexSwap"
            )

        self.account = Account.load_key(private_key)
        self.node_url = settings.aptos_node_url
        self.client = RestClient(self.node_url)
        
        # Add API Key to headers if available
        if settings.aptos_node_api_key:
            self.client.client_config.headers["x-api-key"] = settings.aptos_node_api_key
            logger.info("CellanaDexSwap: Aptos Node API Key configured.")
            
        self.max_gas = settings.aptos_max_gas
        self.default_slippage_pct = settings.dex_swap_slippage_pct

        # Fungible-asset metadata addresses
        self.ami_metadata_addr = AccountAddress.from_str(settings.ami_token_address)
        self.apt_metadata_addr = AccountAddress.from_str(APT_METADATA_ADDRESS)

        # AptosCoin type tag (for generic <CoinType> in coin↔asset entry fns)
        self.aptos_coin_tag = TypeTag(StructTag.from_str(APTOS_COIN_TYPE))

        # Persistent aiohttp session for view functions (avoid session-per-call)
        self._http_session: Optional[aiohttp.ClientSession] = None

        wallet_addr = self.account.address()
        logger.info(
            f"CellanaDexSwap initialised | wallet={wallet_addr} "
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
        """Swap APT → AMI via Cellana router.

        Args:
            amount_apt: Amount of APT to swap (human-readable, e.g. 1.5).
            min_ami_out: Explicit minimum AMI output. If None, uses slippage %.
            slippage_pct: Override default slippage. Used to compute min_ami_out
                          when min_ami_out is not given.
        """
        t0 = time.time()
        slippage = slippage_pct or self.default_slippage_pct

        amount_in_raw = _to_octas(amount_apt, APT_DECIMALS)

        # If no explicit min_out, simulate to get expected output
        if min_ami_out is None:
            try:
                expected = await self.get_amount_out_apt_to_ami(amount_apt)
                min_ami_out = expected * (1.0 - slippage / 100.0)
                logger.info(
                    f"APT→AMI quote: {amount_apt:.6f} APT → "
                    f"~{expected:.4f} AMI (min={min_ami_out:.4f}, "
                    f"slip={slippage}%)"
                )
            except Exception as e:
                logger.warning(f"get_amount_out failed, using 0 min_out: {e}")
                min_ami_out = 0.0

        min_out_raw = _to_octas(min_ami_out, AMI_DECIMALS)
        recipient = self.account.address()

        # swap_coin_for_asset_entry<0x1::aptos_coin::AptosCoin>(
        #     signer, amount_in, min_amount_out,
        #     asset_metadata: Object<Metadata>, is_stable, recipient
        # )
        # Uses Coin<AptosCoin> on the input side (legacy coin form).
        payload = EntryFunction.natural(
            CELLANA_ROUTER,
            "swap_coin_for_asset_entry",
            [self.aptos_coin_tag],                    # <AptosCoin>
            [
                _encode_u64(amount_in_raw),           # amount_in
                _encode_u64(min_out_raw),             # min_amount_out
                _encode_address(self.ami_metadata_addr),  # asset_metadata: AMI
                _encode_bool(False),                  # is_stable = false
                _encode_address(recipient),           # recipient
            ],
        )

        return await self._submit_and_wait(
            payload, amount_apt, "APT→AMI", t0
        )

    # ------------------------------------------------------------------ #
    #  AMI → APT swap
    # ------------------------------------------------------------------ #
    async def swap_ami_to_apt(
        self,
        amount_ami: float,
        min_apt_out: Optional[float] = None,
        slippage_pct: Optional[float] = None,
    ) -> SwapResult:
        """Swap AMI → APT via Cellana router.

        Args:
            amount_ami: Amount of AMI to swap (human-readable).
            min_apt_out: Explicit minimum APT output. If None, uses slippage %.
            slippage_pct: Override default slippage.
        """
        t0 = time.time()
        slippage = slippage_pct or self.default_slippage_pct

        amount_in_raw = _to_octas(amount_ami, AMI_DECIMALS)

        if min_apt_out is None:
            try:
                expected = await self.get_amount_out_ami_to_apt(amount_ami)
                min_apt_out = expected * (1.0 - slippage / 100.0)
                logger.info(
                    f"AMI→APT quote: {amount_ami:.4f} AMI → "
                    f"~{expected:.6f} APT (min={min_apt_out:.6f}, "
                    f"slip={slippage}%)"
                )
            except Exception as e:
                logger.warning(f"get_amount_out failed, using 0 min_out: {e}")
                min_apt_out = 0.0

        min_out_raw = _to_octas(min_apt_out, APT_DECIMALS)
        recipient = self.account.address()

        # swap_asset_for_coin_entry<0x1::aptos_coin::AptosCoin>(
        #     signer, amount_in, min_amount_out,
        #     asset_metadata: Object<Metadata>, is_stable, recipient
        # )
        # AMI is a fungible asset; output is Coin<AptosCoin> (legacy form).
        payload = EntryFunction.natural(
            CELLANA_ROUTER,
            "swap_asset_for_coin_entry",
            [self.aptos_coin_tag],                    # <AptosCoin>
            [
                _encode_u64(amount_in_raw),           # amount_in
                _encode_u64(min_out_raw),             # min_amount_out
                _encode_address(self.ami_metadata_addr),  # asset_metadata: AMI
                _encode_bool(False),                  # is_stable = false
                _encode_address(recipient),           # recipient
            ],
        )

        return await self._submit_and_wait(
            payload, amount_ami, "AMI→APT", t0
        )

    # ------------------------------------------------------------------ #
    #  View: get_amount_out (simulation, no tx)
    # ------------------------------------------------------------------ #
    async def get_amount_out_apt_to_ami(self, amount_apt: float) -> float:
        """Simulate APT→AMI swap: returns expected AMI output (human-readable)."""
        amount_raw = _to_octas(amount_apt, APT_DECIMALS)
        result = await self._view_get_amount_out(
            amount_in=amount_raw,
            from_token=str(self.apt_metadata_addr),
            to_token=str(self.ami_metadata_addr),
        )
        return _from_octas(result, AMI_DECIMALS)

    async def get_amount_out_ami_to_apt(self, amount_ami: float) -> float:
        """Simulate AMI→APT swap: returns expected APT output (human-readable)."""
        amount_raw = _to_octas(amount_ami, AMI_DECIMALS)
        result = await self._view_get_amount_out(
            amount_in=amount_raw,
            from_token=str(self.ami_metadata_addr),
            to_token=str(self.apt_metadata_addr),
        )
        return _from_octas(result, APT_DECIMALS)

    async def _get_http_session(self) -> aiohttp.ClientSession:
        """Lazy-init and return persistent aiohttp session."""
        if self._http_session is None or self._http_session.closed:
            headers = {"x-api-key": settings.cellana_grpc_api_key} if settings.cellana_grpc_api_key else None
            self._http_session = aiohttp.ClientSession(headers=headers)
        return self._http_session

    async def _view_get_amount_out(
        self, amount_in: int, from_token: str, to_token: str
    ) -> int:
        """Call router::get_amount_out view function via REST.

        Uses direct HTTP POST because the SDK's view() may encode booleans
        differently from what the Aptos API expects.

        Returns raw u64 amount_out.
        """
        url = f"{self.node_url}/view"
        payload = {
            "function": f"{CELLANA_ROUTER}::get_amount_out",
            "type_arguments": [],
            "arguments": [
                str(amount_in),  # u64 as string
                from_token,      # Object<Metadata> address
                to_token,        # Object<Metadata> address
                False,           # is_stable: bool (JSON boolean)
            ],
        }
        session = await self._get_http_session()
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                body = await resp.text()
                raise ValueError(f"View function failed ({resp.status}): {body}")
            data = await resp.json()

        # Response: [amount_out_str, fee_str]
        if isinstance(data, (list, tuple)) and len(data) >= 1:
            return int(data[0])
        raise ValueError(f"Unexpected view response: {data}")

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
        return await self.get_balance_of_asset(APT_METADATA_ADDRESS, APT_DECIMALS)

    async def get_ami_balance(self) -> float:
        """Get AMI fungible asset balance (human-readable)."""
        return await self.get_balance_of_asset(str(self.ami_metadata_addr), AMI_DECIMALS)

    async def get_usdt_balance(self) -> float:
        """Get USDT fungible asset balance (human-readable)."""
        return await self.get_balance_of_asset(USDT_METADATA_ADDRESS, 6)

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
        """Build, sign, submit and wait for an on-chain swap transaction.
        Includes exponential backoff for Rate Limit errors.
        """
        max_retries = 3
        retry_delay = 1.0  # seconds

        for attempt in range(max_retries + 1):
            result = SwapResult(amount_in=amount_in_human)
            try:
                tx_payload = TransactionPayload(entry_fn)

                # Create signed tx
                signed_tx = await self.client.create_bcs_signed_transaction(
                    sender=self.account,
                    payload=tx_payload,
                )

                # Submit and wait for confirmation
                logger.info(f"📤 Submitting {label} swap tx (attempt {attempt+1})...")
                tx_response = await self.client.submit_and_wait_for_bcs_transaction(
                    signed_tx
                )

                elapsed = (time.time() - t0) * 1000
                result.elapsed_ms = elapsed

                # Parse response
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
                    # Try to extract amount_out from events
                    result.amount_out = self._extract_swap_output(
                        tx_response, label
                    )
                    logger.success(
                        f"✅ {label} CONFIRMED | tx={tx_hash[:16]}… | "
                        f"in={amount_in_human:.6f} | "
                        f"out={result.amount_out:.6f} | "
                        f"gas={gas_apt:.6f} APT | "
                        f"{elapsed:.0f}ms"
                    )
                    return result
                else:
                    result.error = vm_status
                    # Check if it's a rate limit error in vm_status or response
                    # Note: submit_and_wait_for_bcs_transaction might raise Exception for 429
                    logger.error(
                        f"❌ {label} FAILED | tx={tx_hash[:16]}… | "
                        f"vm_status={vm_status} | {elapsed:.0f}ms"
                    )
                    return result

            except Exception as e:
                err_msg = str(e)
                # Detect rate limit
                if "429" in err_msg or "rate limit" in err_msg.lower():
                    if attempt < max_retries:
                        wait_time = retry_delay * (2 ** attempt)
                        logger.warning(f"⚠️ {label} Rate Limited (attempt {attempt+1}). Retrying in {wait_time:.1f}s...")
                        await asyncio.sleep(wait_time)
                        continue
                
                elapsed = (time.time() - t0) * 1000
                result.elapsed_ms = elapsed
                result.error = err_msg
                logger.error(f"❌ {label} EXCEPTION | {err_msg} | {elapsed:.0f}ms")
                return result

        return result

    # ------------------------------------------------------------------ #
    #  Parse swap output from transaction events
    # ------------------------------------------------------------------ #
    @staticmethod
    def _extract_swap_output(tx_response: dict, label: str) -> float:
        """Try to extract the actual amount_out from SyncEvent or SwapEvent."""
        events = tx_response.get("events", [])
        for ev in events:
            ev_type = ev.get("type", "")
            data = ev.get("data", {})

            # Cellana emits SyncEvent with reserve updates, and SwapEvent
            if "SwapEvent" in ev_type or "Swap" in ev_type:
                # Try common fields
                for key in ("amount_out", "amount1_out", "amount0_out"):
                    val = data.get(key)
                    if val and int(val) > 0:
                        return _from_octas(int(val), 8)

            # Also check WithdrawEvent / DepositEvent for fungible assets
            if "DepositEvent" in ev_type or "Deposit" in ev_type:
                amount = data.get("amount")
                if amount and int(amount) > 0:
                    return _from_octas(int(amount), 8)

        return 0.0

    # ------------------------------------------------------------------ #
    #  Cleanup
    # ------------------------------------------------------------------ #
    async def close(self) -> None:
        """Close the REST client session and HTTP session."""
        try:
            if self._http_session and not self._http_session.closed:
                await self._http_session.close()
                self._http_session = None
            await self.client.close()
        except Exception:
            pass
        except Exception:
            pass
