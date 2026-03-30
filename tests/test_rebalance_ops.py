import asyncio
import os
import argparse
from aptos_sdk.account import Account
from aptos_sdk.async_client import RestClient
from aptos_sdk.transactions import TransactionPayload, EntryFunction, TransactionArgument
from aptos_sdk.type_tag import TypeTag, StructTag
from aptos_sdk.account_address import AccountAddress
from aptos_sdk.bcs import Serializer

from config.settings import settings
from exchanges.bybit_trader import BybitTrader
from exchanges.mexc_trader import MexcTrader
from utils.logger import get_logger

logger = get_logger()

def _encode_u64(value: int) -> TransactionArgument:
    return TransactionArgument(value, Serializer.u64)

def _encode_address(addr: AccountAddress) -> TransactionArgument:
    return TransactionArgument(addr, Serializer.struct)

class RebalanceTester:
    def __init__(self):
        priv_key = settings.aptos_private_key
        if not priv_key:
            raise ValueError("APTOS_PRIVATE_KEY not found in .env")
        self.account = Account.load_key(priv_key)
        self.node_url = settings.aptos_node_url
        self.client = RestClient(self.node_url)
        self.bybit = BybitTrader()
        self.mexc = MexcTrader()
        
    async def get_address(self, exchange: str, coin: str):
        """Fetch deposit address from CEX."""
        if exchange == "bybit":
            return await self.bybit.get_deposit_address(coin, "APTOS")
        elif exchange == "mexc":
            # Try APTOS(APT) first, then APTOS
            addr = await self.mexc.get_deposit_address(coin, "APTOS(APT)")
            if not addr:
                addr = await self.mexc.get_deposit_address(coin, "APTOS")
            return addr
        return None

    async def deposit_apt(self, to_address: str, amount: float):
        """Transfer APT to CEX."""
        amount_octas = int(amount * 10**8)
        logger.info(f"📤 [DEX -> CEX] Deposit {amount} APT to {to_address}")
        
        payload = TransactionPayload(
            EntryFunction.natural(
                "0x1::aptos_account",
                "transfer",
                [],
                [
                    _encode_address(AccountAddress.from_str(to_address)),
                    _encode_u64(amount_octas)
                ]
            )
        )
        return await self._submit_tx(payload, f"Deposit APT to {to_address}")

    async def deposit_ami(self, to_address: str, amount: float):
        """Transfer AMI (Fungible Asset) to CEX."""
        amount_octas = int(amount * 10**8)
        logger.info(f"📤 [DEX -> CEX] Deposit {amount} AMI to {to_address}")
        
        payload = TransactionPayload(
            EntryFunction.natural(
                "0x1::primary_fungible_store",
                "transfer",
                [TypeTag(StructTag.from_str("0x1::fungible_asset::Metadata"))],
                [
                    _encode_address(AccountAddress.from_str(settings.ami_token_address)),
                    _encode_address(AccountAddress.from_str(to_address)),
                    _encode_u64(amount_octas)
                ]
            )
        )
        return await self._submit_tx(payload, f"Deposit AMI to {to_address}")

    async def _submit_tx(self, payload, task_name: str):
        try:
            # Create signed tx
            signed_tx = await self.client.create_bcs_signed_transaction(
                sender=self.account,
                payload=payload,
            )
            # Submit and wait
            tx_response = await self.client.submit_and_wait_for_bcs_transaction(signed_tx)
            tx_hash = tx_response.get("hash", "Unknown")
            success = tx_response.get("success", False)
            
            if success:
                logger.success(f"✅ {task_name} SUCCESS! TX: {tx_hash}")
                return tx_hash
            else:
                logger.error(f"❌ {task_name} FAILED (VM Error): {tx_response.get('vm_status')}")
                return None
        except Exception as e:
            logger.error(f"❌ {task_name} EXCEPTION: {e}")
            return None

async def main():
    parser = argparse.ArgumentParser(description="Aptos Rebalance (Deposit) Tester")
    parser.add_argument("--coin", choices=["APT", "AMI"], help="Coin to deposit")
    parser.add_argument("--to", choices=["bybit", "mexc"], help="Target exchange")
    parser.add_argument("--amount", type=float, help="Amount to deposit")
    parser.add_argument("--confirm", action="store_true", help="Confirm execution")
    
    args = parser.parse_args()
    
    if not args.coin or not args.to or not args.amount:
        print("\n--- Aptos -> CEX Deposit Test ---")
        print("Usage: PYTHONPATH=. python3 tests/test_rebalance_ops.py --coin APT --to bybit --amount 0.1 --confirm")
        return

    tester = RebalanceTester()
    logger.info(f"📡 Fetching address for {args.coin} on {args.to}...")
    addr = await tester.get_address(args.to, args.coin)
    
    if not addr:
        logger.error(f"❌ Could not get deposit address for {args.coin} on {args.to}")
        return

    print(f"\n--- EXECUTION SUMMARY ---")
    print(f"Asset  : {args.coin}")
    print(f"To     : {args.to}")
    print(f"Address: {addr}")
    print(f"Amount : {args.amount}")
    print(f"-------------------------")
    
    if not args.confirm:
        print("Use --confirm to execute.")
        return

    if args.coin == "APT":
        await tester.deposit_apt(addr, args.amount)
    else:
        await tester.deposit_ami(addr, args.amount)

if __name__ == "__main__":
    asyncio.run(main())
