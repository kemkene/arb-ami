"""
AptosTrader: Handles on-chain transfers from DEX (Aptos wallet) to CEX deposit addresses.
"""
from typing import Optional, Dict, Any
from aptos_sdk.account import Account
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

class AptosTrader:
    def __init__(self):
        self.node_url = settings.aptos_node_url
        self.rest_client = RestClient(self.node_url)
        self.wallet = Account.load_key(settings.aptos_private_key)
        
        # Resource Addresses
        self.ami_fa_address = "0xb36527754eb54d7ff55daf13bcb54b42b88ec484bd6f0e3b2e0d1db169de6451"
        self.apt_addr = "0x1"

    async def deposit_apt(self, to_address: str, amount: float) -> Optional[str]:
        """Deposit APT to a CEX address."""
        try:
            from aptos_sdk.account_address import AccountAddress
            recipient = AccountAddress.from_str(to_address)
            amount_raw = int(amount * 10**8)
            payload = EntryFunction.natural(
                "0x1::aptos_account",
                "transfer",
                [],
                [
                    TransactionArgument(recipient, Serializer.struct),
                    TransactionArgument(amount_raw, Serializer.u64),
                ],
            )
            return await self._submit_tx(payload, f"Deposit {amount} APT to {to_address}")
        except Exception as e:
            logger.error(f"❌ AptosTrader: APT deposit failed: {e}")
            return None

    async def deposit_ami(self, to_address: str, amount: float) -> Optional[str]:
        """Deposit AMI (Fungible Asset) to a CEX address."""
        try:
            from aptos_sdk.account_address import AccountAddress
            recipient = AccountAddress.from_str(to_address)
            asset_meta = AccountAddress.from_str(self.ami_fa_address)
            amount_raw = int(amount * 10**8)
            from aptos_sdk.type_tag import TypeTag, StructTag
            payload = EntryFunction.natural(
                "0x1::primary_fungible_store",
                "transfer",
                [TypeTag(StructTag.from_str("0x1::fungible_asset::Metadata"))],
                [
                    TransactionArgument(asset_meta, Serializer.struct),
                    TransactionArgument(recipient, Serializer.struct),
                    TransactionArgument(amount_raw, Serializer.u64),
                ],
            )
            return await self._submit_tx(payload, f"Deposit {amount} AMI to {to_address}")
        except Exception as e:
            logger.error(f"❌ AptosTrader: AMI deposit failed: {e}")
            return None

    async def _submit_tx(self, payload: EntryFunction, task_desc: str) -> Optional[str]:
        """Sign and submit transaction."""
        try:
            logger.info(f"📤 [AptosTrader] {task_desc}...")
            
            raw_transaction = await self.rest_client.create_bcs_transaction(
                self.wallet, 
                TransactionPayload(payload)
            )
            
            simulated_tx = await self.rest_client.simulate_transaction(raw_transaction, self.wallet)
            if not simulated_tx[0].get("success"):
                logger.error(f"❌ tx simulation failed: {simulated_tx[0].get('vm_status')}")
                return None

            signed_transaction = await self.rest_client.create_bcs_signed_transaction(
                self.wallet, 
                TransactionPayload(payload)
            )
            
            tx_hash = await self.rest_client.submit_bcs_transaction(signed_transaction)
            await self.rest_client.wait_for_transaction(tx_hash)
            
            logger.success(f"✅ {task_desc} SUCCESS! TX: {tx_hash}")
            return tx_hash
        except Exception as e:
            logger.error(f"❌ [AptosTrader] Transaction failed: {e}")
            return None

    async def close(self):
        await self.rest_client.close()
